"""Runtime settings persistence for operator-tunable values (0.1.4).

Exposes three Kuzu-related knobs (``kg_kuzu_buffer_pool_mb``,
``kg_kuzu_max_db_size_gb``, ``kg_connection_pool_size``) via a key-value
table in the main app SQLite DB, read by the UI (``Settings`` menu) and
by the backend at boot to override :class:`CoreSettings` defaults.

Kùzu ``Database()`` is constructor-time — values only take effect on
process restart. The endpoint layer communicates that via a
``restart_required`` flag in the GET/PUT response.

Precedence at boot (documented in BR2 ``Precedencia de config``):
    env var (non-empty) > persisted settings table > CoreSettings default
"""

from __future__ import annotations

import asyncio
import logging
import os
import warnings
from datetime import timezone
from typing import Any

from sqlalchemy import String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from okto_pulse.core.infra.database import Base, get_session_factory
from okto_pulse.core.infra.config import CoreSettings, configure_settings, get_settings

logger = logging.getLogger("okto_pulse.services.settings")

# Graph DB keys — changing any of these requires a full process restart
# (Kùzu Database() is constructor-time). The frontend amber banner is
# triggered iff one of these diverges from the boot snapshot.
GRAPH_DB_KEYS: tuple[str, ...] = (
    "kg_kuzu_buffer_pool_mb",
    "kg_kuzu_max_db_size_gb",
    "kg_connection_pool_size",
)

# Event Queue keys (spec bdcda842) — hot-reload, no restart required.
# The worker pool re-reads CoreSettings on every claim (5s cache TTL).
EVENT_QUEUE_KEYS: tuple[str, ...] = (
    "kg_queue_max_concurrent_workers",
    "kg_queue_min_interval_ms",
    "kg_queue_claim_timeout_s",
    "kg_queue_max_attempts",
    "kg_queue_alert_threshold",
)

# Decay Tick keys (spec 54399628 — Wave 2 NC f9732afc). Hot-reload via
# scheduler.reschedule_job — see _maybe_reschedule_tick below. Mudanças
# em qualquer destes NÃO marcam restart_required.
DECAY_TICK_KEYS: tuple[str, ...] = (
    "kg_decay_tick_interval_minutes",
    "kg_decay_tick_staleness_days",
    "kg_decay_tick_max_age_days",
)

# Keys persisted in the `app_settings` table. Adding a new key here is enough
# to expose it via the REST endpoint; update the RuntimeSettingsPayload to
# include a validator.
RUNTIME_KEYS: tuple[str, ...] = GRAPH_DB_KEYS + EVENT_QUEUE_KEYS + DECAY_TICK_KEYS

# Legacy env var name → canonical settings key. Read once at boot in
# apply_persisted_settings_to_core_settings; emits DeprecationWarning if used.
# Removal scheduled for v0.5.0.
_LEGACY_ENV_ALIASES: tuple[tuple[str, str], ...] = (
    ("KG_MAX_QUEUE_DEPTH", "kg_queue_alert_threshold"),
)


class AppSetting(Base):
    """Key-value row for persisted runtime settings (small, string values)."""

    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(String(64), nullable=False)


_write_lock = asyncio.Lock()
# Snapshot of the values loaded at boot. Used to compute restart_required.
_boot_snapshot: dict[str, int] = {}


def _read_boot_snapshot() -> dict[str, int]:
    """Return a copy of the settings that were active at boot.

    If the app started before :func:`apply_persisted_settings_to_core_settings`
    ran (shouldn't happen in prod but tests often bypass boot), falls back to
    the live CoreSettings which is the best approximation.
    """
    if _boot_snapshot:
        return dict(_boot_snapshot)
    s = get_settings()
    return {k: int(getattr(s, k)) for k in RUNTIME_KEYS}


async def _load_persisted_rows(db: AsyncSession) -> dict[str, int]:
    """Load every row from ``app_settings`` and coerce values to int.

    Returns an empty dict on any error so a broken table never blocks boot.
    """
    try:
        result = await db.execute(
            select(AppSetting).where(AppSetting.key.in_(RUNTIME_KEYS))
        )
        rows = result.scalars().all()
        out: dict[str, int] = {}
        for row in rows:
            try:
                out[row.key] = int(row.value)
            except (TypeError, ValueError):
                logger.warning(
                    "settings.invalid_persisted_value key=%s value=%r",
                    row.key, row.value,
                )
        return out
    except Exception as exc:
        logger.warning("settings.load_failed err=%s", exc)
        return {}


def _resolve_legacy_env_aliases() -> dict[str, int]:
    """Resolve deprecated env vars into canonical settings keys.

    Spec bdcda842 (TR12): KG_MAX_QUEUE_DEPTH was the admission-gate threshold
    in v0.1.6; it is now an alerting-only threshold renamed to
    kg_queue_alert_threshold. We honour the legacy env var until v0.5.0 and
    emit a DeprecationWarning + structured log when it fires.

    Only applies when the legacy env var is set AND the new env var is NOT
    set (so an explicit new-style override always wins).
    """
    resolved: dict[str, int] = {}
    for legacy_env, canonical_key in _LEGACY_ENV_ALIASES:
        raw = os.environ.get(legacy_env)
        if not raw:
            continue
        canonical_env = canonical_key.upper()
        if os.environ.get(canonical_env):
            continue
        try:
            resolved[canonical_key] = int(raw)
        except ValueError:
            logger.warning(
                "settings.legacy_env_invalid name=%s value=%r",
                legacy_env, raw,
            )
            continue
        msg = (
            f"Env var {legacy_env} is deprecated and will be removed in "
            f"v0.5.0; use {canonical_env} instead. Mapped value={raw} into "
            f"{canonical_key}."
        )
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        logger.warning(
            "settings.legacy_env_used legacy=%s canonical=%s value=%d",
            legacy_env, canonical_key, resolved[canonical_key],
            extra={
                "event": "settings.legacy_env_used",
                "legacy_env": legacy_env,
                "canonical_key": canonical_key,
                "value": resolved[canonical_key],
                "version_removed": "0.5.0",
            },
        )
    return resolved


async def apply_persisted_settings_to_core_settings() -> dict[str, int]:
    """Read the ``app_settings`` table and override :class:`CoreSettings`.

    Called once at app startup **before** any module imports Kùzu. Logs the
    resolved values in a single structured line for audit.
    Returns the snapshot that was applied (for caller bookkeeping).
    """
    factory = get_session_factory()
    async with factory() as db:
        persisted = await _load_persisted_rows(db)

    # Resolve legacy env aliases (e.g. KG_MAX_QUEUE_DEPTH → kg_queue_alert_threshold).
    legacy = _resolve_legacy_env_aliases()

    # Build the merged view (persisted overrides defaults; legacy env applies
    # only when the canonical key wasn't persisted nor set via canonical env;
    # env is handled by connection_pool at read-time, not here — CoreSettings
    # shouldn't know about env-var overrides).
    base = get_settings()
    merged: dict[str, Any] = base.model_dump()
    for key in RUNTIME_KEYS:
        if key in persisted:
            merged[key] = persisted[key]
        elif key in legacy:
            merged[key] = legacy[key]

    new_settings = CoreSettings(**merged)
    configure_settings(new_settings)

    # Take the boot snapshot *after* CoreSettings was updated so restart_required
    # is computed against the values the current process actually observes.
    snapshot = {k: int(getattr(new_settings, k)) for k in RUNTIME_KEYS}
    _boot_snapshot.clear()
    _boot_snapshot.update(snapshot)

    logger.info(
        "kg.kuzu.config_applied buffer_pool_mb=%d max_db_size_gb=%d pool_size=%d "
        "queue_workers=%d queue_min_interval_ms=%d queue_alert_threshold=%d",
        snapshot["kg_kuzu_buffer_pool_mb"],
        snapshot["kg_kuzu_max_db_size_gb"],
        snapshot["kg_connection_pool_size"],
        snapshot["kg_queue_max_concurrent_workers"],
        snapshot["kg_queue_min_interval_ms"],
        snapshot["kg_queue_alert_threshold"],
        extra={
            "event": "kg.kuzu.config_applied",
            **snapshot,
        },
    )
    return snapshot


async def get_runtime_settings(db: AsyncSession) -> dict[str, Any]:
    """Return the current effective values + restart_required flag.

    Effective = what CoreSettings is currently exposing (used by _open_kuzu_db
    and connection_pool at runtime).

    ``restart_required`` is True when a **Graph DB** key in the persisted
    table diverges from the boot snapshot — those are constructor-time for
    Kùzu and need a process restart to take effect. Event Queue keys are
    hot-reload (worker pool re-reads on every claim with 5s cache TTL) so
    persisting them never marks restart_required (spec bdcda842, BR8/TR11).
    """
    s = get_settings()
    effective = {k: int(getattr(s, k)) for k in RUNTIME_KEYS}

    persisted = await _load_persisted_rows(db)
    boot = _read_boot_snapshot()
    # Only Graph DB keys gate the restart banner — Event Queue keys hot-reload.
    restart_required = any(
        k in persisted and persisted[k] != boot.get(k) for k in GRAPH_DB_KEYS
    )

    return {**effective, "restart_required": restart_required}


def _maybe_reschedule_tick(values: dict[str, int]) -> None:
    """Spec 54399628 (Wave 2 NC f9732afc) — hot-reload tick interval.

    When `kg_decay_tick_interval_minutes` is in the persisted PUT body,
    update the live CoreSettings AND call APScheduler.reschedule_job so
    the cron job picks up the new interval without a server restart.

    Other tick keys (staleness, max_age_cap) take effect on the NEXT
    tick run via CoreSettings live-read in the handler — no scheduler
    intervention needed for them.

    Soft-fails when scheduler singleton is None (test contexts that
    skip lifespan) — settings are still persisted; live process just
    doesn't reschedule.
    """
    if "kg_decay_tick_interval_minutes" not in values:
        return

    new_interval = int(values["kg_decay_tick_interval_minutes"])

    # Update live CoreSettings so subsequent get_settings() returns the
    # new value (next tick handler invocation will read it).
    from okto_pulse.core.infra.config import configure_settings, get_settings
    current = get_settings()
    updated = current.model_copy(update={
        k: int(values[k]) for k in DECAY_TICK_KEYS if k in values
    })
    configure_settings(updated)

    # Hot-reload the APScheduler trigger.
    try:
        from apscheduler.triggers.interval import IntervalTrigger
        from okto_pulse.core.kg.scheduler_singleton import get_scheduler

        scheduler = get_scheduler()
        if scheduler is None:
            logger.info(
                "kg.tick.reschedule_skipped reason=no_scheduler "
                "new_interval_minutes=%d",
                new_interval,
                extra={
                    "event": "kg.tick.reschedule_skipped",
                    "reason": "no_scheduler",
                    "new_interval_minutes": new_interval,
                },
            )
            return
        scheduler.reschedule_job(
            "kg_daily_tick",
            trigger=IntervalTrigger(
                minutes=new_interval,
                timezone=timezone.utc,
            ),
        )
        logger.info(
            "kg.tick.rescheduled new_interval_minutes=%d",
            new_interval,
            extra={
                "event": "kg.tick.rescheduled",
                "new_interval_minutes": new_interval,
            },
        )
    except Exception as exc:
        # Reschedule is best-effort — failure shouldn't block PUT response.
        # Operator can restart server to apply on next boot.
        logger.warning(
            "kg.tick.reschedule_failed err=%s",
            exc,
            extra={"event": "kg.tick.reschedule_failed"},
        )


async def put_runtime_settings(
    db: AsyncSession, values: dict[str, int]
) -> dict[str, Any]:
    """Upsert runtime settings into the table. Caller validates ranges first.

    Returns the GET-style view (effective + restart_required). The lock
    serialises concurrent PUTs so the last-writer-wins semantic is deterministic.

    Spec 54399628 (Wave 2 NC f9732afc) — when `kg_decay_tick_interval_minutes`
    changes, also hot-reloads the APScheduler trigger so the new interval
    takes effect immediately (no restart_required for tick keys).
    """
    async with _write_lock:
        for key, value in values.items():
            if key not in RUNTIME_KEYS:
                continue
            row = await db.get(AppSetting, key)
            if row is None:
                db.add(AppSetting(key=key, value=str(int(value))))
            else:
                row.value = str(int(value))
        await db.commit()

    # Spec 54399628 — hot-reload tick interval after persistence commits.
    _maybe_reschedule_tick(values)

    # Re-read to compute restart_required consistently.
    return await get_runtime_settings(db)
