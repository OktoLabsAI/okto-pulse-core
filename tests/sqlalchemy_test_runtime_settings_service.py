"""Runtime settings persistence for operator-tunable values (0.1.4).

Exposes the Grafx graph-runtime knobs (``kg_grafx_page_size``,
``kg_grafx_descriptor_revalidation``, ``kg_grafx_buffer_pool_mb``,
``kg_grafx_read_participants``, ``kg_grafx_options``) via a key-value
table in the main app SQLite DB,
read by the UI (``Settings`` menu) and by the backend at boot to override
:class:`CoreSettings` defaults.

The active graph runtime consumes these settings at construction time; values
only take effect on process restart. The endpoint layer communicates that via
a ``restart_required`` flag in the GET/PUT response.

Precedence at boot (documented in BR2 ``Precedencia de config``):
    env var (non-empty) > persisted settings table > CoreSettings default
"""

from __future__ import annotations

import json
import logging
import os
import warnings
from contextlib import asynccontextmanager
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.ports.relational_runtime import get_session_factory
from sqlalchemy_test_models import AppSetting
from okto_pulse.core.infra.config import (
    configure_settings,
    get_settings,
)
from okto_pulse.core.ports.coordination import (
    CoordinationProviderMissing,
    get_config_validation_port,
    get_runtime_settings_provider,
    get_write_lock_port,
)
from okto_pulse.core.ports.runtime_settings import (
    KG_TICK_RESCHEDULE_FAILED_SIGNAL,
    RuntimeEffectResult,
    build_reschedule_failed_signal,
)
from okto_pulse.core.ports.scheduler import KG_DAILY_TICK_JOB_ID, SchedulerControl
from okto_pulse.core.services.settings_service import ConfigChangeBlocked

# Grafx constructor-knob validators. Mirrored from the live Community
# adapter (``okto_pulse.community.config`` +
# ``okto_pulse.community.adapters.grafx_settings_catalog``) and reimplemented
# here so the Core test suite keeps NO executable Community import.
PULSE_GRAFX_MIN_PAGE_SIZE = 4096
PULSE_GRAFX_MAX_PAGE_SIZE = 32768


def validate_grafx_page_size(value: object) -> int:
    if type(value) is not int:
        raise ValueError("kg_grafx_page_size must be an integer")
    if not PULSE_GRAFX_MIN_PAGE_SIZE <= value <= PULSE_GRAFX_MAX_PAGE_SIZE:
        raise ValueError("kg_grafx_page_size must be between 4096 and 32768 bytes")
    if value & (value - 1):
        raise ValueError("kg_grafx_page_size must be a power of two")
    return value


def validate_grafx_descriptor_revalidation(value: object) -> str:
    if type(value) is not str or value not in {"strict", "generation"}:
        raise ValueError(
            "kg_grafx_descriptor_revalidation must be 'strict' or 'generation'"
        )
    return value


def validate_grafx_buffer_pool_mb(value: object) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("kg_grafx_buffer_pool_mb must be a positive integer")
    return value


def validate_grafx_read_participants(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 8:
        raise ValueError(
            "kg_grafx_read_participants must be an integer between 1 and 8"
        )
    return value


def validate_grafx_options(value: object) -> dict[str, Any]:
    """Shape-only stand-in for the Community catalog validator.

    The real validator rebuilds an ``okto_grafx`` ``DatabaseConfig`` to reject
    unsupported constructor keys; the Core test double only needs the mapping
    shape so the persistence/restart contract can be exercised.
    """
    if type(value) is not dict:
        raise ValueError("kg_grafx_options must be a mapping")
    return dict(value)

logger = logging.getLogger("okto_pulse.services.settings")

# Grafx constructor options require a full process restart: the active graph
# backend and its read lanes consume them at construction time. The frontend
# amber banner is triggered iff one of these diverges from the boot snapshot.
# Existing storage geometry is immutable, so there is no in-place migration
# group any more (the retired engine's max-db-size knob has no Grafx
# equivalent and was dropped with it).
GRAFX_GRAPH_DB_KEYS: tuple[str, ...] = (
    "kg_grafx_page_size",
    "kg_grafx_descriptor_revalidation",
    "kg_grafx_buffer_pool_mb",
    "kg_grafx_read_participants",
    "kg_grafx_options",
)
GRAPH_DB_KEYS: tuple[str, ...] = GRAFX_GRAPH_DB_KEYS

# Event Queue keys (spec bdcda842) — hot-reload, no restart required.
# The worker pool re-reads CoreSettings on every claim (5s cache TTL).
EVENT_QUEUE_KEYS: tuple[str, ...] = (
    "kg_queue_max_concurrent_workers",
    "kg_queue_min_interval_ms",
    "kg_queue_claim_timeout_s",
    "kg_queue_max_attempts",
    "kg_queue_alert_threshold",
)

# Decay Tick keys (spec 54399628 — Wave 2 NC f9732afc). Hot-reload via the
# SchedulerControl port — see _maybe_reschedule_tick below. Mudanças
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

# R-P2-06C — the general settings-effects contract. A settings change may trigger
# a RUNTIME EFFECT only through a port the composition (edition) INJECTS; the core
# common NEVER constructs a concrete effect provider (no implicit scheduler
# singleton — see R-P2-06B). This is the authoritative inventory of every
# settings -> runtime-effect mapping: the key is the persisted setting, the value
# is the ``RuntimeComposition`` provider key that carries the effect. Today the
# single mapped effect is the KG decay-tick reschedule
# (``kg_decay_tick_interval_minutes`` -> ``SchedulerControl.reschedule_job`` via
# the ``scheduler_control`` provider). GRAPH_DB_KEYS are guarded
# (restart-required) and DELIBERATELY trigger no local
# runtime effect — they are absent from this map (the absence-of-unintended-effect
# invariant). A new settings-effect MUST add an entry here AND flow through an
# injected port, never a core-constructed concrete.
SETTINGS_RUNTIME_EFFECT_PORTS: Final[dict[str, str]] = {
    "kg_decay_tick_interval_minutes": "scheduler_control",
}

# Legacy env var name → canonical settings key. Read once at boot in
# apply_persisted_settings_to_core_settings; emits DeprecationWarning if used.
# Removal scheduled for v0.5.0.
_LEGACY_ENV_ALIASES: tuple[tuple[str, str], ...] = (
    ("KG_MAX_QUEUE_DEPTH", "kg_queue_alert_threshold"),
)


# Snapshot of the values loaded at boot. Used to compute restart_required.
_boot_snapshot: dict[str, Any] = {}


def _validate_runtime_setting_value(key: str, value: Any) -> Any:
    """Validate persisted/runtime values that bypass the FastAPI schema."""
    if key == "kg_grafx_descriptor_revalidation":
        return validate_grafx_descriptor_revalidation(value)
    if key == "kg_grafx_options":
        return validate_grafx_options(
            json.loads(value) if isinstance(value, str) else value
        )
    if key == "kg_grafx_buffer_pool_mb":
        return validate_grafx_buffer_pool_mb(
            int(value) if isinstance(value, str) else value
        )
    if key == "kg_grafx_read_participants":
        return validate_grafx_read_participants(
            int(value) if isinstance(value, str) else value
        )
    if key == "kg_grafx_page_size":
        return validate_grafx_page_size(int(value) if isinstance(value, str) else value)

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer") from exc


def _read_boot_snapshot() -> dict[str, Any]:
    """Return a copy of the settings that were active at boot.

    If the app started before :func:`apply_persisted_settings_to_core_settings`
    ran (shouldn't happen in prod but tests often bypass boot), falls back to
    the live CoreSettings which is the best approximation.
    """
    if _boot_snapshot:
        return dict(_boot_snapshot)
    s = get_settings()
    return {k: _validate_runtime_setting_value(k, getattr(s, k)) for k in RUNTIME_KEYS}


async def _load_persisted_rows(db: AsyncSession) -> dict[str, Any]:
    """Load every row from ``app_settings`` and coerce values to int.

    Returns an empty dict on any error so a broken table never blocks boot.
    """
    try:
        result = await db.execute(
            select(AppSetting).where(AppSetting.key.in_(RUNTIME_KEYS))
        )
        rows = result.scalars().all()
        out: dict[str, Any] = {}
        for row in rows:
            try:
                out[row.key] = _validate_runtime_setting_value(row.key, row.value)
            except (TypeError, ValueError) as exc:
                logger.warning(
                    "settings.invalid_persisted_value key=%s value=%r err=%s",
                    row.key, row.value, exc,
                    extra={
                        "event": "settings.invalid_persisted_value",
                        "key": row.key,
                        "value": row.value,
                        "error": str(exc),
                    },
                )
        return out
    except Exception as exc:
        logger.warning("settings.load_failed err=%s", exc)
        return {}


async def _read_effective_runtime_settings() -> dict[str, Any]:
    """Read effective settings through the edition port when available."""

    s = get_settings()
    effective = {
        k: _validate_runtime_setting_value(k, getattr(s, k)) for k in RUNTIME_KEYS
    }
    try:
        provider = get_runtime_settings_provider()
    except CoordinationProviderMissing:
        return effective

    provided = await provider.read_runtime_settings("global")
    for key in RUNTIME_KEYS:
        if key in provided:
            effective[key] = _validate_runtime_setting_value(key, provided[key])
    return effective


def _validate_runtime_settings_via_port(values: dict[str, Any]) -> None:
    """Let the edition validate runtime settings when it registered a port."""

    try:
        validation_port = get_config_validation_port()
    except CoordinationProviderMissing:
        return
    validation_port.validate_runtime_settings(values)


@asynccontextmanager
async def _settings_write_guard():
    """Serialize runtime-settings writes through the registered write-lock port."""

    try:
        write_lock = get_write_lock_port()
    except CoordinationProviderMissing:
        yield
        return

    handle = await write_lock.acquire("_runtime", "settings")
    try:
        yield
    finally:
        await write_lock.release(handle)


def _resolve_legacy_env_aliases() -> dict[str, int]:
    """Resolve deprecated env vars into canonical settings keys.

    Spec bdcda842 (TR12): KG_MAX_QUEUE_DEPTH was the admission-gate threshold
    in v0.2.0; it is now an alerting-only threshold renamed to
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


async def apply_persisted_settings_to_core_settings() -> dict[str, Any]:
    """Read the ``app_settings`` table and override :class:`CoreSettings`.

    Called once at app startup **before** the graph runtime is built. Logs the
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

    new_settings = type(base)(**merged)
    configure_settings(new_settings)

    # Take the boot snapshot *after* CoreSettings was updated so restart_required
    # is computed against the values the current process actually observes.
    snapshot = {
        k: _validate_runtime_setting_value(k, getattr(new_settings, k))
        for k in RUNTIME_KEYS
    }
    _boot_snapshot.clear()
    _boot_snapshot.update(snapshot)

    logger.info(
        "kg.runtime.config_applied grafx_page_size=%d "
        "grafx_descriptor_revalidation=%s queue_workers=%d "
        "queue_min_interval_ms=%d queue_alert_threshold=%d",
        snapshot["kg_grafx_page_size"],
        snapshot["kg_grafx_descriptor_revalidation"],
        snapshot["kg_queue_max_concurrent_workers"],
        snapshot["kg_queue_min_interval_ms"],
        snapshot["kg_queue_alert_threshold"],
        extra={
            "event": "kg.runtime.config_applied",
            **snapshot,
        },
    )
    return snapshot


async def get_runtime_settings(db: AsyncSession) -> dict[str, Any]:
    """Return effective and persisted desired values + restart flag.

    Effective = what CoreSettings is currently exposing to the graph runtime
    and connection_pool.

    ``desired_values`` overlays persisted values on the effective snapshot so
    the test adapter matches the Community production adapter while a
    constructor-time change is waiting for restart.

    ``restart_required`` is True when a graph-runtime key in the persisted
    table diverges from the boot snapshot; those are constructor-time for the
    active backend/pool and need a process restart to take effect. Event Queue keys are
    hot-reload (worker pool re-reads on every claim with 5s cache TTL) so
    persisting them never marks restart_required (spec bdcda842, BR8/TR11).
    """
    effective = await _read_effective_runtime_settings()
    settings = get_settings()
    active_providers = {
        "kg_graph_backend": str(getattr(settings, "kg_graph_backend", "grafx")),
        "kg_global_graph_backend": str(
            getattr(settings, "kg_global_graph_backend", "grafx")
        ),
    }

    persisted = await _load_persisted_rows(db)
    boot = _read_boot_snapshot()
    # Only graph-runtime keys gate the restart banner; Event Queue keys hot-reload.
    restart_required = any(
        k in persisted and persisted[k] != boot.get(k) for k in GRAPH_DB_KEYS
    )
    desired = dict(effective)
    desired.update(persisted)

    return {
        **active_providers,
        **effective,
        "desired_values": desired,
        "restart_required": restart_required,
    }


def _apply_live_tick_settings(values: dict[str, Any]) -> None:
    """Persist the decay-tick knobs into live CoreSettings — NO scheduler.

    Behaviour-preserving split of the former ``_maybe_reschedule_tick``
    (spec #15 fr_93c9af44/fr_2ae7de62). This is the PERSISTENCE half: it
    updates live ``CoreSettings`` so a subsequent ``get_settings()`` returns
    the new value, and is gated on the interval being in the change set
    exactly as before. It NEVER touches the scheduler singleton, so
    ``RuntimeSettingsPort.persist`` can run with no scheduler wired
    (AC ac_a4d41673).
    """
    if "kg_decay_tick_interval_minutes" not in values:
        return
    current = get_settings()
    updated = current.model_copy(
        update={k: int(values[k]) for k in DECAY_TICK_KEYS if k in values}
    )
    configure_settings(updated)


async def apply_tick_runtime_effects(
    values: dict[str, Any],
    scheduler_control: SchedulerControl | None,
    *,
    actor_id: str = "unknown",
    source: str = "runtime_settings.put",
) -> list[RuntimeEffectResult]:
    """Apply the runtime EFFECT of a tick-interval change via the port.

    Behaviour-preserving split of ``_maybe_reschedule_tick`` (spec #15
    fr_2ae7de62): the scheduler reschedule flows through the injected
    ``SchedulerControl`` port — there is NO direct legacy scheduler state
    access here. Soft-fails (never raises) so it cannot break the PUT
    response, exactly as before:

    - no scheduler wired -> ``kg.tick.reschedule_skipped`` (AC ac_a4d41673 path);
    - success -> ``kg.tick.rescheduled`` stays auditable (AC ac_c9b328fb);
    - failure -> emits/enriches ``kg.tick.reschedule_failed`` with
      ``error_class`` and a sanitized message, never a secret (AC ac_b74a281f,
      fr_70c29790).

    Returns one ``RuntimeEffectResult`` per effect for the conformance suite.
    """
    if "kg_decay_tick_interval_minutes" not in values:
        return []
    new_interval = int(values["kg_decay_tick_interval_minutes"])

    # R-P2-06B: a ``None`` port means the composition injected no
    # ``SchedulerControl`` — an EXPLICIT skip (the core never falls back to the
    # process-global singleton). Same public outcome as a wired-but-unavailable
    # scheduler, with a distinct ``no_provider`` reason for observability.
    if scheduler_control is None or not scheduler_control.is_available():
        reason = "no_provider" if scheduler_control is None else "no_scheduler"
        logger.info(
            "kg.tick.reschedule_skipped reason=%s new_interval_minutes=%d",
            reason,
            new_interval,
            extra={
                "event": "kg.tick.reschedule_skipped",
                "reason": reason,
                "new_interval_minutes": new_interval,
            },
        )
        return [
            RuntimeEffectResult(
                effect="kg_tick_reschedule",
                status="skipped",
                job_id=KG_DAILY_TICK_JOB_ID,
                audit_status="skipped",
            )
        ]

    try:
        result = await scheduler_control.reschedule_job(
            KG_DAILY_TICK_JOB_ID, {"minutes": new_interval}
        )
    except Exception as exc:
        # Reschedule is best-effort — failure must not block the PUT response.
        signal = build_reschedule_failed_signal(
            error=exc, actor_id=actor_id, source=source
        )
        # Log the SANITIZED message — never the raw exception (it may carry a
        # secret/token in its text). The structured `extra` is secret-free too.
        logger.warning(
            "kg.tick.reschedule_failed err=%s",
            signal["sanitized_message"],
            extra={"event": KG_TICK_RESCHEDULE_FAILED_SIGNAL, **signal},
        )
        return [
            RuntimeEffectResult(
                effect="kg_tick_reschedule",
                status="failed",
                job_id=KG_DAILY_TICK_JOB_ID,
                signal=KG_TICK_RESCHEDULE_FAILED_SIGNAL,
                audit_status="failed",
                error_class=type(exc).__name__,
                sanitized_message=signal["sanitized_message"],
            )
        ]

    # Respect the port's own outcome: a SchedulerControl that reports the job was
    # not actually scheduled (scheduled=False / audit_status='skipped') is a
    # skipped effect, NOT an applied one.
    if not result.scheduled or result.audit_status == "skipped":
        logger.info(
            "kg.tick.reschedule_skipped reason=port_skipped new_interval_minutes=%d",
            new_interval,
            extra={
                "event": "kg.tick.reschedule_skipped",
                "reason": "port_skipped",
                "new_interval_minutes": new_interval,
            },
        )
        return [
            RuntimeEffectResult(
                effect="kg_tick_reschedule",
                status="skipped",
                job_id=KG_DAILY_TICK_JOB_ID,
                audit_status="skipped",
            )
        ]

    logger.info(
        "kg.tick.rescheduled new_interval_minutes=%d",
        new_interval,
        extra={
            "event": "kg.tick.rescheduled",
            "new_interval_minutes": new_interval,
        },
    )
    return [
        RuntimeEffectResult(
            effect="kg_tick_reschedule",
            status="applied",
            job_id=KG_DAILY_TICK_JOB_ID,
            audit_status=result.audit_status,
        )
    ]


class _LegacyConfigChangeBlocked(Exception):
    """Raised when KG-01.5 KGConfigChangeGuard rejects a runtime change.

    Carries the bounded ``reason`` code (from ``ConfigBlockReason``) and
    the ``setting_group`` bucket so the API layer can return a safe
    HTTP response without leaking raw values.
    """

    def __init__(
        self,
        *,
        reason: str,
        setting_group: str,
        audit_event: str,
    ) -> None:
        super().__init__(f"{reason} (setting_group={setting_group})")
        self.reason = reason
        self.setting_group = setting_group
        self.audit_event = audit_event


async def put_runtime_settings(
    db: AsyncSession,
    values: dict[str, Any],
    *,
    actor_id: str = "unknown",
    migration_plan_ref: str | None = None,
    restart_policy: str | None = None,
    scheduler_control: SchedulerControl | None = None,
) -> dict[str, Any]:
    """Persist validated options and report differences from the boot snapshot.

    Grafx constructor options require restart, not in-place storage migration,
    so there is no ``KGConfigChangeGuard`` hop any more: the retired engine's
    storage-grow/shrink and migration-plan groups have no Grafx equivalent.
    ``migration_plan_ref`` / ``restart_policy`` stay in the signature because
    the REST payload still accepts them for wire compatibility.

    Queue and decay settings retain their engine-neutral hot-reload effects
    through injected Core ports.

    Returns the GET-style view (effective + restart_required). The lock
    serialises concurrent PUTs so the last-writer-wins semantic is
    deterministic.
    """
    parsed_values: dict[str, Any] = {}
    for key, value in values.items():
        if key not in RUNTIME_KEYS:
            continue
        parsed_values[key] = _validate_runtime_setting_value(key, value)

    if parsed_values:
        _validate_runtime_settings_via_port(parsed_values)

    async with _settings_write_guard():
        # Validate the desired combination, not only the current boot snapshot.
        # This also serializes two partial PUTs against a previous pending edit.
        if any(key in parsed_values for key in GRAFX_GRAPH_DB_KEYS):
            desired = dict(_read_boot_snapshot())
            desired.update(await _load_persisted_rows(db))
            desired.update(parsed_values)
            validate_grafx_options(desired["kg_grafx_options"])
        for key, parsed in parsed_values.items():
            serialized = (
                json.dumps(parsed, allow_nan=False)
                if key == "kg_grafx_options"
                else str(parsed)
            )
            row = await db.get(AppSetting, key)
            if row is None:
                db.add(AppSetting(key=key, value=serialized))
            else:
                row.value = serialized
        await db.commit()

    # Spec 54399628 / spec #15 (fr_93c9af44, fr_2ae7de62) — hot-reload tick
    # interval after persistence commits, split into persistence (live
    # CoreSettings) and an explicit runtime effect through the SchedulerControl
    # port. R08: the core NO LONGER constructs an implicit scheduler-control
    # adapter fallback — ``scheduler_control`` is the port the composition injected
    # (the API resolves it from
    # ``RuntimeComposition.scheduler_control``). A ``None`` port is an explicit
    # skip handled in ``apply_tick_runtime_effects``; the core never reaches the
    # process-global scheduler singleton.
    _apply_live_tick_settings(values)
    await apply_tick_runtime_effects(values, scheduler_control, actor_id=actor_id)

    # Re-read to compute restart_required consistently.
    return await get_runtime_settings(db)
