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
from typing import Any

from sqlalchemy import String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from okto_pulse.core.infra.database import Base, get_session_factory
from okto_pulse.core.infra.config import CoreSettings, configure_settings, get_settings

logger = logging.getLogger("okto_pulse.services.settings")

# Keys persisted in the `app_settings` table. Adding a new key here is enough
# to expose it via the REST endpoint; update the RuntimeSettingsPayload to
# include a validator.
RUNTIME_KEYS: tuple[str, ...] = (
    "kg_kuzu_buffer_pool_mb",
    "kg_kuzu_max_db_size_gb",
    "kg_connection_pool_size",
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


async def apply_persisted_settings_to_core_settings() -> dict[str, int]:
    """Read the ``app_settings`` table and override :class:`CoreSettings`.

    Called once at app startup **before** any module imports Kùzu. Logs the
    resolved values in a single structured line for audit.
    Returns the snapshot that was applied (for caller bookkeeping).
    """
    factory = get_session_factory()
    async with factory() as db:
        persisted = await _load_persisted_rows(db)

    # Build the merged view (persisted overrides defaults; env is handled by
    # connection_pool at read-time, not here — CoreSettings shouldn't know
    # about env-var overrides).
    base = get_settings()
    merged: dict[str, Any] = base.model_dump()
    for key in RUNTIME_KEYS:
        if key in persisted:
            merged[key] = persisted[key]

    new_settings = CoreSettings(**merged)
    configure_settings(new_settings)

    # Take the boot snapshot *after* CoreSettings was updated so restart_required
    # is computed against the values the current process actually observes.
    snapshot = {k: int(getattr(new_settings, k)) for k in RUNTIME_KEYS}
    _boot_snapshot.clear()
    _boot_snapshot.update(snapshot)

    logger.info(
        "kg.kuzu.config_applied buffer_pool_mb=%d max_db_size_gb=%d pool_size=%d",
        snapshot["kg_kuzu_buffer_pool_mb"],
        snapshot["kg_kuzu_max_db_size_gb"],
        snapshot["kg_connection_pool_size"],
        extra={
            "event": "kg.kuzu.config_applied",
            **snapshot,
        },
    )
    return snapshot


async def get_runtime_settings(db: AsyncSession) -> dict[str, Any]:
    """Return the current effective values + restart_required flag.

    Effective = what CoreSettings is currently exposing (used by _open_kuzu_db
    and connection_pool at runtime). restart_required is True when the
    persisted table diverges from the boot snapshot (i.e. someone saved new
    values since startup and Kùzu has not been re-initialised).
    """
    s = get_settings()
    effective = {k: int(getattr(s, k)) for k in RUNTIME_KEYS}

    persisted = await _load_persisted_rows(db)
    boot = _read_boot_snapshot()
    # restart_required if persisted differs from what the process booted with,
    # even if CoreSettings was not re-read yet.
    restart_required = any(
        k in persisted and persisted[k] != boot.get(k) for k in RUNTIME_KEYS
    )

    return {**effective, "restart_required": restart_required}


async def put_runtime_settings(
    db: AsyncSession, values: dict[str, int]
) -> dict[str, Any]:
    """Upsert runtime settings into the table. Caller validates ranges first.

    Returns the GET-style view (effective + restart_required). The lock
    serialises concurrent PUTs so the last-writer-wins semantic is deterministic.
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

    # Re-read to compute restart_required consistently.
    return await get_runtime_settings(db)
