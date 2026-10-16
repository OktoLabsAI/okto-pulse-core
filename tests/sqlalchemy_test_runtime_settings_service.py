"""Read and validate persisted deployment configuration at startup.

The edition applies the existing startup merge before constructing graph and
worker providers. There is no tuning writer or scheduler effect in this module.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.ports.relational_runtime import get_session_factory
from sqlalchemy_test_models import AppSetting
from okto_pulse.core.infra.config import (
    configure_settings,
    get_settings,
)

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

# Decay cadence applied when the scheduler is composed at startup.
DECAY_TICK_KEYS: tuple[str, ...] = (
    "kg_decay_tick_interval_minutes",
    "kg_decay_tick_staleness_days",
    "kg_decay_tick_max_age_days",
)

# Allowlisted deployment configuration read at startup; no public writer.
RUNTIME_KEYS: tuple[str, ...] = GRAPH_DB_KEYS + EVENT_QUEUE_KEYS + DECAY_TICK_KEYS

def _validate_runtime_setting_value(key: str, value: Any) -> Any:
    """Validate persisted deployment values before startup hydration."""
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


async def apply_persisted_settings_to_core_settings() -> dict[str, Any]:
    """Read the ``app_settings`` table and override :class:`CoreSettings`.

    Called once at app startup **before** the graph runtime is built. Logs the
    resolved values in a single structured line for audit.
    Returns the snapshot that was applied (for caller bookkeeping).
    """
    factory = get_session_factory()
    async with factory() as db:
        persisted = await _load_persisted_rows(db)

    # Persisted native deployment values override the configured defaults.
    base = get_settings()
    merged: dict[str, Any] = base.model_dump()
    for key in RUNTIME_KEYS:
        if key in persisted:
            merged[key] = persisted[key]

    new_settings = type(base)(**merged)
    configure_settings(new_settings)

    # Return the applied startup snapshot to the composition root.
    snapshot = {
        k: _validate_runtime_setting_value(k, getattr(new_settings, k))
        for k in RUNTIME_KEYS
    }

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
