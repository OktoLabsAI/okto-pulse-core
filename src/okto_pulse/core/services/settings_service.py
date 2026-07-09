"""Runtime settings service facade.

Persistence is implemented by the SQLAlchemy runtime-settings adapter. The
stable service import path remains available for REST, boot and tests while this
module no longer owns relational definitions or queries.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.repositories import (
    DECAY_TICK_KEYS,
    EVENT_QUEUE_KEYS,
    GRAPH_DB_KEYS,
    RUNTIME_KEYS,
    SETTINGS_RUNTIME_EFFECT_PORTS,
    AppSetting,
    ConfigChangeBlocked,
    _apply_live_tick_settings as _adapter_apply_live_tick_settings,
    _resolve_legacy_env_aliases,
    apply_persisted_settings_to_core_settings as _adapter_apply_persisted_settings_to_core_settings,
    apply_tick_runtime_effects as _adapter_apply_tick_runtime_effects,
    get_runtime_settings as _adapter_get_runtime_settings,
    put_runtime_settings as _adapter_put_runtime_settings,
)


async def apply_persisted_settings_to_core_settings() -> dict[str, int]:
    return await _adapter_apply_persisted_settings_to_core_settings()


async def get_runtime_settings(settings_port) -> dict[str, Any]:
    return await _adapter_get_runtime_settings(settings_port)


def _apply_live_tick_settings(values: dict[str, int]) -> None:
    return _adapter_apply_live_tick_settings(values)


async def apply_tick_runtime_effects(
    values: dict[str, int],
    scheduler_control,
    *,
    actor_id: str = "unknown",
    source: str = "runtime_settings.put",
):
    return await _adapter_apply_tick_runtime_effects(
        values,
        scheduler_control,
        actor_id=actor_id,
        source=source,
    )


async def put_runtime_settings(
    settings_port,
    values: dict[str, int],
    *,
    actor_id: str = "unknown",
    migration_plan_ref: str | None = None,
    restart_policy: str | None = None,
    scheduler_control=None,
) -> dict[str, Any]:
    return await _adapter_put_runtime_settings(
        settings_port,
        values,
        actor_id=actor_id,
        migration_plan_ref=migration_plan_ref,
        restart_policy=restart_policy,
        scheduler_control=scheduler_control,
    )


__all__ = [
    "DECAY_TICK_KEYS",
    "EVENT_QUEUE_KEYS",
    "GRAPH_DB_KEYS",
    "RUNTIME_KEYS",
    "SETTINGS_RUNTIME_EFFECT_PORTS",
    "AppSetting",
    "ConfigChangeBlocked",
    "_apply_live_tick_settings",
    "_resolve_legacy_env_aliases",
    "apply_persisted_settings_to_core_settings",
    "apply_tick_runtime_effects",
    "get_runtime_settings",
    "put_runtime_settings",
]
