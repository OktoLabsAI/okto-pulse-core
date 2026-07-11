"""Runtime-settings application facade.

The setting vocabulary and application error are Core contracts. Persistence
and local runtime effects are supplied by an edition-owned adapter.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.runtime_settings import (
    DECAY_TICK_KEYS,
    EVENT_QUEUE_KEYS,
    GRAPH_DB_KEYS,
    RUNTIME_KEYS,
    SETTINGS_RUNTIME_EFFECT_PORTS,
    ConfigChangeBlocked,
)

from okto_pulse.core.ports.relational_services import (
    resolve_runtime_settings_adapter,
)

async def apply_persisted_settings_to_core_settings() -> dict[str, int]:
    return await resolve_runtime_settings_adapter().apply_persisted_settings_to_core_settings()


async def get_runtime_settings(settings_port: Any) -> dict[str, Any]:
    return await resolve_runtime_settings_adapter().get_runtime_settings(settings_port)


def _apply_live_tick_settings(values: dict[str, int]) -> None:
    resolve_runtime_settings_adapter()._apply_live_tick_settings(values)


def _resolve_legacy_env_aliases() -> dict[str, int]:
    return resolve_runtime_settings_adapter()._resolve_legacy_env_aliases()


async def apply_tick_runtime_effects(
    values: dict[str, int],
    scheduler_control: Any,
    *,
    actor_id: str = "unknown",
    source: str = "runtime_settings.put",
):
    return await resolve_runtime_settings_adapter().apply_tick_runtime_effects(
        values,
        scheduler_control,
        actor_id=actor_id,
        source=source,
    )


async def put_runtime_settings(
    settings_port: Any,
    values: dict[str, int],
    *,
    actor_id: str = "unknown",
    migration_plan_ref: str | None = None,
    restart_policy: str | None = None,
    scheduler_control: Any = None,
) -> dict[str, Any]:
    return await resolve_runtime_settings_adapter().put_runtime_settings(
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
    "ConfigChangeBlocked",
    "_apply_live_tick_settings",
    "_resolve_legacy_env_aliases",
    "apply_persisted_settings_to_core_settings",
    "apply_tick_runtime_effects",
    "get_runtime_settings",
    "put_runtime_settings",
]
