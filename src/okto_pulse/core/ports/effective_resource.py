"""Edition boundary for effective resource persistence operations."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from typing import Any, Protocol


class EffectiveResourcePersistencePort(Protocol):
    async def load_knowledge_bases(
        self,
        context: Any,
        *,
        source_entity_type: str,
        source_entity_id: str,
    ) -> list[dict[str, Any]]: ...

    async def load_mockups(
        self,
        context: Any,
        *,
        source_entity_type: str,
        source_entity_id: str,
    ) -> list[dict[str, Any]]: ...


_RUNTIME_KEY = "ports.effective_resource.port"


def register_effective_resource_persistence_port(
    port: EffectiveResourcePersistencePort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_effective_resource_persistence_port() -> EffectiveResourcePersistencePort:
    return require_runtime_value(_RUNTIME_KEY, "effective_resource_persistence_port_not_configured")


def reset_effective_resource_persistence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "EffectiveResourcePersistencePort",
    "get_effective_resource_persistence_port",
    "register_effective_resource_persistence_port",
    "reset_effective_resource_persistence_port_for_tests",
]
