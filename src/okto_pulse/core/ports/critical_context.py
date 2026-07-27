"""Read boundary for critical workflow full-context snapshots."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from typing import Any, Protocol


class CriticalContextReadPort(Protocol):
    async def resolve_full_context(
        self,
        context: Any,
        *,
        board_id: str,
        entity_type: str,
        entity_id: str,
        critical_action: str,
    ) -> Any: ...


_RUNTIME_KEY = "ports.critical_context.reader"


def register_critical_context_read_port(reader: CriticalContextReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_critical_context_read_port() -> CriticalContextReadPort:
    return require_runtime_value(_RUNTIME_KEY, "critical_context_read_port_not_configured")


def reset_critical_context_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "CriticalContextReadPort",
    "get_critical_context_read_port",
    "register_critical_context_read_port",
    "reset_critical_context_read_port_for_tests",
]
