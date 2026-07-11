"""Edition-neutral board realm authorization boundary."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from typing import Any, Literal, Protocol

from okto_pulse.core.domain.realm import RealmScope

RealmOperation = Literal["read", "write", "event", "outbox", "worker", "kg"]


class RealmAccessPort(Protocol):
    async def require_board_access(
        self,
        context: Any,
        *,
        scope: RealmScope,
        board_id: str,
        operation: RealmOperation,
    ) -> None: ...


_RUNTIME_KEY = "ports.realm_access.port"


def register_realm_access_port(port: RealmAccessPort) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_realm_access_port() -> RealmAccessPort:
    return require_runtime_value(_RUNTIME_KEY, "realm_access_port_not_configured")


def reset_realm_access_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "RealmAccessPort",
    "RealmOperation",
    "get_realm_access_port",
    "register_realm_access_port",
    "reset_realm_access_port_for_tests",
]
