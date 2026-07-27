"""Edition boundary for destructive board-scoped relational cleanup."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from typing import Protocol


class BoardRelationalCleanupPort(Protocol):
    async def wipe_runtime_rows(self, *, board_id: str) -> dict[str, int]: ...


_RUNTIME_KEY = "ports.board_relational_cleanup.port"


def register_board_relational_cleanup_port(port: BoardRelationalCleanupPort) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_board_relational_cleanup_port() -> BoardRelationalCleanupPort:
    return require_runtime_value(_RUNTIME_KEY, "board_relational_cleanup_port_not_configured")


def reset_board_relational_cleanup_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "BoardRelationalCleanupPort",
    "get_board_relational_cleanup_port",
    "register_board_relational_cleanup_port",
    "reset_board_relational_cleanup_port_for_tests",
]
