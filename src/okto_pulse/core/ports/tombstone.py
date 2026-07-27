"""Permanent governed-deletion tombstone boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


@dataclass(frozen=True, slots=True)
class DeletionTombstoneAdvance:
    board_id: str
    artifact_type: str
    artifact_id: str
    delete_event_id: str


@dataclass(frozen=True, slots=True)
class DeletionTombstoneReceipt:
    generation: int
    delete_event_id: str


class TombstonePort(Protocol):
    async def advance_deletion_tombstone(
        self,
        context: Any,
        request: DeletionTombstoneAdvance,
    ) -> DeletionTombstoneReceipt:
        """Create or advance a tombstone without committing the caller's UoW."""
        ...


_RUNTIME_KEY = "ports.tombstone.port"


def register_tombstone_port(port: TombstonePort) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_tombstone_port() -> TombstonePort:
    return require_runtime_value(_RUNTIME_KEY, "tombstone_port_not_configured")


def reset_tombstone_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "DeletionTombstoneAdvance",
    "DeletionTombstoneReceipt",
    "TombstonePort",
    "get_tombstone_port",
    "register_tombstone_port",
    "reset_tombstone_port_for_tests",
]
