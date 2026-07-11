"""Read boundary for legacy architecture propagation snapshots."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class ArchitectureLegacySnapshot:
    id: str
    parent_type: str
    parent_id: str
    source_design_id: str
    source_ref: str | None
    source_version: int | None


@dataclass(frozen=True, slots=True)
class ArchitectureLegacySnapshotPage:
    total: int
    items: tuple[ArchitectureLegacySnapshot, ...]


class ArchitectureLegacySnapshotReadPort(Protocol):
    async def list_page(
        self,
        context: Any,
        *,
        board_id: str,
        parent_type_filter: str | None,
        limit: int,
        offset: int,
    ) -> ArchitectureLegacySnapshotPage: ...


_RUNTIME_KEY = "ports.architecture_legacy.reader"


def register_architecture_legacy_snapshot_read_port(
    reader: ArchitectureLegacySnapshotReadPort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_architecture_legacy_snapshot_read_port() -> ArchitectureLegacySnapshotReadPort:
    return require_runtime_value(_RUNTIME_KEY, "architecture_legacy_snapshot_read_port_not_configured")


def reset_architecture_legacy_snapshot_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ArchitectureLegacySnapshot",
    "ArchitectureLegacySnapshotPage",
    "ArchitectureLegacySnapshotReadPort",
    "get_architecture_legacy_snapshot_read_port",
    "register_architecture_legacy_snapshot_read_port",
    "reset_architecture_legacy_snapshot_read_port_for_tests",
]
