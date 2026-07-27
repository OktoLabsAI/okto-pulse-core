"""Persistence-neutral parent artifact lookup boundary."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class ParentArtifactRecord:
    artifact_type: str
    id: str
    title: str
    status: str


class ParentArtifactReadPort(Protocol):
    async def read_many(
        self,
        context: Any,
        *,
        artifact_type: str,
        ids: frozenset[str],
    ) -> tuple[ParentArtifactRecord, ...]: ...


_RUNTIME_KEY = "ports.parent_artifact.reader"


def register_parent_artifact_read_port(reader: ParentArtifactReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_parent_artifact_read_port() -> ParentArtifactReadPort:
    return require_runtime_value(_RUNTIME_KEY, "parent_artifact_read_port_not_configured")


def reset_parent_artifact_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ParentArtifactReadPort",
    "ParentArtifactRecord",
    "get_parent_artifact_read_port",
    "register_parent_artifact_read_port",
    "reset_parent_artifact_read_port_for_tests",
]
