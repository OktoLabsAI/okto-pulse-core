"""Read port for persisted skip-override audit facts."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class AmbiguitySkipAuditFact:
    actor_id: str | None
    created_at: datetime | None
    source: str | None


class SkipOverrideReadPort(Protocol):
    async def latest_enabled_ambiguity_skip(
        self,
        context: object,
        *,
        board_id: str,
        ideation_id: str,
        action: str,
    ) -> AmbiguitySkipAuditFact | None: ...


_RUNTIME_KEY = "ports.skip_overrides.reader"


def register_skip_override_read_port(reader: SkipOverrideReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_skip_override_read_port() -> SkipOverrideReadPort:
    return require_runtime_value(_RUNTIME_KEY, "skip_override_read_port_not_configured")


def reset_skip_override_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "AmbiguitySkipAuditFact",
    "SkipOverrideReadPort",
    "get_skip_override_read_port",
    "register_skip_override_read_port",
    "reset_skip_override_read_port_for_tests",
]
