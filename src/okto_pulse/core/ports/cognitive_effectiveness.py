"""Read model for cognitive-effectiveness source facts."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class CognitiveDlqFact:
    dead_letter_id: str
    artifact_type: str
    artifact_id: str


@dataclass(frozen=True, slots=True)
class CognitiveDoneCardFact:
    card_id: str
    card_type: str
    action_plan: str | None


@dataclass(frozen=True, slots=True)
class CognitiveDoneSpecFact:
    spec_id: str


@dataclass(frozen=True, slots=True)
class CognitiveEffectivenessSources:
    dead_letters: tuple[CognitiveDlqFact, ...]
    done_cards: tuple[CognitiveDoneCardFact, ...]
    done_specs: tuple[CognitiveDoneSpecFact, ...]


class CognitiveEffectivenessReadPort(Protocol):
    async def load_sources(
        self, context: object, *, board_id: str
    ) -> CognitiveEffectivenessSources: ...


_RUNTIME_KEY = "ports.cognitive_effectiveness.reader"


def register_cognitive_effectiveness_read_port(
    reader: CognitiveEffectivenessReadPort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_cognitive_effectiveness_read_port() -> CognitiveEffectivenessReadPort:
    return require_runtime_value(_RUNTIME_KEY, "cognitive_effectiveness_read_port_not_configured")


def reset_cognitive_effectiveness_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "CognitiveDlqFact",
    "CognitiveDoneCardFact",
    "CognitiveDoneSpecFact",
    "CognitiveEffectivenessReadPort",
    "CognitiveEffectivenessSources",
    "get_cognitive_effectiveness_read_port",
    "register_cognitive_effectiveness_read_port",
    "reset_cognitive_effectiveness_read_port_for_tests",
]
