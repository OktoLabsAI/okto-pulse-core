"""Edition seam for durable Code Traceability event projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)


@dataclass(frozen=True, slots=True)
class CodeTraceabilityEventEffectPlan:
    """Bounded traceability-owned effects derived only from an event type.

    Spec Validation is lifecycle-owned.  Technical traceability events may
    refresh their own projections and activity, but they have no authority to
    clear or supersede the current human validation result.
    """

    invalidate_read_models: bool = True
    record_activity: bool = True


def code_traceability_event_effect_plan(
    event_type: str,
) -> CodeTraceabilityEventEffectPlan:
    from okto_pulse.core.events.types import CODE_TRACEABILITY_EVENT_TYPES

    if event_type not in CODE_TRACEABILITY_EVENT_TYPES:
        raise ValueError("code_traceability_event_type_invalid")
    return CodeTraceabilityEventEffectPlan()


@runtime_checkable
class CodeTraceabilityEventEffectsPort(Protocol):
    """Apply edition-owned read/audit effects in the handler-owned UoW."""

    async def apply(self, session: object, event: object) -> None: ...


class CodeTraceabilityEventEffectsProviderMissing(RuntimeError):
    code = "code_traceability_event_effects_provider_missing"

    def __init__(self) -> None:
        super().__init__(
            "code_traceability_event_effects_provider_missing: required "
            "edition adapter not supplied"
        )


_RUNTIME_KEY = "ports.code_traceability_event_effects.port"


def register_code_traceability_event_effects_port(
    port: CodeTraceabilityEventEffectsPort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_code_traceability_event_effects_port() -> CodeTraceabilityEventEffectsPort:
    port = resolve_runtime_value(_RUNTIME_KEY)
    if port is None:
        raise CodeTraceabilityEventEffectsProviderMissing()
    return port


def reset_code_traceability_event_effects_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "CodeTraceabilityEventEffectPlan",
    "CodeTraceabilityEventEffectsPort",
    "CodeTraceabilityEventEffectsProviderMissing",
    "code_traceability_event_effect_plan",
    "get_code_traceability_event_effects_port",
    "register_code_traceability_event_effects_port",
    "reset_code_traceability_event_effects_port_for_tests",
]
