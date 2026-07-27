"""Ports and transport-neutral facts for bug cognitive closeout.

Core owns the contract and the fail-closed policy.  Editions own every read
needed to assemble the contract (relational state and canonical graph state).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)


@dataclass(frozen=True, slots=True)
class BugLinkedTestTask:
    """State of a test card explicitly linked to a bug."""

    card_id: str
    status: str | None
    card_type: str | None
    conclusions: tuple[Mapping[str, object], ...] = ()
    validations: tuple[Mapping[str, object], ...] = ()


@dataclass(frozen=True, slots=True)
class BugCognitiveContext:
    """Canonical input used by REST, MCP and the closeout worker.

    Collections deliberately contain source facts, not pre-computed booleans.
    The Core classifier remains the single owner of evidence semantics.
    ``canonical_bug_present=None`` means the graph could not be verified; it is
    never interpreted as absence or success.
    """

    board_id: str
    bug_id: str
    card_exists: bool
    card_type: str | None = None
    status: str | None = None
    title: str | None = None
    description: str | None = None
    expected_behavior: str | None = None
    observed_behavior: str | None = None
    steps_to_reproduce: str | None = None
    action_plan: str | None = None
    severity: str | None = None
    spec_id: str | None = None
    origin_task_id: str | None = None
    linked_test_task_ids: tuple[str, ...] = ()
    conclusions: tuple[Mapping[str, object], ...] = ()
    validations: tuple[Mapping[str, object], ...] = ()
    comments: tuple[Mapping[str, object], ...] = ()
    acceptance_criteria: tuple[object, ...] = ()
    test_scenarios: tuple[Mapping[str, object], ...] = ()
    linked_test_tasks: tuple[BugLinkedTestTask, ...] = ()
    lineage: tuple[Mapping[str, object], ...] = ()
    canonical_bug_present: bool | None = None
    provenance_refs: tuple[str, ...] = ()
    load_errors: tuple[str, ...] = ()
    contract_version: str = "bug-cognitive-context/v1"

    @property
    def eligible_for_closeout(self) -> bool:
        """Only a real, terminal bug is expected to have closeout work."""

        return (
            self.card_exists
            and (self.card_type or "").strip().lower() == "bug"
            and (self.status or "").strip().lower() == "done"
        )

    @property
    def verified(self) -> bool:
        return self.card_exists and not self.load_errors

    @classmethod
    def unavailable(
        cls,
        *,
        board_id: str,
        bug_id: str,
        error: str,
    ) -> "BugCognitiveContext":
        return cls(
            board_id=board_id,
            bug_id=bug_id,
            card_exists=False,
            load_errors=(error,),
        )


class BugCognitiveContextAssembler(Protocol):
    async def assemble(
        self,
        context: object,
        *,
        board_id: str,
        bug_id: str,
    ) -> BugCognitiveContext: ...


class CanonicalBugNodeReadPort(Protocol):
    async def exists(self, *, board_id: str, bug_id: str) -> bool: ...


_ASSEMBLER_KEY = "ports.bug_cognitive_context.assembler"
_CANONICAL_BUG_READER_KEY = "ports.bug_cognitive_context.canonical_bug_reader"


def register_bug_cognitive_context_assembler(
    assembler: BugCognitiveContextAssembler,
) -> None:
    register_runtime_value(_ASSEMBLER_KEY, assembler)


def resolve_bug_cognitive_context_assembler() -> BugCognitiveContextAssembler | None:
    value = resolve_runtime_value(_ASSEMBLER_KEY)
    return value  # type: ignore[return-value]


def register_canonical_bug_node_read_port(reader: CanonicalBugNodeReadPort) -> None:
    register_runtime_value(_CANONICAL_BUG_READER_KEY, reader)


def resolve_canonical_bug_node_read_port() -> CanonicalBugNodeReadPort | None:
    value = resolve_runtime_value(_CANONICAL_BUG_READER_KEY)
    return value  # type: ignore[return-value]


def reset_bug_cognitive_context_ports_for_tests() -> None:
    reset_runtime_values(_ASSEMBLER_KEY, _CANONICAL_BUG_READER_KEY)


def freeze_mapping_sequence(
    values: Sequence[Mapping[str, object]] | None,
) -> tuple[Mapping[str, object], ...]:
    """Small adapter helper that prevents callers mutating the outer sequence."""

    return tuple(dict(value) for value in (values or ()))


__all__ = [
    "BugCognitiveContext",
    "BugCognitiveContextAssembler",
    "BugLinkedTestTask",
    "CanonicalBugNodeReadPort",
    "freeze_mapping_sequence",
    "register_bug_cognitive_context_assembler",
    "register_canonical_bug_node_read_port",
    "reset_bug_cognitive_context_ports_for_tests",
    "resolve_bug_cognitive_context_assembler",
    "resolve_canonical_bug_node_read_port",
]
