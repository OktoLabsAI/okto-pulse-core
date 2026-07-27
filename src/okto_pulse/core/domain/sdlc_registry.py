"""Canonical SDLC lifecycle registry.

This module is deliberately a domain leaf: transports, services, resources and
frontends project lifecycle metadata from here instead of maintaining their own
transition tables.  A transition carries both the edge and its stable contract
metadata, which makes UI affordances and automation explanations auditable.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping, Sequence

from okto_pulse.core.domain.enums import (
    CardStatus,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintStatus,
    StoryStatus,
)


@dataclass(frozen=True, slots=True)
class TransitionContract:
    """One executable lifecycle edge and its public explanation contract."""

    to_status: str
    label: str
    gate: str = "none"
    preconditions: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()
    effects: tuple[str, ...] = ("status_changed", "activity_logged")
    reason_codes: tuple[str, ...] = ("transition_not_allowed",)
    card_types: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class LifecycleDefinition:
    entity_type: str
    status_enum: type[Enum]
    transitions: Mapping[str, tuple[TransitionContract, ...]]


def _label(status: str) -> str:
    return status.replace("_", " ").title()


def _edge(
    to_status: str,
    *,
    gate: str = "none",
    preconditions: Sequence[str] = (),
    capabilities: Sequence[str] = (),
    effects: Sequence[str] = ("status_changed", "activity_logged"),
    reason_codes: Sequence[str] = ("transition_not_allowed",),
    card_types: Sequence[str] = (),
) -> TransitionContract:
    return TransitionContract(
        to_status=to_status,
        label=_label(to_status),
        gate=gate,
        preconditions=tuple(preconditions),
        capabilities=tuple(capabilities),
        effects=tuple(effects),
        reason_codes=tuple(reason_codes),
        card_types=tuple(card_types),
    )


def _entity(
    entity_type: str,
    status_enum: type[Enum],
    transitions: Mapping[str, Sequence[TransitionContract]],
) -> LifecycleDefinition:
    return LifecycleDefinition(
        entity_type=entity_type,
        status_enum=status_enum,
        transitions=MappingProxyType(
            {status: tuple(edges) for status, edges in transitions.items()}
        ),
    )


_CANCEL = dict(
    gate="cancellation_reason",
    preconditions=("non_empty_cancellation_reason",),
    capabilities=("cancel",),
    effects=("status_changed", "cancellation_recorded", "activity_logged"),
    reason_codes=("cancellation_reason_required", "transition_not_allowed"),
)

SDLC_REGISTRY: Mapping[str, LifecycleDefinition] = MappingProxyType(
    {
        "story": _entity(
            "story",
            StoryStatus,
            {
                "draft": [_edge("triage"), _edge("ready")],
                "triage": [_edge("draft"), _edge("ready")],
                "ready": [_edge("triage")],
                "converted": [],
            },
        ),
        "ideation": _entity(
            "ideation",
            IdeationStatus,
            {
                "draft": [
                    _edge("review"),
                    _edge("cancelled", **_CANCEL),
                ],
                "review": [
                    _edge("draft"),
                    _edge("approved"),
                    _edge("cancelled", **_CANCEL),
                ],
                "approved": [
                    _edge("review"),
                    _edge("evaluating"),
                    _edge("cancelled", **_CANCEL),
                ],
                "evaluating": [
                    _edge("approved"),
                    _edge(
                        "done",
                        gate="ideation_done",
                        preconditions=(
                            "ambiguity_gate_ready",
                            "resource_gate_ready",
                        ),
                        capabilities=("complete",),
                        reason_codes=(
                            "ambiguity_gate_blocked",
                            "resource_gate_blocked",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "done": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=("status_changed", "version_bumped", "activity_logged"),
                    )
                ],
                "cancelled": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            "status_changed",
                            "cancellation_cleared",
                            "version_bumped",
                            "activity_logged",
                        ),
                    )
                ],
            },
        ),
        "refinement": _entity(
            "refinement",
            RefinementStatus,
            {
                "draft": [
                    _edge(
                        "review",
                        gate="refinement_scope",
                        preconditions=("in_scope_present",),
                        reason_codes=(
                            "refinement_scope_required",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "review": [
                    _edge("draft"),
                    _edge("approved"),
                    _edge("cancelled", **_CANCEL),
                ],
                "approved": [
                    _edge("review"),
                    _edge(
                        "done",
                        gate="refinement_done",
                        preconditions=("resource_gate_ready", "cognitive_gate_ready"),
                        capabilities=("complete",),
                        reason_codes=(
                            "resource_gate_blocked",
                            "cognitive_gate_blocked",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "done": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=("status_changed", "version_bumped", "activity_logged"),
                    )
                ],
                "cancelled": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            "status_changed",
                            "cancellation_cleared",
                            "version_bumped",
                            "activity_logged",
                        ),
                    )
                ],
            },
        ),
        "spec": _entity(
            "spec",
            SpecStatus,
            {
                "draft": [_edge("review"), _edge("cancelled", **_CANCEL)],
                "review": [
                    _edge("draft"),
                    _edge("approved"),
                    _edge("cancelled", **_CANCEL),
                ],
                "approved": [
                    _edge("review"),
                    _edge(
                        "validated",
                        gate="spec_validation",
                        preconditions=("spec_validation_ready",),
                        capabilities=("validate",),
                        reason_codes=("spec_validation_required", "transition_not_allowed"),
                    ),
                    _edge("draft", gate="unlock_content", capabilities=("reopen",)),
                    _edge("cancelled", **_CANCEL),
                ],
                "validated": [
                    _edge("approved", gate="unlock_content", capabilities=("reopen",)),
                    _edge(
                        "in_progress",
                        gate="spec_evaluation",
                        preconditions=("spec_evaluation_ready",),
                        capabilities=("start",),
                        reason_codes=("spec_evaluation_required", "transition_not_allowed"),
                    ),
                    _edge("draft", gate="unlock_content", capabilities=("reopen",)),
                    _edge("cancelled", **_CANCEL),
                ],
                "in_progress": [
                    _edge("validated", gate="reopen", capabilities=("reopen",)),
                    _edge(
                        "done",
                        gate="coverage_and_tasks",
                        preconditions=(
                            "spec_gate_matrix_ready",
                            "all_cards_terminal",
                            "all_sprints_terminal",
                            "resource_gate_ready",
                            "cognitive_gate_ready",
                        ),
                        capabilities=("complete",),
                        reason_codes=(
                            "coverage_incomplete",
                            "cards_incomplete",
                            "sprints_incomplete",
                            "resource_gate_blocked",
                            "cognitive_gate_blocked",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "done": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=("status_changed", "version_bumped", "activity_logged"),
                    )
                ],
                "cancelled": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            "status_changed",
                            "cancellation_cleared",
                            "version_bumped",
                            "activity_logged",
                        ),
                    )
                ],
            },
        ),
        "card": _entity(
            "card",
            CardStatus,
            {
                "not_started": [
                    _edge("started", gate="start_readiness", capabilities=("start",)),
                    _edge(
                        "in_progress",
                        gate="execution_readiness",
                        capabilities=("execute",),
                        card_types=("test", "bug"),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "started": [
                    _edge("not_started", gate="reopen", capabilities=("reopen",)),
                    _edge("in_progress", gate="execution_readiness", capabilities=("execute",)),
                    _edge(
                        "validation",
                        gate="regression_readiness",
                        preconditions=("regression_test_task_linked",),
                        capabilities=("validate",),
                        reason_codes=(
                            "missing_regression_test_task",
                            "transition_not_allowed",
                        ),
                        card_types=("bug",),
                    ),
                    _edge("on_hold", capabilities=("pause",)),
                    _edge("cancelled", **_CANCEL),
                ],
                "in_progress": [
                    _edge("started", gate="reopen", capabilities=("reopen",)),
                    _edge("validation", gate="task_validation", capabilities=("validate",)),
                    _edge(
                        "done",
                        gate="completion",
                        preconditions=("completion_ready", "cognitive_gate_ready"),
                        capabilities=("complete",),
                        reason_codes=(
                            "task_validation_required",
                            "test_scenarios_pending",
                            "cognitive_gate_blocked",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge("on_hold", capabilities=("pause",)),
                    _edge("cancelled", **_CANCEL),
                ],
                "validation": [
                    _edge("in_progress", gate="rework", capabilities=("reopen",)),
                    _edge(
                        "done",
                        gate="task_validation",
                        preconditions=(
                            "task_validation_approved_or_disabled",
                            "completion_ready",
                            "cognitive_gate_ready",
                        ),
                        capabilities=("complete",),
                        reason_codes=(
                            "task_validation_required",
                            "test_scenarios_pending",
                            "cognitive_gate_blocked",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge("on_hold", capabilities=("pause",)),
                    _edge("cancelled", **_CANCEL),
                ],
                "on_hold": [
                    _edge("started", gate="resume", capabilities=("resume",)),
                    _edge("in_progress", gate="resume", capabilities=("resume",)),
                    _edge("cancelled", **_CANCEL),
                ],
                "done": [_edge("in_progress", gate="reopen", capabilities=("reopen",))],
                "cancelled": [
                    _edge(
                        "not_started",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=("status_changed", "cancellation_cleared", "activity_logged"),
                    )
                ],
            },
        ),
        "sprint": _entity(
            "sprint",
            SprintStatus,
            {
                "draft": [
                    _edge(
                        "active",
                        gate="sprint_activation",
                        preconditions=("at_least_one_card", "scope_valid"),
                        capabilities=("start",),
                        reason_codes=("sprint_empty", "scope_invalid", "transition_not_allowed"),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "active": [
                    _edge("draft", gate="reopen", capabilities=("reopen",)),
                    _edge(
                        "review",
                        gate="sprint_review",
                        preconditions=("scoped_tests_ready",),
                        capabilities=("request_review",),
                        reason_codes=("scoped_tests_incomplete", "transition_not_allowed"),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "review": [
                    _edge("active", gate="rework", capabilities=("reopen",)),
                    _edge(
                        "closed",
                        gate="sprint_completion",
                        preconditions=(
                            "all_cards_terminal",
                            "evidence_matrix_ready",
                            "evaluation_approved",
                            "reviewer_separation_ready",
                        ),
                        capabilities=("complete",),
                        reason_codes=(
                            "sprint_has_incomplete_cards",
                            "sprint_scope_gate_blocked",
                            "sprint_evidence_incomplete",
                            "sprint_evaluation_required",
                            "sprint_evaluation_rejected",
                            "sprint_evaluation_below_threshold",
                            "reviewer_separation_required",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "closed": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=("status_changed", "version_bumped", "activity_logged"),
                    )
                ],
                "cancelled": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            "status_changed",
                            "cancellation_cleared",
                            "version_bumped",
                            "activity_logged",
                        ),
                    )
                ],
            },
        ),
    }
)


def lifecycle_definition(entity_type: str) -> LifecycleDefinition:
    normalized = (entity_type or "").strip().lower()
    try:
        return SDLC_REGISTRY[normalized]
    except KeyError as exc:
        raise ValueError(
            f"Unknown SDLC entity_type '{entity_type}'. Expected one of: "
            f"{', '.join(sorted(SDLC_REGISTRY))}"
        ) from exc


def transition_contracts(entity_type: str, current_status: str) -> tuple[TransitionContract, ...]:
    definition = lifecycle_definition(entity_type)
    try:
        definition.status_enum(current_status)
    except ValueError as exc:
        raise ValueError(
            f"Invalid status '{current_status}' for {definition.entity_type}. Expected one of: "
            f"{', '.join(member.value for member in definition.status_enum)}"
        ) from exc
    return definition.transitions.get(current_status, ())


def transition_map(entity_type: str) -> dict[Enum, list[Enum]]:
    """Enum-keyed map consumed by mutation services; always derived here."""

    definition = lifecycle_definition(entity_type)
    return {
        definition.status_enum(from_status): [
            definition.status_enum(edge.to_status) for edge in edges
        ]
        for from_status, edges in definition.transitions.items()
    }


def is_transition_allowed(
    entity_type: str,
    current_status: str,
    target_status: str,
    *,
    card_type: str | None = None,
) -> bool:
    """Return admission for an edge from the canonical lifecycle registry."""

    return any(
        edge.to_status == target_status
        and (not edge.card_types or card_type in edge.card_types)
        for edge in transition_contracts(entity_type, current_status)
    )


__all__ = [
    "LifecycleDefinition",
    "SDLC_REGISTRY",
    "TransitionContract",
    "is_transition_allowed",
    "lifecycle_definition",
    "transition_contracts",
    "transition_map",
]
