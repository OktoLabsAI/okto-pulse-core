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
from typing import Literal, Mapping, Sequence

from okto_pulse.core.domain.enums import (
    CardStatus,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintStatus,
    StoryStatus,
    TestScenarioStatus,
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
    policy_compliance: bool = False
    visibility: Literal["public", "internal"] = "public"


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
    policy_compliance: bool = False,
    visibility: Literal["public", "internal"] = "public",
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
        policy_compliance=policy_compliance,
        visibility=visibility,
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
_NEW_EDITION_EFFECTS = (
    "status_changed",
    "edition_bumped",
    "current_validations_cleared",
    "activity_logged",
)
_TEST_SCENARIO_PROGRESSION = dict(
    gate="test_scenario_progression",
    preconditions=("authenticated_test_evidence",),
    reason_codes=(
        "evidence_required",
        "policy_compliance_receipt_missing",
        "policy_compliance_receipt_stale",
        "policy_compliance_blocked",
        "policy_assessment_unavailable",
        "transition_not_allowed",
    ),
    policy_compliance=True,
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
                    _edge("draft", effects=_NEW_EDITION_EFFECTS),
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
                            "ambiguity_assessment_missing",
                            "ambiguity_assessment_stale",
                            "ambiguity_score_exceeds_threshold",
                            "resource_gate_blocked",
                            "transition_not_allowed",
                        ),
                        policy_compliance=True,
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "done": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            *_NEW_EDITION_EFFECTS[:-1],
                            "version_bumped",
                            "activity_logged",
                        ),
                    )
                ],
                "cancelled": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            "status_changed",
                            "edition_bumped",
                            "current_validations_cleared",
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
                    _edge("draft", effects=_NEW_EDITION_EFFECTS),
                    _edge("approved"),
                    _edge("cancelled", **_CANCEL),
                ],
                "approved": [
                    _edge("review"),
                    _edge(
                        "done",
                        gate="refinement_done",
                        preconditions=(
                            "ambiguity_gate_ready",
                            "resource_gate_ready",
                            "cognitive_gate_ready",
                        ),
                        capabilities=("complete",),
                        reason_codes=(
                            "ambiguity_assessment_missing",
                            "ambiguity_assessment_stale",
                            "ambiguity_score_exceeds_threshold",
                            "resource_gate_blocked",
                            "cognitive_gate_blocked",
                            "transition_not_allowed",
                        ),
                        policy_compliance=True,
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "done": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            *_NEW_EDITION_EFFECTS[:-1],
                            "version_bumped",
                            "activity_logged",
                        ),
                    )
                ],
                "cancelled": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            "status_changed",
                            "edition_bumped",
                            "current_validations_cleared",
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
                    _edge("draft", effects=_NEW_EDITION_EFFECTS),
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
                        reason_codes=(
                            "spec_validation_required",
                            "spec_checklist_gate_required",
                            "transition_not_allowed",
                        ),
                        policy_compliance=True,
                    ),
                    _edge(
                        "draft",
                        gate="unlock_content",
                        capabilities=("reopen",),
                        effects=_NEW_EDITION_EFFECTS,
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "validated": [
                    # Returning to Approved permits a successor assessment in
                    # the same edition. It preserves Current and does not
                    # unlock Spec content; only entering Draft does that.
                    _edge("approved"),
                    _edge(
                        "in_progress",
                        gate="spec_evaluation",
                        preconditions=(
                            "spec_evaluation_ready",
                            "spec_dependencies_ready",
                        ),
                        capabilities=("start",),
                        reason_codes=(
                            "spec_evaluation_required",
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge(
                        "draft",
                        gate="unlock_content",
                        capabilities=("reopen",),
                        effects=_NEW_EDITION_EFFECTS,
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "in_progress": [
                    # This is a same-edition lifecycle move. Current validation
                    # and the content lock remain authoritative.
                    _edge("validated"),
                    _edge(
                        "draft",
                        gate="unlock_content",
                        capabilities=("reopen",),
                        effects=_NEW_EDITION_EFFECTS,
                    ),
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
                        effects=(
                            *_NEW_EDITION_EFFECTS[:-1],
                            "version_bumped",
                            "activity_logged",
                        ),
                    )
                ],
                "cancelled": [
                    _edge(
                        "draft",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            "status_changed",
                            "edition_bumped",
                            "current_validations_cleared",
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
                    _edge(
                        "started",
                        gate="start_readiness",
                        preconditions=("spec_dependencies_ready",),
                        capabilities=("start",),
                        reason_codes=(
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge(
                        "in_progress",
                        gate="execution_readiness",
                        preconditions=("spec_dependencies_ready",),
                        capabilities=("execute",),
                        reason_codes=(
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                        card_types=("test", "bug"),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "started": [
                    _edge("not_started", gate="reopen", capabilities=("reopen",)),
                    _edge(
                        "in_progress",
                        gate="execution_readiness",
                        preconditions=("spec_dependencies_ready",),
                        capabilities=("execute",),
                        reason_codes=(
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                    ),
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
                    _edge(
                        "started",
                        gate="reopen",
                        capabilities=("reopen",),
                        card_types=("normal",),
                    ),
                    _edge(
                        "validation", gate="task_validation", capabilities=("validate",)
                    ),
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
                        policy_compliance=True,
                    ),
                    _edge("on_hold", capabilities=("pause",)),
                    _edge("cancelled", **_CANCEL),
                ],
                "validation": [
                    _edge(
                        "in_progress",
                        gate="test_rework",
                        preconditions=("spec_dependencies_ready",),
                        capabilities=("reopen",),
                        reason_codes=(
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                        card_types=("test",),
                    ),
                    _edge(
                        "rejected",
                        gate="completion_rejection",
                        preconditions=("sealed_rejection_cause",),
                        capabilities=("record_consequence",),
                        effects=(
                            "status_changed",
                            "rejection_cause_sealed",
                            "activity_logged",
                        ),
                        reason_codes=(
                            "task_validation_failed",
                            "completion_gate_blocked",
                            "transition_not_allowed",
                        ),
                        card_types=("normal", "bug"),
                        visibility="internal",
                    ),
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
                        policy_compliance=True,
                    ),
                    _edge("on_hold", capabilities=("pause",)),
                    _edge("cancelled", **_CANCEL),
                ],
                "on_hold": [
                    _edge(
                        "started",
                        gate="resume",
                        preconditions=("spec_dependencies_ready",),
                        capabilities=("resume",),
                        reason_codes=(
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge(
                        "in_progress",
                        gate="resume",
                        preconditions=("spec_dependencies_ready",),
                        capabilities=("resume",),
                        reason_codes=(
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                    ),
                    _edge("cancelled", **_CANCEL),
                ],
                "done": [
                    _edge(
                        "in_progress",
                        gate="reopen",
                        preconditions=("spec_dependencies_ready",),
                        capabilities=("reopen",),
                        reason_codes=(
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                    )
                ],
                "rejected": [
                    _edge(
                        "in_progress",
                        gate="rework_handoff",
                        preconditions=(
                            "current_rejection_cause_present",
                            "spec_dependencies_ready",
                        ),
                        capabilities=("rework",),
                        effects=(
                            "status_changed",
                            "current_rejection_cleared",
                            "rework_started",
                            "activity_logged",
                        ),
                        reason_codes=(
                            "current_rejection_cause_missing",
                            "spec_dependencies_incomplete",
                            "transition_not_allowed",
                        ),
                        card_types=("normal", "bug"),
                    )
                ],
                "cancelled": [
                    _edge(
                        "not_started",
                        gate="reopen",
                        capabilities=("reopen",),
                        effects=(
                            "status_changed",
                            "cancellation_cleared",
                            "activity_logged",
                        ),
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
                        reason_codes=(
                            "sprint_empty",
                            "scope_invalid",
                            "transition_not_allowed",
                        ),
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
                        reason_codes=(
                            "scoped_tests_incomplete",
                            "transition_not_allowed",
                        ),
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
                        policy_compliance=True,
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
        "test_scenario": _entity(
            "test_scenario",
            TestScenarioStatus,
            {
                "draft": [
                    _edge("ready"),
                    _edge("automated", **_TEST_SCENARIO_PROGRESSION),
                    _edge("passed", **_TEST_SCENARIO_PROGRESSION),
                    _edge("failed", **_TEST_SCENARIO_PROGRESSION),
                ],
                "ready": [
                    _edge("draft"),
                    _edge("automated", **_TEST_SCENARIO_PROGRESSION),
                    _edge("passed", **_TEST_SCENARIO_PROGRESSION),
                    _edge("failed", **_TEST_SCENARIO_PROGRESSION),
                ],
                "automated": [
                    _edge("ready"),
                    _edge("passed", **_TEST_SCENARIO_PROGRESSION),
                ],
                "failed": [
                    _edge("ready"),
                    _edge("passed", **_TEST_SCENARIO_PROGRESSION),
                ],
                "passed": [_edge("ready")],
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


def transition_contracts(
    entity_type: str, current_status: str
) -> tuple[TransitionContract, ...]:
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
            definition.status_enum(edge.to_status)
            for edge in edges
            if edge.visibility == "public"
        ]
        for from_status, edges in definition.transitions.items()
    }


def transition_permission_flag(
    entity_type: str,
    current_status: str,
    target_status: str,
) -> str:
    """Return the canonical permission leaf for one registered edge."""

    normalized_entity = lifecycle_definition(entity_type).entity_type
    # Permission leaves describe the registered edge itself. Card-type
    # restrictions remain a domain admission concern in ``is_transition_allowed``
    # and the mutation service; requiring a card_type here would make valid
    # subtype-specific edges impossible to authorize.
    if not any(
        edge.to_status == target_status and edge.visibility == "public"
        for edge in transition_contracts(normalized_entity, current_status)
    ):
        raise ValueError(
            f"Unregistered transition '{normalized_entity}:{current_status}"
            f"->{target_status}'"
        )
    return f"{normalized_entity}.move.{current_status}_to_{target_status}"


def transition_permission_flags(entity_type: str | None = None) -> tuple[str, ...]:
    """Project exact transition permission leaves from ``SDLC_REGISTRY``."""

    definitions = (
        (lifecycle_definition(entity_type),)
        if entity_type is not None
        else tuple(SDLC_REGISTRY.values())
    )
    return tuple(
        f"{definition.entity_type}.move.{current_status}_to_{edge.to_status}"
        for definition in definitions
        for current_status, edges in definition.transitions.items()
        for edge in edges
        if edge.visibility == "public"
    )


def transition_permission_registry(entity_type: str) -> dict[str, bool]:
    """Return the nested ``move`` branch consumed by the policy registry."""

    prefix = f"{lifecycle_definition(entity_type).entity_type}.move."
    return {
        flag.removeprefix(prefix): True
        for flag in transition_permission_flags(entity_type)
    }


def lifecycle_state_permission_registry(entity_type: str) -> dict[str, bool]:
    """Return the canonical ``interact_in`` branch for a lifecycle entity."""

    definition = lifecycle_definition(entity_type)
    return {member.value: True for member in definition.status_enum}


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
        and edge.visibility == "public"
        and (not edge.card_types or card_type in edge.card_types)
        for edge in transition_contracts(entity_type, current_status)
    )


def is_internal_transition_allowed(
    entity_type: str,
    current_status: str,
    target_status: str,
    *,
    card_type: str | None = None,
) -> bool:
    """Admit a consequence-only edge that transports must never expose."""

    return any(
        edge.to_status == target_status
        and edge.visibility == "internal"
        and (not edge.card_types or card_type in edge.card_types)
        for edge in transition_contracts(entity_type, current_status)
    )


def transition_requires_policy_compliance(
    entity_type: str,
    current_status: str,
    target_status: str,
    *,
    card_type: str | None = None,
) -> bool:
    """Whether one legal edge carries the frozen policy-compliance gate."""

    return any(
        edge.to_status == target_status
        and edge.policy_compliance
        and (not edge.card_types or card_type in edge.card_types)
        for edge in transition_contracts(entity_type, current_status)
    )


__all__ = [
    "LifecycleDefinition",
    "SDLC_REGISTRY",
    "TransitionContract",
    "is_internal_transition_allowed",
    "is_transition_allowed",
    "lifecycle_definition",
    "lifecycle_state_permission_registry",
    "transition_contracts",
    "transition_map",
    "transition_permission_flag",
    "transition_permission_flags",
    "transition_permission_registry",
    "transition_requires_policy_compliance",
]
