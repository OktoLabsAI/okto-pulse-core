"""Focused contracts for the canonical SDLC and sprint lifecycle authority."""

from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.sdlc_registry import (
    SDLC_REGISTRY,
    is_transition_allowed,
)
from okto_pulse.core.services.card_traceability import (
    TraceabilityTargetNotFoundError,
    link_card_traceability,
)
from okto_pulse.core.services.reviewer_separation import (
    evaluate_reviewer_separation,
)
from okto_pulse.core.services.sprint_scope import (
    SprintScopeResolver,
    completion_blockers,
)


def test_registry_covers_every_sdlc_entity_and_exposes_transition_contracts() -> None:
    assert set(SDLC_REGISTRY) == {
        "story",
        "ideation",
        "refinement",
        "spec",
        "card",
        "sprint",
    }
    for definition in SDLC_REGISTRY.values():
        assert set(definition.transitions) == {
            member.value for member in definition.status_enum
        }
        for edges in definition.transitions.values():
            for edge in edges:
                assert edge.label
                assert edge.gate
                assert edge.effects
                assert edge.reason_codes

    assert not is_transition_allowed("card", "not_started", "in_progress")
    assert is_transition_allowed(
        "card", "not_started", "in_progress", card_type="test"
    )


def test_traceability_validates_all_targets_before_mutation_and_is_idempotent() -> None:
    spec = SimpleNamespace(
        id="spec-1",
        test_scenarios=[{"id": "ts-1", "linked_task_ids": []}],
        functional_requirements=[{"id": "fr-1", "linked_task_ids": []}],
        business_rules=[{"id": "br-1", "linked_task_ids": []}],
    )
    card = SimpleNamespace(
        id="card-1", spec_id="spec-1", test_scenario_ids=[]
    )

    with pytest.raises(TraceabilityTargetNotFoundError):
        link_card_traceability(
            spec=spec,
            card=card,
            targets=[("scenario", "ts-1"), ("fr", "missing")],
        )
    assert spec.test_scenarios[0]["linked_task_ids"] == []
    assert card.test_scenario_ids == []

    first = link_card_traceability(
        spec=spec,
        card=card,
        targets=[("scenario", "ts-1"), ("fr", "fr-1"), ("rule", "br-1")],
    )
    second = link_card_traceability(
        spec=spec,
        card=card,
        targets=[("test_scenario", "ts-1"), ("functional_requirement", "fr-1")],
    )
    assert not first.idempotent
    assert second.idempotent
    assert card.test_scenario_ids == ["ts-1"]
    assert spec.test_scenarios[0]["linked_task_ids"] == ["card-1"]
    assert spec.functional_requirements[0]["linked_task_ids"] == ["card-1"]
    assert spec.business_rules[0]["linked_task_ids"] == ["card-1"]


def test_sprint_scope_union_is_version_cached_and_evidence_is_proportional() -> None:
    SprintScopeResolver.clear_cache()
    sprint = SimpleNamespace(
        id="sprint-1",
        version=1,
        test_scenario_ids=["ts-explicit"],
        business_rule_ids=["br-1"],
    )
    spec = SimpleNamespace(
        id="spec-1",
        version=1,
        functional_requirements=[],
        acceptance_criteria=[],
        test_scenarios=[
            {"id": "ts-explicit", "status": "passed", "linked_task_ids": []},
            {"id": "ts-card", "status": "passed", "linked_task_ids": []},
        ],
        business_rules=[{"id": "br-1", "linked_task_ids": []}],
        technical_requirements=[],
        api_contracts=[],
        integration_requirements=[],
        observability_requirements=[],
        decisions=[],
    )
    card = SimpleNamespace(
        id="card-1",
        version=1,
        status="done",
        card_type="test",
        test_scenario_ids=["ts-card"],
    )

    first = SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[card])
    assert SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[card]) is first
    assert set(first.ids("test_scenarios")) == {"ts-explicit", "ts-card"}
    blocker_codes = {item.code for item in completion_blockers(first)}
    assert blocker_codes == {
        "sprint_test_evidence_missing",
        "sprint_business_rule_uncovered",
    }

    spec.version = 2
    assert SprintScopeResolver.resolve(sprint=sprint, spec=spec, cards=[card]) is not first


@pytest.mark.parametrize(
    ("mode", "allowed", "warning", "source"),
    [
        (None, True, False, "legacy_absent_compat"),
        ("off", True, False, "board_settings"),
        ("warn", True, True, "board_settings"),
        ("enforce", False, False, "board_settings"),
    ],
)
def test_reviewer_separation_modes_are_explicit(
    mode: str | None, allowed: bool, warning: bool, source: str
) -> None:
    settings = {} if mode is None else {"reviewer_separation_mode": mode}
    decision = evaluate_reviewer_separation(
        board=SimpleNamespace(settings=settings),
        reviewer_id="same-user",
        sprint=SimpleNamespace(created_by="same-user"),
    )
    assert decision.allowed is allowed
    assert decision.warning is warning
    assert decision.source == source

