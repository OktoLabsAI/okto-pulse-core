"""BASE F2B: preserve exact values without freezing inherited fields."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.task_validation_policy import (
    FIELDS, MigratedTaskValidationPolicy, plan_migrated_validation_policy, resolve_task_validation_config,
)
from okto_pulse.core.models.schemas import CardCreate, CardUpdate
from okto_pulse.core.services import CardService


def population(**sprint_values):
    return (SimpleNamespace(id="card", board_id="board", spec_id="spec", sprint_id="sprint"),
        SimpleNamespace(id="spec", board_id="board", validation_min_completeness=85),
        SimpleNamespace(id="sprint", board_id="board", **sprint_values))


def plan(card, spec, sprint, board=None):
    return plan_migrated_validation_policy(card=card, spec=spec, sprint=sprint,
        board_settings=board or {}, migration_id="migration-1")


def detached(card, policy):
    return SimpleNamespace(**{**vars(card), "sprint_id": None,
        "migrated_validation_policy": policy.model_dump(mode="json", exclude_none=True) if policy else None})


@pytest.mark.parametrize("confidence", [0, 60, 90, 100])
def test_card_preservation_keeps_distinct_sprint_values_in_same_spec(confidence):
    card, spec, sprint = population(validation_min_confidence=confidence)
    before = CardService._resolve_validation_config(None, card, spec, sprint, {})
    policy = plan(card, spec, sprint)
    assert policy.overrides.model_dump(exclude_none=True) == {"min_confidence": confidence}
    assert policy.source_sprint_id == "sprint" and policy.source_spec_id == "spec"
    after = CardService._resolve_validation_config(None, detached(card, policy), spec, None, {})
    assert {field: before[field] for field in FIELDS} == {field: after[field] for field in FIELDS}
    assert after["resolved_sources"] == dict(required="board", min_confidence="card_compatibility",
        min_completeness="spec", max_drift="board")


def test_false_zero_and_null_remain_independent_with_live_inheritance():
    card, spec, sprint = population(require_task_validation=False, validation_min_confidence=0,
        validation_min_completeness=None, validation_max_drift=0)
    policy = plan(card, spec, sprint)
    assert policy.overrides.model_dump(exclude_none=True) == dict(required=False, min_confidence=0, max_drift=0)
    migrated = detached(card, policy)
    spec.validation_min_completeness = 95
    result = resolve_task_validation_config(migrated, spec, None, {"min_confidence": 99})
    assert result["required"] is False and result["min_confidence"] == result["max_drift"] == 0
    assert result["min_completeness"] == 95 and result["resolved_from"] == "card_compatibility"


def test_equal_sprint_override_does_not_freeze_board_or_new_cards():
    card, spec, sprint = population(validation_min_confidence=70)
    assert plan(card, spec, sprint) is None
    migrated = detached(card, None)
    assert resolve_task_validation_config(migrated, spec, None, {"min_confidence": 91})["min_confidence"] == 91
    assert resolve_task_validation_config(SimpleNamespace(id="new"), spec, None, {})["min_confidence"] == 70


@pytest.mark.parametrize("score", [True, -1, 101, "70"])
def test_equal_but_unrepresentable_legacy_scores_cannot_silently_pass_migration(score):
    card, spec, sprint = population(validation_min_confidence=score)
    with pytest.raises(ValueError):
        plan(card, spec, sprint, {"min_confidence": score})


def test_valid_effective_score_is_preserved_when_it_masks_an_invalid_lower_layer():
    card, spec, sprint = population(validation_min_confidence=1)
    policy = plan(card, spec, sprint, {"min_confidence": True})
    assert policy.overrides.min_confidence == 1
    after = resolve_task_validation_config(detached(card, policy), spec, None, {"min_confidence": True})
    assert type(after["min_confidence"]) is int and after["min_confidence"] == 1


@pytest.mark.parametrize("board,expected", [({}, True), ({"require_task_validation": None}, False),
    ({"require_task_validation": False}, False)])
def test_historical_board_required_default_and_explicit_null_are_preserved(board, expected):
    assert resolve_task_validation_config(None, None, None, board)["required"] is expected


@pytest.mark.parametrize("mutation", ["board", "card", "active_sprint", "unknown_version", "unknown_field", "bool_score", "empty"])
def test_corrupt_or_misplaced_compatibility_never_falls_back_silently(mutation):
    card, spec, sprint = population(validation_min_confidence=90)
    migrated = detached(card, plan(card, spec, sprint))
    raw = migrated.migrated_validation_policy
    if mutation in {"board", "card"}:
        raw[mutation + "_id"] = "foreign"
    elif mutation == "active_sprint":
        migrated.sprint_id = "sprint"
    elif mutation == "unknown_version":
        raw["contract_version"] = "future"
    elif mutation == "unknown_field":
        raw["approved"] = True
    elif mutation == "bool_score":
        raw["overrides"]["min_confidence"] = True
    else:
        raw["overrides"] = {}
    with pytest.raises(ValueError):
        resolve_task_validation_config(migrated, spec, None, {})


@pytest.mark.parametrize("mutation", ["orphan", "cross_board", "wrong_spec", "already_migrated"])
def test_migration_refuses_ambiguous_scope_and_recapture(mutation):
    card, spec, sprint = population(validation_min_confidence=90)
    if mutation == "orphan":
        sprint = None
    elif mutation == "cross_board":
        sprint.board_id = "other"
    elif mutation == "wrong_spec":
        card.spec_id = "other"
    else:
        card.migrated_validation_policy = plan(card, spec, sprint).model_dump()
    with pytest.raises(ValueError):
        plan(card, spec, sprint)


@pytest.mark.parametrize("model", [CardCreate, CardUpdate])
@pytest.mark.parametrize("value", [None, {}, {"overrides": {"required": False}}])
def test_executor_dtos_refuse_migration_policy_even_when_null(model, value):
    with pytest.raises(ValidationError, match="migration_only"):
        model.model_validate({"title": "Task", "migrated_validation_policy": value})
    assert "migrated_validation_policy" not in model.model_json_schema()["properties"]


def test_contract_roundtrip_is_immutable_and_planning_does_not_mutate_inputs():
    card, spec, sprint = population(validation_min_confidence=90)
    before = deepcopy((vars(card), vars(spec), vars(sprint)))
    policy = plan(card, spec, sprint)
    assert MigratedTaskValidationPolicy.model_validate_json(policy.model_dump_json()) == policy
    assert before == (vars(card), vars(spec), vars(sprint))
    with pytest.raises(ValidationError):
        policy.overrides.min_confidence = 70
