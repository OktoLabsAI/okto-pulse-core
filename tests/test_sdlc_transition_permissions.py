"""Permission paths must be an exact projection of the canonical SDLC graph."""

from __future__ import annotations

import copy

import pytest

from okto_pulse.core.application.use_cases.mutation_permissions import (
    transition_permission_requirement,
)
from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.domain.permissions import (
    ALL_FLAGS,
    DefaultPermissionPolicy,
    PERMISSION_INTRODUCTION_MANIFESTS,
    PERMISSION_REGISTRY,
    PermissionContext,
    SDLC_TRANSITION_PERMISSION_INTRODUCTION_V1,
    PermissionSet,
    get_builtin_presets,
    normalize_agent_permission_overrides,
    resolve_permissions,
)
from okto_pulse.core.domain.sdlc_registry import (
    SDLC_REGISTRY,
    transition_permission_flag,
    transition_permission_flags,
    transition_permission_registry,
    lifecycle_state_permission_registry,
)


def _delete(document: dict[str, object], path: str) -> None:
    parts = path.split(".")
    current = document
    parents: list[tuple[dict[str, object], str]] = []
    for part in parts[:-1]:
        child = current.get(part)
        if not isinstance(child, dict):
            return
        parents.append((current, part))
        current = child
    current.pop(parts[-1], None)
    for parent, key in reversed(parents):
        child = parent.get(key)
        if isinstance(child, dict) and not child:
            parent.pop(key)


def _set(document: dict[str, object], path: str, value: bool) -> None:
    current = document
    parts = path.split(".")
    for part in parts[:-1]:
        child = current.setdefault(part, {})
        assert isinstance(child, dict)
        current = child
    current[parts[-1]] = value


def test_registry_move_flags_are_exact_sdlc_projection() -> None:
    expected = set(transition_permission_flags())
    actual = {flag for flag in ALL_FLAGS if ".move." in flag}

    assert len(expected) == 92
    assert actual == expected
    introduced_moves = {
        leaf
        for leaf in SDLC_TRANSITION_PERMISSION_INTRODUCTION_V1.leaves
        if ".move." in leaf
    }
    assert len(introduced_moves) == 66
    assert introduced_moves < expected
    assert set(SDLC_TRANSITION_PERMISSION_INTRODUCTION_V1.leaves) - introduced_moves == {
        "ideation.interact_in.review",
        "ideation.interact_in.approved",
    }
    assert not any("any_to_" in flag for flag in actual)


@pytest.mark.parametrize("entity", sorted(SDLC_REGISTRY))
def test_nested_move_branch_is_generated_from_each_lifecycle(entity: str) -> None:
    expected = {
        flag.removeprefix(f"{entity}.move."): True
        for flag in transition_permission_flags(entity)
    }

    assert transition_permission_registry(entity) == expected
    assert lifecycle_state_permission_registry(entity) == {
        member.value: True for member in SDLC_REGISTRY[entity].status_enum
    }


def test_transition_permission_flag_rejects_edges_absent_from_sdlc() -> None:
    assert (
        transition_permission_flag("card", "validation", "in_progress")
        == "card.move.validation_to_in_progress"
    )
    with pytest.raises(ValueError, match="Unregistered transition"):
        transition_permission_flag("card", "validation", "not_started")


@pytest.mark.parametrize(
    ("current", "target"),
    [
        ("not_started", "in_progress"),
        ("started", "validation"),
    ],
)
def test_card_type_restricted_edges_still_have_authorizable_flags(
    current: str,
    target: str,
) -> None:
    assert transition_permission_flag("card", current, target) == (
        f"card.move.{current}_to_{target}"
    )


def test_transition_requirement_normalizes_enum_values_and_keeps_state_gate() -> None:
    requirement = transition_permission_requirement(
        "card",
        CardStatus.VALIDATION,
        CardStatus.IN_PROGRESS,
        legacy_operation="cards:move",
    )

    assert requirement.operation == "card.move.validation_to_in_progress"
    assert requirement.legacy_operation == "cards:move"
    assert requirement.entity == "card"
    assert requirement.state == "validation"


def test_transition_introduction_accepts_only_the_explicit_legacy_move_token() -> None:
    policy = DefaultPermissionPolicy()
    requirement = transition_permission_requirement(
        "card",
        "validation",
        "in_progress",
        legacy_operation="cards:move",
    )

    allowed = policy.evaluate(
        PermissionContext(
            requirement.operation,
            permissions=["cards:move"],
            entity=requirement.entity,
            state=requirement.state,
            legacy_operation=requirement.legacy_operation,
        )
    )
    denied = policy.evaluate(
        PermissionContext(
            requirement.operation,
            permissions=["cards:update"],
            entity=requirement.entity,
            state=requirement.state,
            legacy_operation=requirement.legacy_operation,
        )
    )

    assert allowed.allowed is True
    assert denied.allowed is False


def test_transition_grants_are_fail_closed_and_explicit_per_preset() -> None:
    manifest = SDLC_TRANSITION_PERMISSION_INTRODUCTION_V1
    assert {name for name, _grants in manifest.preset_grants} == {
        "Full Control",
        "Executor",
        "Validator",
        "QA",
        "Reporter",
        "Sprint Manager",
        "Spec",
    }

    sample = "card.move.validation_to_in_progress"
    assert PermissionSet({}).has(sample) is False
    assert PermissionSet({"card": {"move": {"validation_to_in_progress": True}}}).has(
        sample
    ) is False

    # Existing exact leaves are not reclassified as a new generation.
    assert PermissionSet({}).has("card.move.not_started_to_started") is True

    for preset in get_builtin_presets():
        permissions = PermissionSet(preset["flags"])
        expected = set(manifest.grants_for(preset["name"]))
        for leaf in manifest.leaves:
            assert permissions.has(leaf) is (leaf in expected)


def test_qa_preset_can_use_granted_test_scenario_edge_and_state_gate() -> None:
    qa = next(preset for preset in get_builtin_presets() if preset["name"] == "QA")
    permissions = PermissionSet(qa["flags"])

    assert permissions.check_with_state(
        "test_scenario.move.draft_to_ready",
        "test_scenario",
        "draft",
    ) is None


def test_spec_preset_preserves_historical_test_scenario_status_authority() -> None:
    spec = next(preset for preset in get_builtin_presets() if preset["name"] == "Spec")
    permissions = PermissionSet(spec["flags"])

    assert permissions.has("spec.tests.update_status") is True
    assert permissions.has("spec.tests.execute") is True
    assert permissions.check_with_state(
        "test_scenario.move.draft_to_ready",
        "test_scenario",
        "draft",
    ) is None


def test_pre_registry_full_control_snapshot_normalizes_without_transition_denials() -> None:
    snapshot = copy.deepcopy(PERMISSION_REGISTRY)
    for manifest in PERMISSION_INTRODUCTION_MANIFESTS:
        for leaf in manifest.leaves:
            _delete(snapshot, leaf)
    for retired in (
        "card.move.any_to_cancelled",
        "card.move.validation_to_not_started",
        "ideation.move.any_to_cancelled",
        "ideation.move.draft_to_evaluating",
        "ideation.move.evaluating_to_refined",
        "ideation.move.refined_to_done",
        "refinement.move.any_to_cancelled",
        "refinement.move.draft_to_in_progress",
        "refinement.move.in_progress_to_review",
        "spec.move.any_to_cancelled",
        "sprint.move.any_to_cancelled",
        "ideation.interact_in.refined",
        "refinement.interact_in.in_progress",
    ):
        _set(snapshot, retired, True)

    normalized = normalize_agent_permission_overrides(snapshot)

    assert normalized is None
    permissions = resolve_permissions(normalized, None, None)
    assert all(permissions.has(flag) for flag in transition_permission_flags())


def test_legacy_cancellation_wildcard_does_not_become_an_exact_deny() -> None:
    executor = next(
        preset for preset in get_builtin_presets() if preset["name"] == "Executor"
    )
    snapshot = copy.deepcopy(executor["flags"])
    for manifest in PERMISSION_INTRODUCTION_MANIFESTS:
        for leaf in manifest.leaves:
            _delete(snapshot, leaf)
    _set(snapshot, "card.move.any_to_cancelled", True)
    # Historical wildcard authorization dominated this stale exact value.
    _set(snapshot, "card.move.validation_to_cancelled", False)

    normalized = normalize_agent_permission_overrides(snapshot, executor["flags"])
    permissions = resolve_permissions(normalized, executor["flags"], None)

    assert permissions.has("card.move.validation_to_cancelled") is True
    for flag in transition_permission_flags("card"):
        assert permissions.has(flag) is PermissionSet(executor["flags"]).has(flag)
