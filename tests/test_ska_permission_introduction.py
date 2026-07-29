"""SK-A/v1 permission introduction, inheritance and fail-closed composition."""

from __future__ import annotations

import copy

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.permissions import (
    PermissionContext,
    PermissionPresetLineageNode,
    PermissionSet,
    SKA_PERMISSION_INTRODUCTION_V1,
    _get_nested,
    evaluate_permission,
    get_builtin_presets,
    map_legacy_permissions,
    merge_missing_flags,
    normalize_agent_permission_overrides,
    permission_flag_overrides,
    resolve_permission_preset_lineage,
    resolve_permissions,
)
from okto_pulse.core.models.schemas import (
    AgentBoardOverridesUpdate,
    AgentCreate,
    AgentUpdate,
)
from okto_pulse.core.ports.permission_policy import (
    registered_permission_flags,
    resolve_effective_permissions,
    ska_permission_introduction_v1,
)


LEAVES = {
    "ideation.quality.read",
    "ideation.quality.assess",
    "refinement.quality.read",
    "refinement.quality.assess",
    "spec.quality.read",
    "spec.quality.assess",
    "refinement.research_decisions.read",
    "refinement.research_decisions.append",
    "spec.checklist.read",
    "spec.checklist.execute",
}

READS = {
    "ideation.quality.read",
    "refinement.quality.read",
    "spec.quality.read",
    "refinement.research_decisions.read",
    "spec.checklist.read",
}


def _values(flags: dict) -> dict[str, bool]:
    return {leaf: _get_nested(flags, leaf) for leaf in LEAVES}


def test_manifest_is_public_versioned_and_contains_exactly_ten_leaves() -> None:
    manifest = SKA_PERMISSION_INTRODUCTION_V1
    assert ska_permission_introduction_v1() is manifest
    assert manifest.version == "SK-A/v1"
    assert set(manifest.leaves) == LEAVES
    assert len(manifest.leaves) == 10
    assert {
        leaf for leaf, _authority in manifest.historical_authorities
    } == LEAVES
    registry = registered_permission_flags()
    assert all(_get_nested(registry, leaf) is True for leaf in LEAVES)


def test_builtin_preset_matrix_is_exact() -> None:
    presets = {preset["name"]: preset["flags"] for preset in get_builtin_presets()}
    expected = {
        "Full Control": LEAVES,
        "Spec": READS
        | {
            "ideation.quality.assess",
            "refinement.quality.assess",
            "refinement.research_decisions.append",
            "spec.checklist.execute",
        },
        "Validator": READS | {"spec.quality.assess"},
        "QA": READS,
        "Reporter": READS,
        "Sprint Manager": READS,
        "Executor": {"spec.quality.read", "spec.checklist.read"},
    }
    assert set(presets) == set(expected)
    for name, grants in expected.items():
        assert {
            leaf for leaf, enabled in _values(presets[name]).items() if enabled
        } == grants
        permission_set = PermissionSet(presets[name])
        assert all(permission_set.has(leaf) for leaf in grants)
        assert all(
            not permission_set.has(leaf)
            for leaf in LEAVES - grants
        )


def test_legacy_bridge_grants_only_the_closed_ra2_matrix() -> None:
    legacy = map_legacy_permissions(["board:read", "specs:update"])
    expected = {
        **{leaf: True for leaf in READS},
        "ideation.quality.assess": True,
        "refinement.quality.assess": True,
        "refinement.research_decisions.append": True,
        "spec.checklist.execute": True,
        "spec.quality.assess": False,
    }
    assert _values(legacy) == expected
    resolved = PermissionSet(legacy)
    assert all(resolved.has(leaf) for leaf, enabled in expected.items() if enabled)
    assert resolved.has("spec.quality.assess") is False

    evaluator = PermissionSet(map_legacy_permissions(["specs:evaluate"]))
    assert all(evaluator.has(leaf) for leaf in READS)
    assert evaluator.has("spec.quality.assess") is True
    assert evaluator.has("ideation.quality.assess") is False

    reads_only = PermissionSet(map_legacy_permissions([]))
    assert all(reads_only.has(leaf) for leaf in READS)
    assert all(reads_only.has(leaf) is False for leaf in LEAVES - READS)


def test_missing_introduced_leaves_remain_denied_on_generic_merge() -> None:
    stored = {"board": {"read": False}}
    merged, added = merge_missing_flags(stored, registered_permission_flags())
    assert added > 0
    assert merged["board"]["read"] is False
    assert merged["profile"]["update"] is True
    assert _values(merged) == {leaf: False for leaf in LEAVES}

    # The old absent-leaf compatibility rule remains scoped to old flags.
    partial = PermissionSet({})
    assert partial.has("board.read") is True
    assert partial.has("spec.quality.read") is False


def test_resolution_preserves_full_control_but_closes_direct_partial_data() -> None:
    trusted_local = resolve_permissions(None, None, None)
    assert _values(trusted_local.flags) == {leaf: True for leaf in LEAVES}

    direct_partial = resolve_permissions({"board": {"read": True}}, None, None)
    assert _values(direct_partial.flags) == {leaf: False for leaf in LEAVES}


def test_sparse_preset_cannot_escalate_through_historical_fallback() -> None:
    resolved = resolve_permissions(None, {}, None)

    assert resolved.owner_review_required is True
    assert resolved.review_reason == "invalid_preset_flags"
    assert all(
        resolved.has(path) is False
        for path in registered_permission_flags_flat()
    )


def test_malformed_agent_layer_is_not_ignored_when_preset_is_valid() -> None:
    full = next(
        preset["flags"]
        for preset in get_builtin_presets()
        if preset["name"] == "Full Control"
    )
    resolved = resolve_permissions(
        {"board": {"read": "true"}},
        full,
        None,
    )

    assert resolved.owner_review_required is True
    assert resolved.review_reason == "invalid_agent_flags"
    assert all(
        resolved.has(path) is False
        for path in registered_permission_flags_flat()
    )


def test_board_ceiling_null_and_materialized_missing_leaf_semantics() -> None:
    spec = {
        preset["name"]: preset["flags"] for preset in get_builtin_presets()
    }["Spec"]
    without_ceiling = resolve_permissions(None, spec, None)
    assert without_ceiling.has("ideation.quality.assess") is True

    missing_new_leaves = resolve_permissions(None, spec, {"board": {"read": True}})
    assert _values(missing_new_leaves.flags) == {
        leaf: False for leaf in LEAVES
    }

    explicit_ceiling = resolve_permissions(
        None,
        spec,
        {
            "ideation": {"quality": {"assess": True}},
            "spec": {"quality": {"assess": True}},
        },
    )
    assert explicit_ceiling.has("ideation.quality.assess") is True
    # A True ceiling cannot expand a preset denial.
    assert explicit_ceiling.has("spec.quality.assess") is False
    # Every other absent introduced ceiling leaf is denied.
    assert explicit_ceiling.has("refinement.quality.assess") is False


@pytest.mark.parametrize(
    "malformed",
    (
        ["not", "an", "object"],
        {"board": {"read": "true"}},
        {"board": True},
        {"vendor_extension": {"grant": 1}},
    ),
    ids=("top-level-list", "canonical-leaf-string", "canonical-branch-bool", "extension-int"),
)
def test_malformed_board_ceiling_is_all_denied_and_requires_review(
    malformed,
) -> None:
    resolved = resolve_effective_permissions(None, None, malformed)

    assert resolved.owner_review_required is True
    assert resolved.review_reason == "invalid_board_overrides"
    assert all(
        resolved.has(path) is False
        for path in registered_permission_flags_flat()
    )
    # Review-required is a complete stop, including unregistered extension
    # paths that would otherwise use historical absent=True compatibility.
    assert resolved.has("vendor_extension.grant") is False


def test_valid_custom_lineage_inherits_absent_and_preserves_explicit_false() -> None:
    spec = next(p for p in get_builtin_presets() if p["name"] == "Spec")
    resolution = resolve_permission_preset_lineage(
        "custom",
        [
            PermissionPresetLineageNode("builtin-spec", spec["flags"]),
            PermissionPresetLineageNode(
                "custom",
                {"ideation": {"quality": {"read": False}}},
                base_preset_id="builtin-spec",
            ),
        ],
    )
    assert resolution.owner_review_required is False
    assert _get_nested(resolution.flags, "ideation.quality.read") is False
    assert _get_nested(resolution.flags, "ideation.quality.assess") is True
    assert _get_nested(resolution.flags, "spec.quality.assess") is False


def test_valid_partial_custom_root_uses_historical_compatibility_baseline() -> None:
    resolution = resolve_permission_preset_lineage(
        "custom-root",
        [
            PermissionPresetLineageNode(
                "custom-root",
                {"board": {"read": False}},
            )
        ],
    )
    assert resolution.owner_review_required is False
    assert _get_nested(resolution.flags, "board.read") is False
    assert _get_nested(resolution.flags, "profile.update") is True
    assert _values(resolution.flags) == {leaf: False for leaf in LEAVES}


def test_invalid_lineages_are_all_denied_and_require_owner_review() -> None:
    cases = (
        (
            "missing",
            [],
            "unknown_preset",
        ),
        (
            "custom",
            [
                PermissionPresetLineageNode(
                    "custom",
                    {"board": {"read": True}},
                    base_preset_id="missing",
                )
            ],
            "dangling_base_preset",
        ),
        (
            "a",
            [
                PermissionPresetLineageNode("a", {}, base_preset_id="b"),
                PermissionPresetLineageNode("b", {}, base_preset_id="a"),
            ],
            "preset_lineage_cycle",
        ),
    )
    for preset_id, nodes, reason in cases:
        resolution = resolve_permission_preset_lineage(preset_id, nodes)
        assert resolution.owner_review_required is True
        assert resolution.review_reason == reason
        assert all(
            _get_nested(resolution.flags, leaf) is False
            for leaf in registered_permission_flags_flat()
        )


def test_non_object_preset_flags_are_malformed_not_an_empty_root() -> None:
    resolution = resolve_permission_preset_lineage(
        "malformed",
        [
            PermissionPresetLineageNode(
                "malformed",
                ["not", "an", "object"],
            )
        ],
    )
    assert resolution.owner_review_required is True
    assert resolution.review_reason == "invalid_preset_flags"
    assert all(
        _get_nested(resolution.flags, leaf) is False
        for leaf in registered_permission_flags_flat()
    )


def registered_permission_flags_flat() -> tuple[str, ...]:
    def walk(document: dict, prefix: str = "") -> list[str]:
        result: list[str] = []
        for key, value in document.items():
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                result.extend(walk(value, path))
            else:
                result.append(path)
        return result

    return tuple(walk(registered_permission_flags()))


def test_owner_review_forces_introduced_leaves_false() -> None:
    full = next(
        preset["flags"]
        for preset in get_builtin_presets()
        if preset["name"] == "Full Control"
    )
    resolved = resolve_permissions(
        None,
        full,
        None,
        owner_review_required=True,
        review_reason="dangling_base_preset",
    )
    assert resolved.owner_review_required is True
    assert resolved.review_reason == "dangling_base_preset"
    assert _values(resolved.flags) == {leaf: False for leaf in LEAVES}
    assert resolved.has("board.read") is False


def test_introduced_permission_requires_granular_and_historical_authority() -> None:
    assert PermissionSet(
        {"spec": {"quality": {"assess": True}}}
    ).has("spec.quality.assess") is False

    granular_only = PermissionSet(
        {
            "spec": {
                "quality": {"assess": True},
                "validation": {"submit": False},
            }
        }
    )
    denied = evaluate_permission(
        PermissionContext(
            operation="spec.quality.assess",
            permissions=granular_only,
        )
    )
    assert denied.allowed is False

    both = PermissionSet(
        {
            "spec": {
                "quality": {"assess": True},
                "validation": {"submit": True},
            }
        }
    )
    allowed = evaluate_permission(
        PermissionContext(
            operation="spec.quality.assess",
            permissions=both,
        )
    )
    assert allowed.allowed is True

    # The old OR fallback must not grant an introduced leaf to a flat legacy
    # permission list.
    legacy = evaluate_permission(
        PermissionContext(
            operation="spec.quality.assess",
            permissions=["specs:evaluate"],
            legacy_operation="specs:evaluate",
        )
    )
    assert legacy.allowed is False


def test_minimal_override_tree_keeps_only_real_differences() -> None:
    base = {
        "spec": {
            "quality": {"read": True, "assess": False},
            "checklist": {"read": True, "execute": False},
        }
    }
    desired = {
        "spec": {
            "quality": {"read": False, "assess": False},
            "checklist": {"read": True, "execute": True},
        }
    }
    assert permission_flag_overrides(base, desired) == {
        "spec": {
            "quality": {"read": False},
            "checklist": {"execute": True},
        }
    }


def _without_ska_branches(flags: dict) -> dict:
    result = copy.deepcopy(flags)
    del result["ideation"]["quality"]
    del result["refinement"]["quality"]
    del result["refinement"]["research_decisions"]
    del result["spec"]["quality"]
    del result["spec"]["checklist"]
    return result


def test_agent_upgrade_normalizes_full_control_and_preset_snapshots() -> None:
    full = next(
        preset["flags"]
        for preset in get_builtin_presets()
        if preset["name"] == "Full Control"
    )
    spec = next(
        preset["flags"]
        for preset in get_builtin_presets()
        if preset["name"] == "Spec"
    )

    historical_full_control = _without_ska_branches(full)
    assert normalize_agent_permission_overrides(historical_full_control) is None

    # Recover the generic False materialization produced by the superseded
    # backfill for a recognizable historical Full Control snapshot.
    faulty_full_control = copy.deepcopy(full)
    for leaf in LEAVES:
        current = faulty_full_control
        parts = leaf.split(".")
        for part in parts[:-1]:
            current = current[part]
        current[parts[-1]] = False
    assert normalize_agent_permission_overrides(faulty_full_control) is None

    historical_full_with_extensions = copy.deepcopy(historical_full_control)
    historical_full_with_extensions["vendor_extension"] = {
        "grant": False,
        "audit": True,
    }
    historical_full_with_extensions["board"]["vendor_extension"] = {
        "strict": True,
    }
    assert normalize_agent_permission_overrides(
        historical_full_with_extensions
    ) == {
        "board": {"vendor_extension": {"strict": True}},
        "vendor_extension": {"grant": False, "audit": True},
    }

    historical_preset_snapshot = _without_ska_branches(spec)
    normalized = normalize_agent_permission_overrides(
        historical_preset_snapshot,
        spec,
    )
    assert normalized == {}
    assert normalize_agent_permission_overrides(normalized, spec) == {}

    explicit_sparse_false = {"ideation": {"quality": {"read": False}}}
    assert normalize_agent_permission_overrides(
        explicit_sparse_false,
        spec,
    ) == explicit_sparse_false


def test_non_boolean_direct_agent_values_deny_instead_of_coercing_truthiness() -> None:
    assert PermissionSet({"board": {"read": "true"}}).has("board.read") is False
    assert PermissionSet({"board": {"read": None}}).has("board.read") is False

    malformed_leaf = resolve_permissions(
        {"board": {"read": "true"}},
        None,
        None,
    )
    assert malformed_leaf.has("board.read") is False

    malformed_branch = resolve_permissions({"board": "true"}, None, None)
    assert malformed_branch.has("board.read") is False
    assert malformed_branch.has("board.analytics_read") is False


@pytest.mark.parametrize(
    "model",
    (
        lambda: AgentCreate(
            name="strict",
            permission_flags={"board": {"read": 1}},
        ),
        lambda: AgentUpdate(
            permission_flags={"board": {"read": "true"}},
        ),
        lambda: AgentBoardOverridesUpdate(
            permission_overrides={"board": {"read": None}},
        ),
    ),
    ids=("create-int", "update-string", "board-null"),
)
def test_agent_rest_schemas_require_exact_boolean_permission_leaves(model) -> None:
    with pytest.raises(ValidationError, match="must be boolean"):
        model()


@pytest.mark.asyncio
async def test_agent_service_keeps_empty_preset_delta_and_full_control_sentinel() -> None:
    import uuid

    from sqlalchemy_test_models import PermissionPreset

    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.main import AgentService

    preset_id = f"ska-empty-clone-{uuid.uuid4()}"
    owner_id = f"ska-owner-{uuid.uuid4()}"
    async with get_session_factory()() as session:
        session.add(
            PermissionPreset(
                id=preset_id,
                owner_id=owner_id,
                name=f"Empty delta clone {uuid.uuid4()}",
                description="inherits its base",
                is_builtin=False,
                base_preset_id="historical-source",
                flags={},
            )
        )
        await session.commit()

        service = AgentService(session)
        assigned, _secret = await service.create_agent(
            owner_id,
            AgentCreate(name="Assigned clone", preset_id=preset_id),
        )
        assert assigned.preset_id == preset_id
        assert assigned.permission_flags == {}

        full_control, _secret = await service.create_agent(
            owner_id,
            AgentCreate(name="Full control"),
        )
        assert full_control.permission_flags is None

        updated = await service.update_agent(
            full_control.id,
            AgentUpdate(preset_id=preset_id),
        )
        assert updated is not None
        assert updated.permission_flags == {}
