"""Fail-closed contracts for the policy-coverage namespace generation."""

from __future__ import annotations

from okto_pulse.core.domain.permissions import (
    ADMIN_CATALOG_PERMISSION_INTRODUCTION_V1,
    ALL_FLAGS,
    KG_OPERATIONS_PERMISSION_INTRODUCTION_V1,
    MCP_GAPS_PERMISSION_INTRODUCTION_V1,
    OPERATIONAL_PERMISSION_INTRODUCTION_V1,
    PERMISSION_INTRODUCTION_MANIFESTS,
    PermissionSet,
    get_builtin_presets,
)


NEW_MANIFESTS = (
    ADMIN_CATALOG_PERMISSION_INTRODUCTION_V1,
    OPERATIONAL_PERMISSION_INTRODUCTION_V1,
    MCP_GAPS_PERMISSION_INTRODUCTION_V1,
    KG_OPERATIONS_PERMISSION_INTRODUCTION_V1,
)


def _flags(*paths: str) -> dict[str, object]:
    result: dict[str, object] = {}
    for path in paths:
        current = result
        parts = path.split(".")
        for part in parts[:-1]:
            child = current.setdefault(part, {})
            assert isinstance(child, dict)
            current = child
        current[parts[-1]] = True
    return result


def test_new_namespace_manifests_are_ordered_and_registered() -> None:
    assert PERMISSION_INTRODUCTION_MANIFESTS[2:6] == NEW_MANIFESTS
    assert [manifest.version for manifest in NEW_MANIFESTS] == [
        "ADMIN-CATALOG/v1",
        "OPERATIONAL/v1",
        "MCP-GAPS/v1",
        "KG-OPERATIONS/v1",
    ]

    leaves = [leaf for manifest in NEW_MANIFESTS for leaf in manifest.leaves]
    assert len(leaves) == len(set(leaves)) == 110
    assert set(leaves) <= set(ALL_FLAGS)
    assert len(ALL_FLAGS) == 583


def test_every_new_leaf_has_one_authority_and_explicit_builtin_grants() -> None:
    expected_presets = {
        "Full Control",
        "Executor",
        "Validator",
        "QA",
        "Reporter",
        "Sprint Manager",
        "Spec",
    }
    for manifest in NEW_MANIFESTS:
        assert set(dict(manifest.historical_authorities)) == set(manifest.leaves)
        assert {name for name, _grants in manifest.preset_grants} == expected_presets
        assert set(manifest.grants_for("Full Control")) == set(manifest.leaves)


def test_new_leaves_require_both_capability_and_historical_authority() -> None:
    for manifest in NEW_MANIFESTS:
        for leaf, authority in manifest.historical_authorities:
            assert PermissionSet({}).has(leaf) is False
            assert PermissionSet(_flags(leaf)).has(leaf) is False
            assert PermissionSet(_flags(leaf, authority)).has(leaf) is True


def test_builtin_grant_matrix_matches_effective_policy() -> None:
    for preset in get_builtin_presets():
        permissions = PermissionSet(preset["flags"])
        for manifest in NEW_MANIFESTS:
            expected = set(manifest.grants_for(preset["name"]))
            for leaf in manifest.leaves:
                assert permissions.has(leaf) is (leaf in expected)


def test_required_action_namespaces_are_present() -> None:
    required_prefixes = {
        "agent.",
        "board.admin.",
        "board.share.",
        "permission_preset.",
        "default_board_config.",
        "design_system.",
        "runtime.",
        "metrics.",
        "amendment.",
        "kg.operations.",
        "test_scenario.interact_in.",
    }
    introduced = {leaf for manifest in NEW_MANIFESTS for leaf in manifest.leaves}
    for prefix in required_prefixes:
        assert any(leaf.startswith(prefix) for leaf in introduced), prefix
