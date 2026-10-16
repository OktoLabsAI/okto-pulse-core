"""SK-B3/v1 permission introduction and ordered-manifest regression coverage."""

from __future__ import annotations


from okto_pulse.core.domain.permissions import (
    GUIDELINE_ADOPTION_MANAGE,
    GUIDELINE_ASSESSMENTS_READ,
    GUIDELINE_ASSESSMENTS_RECORD,
    GUIDELINE_IMPACT_PREVIEW,
    GUIDELINE_METRICS_AUTHOR,
    GUIDELINE_REVISIONS_CREATE,
    GUIDELINE_REVISIONS_READ,
    GUIDELINE_REVISIONS_RETIRE,
    PERMISSION_INTRODUCTION_MANIFESTS,
    PermissionSet,
    SKA_PERMISSION_INTRODUCTION_V1,
    SKB3_PERMISSION_INTRODUCTION_V1,
    _get_nested,
    get_builtin_presets,
    resolve_permissions,
)
from okto_pulse.core.ports.permission_policy import (
    permission_introduction_manifests,
    registered_permission_flags,
    skb3_permission_introduction_v1,
)


LEAVES = {
    GUIDELINE_REVISIONS_READ,
    GUIDELINE_REVISIONS_CREATE,
    GUIDELINE_REVISIONS_RETIRE,
    GUIDELINE_METRICS_AUTHOR,
    GUIDELINE_IMPACT_PREVIEW,
    GUIDELINE_ADOPTION_MANAGE,
    GUIDELINE_ASSESSMENTS_READ,
    GUIDELINE_ASSESSMENTS_RECORD,
    "guidelines.waiver.read",
    "guidelines.waiver.request",
    "guidelines.waiver.review",
    "guidelines.waiver.revoke",
    "guidelines.waiver.revalidate",
}

AUTHORITIES = {
    GUIDELINE_REVISIONS_READ: "guidelines.read",
    GUIDELINE_REVISIONS_CREATE: "spec.entity.edit_fields",
    GUIDELINE_REVISIONS_RETIRE: "guidelines.delete",
    GUIDELINE_METRICS_AUTHOR: "spec.entity.edit_fields",
    GUIDELINE_IMPACT_PREVIEW: "guidelines.read",
    GUIDELINE_ADOPTION_MANAGE: "spec.entity.edit_fields",
    GUIDELINE_ASSESSMENTS_READ: "guidelines.read",
    GUIDELINE_ASSESSMENTS_RECORD: "guidelines.read",
    "guidelines.waiver.read": "guidelines.read",
    "guidelines.waiver.request": "guidelines.read",
    "guidelines.waiver.review": "spec.validation.submit",
    "guidelines.waiver.revoke": "guidelines.delete",
    "guidelines.waiver.revalidate": "spec.validation.submit",
}

PRESET_GRANTS = {
    "Full Control": LEAVES,
    "Spec": {
        GUIDELINE_REVISIONS_READ,
        GUIDELINE_REVISIONS_CREATE,
        GUIDELINE_METRICS_AUTHOR,
        GUIDELINE_IMPACT_PREVIEW,
        GUIDELINE_ADOPTION_MANAGE,
        GUIDELINE_ASSESSMENTS_READ,
        GUIDELINE_ASSESSMENTS_RECORD,
        "guidelines.waiver.read",
        "guidelines.waiver.request",
    },
    "Validator": {
        GUIDELINE_REVISIONS_READ,
        GUIDELINE_IMPACT_PREVIEW,
        GUIDELINE_ASSESSMENTS_READ,
        GUIDELINE_ASSESSMENTS_RECORD,
        "guidelines.waiver.read",
        "guidelines.waiver.review",
        "guidelines.waiver.revalidate",
    },
    "QA": {
        GUIDELINE_REVISIONS_READ,
        GUIDELINE_ASSESSMENTS_READ,
        GUIDELINE_ASSESSMENTS_RECORD,
        "guidelines.waiver.read",
        "guidelines.waiver.request",
    },
    "Reporter": {
        GUIDELINE_REVISIONS_READ,
        GUIDELINE_ASSESSMENTS_READ,
        "guidelines.waiver.read",
    },
    "Executor": {
        GUIDELINE_REVISIONS_READ,
        GUIDELINE_ASSESSMENTS_READ,
        GUIDELINE_ASSESSMENTS_RECORD,
        "guidelines.waiver.read",
        "guidelines.waiver.request",
    },
}


def _values(flags: dict) -> dict[str, bool]:
    return {leaf: _get_nested(flags, leaf) for leaf in LEAVES}


def _set(flags: dict, path: str, value: bool) -> None:
    current = flags
    parts = path.split(".")
    for part in parts[:-1]:
        current = current[part]
    current[parts[-1]] = value


def test_public_manifest_tuple_is_ordered_unique_and_exact() -> None:
    assert SKB3_PERMISSION_INTRODUCTION_V1.version == "SK-B3/v1"
    assert set(SKB3_PERMISSION_INTRODUCTION_V1.leaves) == LEAVES
    assert len(SKB3_PERMISSION_INTRODUCTION_V1.leaves) == 13
    assert dict(SKB3_PERMISSION_INTRODUCTION_V1.historical_authorities) == AUTHORITIES
    assert skb3_permission_introduction_v1() is SKB3_PERMISSION_INTRODUCTION_V1
    assert permission_introduction_manifests() is PERMISSION_INTRODUCTION_MANIFESTS
    assert PERMISSION_INTRODUCTION_MANIFESTS[:2] == (
        SKA_PERMISSION_INTRODUCTION_V1,
        SKB3_PERMISSION_INTRODUCTION_V1,
    )
    flattened = [
        leaf
        for manifest in PERMISSION_INTRODUCTION_MANIFESTS
        for leaf in manifest.leaves
    ]
    assert len(flattened) == len(set(flattened))
    registry = registered_permission_flags()
    assert all(_get_nested(registry, leaf) is True for leaf in LEAVES)


def test_builtin_preset_matrix_is_exact_and_full_control_propagates() -> None:
    presets = {preset["name"]: preset["flags"] for preset in get_builtin_presets()}
    assert set(presets) == set(PRESET_GRANTS)
    for name, expected in PRESET_GRANTS.items():
        actual = {leaf for leaf, enabled in _values(presets[name]).items() if enabled}
        assert actual == expected
        permission_set = PermissionSet(presets[name])
        assert all(permission_set.has(leaf) for leaf in expected)
        assert all(not permission_set.has(leaf) for leaf in LEAVES - expected)
    assert _values(presets["Full Control"]) == {leaf: True for leaf in LEAVES}
    assert presets["Full Control"]["guidelines"]["read"] is True
    assert presets["Full Control"]["guidelines"]["edit"] is True
    assert {
        leaf
        for leaf, enabled in _values(presets["Reporter"]).items()
        if enabled
    } == {
        GUIDELINE_REVISIONS_READ,
        GUIDELINE_ASSESSMENTS_READ,
        "guidelines.waiver.read",
    }


def test_retired_policy_v1_and_human_skip_leaves_are_not_exposed() -> None:
    registry = registered_permission_flags()
    retired = (
        "guidelines.rules.author_blocking",
        "guidelines.compliance.read",
        "guidelines.compliance.evaluate",
    )
    assert all(_get_nested(registry, leaf) is None for leaf in retired)
    assert all(leaf not in LEAVES for leaf in retired)
    assert all("skip" not in leaf for leaf in LEAVES)


def test_introduced_leaves_fail_closed_and_require_historical_authority() -> None:
    assert all(PermissionSet({}).has(leaf) is False for leaf in LEAVES)
    for leaf, authority in AUTHORITIES.items():
        granular_only: dict = {}
        authority_only: dict = {}
        both: dict = {}
        for document, path in (
            (granular_only, leaf),
            (authority_only, authority),
            (both, leaf),
            (both, authority),
        ):
            current = document
            parts = path.split(".")
            for part in parts[:-1]:
                current = current.setdefault(part, {})
            current[parts[-1]] = True
        assert PermissionSet(granular_only).has(leaf) is False
        assert PermissionSet(authority_only).has(leaf) is False
        assert PermissionSet(both).has(leaf) is True


def test_native_materialized_ceiling_stays_fail_closed() -> None:

    full = next(
        preset["flags"]
        for preset in get_builtin_presets()
        if preset["name"] == "Full Control"
    )
    omitted = resolve_permissions(
        None,
        full,
        {"guidelines": {"read": True, "edit": True}},
    )
    assert _values(omitted.flags) == {leaf: False for leaf in LEAVES}

    admitted = resolve_permissions(
        None,
        full,
        {
            "guidelines": {
                "read": True,
                "create": True,
                "revisions": {"read": True, "create": True},
            }
        },
    )
    assert admitted.has("guidelines.revisions.read") is True
    assert admitted.has("guidelines.revisions.create") is True
    assert admitted.has("guidelines.revisions.retire") is False
