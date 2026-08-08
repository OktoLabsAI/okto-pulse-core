"""Public, persistence-free permission policy contract and helpers.

Adapters may use these functions to apply the same policy as Core without
reaching into a private infrastructure module.  This module and its domain
dependencies are standard-library-only so a Core-only installation can define
and validate a SaaS adapter without importing Community.
"""

from __future__ import annotations

import copy
from typing import Any, Protocol, runtime_checkable

from okto_pulse.core.domain.permissions import (
    DefaultPermissionPolicy,
    GUIDELINE_ADOPTION_MANAGE,
    GUIDELINE_ASSESSMENTS_READ,
    GUIDELINE_ASSESSMENTS_RECORD,
    GUIDELINE_IMPACT_PREVIEW,
    GUIDELINE_METRICS_AUTHOR,
    GUIDELINE_REVISIONS_CREATE,
    GUIDELINE_REVISIONS_READ,
    GUIDELINE_REVISIONS_RETIRE,
    InvalidPermissionContext,
    PermissionContext,
    PermissionContractViolation,
    PermissionDecision,
    PermissionFlags,
    PermissionIntroductionManifest,
    PermissionPolicyError,
    PermissionPresetLineageNode,
    PermissionPresetLineageResolution,
    PermissionSet,
    PERMISSION_INTRODUCTION_MANIFESTS,
    PERMISSION_REGISTRY,
    SKA_PERMISSION_INTRODUCTION_V1,
    SKB3_PERMISSION_INTRODUCTION_V1,
    _flatten_registry,
    _get_nested,
    _match_builtin_preset_name,
    _set_nested,
    evaluate_permission,
    get_builtin_presets,
    map_legacy_permissions,
    merge_missing_flags,
    normalize_agent_permission_overrides,
    permission_flag_overrides,
    resolve_permission_preset_lineage,
    resolve_permissions,
    validate_strict_permission_flags,
)


@runtime_checkable
class PermissionPolicyPort(Protocol):
    """Edition-neutral permission policy boundary.

    Implementations may load flags from any edition-owned source, but they must
    return the Core value objects and preserve the canonical ceiling model.
    """

    def resolve(
        self,
        agent_flags: PermissionFlags | None,
        preset_flags: PermissionFlags | None,
        board_overrides: PermissionFlags | None,
        *,
        owner_review_required: bool = False,
        review_reason: str | None = None,
    ) -> PermissionSet:
        """Resolve effective permissions for one application scope."""
        ...

    def evaluate(self, context: PermissionContext) -> PermissionDecision:
        """Evaluate one canonical permission operation."""
        ...


def flatten_permission_flags(flags: PermissionFlags) -> list[str]:
    """Return the registered flag paths present in a partial override payload."""

    return _flatten_registry(flags)


def get_permission_flag(flags: PermissionFlags, path: str) -> Any:
    """Read a nested permission flag by its canonical dotted path."""

    return _get_nested(flags, path)


def set_permission_flag(flags: PermissionFlags, path: str, value: Any) -> None:
    """Set a nested permission flag by its canonical dotted path."""

    _set_nested(flags, path, value)


def builtin_preset_name(flags: PermissionFlags) -> str | None:
    """Return the matching built-in preset name, if the policy has one."""

    return _match_builtin_preset_name(flags)


def legacy_permissions_to_flags(values: list[str]) -> PermissionFlags:
    """Normalize legacy flat permissions into the canonical flag tree."""

    return map_legacy_permissions(values)


def resolve_effective_permissions(
    agent_flags: PermissionFlags | None,
    preset_flags: PermissionFlags | None,
    board_overrides: PermissionFlags | None,
    *,
    owner_review_required: bool = False,
    review_reason: str | None = None,
) -> PermissionSet:
    """Apply the canonical policy merge used by all editions."""

    return resolve_permissions(
        agent_flags,
        preset_flags,
        board_overrides,
        owner_review_required=owner_review_required,
        review_reason=review_reason,
    )


def builtin_permission_presets() -> list[dict[str, Any]]:
    """Return canonical built-in preset definitions for edition bootstrap."""

    return copy.deepcopy(get_builtin_presets())


def registered_permission_flags() -> PermissionFlags:
    """Return an isolated copy of the canonical permission registry."""

    return copy.deepcopy(PERMISSION_REGISTRY)


def ska_permission_introduction_v1() -> PermissionIntroductionManifest:
    """Return the immutable SK-A/v1 permission-introduction manifest."""

    return SKA_PERMISSION_INTRODUCTION_V1


def skb3_permission_introduction_v1() -> PermissionIntroductionManifest:
    """Return the immutable SK-B3/v1 permission-introduction manifest."""

    return SKB3_PERMISSION_INTRODUCTION_V1


def permission_introduction_manifests() -> tuple[PermissionIntroductionManifest, ...]:
    """Return all permission introductions in deterministic upgrade order."""

    return PERMISSION_INTRODUCTION_MANIFESTS


def resolve_preset_lineage(
    preset_id: str,
    presets: list[PermissionPresetLineageNode]
    | tuple[PermissionPresetLineageNode, ...],
) -> PermissionPresetLineageResolution:
    """Resolve custom preset inheritance through the canonical Core policy."""

    return resolve_permission_preset_lineage(preset_id, presets)


def explicit_permission_overrides(
    base: PermissionFlags,
    desired: PermissionFlags,
) -> PermissionFlags:
    """Return direct values that differ from an inherited base tree."""

    return permission_flag_overrides(base, desired)


def normalize_agent_permission_layer(
    agent_flags: PermissionFlags,
    preset_flags: PermissionFlags | None = None,
) -> PermissionFlags | None:
    """Reduce a historical materialized agent snapshot to direct overrides."""

    return normalize_agent_permission_overrides(agent_flags, preset_flags)


def validate_permission_flag_values(
    flags: PermissionFlags | None,
) -> PermissionFlags | None:
    """Require exact boolean leaves for a transport permission document."""

    validate_strict_permission_flags(flags)
    return flags


def merge_permission_registry_defaults(
    stored: PermissionFlags,
) -> tuple[PermissionFlags, int]:
    """Backfill missing canonical flags without overwriting stored values."""

    return merge_missing_flags(stored, PERMISSION_REGISTRY)


__all__ = [
    "DefaultPermissionPolicy",
    "GUIDELINE_ADOPTION_MANAGE",
    "GUIDELINE_ASSESSMENTS_READ",
    "GUIDELINE_ASSESSMENTS_RECORD",
    "GUIDELINE_IMPACT_PREVIEW",
    "GUIDELINE_METRICS_AUTHOR",
    "GUIDELINE_REVISIONS_CREATE",
    "GUIDELINE_REVISIONS_READ",
    "GUIDELINE_REVISIONS_RETIRE",
    "InvalidPermissionContext",
    "PermissionContext",
    "PermissionContractViolation",
    "PermissionDecision",
    "PermissionFlags",
    "PermissionIntroductionManifest",
    "PermissionPolicyError",
    "PermissionPolicyPort",
    "PermissionPresetLineageNode",
    "PermissionPresetLineageResolution",
    "PermissionSet",
    "PERMISSION_INTRODUCTION_MANIFESTS",
    "SKA_PERMISSION_INTRODUCTION_V1",
    "SKB3_PERMISSION_INTRODUCTION_V1",
    "builtin_permission_presets",
    "builtin_preset_name",
    "evaluate_permission",
    "explicit_permission_overrides",
    "flatten_permission_flags",
    "get_permission_flag",
    "legacy_permissions_to_flags",
    "merge_permission_registry_defaults",
    "normalize_agent_permission_layer",
    "permission_introduction_manifests",
    "registered_permission_flags",
    "resolve_effective_permissions",
    "resolve_preset_lineage",
    "set_permission_flag",
    "ska_permission_introduction_v1",
    "skb3_permission_introduction_v1",
    "validate_permission_flag_values",
]
