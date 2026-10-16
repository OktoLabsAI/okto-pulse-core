"""Public, persistence-free permission policy contract and helpers.

Adapters may use these functions to apply the same policy as Core without
reaching into a private infrastructure module.  This module and its domain
dependencies are standard-library-only so a Core-only installation can define
and validate a SaaS adapter without importing Community.
"""

from __future__ import annotations

import copy
from collections.abc import Collection
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
    SKM_PERMISSION_INTRODUCTION_V1,
    _flatten_registry,
    _get_nested,
    _match_builtin_preset_name,
    _set_nested,
    evaluate_permission,
    get_builtin_presets,
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


def board_membership_allows_read(
    *, owner_id: str | None, actor_id: str, share_permission: str | None = None,
    verified_agent_board_access: bool = False,
    allowed_share_permissions: Collection[str] | None = None,
) -> bool:
    """Canonical Board membership decision after identity/realm/binding checks.

    Adapters load facts only. A verified agent binding is supplied by the
    authentication path; this helper never infers it from a role or Board owner.
    Owner access is independent of a share's optional mutation-role restriction.
    """
    return (
        verified_agent_board_access is True or owner_id == actor_id
        or (share_permission is not None and (allowed_share_permissions is None
            or share_permission in allowed_share_permissions))
    )


def direct_permission_review(agent_flags: object, *, preset_id: str | None) -> tuple[bool, str | None]:
    """Canonical classification of a persisted, preset-less direct document."""
    if preset_id is not None or agent_flags is None:
        return False, None
    if not isinstance(agent_flags, dict):
        return True, "invalid_agent_flags"
    try:
        validate_strict_permission_flags(agent_flags)
    except (TypeError, ValueError, PermissionContractViolation):
        return True, "invalid_agent_flags"
    return (False, None) if agent_flags == PERMISSION_REGISTRY else (True, "unrecognized_direct_permissions")


def resolve_agent_permission_facts(
    *, agent_flags: object, preset_id: str | None,
    presets: tuple[PermissionPresetLineageNode, ...], board_overrides: object,
    policy: PermissionPolicyPort | None = None,
) -> PermissionSet:
    """Resolve current edition-loaded facts through the canonical agent policy."""
    review, reason = direct_permission_review(agent_flags, preset_id=preset_id)
    direct = copy.deepcopy(agent_flags)
    preset_flags = None
    if preset_id:
        lineage = resolve_permission_preset_lineage(preset_id, presets)
        preset_flags, review, reason = lineage.flags, lineage.owner_review_required, lineage.review_reason
    effective = (policy or DefaultPermissionPolicy()).resolve(direct, preset_flags, board_overrides,
        owner_review_required=review, review_reason=reason)
    return effective


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


def skm_permission_introduction_v1() -> PermissionIntroductionManifest:
    """Return the immutable SK-M/v1 permission-introduction manifest."""

    return SKM_PERMISSION_INTRODUCTION_V1


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




def validate_permission_flag_values(
    flags: PermissionFlags | None,
) -> PermissionFlags | None:
    """Require exact boolean leaves for a transport permission document."""

    validate_strict_permission_flags(flags)
    return flags




__all__ = [
    "direct_permission_review",
    "resolve_agent_permission_facts",
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
    "SKM_PERMISSION_INTRODUCTION_V1",
    "builtin_permission_presets",
    "board_membership_allows_read",
    "builtin_preset_name",
    "evaluate_permission",
    "explicit_permission_overrides",
    "flatten_permission_flags",
    "get_permission_flag",
    "permission_introduction_manifests",
    "registered_permission_flags",
    "resolve_effective_permissions",
    "resolve_preset_lineage",
    "set_permission_flag",
    "ska_permission_introduction_v1",
    "skb3_permission_introduction_v1",
    "skm_permission_introduction_v1",
    "validate_permission_flag_values",
]
