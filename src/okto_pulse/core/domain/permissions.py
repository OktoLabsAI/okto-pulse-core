"""Pure permission policy and value objects.

Provides:
- Legacy flat permission constants (Permissions class) for backward compat
- Granular permission registry (PERMISSION_REGISTRY) with ~190 flags
- PermissionSet class for resolved, board-scoped permissions

This module is deliberately standard-library-only.  Persistence, credential
loading and edition composition belong to adapters; permission semantics and
state-aware decisions remain Core business policy.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Mapping, TypeAlias

from okto_pulse.core.domain.mcp_permission_registry import (
    HUMAN_ONLY_MCP_TOOL_EXEMPTIONS,
    MAX_HUMAN_ONLY_TOOL_EXEMPTIONS,
    MCP_TOOL_PERMISSION_POLICIES,
    HumanOnlyToolExemption,
    McpPermissionRegistryError,
    McpPermissionRegistryReport,
    McpToolPermissionPolicy,
    build_mcp_permission_registry_report,
)
from okto_pulse.core.domain.sdlc_registry import (
    SDLC_REGISTRY,
    lifecycle_state_permission_registry,
    transition_permission_flags,
    transition_permission_registry,
)


PermissionFlags: TypeAlias = dict[str, Any]


class PermissionPolicyError(Exception):
    """Base class for transport-free permission contract failures."""


class InvalidPermissionContext(PermissionPolicyError):
    """A permission decision was requested without a canonical operation."""


class PermissionContractViolation(PermissionPolicyError):
    """A permission policy implementation returned an invalid result."""


@dataclass(frozen=True)
class PermissionIntroductionManifest:
    """Versioned, fail-closed introduction contract for permission leaves.

    A permission added through this contract is deliberately different from
    the historical registry additions: an absent introduced leaf is denied
    until a built-in preset, a valid custom-preset lineage, or an explicit
    direct override grants it.
    """

    version: str
    leaves: tuple[str, ...]
    preset_grants: tuple[tuple[str, tuple[str, ...]], ...]
    historical_authorities: tuple[tuple[str, str], ...]
    recover_all_false_materialization: bool = False

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise PermissionContractViolation(
                "permission introduction version must not be empty"
            )
        if not self.leaves or len(set(self.leaves)) != len(self.leaves):
            raise PermissionContractViolation(
                "permission introduction leaves must be non-empty and unique"
            )
        known = set(self.leaves)
        names: set[str] = set()
        for preset_name, grants in self.preset_grants:
            if not preset_name.strip() or preset_name in names:
                raise PermissionContractViolation(
                    "permission introduction preset names must be unique"
                )
            names.add(preset_name)
            if len(set(grants)) != len(grants) or not set(grants) <= known:
                raise PermissionContractViolation(
                    f"invalid permission introduction grants for {preset_name!r}"
                )
        authority_leaves = tuple(
            leaf for leaf, _authority in self.historical_authorities
        )
        if (
            len(set(authority_leaves)) != len(authority_leaves)
            or set(authority_leaves) != known
            or any(
                not authority.strip() for _, authority in self.historical_authorities
            )
        ):
            raise PermissionContractViolation(
                "every introduced leaf requires one historical authority"
            )

    def grants_for(self, preset_name: str) -> tuple[str, ...]:
        for name, grants in self.preset_grants:
            if name == preset_name:
                return grants
        return ()

    def historical_authority_for(self, permission: str) -> str | None:
        for leaf, authority in self.historical_authorities:
            if leaf == permission:
                return authority
        return None


_SKA_PERMISSION_LEAVES: tuple[str, ...] = (
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
)

_SKA_CONTEXT_READ_LEAVES: tuple[str, ...] = (
    "ideation.quality.read",
    "refinement.quality.read",
    "spec.quality.read",
    "refinement.research_decisions.read",
    "spec.checklist.read",
)


SKA_PERMISSION_INTRODUCTION_V1 = PermissionIntroductionManifest(
    version="SK-A/v1",
    leaves=_SKA_PERMISSION_LEAVES,
    preset_grants=(
        ("Full Control", _SKA_PERMISSION_LEAVES),
        (
            "Spec",
            (
                *_SKA_CONTEXT_READ_LEAVES,
                "ideation.quality.assess",
                "refinement.quality.assess",
                "refinement.research_decisions.append",
                "spec.checklist.execute",
            ),
        ),
        (
            "Validator",
            (
                *_SKA_CONTEXT_READ_LEAVES,
                "spec.quality.assess",
            ),
        ),
        ("QA", _SKA_CONTEXT_READ_LEAVES),
        ("Reporter", _SKA_CONTEXT_READ_LEAVES),
        ("Sprint Manager", _SKA_CONTEXT_READ_LEAVES),
        (
            "Executor",
            (
                "spec.quality.read",
                "spec.checklist.read",
            ),
        ),
    ),
    historical_authorities=(
        ("ideation.quality.read", "ideation.entity.read"),
        ("ideation.quality.assess", "spec.entity.edit_fields"),
        ("refinement.quality.read", "refinement.entity.read"),
        ("refinement.quality.assess", "spec.entity.edit_fields"),
        ("spec.quality.read", "spec.entity.read"),
        ("spec.quality.assess", "spec.validation.submit"),
        (
            "refinement.research_decisions.read",
            "refinement.entity.read",
        ),
        (
            "refinement.research_decisions.append",
            "spec.entity.edit_fields",
        ),
        ("spec.checklist.read", "spec.entity.read"),
        ("spec.checklist.execute", "spec.entity.edit_fields"),
    ),
    # A superseded SK-A migration materialized the entire generation as False
    # before lineage reconciliation existed.  This one-time compatibility
    # marker lets recognizable legacy snapshots recover without making that
    # unsafe inference for later manifests.
    recover_all_false_materialization=True,
)


GUIDELINE_REVISIONS_READ = "guidelines.revisions.read"
GUIDELINE_REVISIONS_CREATE = "guidelines.revisions.create"
GUIDELINE_REVISIONS_RETIRE = "guidelines.revisions.retire"
GUIDELINE_METRICS_AUTHOR = "guidelines.metrics.author"
GUIDELINE_IMPACT_PREVIEW = "guidelines.impact.preview"
GUIDELINE_ADOPTION_MANAGE = "guidelines.adoption.manage"
GUIDELINE_ASSESSMENTS_READ = "guidelines.assessments.read"
GUIDELINE_ASSESSMENTS_RECORD = "guidelines.assessments.record"


_SKB3_PERMISSION_LEAVES: tuple[str, ...] = (
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
)


# This explicit bridge is intentionally conservative and auditable.  A new
# SK-B3 capability never manufactures authority that the same actor did not
# already hold through the historical guidelines/SDLC surface.
_SKB3_HISTORICAL_AUTHORITIES: tuple[tuple[str, str], ...] = (
    (GUIDELINE_REVISIONS_READ, "guidelines.read"),
    (GUIDELINE_REVISIONS_CREATE, "spec.entity.edit_fields"),
    (GUIDELINE_REVISIONS_RETIRE, "guidelines.delete"),
    (GUIDELINE_METRICS_AUTHOR, "spec.entity.edit_fields"),
    (GUIDELINE_IMPACT_PREVIEW, "guidelines.read"),
    (GUIDELINE_ADOPTION_MANAGE, "spec.entity.edit_fields"),
    (GUIDELINE_ASSESSMENTS_READ, "guidelines.read"),
    (GUIDELINE_ASSESSMENTS_RECORD, "guidelines.read"),
    ("guidelines.waiver.read", "guidelines.read"),
    ("guidelines.waiver.request", "guidelines.read"),
    ("guidelines.waiver.review", "spec.validation.submit"),
    ("guidelines.waiver.revoke", "guidelines.delete"),
    ("guidelines.waiver.revalidate", "spec.validation.submit"),
)


SKB3_PERMISSION_INTRODUCTION_V1 = PermissionIntroductionManifest(
    version="SK-B3/v1",
    leaves=_SKB3_PERMISSION_LEAVES,
    preset_grants=(
        ("Full Control", _SKB3_PERMISSION_LEAVES),
        (
            "Spec",
            (
                GUIDELINE_REVISIONS_READ,
                GUIDELINE_REVISIONS_CREATE,
                GUIDELINE_METRICS_AUTHOR,
                GUIDELINE_IMPACT_PREVIEW,
                GUIDELINE_ADOPTION_MANAGE,
                GUIDELINE_ASSESSMENTS_READ,
                GUIDELINE_ASSESSMENTS_RECORD,
                "guidelines.waiver.read",
                "guidelines.waiver.request",
            ),
        ),
        (
            "Validator",
            (
                GUIDELINE_REVISIONS_READ,
                GUIDELINE_IMPACT_PREVIEW,
                GUIDELINE_ASSESSMENTS_READ,
                GUIDELINE_ASSESSMENTS_RECORD,
                "guidelines.waiver.read",
                "guidelines.waiver.review",
                "guidelines.waiver.revalidate",
            ),
        ),
        (
            "QA",
            (
                GUIDELINE_REVISIONS_READ,
                GUIDELINE_ASSESSMENTS_READ,
                GUIDELINE_ASSESSMENTS_RECORD,
                "guidelines.waiver.read",
                "guidelines.waiver.request",
            ),
        ),
        (
            "Reporter",
            (
                GUIDELINE_REVISIONS_READ,
                GUIDELINE_ASSESSMENTS_READ,
                "guidelines.waiver.read",
            ),
        ),
        (
            "Sprint Manager",
            (
                GUIDELINE_REVISIONS_READ,
                GUIDELINE_IMPACT_PREVIEW,
                GUIDELINE_ASSESSMENTS_READ,
                GUIDELINE_ASSESSMENTS_RECORD,
                "guidelines.waiver.read",
                "guidelines.waiver.request",
            ),
        ),
        (
            "Executor",
            (
                GUIDELINE_REVISIONS_READ,
                GUIDELINE_ASSESSMENTS_READ,
                GUIDELINE_ASSESSMENTS_RECORD,
                "guidelines.waiver.read",
                "guidelines.waiver.request",
            ),
        ),
    ),
    historical_authorities=_SKB3_HISTORICAL_AUTHORITIES,
)


_BUILTIN_PRESET_NAMES: tuple[str, ...] = (
    "Full Control",
    "Executor",
    "Validator",
    "QA",
    "Reporter",
    "Sprint Manager",
    "Spec",
)


def _explicit_preset_grants(
    leaves: tuple[str, ...],
    grants: Mapping[str, tuple[str, ...]],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Materialize an explicit grant row for every built-in preset."""

    unknown = set(grants) - set(_BUILTIN_PRESET_NAMES)
    if unknown:
        raise PermissionContractViolation(
            f"unknown built-in presets in introduction manifest: {sorted(unknown)}"
        )
    return tuple(
        (
            name,
            leaves if name == "Full Control" else tuple(grants.get(name, ())),
        )
        for name in _BUILTIN_PRESET_NAMES
    )


_ADMIN_CATALOG_PERMISSION_LEAVES: tuple[str, ...] = (
    "agent.entity.read",
    "agent.entity.create",
    "agent.entity.edit",
    "agent.entity.delete",
    "agent.api_key.rotate",
    "agent.board_access.read",
    "agent.board_access.grant",
    "agent.board_access.edit",
    "agent.board_access.revoke",
    "board.admin.create",
    "board.admin.edit",
    "board.admin.delete",
    "board.share.read",
    "board.share.create",
    "board.share.edit",
    "board.share.revoke",
    "board.share.leave",
    "permission_preset.entity.read",
    "permission_preset.entity.create",
    "permission_preset.entity.edit",
    "permission_preset.entity.delete",
    "permission_preset.clone",
    "permission_preset.import",
    "permission_preset.export",
    "default_board_config.read",
    "default_board_config.diff_read",
    "default_board_config.candidates_read",
    "default_board_config.export",
    "default_board_config.create",
    "default_board_config.activate",
    "default_board_config.deactivate",
    "default_board_config.import",
    "default_board_config.set_design_system",
    "default_board_config.guidelines.edit",
    "design_system.entity.read",
    "design_system.entity.create",
    "design_system.entity.edit",
    "design_system.entity.delete",
    "design_system.import",
    "design_system.export",
    "design_system.board_link.read",
    "design_system.board_link.create",
    "design_system.board_link.delete",
)

_ADMIN_CATALOG_READ_GRANTS: tuple[str, ...] = (
    "default_board_config.read",
    "default_board_config.diff_read",
    "default_board_config.candidates_read",
    "default_board_config.export",
    "design_system.entity.read",
    "design_system.export",
    "design_system.board_link.read",
)

ADMIN_CATALOG_PERMISSION_INTRODUCTION_V1 = PermissionIntroductionManifest(
    version="ADMIN-CATALOG/v1",
    leaves=_ADMIN_CATALOG_PERMISSION_LEAVES,
    preset_grants=_explicit_preset_grants(
        _ADMIN_CATALOG_PERMISSION_LEAVES,
        {
            "Executor": _ADMIN_CATALOG_READ_GRANTS,
            "Validator": _ADMIN_CATALOG_READ_GRANTS,
            "QA": _ADMIN_CATALOG_READ_GRANTS,
            "Reporter": _ADMIN_CATALOG_READ_GRANTS,
            "Sprint Manager": _ADMIN_CATALOG_READ_GRANTS,
            "Spec": (
                *_ADMIN_CATALOG_READ_GRANTS,
                "default_board_config.create",
                "default_board_config.activate",
                "default_board_config.deactivate",
                "default_board_config.import",
                "default_board_config.set_design_system",
                "default_board_config.guidelines.edit",
                "design_system.entity.create",
                "design_system.entity.edit",
                "design_system.entity.delete",
                "design_system.import",
                "design_system.board_link.create",
                "design_system.board_link.delete",
            ),
        },
    ),
    historical_authorities=(
        ("agent.entity.read", "board.read"),
        ("agent.entity.create", "profile.update"),
        ("agent.entity.edit", "profile.update"),
        ("agent.entity.delete", "profile.update"),
        ("agent.api_key.rotate", "profile.update"),
        ("agent.board_access.read", "board.read"),
        ("agent.board_access.grant", "board.read"),
        ("agent.board_access.edit", "board.read"),
        ("agent.board_access.revoke", "board.read"),
        ("board.admin.create", "board.read"),
        ("board.admin.edit", "board.read"),
        ("board.admin.delete", "board.read"),
        ("board.share.read", "board.read"),
        ("board.share.create", "board.read"),
        ("board.share.edit", "board.read"),
        ("board.share.revoke", "board.read"),
        ("board.share.leave", "board.read"),
        ("permission_preset.entity.read", "board.read"),
        ("permission_preset.entity.create", "profile.update"),
        ("permission_preset.entity.edit", "profile.update"),
        ("permission_preset.entity.delete", "profile.update"),
        ("permission_preset.clone", "profile.update"),
        ("permission_preset.import", "profile.update"),
        ("permission_preset.export", "board.read"),
        ("default_board_config.read", "board.read"),
        ("default_board_config.diff_read", "board.read"),
        ("default_board_config.candidates_read", "board.read"),
        ("default_board_config.export", "board.read"),
        ("default_board_config.create", "spec.entity.edit_fields"),
        ("default_board_config.activate", "spec.entity.edit_fields"),
        ("default_board_config.deactivate", "spec.entity.edit_fields"),
        ("default_board_config.import", "spec.entity.edit_fields"),
        ("default_board_config.set_design_system", "spec.entity.edit_fields"),
        ("default_board_config.guidelines.edit", "guidelines.adoption.manage"),
        ("design_system.entity.read", "board.read"),
        ("design_system.entity.create", "spec.architecture.create"),
        ("design_system.entity.edit", "spec.architecture.edit"),
        ("design_system.entity.delete", "spec.architecture.delete"),
        ("design_system.import", "spec.architecture.import"),
        ("design_system.export", "spec.architecture.read"),
        ("design_system.board_link.read", "board.read"),
        ("design_system.board_link.create", "spec.architecture.edit"),
        ("design_system.board_link.delete", "spec.architecture.edit"),
    ),
)


_OPERATIONAL_PERMISSION_LEAVES: tuple[str, ...] = (
    "runtime.settings.read",
    "runtime.settings.write",
    "metrics.local.summary.read",
    "metrics.publish_health.read",
    "metrics.local.events.create",
    "metrics.settings.edit",
    "metrics.settings.migration_notice_seen",
    "metrics.local.export",
    "metrics.local.purge",
    "amendment.revision.read",
    "amendment.revision.create",
    "amendment.revision.associate",
    "amendment.revision.transition",
    "amendment.coverage.confirm",
)

_OPERATIONAL_READ_GRANTS: tuple[str, ...] = (
    "metrics.local.summary.read",
    "metrics.publish_health.read",
    "amendment.revision.read",
)

OPERATIONAL_PERMISSION_INTRODUCTION_V1 = PermissionIntroductionManifest(
    version="OPERATIONAL/v1",
    leaves=_OPERATIONAL_PERMISSION_LEAVES,
    preset_grants=_explicit_preset_grants(
        _OPERATIONAL_PERMISSION_LEAVES,
        {
            "Executor": (
                *_OPERATIONAL_READ_GRANTS,
                "amendment.revision.create",
                "amendment.revision.associate",
                "amendment.revision.transition",
            ),
            "Validator": (
                *_OPERATIONAL_READ_GRANTS,
                "amendment.coverage.confirm",
            ),
            "QA": _OPERATIONAL_READ_GRANTS,
            "Reporter": _OPERATIONAL_READ_GRANTS,
            "Sprint Manager": _OPERATIONAL_READ_GRANTS,
            "Spec": _OPERATIONAL_READ_GRANTS,
        },
    ),
    historical_authorities=(
        ("runtime.settings.read", "kg.admin.settings_read"),
        ("runtime.settings.write", "kg.admin.settings_write"),
        ("metrics.local.summary.read", "board.read"),
        ("metrics.publish_health.read", "board.read"),
        ("metrics.local.events.create", "board.analytics_read"),
        ("metrics.settings.edit", "board.analytics_read"),
        ("metrics.settings.migration_notice_seen", "board.analytics_read"),
        ("metrics.local.export", "board.analytics_read"),
        ("metrics.local.purge", "board.analytics_read"),
        ("amendment.revision.read", "card.entity.read"),
        ("amendment.revision.create", "card.entity.edit_bug_fields"),
        ("amendment.revision.associate", "card.entity.edit_bug_fields"),
        ("amendment.revision.transition", "card.entity.edit_bug_fields"),
        ("amendment.coverage.confirm", "card.validation.submit"),
    ),
)


_MCP_GAPS_PERMISSION_LEAVES: tuple[str, ...] = (
    "ideation.knowledge.read",
    "ideation.knowledge.create",
    "ideation.knowledge.delete",
    "ideation.qa.delete",
    "refinement.qa.delete",
    "spec.qa.delete",
    "sprint.qa.delete",
    "spec.tests.execute",
    "spec.tests.edit",
    "spec.tests.delete",
    "story.mockups.read",
    "story.mockups.create",
    "story.mockups.edit",
    "story.mockups.delete",
    "story.mockups.annotate",
    "sprint.tasks.assign",
    "test_scenario.interact_in.draft",
    "test_scenario.interact_in.ready",
    "test_scenario.interact_in.automated",
    "test_scenario.interact_in.passed",
    "test_scenario.interact_in.failed",
)

MCP_GAPS_PERMISSION_INTRODUCTION_V1 = PermissionIntroductionManifest(
    version="MCP-GAPS/v1",
    leaves=_MCP_GAPS_PERMISSION_LEAVES,
    preset_grants=_explicit_preset_grants(
        _MCP_GAPS_PERMISSION_LEAVES,
        {
            "Executor": (
                "story.mockups.read",
            ),
            "Validator": (
                "ideation.knowledge.read",
                "story.mockups.read",
            ),
            "QA": (
                "ideation.knowledge.read",
                "spec.tests.execute",
                "spec.tests.edit",
                "spec.tests.delete",
                "story.mockups.read",
                "test_scenario.interact_in.draft",
                "test_scenario.interact_in.ready",
                "test_scenario.interact_in.automated",
                "test_scenario.interact_in.passed",
                "test_scenario.interact_in.failed",
            ),
            "Reporter": (
                "ideation.knowledge.read",
                "story.mockups.read",
            ),
            "Sprint Manager": (
                "ideation.knowledge.read",
                "story.mockups.read",
                "sprint.tasks.assign",
            ),
            "Spec": (
                "ideation.knowledge.read",
                "ideation.knowledge.create",
                "ideation.knowledge.delete",
                "spec.tests.execute",
                "spec.tests.edit",
                "spec.tests.delete",
                "story.mockups.read",
                "story.mockups.create",
                "story.mockups.edit",
                "story.mockups.delete",
                "story.mockups.annotate",
                "test_scenario.interact_in.draft",
                "test_scenario.interact_in.ready",
                "test_scenario.interact_in.automated",
                "test_scenario.interact_in.passed",
                "test_scenario.interact_in.failed",
            ),
        },
    ),
    historical_authorities=(
        ("ideation.knowledge.read", "ideation.entity.read"),
        ("ideation.knowledge.create", "ideation.entity.edit_fields"),
        ("ideation.knowledge.delete", "ideation.entity.edit_fields"),
        ("ideation.qa.delete", "ideation.qa.answer"),
        ("refinement.qa.delete", "refinement.qa.answer"),
        ("spec.qa.delete", "spec.qa.answer"),
        ("sprint.qa.delete", "sprint.qa.answer"),
        ("spec.tests.execute", "spec.tests.update_status"),
        ("spec.tests.edit", "spec.tests.create"),
        ("spec.tests.delete", "spec.tests.create"),
        ("story.mockups.read", "story.entity.read"),
        ("story.mockups.create", "story.entity.edit_fields"),
        ("story.mockups.edit", "story.entity.edit_fields"),
        ("story.mockups.delete", "story.entity.edit_fields"),
        ("story.mockups.annotate", "story.entity.edit_fields"),
        ("sprint.tasks.assign", "sprint.entity.assign"),
        ("test_scenario.interact_in.draft", "spec.tests.update_status"),
        ("test_scenario.interact_in.ready", "spec.tests.update_status"),
        ("test_scenario.interact_in.automated", "spec.tests.update_status"),
        ("test_scenario.interact_in.passed", "spec.tests.update_status"),
        ("test_scenario.interact_in.failed", "spec.tests.update_status"),
    ),
)


_KG_OPERATIONS_PERMISSION_LEAVES: tuple[str, ...] = (
    "kg.operations.health.read",
    "kg.operations.integrity.read",
    "kg.operations.integrity.reconcile",
    "kg.operations.integrity.backfill",
    "kg.operations.cognitive.read",
    "kg.operations.cognitive.skip",
    "kg.operations.cognitive.clear",
    "kg.operations.queue.read",
    "kg.operations.queue.reprocess",
    "kg.operations.audit.read",
    "kg.operations.schema.migrate",
    "kg.operations.tick.run",
    "kg.operations.global_outbox.read",
    "kg.operations.global_outbox.reprocess",
    "kg.operations.global_outbox.verify",
    "kg.operations.global_recovery.preflight",
    "kg.operations.global_recovery.confirm",
    "kg.operations.global_recovery.read",
    "kg.operations.global_recovery.cancel",
    "kg.operations.global_recovery.resume",
    "kg.operations.global_recovery.run",
    "kg.operations.historical.read",
    "kg.operations.historical.start",
    "kg.operations.historical.cancel",
    "kg.operations.node.boost",
    "kg.operations.settings.read",
    "kg.operations.settings.write",
    "kg.operations.rebuild.preflight",
    "kg.operations.rebuild.confirm",
    "kg.operations.rebuild.run",
    "kg.operations.quarantine.restore",
    "kg.operations.board.erase",
)

KG_OPERATIONS_PERMISSION_INTRODUCTION_V1 = PermissionIntroductionManifest(
    version="KG-OPERATIONS/v1",
    leaves=_KG_OPERATIONS_PERMISSION_LEAVES,
    preset_grants=_explicit_preset_grants(_KG_OPERATIONS_PERMISSION_LEAVES, {}),
    historical_authorities=(
        ("kg.operations.health.read", "kg.admin.settings_read"),
        ("kg.operations.integrity.read", "kg.admin.settings_read"),
        ("kg.operations.integrity.reconcile", "kg.admin.settings_write"),
        ("kg.operations.integrity.backfill", "kg.admin.settings_write"),
        ("kg.operations.cognitive.read", "kg.admin.settings_read"),
        ("kg.operations.cognitive.skip", "kg.admin.settings_write"),
        ("kg.operations.cognitive.clear", "kg.admin.settings_write"),
        ("kg.operations.queue.read", "kg.admin.settings_read"),
        ("kg.operations.queue.reprocess", "kg.admin.settings_write"),
        ("kg.operations.audit.read", "kg.admin.settings_read"),
        ("kg.operations.schema.migrate", "kg.admin.settings_write"),
        ("kg.operations.tick.run", "kg.admin.settings_write"),
        ("kg.operations.global_outbox.read", "kg.admin.settings_read"),
        ("kg.operations.global_outbox.reprocess", "kg.admin.settings_write"),
        ("kg.operations.global_outbox.verify", "kg.admin.settings_read"),
        ("kg.operations.global_recovery.preflight", "kg.admin.settings_read"),
        ("kg.operations.global_recovery.confirm", "kg.admin.settings_write"),
        ("kg.operations.global_recovery.read", "kg.admin.settings_read"),
        ("kg.operations.global_recovery.cancel", "kg.admin.settings_write"),
        ("kg.operations.global_recovery.resume", "kg.admin.settings_write"),
        ("kg.operations.global_recovery.run", "kg.admin.settings_write"),
        (
            "kg.operations.historical.read",
            "kg.admin.historical_consolidation",
        ),
        (
            "kg.operations.historical.start",
            "kg.admin.historical_consolidation",
        ),
        (
            "kg.operations.historical.cancel",
            "kg.admin.historical_consolidation",
        ),
        ("kg.operations.node.boost", "kg.admin.settings_write"),
        ("kg.operations.settings.read", "kg.admin.settings_read"),
        ("kg.operations.settings.write", "kg.admin.settings_write"),
        ("kg.operations.rebuild.preflight", "kg.admin.settings_read"),
        ("kg.operations.rebuild.confirm", "kg.admin.settings_write"),
        ("kg.operations.rebuild.run", "kg.admin.settings_write"),
        ("kg.operations.quarantine.restore", "kg.admin.settings_write"),
        ("kg.operations.board.erase", "kg.admin.wipe_board"),
    ),
)


# These exact leaves existed before the lifecycle registry became the source of
# truth.  They remain ordinary historical permissions.  Only newly projected
# edges belong to the fail-closed introduction generation; mixing both sets in
# one manifest makes a materialized pre-upgrade snapshot look like a partial
# explicit deny document.
_PRE_REGISTRY_TRANSITION_PERMISSION_LEAVES = frozenset(
    {
        "card.move.in_progress_to_done",
        "card.move.in_progress_to_on_hold",
        "card.move.in_progress_to_validation",
        "card.move.not_started_to_started",
        "card.move.on_hold_to_in_progress",
        "card.move.started_to_in_progress",
        "card.move.validation_to_cancelled",
        "card.move.validation_to_done",
        "card.move.validation_to_on_hold",
        "refinement.move.approved_to_done",
        "refinement.move.review_to_approved",
        "spec.move.approved_to_draft",
        "spec.move.approved_to_validated",
        "spec.move.draft_to_review",
        "spec.move.in_progress_to_done",
        "spec.move.review_to_approved",
        "spec.move.validated_to_draft",
        "spec.move.validated_to_in_progress",
        "sprint.move.active_to_review",
        "sprint.move.draft_to_active",
        "sprint.move.review_to_closed",
        "story.move.draft_to_ready",
        "story.move.draft_to_triage",
        "story.move.ready_to_triage",
        "story.move.triage_to_draft",
        "story.move.triage_to_ready",
    }
)
_RETIRED_TRANSITION_PERMISSION_LEAVES: tuple[str, ...] = (
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
)
_RETIRED_STATE_PERMISSION_LEAVES: tuple[str, ...] = (
    "ideation.interact_in.refined",
    "refinement.interact_in.in_progress",
)
if not _PRE_REGISTRY_TRANSITION_PERMISSION_LEAVES <= set(
    transition_permission_flags()
):
    raise PermissionContractViolation(
        "historical transition fingerprint is not present in SDLC_REGISTRY"
    )

_NEW_SDLC_TRANSITION_PERMISSION_LEAVES: tuple[str, ...] = tuple(
    flag
    for flag in transition_permission_flags()
    if flag not in _PRE_REGISTRY_TRANSITION_PERMISSION_LEAVES
)
_NEW_SDLC_STATE_PERMISSION_LEAVES: tuple[str, ...] = (
    "ideation.interact_in.review",
    "ideation.interact_in.approved",
)
_SDLC_TRANSITION_PERMISSION_LEAVES: tuple[str, ...] = (
    *_NEW_SDLC_TRANSITION_PERMISSION_LEAVES,
    *_NEW_SDLC_STATE_PERMISSION_LEAVES,
)


def _transition_subset(
    entity: str,
    *edges: tuple[str, str],
) -> tuple[str, ...]:
    available = set(transition_permission_flags(entity))
    selected = tuple(
        f"{entity}.move.{current}_to_{target}" for current, target in edges
    )
    if not set(selected) <= available:
        raise PermissionContractViolation(
            f"transition preset references unregistered {entity} edges: "
            f"{sorted(set(selected) - available)}"
        )
    return selected


def _transitions_to(entity: str, target: str) -> tuple[str, ...]:
    suffix = f"_to_{target}"
    return tuple(
        flag for flag in transition_permission_flags(entity) if flag.endswith(suffix)
    )


_EXECUTOR_TRANSITION_GRANTS = (
    *_transition_subset(
        "card",
        ("not_started", "started"),
        ("started", "in_progress"),
        ("in_progress", "on_hold"),
        ("on_hold", "in_progress"),
        ("in_progress", "validation"),
    ),
    *_transitions_to("card", "cancelled"),
)

_VALIDATOR_TRANSITION_GRANTS = (
    *_transition_subset(
        "spec",
        ("approved", "validated"),
        ("validated", "in_progress"),
        ("in_progress", "done"),
        ("approved", "draft"),
        ("validated", "draft"),
    ),
    *_transition_subset(
        "sprint",
        ("active", "review"),
        ("review", "closed"),
    ),
    *_transition_subset(
        "card",
        ("validation", "done"),
        ("validation", "in_progress"),
    ),
)

_QA_TRANSITION_GRANTS = (
    *_transition_subset(
        "card",
        ("not_started", "started"),
        ("not_started", "in_progress"),
        ("started", "in_progress"),
        ("in_progress", "on_hold"),
        ("on_hold", "in_progress"),
        ("in_progress", "done"),
    ),
    *_transitions_to("card", "cancelled"),
    *transition_permission_flags("test_scenario"),
)

_SPEC_TRANSITION_GRANTS = (
    *transition_permission_flags("story"),
    *transition_permission_flags("ideation"),
    *transition_permission_flags("refinement"),
    *_transition_subset(
        "spec",
        ("draft", "review"),
        ("review", "draft"),
        ("review", "approved"),
        ("approved", "review"),
    ),
    *_transitions_to("spec", "cancelled"),
    *_transition_subset(
        "sprint",
        ("draft", "active"),
        ("active", "review"),
    ),
    *_transitions_to("sprint", "cancelled"),
    *transition_permission_flags("test_scenario"),
)

_TRANSITION_HISTORICAL_AUTHORITY = {
    "story": "story.entity.read",
    "ideation": "ideation.entity.read",
    "refinement": "refinement.entity.read",
    "spec": "spec.entity.read",
    "card": "card.entity.read",
    "sprint": "sprint.entity.read",
    "test_scenario": "spec.tests.update_status",
}


def _introduced_sdlc_grants(*flags: str) -> tuple[str, ...]:
    introduced = set(_SDLC_TRANSITION_PERMISSION_LEAVES)
    return tuple(flag for flag in flags if flag in introduced)

SDLC_TRANSITION_PERMISSION_INTRODUCTION_V1 = PermissionIntroductionManifest(
    version="SDLC-TRANSITIONS/v1",
    leaves=_SDLC_TRANSITION_PERMISSION_LEAVES,
    preset_grants=_explicit_preset_grants(
        _SDLC_TRANSITION_PERMISSION_LEAVES,
        {
            "Executor": _introduced_sdlc_grants(*_EXECUTOR_TRANSITION_GRANTS),
            "Validator": _introduced_sdlc_grants(*_VALIDATOR_TRANSITION_GRANTS),
            "QA": _introduced_sdlc_grants(*_QA_TRANSITION_GRANTS),
            "Reporter": (),
            "Sprint Manager": _introduced_sdlc_grants(
                *transition_permission_flags("sprint")
            ),
            "Spec": _introduced_sdlc_grants(
                *_SPEC_TRANSITION_GRANTS,
                *_NEW_SDLC_STATE_PERMISSION_LEAVES,
            ),
        },
    ),
    historical_authorities=tuple(
        (
            leaf,
            (
                "ideation.entity.read"
                if leaf in _NEW_SDLC_STATE_PERMISSION_LEAVES
                else _TRANSITION_HISTORICAL_AUTHORITY[leaf.split(".", 1)[0]]
            ),
        )
        for leaf in _SDLC_TRANSITION_PERMISSION_LEAVES
    ),
)


# Ordered oldest-to-newest.  Upgrade and normalization logic depends on this
# order so that each introduction generation is classified independently.
PERMISSION_INTRODUCTION_MANIFESTS: tuple[PermissionIntroductionManifest, ...] = (
    SKA_PERMISSION_INTRODUCTION_V1,
    SKB3_PERMISSION_INTRODUCTION_V1,
    ADMIN_CATALOG_PERMISSION_INTRODUCTION_V1,
    OPERATIONAL_PERMISSION_INTRODUCTION_V1,
    MCP_GAPS_PERMISSION_INTRODUCTION_V1,
    KG_OPERATIONS_PERMISSION_INTRODUCTION_V1,
    SDLC_TRANSITION_PERMISSION_INTRODUCTION_V1,
)


def _permission_introduction_manifest_for(
    permission: str,
) -> PermissionIntroductionManifest | None:
    for manifest in PERMISSION_INTRODUCTION_MANIFESTS:
        if permission in manifest.leaves:
            return manifest
    return None


def _introduced_historical_authority(permission: str) -> str | None:
    manifest = _permission_introduction_manifest_for(permission)
    if manifest is None:
        return None
    return manifest.historical_authority_for(permission)


_INTRODUCED_PERMISSION_LEAVES: tuple[str, ...] = tuple(
    leaf for manifest in PERMISSION_INTRODUCTION_MANIFESTS for leaf in manifest.leaves
)

if len(set(_INTRODUCED_PERMISSION_LEAVES)) != len(_INTRODUCED_PERMISSION_LEAVES):
    raise PermissionContractViolation(
        "permission introduction manifests must not repeat leaves"
    )
if len({manifest.version for manifest in PERMISSION_INTRODUCTION_MANIFESTS}) != len(
    PERMISSION_INTRODUCTION_MANIFESTS
):
    raise PermissionContractViolation(
        "permission introduction manifest versions must be unique"
    )

_FAIL_CLOSED_INTRODUCED_FLAGS = frozenset(_INTRODUCED_PERMISSION_LEAVES)

# SK-A/SK-B were introduced as strict governance boundaries and deliberately
# never accept flat-token fallbacks.  The later catalog and SDLC generations
# are a staged migration of actions that already existed behind flat MCP
# permissions.  Their use cases name the one accepted legacy token explicitly;
# retaining that narrow fallback lets old agents keep working while persisted
# permission documents are reconciled to the new canonical leaves.
_LEGACY_COMPATIBLE_INTRODUCED_FLAGS = frozenset(
    leaf
    for manifest in PERMISSION_INTRODUCTION_MANIFESTS[2:]
    for leaf in manifest.leaves
)


@dataclass(frozen=True)
class PermissionContext:
    """Edition-neutral input to a permission decision.

    ``permissions=None`` retains the historical trusted/local behavior.  The
    optional entity and state fields activate the same state-aware gate used by
    ``PermissionSet.check_with_state``.  Metadata is bounded context only and
    must not contain adapter or transport objects.
    """

    operation: str
    permissions: PermissionSet | tuple[str, ...] | list[str] | None = None
    entity: str | None = None
    state: str | None = None
    legacy_operation: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PermissionDecision:
    """Deterministic, transport-free result of evaluating a permission."""

    allowed: bool
    required_permission: str
    reason: str | None = None

    @classmethod
    def allow(cls, required_permission: str) -> PermissionDecision:
        return cls(allowed=True, required_permission=required_permission)

    @classmethod
    def deny(cls, required_permission: str, reason: str) -> PermissionDecision:
        return cls(
            allowed=False,
            required_permission=required_permission,
            reason=reason,
        )


STRUCTURED_SPEC_ENTITY_TYPES: tuple[str, ...] = (
    "functional_requirement",
    "business_rule",
    "technical_requirement",
    "decision",
    "acceptance_criterion",
    "api_contract",
    "integration_requirement",
    "observability_requirement",
)

STRUCTURED_SPEC_ENTITY_OPERATIONS: tuple[str, ...] = (
    "create",
    "update",
    "revoke",
    "supersede",
    "restore",
    "reorder",
    "link_task",
    "unlink_task",
)


def _structured_spec_entity_registry() -> dict[str, dict[str, bool]]:
    return {
        entity_type: {
            operation: True for operation in STRUCTURED_SPEC_ENTITY_OPERATIONS
        }
        for entity_type in STRUCTURED_SPEC_ENTITY_TYPES
    }


def structured_spec_entity_permission_flags() -> list[str]:
    return [
        f"spec.structured_entity.{entity_type}.{operation}"
        for entity_type in STRUCTURED_SPEC_ENTITY_TYPES
        for operation in STRUCTURED_SPEC_ENTITY_OPERATIONS
    ]


# ---------------------------------------------------------------------------
# Legacy flat permissions (kept for backward compat during migration)
# ---------------------------------------------------------------------------


class Permissions:
    """Permission constants for agent access control (legacy flat model)."""

    # Board
    BOARD_READ = "board:read"

    # Cards
    CARDS_CREATE = "cards:create"
    CARDS_UPDATE = "cards:update"
    CARDS_DELETE = "cards:delete"
    CARDS_MOVE = "cards:move"

    # Comments
    COMMENTS_CREATE = "comments:create"
    COMMENTS_UPDATE = "comments:update"
    COMMENTS_DELETE = "comments:delete"

    # Q&A
    QA_CREATE = "qa:create"
    QA_ANSWER = "qa:answer"
    QA_DELETE = "qa:delete"

    # Specs
    SPECS_CREATE = "specs:create"
    SPECS_UPDATE = "specs:update"
    SPECS_DELETE = "specs:delete"
    SPECS_MOVE = "specs:move"
    SPECS_EVALUATE = "specs:evaluate"

    # Attachments
    ATTACHMENTS_UPLOAD = "attachments:upload"
    ATTACHMENTS_DELETE = "attachments:delete"

    # Self
    SELF_UPDATE = "self:update"

    ALL = [
        BOARD_READ,
        CARDS_CREATE,
        CARDS_UPDATE,
        CARDS_DELETE,
        CARDS_MOVE,
        SPECS_CREATE,
        SPECS_UPDATE,
        SPECS_DELETE,
        SPECS_MOVE,
        SPECS_EVALUATE,
        COMMENTS_CREATE,
        COMMENTS_UPDATE,
        COMMENTS_DELETE,
        QA_CREATE,
        QA_ANSWER,
        QA_DELETE,
        ATTACHMENTS_UPLOAD,
        ATTACHMENTS_DELETE,
        SELF_UPDATE,
    ]

    # Default permissions for new agents
    DEFAULT = [
        BOARD_READ,
        CARDS_CREATE,
        CARDS_UPDATE,
        CARDS_MOVE,
        SPECS_CREATE,
        SPECS_UPDATE,
        SPECS_MOVE,
        SPECS_EVALUATE,
        COMMENTS_CREATE,
        QA_CREATE,
        QA_ANSWER,
        ATTACHMENTS_UPLOAD,
        SELF_UPDATE,
    ]


# ---------------------------------------------------------------------------
# Granular permission registry (~190 flags)
# ---------------------------------------------------------------------------

PERMISSION_REGISTRY: dict[str, dict[str, Any]] = {
    # ---- Board & Context ----
    "board": {
        "read": True,
        "activity_read": True,
        "analytics_read": True,
        "mentions_read": True,
        "mentions_mark_seen": True,
        "admin": {
            "create": True,
            "edit": True,
            "delete": True,
        },
        "share": {
            "read": True,
            "create": True,
            "edit": True,
            "revoke": True,
            "leave": True,
        },
    },
    "agent": {
        "entity": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
        },
        "api_key": {"rotate": True},
        "board_access": {
            "read": True,
            "grant": True,
            "edit": True,
            "revoke": True,
        },
    },
    "permission_preset": {
        "entity": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
        },
        "clone": True,
        "import": True,
        "export": True,
    },
    "default_board_config": {
        "read": True,
        "diff_read": True,
        "candidates_read": True,
        "export": True,
        "create": True,
        "activate": True,
        "deactivate": True,
        "import": True,
        "set_design_system": True,
        "guidelines": {"edit": True},
    },
    "design_system": {
        "entity": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
        },
        "import": True,
        "export": True,
        "board_link": {
            "read": True,
            "create": True,
            "delete": True,
        },
    },
    "runtime": {"settings": {"read": True, "write": True}},
    "metrics": {
        "local": {
            "summary": {"read": True},
            "events": {"create": True},
            "export": True,
            "purge": True,
        },
        "publish_health": {"read": True},
        "settings": {
            "edit": True,
            "migration_notice_seen": True,
        },
    },
    "amendment": {
        "revision": {
            "read": True,
            "create": True,
            "associate": True,
            "transition": True,
        },
        "coverage": {"confirm": True},
    },
    "profile": {
        "update": True,
    },
    "guidelines": {
        "read": True,
        "create": True,
        "edit": True,
        "delete": True,
        "link": True,
        "unlink": True,
        "revisions": {
            "read": True,
            "create": True,
            "retire": True,
        },
        "metrics": {
            "author": True,
        },
        "impact": {
            "preview": True,
        },
        "adoption": {
            "manage": True,
        },
        "assessments": {
            "read": True,
            "record": True,
        },
        "waiver": {
            "read": True,
            "request": True,
            "review": True,
            "revoke": True,
            "revalidate": True,
        },
    },
    # ---- Stories & Topics ----
    "story": {
        "entity": {
            "read": True,
            "create": True,
            "edit_fields": True,
            "assign": True,
            "label": True,
            "archive": True,
            "restore": True,
            "delete": True,
        },
        "move": transition_permission_registry("story"),
        "interact_in": {
            **lifecycle_state_permission_registry("story"),
            # Archival is represented separately from StoryStatus but remains
            # a supported historical policy scope.
            "archived": True,
        },
        "links": {
            "ideation": True,
        },
        "mockups": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "annotate": True,
        },
        "conversion": {
            "to_ideation": True,
        },
        "history_read": True,
    },
    "topic": {
        "entity": {
            "read": True,
            "create": True,
            "edit_fields": True,
            "archive": True,
            "restore": True,
            "merge": True,
            "delete": True,
        },
    },
    # ---- Ideation ----
    "ideation": {
        "entity": {
            "read": True,
            "create": True,
            "edit_fields": True,
            "assign": True,
            "label": True,
            "evaluate": True,
            "archive": True,
            "restore": True,
            "delete": True,
        },
        "move": transition_permission_registry("ideation"),
        "interact_in": {
            **lifecycle_state_permission_registry("ideation"),
        },
        "qa": {
            "read": True,
            "ask": True,
            "ask_choice": True,
            "answer": True,
            "delete": True,
        },
        "mockups": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "annotate": True,
        },
        "architecture": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "import": True,
            "render": True,
        },
        "quality": {"read": True, "assess": True},
        "knowledge": {"read": True, "create": True, "delete": True},
        "specs_derive": True,
        "versions_read": True,
        "history_read": True,
    },
    # ---- Refinement ----
    "refinement": {
        "entity": {
            "read": True,
            "create": True,
            "edit_fields": True,
            "assign": True,
            "label": True,
            "archive": True,
            "restore": True,
            "delete": True,
        },
        "move": transition_permission_registry("refinement"),
        "interact_in": {
            **lifecycle_state_permission_registry("refinement"),
        },
        "qa": {
            "read": True,
            "ask": True,
            "ask_choice": True,
            "answer": True,
            "delete": True,
        },
        "mockups": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "annotate": True,
        },
        "architecture": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "import": True,
            "render": True,
        },
        "quality": {"read": True, "assess": True},
        "research_decisions": {"read": True, "append": True},
        "knowledge": {"read": True, "create": True, "delete": True},
        "specs_derive": True,
        "versions_read": True,
        "history_read": True,
    },
    # ---- Spec ----
    "spec": {
        "entity": {
            "read": True,
            "create": True,
            "edit_fields": True,
            "edit_coverage_flags": True,
            "assign": True,
            "label": True,
            "link_card": True,
            "archive": True,
            "restore": True,
            "delete": True,
        },
        "move": transition_permission_registry("spec"),
        "interact_in": lifecycle_state_permission_registry("spec"),
        "qa": {
            "read": True,
            "ask": True,
            "ask_choice": True,
            "answer": True,
            "delete": True,
        },
        "tests": {
            "read": True,
            "create": True,
            "execute": True,
            "edit": True,
            "delete": True,
            "update_status": True,
        },
        "rules": {"read": True, "create": True, "edit": True, "delete": True},
        "contracts": {"read": True, "create": True, "edit": True, "delete": True},
        "integration_requirements": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "link_task": True,
        },
        "observability_requirements": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "link_task": True,
        },
        "structured_entity": _structured_spec_entity_registry(),
        "mockups": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "annotate": True,
        },
        "architecture": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "import": True,
            "render": True,
        },
        "quality": {"read": True, "assess": True},
        "checklist": {"read": True, "execute": True},
        "knowledge": {"read": True, "create": True, "delete": True},
        "evaluations": {"read": True, "submit": True, "delete": True},
        # Spec Validation Gate — dedicated flags mirroring card.validation.
        # Different from spec.evaluations (which is the qualitative gate for
        # validated→in_progress). This is the approved→validated content gate.
        "validation": {"submit": True, "read": True, "delete": True},
        "cards_derive": True,
        "history_read": True,
    },
    "test_scenario": {
        "move": transition_permission_registry("test_scenario"),
        "interact_in": lifecycle_state_permission_registry("test_scenario"),
    },
    # ---- Sprint ----
    "sprint": {
        "entity": {
            "read": True,
            "create": True,
            "edit_fields": True,
            "edit_coverage_flags": True,
            "assign": True,
            "label": True,
            "archive": True,
            "restore": True,
            "delete": True,
        },
        "move": transition_permission_registry("sprint"),
        "interact_in": lifecycle_state_permission_registry("sprint"),
        "qa": {"read": True, "ask": True, "answer": True, "delete": True},
        "tasks": {"assign": True},
        "evaluations": {"read": True, "submit": True, "delete": True},
        "history_read": True,
    },
    # ---- Card ----
    "card": {
        "entity": {
            "read": True,
            "context_read": True,
            "create": True,
            "create_test": True,
            "edit_fields": True,
            "edit_bug_fields": True,
            "assign": True,
            "label": True,
            "link_spec": True,
            "link_tests": True,
            "manage_dependencies": True,
            "delete": True,
        },
        "copy_from_spec": {
            "mockups": True,
            "knowledge": True,
            "qa": True,
            "architecture": True,
        },
        "link_to": {
            "scenario": True,
            "tr": True,
            "rule": True,
            "contract": True,
            "ir": True,
            "or": True,
        },
        "move": transition_permission_registry("card"),
        "interact_in": lifecycle_state_permission_registry("card"),
        "validation": {
            "submit": True,
            "read": True,
            "delete": True,
        },
        "qa": {"read": True, "ask": True, "answer": True, "delete": True},
        "comments": {
            "read": True,
            "create": True,
            "create_choice": True,
            "respond_choice": True,
            "get_responses": True,
            "edit": True,
            "delete": True,
        },
        "attachments": {"read": True, "upload": True, "delete": True},
        "mockups": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "annotate": True,
        },
        "architecture": {
            "read": True,
            "create": True,
            "edit": True,
            "delete": True,
            "import": True,
            "render": True,
        },
        "tests": {"read": True, "link": True, "update_status": True},
        "conclusion": {"read": True, "write": True},
        "activity_read": True,
    },
    # ---- Knowledge Graph ----
    "kg": {
        "query": {
            "decision_history": True,
            "related_context": True,
            "supersedence_chain": True,
            "contradictions": True,
            "similar_decisions": True,
            "constraint_explain": True,
            "alternatives": True,
            "learning_from_bugs": True,
            "global": True,
        },
        "power": {
            "cypher": True,
            "natural": True,
            "schema_info": True,
        },
        "session": {
            "begin": True,
            "add_node": True,
            "add_edge": True,
            "get_similar": True,
            "propose": True,
            "commit": True,
            "abort": True,
        },
        "operations": {
            "health": {"read": True},
            "integrity": {
                "read": True,
                "reconcile": True,
                "backfill": True,
            },
            "cognitive": {
                "read": True,
                "skip": True,
                "clear": True,
            },
            "queue": {"read": True, "reprocess": True},
            "audit": {"read": True},
            "schema": {"migrate": True},
            "tick": {"run": True},
            "global_outbox": {
                "read": True,
                "reprocess": True,
                "verify": True,
            },
            "global_recovery": {
                "preflight": True,
                "confirm": True,
                "read": True,
                "cancel": True,
                "resume": True,
                "run": True,
            },
            "historical": {
                "read": True,
                "start": True,
                "cancel": True,
            },
            "node": {"boost": True},
            "settings": {"read": True, "write": True},
            "rebuild": {
                "preflight": True,
                "confirm": True,
                "run": True,
            },
            "quarantine": {"restore": True},
            "board": {"erase": True},
        },
        "admin": {
            "wipe_board": True,
            "settings_write": True,
            "settings_read": True,
            "historical_consolidation": True,
        },
    },
}


def _flatten_registry(d: dict[str, Any], prefix: str = "") -> list[str]:
    """Flatten nested registry dict into dot-separated flag names."""
    flags: list[str] = []
    for key, value in d.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flags.extend(_flatten_registry(value, path))
        else:
            flags.append(path)
    return flags


# All flag names as flat list (e.g., "spec.tests.create", "card.move.in_progress_to_done")
ALL_FLAGS: list[str] = _flatten_registry(PERMISSION_REGISTRY)


def _get_nested(d: dict[str, Any], path: str) -> Any:
    """Get value from nested dict by dot-separated path."""
    parts = path.split(".")
    current = d
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def _set_nested(d: dict[str, Any], path: str, value: Any) -> None:
    """Set value in nested dict by dot-separated path."""
    parts = path.split(".")
    current = d
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


# ---------------------------------------------------------------------------
# PermissionSet — resolved, board-scoped permissions
# ---------------------------------------------------------------------------


class PermissionSet:
    """Resolved permission flags for an agent on a specific board.

    Encapsulates the merged result of agent_flags ∩ board_overrides.
    Provides typed methods for checking permissions with state awareness.
    """

    def __init__(
        self,
        flags: dict[str, Any],
        preset_name: str | None = None,
        *,
        owner_review_required: bool = False,
        review_reason: str | None = None,
    ):
        self.flags = flags
        self.preset_name = preset_name
        self.owner_review_required = owner_review_required
        self.review_reason = review_reason

    def has(self, flag: str) -> bool:
        """Check if a specific flag is active.

        Historical flags absent from the dict still default to True for
        backward compatibility.  Leaves introduced by a versioned manifest
        are fail-closed and therefore default to False.
        """
        # A malformed persisted permission layer is an explicit governance
        # stop, not another backward-compatibility case.  Deny even unknown
        # extension paths while the owner-review signal is active.
        if self.owner_review_required:
            return False
        present, value = _permission_value_presence(self.flags, flag)
        if not present:
            enabled = flag not in _FAIL_CLOSED_INTRODUCED_FLAGS
        else:
            # Permission values are booleans, not merely truthy values.
            # Persisted data predating strict transport validation must deny
            # when a malformed scalar reaches the policy.
            enabled = value is True
        if not enabled:
            return False
        historical_authority = _introduced_historical_authority(flag)
        if historical_authority is not None:
            # The historical half of an introduced capability must also be
            # explicit.  It must not inherit the old absent=True behavior.
            return _get_nested(self.flags, historical_authority) is True
        return True

    def check(self, flag: str) -> str | None:
        """Check permission flag. Returns None if allowed, error dict as JSON if denied."""
        if self.has(flag):
            return None
        historical_authority = _introduced_historical_authority(flag)
        if (
            historical_authority is not None
            and _get_nested(self.flags, flag) is True
            and _get_nested(self.flags, historical_authority) is not True
        ):
            return _perm_error_detailed(
                reason="historical_authority_missing",
                required_permission=historical_authority,
                detail=(
                    f"The introduced permission '{flag}' also requires "
                    f"historical authority '{historical_authority}'."
                ),
            )
        return _perm_error_detailed(
            reason="permission_missing",
            required_permission=flag,
            detail=f"Agent does not have the '{flag}' permission.",
        )

    def can_interact_in(self, entity: str, status: str) -> bool:
        """Check if agent can interact with an entity in a given status."""
        flag = f"{entity}.interact_in.{status}"
        return self.has(flag)

    def check_with_state(self, flag: str, entity: str, status: str) -> str | None:
        """Check permission flag considering entity state.

        Read flags bypass interact_in. For all other actions, interact_in
        must be active for the current entity status.
        """
        # Read actions bypass interact_in
        is_read = flag.endswith(".read") or flag.endswith("_read")
        if not is_read:
            if not self.can_interact_in(entity, status):
                return _perm_error_detailed(
                    reason="interact_in_blocked",
                    required_permission=f"{entity}.interact_in.{status}",
                    current_state=status,
                    detail=(
                        f"Agent cannot interact with {entity} in '{status}' status. "
                        f"Required: {entity}.interact_in.{status}"
                    ),
                )
        # Check the action flag itself
        return self.check(flag)


# ---------------------------------------------------------------------------
# Permission resolution
# ---------------------------------------------------------------------------


def resolve_permissions(
    agent_flags: dict[str, Any] | None,
    preset_flags: dict[str, Any] | None,
    board_overrides: dict[str, Any] | None,
    *,
    owner_review_required: bool = False,
    review_reason: str | None = None,
) -> PermissionSet:
    """Resolve effective permissions: preset → agent customization → board override.

    Ceiling model: board_overrides can only restrict (AND), never expand.
    """
    import copy

    # A valid preset lineage is already resolved by the persistence adapter.
    # Complete historical missing flags as allowed, but keep introduced flags
    # denied.  Direct granular data without a preset follows the same
    # fail-closed introduction rule.  No data at all retains trusted/local
    # Full Control compatibility.
    malformed_reason: str | None = None
    agent_layer_valid = agent_flags is None or (
        isinstance(agent_flags, Mapping)
        and _canonical_permission_shape_is_valid(agent_flags)
        and _permission_document_has_boolean_leaves(agent_flags)
    )
    preset_layer_valid = preset_flags is None or (
        isinstance(preset_flags, Mapping)
        and _canonical_permission_shape_is_complete(preset_flags)
        and _permission_document_has_boolean_leaves(preset_flags)
    )

    if not agent_layer_valid:
        base = _fail_closed_permission_flags()
        malformed_reason = "invalid_agent_flags"
    elif not preset_layer_valid:
        base = _fail_closed_permission_flags()
        malformed_reason = "invalid_preset_flags"
    elif preset_flags is not None:
        base = copy.deepcopy(preset_flags)
    elif agent_flags is not None:
        base = _historical_compatibility_permission_flags()
    else:
        base = copy.deepcopy(PERMISSION_REGISTRY)

    # Apply agent-level customizations (override preset values)
    if malformed_reason is None and isinstance(agent_flags, Mapping):
        _apply_direct_permission_layer(base, agent_flags)

    # Apply board overrides (AND — can only restrict)
    if board_overrides is not None:
        board_overrides_valid = isinstance(
            board_overrides, Mapping
        ) and _canonical_permission_shape_is_valid(board_overrides)
        if board_overrides_valid:
            try:
                validate_strict_permission_flags(board_overrides)
            except PermissionContractViolation:
                board_overrides_valid = False

        if not board_overrides_valid:
            malformed_reason = malformed_reason or "invalid_board_overrides"
        else:
            for flag_path in _flatten_registry(board_overrides):
                override_value = _get_nested(board_overrides, flag_path)
                if override_value is False:
                    _set_nested(base, flag_path, False)
                # True in override does NOT expand — ceiling model

            # A materialized board ceiling must explicitly admit every introduced
            # permission.  An absent leaf is a denial, while a True ceiling still
            # cannot expand an already-denied agent/preset permission.
            for flag_path in _INTRODUCED_PERMISSION_LEAVES:
                if _get_nested(board_overrides, flag_path) is not True:
                    _set_nested(base, flag_path, False)

    if malformed_reason is not None:
        owner_review_required = True
        review_reason = malformed_reason
    if owner_review_required:
        _set_all_flags(base, False)

    return PermissionSet(
        base,
        owner_review_required=owner_review_required,
        review_reason=review_reason,
    )


# ---------------------------------------------------------------------------
# Legacy permission mapping (19 old → ~190 new)
# ---------------------------------------------------------------------------

LEGACY_PERMISSION_MAP: dict[str, list[str]] = {
    "board:read": [
        "board.read",
        "board.activity_read",
        "board.analytics_read",
        "board.mentions_read",
        "board.mentions_mark_seen",
        "story.entity.read",
        "story.history_read",
        "topic.entity.read",
        "ideation.architecture.read",
        "refinement.architecture.read",
        "spec.architecture.read",
        "card.architecture.read",
    ],
    "cards:create": [
        "card.entity.create",
        "card.entity.create_test",
    ],
    "cards:update": [
        "card.entity.edit_fields",
        "card.entity.edit_bug_fields",
        "card.entity.assign",
        "card.entity.label",
        "card.entity.link_spec",
        "card.entity.link_tests",
        "card.entity.manage_dependencies",
        "card.copy_from_spec.mockups",
        "card.copy_from_spec.knowledge",
        "card.copy_from_spec.qa",
        "card.copy_from_spec.architecture",
        "card.architecture.create",
        "card.architecture.edit",
        "card.architecture.delete",
        "card.architecture.import",
        "card.architecture.render",
        "card.link_to.scenario",
        "card.link_to.tr",
        "card.link_to.rule",
        "card.link_to.contract",
        "card.link_to.ir",
        "card.link_to.or",
    ],
    "cards:delete": ["card.entity.delete"],
    "cards:move": list(transition_permission_flags("card")),
    "specs:create": [
        "story.entity.create",
        "topic.entity.create",
        "spec.entity.create",
        "sprint.entity.create",
    ],
    "specs:update": [
        "story.entity.edit_fields",
        "story.entity.assign",
        "story.entity.label",
        "story.entity.archive",
        "story.entity.restore",
        "story.links.ideation",
        "story.conversion.to_ideation",
        "topic.entity.edit_fields",
        "topic.entity.archive",
        "topic.entity.restore",
        "topic.entity.merge",
        "spec.entity.edit_fields",
        "spec.entity.edit_coverage_flags",
        "spec.entity.assign",
        "spec.entity.label",
        "spec.entity.link_card",
        "spec.tests.create",
        "spec.tests.update_status",
        "spec.rules.create",
        "spec.rules.edit",
        "spec.rules.delete",
        "spec.contracts.create",
        "spec.contracts.edit",
        "spec.contracts.delete",
        "spec.integration_requirements.create",
        "spec.integration_requirements.edit",
        "spec.integration_requirements.delete",
        "spec.integration_requirements.link_task",
        "spec.observability_requirements.create",
        "spec.observability_requirements.edit",
        "spec.observability_requirements.delete",
        "spec.observability_requirements.link_task",
        "spec.mockups.create",
        "spec.mockups.edit",
        "spec.mockups.delete",
        "spec.mockups.annotate",
        "ideation.architecture.create",
        "ideation.architecture.edit",
        "ideation.architecture.delete",
        "ideation.architecture.import",
        "ideation.architecture.render",
        "refinement.architecture.create",
        "refinement.architecture.edit",
        "refinement.architecture.delete",
        "refinement.architecture.import",
        "refinement.architecture.render",
        "spec.architecture.create",
        "spec.architecture.edit",
        "spec.architecture.delete",
        "spec.architecture.import",
        "spec.architecture.render",
        "spec.knowledge.create",
        "spec.knowledge.delete",
        "spec.cards_derive",
    ]
    + structured_spec_entity_permission_flags(),
    "specs:delete": [
        "story.entity.delete",
        "topic.entity.delete",
        "spec.entity.delete",
    ],
    "specs:move": [
        *transition_permission_flags("story"),
        *transition_permission_flags("ideation"),
        *transition_permission_flags("refinement"),
        *transition_permission_flags("spec"),
        *transition_permission_flags("sprint"),
    ],
    "specs:evaluate": [
        "spec.evaluations.submit",
        "spec.evaluations.delete",
        # Spec Validation Gate — legacy agents with specs:evaluate also get
        # the new validation gate submit/read permissions automatically.
        "spec.validation.submit",
        "spec.validation.read",
        "sprint.evaluations.submit",
        "sprint.evaluations.delete",
    ],
    "comments:create": [
        "card.comments.create",
        "card.comments.create_choice",
        "card.comments.respond_choice",
    ],
    "comments:update": ["card.comments.edit"],
    "comments:delete": ["card.comments.delete"],
    "qa:create": [
        "card.qa.ask",
        "spec.qa.ask",
        "spec.qa.ask_choice",
        "ideation.qa.ask",
        "ideation.qa.ask_choice",
        "refinement.qa.ask",
        "refinement.qa.ask_choice",
        "sprint.qa.ask",
    ],
    "qa:answer": [
        "card.qa.answer",
        "spec.qa.answer",
        "ideation.qa.answer",
        "refinement.qa.answer",
        "sprint.qa.answer",
    ],
    "qa:delete": ["card.qa.delete"],
    "attachments:upload": ["card.attachments.upload"],
    "attachments:delete": ["card.attachments.delete"],
    "self:update": ["profile.update"],
}

# Use cases express their transitional authority as the pre-introduction
# canonical leaf (for example ``board.read``). Persisted permission documents
# can evaluate that leaf directly, while pre-migration MCP agents still carry
# flat tokens such as ``board:read``. Keep that compatibility translation in
# the policy itself so every inbound adapter gets the same bounded fallback.
_CANONICAL_TO_LEGACY_TOKENS: dict[str, tuple[str, ...]] = {
    canonical: tuple(
        legacy
        for legacy, mapped in LEGACY_PERMISSION_MAP.items()
        if canonical in mapped
    )
    for canonical in {
        canonical
        for mapped in LEGACY_PERMISSION_MAP.values()
        for canonical in mapped
    }
}


def map_legacy_permissions(old_permissions: list[str]) -> dict[str, Any]:
    """Map legacy flat permissions to new granular flag structure.

    Flags mapped from old permissions → True. All others → False.
    All interact_in flags → True (backward compat).
    All read flags → True (backward compat).
    """
    import copy

    # Start with all False
    flags = _set_all_flags(copy.deepcopy(PERMISSION_REGISTRY), False)

    # Enable all interact_in (backward compat — existing agents could interact in all states)
    for entity in ("story", "ideation", "refinement", "spec", "sprint", "card"):
        interact_in = flags.get(entity, {}).get("interact_in", {})
        if isinstance(interact_in, dict):
            for status in interact_in:
                interact_in[status] = True

    # Enable all read flags (backward compat)
    for flag_path in ALL_FLAGS:
        if flag_path.endswith(".read") or flag_path.endswith("_read"):
            _set_nested(flags, flag_path, True)

    # Map each legacy permission to new flags
    for old_perm in old_permissions:
        new_flags = LEGACY_PERMISSION_MAP.get(old_perm, [])
        for flag_path in new_flags:
            _set_nested(flags, flag_path, True)

    # The SK-A introduction has one deliberately bounded legacy bridge (RA2).
    # First erase any grant produced by broad compatibility, then re-apply
    # exactly the five context reads and the mutations backed by the two
    # historical authorities.  No skip/template/binding authority is created.
    for flag_path in _INTRODUCED_PERMISSION_LEAVES:
        _set_nested(flags, flag_path, False)
    for flag_path in _SKA_CONTEXT_READ_LEAVES:
        _set_nested(flags, flag_path, True)
    if "specs:update" in old_permissions:
        for flag_path in (
            "ideation.quality.assess",
            "refinement.quality.assess",
            "refinement.research_decisions.append",
            "spec.checklist.execute",
        ):
            _set_nested(flags, flag_path, True)
    if "specs:evaluate" in old_permissions:
        _set_nested(flags, "spec.quality.assess", True)
    if "cards:move" in old_permissions:
        for flag_path in transition_permission_flags("card"):
            _set_nested(flags, flag_path, True)
    if "specs:move" in old_permissions:
        for entity_type in ("story", "ideation", "refinement", "spec", "sprint"):
            for flag_path in transition_permission_flags(entity_type):
                _set_nested(flags, flag_path, True)
        for flag_path in _NEW_SDLC_STATE_PERMISSION_LEAVES:
            _set_nested(flags, flag_path, True)
    if "specs:update" in old_permissions:
        _set_nested(flags, "spec.tests.execute", True)
        for flag_path in transition_permission_flags("test_scenario"):
            _set_nested(flags, flag_path, True)
        for state in SDLC_REGISTRY["test_scenario"].status_enum:
            _set_nested(flags, f"test_scenario.interact_in.{state.value}", True)

    return flags


def _set_all_flags(d: dict[str, Any], value: bool) -> dict[str, Any]:
    """Set all leaf values in a nested dict to a specific value."""
    for key in d:
        if isinstance(d[key], dict):
            _set_all_flags(d[key], value)
        else:
            d[key] = value
    return d


def merge_missing_flags(
    stored: dict,
    registry: dict,
    *,
    _prefix: str = "",
) -> tuple[dict, int]:
    """Deep-merge missing permission keys while preserving existing values.

    Historical registry leaves retain the legacy default of True.  Leaves in
    a versioned introduction manifest are inserted as False.
    """
    added = 0
    for key, reg_val in registry.items():
        path = f"{_prefix}.{key}" if _prefix else key
        if key not in stored:
            if isinstance(reg_val, dict):
                import copy as _copy

                subtree = _copy.deepcopy(reg_val)
                _set_all_leaves(subtree, True)
                stored[key] = subtree
                for flag_path in _INTRODUCED_PERMISSION_LEAVES:
                    prefix = f"{path}."
                    if flag_path.startswith(prefix):
                        relative_path = flag_path[len(prefix) :]
                        _set_nested(stored[key], relative_path, False)
                added += _count_leaves(subtree)
            else:
                stored[key] = path not in _FAIL_CLOSED_INTRODUCED_FLAGS
                added += 1
        elif isinstance(reg_val, dict) and isinstance(stored[key], dict):
            _, sub_added = merge_missing_flags(
                stored[key],
                reg_val,
                _prefix=path,
            )
            added += sub_added
    return stored, added


def _set_all_leaves(d: dict, value: bool) -> None:
    for k, v in d.items():
        if isinstance(v, dict):
            _set_all_leaves(v, value)
        else:
            d[k] = value


def _count_leaves(d: dict) -> int:
    n = 0
    for v in d.values():
        if isinstance(v, dict):
            n += _count_leaves(v)
        else:
            n += 1
    return n


@dataclass(frozen=True)
class PermissionPresetLineageNode:
    """Persistence-neutral preset node used by the canonical lineage resolver."""

    id: str
    flags: Any
    base_preset_id: str | None = None

    def __post_init__(self) -> None:
        import copy

        if not self.id.strip():
            raise PermissionContractViolation("preset lineage id must not be empty")
        object.__setattr__(self, "flags", copy.deepcopy(self.flags))


@dataclass(frozen=True)
class PermissionPresetLineageResolution:
    """Resolved flags plus an explicit governance signal for invalid lineage."""

    flags: PermissionFlags
    owner_review_required: bool
    review_reason: str | None = None

    def __post_init__(self) -> None:
        import copy

        object.__setattr__(self, "flags", copy.deepcopy(self.flags))
        if self.owner_review_required and not self.review_reason:
            raise PermissionContractViolation(
                "owner review resolution requires a review reason"
            )
        if not self.owner_review_required and self.review_reason is not None:
            raise PermissionContractViolation(
                "valid lineage resolution cannot carry a review reason"
            )


def _canonical_permission_shape_is_valid(
    document: Any,
    canonical: Mapping[str, Any] = PERMISSION_REGISTRY,
) -> bool:
    if not isinstance(document, Mapping):
        return False
    for key, canonical_value in canonical.items():
        if key not in document:
            continue
        value = document[key]
        if isinstance(canonical_value, Mapping):
            if not isinstance(value, Mapping):
                return False
            if not _canonical_permission_shape_is_valid(value, canonical_value):
                return False
        elif not isinstance(value, bool):
            return False
    return True


def _canonical_permission_shape_is_complete(
    document: Any,
) -> bool:
    """Return whether every canonical leaf is explicitly materialized.

    ``preset_flags`` entering ``resolve_permissions`` must already be the
    output of the lineage resolver.  Treating a sparse preset delta as that
    output would resurrect the historical absent=True fallback and escalate a
    clone to Full Control.
    """

    if not _canonical_permission_shape_is_valid(document):
        return False
    return all(
        _permission_value_presence(document, path)[0]
        for path in _flatten_registry(PERMISSION_REGISTRY)
    )


def _permission_document_has_boolean_leaves(document: Mapping[str, Any]) -> bool:
    try:
        validate_strict_permission_flags(document)
    except PermissionContractViolation:
        return False
    return True


def _overlay_permission_flags(
    base: PermissionFlags,
    overrides: Mapping[str, Any],
) -> None:
    import copy

    for key, value in overrides.items():
        if isinstance(value, Mapping):
            existing = base.get(key)
            if not isinstance(existing, dict):
                existing = {}
                base[key] = existing
            _overlay_permission_flags(existing, value)
        else:
            base[key] = copy.deepcopy(value)


def _fail_closed_permission_flags() -> PermissionFlags:
    import copy

    return _set_all_flags(copy.deepcopy(PERMISSION_REGISTRY), False)


def _historical_compatibility_permission_flags() -> PermissionFlags:
    """Return historical absent=True with introduced leaves denied."""

    import copy

    flags = copy.deepcopy(PERMISSION_REGISTRY)
    for flag_path in _INTRODUCED_PERMISSION_LEAVES:
        _set_nested(flags, flag_path, False)
    return flags


def _apply_direct_permission_layer(
    base: PermissionFlags,
    overrides: Mapping[str, Any],
    canonical: Mapping[str, Any] = PERMISSION_REGISTRY,
) -> None:
    """Overlay direct flags while denying malformed canonical values."""

    import copy

    for key, value in overrides.items():
        canonical_value = canonical.get(key)
        if isinstance(canonical_value, Mapping):
            if not isinstance(value, Mapping):
                denied = copy.deepcopy(dict(canonical_value))
                _set_all_leaves(denied, False)
                base[key] = denied
                continue
            existing = base.get(key)
            if not isinstance(existing, dict):
                existing = {}
                base[key] = existing
            _apply_direct_permission_layer(existing, value, canonical_value)
            continue
        if canonical_value is not None:
            base[key] = value if type(value) is bool else False
            continue
        # Extension flags remain round-trippable, but PermissionSet.has still
        # requires the exact boolean True before they can grant anything.
        base[key] = copy.deepcopy(value)


def resolve_permission_preset_lineage(
    preset_id: str,
    presets: list[PermissionPresetLineageNode]
    | tuple[PermissionPresetLineageNode, ...],
) -> PermissionPresetLineageResolution:
    """Resolve base-preset inheritance or return an all-denied review state.

    A valid child inherits every absent leaf from its base and then applies
    its own direct values, including explicit False values.  Unknown targets,
    dangling bases, cycles, duplicate identities and malformed canonical
    shapes deny the complete registry and require an owner review.
    """

    nodes_by_id: dict[str, PermissionPresetLineageNode] = {}
    for node in presets:
        if node.id in nodes_by_id:
            return PermissionPresetLineageResolution(
                flags=_fail_closed_permission_flags(),
                owner_review_required=True,
                review_reason="duplicate_preset_id",
            )
        nodes_by_id[node.id] = node

    chain: list[PermissionPresetLineageNode] = []
    seen: set[str] = set()
    current_id = preset_id
    first = True
    while True:
        if current_id in seen:
            return PermissionPresetLineageResolution(
                flags=_fail_closed_permission_flags(),
                owner_review_required=True,
                review_reason="preset_lineage_cycle",
            )
        seen.add(current_id)
        node = nodes_by_id.get(current_id)
        if node is None:
            return PermissionPresetLineageResolution(
                flags=_fail_closed_permission_flags(),
                owner_review_required=True,
                review_reason=("unknown_preset" if first else "dangling_base_preset"),
            )
        if not _canonical_permission_shape_is_valid(node.flags):
            return PermissionPresetLineageResolution(
                flags=_fail_closed_permission_flags(),
                owner_review_required=True,
                review_reason="invalid_preset_flags",
            )
        chain.append(node)
        if node.base_preset_id is None:
            break
        current_id = node.base_preset_id
        first = False

    resolved = _historical_compatibility_permission_flags()
    for node in reversed(chain):
        _overlay_permission_flags(resolved, node.flags)
    return PermissionPresetLineageResolution(
        flags=resolved,
        owner_review_required=False,
    )


def permission_flag_overrides(
    base: Mapping[str, Any],
    desired: Mapping[str, Any],
) -> PermissionFlags:
    """Return the minimal explicit override tree for a desired effective tree."""

    import copy

    result: PermissionFlags = {}
    for key, desired_value in desired.items():
        base_value = base.get(key)
        if isinstance(desired_value, Mapping):
            nested_base = base_value if isinstance(base_value, Mapping) else {}
            nested = permission_flag_overrides(nested_base, desired_value)
            if nested:
                result[key] = nested
        elif desired_value != base_value:
            result[key] = copy.deepcopy(desired_value)
    return result


def validate_strict_permission_flags(
    document: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    """Require exact booleans at every permission-document leaf.

    Unknown branches remain supported for extensions, but values such as
    ``1``, ``"true"`` and ``None`` are never coerced into permissions.
    """

    if document is None:
        return None
    if not isinstance(document, Mapping):
        raise PermissionContractViolation(
            "permission flags must be an object of boolean leaves"
        )

    def walk(value: Mapping[str, Any], path: str = "") -> None:
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if isinstance(child, Mapping):
                walk(child, child_path)
            elif type(child) is not bool:
                raise PermissionContractViolation(
                    f"permission flag {child_path!r} must be boolean"
                )

    walk(document)
    return document


def _permission_value_presence(
    document: Mapping[str, Any],
    path: str,
) -> tuple[bool, Any]:
    current: Any = document
    parts = path.split(".")
    for part in parts:
        if not isinstance(current, Mapping) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _delete_permission_value(document: PermissionFlags, path: str) -> None:
    parts = path.split(".")
    current: Any = document
    parents: list[tuple[dict[str, Any], str]] = []
    for part in parts[:-1]:
        if not isinstance(current, dict):
            return
        child = current.get(part)
        if not isinstance(child, dict):
            return
        parents.append((current, part))
        current = child
    if not isinstance(current, dict):
        return
    current.pop(parts[-1], None)
    for parent, key in reversed(parents):
        child = parent.get(key)
        if isinstance(child, dict) and not child:
            parent.pop(key, None)
        else:
            break


def normalize_agent_permission_overrides(
    agent_flags: Mapping[str, Any],
    preset_flags: Mapping[str, Any] | None = None,
) -> PermissionFlags | None:
    """Normalize historical materialized agent snapshots into direct deltas.

    Before preset lineage existed, assigning a preset copied its complete flag
    tree into the agent row.  A previous introduction backfill could then add
    generic False values for every SK-A leaf, unintentionally shadowing a newly
    reconciled preset.  Each ordered manifest generation is classified
    independently.  An entirely absent generation is historical; an all-False
    generation is recoverable only when that manifest explicitly records a
    known legacy backfill.  Partial generations and every False value in newer
    manifests remain explicit.  Sparse documents are retained verbatim so
    explicit custom False overrides are never elevated.

    Historical all-True snapshots without a preset represent Full Control and
    normalize to ``None``.  That trusted sentinel lets future manifest grants
    propagate without materializing another snapshot.
    """

    import copy

    working = copy.deepcopy(dict(agent_flags))
    historical_paths = tuple(
        path
        for path in _flatten_registry(PERMISSION_REGISTRY)
        if path not in _FAIL_CLOSED_INTRODUCED_FLAGS
    )
    historical_values = tuple(
        _permission_value_presence(working, path) for path in historical_paths
    )
    materialized = all(
        present and type(value) is bool for present, value in historical_values
    )
    if not materialized:
        return working

    # The retired ``any_to_cancelled`` leaf dominated every source-specific
    # cancellation check.  Some historical presets therefore carried a False
    # exact leaf that was semantically irrelevant beside a True wildcard.  Do
    # not project the wildcard into newly introduced leaves yet: doing so would
    # turn an otherwise absent manifest generation into a partial explicit
    # document.  Remove only overlapping historical exact leaves now, then
    # preserve any missing capability in the final sparse delta below.
    legacy_cancel_all_entities = {
        entity_type
        for entity_type in ("ideation", "refinement", "spec", "card", "sprint")
        if _permission_value_presence(
            working, f"{entity_type}.move.any_to_cancelled"
        )
        == (True, True)
    }
    for flag_path in _PRE_REGISTRY_TRANSITION_PERMISSION_LEAVES:
        entity_type = flag_path.split(".", 1)[0]
        if entity_type in legacy_cancel_all_entities and flag_path.endswith(
            "_to_cancelled"
        ):
            _delete_permission_value(working, flag_path)

    # Exact aliases removed when the canonical graph gained source-specific
    # edges are migration fingerprints, not extension grants.  They no longer
    # authorize an operation and must not prevent a materialized historical
    # Full Control snapshot from normalizing to the trusted ``None`` sentinel.
    for path in (
        *_RETIRED_TRANSITION_PERMISSION_LEAVES,
        *_RETIRED_STATE_PERMISSION_LEAVES,
    ):
        present, value = _permission_value_presence(working, path)
        if present and type(value) is bool:
            _delete_permission_value(working, path)

    generic_introduction_manifests: list[PermissionIntroductionManifest] = []
    non_propagating_introduction_manifests: list[PermissionIntroductionManifest] = []
    complete_explicit_introduction_manifests: list[PermissionIntroductionManifest] = []
    for manifest in PERMISSION_INTRODUCTION_MANIFESTS:
        introduced_values = tuple(
            _permission_value_presence(working, path) for path in manifest.leaves
        )
        all_absent = all(not present for present, _value in introduced_values)
        all_true_materialized = all(
            present and value is True for present, value in introduced_values
        )
        all_false_materialized = all(
            present and value is False for present, value in introduced_values
        )
        if all_absent or (
            manifest.recover_all_false_materialization and all_false_materialized
        ):
            generic_introduction_manifests.append(manifest)
        elif not all_true_materialized:
            non_propagating_introduction_manifests.append(manifest)
            matches_preset_generation = preset_flags is not None and all(
                present and _get_nested(preset_flags, path) is value
                for path, (present, value) in zip(
                    manifest.leaves,
                    introduced_values,
                    strict=True,
                )
            )
            # A mixed or partial materialized generation is an explicit
            # permission document, never a Full Control migration
            # fingerprint.  Materialize every absent leaf as False before
            # reducing it to a delta so an inherited preset cannot turn those
            # absences into grants.
            if not matches_preset_generation:
                complete_explicit_introduction_manifests.append(manifest)
            for path, (present, _value) in zip(
                manifest.leaves,
                introduced_values,
                strict=True,
            ):
                if not present:
                    _set_nested(working, path, False)

    if (
        preset_flags is None
        and all(value is True for _present, value in historical_values)
        and not non_propagating_introduction_manifests
    ):
        normalized_full_control = copy.deepcopy(working)
        for manifest in generic_introduction_manifests:
            for path in manifest.leaves:
                _delete_permission_value(normalized_full_control, path)
        # ``None`` is safe only for an exact historical Full Control snapshot.
        # Unknown extension leaves (and any other explicit difference) remain
        # a sparse direct delta instead of being silently discarded.
        explicit_delta = permission_flag_overrides(
            PERMISSION_REGISTRY,
            normalized_full_control,
        )
        return explicit_delta or None

    for manifest in generic_introduction_manifests:
        for path in manifest.leaves:
            _delete_permission_value(working, path)

    base = (
        preset_flags
        if preset_flags is not None
        else _historical_compatibility_permission_flags()
    )
    explicit_delta = permission_flag_overrides(base, working)
    # Preserve the complete explicit generation, including False values that
    # happen to equal the current base.  This keeps custom denies auditable
    # and prevents a later preset or manifest reconciliation from elevating
    # them.
    for manifest in complete_explicit_introduction_manifests:
        for path in manifest.leaves:
            present, value = _permission_value_presence(working, path)
            if present and type(value) is bool:
                _set_nested(explicit_delta, path, value)
    for entity_type in legacy_cancel_all_entities:
        for flag_path in transition_permission_flags(entity_type):
            if (
                flag_path.endswith("_to_cancelled")
                and _get_nested(base, flag_path) is not True
            ):
                _set_nested(explicit_delta, flag_path, True)
    return explicit_delta


# ---------------------------------------------------------------------------
# Built-in preset definitions
# ---------------------------------------------------------------------------


def _build_preset_flags(enabled_flags: list[str]) -> dict[str, Any]:
    """Build a flags dict from a list of enabled flag paths. All others are False."""
    import copy

    flags = _set_all_flags(copy.deepcopy(PERMISSION_REGISTRY), False)
    for path in enabled_flags:
        if path.endswith(".*"):
            # Wildcard: enable all flags under this prefix
            prefix = path[:-2]
            for flag in ALL_FLAGS:
                if flag.startswith(prefix):
                    _set_nested(flags, flag, True)
        else:
            _set_nested(flags, path, True)
    return flags


def get_builtin_presets() -> list[dict[str, Any]]:
    """Return the 7 built-in preset definitions with clean role separation.

    Role boundaries (see docstring for each preset):
    - Full Control: unrestricted
    - Spec:       defines WHAT to build — owns ideation/refinement/spec content,
                  plans sprints, drafts card breakdown. Never submits gates.
    - Executor:   implements normal cards. Moves not_started→validation. Never
                  submits gates, never crosses into validation→done.
    - QA:         owns test scenarios and test card lifecycle. Reads specs,
                  asks questions. Never submits any gate.
    - Validator:  exclusive gate-holder. Submits spec_validation, spec_evaluation,
                  sprint_evaluation, task_validation. Owns approved→validated,
                  validated→in_progress, in_progress→done (spec) and the backward
                  unlock transitions. On cards, only touches validation status
                  and only moves validation→done or validation→not_started.
    """
    import copy

    full_control = copy.deepcopy(PERMISSION_REGISTRY)  # all True

    # ------------------------------------------------------------------
    # Spec — defines WHAT to build
    # ------------------------------------------------------------------
    # Owns: ideation + refinement + spec content (BRs/TRs/contracts/IRs/ORs/
    # mockups/knowledge/test scenarios), sprint planning, initial card breakdown.
    # Cannot: submit gates, validate anything, move cards past not_started,
    # move specs past approved (Validator promotes to validated).
    spec_writer = _build_preset_flags(
        [
            "board.read",
            "board.activity_read",
            "board.analytics_read",
            "board.mentions_read",
            "board.mentions_mark_seen",
            "guidelines.read",
            "profile.update",
            # Stories/Topics — pre-ideation intake and grouping owned by Spec.
            "story.entity.read",
            "story.entity.create",
            "story.entity.edit_fields",
            "story.entity.assign",
            "story.entity.label",
            "story.entity.archive",
            "story.entity.restore",
            "story.entity.delete",
            "story.interact_in.draft",
            "story.interact_in.triage",
            "story.interact_in.ready",
            "story.interact_in.converted",
            "story.interact_in.archived",
            "story.links.ideation",
            "story.conversion.to_ideation",
            "story.history_read",
            "topic.entity.read",
            "topic.entity.create",
            "topic.entity.edit_fields",
            "topic.entity.archive",
            "topic.entity.restore",
            "topic.entity.merge",
            "topic.entity.delete",
            # Ideation — full ownership (create → done), evaluate, derive spec
            "ideation.entity.read",
            "ideation.entity.create",
            "ideation.entity.edit_fields",
            "ideation.entity.assign",
            "ideation.entity.label",
            "ideation.entity.evaluate",
            "ideation.entity.archive",
            "ideation.entity.restore",
            "ideation.entity.delete",
            "ideation.interact_in.draft",
            "ideation.interact_in.evaluating",
            "ideation.interact_in.refined",
            "ideation.qa.read",
            "ideation.qa.ask",
            "ideation.qa.ask_choice",
            "ideation.qa.answer",
            "ideation.mockups.read",
            "ideation.mockups.create",
            "ideation.mockups.edit",
            "ideation.mockups.delete",
            "ideation.mockups.annotate",
            "ideation.architecture.read",
            "ideation.architecture.create",
            "ideation.architecture.edit",
            "ideation.architecture.delete",
            "ideation.architecture.import",
            "ideation.architecture.render",
            "ideation.specs_derive",
            "ideation.versions_read",
            "ideation.history_read",
            # Refinement — full ownership (create → done), derive spec
            "refinement.entity.read",
            "refinement.entity.create",
            "refinement.entity.edit_fields",
            "refinement.entity.assign",
            "refinement.entity.label",
            "refinement.entity.archive",
            "refinement.entity.restore",
            "refinement.entity.delete",
            "refinement.interact_in.draft",
            "refinement.interact_in.in_progress",
            "refinement.interact_in.review",
            "refinement.interact_in.approved",
            "refinement.qa.read",
            "refinement.qa.ask",
            "refinement.qa.ask_choice",
            "refinement.qa.answer",
            "refinement.mockups.read",
            "refinement.mockups.create",
            "refinement.mockups.edit",
            "refinement.mockups.delete",
            "refinement.mockups.annotate",
            "refinement.architecture.read",
            "refinement.architecture.create",
            "refinement.architecture.edit",
            "refinement.architecture.delete",
            "refinement.architecture.import",
            "refinement.architecture.render",
            "refinement.knowledge.read",
            "refinement.knowledge.create",
            "refinement.knowledge.delete",
            "refinement.specs_derive",
            "refinement.versions_read",
            "refinement.history_read",
            # Spec — content CRUD up to approved. Gates and beyond are Validator's.
            "spec.entity.read",
            "spec.entity.create",
            "spec.entity.edit_fields",
            "spec.entity.edit_coverage_flags",
            "spec.entity.assign",
            "spec.entity.label",
            "spec.entity.link_card",
            "spec.entity.archive",
            "spec.entity.restore",
            "spec.entity.delete",
            # Spec interacts in every forward status. Post-validated edits are
            # allowed to reduce the "dance back to draft" friction for cosmetic
            # fixes (knowledge typo, mockup annotation). Convention in
            # agent_instructions.md guides Spec away from structural edits
            # (BR/TR/contract/rules) in validated/in_progress — those still
            # require validated_to_draft. Opção A (permissiva) do refinement
            # de Ideação 3 — Opção B (granularização por flag .edit_in_validated)
            # fica como evolução se drift materializar.
            "spec.interact_in.draft",
            "spec.interact_in.review",
            "spec.interact_in.approved",
            "spec.interact_in.validated",
            "spec.interact_in.in_progress",
            "spec.qa.read",
            "spec.qa.ask",
            "spec.qa.ask_choice",
            "spec.qa.answer",
            "spec.tests.read",
            "spec.tests.create",
            "spec.tests.update_status",
            "spec.rules.read",
            "spec.rules.create",
            "spec.rules.edit",
            "spec.rules.delete",
            "spec.contracts.read",
            "spec.contracts.create",
            "spec.contracts.edit",
            "spec.contracts.delete",
            "spec.integration_requirements.read",
            "spec.integration_requirements.create",
            "spec.integration_requirements.edit",
            "spec.integration_requirements.delete",
            "spec.integration_requirements.link_task",
            "spec.observability_requirements.read",
            "spec.observability_requirements.create",
            "spec.observability_requirements.edit",
            "spec.observability_requirements.delete",
            "spec.observability_requirements.link_task",
            "spec.structured_entity.*",
            "spec.mockups.read",
            "spec.mockups.create",
            "spec.mockups.edit",
            "spec.mockups.delete",
            "spec.mockups.annotate",
            "spec.architecture.read",
            "spec.architecture.create",
            "spec.architecture.edit",
            "spec.architecture.delete",
            "spec.architecture.import",
            "spec.architecture.render",
            "spec.knowledge.read",
            "spec.knowledge.create",
            "spec.knowledge.delete",
            # Spec read-only on gates (sees history, cannot submit)
            "spec.evaluations.read",
            "spec.validation.read",
            "spec.cards_derive",
            "spec.history_read",
            # Sprint — planner owns structure, reads gate history
            "sprint.entity.read",
            "sprint.entity.create",
            "sprint.entity.edit_fields",
            "sprint.entity.edit_coverage_flags",
            "sprint.entity.assign",
            "sprint.entity.label",
            "sprint.entity.archive",
            "sprint.entity.restore",
            "sprint.entity.delete",
            "sprint.interact_in.draft",
            "sprint.interact_in.active",
            "sprint.qa.read",
            "sprint.qa.ask",
            "sprint.qa.answer",
            "sprint.evaluations.read",
            "sprint.history_read",
            # Card — breakdown only (create, link, configure). Lifecycle is Executor/QA/Validator.
            "card.entity.read",
            "card.entity.context_read",
            "card.entity.create",
            "card.entity.create_test",
            "card.entity.edit_fields",
            "card.entity.assign",
            "card.entity.label",
            "card.entity.link_spec",
            "card.entity.link_tests",
            "card.entity.manage_dependencies",
            "card.copy_from_spec.mockups",
            "card.copy_from_spec.knowledge",
            "card.copy_from_spec.qa",
            "card.copy_from_spec.architecture",
            "card.link_to.scenario",
            "card.link_to.tr",
            "card.link_to.rule",
            "card.link_to.contract",
            "card.link_to.ir",
            "card.link_to.or",
            "card.comments.read",
            "card.comments.create",
            "card.attachments.read",
            "card.mockups.read",
            "card.architecture.read",
            "card.architecture.create",
            "card.architecture.edit",
            "card.architecture.delete",
            "card.architecture.import",
            "card.architecture.render",
            "card.tests.read",
            "card.qa.read",
            "card.qa.ask",
            "card.validation.read",
            "card.activity_read",
            "card.interact_in.not_started",
            # KG — spec is the content owner: full power + full session + admin.
            # Cypher here because Spec runs deep supersedence/contradiction
            # investigation when closing a refinement. settings_write +
            # historical_consolidation are exclusive to Spec (they tune the
            # consolidation that produces the content Spec owns).
            "kg.query.*",
            "kg.power.natural",
            "kg.power.schema_info",
            "kg.power.cypher",
            "kg.session.begin",
            "kg.session.add_node",
            "kg.session.add_edge",
            "kg.session.get_similar",
            "kg.session.propose",
            "kg.session.commit",
            "kg.session.abort",
            "kg.admin.settings_read",
            "kg.admin.settings_write",
            "kg.admin.historical_consolidation",
        ]
    )

    # ------------------------------------------------------------------
    # Executor — implements normal cards
    # ------------------------------------------------------------------
    # Owns: card lifecycle from not_started → started → in_progress → validation
    # (and on_hold detours). Reads spec context to implement correctly.
    # Cannot: create cards, submit validation, promote validation→done,
    # create/edit spec content, touch sprint/gates.
    executor = _build_preset_flags(
        [
            "board.read",
            "board.activity_read",
            "board.mentions_read",
            "board.mentions_mark_seen",
            "guidelines.read",
            "profile.update",
            "story.entity.read",
            "story.history_read",
            "topic.entity.read",
            "ideation.architecture.read",
            "refinement.architecture.read",
            # Spec — read-only, interact while in_progress lifecycle states
            "spec.entity.read",
            "spec.qa.read",
            "spec.qa.ask",
            "spec.tests.read",
            "spec.rules.read",
            "spec.contracts.read",
            "spec.integration_requirements.read",
            "spec.integration_requirements.link_task",
            "spec.observability_requirements.read",
            "spec.observability_requirements.link_task",
            "spec.mockups.read",
            "spec.architecture.read",
            "spec.knowledge.read",
            "spec.evaluations.read",
            "spec.validation.read",
            "spec.history_read",
            "spec.interact_in.validated",
            "spec.interact_in.in_progress",
            "spec.interact_in.done",
            # Sprint — read active sprint to know scope
            "sprint.entity.read",
            "sprint.qa.read",
            "sprint.qa.ask",
            "sprint.evaluations.read",
            "sprint.history_read",
            "sprint.interact_in.active",
            # Card — implementer: owns everything up to moving into validation.
            # card.entity.create here unlocks bug/subtask creation when a problem
            # surfaces mid-implementation (convention: only card_type="bug" or a
            # subtask linked to the in_progress card — NOT fresh normal tasks;
            # those remain Spec territory as part of the breakdown).
            "card.entity.read",
            "card.entity.context_read",
            "card.entity.create",
            "card.entity.edit_fields",
            "card.entity.edit_bug_fields",
            "card.entity.assign",
            "card.entity.label",
            "card.interact_in.not_started",
            "card.interact_in.started",
            "card.interact_in.in_progress",
            "card.interact_in.on_hold",
            "card.interact_in.validation",  # read-only touch (to see failed validation feedback)
            "card.qa.read",
            "card.qa.ask",
            "card.qa.answer",
            "card.comments.read",
            "card.comments.create",
            "card.comments.create_choice",
            "card.comments.respond_choice",
            "card.comments.get_responses",
            "card.attachments.read",
            "card.attachments.upload",
            "card.attachments.delete",
            "card.mockups.read",
            "card.mockups.annotate",
            "card.architecture.read",
            "card.tests.read",
            "card.link_to.ir",
            "card.link_to.or",
            "card.conclusion.read",
            "card.conclusion.write",
            "card.validation.read",  # read-only — cannot submit, cannot delete
            "card.activity_read",
            # KG — read-only queries for implementation context.
            # Natural + schema_info are baseline exploration (zero risk).
            # Cypher stays gated (expert mode) and session is not exposed —
            # executor focuses on executing cards, not enriching the KG.
            "kg.query.*",
            "kg.power.natural",
            "kg.power.schema_info",
            "kg.admin.settings_read",
        ]
    )

    # ------------------------------------------------------------------
    # QA — owns test scenarios and test card lifecycle
    # ------------------------------------------------------------------
    # Owns: test_scenarios CRUD on specs, test cards (card_type="test")
    # throughout their lifecycle, test scenario status updates.
    # Cannot: submit any gate (spec_validation, spec_evaluation,
    # sprint_evaluation, task_validation — all exclusive to Validator),
    # create normal cards, touch implementation cards.
    # NOTE: card_type enforcement is a convention, not hard-blocked by flags.
    # The agent is instructed to only work on test cards.
    qa = _build_preset_flags(
        [
            "board.read",
            "board.activity_read",
            "board.mentions_read",
            "board.mentions_mark_seen",
            "guidelines.read",
            "profile.update",
            # Ideation — read + Q&A to raise test-related questions
            "story.entity.read",
            "story.history_read",
            "topic.entity.read",
            "ideation.entity.read",
            "ideation.qa.read",
            "ideation.qa.ask",
            "ideation.qa.ask_choice",
            "ideation.qa.answer",
            "ideation.mockups.read",
            "ideation.architecture.read",
            "ideation.versions_read",
            "ideation.history_read",
            "ideation.interact_in.evaluating",
            "ideation.interact_in.refined",
            # Refinement — read + Q&A
            "refinement.entity.read",
            "refinement.qa.read",
            "refinement.qa.ask",
            "refinement.qa.ask_choice",
            "refinement.qa.answer",
            "refinement.mockups.read",
            "refinement.knowledge.read",
            "refinement.architecture.read",
            "refinement.versions_read",
            "refinement.history_read",
            "refinement.interact_in.review",
            "refinement.interact_in.approved",
            # Spec — tests CRUD (QA's core); read everything else, no gate submissions
            "spec.entity.read",
            "spec.qa.read",
            "spec.qa.ask",
            "spec.qa.ask_choice",
            "spec.qa.answer",
            "spec.tests.read",
            "spec.tests.create",
            "spec.tests.update_status",
            "spec.rules.read",
            "spec.contracts.read",
            "spec.mockups.read",
            "spec.integration_requirements.read",
            "spec.observability_requirements.read",
            "spec.architecture.read",
            "spec.knowledge.read",
            "spec.evaluations.read",  # read-only — Validator submits
            "spec.validation.read",  # read-only — Validator submits
            "spec.history_read",
            "spec.interact_in.approved",
            "spec.interact_in.validated",
            "spec.interact_in.in_progress",
            # Sprint — read + Q&A only (no evaluation submission)
            "sprint.entity.read",
            "sprint.qa.read",
            "sprint.qa.ask",
            "sprint.qa.answer",
            "sprint.evaluations.read",  # read-only — Validator submits
            "sprint.history_read",
            "sprint.interact_in.active",
            "sprint.interact_in.review",
            # Card — test cards lifecycle (create, implement, complete) + read others.
            # card.entity.create added alongside create_test: QA opens bug cards
            # when it spots defects during test execution (convention: QA creates
            # card_type="bug" or "test", never "normal").
            "card.entity.read",
            "card.entity.context_read",
            "card.entity.create",
            "card.entity.create_test",
            "card.entity.edit_fields",
            "card.link_to.scenario",
            "card.qa.read",
            "card.qa.ask",
            "card.qa.answer",
            "card.comments.read",
            "card.comments.create",
            "card.attachments.read",
            "card.attachments.upload",
            "card.mockups.read",
            "card.architecture.read",
            "card.tests.read",
            "card.tests.link",
            "card.tests.update_status",
            "card.conclusion.read",
            "card.conclusion.write",
            "card.validation.read",  # read-only
            "card.activity_read",
            # Test cards don't go through validation gate — QA moves them directly through lifecycle
            "card.interact_in.not_started",
            "card.interact_in.started",
            "card.interact_in.in_progress",
            "card.interact_in.on_hold",
            "card.interact_in.done",
            # KG — QA reads and surfaces gaps. Propose-only session (no commit
            # or abort); Spec/Validator commit on review. Natural + schema
            # help QA investigate, cypher stays gated.
            "kg.query.*",
            "kg.power.natural",
            "kg.power.schema_info",
            "kg.session.begin",
            "kg.session.add_node",
            "kg.session.add_edge",
            "kg.session.get_similar",
            "kg.session.propose",
            "kg.admin.settings_read",
        ]
    )

    # ------------------------------------------------------------------
    # Validator — exclusive gate-holder for every SDLC checkpoint
    # ------------------------------------------------------------------
    # Owns: spec_validation submit, spec_evaluation submit, sprint_evaluation
    # submit, task_validation submit, spec promotions (approved→validated,
    # validated→in_progress, in_progress→done), spec backward unlock
    # (approved→draft, validated→draft), sprint review→closed.
    # Cards: ONLY interact_in validation. ONLY move validation→done or
    # validation→not_started (user requirement — strict).
    # Cannot: create/edit anything, touch cards outside validation status,
    # move specs forward without the gate.
    validator = _build_preset_flags(
        [
            "board.read",
            "board.activity_read",
            "board.mentions_read",
            "board.mentions_mark_seen",
            "guidelines.read",
            "profile.update",
            # Ideation — read + Q&A (observer, cannot edit or promote)
            "story.entity.read",
            "story.history_read",
            "topic.entity.read",
            "ideation.entity.read",
            "ideation.qa.read",
            "ideation.qa.ask",
            "ideation.qa.answer",
            "ideation.mockups.read",
            "ideation.architecture.read",
            "ideation.versions_read",
            "ideation.history_read",
            "ideation.interact_in.evaluating",
            "ideation.interact_in.refined",
            # Refinement — read + Q&A
            "refinement.entity.read",
            "refinement.qa.read",
            "refinement.qa.ask",
            "refinement.qa.answer",
            "refinement.mockups.read",
            "refinement.knowledge.read",
            "refinement.architecture.read",
            "refinement.versions_read",
            "refinement.history_read",
            "refinement.interact_in.review",
            "refinement.interact_in.approved",
            # Spec — full read + both gates (validation + evaluation) EXCLUSIVE submit
            "spec.entity.read",
            "spec.qa.read",
            "spec.qa.ask",
            "spec.qa.answer",
            "spec.tests.read",
            "spec.rules.read",
            "spec.contracts.read",
            "spec.integration_requirements.read",
            "spec.observability_requirements.read",
            "spec.mockups.read",
            "spec.architecture.read",
            "spec.knowledge.read",
            "spec.history_read",
            # Exclusive gate capabilities
            "spec.evaluations.read",
            "spec.evaluations.submit",
            "spec.validation.read",
            "spec.validation.submit",
            # Spec status promotions — only the gate-bound moves
            # Backward unlock paths (preserved from current preset — enables the
            # fix-and-revalidate loop after a gate failure).
            "spec.interact_in.approved",
            "spec.interact_in.validated",
            "spec.interact_in.in_progress",
            # Sprint — evaluation gate EXCLUSIVE + active→review→closed.
            # active→review lives here because Validator owns the sprint-close
            # ceremony: it promotes active→review then runs submit_sprint_evaluation
            # (allowed only in review) then moves review→closed. Without
            # active_to_review + interact_in.active the cycle deadlocks for any
            # team without a Full Control agent.
            "sprint.entity.read",
            "sprint.qa.read",
            "sprint.qa.ask",
            "sprint.qa.answer",
            "sprint.evaluations.read",
            "sprint.evaluations.submit",
            "sprint.history_read",
            "sprint.interact_in.active",
            "sprint.interact_in.review",
            # Card — ONLY the validation status, EXCLUSIVE task_validation submit
            "card.entity.read",
            "card.entity.context_read",
            "card.qa.read",
            "card.qa.ask",
            "card.qa.answer",
            "card.comments.read",
            "card.comments.create",  # leave feedback
            "card.conclusion.read",
            "card.tests.read",
            "card.mockups.read",
            "card.architecture.read",
            "card.attachments.read",
            "card.validation.read",
            "card.validation.submit",  # exclusive submit
            "card.activity_read",
            # interact_in ONLY validation — hard user requirement
            "card.interact_in.validation",
            # moves ONLY validation → {done, not_started} — hard user requirement.
            # submit_task_validation auto-routes via these flags.
            # KG — Validator investigates deeply and consolidates autonomously.
            # Cypher to trace supersedence/contradictions during spec validation;
            # full session to commit decisions emerged from the gate. Admin stays
            # read-only (thresholds + historical are Spec territory).
            "kg.query.*",
            "kg.power.natural",
            "kg.power.schema_info",
            "kg.power.cypher",
            "kg.session.begin",
            "kg.session.add_node",
            "kg.session.add_edge",
            "kg.session.get_similar",
            "kg.session.propose",
            "kg.session.commit",
            "kg.session.abort",
            "kg.admin.settings_read",
        ]
    )

    # ------------------------------------------------------------------
    # Reporter — observer who opens bugs, asks questions, votes on choices
    # ------------------------------------------------------------------
    # Owns: read across every entity/state, opening bug cards, Q&A (ask
    # only — not answer), responding to choice comments, uploading
    # attachments, and KG query + natural + schema_info.
    # Cannot: submit any gate, promote any state, create/edit specs,
    # answer Q&A (observer asks, doesn't answer), consolidate in the KG,
    # use cypher or admin writes.
    # Convention: bug cards only (enforced in agent_instructions, not flags).
    # Use case: PO / stakeholder / onboarding contributor / external auditor.
    reporter = _build_preset_flags(
        [
            # Board baseline
            "board.read",
            "board.activity_read",
            "board.analytics_read",
            "board.mentions_read",
            "board.mentions_mark_seen",
            "guidelines.read",
            "profile.update",
            # Ideation — read + Q&A ask
            "story.entity.read",
            "story.history_read",
            "topic.entity.read",
            "ideation.entity.read",
            "ideation.qa.read",
            "ideation.qa.ask",
            "ideation.mockups.read",
            "ideation.architecture.read",
            "ideation.versions_read",
            "ideation.history_read",
            "ideation.interact_in.draft",
            "ideation.interact_in.evaluating",
            "ideation.interact_in.refined",
            # Refinement — read + Q&A ask
            "refinement.entity.read",
            "refinement.qa.read",
            "refinement.qa.ask",
            "refinement.mockups.read",
            "refinement.knowledge.read",
            "refinement.architecture.read",
            "refinement.versions_read",
            "refinement.history_read",
            "refinement.interact_in.draft",
            "refinement.interact_in.in_progress",
            "refinement.interact_in.review",
            "refinement.interact_in.approved",
            # Spec — full read (all states, all artifacts) + Q&A ask
            "spec.entity.read",
            "spec.qa.read",
            "spec.qa.ask",
            "spec.tests.read",
            "spec.rules.read",
            "spec.contracts.read",
            "spec.integration_requirements.read",
            "spec.observability_requirements.read",
            "spec.mockups.read",
            "spec.architecture.read",
            "spec.knowledge.read",
            "spec.evaluations.read",
            "spec.validation.read",
            "spec.history_read",
            "spec.interact_in.draft",
            "spec.interact_in.review",
            "spec.interact_in.approved",
            "spec.interact_in.validated",
            "spec.interact_in.in_progress",
            "spec.interact_in.done",
            # Sprint — read + Q&A ask
            "sprint.entity.read",
            "sprint.qa.read",
            "sprint.qa.ask",
            "sprint.evaluations.read",
            "sprint.history_read",
            "sprint.interact_in.draft",
            "sprint.interact_in.active",
            "sprint.interact_in.review",
            "sprint.interact_in.closed",
            # Card — read + bug creation (by convention) + comments + choice voting
            "card.entity.read",
            "card.entity.context_read",
            "card.entity.create",
            "card.qa.read",
            "card.qa.ask",
            "card.comments.read",
            "card.comments.create",
            "card.comments.respond_choice",
            "card.comments.get_responses",
            "card.attachments.read",
            "card.attachments.upload",
            "card.mockups.read",
            "card.architecture.read",
            "card.tests.read",
            "card.validation.read",
            "card.activity_read",
            "card.interact_in.not_started",
            # KG — read-only exploration (zero session, no cypher, no admin write)
            "kg.query.*",
            "kg.power.natural",
            "kg.power.schema_info",
            "kg.admin.settings_read",
        ]
    )

    # ------------------------------------------------------------------
    # Sprint Manager — owns the sprint lifecycle end-to-end
    # ------------------------------------------------------------------
    # Owns: sprint CRUD + full state machine (draft→active→review→closed)
    # + sprint_evaluation submission + card.assign for planning.
    # Reads ideation/refinement/spec for context. Card interact_in wide so
    # the sprint can observe execution without touching implementation.
    # Cannot: create cards, submit tech gates, edit spec content, run KG
    # session or cypher.
    # Coexists with Validator on sprint.evaluations.submit — both can
    # submit; audit log differentiates. Adoption is opt-in per team.
    sprint_manager = _build_preset_flags(
        [
            # Board + context read
            "board.read",
            "board.activity_read",
            "board.analytics_read",
            "board.mentions_read",
            "board.mentions_mark_seen",
            "guidelines.read",
            "profile.update",
            # Ideation / Refinement — read + Q&A for planning context
            "story.entity.read",
            "story.history_read",
            "topic.entity.read",
            "ideation.entity.read",
            "ideation.qa.read",
            "ideation.qa.ask",
            "ideation.architecture.read",
            "ideation.history_read",
            "refinement.entity.read",
            "refinement.qa.read",
            "refinement.qa.ask",
            "refinement.architecture.read",
            "refinement.history_read",
            # Spec — read full content + artifacts (planner needs scope)
            "spec.entity.read",
            "spec.qa.read",
            "spec.qa.ask",
            "spec.tests.read",
            "spec.rules.read",
            "spec.contracts.read",
            "spec.integration_requirements.read",
            "spec.observability_requirements.read",
            "spec.mockups.read",
            "spec.architecture.read",
            "spec.knowledge.read",
            "spec.evaluations.read",
            "spec.validation.read",
            "spec.history_read",
            "spec.interact_in.validated",
            "spec.interact_in.in_progress",
            "spec.interact_in.done",
            # Sprint — full ownership
            "sprint.entity.read",
            "sprint.entity.create",
            "sprint.entity.edit_fields",
            "sprint.entity.edit_coverage_flags",
            "sprint.entity.assign",
            "sprint.entity.label",
            "sprint.entity.archive",
            "sprint.entity.restore",
            "sprint.entity.delete",
            "sprint.interact_in.draft",
            "sprint.interact_in.active",
            "sprint.interact_in.review",
            "sprint.interact_in.closed",
            "sprint.qa.read",
            "sprint.qa.ask",
            "sprint.qa.answer",
            "sprint.evaluations.read",
            "sprint.evaluations.submit",
            "sprint.evaluations.delete",
            "sprint.history_read",
            # Card — read, assign, label, observe every state
            "card.entity.read",
            "card.entity.context_read",
            "card.entity.assign",
            "card.entity.label",
            "card.qa.read",
            "card.qa.ask",
            "card.comments.read",
            "card.comments.create",
            "card.conclusion.read",
            "card.tests.read",
            "card.mockups.read",
            "card.architecture.read",
            "card.attachments.read",
            "card.validation.read",
            "card.activity_read",
            "card.interact_in.not_started",
            "card.interact_in.started",
            "card.interact_in.in_progress",
            "card.interact_in.on_hold",
            "card.interact_in.validation",
            "card.interact_in.done",
            # KG baseline — query + natural + schema. No cypher/session/write.
            "kg.query.*",
            "kg.power.natural",
            "kg.power.schema_info",
            "kg.admin.settings_read",
        ]
    )

    definitions = [
        {
            "name": "Full Control",
            "description": "All permissions active — unrestricted access.",
            "flags": full_control,
        },
        {
            "name": "Executor",
            "description": "Implement normal cards. Moves not_started→validation. Cannot submit gates or promote validation→done.",
            "flags": executor,
        },
        {
            "name": "Validator",
            "description": "Exclusive gate-holder. Submits spec/task/sprint validations and evaluations. On cards, only touches validation status.",
            "flags": validator,
        },
        {
            "name": "QA",
            "description": "Owns test scenarios and test card lifecycle. No gate submissions.",
            "flags": qa,
        },
        {
            "name": "Reporter",
            "description": "Observador — lê tudo, abre bug card, pergunta e vota em choice. Zero submit de gate, zero edit, zero consolidação KG. Ideal para PO/stakeholder/onboarding.",
            "flags": reporter,
        },
        {
            "name": "Sprint Manager",
            "description": "Dono do ciclo de sprint (create → active → review → closed + evaluation). Lê contexto de spec/refinement/ideation e orquestra assign de cards. Não cria cards nem submete gates técnicos. Coexiste com Validator.",
            "flags": sprint_manager,
        },
        {
            "name": "Spec",
            "description": "Defines the spec (ideation→refinement→spec content, sprint plan, card breakdown). No gate submissions, no card execution.",
            "flags": spec_writer,
        },
    ]
    # Keep the introduction matrix centralized and exact.  Every introduced
    # leaf is written explicitly even though the preset builder starts false.
    for definition in definitions:
        for manifest in PERMISSION_INTRODUCTION_MANIFESTS:
            grants = set(manifest.grants_for(definition["name"]))
            for flag_path in manifest.leaves:
                _set_nested(definition["flags"], flag_path, flag_path in grants)
        transition_grants = {
            "Executor": _EXECUTOR_TRANSITION_GRANTS,
            "Validator": _VALIDATOR_TRANSITION_GRANTS,
            "QA": _QA_TRANSITION_GRANTS,
            "Reporter": (),
            "Sprint Manager": transition_permission_flags("sprint"),
            "Spec": _SPEC_TRANSITION_GRANTS,
        }
        allowed_transitions = set(
            transition_permission_flags()
            if definition["name"] == "Full Control"
            else transition_grants[definition["name"]]
        )
        for flag_path in transition_permission_flags():
            _set_nested(
                definition["flags"],
                flag_path,
                flag_path in allowed_transitions,
            )
    return definitions


# ---------------------------------------------------------------------------
# role_summary — self-describing agent role, derived from effective flags
# ---------------------------------------------------------------------------


# Flag → short label used to build the "Owns" section.
_OWNS_LABELS: list[tuple[str, str]] = [
    ("story.entity.create", "create stories"),
    ("topic.entity.create", "create topics"),
    ("spec.validation.submit", "submit spec validations"),
    ("spec.evaluations.submit", "submit spec evaluations"),
    ("card.validation.submit", "submit task validations"),
    ("sprint.evaluations.submit", "submit sprint evaluations"),
    ("spec.entity.create", "create specs"),
    ("spec.integration_requirements.create", "author integration requirements"),
    ("spec.observability_requirements.create", "author observability requirements"),
    ("card.entity.create", "create cards"),
    ("card.entity.create_test", "create test cards"),
    ("kg.session.commit", "commit KG consolidation"),
    ("kg.admin.settings_write", "edit KG settings"),
    ("kg.admin.historical_consolidation", "run historical KG consolidation"),
]


# Flag → short label for "Cannot" — only when flag is False (to highlight gaps).
_CANNOT_LABELS: list[tuple[str, str]] = [
    ("spec.validation.submit", "submit gates"),
    ("card.entity.create", "create cards"),
    ("spec.entity.create", "create specs"),
]


_KG_LABELS: list[tuple[str, str]] = [
    ("kg.query.global", "query"),
    ("kg.power.natural", "natural"),
    ("kg.power.cypher", "cypher"),
    ("kg.session.commit", "consolidate"),
]


def _match_builtin_preset_name(flags: dict) -> str | None:
    """Return the built-in preset name whose flags match, or None for custom."""
    for preset in get_builtin_presets():
        if preset["flags"] == flags:
            return preset["name"]
    return None


def generate_role_summary(permissions: Any) -> str:
    """Produce a human-readable, one-line summary of an agent's effective role.

    Format: ``Role: <preset> | Owns: <a, b> | Cannot: <x> | KG: <caps>``.
    Empty sections are omitted. The value is always recomputed — never cached —
    so preset edits and board overrides propagate immediately.

    Accepts:
    - ``None``: legacy agent (permissions column NULL) — grants all by compat.
    - ``list[str]``: legacy flat permissions — mapped to granular for analysis.
    - ``dict``: granular flags (the current canonical form).

    The returned string always starts with ``Role: `` and never contains
    newlines.
    """
    # Legacy permissions=null — unrestricted by backward-compat path in
    # has_permission/check_permission. Signal it explicitly so the agent
    # understands the source of its access.
    if permissions is None:
        return (
            "Role: Full Control (legacy) | "
            "Owns: unrestricted (permissions=null grants all)"
        )

    # Normalize to granular dict + guess preset name.
    if isinstance(permissions, list):
        flags = map_legacy_permissions(permissions)
        preset_name = _match_builtin_preset_name(flags) or "Custom (legacy)"
    elif isinstance(permissions, dict):
        flags = permissions
        preset_name = _match_builtin_preset_name(flags) or "Custom"
    else:
        return "Role: unknown"

    owns = [label for flag, label in _OWNS_LABELS if _get_nested(flags, flag) is True]
    cannot = [
        label for flag, label in _CANNOT_LABELS if _get_nested(flags, flag) is False
    ]
    # Dedupe cannot against owns (in case the flag is both True and False
    # across entities — shouldn't happen but defensive).
    cannot = [c for c in cannot if c not in owns]

    kg = [label for flag, label in _KG_LABELS if _get_nested(flags, flag) is True]

    parts = [f"Role: {preset_name}"]
    if owns:
        parts.append(f"Owns: {', '.join(owns)}")
    if cannot:
        parts.append(f"Cannot: {', '.join(cannot)}")
    if kg:
        parts.append(f"KG: {', '.join(kg)}")
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------


def registry_vs_tools_report(tool_names: list[str]) -> McpPermissionRegistryReport:
    """Return the pure, deterministic MCP permission inventory comparison."""

    return build_mcp_permission_registry_report(tool_names, all_flags=ALL_FLAGS)


def validate_registry_vs_tools(
    tool_names: list[str],
    *,
    strict: bool = False,
) -> McpPermissionRegistryReport:
    """Validate the exact MCP permission manifest.

    The default keeps the historical runtime logging behavior.  CI and other
    fail-closed boundaries pass ``strict=True`` to raise on any drift.
    """
    import logging

    logger = logging.getLogger("okto_pulse.permissions")
    report = registry_vs_tools_report(tool_names)
    if strict:
        report.assert_valid()
    elif not report.is_valid:
        logger.warning("MCP permission registry drift: %s", report.render())
    else:
        logger.info(
            "Permission registry: %d flags, %d MCP tools, %d human-only exemptions.",
            len(ALL_FLAGS),
            len(report.live_tools),
            report.exemption_count,
        )
    return report


def _perm_error_detailed(
    reason: str,
    required_permission: str,
    current_state: str | None = None,
    detail: str = "",
) -> str:
    """Build detailed permission error JSON string."""
    error: dict[str, Any] = {
        "error": "Permission denied",
        "reason": reason,
        "required_permission": required_permission,
    }
    if current_state:
        error["current_state"] = current_state
    if detail:
        error["detail"] = detail
    return json.dumps(error)


# ---------------------------------------------------------------------------
# Backward-compatible check functions
# ---------------------------------------------------------------------------


def has_permission(
    agent_permissions: "list[str] | PermissionSet | None", required: str
) -> bool:
    """Check if agent has a specific permission.

    Accepts:
    - None: full access (backwards compat)
    - list[str]: legacy flat permissions
    - PermissionSet: new granular permissions
    """
    if agent_permissions is None:
        return True
    if isinstance(agent_permissions, PermissionSet):
        return agent_permissions.has(required)
    return required in agent_permissions


def check_permission(
    agent_permissions: "list[str] | PermissionSet | None", required: str
) -> str | None:
    """Check permission and return error message if denied.

    Returns None if allowed, error message string if denied.
    Accepts list[str] (legacy), PermissionSet (new), or None (full access).
    """
    if agent_permissions is None:
        return None
    if isinstance(agent_permissions, PermissionSet):
        return agent_permissions.check(required)
    if required in agent_permissions:
        return None
    return f"Permission denied: requires '{required}'"


def evaluate_permission(context: PermissionContext) -> PermissionDecision:
    """Evaluate a permission context with the canonical Core policy."""

    operation = context.operation.strip()
    if not operation:
        raise InvalidPermissionContext("operation must be a non-empty flag path")
    if operation not in ALL_FLAGS:
        return PermissionDecision.deny(
            operation,
            _perm_error_detailed(
                reason="unknown_permission",
                required_permission=operation,
                detail=(
                    f"The permission '{operation}' is not registered by the Core "
                    "permission policy."
                ),
            ),
        )

    permissions = context.permissions

    def _reason_for(required: str, *, state_aware: bool) -> str | None:
        if isinstance(permissions, PermissionSet):
            if state_aware and context.entity and context.state:
                return permissions.check_with_state(
                    required,
                    context.entity,
                    context.state,
                )
            return permissions.check(required)
        return check_permission(permissions, required)

    reason = _reason_for(operation, state_aware=True)
    if (
        not isinstance(permissions, PermissionSet)
        and (
            operation not in _FAIL_CLOSED_INTRODUCED_FLAGS
            or operation in _LEGACY_COMPATIBLE_INTRODUCED_FLAGS
        )
        and reason
        and context.legacy_operation
    ):
        # Historical leaves and explicitly staged post-SK-B introductions keep
        # their caller-declared flat-token compatibility during migration.
        reason = _reason_for(
            context.legacy_operation,
            state_aware=False,
        )
        if reason and isinstance(permissions, (list, tuple)):
            # A flat pre-migration principal cannot carry canonical tree paths.
            # Resolve only the explicit inverse edges declared by
            # LEGACY_PERMISSION_MAP; no wildcard or inferred widening occurs.
            for legacy_token in _CANONICAL_TO_LEGACY_TOKENS.get(
                context.legacy_operation,
                (),
            ):
                reason = _reason_for(legacy_token, state_aware=False)
                if reason is None:
                    break

    if reason is None:
        return PermissionDecision.allow(operation)
    return PermissionDecision.deny(operation, reason)


class DefaultPermissionPolicy:
    """Stateless Core implementation of the permission policy contract."""

    def resolve(
        self,
        agent_flags: PermissionFlags | None,
        preset_flags: PermissionFlags | None,
        board_overrides: PermissionFlags | None,
        *,
        owner_review_required: bool = False,
        review_reason: str | None = None,
    ) -> PermissionSet:
        return resolve_permissions(
            agent_flags,
            preset_flags,
            board_overrides,
            owner_review_required=owner_review_required,
            review_reason=review_reason,
        )

    def evaluate(self, context: PermissionContext) -> PermissionDecision:
        return evaluate_permission(context)


__all__ = [
    "ADMIN_CATALOG_PERMISSION_INTRODUCTION_V1",
    "ALL_FLAGS",
    "DefaultPermissionPolicy",
    "HUMAN_ONLY_MCP_TOOL_EXEMPTIONS",
    "GUIDELINE_ADOPTION_MANAGE",
    "GUIDELINE_ASSESSMENTS_READ",
    "GUIDELINE_ASSESSMENTS_RECORD",
    "GUIDELINE_IMPACT_PREVIEW",
    "GUIDELINE_METRICS_AUTHOR",
    "GUIDELINE_REVISIONS_CREATE",
    "GUIDELINE_REVISIONS_READ",
    "GUIDELINE_REVISIONS_RETIRE",
    "InvalidPermissionContext",
    "LEGACY_PERMISSION_MAP",
    "KG_OPERATIONS_PERMISSION_INTRODUCTION_V1",
    "MAX_HUMAN_ONLY_TOOL_EXEMPTIONS",
    "MCP_TOOL_PERMISSION_POLICIES",
    "MCP_GAPS_PERMISSION_INTRODUCTION_V1",
    "HumanOnlyToolExemption",
    "McpPermissionRegistryError",
    "McpPermissionRegistryReport",
    "McpToolPermissionPolicy",
    "OPERATIONAL_PERMISSION_INTRODUCTION_V1",
    "PERMISSION_REGISTRY",
    "PermissionContext",
    "PermissionContractViolation",
    "PermissionDecision",
    "PermissionFlags",
    "PermissionIntroductionManifest",
    "PermissionPolicyError",
    "PermissionPresetLineageNode",
    "PermissionPresetLineageResolution",
    "PermissionSet",
    "Permissions",
    "PERMISSION_INTRODUCTION_MANIFESTS",
    "SKA_PERMISSION_INTRODUCTION_V1",
    "SKB3_PERMISSION_INTRODUCTION_V1",
    "SDLC_TRANSITION_PERMISSION_INTRODUCTION_V1",
    "STRUCTURED_SPEC_ENTITY_OPERATIONS",
    "STRUCTURED_SPEC_ENTITY_TYPES",
    "check_permission",
    "evaluate_permission",
    "generate_role_summary",
    "get_builtin_presets",
    "has_permission",
    "map_legacy_permissions",
    "merge_missing_flags",
    "normalize_agent_permission_overrides",
    "permission_flag_overrides",
    "resolve_permission_preset_lineage",
    "resolve_permissions",
    "registry_vs_tools_report",
    "structured_spec_entity_permission_flags",
    "validate_strict_permission_flags",
    "validate_registry_vs_tools",
]
