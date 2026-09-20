"""Frozen, pure v0.3.4 permission semantics for historical archive migration.

Source: Core commit 20707250, domain/permissions.py blob
74101618064a1e50a1e9e11c012f7ff6f1f8f7a9. This is a versioned compatibility
evaluator, never the live registry, a preset seed or an authorization endpoint.
Keep this source generation stable when retiring live permissions/lifecycles.
The public migration facade exposes only the four archived read decisions.

Canonical function bodies and policy data are frozen together. The only source
substitutions replace live lifecycle lookups with their original literal values.
No filesystem, database, current registry or lifecycle imports are permitted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, TypeAlias

_FROZEN_TRANSITION_FLAGS = {'card': ('card.move.not_started_to_started',
          'card.move.not_started_to_in_progress',
          'card.move.not_started_to_cancelled',
          'card.move.started_to_not_started',
          'card.move.started_to_in_progress',
          'card.move.started_to_validation',
          'card.move.started_to_on_hold',
          'card.move.started_to_cancelled',
          'card.move.in_progress_to_started',
          'card.move.in_progress_to_validation',
          'card.move.in_progress_to_done',
          'card.move.in_progress_to_on_hold',
          'card.move.in_progress_to_cancelled',
          'card.move.validation_to_in_progress',
          'card.move.validation_to_done',
          'card.move.validation_to_on_hold',
          'card.move.validation_to_cancelled',
          'card.move.on_hold_to_started',
          'card.move.on_hold_to_in_progress',
          'card.move.on_hold_to_cancelled',
          'card.move.done_to_in_progress',
          'card.move.rejected_to_in_progress',
          'card.move.cancelled_to_not_started'),
 'ideation': ('ideation.move.draft_to_review',
              'ideation.move.draft_to_cancelled',
              'ideation.move.review_to_draft',
              'ideation.move.review_to_approved',
              'ideation.move.review_to_cancelled',
              'ideation.move.approved_to_review',
              'ideation.move.approved_to_evaluating',
              'ideation.move.approved_to_cancelled',
              'ideation.move.evaluating_to_approved',
              'ideation.move.evaluating_to_done',
              'ideation.move.evaluating_to_cancelled',
              'ideation.move.done_to_draft',
              'ideation.move.cancelled_to_draft'),
 'refinement': ('refinement.move.draft_to_review',
                'refinement.move.draft_to_cancelled',
                'refinement.move.review_to_draft',
                'refinement.move.review_to_approved',
                'refinement.move.review_to_cancelled',
                'refinement.move.approved_to_review',
                'refinement.move.approved_to_done',
                'refinement.move.approved_to_cancelled',
                'refinement.move.done_to_draft',
                'refinement.move.cancelled_to_draft'),
 'spec': ('spec.move.draft_to_review',
          'spec.move.draft_to_cancelled',
          'spec.move.review_to_draft',
          'spec.move.review_to_approved',
          'spec.move.review_to_cancelled',
          'spec.move.approved_to_review',
          'spec.move.approved_to_validated',
          'spec.move.approved_to_draft',
          'spec.move.approved_to_cancelled',
          'spec.move.validated_to_approved',
          'spec.move.validated_to_in_progress',
          'spec.move.validated_to_draft',
          'spec.move.validated_to_cancelled',
          'spec.move.in_progress_to_validated',
          'spec.move.in_progress_to_draft',
          'spec.move.in_progress_to_done',
          'spec.move.in_progress_to_cancelled',
          'spec.move.done_to_draft',
          'spec.move.cancelled_to_draft'),
 'sprint': ('sprint.move.draft_to_active',
            'sprint.move.draft_to_cancelled',
            'sprint.move.active_to_draft',
            'sprint.move.active_to_review',
            'sprint.move.active_to_cancelled',
            'sprint.move.review_to_active',
            'sprint.move.review_to_closed',
            'sprint.move.review_to_cancelled',
            'sprint.move.closed_to_draft',
            'sprint.move.cancelled_to_draft'),
 'story': ('story.move.draft_to_triage',
           'story.move.draft_to_ready',
           'story.move.triage_to_draft',
           'story.move.triage_to_ready',
           'story.move.ready_to_triage'),
 'test_scenario': ('test_scenario.move.draft_to_ready',
                   'test_scenario.move.draft_to_automated',
                   'test_scenario.move.draft_to_passed',
                   'test_scenario.move.draft_to_failed',
                   'test_scenario.move.ready_to_draft',
                   'test_scenario.move.ready_to_automated',
                   'test_scenario.move.ready_to_passed',
                   'test_scenario.move.ready_to_failed',
                   'test_scenario.move.automated_to_ready',
                   'test_scenario.move.automated_to_passed',
                   'test_scenario.move.failed_to_ready',
                   'test_scenario.move.failed_to_passed',
                   'test_scenario.move.passed_to_ready')}

def transition_permission_flags(entity_type):
    return _FROZEN_TRANSITION_FLAGS[entity_type]

_TEST_SCENARIO_STATUSES = ('draft', 'ready', 'automated', 'passed', 'failed')

PermissionFlags: TypeAlias = dict[str, Any]


class PermissionPolicyError(Exception):
    """Base class for transport-free permission contract failures."""


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
    legacy_compatible: bool = False

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


_SKA_CONTEXT_READ_LEAVES = ('ideation.quality.read',
 'refinement.quality.read',
 'spec.quality.read',
 'refinement.research_decisions.read',
 'spec.checklist.read')


_PRE_REGISTRY_TRANSITION_PERMISSION_LEAVES = frozenset({'card.move.in_progress_to_done',
           'card.move.in_progress_to_on_hold',
           'card.move.in_progress_to_validation',
           'card.move.not_started_to_started',
           'card.move.on_hold_to_in_progress',
           'card.move.started_to_in_progress',
           'card.move.validation_to_cancelled',
           'card.move.validation_to_done',
           'card.move.validation_to_on_hold',
           'refinement.move.approved_to_done',
           'refinement.move.review_to_approved',
           'spec.move.approved_to_draft',
           'spec.move.approved_to_validated',
           'spec.move.draft_to_review',
           'spec.move.in_progress_to_done',
           'spec.move.review_to_approved',
           'spec.move.validated_to_draft',
           'spec.move.validated_to_in_progress',
           'sprint.move.active_to_review',
           'sprint.move.draft_to_active',
           'sprint.move.review_to_closed',
           'story.move.draft_to_ready',
           'story.move.draft_to_triage',
           'story.move.ready_to_triage',
           'story.move.triage_to_draft',
           'story.move.triage_to_ready'})


_RETIRED_TRANSITION_PERMISSION_LEAVES = ('card.move.any_to_cancelled',
 'card.move.validation_to_not_started',
 'ideation.move.any_to_cancelled',
 'ideation.move.draft_to_evaluating',
 'ideation.move.evaluating_to_refined',
 'ideation.move.refined_to_done',
 'refinement.move.any_to_cancelled',
 'refinement.move.draft_to_in_progress',
 'refinement.move.in_progress_to_review',
 'spec.move.any_to_cancelled',
 'sprint.move.any_to_cancelled')


_RETIRED_STATE_PERMISSION_LEAVES = ('ideation.interact_in.refined', 'refinement.interact_in.in_progress')


_NEW_SDLC_STATE_PERMISSION_LEAVES = ('ideation.interact_in.review', 'ideation.interact_in.approved')


PERMISSION_INTRODUCTION_MANIFESTS = (
PermissionIntroductionManifest(version='SK-A/v1', leaves=('ideation.quality.read',
 'ideation.quality.assess',
 'refinement.quality.read',
 'refinement.quality.assess',
 'spec.quality.read',
 'spec.quality.assess',
 'refinement.research_decisions.read',
 'refinement.research_decisions.append',
 'spec.checklist.read',
 'spec.checklist.execute'), preset_grants=(('Full Control',
  ('ideation.quality.read',
   'ideation.quality.assess',
   'refinement.quality.read',
   'refinement.quality.assess',
   'spec.quality.read',
   'spec.quality.assess',
   'refinement.research_decisions.read',
   'refinement.research_decisions.append',
   'spec.checklist.read',
   'spec.checklist.execute')),
 ('Spec',
  ('ideation.quality.read',
   'refinement.quality.read',
   'spec.quality.read',
   'refinement.research_decisions.read',
   'spec.checklist.read',
   'ideation.quality.assess',
   'refinement.quality.assess',
   'refinement.research_decisions.append',
   'spec.checklist.execute')),
 ('Validator',
  ('ideation.quality.read',
   'refinement.quality.read',
   'spec.quality.read',
   'refinement.research_decisions.read',
   'spec.checklist.read',
   'spec.quality.assess')),
 ('QA',
  ('ideation.quality.read',
   'refinement.quality.read',
   'spec.quality.read',
   'refinement.research_decisions.read',
   'spec.checklist.read')),
 ('Reporter',
  ('ideation.quality.read',
   'refinement.quality.read',
   'spec.quality.read',
   'refinement.research_decisions.read',
   'spec.checklist.read')),
 ('Sprint Manager',
  ('ideation.quality.read',
   'refinement.quality.read',
   'spec.quality.read',
   'refinement.research_decisions.read',
   'spec.checklist.read')),
 ('Executor', ('spec.quality.read', 'spec.checklist.read'))), historical_authorities=(('ideation.quality.read', 'ideation.entity.read'),
 ('ideation.quality.assess', 'spec.entity.edit_fields'),
 ('refinement.quality.read', 'refinement.entity.read'),
 ('refinement.quality.assess', 'spec.entity.edit_fields'),
 ('spec.quality.read', 'spec.entity.read'),
 ('spec.quality.assess', 'spec.validation.submit'),
 ('refinement.research_decisions.read', 'refinement.entity.read'),
 ('refinement.research_decisions.append', 'spec.entity.edit_fields'),
 ('spec.checklist.read', 'spec.entity.read'),
 ('spec.checklist.execute', 'spec.entity.edit_fields')), recover_all_false_materialization=True, legacy_compatible=False),
PermissionIntroductionManifest(version='SK-B3/v1', leaves=('guidelines.revisions.read',
 'guidelines.revisions.create',
 'guidelines.revisions.retire',
 'guidelines.metrics.author',
 'guidelines.impact.preview',
 'guidelines.adoption.manage',
 'guidelines.assessments.read',
 'guidelines.assessments.record',
 'guidelines.waiver.read',
 'guidelines.waiver.request',
 'guidelines.waiver.review',
 'guidelines.waiver.revoke',
 'guidelines.waiver.revalidate'), preset_grants=(('Full Control',
  ('guidelines.revisions.read',
   'guidelines.revisions.create',
   'guidelines.revisions.retire',
   'guidelines.metrics.author',
   'guidelines.impact.preview',
   'guidelines.adoption.manage',
   'guidelines.assessments.read',
   'guidelines.assessments.record',
   'guidelines.waiver.read',
   'guidelines.waiver.request',
   'guidelines.waiver.review',
   'guidelines.waiver.revoke',
   'guidelines.waiver.revalidate')),
 ('Spec',
  ('guidelines.revisions.read',
   'guidelines.revisions.create',
   'guidelines.metrics.author',
   'guidelines.impact.preview',
   'guidelines.adoption.manage',
   'guidelines.assessments.read',
   'guidelines.assessments.record',
   'guidelines.waiver.read',
   'guidelines.waiver.request')),
 ('Validator',
  ('guidelines.revisions.read',
   'guidelines.impact.preview',
   'guidelines.assessments.read',
   'guidelines.assessments.record',
   'guidelines.waiver.read',
   'guidelines.waiver.review',
   'guidelines.waiver.revalidate')),
 ('QA',
  ('guidelines.revisions.read',
   'guidelines.assessments.read',
   'guidelines.assessments.record',
   'guidelines.waiver.read',
   'guidelines.waiver.request')),
 ('Reporter',
  ('guidelines.revisions.read', 'guidelines.assessments.read', 'guidelines.waiver.read')),
 ('Sprint Manager',
  ('guidelines.revisions.read',
   'guidelines.impact.preview',
   'guidelines.assessments.read',
   'guidelines.assessments.record',
   'guidelines.waiver.read',
   'guidelines.waiver.request')),
 ('Executor',
  ('guidelines.revisions.read',
   'guidelines.assessments.read',
   'guidelines.assessments.record',
   'guidelines.waiver.read',
   'guidelines.waiver.request'))), historical_authorities=(('guidelines.revisions.read', 'guidelines.read'),
 ('guidelines.revisions.create', 'spec.entity.edit_fields'),
 ('guidelines.revisions.retire', 'guidelines.delete'),
 ('guidelines.metrics.author', 'spec.entity.edit_fields'),
 ('guidelines.impact.preview', 'guidelines.read'),
 ('guidelines.adoption.manage', 'spec.entity.edit_fields'),
 ('guidelines.assessments.read', 'guidelines.read'),
 ('guidelines.assessments.record', 'guidelines.read'),
 ('guidelines.waiver.read', 'guidelines.read'),
 ('guidelines.waiver.request', 'guidelines.read'),
 ('guidelines.waiver.review', 'spec.validation.submit'),
 ('guidelines.waiver.revoke', 'guidelines.delete'),
 ('guidelines.waiver.revalidate', 'spec.validation.submit')), recover_all_false_materialization=False, legacy_compatible=False),
PermissionIntroductionManifest(version='ADMIN-CATALOG/v1', leaves=('agent.entity.read',
 'agent.entity.create',
 'agent.entity.edit',
 'agent.entity.delete',
 'agent.api_key.rotate',
 'agent.board_access.read',
 'agent.board_access.grant',
 'agent.board_access.edit',
 'agent.board_access.revoke',
 'board.admin.create',
 'board.admin.edit',
 'board.admin.delete',
 'board.share.read',
 'board.share.create',
 'board.share.edit',
 'board.share.revoke',
 'board.share.leave',
 'permission_preset.entity.read',
 'permission_preset.entity.create',
 'permission_preset.entity.edit',
 'permission_preset.entity.delete',
 'permission_preset.clone',
 'permission_preset.import',
 'permission_preset.export',
 'default_board_config.read',
 'default_board_config.diff_read',
 'default_board_config.candidates_read',
 'default_board_config.export',
 'default_board_config.create',
 'default_board_config.activate',
 'default_board_config.deactivate',
 'default_board_config.import',
 'default_board_config.set_design_system',
 'default_board_config.guidelines.edit',
 'design_system.entity.read',
 'design_system.entity.create',
 'design_system.entity.edit',
 'design_system.entity.delete',
 'design_system.import',
 'design_system.export',
 'design_system.board_link.read',
 'design_system.board_link.create',
 'design_system.board_link.delete'), preset_grants=(('Full Control',
  ('agent.entity.read',
   'agent.entity.create',
   'agent.entity.edit',
   'agent.entity.delete',
   'agent.api_key.rotate',
   'agent.board_access.read',
   'agent.board_access.grant',
   'agent.board_access.edit',
   'agent.board_access.revoke',
   'board.admin.create',
   'board.admin.edit',
   'board.admin.delete',
   'board.share.read',
   'board.share.create',
   'board.share.edit',
   'board.share.revoke',
   'board.share.leave',
   'permission_preset.entity.read',
   'permission_preset.entity.create',
   'permission_preset.entity.edit',
   'permission_preset.entity.delete',
   'permission_preset.clone',
   'permission_preset.import',
   'permission_preset.export',
   'default_board_config.read',
   'default_board_config.diff_read',
   'default_board_config.candidates_read',
   'default_board_config.export',
   'default_board_config.create',
   'default_board_config.activate',
   'default_board_config.deactivate',
   'default_board_config.import',
   'default_board_config.set_design_system',
   'default_board_config.guidelines.edit',
   'design_system.entity.read',
   'design_system.entity.create',
   'design_system.entity.edit',
   'design_system.entity.delete',
   'design_system.import',
   'design_system.export',
   'design_system.board_link.read',
   'design_system.board_link.create',
   'design_system.board_link.delete')),
 ('Executor',
  ('default_board_config.read',
   'default_board_config.diff_read',
   'default_board_config.candidates_read',
   'default_board_config.export',
   'design_system.entity.read',
   'design_system.export',
   'design_system.board_link.read')),
 ('Validator',
  ('default_board_config.read',
   'default_board_config.diff_read',
   'default_board_config.candidates_read',
   'default_board_config.export',
   'design_system.entity.read',
   'design_system.export',
   'design_system.board_link.read')),
 ('QA',
  ('default_board_config.read',
   'default_board_config.diff_read',
   'default_board_config.candidates_read',
   'default_board_config.export',
   'design_system.entity.read',
   'design_system.export',
   'design_system.board_link.read')),
 ('Reporter',
  ('default_board_config.read',
   'default_board_config.diff_read',
   'default_board_config.candidates_read',
   'default_board_config.export',
   'design_system.entity.read',
   'design_system.export',
   'design_system.board_link.read')),
 ('Sprint Manager',
  ('default_board_config.read',
   'default_board_config.diff_read',
   'default_board_config.candidates_read',
   'default_board_config.export',
   'design_system.entity.read',
   'design_system.export',
   'design_system.board_link.read')),
 ('Spec',
  ('default_board_config.read',
   'default_board_config.diff_read',
   'default_board_config.candidates_read',
   'default_board_config.export',
   'design_system.entity.read',
   'design_system.export',
   'design_system.board_link.read',
   'default_board_config.create',
   'default_board_config.activate',
   'default_board_config.deactivate',
   'default_board_config.import',
   'default_board_config.set_design_system',
   'default_board_config.guidelines.edit',
   'design_system.entity.create',
   'design_system.entity.edit',
   'design_system.entity.delete',
   'design_system.import',
   'design_system.board_link.create',
   'design_system.board_link.delete'))), historical_authorities=(('agent.entity.read', 'board.read'),
 ('agent.entity.create', 'profile.update'),
 ('agent.entity.edit', 'profile.update'),
 ('agent.entity.delete', 'profile.update'),
 ('agent.api_key.rotate', 'profile.update'),
 ('agent.board_access.read', 'board.read'),
 ('agent.board_access.grant', 'board.read'),
 ('agent.board_access.edit', 'board.read'),
 ('agent.board_access.revoke', 'board.read'),
 ('board.admin.create', 'board.read'),
 ('board.admin.edit', 'board.read'),
 ('board.admin.delete', 'board.read'),
 ('board.share.read', 'board.read'),
 ('board.share.create', 'board.read'),
 ('board.share.edit', 'board.read'),
 ('board.share.revoke', 'board.read'),
 ('board.share.leave', 'board.read'),
 ('permission_preset.entity.read', 'board.read'),
 ('permission_preset.entity.create', 'profile.update'),
 ('permission_preset.entity.edit', 'profile.update'),
 ('permission_preset.entity.delete', 'profile.update'),
 ('permission_preset.clone', 'profile.update'),
 ('permission_preset.import', 'profile.update'),
 ('permission_preset.export', 'board.read'),
 ('default_board_config.read', 'board.read'),
 ('default_board_config.diff_read', 'board.read'),
 ('default_board_config.candidates_read', 'board.read'),
 ('default_board_config.export', 'board.read'),
 ('default_board_config.create', 'spec.entity.edit_fields'),
 ('default_board_config.activate', 'spec.entity.edit_fields'),
 ('default_board_config.deactivate', 'spec.entity.edit_fields'),
 ('default_board_config.import', 'spec.entity.edit_fields'),
 ('default_board_config.set_design_system', 'spec.entity.edit_fields'),
 ('default_board_config.guidelines.edit', 'guidelines.adoption.manage'),
 ('design_system.entity.read', 'board.read'),
 ('design_system.entity.create', 'spec.architecture.create'),
 ('design_system.entity.edit', 'spec.architecture.edit'),
 ('design_system.entity.delete', 'spec.architecture.delete'),
 ('design_system.import', 'spec.architecture.import'),
 ('design_system.export', 'spec.architecture.read'),
 ('design_system.board_link.read', 'board.read'),
 ('design_system.board_link.create', 'spec.architecture.edit'),
 ('design_system.board_link.delete', 'spec.architecture.edit')), recover_all_false_materialization=False, legacy_compatible=True),
PermissionIntroductionManifest(version='OPERATIONAL/v1', leaves=('runtime.settings.read',
 'runtime.settings.write',
 'metrics.local.summary.read',
 'metrics.publish_health.read',
 'metrics.local.events.create',
 'metrics.settings.edit',
 'metrics.settings.migration_notice_seen',
 'metrics.local.export',
 'metrics.local.purge',
 'amendment.revision.read',
 'amendment.revision.create',
 'amendment.revision.associate',
 'amendment.revision.transition',
 'amendment.coverage.confirm'), preset_grants=(('Full Control',
  ('runtime.settings.read',
   'runtime.settings.write',
   'metrics.local.summary.read',
   'metrics.publish_health.read',
   'metrics.local.events.create',
   'metrics.settings.edit',
   'metrics.settings.migration_notice_seen',
   'metrics.local.export',
   'metrics.local.purge',
   'amendment.revision.read',
   'amendment.revision.create',
   'amendment.revision.associate',
   'amendment.revision.transition',
   'amendment.coverage.confirm')),
 ('Executor',
  ('metrics.local.summary.read',
   'metrics.publish_health.read',
   'amendment.revision.read',
   'amendment.revision.create',
   'amendment.revision.associate',
   'amendment.revision.transition')),
 ('Validator',
  ('metrics.local.summary.read',
   'metrics.publish_health.read',
   'amendment.revision.read',
   'amendment.coverage.confirm')),
 ('QA', ('metrics.local.summary.read', 'metrics.publish_health.read', 'amendment.revision.read')),
 ('Reporter',
  ('metrics.local.summary.read', 'metrics.publish_health.read', 'amendment.revision.read')),
 ('Sprint Manager',
  ('metrics.local.summary.read', 'metrics.publish_health.read', 'amendment.revision.read')),
 ('Spec', ('metrics.local.summary.read', 'metrics.publish_health.read', 'amendment.revision.read'))), historical_authorities=(('runtime.settings.read', 'kg.admin.settings_read'),
 ('runtime.settings.write', 'kg.admin.settings_write'),
 ('metrics.local.summary.read', 'board.read'),
 ('metrics.publish_health.read', 'board.read'),
 ('metrics.local.events.create', 'board.analytics_read'),
 ('metrics.settings.edit', 'board.analytics_read'),
 ('metrics.settings.migration_notice_seen', 'board.analytics_read'),
 ('metrics.local.export', 'board.analytics_read'),
 ('metrics.local.purge', 'board.analytics_read'),
 ('amendment.revision.read', 'card.entity.read'),
 ('amendment.revision.create', 'card.entity.edit_bug_fields'),
 ('amendment.revision.associate', 'card.entity.edit_bug_fields'),
 ('amendment.revision.transition', 'card.entity.edit_bug_fields'),
 ('amendment.coverage.confirm', 'card.validation.submit')), recover_all_false_materialization=False, legacy_compatible=True),
PermissionIntroductionManifest(version='MCP-GAPS/v1', leaves=('ideation.knowledge.read',
 'ideation.knowledge.create',
 'ideation.knowledge.delete',
 'ideation.qa.delete',
 'refinement.qa.delete',
 'spec.qa.delete',
 'sprint.qa.delete',
 'spec.tests.execute',
 'spec.tests.edit',
 'spec.tests.delete',
 'story.mockups.read',
 'story.mockups.create',
 'story.mockups.edit',
 'story.mockups.delete',
 'story.mockups.annotate',
 'sprint.tasks.assign',
 'test_scenario.interact_in.draft',
 'test_scenario.interact_in.ready',
 'test_scenario.interact_in.automated',
 'test_scenario.interact_in.passed',
 'test_scenario.interact_in.failed'), preset_grants=(('Full Control',
  ('ideation.knowledge.read',
   'ideation.knowledge.create',
   'ideation.knowledge.delete',
   'ideation.qa.delete',
   'refinement.qa.delete',
   'spec.qa.delete',
   'sprint.qa.delete',
   'spec.tests.execute',
   'spec.tests.edit',
   'spec.tests.delete',
   'story.mockups.read',
   'story.mockups.create',
   'story.mockups.edit',
   'story.mockups.delete',
   'story.mockups.annotate',
   'sprint.tasks.assign',
   'test_scenario.interact_in.draft',
   'test_scenario.interact_in.ready',
   'test_scenario.interact_in.automated',
   'test_scenario.interact_in.passed',
   'test_scenario.interact_in.failed')),
 ('Executor', ('story.mockups.read',)),
 ('Validator', ('ideation.knowledge.read', 'story.mockups.read')),
 ('QA',
  ('ideation.knowledge.read',
   'spec.tests.execute',
   'spec.tests.edit',
   'spec.tests.delete',
   'story.mockups.read',
   'test_scenario.interact_in.draft',
   'test_scenario.interact_in.ready',
   'test_scenario.interact_in.automated',
   'test_scenario.interact_in.passed',
   'test_scenario.interact_in.failed')),
 ('Reporter', ('ideation.knowledge.read', 'story.mockups.read')),
 ('Sprint Manager', ('ideation.knowledge.read', 'story.mockups.read', 'sprint.tasks.assign')),
 ('Spec',
  ('ideation.knowledge.read',
   'ideation.knowledge.create',
   'ideation.knowledge.delete',
   'spec.tests.execute',
   'spec.tests.edit',
   'spec.tests.delete',
   'story.mockups.read',
   'story.mockups.create',
   'story.mockups.edit',
   'story.mockups.delete',
   'story.mockups.annotate',
   'test_scenario.interact_in.draft',
   'test_scenario.interact_in.ready',
   'test_scenario.interact_in.automated',
   'test_scenario.interact_in.passed',
   'test_scenario.interact_in.failed'))), historical_authorities=(('ideation.knowledge.read', 'ideation.entity.read'),
 ('ideation.knowledge.create', 'ideation.entity.edit_fields'),
 ('ideation.knowledge.delete', 'ideation.entity.edit_fields'),
 ('ideation.qa.delete', 'ideation.qa.answer'),
 ('refinement.qa.delete', 'refinement.qa.answer'),
 ('spec.qa.delete', 'spec.qa.answer'),
 ('sprint.qa.delete', 'sprint.qa.answer'),
 ('spec.tests.execute', 'spec.tests.update_status'),
 ('spec.tests.edit', 'spec.tests.create'),
 ('spec.tests.delete', 'spec.tests.create'),
 ('story.mockups.read', 'story.entity.read'),
 ('story.mockups.create', 'story.entity.edit_fields'),
 ('story.mockups.edit', 'story.entity.edit_fields'),
 ('story.mockups.delete', 'story.entity.edit_fields'),
 ('story.mockups.annotate', 'story.entity.edit_fields'),
 ('sprint.tasks.assign', 'sprint.entity.assign'),
 ('test_scenario.interact_in.draft', 'spec.tests.update_status'),
 ('test_scenario.interact_in.ready', 'spec.tests.update_status'),
 ('test_scenario.interact_in.automated', 'spec.tests.update_status'),
 ('test_scenario.interact_in.passed', 'spec.tests.update_status'),
 ('test_scenario.interact_in.failed', 'spec.tests.update_status')), recover_all_false_materialization=False, legacy_compatible=True),
PermissionIntroductionManifest(version='KG-OPERATIONS/v1', leaves=('kg.operations.health.read',
 'kg.operations.integrity.read',
 'kg.operations.integrity.reconcile',
 'kg.operations.integrity.backfill',
 'kg.operations.cognitive.read',
 'kg.operations.cognitive.skip',
 'kg.operations.cognitive.clear',
 'kg.operations.queue.read',
 'kg.operations.queue.reprocess',
 'kg.operations.audit.read',
 'kg.operations.schema.migrate',
 'kg.operations.tick.run',
 'kg.operations.global_outbox.read',
 'kg.operations.global_outbox.reprocess',
 'kg.operations.global_outbox.verify',
 'kg.operations.global_recovery.preflight',
 'kg.operations.global_recovery.confirm',
 'kg.operations.global_recovery.read',
 'kg.operations.global_recovery.cancel',
 'kg.operations.global_recovery.resume',
 'kg.operations.global_recovery.run',
 'kg.operations.historical.read',
 'kg.operations.historical.start',
 'kg.operations.historical.cancel',
 'kg.operations.node.boost',
 'kg.operations.settings.read',
 'kg.operations.settings.write',
 'kg.operations.rebuild.preflight',
 'kg.operations.rebuild.confirm',
 'kg.operations.rebuild.run',
 'kg.operations.quarantine.restore',
 'kg.operations.board.erase'), preset_grants=(('Full Control',
  ('kg.operations.health.read',
   'kg.operations.integrity.read',
   'kg.operations.integrity.reconcile',
   'kg.operations.integrity.backfill',
   'kg.operations.cognitive.read',
   'kg.operations.cognitive.skip',
   'kg.operations.cognitive.clear',
   'kg.operations.queue.read',
   'kg.operations.queue.reprocess',
   'kg.operations.audit.read',
   'kg.operations.schema.migrate',
   'kg.operations.tick.run',
   'kg.operations.global_outbox.read',
   'kg.operations.global_outbox.reprocess',
   'kg.operations.global_outbox.verify',
   'kg.operations.global_recovery.preflight',
   'kg.operations.global_recovery.confirm',
   'kg.operations.global_recovery.read',
   'kg.operations.global_recovery.cancel',
   'kg.operations.global_recovery.resume',
   'kg.operations.global_recovery.run',
   'kg.operations.historical.read',
   'kg.operations.historical.start',
   'kg.operations.historical.cancel',
   'kg.operations.node.boost',
   'kg.operations.settings.read',
   'kg.operations.settings.write',
   'kg.operations.rebuild.preflight',
   'kg.operations.rebuild.confirm',
   'kg.operations.rebuild.run',
   'kg.operations.quarantine.restore',
   'kg.operations.board.erase')),
 ('Executor', ()),
 ('Validator', ()),
 ('QA', ()),
 ('Reporter', ()),
 ('Sprint Manager', ()),
 ('Spec', ())), historical_authorities=(('kg.operations.health.read', 'kg.admin.settings_read'),
 ('kg.operations.integrity.read', 'kg.admin.settings_read'),
 ('kg.operations.integrity.reconcile', 'kg.admin.settings_write'),
 ('kg.operations.integrity.backfill', 'kg.admin.settings_write'),
 ('kg.operations.cognitive.read', 'kg.admin.settings_read'),
 ('kg.operations.cognitive.skip', 'kg.admin.settings_write'),
 ('kg.operations.cognitive.clear', 'kg.admin.settings_write'),
 ('kg.operations.queue.read', 'kg.admin.settings_read'),
 ('kg.operations.queue.reprocess', 'kg.admin.settings_write'),
 ('kg.operations.audit.read', 'kg.admin.settings_read'),
 ('kg.operations.schema.migrate', 'kg.admin.settings_write'),
 ('kg.operations.tick.run', 'kg.admin.settings_write'),
 ('kg.operations.global_outbox.read', 'kg.admin.settings_read'),
 ('kg.operations.global_outbox.reprocess', 'kg.admin.settings_write'),
 ('kg.operations.global_outbox.verify', 'kg.admin.settings_read'),
 ('kg.operations.global_recovery.preflight', 'kg.admin.settings_read'),
 ('kg.operations.global_recovery.confirm', 'kg.admin.settings_write'),
 ('kg.operations.global_recovery.read', 'kg.admin.settings_read'),
 ('kg.operations.global_recovery.cancel', 'kg.admin.settings_write'),
 ('kg.operations.global_recovery.resume', 'kg.admin.settings_write'),
 ('kg.operations.global_recovery.run', 'kg.admin.settings_write'),
 ('kg.operations.historical.read', 'kg.admin.historical_consolidation'),
 ('kg.operations.historical.start', 'kg.admin.historical_consolidation'),
 ('kg.operations.historical.cancel', 'kg.admin.historical_consolidation'),
 ('kg.operations.node.boost', 'kg.admin.settings_write'),
 ('kg.operations.settings.read', 'kg.admin.settings_read'),
 ('kg.operations.settings.write', 'kg.admin.settings_write'),
 ('kg.operations.rebuild.preflight', 'kg.admin.settings_read'),
 ('kg.operations.rebuild.confirm', 'kg.admin.settings_write'),
 ('kg.operations.rebuild.run', 'kg.admin.settings_write'),
 ('kg.operations.quarantine.restore', 'kg.admin.settings_write'),
 ('kg.operations.board.erase', 'kg.admin.wipe_board')), recover_all_false_materialization=False, legacy_compatible=True),
PermissionIntroductionManifest(version='SDLC-TRANSITIONS/v1', leaves=('ideation.move.draft_to_review',
 'ideation.move.draft_to_cancelled',
 'ideation.move.review_to_draft',
 'ideation.move.review_to_approved',
 'ideation.move.review_to_cancelled',
 'ideation.move.approved_to_review',
 'ideation.move.approved_to_evaluating',
 'ideation.move.approved_to_cancelled',
 'ideation.move.evaluating_to_approved',
 'ideation.move.evaluating_to_done',
 'ideation.move.evaluating_to_cancelled',
 'ideation.move.done_to_draft',
 'ideation.move.cancelled_to_draft',
 'refinement.move.draft_to_review',
 'refinement.move.draft_to_cancelled',
 'refinement.move.review_to_draft',
 'refinement.move.review_to_cancelled',
 'refinement.move.approved_to_review',
 'refinement.move.approved_to_cancelled',
 'refinement.move.done_to_draft',
 'refinement.move.cancelled_to_draft',
 'spec.move.draft_to_cancelled',
 'spec.move.review_to_draft',
 'spec.move.review_to_cancelled',
 'spec.move.approved_to_review',
 'spec.move.approved_to_cancelled',
 'spec.move.validated_to_approved',
 'spec.move.validated_to_cancelled',
 'spec.move.in_progress_to_validated',
 'spec.move.in_progress_to_draft',
 'spec.move.in_progress_to_cancelled',
 'spec.move.done_to_draft',
 'spec.move.cancelled_to_draft',
 'card.move.not_started_to_in_progress',
 'card.move.not_started_to_cancelled',
 'card.move.started_to_not_started',
 'card.move.started_to_validation',
 'card.move.started_to_on_hold',
 'card.move.started_to_cancelled',
 'card.move.in_progress_to_started',
 'card.move.in_progress_to_cancelled',
 'card.move.validation_to_in_progress',
 'card.move.on_hold_to_started',
 'card.move.on_hold_to_cancelled',
 'card.move.done_to_in_progress',
 'card.move.cancelled_to_not_started',
 'sprint.move.draft_to_cancelled',
 'sprint.move.active_to_draft',
 'sprint.move.active_to_cancelled',
 'sprint.move.review_to_active',
 'sprint.move.review_to_cancelled',
 'sprint.move.closed_to_draft',
 'sprint.move.cancelled_to_draft',
 'test_scenario.move.draft_to_ready',
 'test_scenario.move.draft_to_automated',
 'test_scenario.move.draft_to_passed',
 'test_scenario.move.draft_to_failed',
 'test_scenario.move.ready_to_draft',
 'test_scenario.move.ready_to_automated',
 'test_scenario.move.ready_to_passed',
 'test_scenario.move.ready_to_failed',
 'test_scenario.move.automated_to_ready',
 'test_scenario.move.automated_to_passed',
 'test_scenario.move.failed_to_ready',
 'test_scenario.move.failed_to_passed',
 'test_scenario.move.passed_to_ready',
 'ideation.interact_in.review',
 'ideation.interact_in.approved'), preset_grants=(('Full Control',
  ('ideation.move.draft_to_review',
   'ideation.move.draft_to_cancelled',
   'ideation.move.review_to_draft',
   'ideation.move.review_to_approved',
   'ideation.move.review_to_cancelled',
   'ideation.move.approved_to_review',
   'ideation.move.approved_to_evaluating',
   'ideation.move.approved_to_cancelled',
   'ideation.move.evaluating_to_approved',
   'ideation.move.evaluating_to_done',
   'ideation.move.evaluating_to_cancelled',
   'ideation.move.done_to_draft',
   'ideation.move.cancelled_to_draft',
   'refinement.move.draft_to_review',
   'refinement.move.draft_to_cancelled',
   'refinement.move.review_to_draft',
   'refinement.move.review_to_cancelled',
   'refinement.move.approved_to_review',
   'refinement.move.approved_to_cancelled',
   'refinement.move.done_to_draft',
   'refinement.move.cancelled_to_draft',
   'spec.move.draft_to_cancelled',
   'spec.move.review_to_draft',
   'spec.move.review_to_cancelled',
   'spec.move.approved_to_review',
   'spec.move.approved_to_cancelled',
   'spec.move.validated_to_approved',
   'spec.move.validated_to_cancelled',
   'spec.move.in_progress_to_validated',
   'spec.move.in_progress_to_draft',
   'spec.move.in_progress_to_cancelled',
   'spec.move.done_to_draft',
   'spec.move.cancelled_to_draft',
   'card.move.not_started_to_in_progress',
   'card.move.not_started_to_cancelled',
   'card.move.started_to_not_started',
   'card.move.started_to_validation',
   'card.move.started_to_on_hold',
   'card.move.started_to_cancelled',
   'card.move.in_progress_to_started',
   'card.move.in_progress_to_cancelled',
   'card.move.validation_to_in_progress',
   'card.move.on_hold_to_started',
   'card.move.on_hold_to_cancelled',
   'card.move.done_to_in_progress',
   'card.move.cancelled_to_not_started',
   'sprint.move.draft_to_cancelled',
   'sprint.move.active_to_draft',
   'sprint.move.active_to_cancelled',
   'sprint.move.review_to_active',
   'sprint.move.review_to_cancelled',
   'sprint.move.closed_to_draft',
   'sprint.move.cancelled_to_draft',
   'test_scenario.move.draft_to_ready',
   'test_scenario.move.draft_to_automated',
   'test_scenario.move.draft_to_passed',
   'test_scenario.move.draft_to_failed',
   'test_scenario.move.ready_to_draft',
   'test_scenario.move.ready_to_automated',
   'test_scenario.move.ready_to_passed',
   'test_scenario.move.ready_to_failed',
   'test_scenario.move.automated_to_ready',
   'test_scenario.move.automated_to_passed',
   'test_scenario.move.failed_to_ready',
   'test_scenario.move.failed_to_passed',
   'test_scenario.move.passed_to_ready',
   'ideation.interact_in.review',
   'ideation.interact_in.approved')),
 ('Executor',
  ('card.move.not_started_to_cancelled',
   'card.move.started_to_cancelled',
   'card.move.in_progress_to_cancelled',
   'card.move.on_hold_to_cancelled')),
 ('Validator', ('card.move.validation_to_in_progress',)),
 ('QA',
  ('card.move.not_started_to_in_progress',
   'card.move.not_started_to_cancelled',
   'card.move.started_to_cancelled',
   'card.move.in_progress_to_cancelled',
   'card.move.on_hold_to_cancelled',
   'test_scenario.move.draft_to_ready',
   'test_scenario.move.draft_to_automated',
   'test_scenario.move.draft_to_passed',
   'test_scenario.move.draft_to_failed',
   'test_scenario.move.ready_to_draft',
   'test_scenario.move.ready_to_automated',
   'test_scenario.move.ready_to_passed',
   'test_scenario.move.ready_to_failed',
   'test_scenario.move.automated_to_ready',
   'test_scenario.move.automated_to_passed',
   'test_scenario.move.failed_to_ready',
   'test_scenario.move.failed_to_passed',
   'test_scenario.move.passed_to_ready')),
 ('Reporter', ()),
 ('Sprint Manager',
  ('sprint.move.draft_to_cancelled',
   'sprint.move.active_to_draft',
   'sprint.move.active_to_cancelled',
   'sprint.move.review_to_active',
   'sprint.move.review_to_cancelled',
   'sprint.move.closed_to_draft',
   'sprint.move.cancelled_to_draft')),
 ('Spec',
  ('ideation.move.draft_to_review',
   'ideation.move.draft_to_cancelled',
   'ideation.move.review_to_draft',
   'ideation.move.review_to_approved',
   'ideation.move.review_to_cancelled',
   'ideation.move.approved_to_review',
   'ideation.move.approved_to_evaluating',
   'ideation.move.approved_to_cancelled',
   'ideation.move.evaluating_to_approved',
   'ideation.move.evaluating_to_done',
   'ideation.move.evaluating_to_cancelled',
   'ideation.move.done_to_draft',
   'ideation.move.cancelled_to_draft',
   'refinement.move.draft_to_review',
   'refinement.move.draft_to_cancelled',
   'refinement.move.review_to_draft',
   'refinement.move.review_to_cancelled',
   'refinement.move.approved_to_review',
   'refinement.move.approved_to_cancelled',
   'refinement.move.done_to_draft',
   'refinement.move.cancelled_to_draft',
   'spec.move.review_to_draft',
   'spec.move.approved_to_review',
   'spec.move.draft_to_cancelled',
   'spec.move.review_to_cancelled',
   'spec.move.approved_to_cancelled',
   'spec.move.validated_to_cancelled',
   'spec.move.in_progress_to_cancelled',
   'sprint.move.draft_to_cancelled',
   'sprint.move.active_to_cancelled',
   'sprint.move.review_to_cancelled',
   'test_scenario.move.draft_to_ready',
   'test_scenario.move.draft_to_automated',
   'test_scenario.move.draft_to_passed',
   'test_scenario.move.draft_to_failed',
   'test_scenario.move.ready_to_draft',
   'test_scenario.move.ready_to_automated',
   'test_scenario.move.ready_to_passed',
   'test_scenario.move.ready_to_failed',
   'test_scenario.move.automated_to_ready',
   'test_scenario.move.automated_to_passed',
   'test_scenario.move.failed_to_ready',
   'test_scenario.move.failed_to_passed',
   'test_scenario.move.passed_to_ready',
   'ideation.interact_in.review',
   'ideation.interact_in.approved'))), historical_authorities=(('ideation.move.draft_to_review', 'ideation.entity.read'),
 ('ideation.move.draft_to_cancelled', 'ideation.entity.read'),
 ('ideation.move.review_to_draft', 'ideation.entity.read'),
 ('ideation.move.review_to_approved', 'ideation.entity.read'),
 ('ideation.move.review_to_cancelled', 'ideation.entity.read'),
 ('ideation.move.approved_to_review', 'ideation.entity.read'),
 ('ideation.move.approved_to_evaluating', 'ideation.entity.read'),
 ('ideation.move.approved_to_cancelled', 'ideation.entity.read'),
 ('ideation.move.evaluating_to_approved', 'ideation.entity.read'),
 ('ideation.move.evaluating_to_done', 'ideation.entity.read'),
 ('ideation.move.evaluating_to_cancelled', 'ideation.entity.read'),
 ('ideation.move.done_to_draft', 'ideation.entity.read'),
 ('ideation.move.cancelled_to_draft', 'ideation.entity.read'),
 ('refinement.move.draft_to_review', 'refinement.entity.read'),
 ('refinement.move.draft_to_cancelled', 'refinement.entity.read'),
 ('refinement.move.review_to_draft', 'refinement.entity.read'),
 ('refinement.move.review_to_cancelled', 'refinement.entity.read'),
 ('refinement.move.approved_to_review', 'refinement.entity.read'),
 ('refinement.move.approved_to_cancelled', 'refinement.entity.read'),
 ('refinement.move.done_to_draft', 'refinement.entity.read'),
 ('refinement.move.cancelled_to_draft', 'refinement.entity.read'),
 ('spec.move.draft_to_cancelled', 'spec.entity.read'),
 ('spec.move.review_to_draft', 'spec.entity.read'),
 ('spec.move.review_to_cancelled', 'spec.entity.read'),
 ('spec.move.approved_to_review', 'spec.entity.read'),
 ('spec.move.approved_to_cancelled', 'spec.entity.read'),
 ('spec.move.validated_to_approved', 'spec.entity.read'),
 ('spec.move.validated_to_cancelled', 'spec.entity.read'),
 ('spec.move.in_progress_to_validated', 'spec.entity.read'),
 ('spec.move.in_progress_to_draft', 'spec.entity.read'),
 ('spec.move.in_progress_to_cancelled', 'spec.entity.read'),
 ('spec.move.done_to_draft', 'spec.entity.read'),
 ('spec.move.cancelled_to_draft', 'spec.entity.read'),
 ('card.move.not_started_to_in_progress', 'card.entity.read'),
 ('card.move.not_started_to_cancelled', 'card.entity.read'),
 ('card.move.started_to_not_started', 'card.entity.read'),
 ('card.move.started_to_validation', 'card.entity.read'),
 ('card.move.started_to_on_hold', 'card.entity.read'),
 ('card.move.started_to_cancelled', 'card.entity.read'),
 ('card.move.in_progress_to_started', 'card.entity.read'),
 ('card.move.in_progress_to_cancelled', 'card.entity.read'),
 ('card.move.validation_to_in_progress', 'card.entity.read'),
 ('card.move.on_hold_to_started', 'card.entity.read'),
 ('card.move.on_hold_to_cancelled', 'card.entity.read'),
 ('card.move.done_to_in_progress', 'card.entity.read'),
 ('card.move.cancelled_to_not_started', 'card.entity.read'),
 ('sprint.move.draft_to_cancelled', 'sprint.entity.read'),
 ('sprint.move.active_to_draft', 'sprint.entity.read'),
 ('sprint.move.active_to_cancelled', 'sprint.entity.read'),
 ('sprint.move.review_to_active', 'sprint.entity.read'),
 ('sprint.move.review_to_cancelled', 'sprint.entity.read'),
 ('sprint.move.closed_to_draft', 'sprint.entity.read'),
 ('sprint.move.cancelled_to_draft', 'sprint.entity.read'),
 ('test_scenario.move.draft_to_ready', 'spec.tests.update_status'),
 ('test_scenario.move.draft_to_automated', 'spec.tests.update_status'),
 ('test_scenario.move.draft_to_passed', 'spec.tests.update_status'),
 ('test_scenario.move.draft_to_failed', 'spec.tests.update_status'),
 ('test_scenario.move.ready_to_draft', 'spec.tests.update_status'),
 ('test_scenario.move.ready_to_automated', 'spec.tests.update_status'),
 ('test_scenario.move.ready_to_passed', 'spec.tests.update_status'),
 ('test_scenario.move.ready_to_failed', 'spec.tests.update_status'),
 ('test_scenario.move.automated_to_ready', 'spec.tests.update_status'),
 ('test_scenario.move.automated_to_passed', 'spec.tests.update_status'),
 ('test_scenario.move.failed_to_ready', 'spec.tests.update_status'),
 ('test_scenario.move.failed_to_passed', 'spec.tests.update_status'),
 ('test_scenario.move.passed_to_ready', 'spec.tests.update_status'),
 ('ideation.interact_in.review', 'ideation.entity.read'),
 ('ideation.interact_in.approved', 'ideation.entity.read')), recover_all_false_materialization=False, legacy_compatible=True),
PermissionIntroductionManifest(version='CODE-TRACEABILITY/v1', leaves=('code_traceability.investigation.start',
 'code_traceability.investigation.read',
 'code_traceability.investigation.receipt_submit',
 'code_traceability.investigation.revoke',
 'code_traceability.evidence.read',
 'code_traceability.evidence.submit',
 'code_traceability.evidence.supersede',
 'code_traceability.evidence.revoke',
 'code_traceability.spec_link.create',
 'code_traceability.spec_link.delete',
 'code_traceability.spec_link.set_disposition',
 'code_traceability.spec_link.rebase',
 'code_traceability.target.read',
 'code_traceability.target.suggest',
 'code_traceability.target.create',
 'code_traceability.target.edit',
 'code_traceability.target.resolution_submit',
 'code_traceability.target.execution_submit',
 'code_traceability.overlap.read',
 'code_traceability.overlap.acknowledge',
 'code_traceability.waiver.create',
 'code_traceability.waiver.clear'), preset_grants=(('Full Control',
  ('code_traceability.investigation.start',
   'code_traceability.investigation.read',
   'code_traceability.investigation.receipt_submit',
   'code_traceability.investigation.revoke',
   'code_traceability.evidence.read',
   'code_traceability.evidence.submit',
   'code_traceability.evidence.supersede',
   'code_traceability.evidence.revoke',
   'code_traceability.spec_link.create',
   'code_traceability.spec_link.delete',
   'code_traceability.spec_link.set_disposition',
   'code_traceability.spec_link.rebase',
   'code_traceability.target.read',
   'code_traceability.target.suggest',
   'code_traceability.target.create',
   'code_traceability.target.edit',
   'code_traceability.target.resolution_submit',
   'code_traceability.target.execution_submit',
   'code_traceability.overlap.read',
   'code_traceability.overlap.acknowledge',
   'code_traceability.waiver.create',
   'code_traceability.waiver.clear')),
 ('Executor',
  ('code_traceability.investigation.read',
   'code_traceability.evidence.read',
   'code_traceability.target.read',
   'code_traceability.overlap.read',
   'code_traceability.investigation.start',
   'code_traceability.investigation.receipt_submit',
   'code_traceability.evidence.submit',
   'code_traceability.evidence.supersede',
   'code_traceability.target.suggest',
   'code_traceability.target.create',
   'code_traceability.target.edit',
   'code_traceability.target.resolution_submit',
   'code_traceability.target.execution_submit')),
 ('Validator',
  ('code_traceability.investigation.read',
   'code_traceability.evidence.read',
   'code_traceability.target.read',
   'code_traceability.overlap.read')),
 ('QA',
  ('code_traceability.investigation.read',
   'code_traceability.evidence.read',
   'code_traceability.target.read',
   'code_traceability.overlap.read')),
 ('Reporter',
  ('code_traceability.investigation.read',
   'code_traceability.evidence.read',
   'code_traceability.target.read',
   'code_traceability.overlap.read')),
 ('Sprint Manager',
  ('code_traceability.investigation.read',
   'code_traceability.evidence.read',
   'code_traceability.target.read',
   'code_traceability.overlap.read',
   'code_traceability.overlap.acknowledge',
   'code_traceability.waiver.create',
   'code_traceability.waiver.clear')),
 ('Spec',
  ('code_traceability.investigation.read',
   'code_traceability.evidence.read',
   'code_traceability.target.read',
   'code_traceability.overlap.read',
   'code_traceability.investigation.revoke',
   'code_traceability.evidence.revoke',
   'code_traceability.spec_link.create',
   'code_traceability.spec_link.delete',
   'code_traceability.spec_link.set_disposition',
   'code_traceability.spec_link.rebase',
   'code_traceability.target.create',
   'code_traceability.target.edit',
   'code_traceability.overlap.acknowledge',
   'code_traceability.waiver.create',
   'code_traceability.waiver.clear'))), historical_authorities=(('code_traceability.investigation.read', 'board.read'),
 ('code_traceability.evidence.read', 'board.read'),
 ('code_traceability.target.read', 'board.read'),
 ('code_traceability.overlap.read', 'board.read'),
 ('code_traceability.investigation.revoke', 'spec.entity.edit_fields'),
 ('code_traceability.evidence.revoke', 'spec.entity.edit_fields'),
 ('code_traceability.spec_link.create', 'spec.entity.edit_fields'),
 ('code_traceability.spec_link.delete', 'spec.entity.edit_fields'),
 ('code_traceability.spec_link.set_disposition', 'spec.entity.edit_fields'),
 ('code_traceability.spec_link.rebase', 'spec.entity.edit_fields'),
 ('code_traceability.target.suggest', 'card.entity.edit_fields'),
 ('code_traceability.target.create', 'card.entity.edit_fields'),
 ('code_traceability.target.edit', 'card.entity.edit_fields'),
 ('code_traceability.overlap.acknowledge', 'card.entity.edit_fields'),
 ('code_traceability.waiver.create', 'card.entity.edit_fields'),
 ('code_traceability.waiver.clear', 'card.entity.edit_fields'),
 ('code_traceability.investigation.start', 'agent.entity.read'),
 ('code_traceability.investigation.receipt_submit', 'agent.entity.read'),
 ('code_traceability.evidence.submit', 'agent.entity.read'),
 ('code_traceability.evidence.supersede', 'agent.entity.read'),
 ('code_traceability.target.resolution_submit', 'agent.entity.read'),
 ('code_traceability.target.execution_submit', 'agent.entity.read')), recover_all_false_materialization=False, legacy_compatible=False),
PermissionIntroductionManifest(version='CODE-EVIDENCE-LEGACY-CLASSIFICATION/v1', leaves=('code_traceability.evidence.classify_legacy',), preset_grants=(('Full Control', ('code_traceability.evidence.classify_legacy',)),
 ('Executor', ()),
 ('Validator', ()),
 ('QA', ()),
 ('Reporter', ()),
 ('Sprint Manager', ()),
 ('Spec', ('code_traceability.evidence.classify_legacy',))), historical_authorities=(('code_traceability.evidence.classify_legacy', 'spec.entity.edit_fields'),), recover_all_false_materialization=False, legacy_compatible=False),
PermissionIntroductionManifest(version='SK-M/v1', leaves=('spec.entity.manage_dependencies',), preset_grants=(('Full Control', ('spec.entity.manage_dependencies',)),
 ('Executor', ()),
 ('Validator', ()),
 ('QA', ()),
 ('Reporter', ()),
 ('Sprint Manager', ()),
 ('Spec', ('spec.entity.manage_dependencies',))), historical_authorities=(('spec.entity.manage_dependencies', 'spec.entity.edit_fields'),), recover_all_false_materialization=False, legacy_compatible=False),
PermissionIntroductionManifest(version='TASK-REJECTED/v1', leaves=('card.interact_in.rejected', 'card.move.rejected_to_in_progress'), preset_grants=(('Full Control', ('card.interact_in.rejected', 'card.move.rejected_to_in_progress')),
 ('Executor', ('card.interact_in.rejected', 'card.move.rejected_to_in_progress')),
 ('Validator', ()),
 ('QA', ()),
 ('Reporter', ()),
 ('Sprint Manager', ()),
 ('Spec', ())), historical_authorities=(('card.interact_in.rejected', 'card.entity.read'),
 ('card.move.rejected_to_in_progress', 'card.entity.edit_fields')), recover_all_false_materialization=False, legacy_compatible=False),
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


_FAIL_CLOSED_INTRODUCED_FLAGS = frozenset(_INTRODUCED_PERMISSION_LEAVES)


PERMISSION_REGISTRY = {'agent': {'api_key': {'rotate': True},
           'board_access': {'edit': True, 'grant': True, 'read': True, 'revoke': True},
           'entity': {'create': True, 'delete': True, 'edit': True, 'read': True}},
 'amendment': {'coverage': {'confirm': True},
               'revision': {'associate': True, 'create': True, 'read': True, 'transition': True}},
 'board': {'activity_read': True,
           'admin': {'create': True, 'delete': True, 'edit': True},
           'analytics_read': True,
           'mentions_mark_seen': True,
           'mentions_read': True,
           'read': True,
           'share': {'create': True, 'edit': True, 'leave': True, 'read': True, 'revoke': True}},
 'card': {'activity_read': True,
          'architecture': {'create': True,
                           'delete': True,
                           'edit': True,
                           'import': True,
                           'read': True,
                           'render': True},
          'attachments': {'delete': True, 'read': True, 'upload': True},
          'comments': {'create': True,
                       'create_choice': True,
                       'delete': True,
                       'edit': True,
                       'get_responses': True,
                       'read': True,
                       'respond_choice': True},
          'conclusion': {'read': True, 'write': True},
          'copy_from_spec': {'architecture': True, 'knowledge': True, 'mockups': True, 'qa': True},
          'entity': {'assign': True,
                     'context_read': True,
                     'create': True,
                     'create_test': True,
                     'delete': True,
                     'edit_bug_fields': True,
                     'edit_fields': True,
                     'label': True,
                     'link_spec': True,
                     'link_tests': True,
                     'manage_dependencies': True,
                     'read': True},
          'interact_in': {'cancelled': True,
                          'done': True,
                          'in_progress': True,
                          'not_started': True,
                          'on_hold': True,
                          'rejected': True,
                          'started': True,
                          'validation': True},
          'link_to': {'contract': True,
                      'ir': True,
                      'or': True,
                      'rule': True,
                      'scenario': True,
                      'tr': True},
          'mockups': {'annotate': True, 'create': True, 'delete': True, 'edit': True, 'read': True},
          'move': {'cancelled_to_not_started': True,
                   'done_to_in_progress': True,
                   'in_progress_to_cancelled': True,
                   'in_progress_to_done': True,
                   'in_progress_to_on_hold': True,
                   'in_progress_to_started': True,
                   'in_progress_to_validation': True,
                   'not_started_to_cancelled': True,
                   'not_started_to_in_progress': True,
                   'not_started_to_started': True,
                   'on_hold_to_cancelled': True,
                   'on_hold_to_in_progress': True,
                   'on_hold_to_started': True,
                   'rejected_to_in_progress': True,
                   'started_to_cancelled': True,
                   'started_to_in_progress': True,
                   'started_to_not_started': True,
                   'started_to_on_hold': True,
                   'started_to_validation': True,
                   'validation_to_cancelled': True,
                   'validation_to_done': True,
                   'validation_to_in_progress': True,
                   'validation_to_on_hold': True},
          'qa': {'answer': True, 'ask': True, 'delete': True, 'read': True},
          'tests': {'link': True, 'read': True, 'update_status': True},
          'validation': {'delete': True, 'read': True, 'submit': True}},
 'code_traceability': {'evidence': {'classify_legacy': True,
                                    'read': True,
                                    'revoke': True,
                                    'submit': True,
                                    'supersede': True},
                       'investigation': {'read': True,
                                         'receipt_submit': True,
                                         'revoke': True,
                                         'start': True},
                       'overlap': {'acknowledge': True, 'read': True},
                       'spec_link': {'create': True,
                                     'delete': True,
                                     'rebase': True,
                                     'set_disposition': True},
                       'target': {'create': True,
                                  'edit': True,
                                  'execution_submit': True,
                                  'read': True,
                                  'resolution_submit': True,
                                  'suggest': True},
                       'waiver': {'clear': True, 'create': True}},
 'default_board_config': {'activate': True,
                          'candidates_read': True,
                          'create': True,
                          'deactivate': True,
                          'diff_read': True,
                          'export': True,
                          'guidelines': {'edit': True},
                          'import': True,
                          'read': True,
                          'set_design_system': True},
 'design_system': {'board_link': {'create': True, 'delete': True, 'read': True},
                   'entity': {'create': True, 'delete': True, 'edit': True, 'read': True},
                   'export': True,
                   'import': True},
 'guidelines': {'adoption': {'manage': True},
                'assessments': {'read': True, 'record': True},
                'create': True,
                'delete': True,
                'edit': True,
                'impact': {'preview': True},
                'link': True,
                'metrics': {'author': True},
                'read': True,
                'revisions': {'create': True, 'read': True, 'retire': True},
                'unlink': True,
                'waiver': {'read': True,
                           'request': True,
                           'revalidate': True,
                           'review': True,
                           'revoke': True}},
 'ideation': {'architecture': {'create': True,
                               'delete': True,
                               'edit': True,
                               'import': True,
                               'read': True,
                               'render': True},
              'entity': {'archive': True,
                         'assign': True,
                         'create': True,
                         'delete': True,
                         'edit_fields': True,
                         'evaluate': True,
                         'label': True,
                         'read': True,
                         'restore': True},
              'history_read': True,
              'interact_in': {'approved': True,
                              'cancelled': True,
                              'done': True,
                              'draft': True,
                              'evaluating': True,
                              'review': True},
              'knowledge': {'create': True, 'delete': True, 'read': True},
              'mockups': {'annotate': True,
                          'create': True,
                          'delete': True,
                          'edit': True,
                          'read': True},
              'move': {'approved_to_cancelled': True,
                       'approved_to_evaluating': True,
                       'approved_to_review': True,
                       'cancelled_to_draft': True,
                       'done_to_draft': True,
                       'draft_to_cancelled': True,
                       'draft_to_review': True,
                       'evaluating_to_approved': True,
                       'evaluating_to_cancelled': True,
                       'evaluating_to_done': True,
                       'review_to_approved': True,
                       'review_to_cancelled': True,
                       'review_to_draft': True},
              'qa': {'answer': True, 'ask': True, 'ask_choice': True, 'delete': True, 'read': True},
              'quality': {'assess': True, 'read': True},
              'specs_derive': True,
              'versions_read': True},
 'kg': {'admin': {'historical_consolidation': True,
                  'settings_read': True,
                  'settings_write': True,
                  'wipe_board': True},
        'operations': {'audit': {'read': True},
                       'board': {'erase': True},
                       'cognitive': {'clear': True, 'read': True, 'skip': True},
                       'global_outbox': {'read': True, 'reprocess': True, 'verify': True},
                       'global_recovery': {'cancel': True,
                                           'confirm': True,
                                           'preflight': True,
                                           'read': True,
                                           'resume': True,
                                           'run': True},
                       'health': {'read': True},
                       'historical': {'cancel': True, 'read': True, 'start': True},
                       'integrity': {'backfill': True, 'read': True, 'reconcile': True},
                       'node': {'boost': True},
                       'quarantine': {'restore': True},
                       'queue': {'read': True, 'reprocess': True},
                       'rebuild': {'confirm': True, 'preflight': True, 'run': True},
                       'schema': {'migrate': True},
                       'settings': {'read': True, 'write': True},
                       'tick': {'run': True}},
        'power': {'cypher': True, 'natural': True, 'schema_info': True},
        'query': {'alternatives': True,
                  'constraint_explain': True,
                  'contradictions': True,
                  'decision_history': True,
                  'global': True,
                  'learning_from_bugs': True,
                  'related_context': True,
                  'similar_decisions': True,
                  'supersedence_chain': True},
        'session': {'abort': True,
                    'add_edge': True,
                    'add_node': True,
                    'begin': True,
                    'commit': True,
                    'get_similar': True,
                    'propose': True}},
 'metrics': {'local': {'events': {'create': True},
                       'export': True,
                       'purge': True,
                       'summary': {'read': True}},
             'publish_health': {'read': True},
             'settings': {'edit': True, 'migration_notice_seen': True}},
 'permission_preset': {'clone': True,
                       'entity': {'create': True, 'delete': True, 'edit': True, 'read': True},
                       'export': True,
                       'import': True},
 'profile': {'update': True},
 'refinement': {'architecture': {'create': True,
                                 'delete': True,
                                 'edit': True,
                                 'import': True,
                                 'read': True,
                                 'render': True},
                'entity': {'archive': True,
                           'assign': True,
                           'create': True,
                           'delete': True,
                           'edit_fields': True,
                           'label': True,
                           'read': True,
                           'restore': True},
                'history_read': True,
                'interact_in': {'approved': True,
                                'cancelled': True,
                                'done': True,
                                'draft': True,
                                'review': True},
                'knowledge': {'create': True, 'delete': True, 'read': True},
                'mockups': {'annotate': True,
                            'create': True,
                            'delete': True,
                            'edit': True,
                            'read': True},
                'move': {'approved_to_cancelled': True,
                         'approved_to_done': True,
                         'approved_to_review': True,
                         'cancelled_to_draft': True,
                         'done_to_draft': True,
                         'draft_to_cancelled': True,
                         'draft_to_review': True,
                         'review_to_approved': True,
                         'review_to_cancelled': True,
                         'review_to_draft': True},
                'qa': {'answer': True,
                       'ask': True,
                       'ask_choice': True,
                       'delete': True,
                       'read': True},
                'quality': {'assess': True, 'read': True},
                'research_decisions': {'append': True, 'read': True},
                'specs_derive': True,
                'versions_read': True},
 'runtime': {'settings': {'read': True, 'write': True}},
 'spec': {'architecture': {'create': True,
                           'delete': True,
                           'edit': True,
                           'import': True,
                           'read': True,
                           'render': True},
          'cards_derive': True,
          'checklist': {'execute': True, 'read': True},
          'contracts': {'create': True, 'delete': True, 'edit': True, 'read': True},
          'entity': {'archive': True,
                     'assign': True,
                     'create': True,
                     'delete': True,
                     'edit_coverage_flags': True,
                     'edit_fields': True,
                     'label': True,
                     'link_card': True,
                     'manage_dependencies': True,
                     'read': True,
                     'restore': True},
          'evaluations': {'delete': True, 'read': True, 'submit': True},
          'history_read': True,
          'integration_requirements': {'create': True,
                                       'delete': True,
                                       'edit': True,
                                       'link_task': True,
                                       'read': True},
          'interact_in': {'approved': True,
                          'cancelled': True,
                          'done': True,
                          'draft': True,
                          'in_progress': True,
                          'review': True,
                          'validated': True},
          'knowledge': {'create': True, 'delete': True, 'read': True},
          'mockups': {'annotate': True, 'create': True, 'delete': True, 'edit': True, 'read': True},
          'move': {'approved_to_cancelled': True,
                   'approved_to_draft': True,
                   'approved_to_review': True,
                   'approved_to_validated': True,
                   'cancelled_to_draft': True,
                   'done_to_draft': True,
                   'draft_to_cancelled': True,
                   'draft_to_review': True,
                   'in_progress_to_cancelled': True,
                   'in_progress_to_done': True,
                   'in_progress_to_draft': True,
                   'in_progress_to_validated': True,
                   'review_to_approved': True,
                   'review_to_cancelled': True,
                   'review_to_draft': True,
                   'validated_to_approved': True,
                   'validated_to_cancelled': True,
                   'validated_to_draft': True,
                   'validated_to_in_progress': True},
          'observability_requirements': {'create': True,
                                         'delete': True,
                                         'edit': True,
                                         'link_task': True,
                                         'read': True},
          'qa': {'answer': True, 'ask': True, 'ask_choice': True, 'delete': True, 'read': True},
          'quality': {'assess': True, 'read': True},
          'rules': {'create': True, 'delete': True, 'edit': True, 'read': True},
          'structured_entity': {'acceptance_criterion': {'create': True,
                                                         'link_task': True,
                                                         'reorder': True,
                                                         'restore': True,
                                                         'revoke': True,
                                                         'supersede': True,
                                                         'unlink_task': True,
                                                         'update': True},
                                'api_contract': {'create': True,
                                                 'link_task': True,
                                                 'reorder': True,
                                                 'restore': True,
                                                 'revoke': True,
                                                 'supersede': True,
                                                 'unlink_task': True,
                                                 'update': True},
                                'business_rule': {'create': True,
                                                  'link_task': True,
                                                  'reorder': True,
                                                  'restore': True,
                                                  'revoke': True,
                                                  'supersede': True,
                                                  'unlink_task': True,
                                                  'update': True},
                                'decision': {'create': True,
                                             'link_task': True,
                                             'reorder': True,
                                             'restore': True,
                                             'revoke': True,
                                             'supersede': True,
                                             'unlink_task': True,
                                             'update': True},
                                'functional_requirement': {'create': True,
                                                           'link_task': True,
                                                           'reorder': True,
                                                           'restore': True,
                                                           'revoke': True,
                                                           'supersede': True,
                                                           'unlink_task': True,
                                                           'update': True},
                                'integration_requirement': {'create': True,
                                                            'link_task': True,
                                                            'reorder': True,
                                                            'restore': True,
                                                            'revoke': True,
                                                            'supersede': True,
                                                            'unlink_task': True,
                                                            'update': True},
                                'observability_requirement': {'create': True,
                                                              'link_task': True,
                                                              'reorder': True,
                                                              'restore': True,
                                                              'revoke': True,
                                                              'supersede': True,
                                                              'unlink_task': True,
                                                              'update': True},
                                'project_structure_node': {'create': True,
                                                           'link_evidence': True,
                                                           'link_task': True,
                                                           'link_test': True,
                                                           'reorder': True,
                                                           'restore': True,
                                                           'revoke': True,
                                                           'unlink_evidence': True,
                                                           'unlink_task': True,
                                                           'unlink_test': True,
                                                           'update': True},
                                'technical_requirement': {'create': True,
                                                          'link_task': True,
                                                          'reorder': True,
                                                          'restore': True,
                                                          'revoke': True,
                                                          'supersede': True,
                                                          'unlink_task': True,
                                                          'update': True}},
          'tests': {'create': True,
                    'delete': True,
                    'edit': True,
                    'execute': True,
                    'read': True,
                    'update_status': True},
          'validation': {'delete': True, 'read': True, 'submit': True}},
 'sprint': {'entity': {'archive': True,
                       'assign': True,
                       'create': True,
                       'delete': True,
                       'edit_coverage_flags': True,
                       'edit_fields': True,
                       'label': True,
                       'read': True,
                       'restore': True},
            'evaluations': {'delete': True, 'read': True, 'submit': True},
            'history_read': True,
            'interact_in': {'active': True,
                            'cancelled': True,
                            'closed': True,
                            'draft': True,
                            'review': True},
            'move': {'active_to_cancelled': True,
                     'active_to_draft': True,
                     'active_to_review': True,
                     'cancelled_to_draft': True,
                     'closed_to_draft': True,
                     'draft_to_active': True,
                     'draft_to_cancelled': True,
                     'review_to_active': True,
                     'review_to_cancelled': True,
                     'review_to_closed': True},
            'qa': {'answer': True, 'ask': True, 'delete': True, 'read': True},
            'tasks': {'assign': True}},
 'story': {'conversion': {'to_ideation': True},
           'entity': {'archive': True,
                      'assign': True,
                      'create': True,
                      'delete': True,
                      'edit_fields': True,
                      'label': True,
                      'read': True,
                      'restore': True},
           'history_read': True,
           'interact_in': {'archived': True,
                           'converted': True,
                           'draft': True,
                           'ready': True,
                           'triage': True},
           'links': {'ideation': True},
           'mockups': {'annotate': True,
                       'create': True,
                       'delete': True,
                       'edit': True,
                       'read': True},
           'move': {'draft_to_ready': True,
                    'draft_to_triage': True,
                    'ready_to_triage': True,
                    'triage_to_draft': True,
                    'triage_to_ready': True}},
 'test_scenario': {'interact_in': {'automated': True,
                                   'draft': True,
                                   'failed': True,
                                   'passed': True,
                                   'ready': True},
                   'move': {'automated_to_passed': True,
                            'automated_to_ready': True,
                            'draft_to_automated': True,
                            'draft_to_failed': True,
                            'draft_to_passed': True,
                            'draft_to_ready': True,
                            'failed_to_passed': True,
                            'failed_to_ready': True,
                            'passed_to_ready': True,
                            'ready_to_automated': True,
                            'ready_to_draft': True,
                            'ready_to_failed': True,
                            'ready_to_passed': True}},
 'topic': {'entity': {'archive': True,
                      'create': True,
                      'delete': True,
                      'edit_fields': True,
                      'merge': True,
                      'read': True,
                      'restore': True}}}


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


LEGACY_PERMISSION_MAP = {'attachments:delete': ['card.attachments.delete'],
 'attachments:upload': ['card.attachments.upload'],
 'board:read': ['board.read',
                'board.activity_read',
                'board.analytics_read',
                'board.mentions_read',
                'board.mentions_mark_seen',
                'story.entity.read',
                'story.history_read',
                'topic.entity.read',
                'ideation.architecture.read',
                'refinement.architecture.read',
                'spec.architecture.read',
                'card.architecture.read'],
 'cards:create': ['card.entity.create', 'card.entity.create_test'],
 'cards:delete': ['card.entity.delete'],
 'cards:move': ['card.move.not_started_to_started',
                'card.move.not_started_to_in_progress',
                'card.move.not_started_to_cancelled',
                'card.move.started_to_not_started',
                'card.move.started_to_in_progress',
                'card.move.started_to_validation',
                'card.move.started_to_on_hold',
                'card.move.started_to_cancelled',
                'card.move.in_progress_to_started',
                'card.move.in_progress_to_validation',
                'card.move.in_progress_to_done',
                'card.move.in_progress_to_on_hold',
                'card.move.in_progress_to_cancelled',
                'card.move.validation_to_in_progress',
                'card.move.validation_to_done',
                'card.move.validation_to_on_hold',
                'card.move.validation_to_cancelled',
                'card.move.on_hold_to_started',
                'card.move.on_hold_to_in_progress',
                'card.move.on_hold_to_cancelled',
                'card.move.done_to_in_progress',
                'card.move.rejected_to_in_progress',
                'card.move.cancelled_to_not_started'],
 'cards:update': ['card.entity.edit_fields',
                  'card.entity.edit_bug_fields',
                  'card.entity.assign',
                  'card.entity.label',
                  'card.entity.link_spec',
                  'card.entity.link_tests',
                  'card.entity.manage_dependencies',
                  'card.copy_from_spec.mockups',
                  'card.copy_from_spec.knowledge',
                  'card.copy_from_spec.qa',
                  'card.copy_from_spec.architecture',
                  'card.architecture.create',
                  'card.architecture.edit',
                  'card.architecture.delete',
                  'card.architecture.import',
                  'card.architecture.render',
                  'card.link_to.scenario',
                  'card.link_to.tr',
                  'card.link_to.rule',
                  'card.link_to.contract',
                  'card.link_to.ir',
                  'card.link_to.or'],
 'comments:create': ['card.comments.create',
                     'card.comments.create_choice',
                     'card.comments.respond_choice'],
 'comments:delete': ['card.comments.delete'],
 'comments:update': ['card.comments.edit'],
 'qa:answer': ['card.qa.answer',
               'spec.qa.answer',
               'ideation.qa.answer',
               'refinement.qa.answer',
               'sprint.qa.answer'],
 'qa:create': ['card.qa.ask',
               'spec.qa.ask',
               'spec.qa.ask_choice',
               'ideation.qa.ask',
               'ideation.qa.ask_choice',
               'refinement.qa.ask',
               'refinement.qa.ask_choice',
               'sprint.qa.ask'],
 'qa:delete': ['card.qa.delete'],
 'self:update': ['profile.update'],
 'specs:create': ['story.entity.create',
                  'topic.entity.create',
                  'spec.entity.create',
                  'sprint.entity.create'],
 'specs:delete': ['story.entity.delete', 'topic.entity.delete', 'spec.entity.delete'],
 'specs:evaluate': ['spec.evaluations.submit',
                    'spec.evaluations.delete',
                    'spec.validation.submit',
                    'spec.validation.read',
                    'sprint.evaluations.submit',
                    'sprint.evaluations.delete'],
 'specs:move': ['story.move.draft_to_triage',
                'story.move.draft_to_ready',
                'story.move.triage_to_draft',
                'story.move.triage_to_ready',
                'story.move.ready_to_triage',
                'ideation.move.draft_to_review',
                'ideation.move.draft_to_cancelled',
                'ideation.move.review_to_draft',
                'ideation.move.review_to_approved',
                'ideation.move.review_to_cancelled',
                'ideation.move.approved_to_review',
                'ideation.move.approved_to_evaluating',
                'ideation.move.approved_to_cancelled',
                'ideation.move.evaluating_to_approved',
                'ideation.move.evaluating_to_done',
                'ideation.move.evaluating_to_cancelled',
                'ideation.move.done_to_draft',
                'ideation.move.cancelled_to_draft',
                'refinement.move.draft_to_review',
                'refinement.move.draft_to_cancelled',
                'refinement.move.review_to_draft',
                'refinement.move.review_to_approved',
                'refinement.move.review_to_cancelled',
                'refinement.move.approved_to_review',
                'refinement.move.approved_to_done',
                'refinement.move.approved_to_cancelled',
                'refinement.move.done_to_draft',
                'refinement.move.cancelled_to_draft',
                'spec.move.draft_to_review',
                'spec.move.draft_to_cancelled',
                'spec.move.review_to_draft',
                'spec.move.review_to_approved',
                'spec.move.review_to_cancelled',
                'spec.move.approved_to_review',
                'spec.move.approved_to_validated',
                'spec.move.approved_to_draft',
                'spec.move.approved_to_cancelled',
                'spec.move.validated_to_approved',
                'spec.move.validated_to_in_progress',
                'spec.move.validated_to_draft',
                'spec.move.validated_to_cancelled',
                'spec.move.in_progress_to_validated',
                'spec.move.in_progress_to_draft',
                'spec.move.in_progress_to_done',
                'spec.move.in_progress_to_cancelled',
                'spec.move.done_to_draft',
                'spec.move.cancelled_to_draft',
                'sprint.move.draft_to_active',
                'sprint.move.draft_to_cancelled',
                'sprint.move.active_to_draft',
                'sprint.move.active_to_review',
                'sprint.move.active_to_cancelled',
                'sprint.move.review_to_active',
                'sprint.move.review_to_closed',
                'sprint.move.review_to_cancelled',
                'sprint.move.closed_to_draft',
                'sprint.move.cancelled_to_draft'],
 'specs:update': ['story.entity.edit_fields',
                  'story.entity.assign',
                  'story.entity.label',
                  'story.entity.archive',
                  'story.entity.restore',
                  'story.links.ideation',
                  'story.conversion.to_ideation',
                  'topic.entity.edit_fields',
                  'topic.entity.archive',
                  'topic.entity.restore',
                  'topic.entity.merge',
                  'spec.entity.edit_fields',
                  'spec.entity.edit_coverage_flags',
                  'spec.entity.assign',
                  'spec.entity.label',
                  'spec.entity.link_card',
                  'spec.entity.manage_dependencies',
                  'spec.tests.create',
                  'spec.tests.update_status',
                  'spec.rules.create',
                  'spec.rules.edit',
                  'spec.rules.delete',
                  'spec.contracts.create',
                  'spec.contracts.edit',
                  'spec.contracts.delete',
                  'spec.integration_requirements.create',
                  'spec.integration_requirements.edit',
                  'spec.integration_requirements.delete',
                  'spec.integration_requirements.link_task',
                  'spec.observability_requirements.create',
                  'spec.observability_requirements.edit',
                  'spec.observability_requirements.delete',
                  'spec.observability_requirements.link_task',
                  'spec.mockups.create',
                  'spec.mockups.edit',
                  'spec.mockups.delete',
                  'spec.mockups.annotate',
                  'ideation.architecture.create',
                  'ideation.architecture.edit',
                  'ideation.architecture.delete',
                  'ideation.architecture.import',
                  'ideation.architecture.render',
                  'refinement.architecture.create',
                  'refinement.architecture.edit',
                  'refinement.architecture.delete',
                  'refinement.architecture.import',
                  'refinement.architecture.render',
                  'spec.architecture.create',
                  'spec.architecture.edit',
                  'spec.architecture.delete',
                  'spec.architecture.import',
                  'spec.architecture.render',
                  'spec.knowledge.create',
                  'spec.knowledge.delete',
                  'spec.cards_derive',
                  'spec.structured_entity.functional_requirement.create',
                  'spec.structured_entity.functional_requirement.update',
                  'spec.structured_entity.functional_requirement.revoke',
                  'spec.structured_entity.functional_requirement.supersede',
                  'spec.structured_entity.functional_requirement.restore',
                  'spec.structured_entity.functional_requirement.reorder',
                  'spec.structured_entity.functional_requirement.link_task',
                  'spec.structured_entity.functional_requirement.unlink_task',
                  'spec.structured_entity.business_rule.create',
                  'spec.structured_entity.business_rule.update',
                  'spec.structured_entity.business_rule.revoke',
                  'spec.structured_entity.business_rule.supersede',
                  'spec.structured_entity.business_rule.restore',
                  'spec.structured_entity.business_rule.reorder',
                  'spec.structured_entity.business_rule.link_task',
                  'spec.structured_entity.business_rule.unlink_task',
                  'spec.structured_entity.technical_requirement.create',
                  'spec.structured_entity.technical_requirement.update',
                  'spec.structured_entity.technical_requirement.revoke',
                  'spec.structured_entity.technical_requirement.supersede',
                  'spec.structured_entity.technical_requirement.restore',
                  'spec.structured_entity.technical_requirement.reorder',
                  'spec.structured_entity.technical_requirement.link_task',
                  'spec.structured_entity.technical_requirement.unlink_task',
                  'spec.structured_entity.decision.create',
                  'spec.structured_entity.decision.update',
                  'spec.structured_entity.decision.revoke',
                  'spec.structured_entity.decision.supersede',
                  'spec.structured_entity.decision.restore',
                  'spec.structured_entity.decision.reorder',
                  'spec.structured_entity.decision.link_task',
                  'spec.structured_entity.decision.unlink_task',
                  'spec.structured_entity.acceptance_criterion.create',
                  'spec.structured_entity.acceptance_criterion.update',
                  'spec.structured_entity.acceptance_criterion.revoke',
                  'spec.structured_entity.acceptance_criterion.supersede',
                  'spec.structured_entity.acceptance_criterion.restore',
                  'spec.structured_entity.acceptance_criterion.reorder',
                  'spec.structured_entity.acceptance_criterion.link_task',
                  'spec.structured_entity.acceptance_criterion.unlink_task',
                  'spec.structured_entity.api_contract.create',
                  'spec.structured_entity.api_contract.update',
                  'spec.structured_entity.api_contract.revoke',
                  'spec.structured_entity.api_contract.supersede',
                  'spec.structured_entity.api_contract.restore',
                  'spec.structured_entity.api_contract.reorder',
                  'spec.structured_entity.api_contract.link_task',
                  'spec.structured_entity.api_contract.unlink_task',
                  'spec.structured_entity.integration_requirement.create',
                  'spec.structured_entity.integration_requirement.update',
                  'spec.structured_entity.integration_requirement.revoke',
                  'spec.structured_entity.integration_requirement.supersede',
                  'spec.structured_entity.integration_requirement.restore',
                  'spec.structured_entity.integration_requirement.reorder',
                  'spec.structured_entity.integration_requirement.link_task',
                  'spec.structured_entity.integration_requirement.unlink_task',
                  'spec.structured_entity.observability_requirement.create',
                  'spec.structured_entity.observability_requirement.update',
                  'spec.structured_entity.observability_requirement.revoke',
                  'spec.structured_entity.observability_requirement.supersede',
                  'spec.structured_entity.observability_requirement.restore',
                  'spec.structured_entity.observability_requirement.reorder',
                  'spec.structured_entity.observability_requirement.link_task',
                  'spec.structured_entity.observability_requirement.unlink_task',
                  'spec.structured_entity.project_structure_node.create',
                  'spec.structured_entity.project_structure_node.update',
                  'spec.structured_entity.project_structure_node.revoke',
                  'spec.structured_entity.project_structure_node.restore',
                  'spec.structured_entity.project_structure_node.reorder',
                  'spec.structured_entity.project_structure_node.link_task',
                  'spec.structured_entity.project_structure_node.unlink_task',
                  'spec.structured_entity.project_structure_node.link_test',
                  'spec.structured_entity.project_structure_node.unlink_test',
                  'spec.structured_entity.project_structure_node.link_evidence',
                  'spec.structured_entity.project_structure_node.unlink_evidence']}


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
        for state in _TEST_SCENARIO_STATUSES:
            _set_nested(flags, f"test_scenario.interact_in.{state}", True)

    return flags


def _set_all_flags(d: dict[str, Any], value: bool) -> dict[str, Any]:
    """Set all leaf values in a nested dict to a specific value."""
    for key in d:
        if isinstance(d[key], dict):
            _set_all_flags(d[key], value)
        else:
            d[key] = value
    return d


def _set_all_leaves(d: dict, value: bool) -> None:
    for k, v in d.items():
        if isinstance(v, dict):
            _set_all_leaves(v, value)
        else:
            d[k] = value


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
        if _permission_value_presence(working, f"{entity_type}.move.any_to_cancelled")
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
