"""BASE T03: authenticated planner cannot turn content authority into approval."""
from copy import deepcopy
import json

import pytest
from fastmcp import Client
from sqlalchemy import func, select, update

from okto_pulse.community.adapters.mcp_host import CommunityMcpHostProvider
from okto_pulse.community.adapters.sqlalchemy_models import (
    Agent, AgentBoard, Board, Spec, QualityAssessmentReceiptRow, ProjectStructureMutationReceiptRow,
    PermissionPreset,
)
from okto_pulse.community.adapters.sqlalchemy_unit_of_work import CommunityUnitOfWorkFactory
from okto_pulse.community.adapters.sqlalchemy_structured_spec import CommunitySqlAlchemyStructuredSpecStore
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.domain.architecture_adoption import ArchitectureAdoptionScope
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.manifest import tool_inventory_document
from okto_pulse.core.ports import McpCredential
from okto_pulse.core.ports.mcp_resources import freeze_mcp_resource_catalog
from okto_pulse.core.ports.structured_spec import register_structured_spec_store
from okto_pulse.core.ports.permission_policy import (
    builtin_permission_presets, flatten_permission_flags, get_permission_flag,
    registered_permission_flags, set_permission_flag,
)
from okto_pulse.core.runtime_registry import register_unit_of_work_factory
from test_r08d_auth_replay_gates import _harness_env, _seed, _db_mod
from test_spec_evaluation_rest_parity import _direct_spec_context_fields, _evaluation_payload
from spec_planning_fixtures import manifest_payload

__all__ = ['_harness_env']


@pytest.mark.asyncio
@pytest.mark.parametrize("review,grant", [("approve", True), ("reject", True), ("approve", False)])
async def test_authenticated_mcp_composed_conclusion(_harness_env, monkeypatch, review, grant):
    from datetime import datetime, timezone
    from okto_pulse.community.adapters.sqlalchemy_semantic_guideline_assessment import (
        CommunitySqlAlchemySemanticGuidelineAssessment,
    )
    from okto_pulse.core.domain.guideline_policy import PolicyEntityType
    from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
    from okto_pulse.community.adapters.sqlalchemy_checklist import CommunitySqlAlchemyChecklist
    from okto_pulse.core.domain.checklist import (
        ChecklistBinding, ChecklistMode, ChecklistTargetType, ChecklistPhase,
    )
    from test_e2e_regression_hardening import (
        _install_current_external_requirement_lint, _validation_payload,
    )

    await _seed(_harness_env)
    _install_current_external_requirement_lint(monkeypatch)
    factory = _db_mod.get_session_factory()
    register_unit_of_work_factory(CommunityUnitOfWorkFactory(factory))
    from okto_pulse.community.adapters.sqlalchemy_validation_cycle import CommunitySqlAlchemyValidationCycleReader
    from okto_pulse.core.ports.validation_cycle import register_validation_cycle_reader
    register_validation_cycle_reader(CommunitySqlAlchemyValidationCycleReader(factory))
    async with factory() as db:
        root = next(row for row in builtin_permission_presets() if row['name'] == 'Full Control')
        db.add(PermissionPreset(id='compose-root', name=root['name'], flags=root['flags'], is_builtin=True))
        agent = await db.get(Agent, 'A1')
        agent.preset_id = 'compose-root'
        agent.permission_flags = {} if grant else {'spec': {'evaluations': {'submit': False}}}
        db.add(AgentBoard(id='compose-other-board', agent_id='A1', board_id='B2', granted_by='owner'))
        db.add(Spec(
            id='other-board-spec', board_id='B2', title='Other Board content', created_by='A2',
            architecture_adoption=ArchitectureAdoptionScope(
                board_id='B2', spec_id='other-board-spec', adopted_in_edition=1,
                actor_id='A2', inherited_resource_ids=(),
            ).model_dump(mode='json'),
        ))
        db.add(Spec(
            architecture_adoption=ArchitectureAdoptionScope(
                board_id='B1', spec_id='composed', adopted_in_edition=1,
                actor_id='A1', inherited_resource_ids=(),
            ).model_dump(mode='json'),
            id='composed', board_id='B1', title='Composed planning',
            status=SpecStatus.APPROVED, created_by='A1',
            functional_requirements=[], acceptance_criteria=[], test_scenarios=[],
            business_rules=[], technical_requirements=[], api_contracts=[],
            integration_requirements=[], observability_requirements=[], decisions=[],
        ))
        await db.flush()
        await CommunitySqlAlchemySemanticGuidelineAssessment(db).record_semantic_subject_mutation(
            board_id='B1', entity_type=PolicyEntityType.SPEC, subject_id='composed',
            actor_id='A1', idempotency_key='fixture-composed',
            request_digest=canonical_sha256({'fixture': 'composed'}),
            changed_at=datetime.now(timezone.utc),
        )
        checklist = CommunitySqlAlchemyChecklist(db)
        await checklist.apply_binding_cas(
            ChecklistBinding(board_id='B1', mode=ChecklistMode.OFF, version=1),
            expected_version=0, expected_digest=None,
        )
        await checklist.freeze_validation_binding(
            board_id='B1', spec_id='composed', spec_edition=1,
            target_type=ChecklistTargetType.SPEC, phase=ChecklistPhase.SPEC_VALIDATION,
        )
        await db.commit()
    monkeypatch.setattr(server, 'active_api_key_credential',
                        lambda: McpCredential(source='x_api_key_header', value='kA1'))
    frozen = freeze_mcp_resource_catalog(server.effective_resource_catalog())
    host = CommunityMcpHostProvider().materialize_catalog(
        server.mcp, resource_catalog=frozen, projection_identity=frozen.identity,
    )
    async with Client(host) as client:
        cycle_response = await client.call_tool('okto_pulse_get_spec_validation_cycle', {
            'board_id': 'B1', 'spec_id': 'composed',
        })
        cycle = json.loads(cycle_response.content[0].text)['data']
        manifest = cycle['planning_manifest']
        denied_scope = await client.call_tool('okto_pulse_get_spec_validation_cycle', {
            'board_id': 'B1', 'spec_id': 'other-board-spec',
        }, raise_on_error=False)
        assert 'validation_subject_not_found' in str(denied_scope), denied_scope
        assert 'Other Board content' not in str(denied_scope)
        response = await client.call_tool('okto_pulse_submit_spec_validation', {
            'idempotency_key': 'mcp-composition',
            'expected_planning_manifest': manifest,
            'board_id': 'B1', 'spec_id': 'composed', **_validation_payload(),
            'decomposition': _evaluation_payload(review),
        }, raise_on_error=False)
        if grant:
            replay = await client.call_tool('okto_pulse_submit_spec_validation', {
                'idempotency_key': 'mcp-composition',
                'expected_planning_manifest': manifest,
                'board_id': 'B1', 'spec_id': 'composed', **_validation_payload(),
                'decomposition': _evaluation_payload(review),
            }, raise_on_error=False)
            assert not replay.is_error, replay
            assert json.loads(replay.content[0].text)['data'] == json.loads(response.content[0].text)['data']
    async with factory() as db:
        spec = await db.get(Spec, 'composed')
        if not grant:
            assert 'spec.evaluations.submit' in str(response), response
            assert spec.status == SpecStatus.APPROVED
            assert not spec.evaluations and not spec.validations
            return
        assert not response.is_error, response
        envelope = json.loads(response.content[0].text)
        assert envelope['outcome'] == 'success', envelope
        payload = envelope['data']
        assert payload['outcome'] == 'success', payload
        assert payload['planning_result'] == ('ready' if review == 'approve' else 'blocked')
        assert payload['spec_status'] == spec.status.value
        assert spec.status == (SpecStatus.VALIDATED if review == 'approve' else SpecStatus.APPROVED)
        assert len(spec.evaluations) == len(spec.validations) == 1
        assert spec.evaluations[0]['evaluator_type'] == 'agent'
        assert spec.evaluations[0]['evaluator_id'] == spec.validations[0]['reviewer_id'] == 'A1'


@pytest.mark.asyncio
async def test_authenticated_mcp_can_record_decomposition_in_approved_without_promotion(
    _harness_env, monkeypatch,
):
    await _seed(_harness_env)
    factory = _db_mod.get_session_factory()
    register_unit_of_work_factory(CommunityUnitOfWorkFactory(factory))
    async with factory() as db:
        root = next(row for row in builtin_permission_presets() if row['name'] == 'Full Control')
        db.add(PermissionPreset(id='review-root', name=root['name'], flags=root['flags'], is_builtin=True))
        agent = await db.get(Agent, 'A1')
        agent.preset_id = 'review-root'
        agent.permission_flags = {}
        db.add(Spec(
            architecture_adoption=ArchitectureAdoptionScope(
                board_id='B1', spec_id='review-approved', adopted_in_edition=1,
                actor_id='A1', inherited_resource_ids=(),
            ).model_dump(mode='json'),
            id='review-approved', board_id='B1', title='Review planning',
            status=SpecStatus.APPROVED, created_by='A1',
            functional_requirements=[], acceptance_criteria=[], test_scenarios=[],
            business_rules=[], technical_requirements=[], api_contracts=[],
            integration_requirements=[], observability_requirements=[], decisions=[],
        ))
        await db.commit()
    monkeypatch.setattr(server, 'active_api_key_credential',
                        lambda: McpCredential(source='x_api_key_header', value='kA1'))
    frozen = freeze_mcp_resource_catalog(server.effective_resource_catalog())
    host = CommunityMcpHostProvider().materialize_catalog(
        server.mcp, resource_catalog=frozen, projection_identity=frozen.identity,
    )
    async with Client(host) as client:
        response = await client.call_tool('okto_pulse_submit_spec_evaluation', {
            'board_id': 'B1', 'spec_id': 'review-approved', **_evaluation_payload(),
        }, raise_on_error=False)
        assert not response.is_error, response
    async with factory() as db:
        spec = await db.get(Spec, 'review-approved')
        assert spec.status == SpecStatus.APPROVED
        assert len(spec.evaluations) == 1
        assert spec.evaluations[0]['evaluator_id'] == 'A1'
        assert spec.evaluations[0]['evaluator_type'] == 'agent'
        assert spec.evaluations[0]['spec_edition'] == spec.edition
        assert not spec.validations


async def _snapshot(factory):
    async with factory() as db:
        rows = (await db.execute(select(Spec).order_by(Spec.id))).scalars().all()
        result = deepcopy({row.id: {
            'title': row.title, 'status': row.status.value, 'version': row.version,
            'edition': row.edition, 'evaluations': row.evaluations,
            'project_structure': row.project_structure,
        } for row in rows})
        result['_review_receipts'] = await db.scalar(select(func.count()).select_from(QualityAssessmentReceiptRow))
        result['_structure_receipts'] = await db.scalar(select(func.count()).select_from(ProjectStructureMutationReceiptRow))
        return result


@pytest.mark.asyncio
async def test_authenticated_planner_review_boundary_across_mcp_variants(
    _harness_env, monkeypatch,
):
    await _seed(_harness_env)
    factory = _db_mod.get_session_factory()
    register_unit_of_work_factory(CommunityUnitOfWorkFactory(factory))
    register_structured_spec_store(CommunitySqlAlchemyStructuredSpecStore())
    async with factory() as db:
        await db.execute(update(Board).values(realm_id=LOCAL_REALM_ID,
                                             settings={'require_spec_validation': True}))
        agent = await db.get(Agent, 'A1')
        selected = {
            'spec': {
                'entity': {'read': True, 'edit_fields': True},
                'evaluations': {'read': True, 'submit': False},
                'validation': {'read': True, 'submit': False},
                'interact_in': {'draft': True, 'approved': True, 'validated': True},
                'move': {'approved_to_validated': True, 'validated_to_in_progress': True},
                'structured_entity': {'project_structure_node': {'create': True, 'update': True}},
            },
        }
        flags = {}
        for permission in flatten_permission_flags(registered_permission_flags()):
            set_permission_flag(flags, permission, False)
        for permission in flatten_permission_flags(selected):
            set_permission_flag(flags, permission, get_permission_flag(selected, permission))
        root = next(row for row in builtin_permission_presets() if row['name'] == 'Full Control')
        db.add(PermissionPreset(id='planner-root', name=root['name'], flags=root['flags'], is_builtin=True))
        agent.preset_id = 'planner-root'
        agent.permission_flags = flags
        for spec_id, state in [('draft', SpecStatus.DRAFT), ('approved', SpecStatus.APPROVED),
                               ('validated', SpecStatus.VALIDATED)]:
            db.add(Spec(
                architecture_adoption=ArchitectureAdoptionScope(
                    board_id='B1', spec_id=spec_id, adopted_in_edition=1,
                    actor_id='A1', inherited_resource_ids=(),
                ).model_dump(mode='json'),
                id=spec_id, board_id='B1', title=spec_id, status=state, created_by='A1',
                functional_requirements=[], acceptance_criteria=[], test_scenarios=[],
                business_rules=[], api_contracts=[], evaluations=[],
                technical_requirements=[], integration_requirements=[], observability_requirements=[],
                skip_test_coverage=True, skip_rules_coverage=True, skip_trs_coverage=True,
                skip_contract_coverage=True, skip_ir_coverage=True, skip_or_coverage=True,
                skip_decisions_coverage=True,
                decisions=[],
                **_direct_spec_context_fields(spec_id),
            ))
        await db.commit()

    # Only credential extraction is supplied: authentication, persisted grants,
    # host argument validation, use cases and Community SQL are real.
    monkeypatch.setattr(server, 'active_api_key_credential',
                        lambda: McpCredential(source='x_api_key_header', value='kA1'))
    resolved = await server._get_agent_ctx('B1')
    assert resolved.agent_id == 'A1'
    assert not resolved.permissions.owner_review_required
    assert resolved.permissions.flags['spec']['evaluations']['submit'] is False
    assert resolved.permissions.flags['spec']['entity']['edit_fields'] is True
    frozen = freeze_mcp_resource_catalog(server.effective_resource_catalog())
    host = CommunityMcpHostProvider().materialize_catalog(
        server.mcp, resource_catalog=frozen, projection_identity=frozen.identity,
    )
    async with Client(host) as client:
        names = tuple(tool.name for tool in await client.list_tools())
        assert 'okto_pulse_submit_spec_evaluation' in names
        assert 'submit_spec_evaluation' not in names
        assert 'okto_pulse_submit_spec_evaluation' not in tool_inventory_document(server.mcp)["aliases"].values()
        before = await _snapshot(factory)
        for state in ('approved', 'validated'):
            denied = await client.call_tool('okto_pulse_submit_spec_evaluation', {
                'board_id': 'B1', 'spec_id': state, **_evaluation_payload(),
            }, raise_on_error=False)
            assert 'spec.evaluations.submit' in str(denied), denied
            assert 'permission_missing' in str(denied), denied
            assert await _snapshot(factory) == before

        validation = {'idempotency_key': 'planner-review', 'expected_validation_edition': 1, 'expected_spec_version': 1,
                      'expected_head_revision': 0, 'recommendation': 'approve',
                      'expected_planning_manifest': manifest_payload('approved', 'B1')}
        for dimension in ('confidence', 'clarity', 'assertiveness', 'decidability', 'ambiguity'):
            validation[dimension] = 90
            validation[dimension + '_justification'] = 'Attempted unauthorized approval'
        denied = await client.call_tool('okto_pulse_submit_spec_validation', {
            'board_id': 'B1', 'spec_id': 'approved', **validation,
        }, raise_on_error=False)
        assert 'spec.validation.submit' in str(denied), denied
        assert 'permission_missing' in str(denied), denied
        assert await _snapshot(factory) == before

        moved = await client.call_tool('okto_pulse_move_spec', {
            'board_id': 'B1', 'spec_id': 'approved', 'status': 'validated',
        }, raise_on_error=False)
        assert 'spec_validation_gate_required' in str(moved), moved
        assert await _snapshot(factory) == before

        moved = await client.call_tool('okto_pulse_move_spec', {
            'board_id': 'B1', 'spec_id': 'validated', 'status': 'in_progress',
        }, raise_on_error=False)
        assert "no evaluation with 'approve'" in str(moved), moved
        assert await _snapshot(factory) == before

        # The host rejects review/status injection before entering a writer.
        forged = await client.call_tool('okto_pulse_update_spec', {
            'board_id': 'B1', 'spec_id': 'draft', 'title': 'Planner-authored title',
            'status': 'validated', 'evaluations': [_evaluation_payload()],
            'validations': [validation],
        }, raise_on_error=False)
        assert forged.is_error and 'unexpected_keyword_argument' in str(forged), forged
        assert await _snapshot(factory) == before
        edited = await client.call_tool('okto_pulse_update_spec', {
            'board_id': 'B1', 'spec_id': 'draft', 'title': 'Planner-authored title',
        }, raise_on_error=False)
        assert not edited.is_error, edited
        after_edit = await _snapshot(factory)
        assert after_edit['draft']['title'] == 'Planner-authored title', edited
        assert after_edit['draft']['status'] == 'draft'
        assert after_edit['draft']['evaluations'] == []
        assert after_edit['validated'] == before['validated']

        batch = await client.call_tool('okto_pulse_update_spec_entity', {
            'board_id': 'B1', 'spec_id': 'draft', 'entity_type': 'project_structure_node',
            'operation': 'batch', 'expected_spec_version': after_edit['draft']['version'],
            'expected_structure_revision': 0, 'idempotency_key': 'forge-review',
            'payload_json': {'operations': [{'operation': 'approve',
                                            'payload': {'evaluations': [_evaluation_payload()]}}]},
        }, raise_on_error=False)
        assert batch.is_error, batch.content
        batch_error = json.loads(batch.content[0].text)['data']
        # An unknown batch operation has no granted permission leaf. Refusal
        # precedes payload validation and cannot turn content access into review.
        assert batch_error['error_code'] == 'authorization_denied', batch_error
        assert batch_error['required_permission'] == 'spec.structured_entity.project_structure_node.batch'
        assert await _snapshot(factory) == after_edit

        # Even a permitted create operation cannot smuggle a review field.
        batch = await client.call_tool('okto_pulse_update_spec_entity', {
            'board_id': 'B1', 'spec_id': 'draft', 'entity_type': 'project_structure_node',
            'operation': 'batch', 'expected_spec_version': after_edit['draft']['version'],
            'expected_structure_revision': 0, 'idempotency_key': 'forge-review-field',
            'payload_json': {'operations': [{'operation': 'create', 'payload': {
                'id': 'psn_forgery', 'kind': 'file', 'name': 'file.py',
                'classification': 'as_is', 'evaluations': [_evaluation_payload()],
            }}]},
        }, raise_on_error=False)
        assert batch.is_error, batch.content
        batch_error = json.loads(batch.content[0].text)['data']
        assert batch_error['error_code'] == 'validation_failed', batch_error
        assert any(issue['path'] == 'evaluations' for issue in batch_error['details']['issues']), batch_error
        assert await _snapshot(factory) == after_edit

        absent_alias = await client.call_tool('submit_spec_evaluation', {
            'board_id': 'B1', 'spec_id': 'validated', **_evaluation_payload(),
        }, raise_on_error=False)
        assert absent_alias.is_error, absent_alias
        assert await _snapshot(factory) == after_edit
