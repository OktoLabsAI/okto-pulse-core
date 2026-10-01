"""BASE T03: authenticated planner cannot turn content authority into approval."""
from copy import deepcopy
import json

import pytest
from fastmcp import Client
from sqlalchemy import func, select, update

from okto_pulse.community.adapters.mcp_host import CommunityMcpHostProvider
from okto_pulse.community.adapters.sqlalchemy_models import (
    Agent, Board, Spec, QualityAssessmentReceiptRow, ProjectStructureMutationReceiptRow,
)
from okto_pulse.community.adapters.sqlalchemy_unit_of_work import CommunityUnitOfWorkFactory
from okto_pulse.community.adapters.sqlalchemy_structured_spec import CommunitySqlAlchemyStructuredSpecStore
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.manifest import installed_aliases
from okto_pulse.core.ports import McpCredential
from okto_pulse.core.ports.mcp_resources import freeze_mcp_resource_catalog
from okto_pulse.core.ports.structured_spec import register_structured_spec_store
from okto_pulse.core.runtime_registry import register_unit_of_work_factory
from test_r08d_auth_replay_gates import _harness_env, _seed, _db_mod
from test_spec_evaluation_rest_parity import _direct_spec_context_fields, _evaluation_payload

__all__ = ['_harness_env']


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
        agent.permissions = ['specs:evaluate']  # Explicit granular denial still wins.
        agent.permission_flags = {
            'spec': {
                'entity': {'read': True, 'edit_fields': True},
                'evaluations': {'read': True, 'submit': False},
                'validation': {'read': True, 'submit': False},
                'interact_in': {'draft': True, 'approved': True, 'validated': True},
                'move': {'approved_to_validated': True, 'validated_to_in_progress': True},
                'structured_entity': {'project_structure_node': {'create': True, 'update': True}},
            },
        }
        for spec_id, state in [('draft', SpecStatus.DRAFT), ('approved', SpecStatus.APPROVED),
                               ('validated', SpecStatus.VALIDATED)]:
            db.add(Spec(
                id=spec_id, board_id='B1', title=spec_id, status=state, created_by='A1',
                functional_requirements=[], acceptance_criteria=[], test_scenarios=[],
                business_rules=[], api_contracts=[], evaluations=[],
                skip_test_coverage=True, skip_rules_coverage=True, skip_trs_coverage=True,
                skip_contract_coverage=True, skip_ir_coverage=True, skip_or_coverage=True,
                skip_decisions_coverage=True,
                decisions=[{'id': 'dec_seed', 'title': 'Explicit fixture decision',
                            'rationale': 'Isolate approval boundary', 'status': 'active'}],
                **_direct_spec_context_fields(spec_id),
            ))
        await db.commit()

    # Only credential extraction is supplied: authentication, persisted grants,
    # host argument validation, use cases and Community SQL are real.
    monkeypatch.setattr(server, 'active_api_key_credential',
                        lambda: McpCredential(source='x_api_key_header', value='kA1'))
    resolved = await server._get_agent_ctx('B1')
    assert resolved.agent_id == 'A1'
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
        assert 'okto_pulse_submit_spec_evaluation' not in installed_aliases(names).values()
        before = await _snapshot(factory)
        denied = await client.call_tool('okto_pulse_submit_spec_evaluation', {
            'board_id': 'B1', 'spec_id': 'validated', **_evaluation_payload(),
        }, raise_on_error=False)
        assert 'spec.evaluations.submit' in str(denied), denied
        assert 'permission_missing' in str(denied), denied
        assert await _snapshot(factory) == before

        validation = {'expected_validation_edition': 1, 'expected_spec_version': 1,
                      'expected_head_revision': 0, 'recommendation': 'approve'}
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
        assert batch_error['error_code'] == 'validation_failed', batch_error
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
