"""KG28 diagnostics require source permissions and preserve exact bounded IDs."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import json

import pytest

from okto_pulse.core.application.use_cases import missing_link_context as context
from okto_pulse.core.application.use_cases.base import ActorContext, PermissionDeniedError
from okto_pulse.core.domain.missing_link_gate import SemanticLinkFinding
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp.context_projection import project_spec_context, project_task_context
from okto_pulse.core.models.reference_context import MissingLinkContext
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.services.missing_link_gate import MissingLinkEvaluation
from test_missing_link_gate_service import seed, USER_ID


@pytest.mark.asyncio
@pytest.mark.parametrize('entity_type', ['spec', 'card'])
async def test_native_source_diagnostic_matches_gate_without_writes(db_factory, entity_type):
    board_id, subject_id = await seed(db_factory, entity_type=entity_type)
    async with db_factory() as db:
        uow = resolve_unit_of_work_factory().wrap(db)
        actor = ActorContext(USER_ID, 'mcp', board_id=board_id, realm_id=LOCAL_REALM_ID, permissions=['*'])
        result = await context.GetMissingLinkContextUseCase().execute(
            board_id=board_id, entity_type=entity_type, entity_id=subject_id, actor=actor, uow=uow)
        assert result.status == 'available'
        assert result.would_block_done and result.finding_count == 1
        assert result.findings[0].target_ref.startswith('absent-')
        assert not db.new and not db.dirty and not db.deleted


@pytest.mark.asyncio
@pytest.mark.parametrize('entity_type', ['spec', 'card'])
async def test_permission_denial_precedes_every_source_read(monkeypatch, entity_type):
    deny = AsyncMock(side_effect=PermissionDeniedError('source denied'))
    board_read = AsyncMock()
    service_read = AsyncMock()
    monkeypatch.setattr(context, 'require_all', deny)
    monkeypatch.setattr(context, 'load_accessible_board', board_read)
    uow = SimpleNamespace(services=SimpleNamespace(get_application_record=service_read))
    result = await context.GetMissingLinkContextUseCase().execute(
        board_id='board', entity_type=entity_type, entity_id='private', actor=object(), uow=uow)
    assert result.status == 'not_authorized'
    assert result.finding_count is None and result.findings == [] and result.mode is None
    board_read.assert_not_awaited()
    service_read.assert_not_awaited()
    requirements = deny.call_args.args[1:]
    assert len(requirements) == len(context.MISSING_LINK_CONTEXT_PERMISSIONS[entity_type])


@pytest.mark.parametrize('profile', ['summary', 'detail', 'full'])
def test_projectors_preserve_bounded_reference_identity_and_unknown_counts(profile):
    finding = SemanticLinkFinding('spec:s:decisions:d', 'linked_requirements', 'target-' + 'x' * 200,
                                  'target_absent', 'update_spec_entity')
    report = MissingLinkEvaluation('blocking', 'available', (finding,) * 100).to_payload()
    assert report['finding_count'] == 100 and report['truncated']
    assert len(report['findings']) < 20
    typed = MissingLinkContext.model_validate(report).model_dump(mode='json')
    source = {'card': {'id': 'card', 'status': 'in_progress'}, 'spec': {'id': 's'}, 'missing_link_context': typed}
    for projected in (project_task_context(source, card_id='card', profile=profile),
                      project_task_context(source, card_id='card', profile='full', context_scope='gate'),
                      project_spec_context(source, profile=profile)):
        assert projected['missing_link_context'] == typed


def test_unavailable_context_cannot_be_claimed_as_zero():
    with pytest.raises(ValueError, match='unobserved_missing_links'):
        MissingLinkContext(status='unavailable', finding_count=0)


@pytest.mark.asyncio
async def test_spec_rest_model_and_mcp_context_show_same_authorized_observation(db_factory, monkeypatch):
    from mcp_runtime_testing import register_mcp_test_runtime
    from okto_pulse.core.application.use_cases.spec_crud import GetSpecCommand, GetSpecUseCase
    from okto_pulse.core.models.schemas import SpecResponse
    from okto_pulse.core.mcp import server

    board_id, spec_id = await seed(db_factory)
    actor = ActorContext(USER_ID, 'mcp', board_id=board_id, realm_id=LOCAL_REALM_ID, permissions=['*'])
    async with db_factory() as db:
        result = await GetSpecUseCase().execute(GetSpecCommand(spec_id), actor=actor,
                                                uow=resolve_unit_of_work_factory().wrap(db))
        diagnostic = SpecResponse.model_validate(result.spec).missing_link_context
        assert diagnostic.status == 'available' and diagnostic.would_block_done
    register_mcp_test_runtime(db_factory)
    monkeypatch.setattr(server, '_get_agent_ctx', AsyncMock(return_value=SimpleNamespace(
        agent_id=USER_ID, agent_name='Reader', board_id=board_id,
        permissions=list(context.MISSING_LINK_CONTEXT_PERMISSIONS['spec']))))
    monkeypatch.setattr(server, '_mcp_code_traceability_projection', AsyncMock(return_value={}))
    tool = await server.mcp.get_tool('okto_pulse_get_spec_context')
    payload = json.loads(await tool.fn(board_id=board_id, spec_id=spec_id, include_knowledge=False,
        include_mockups=False, include_qa=False, include_architecture=False, profile='summary'))
    assert 'missing_link_context' in payload, payload
    assert payload['missing_link_context'] == diagnostic.model_dump(mode='json')
