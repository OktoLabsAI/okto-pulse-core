"""Authorized source reads distinguish absence, denial and provider failure."""
import copy
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.card_reference_context import (
    CARD_REFERENCE_CONTEXT_PERMISSIONS, GetCardScenarioReferenceContextUseCase,
)
from okto_pulse.core.mcp.context_projection import project_task_context
from okto_pulse.core.mcp.projection_envelope import _stable_payload_bytes


def source():
    card = NS(id='card', board_id='board', spec_id='spec', test_scenario_ids=['missing'])
    parent = NS(id='spec', board_id='board', test_scenarios=[])
    async def query(request):
        return (card,) if request.entity == 'card' else (parent,)
    uow = NS(boards=NS(get=AsyncMock(return_value=NS(owner_id='owner', realm_id='local'))),
        services=NS(list_application_records=AsyncMock(side_effect=query)), commit=AsyncMock())
    actor = ActorContext('agent', 'mcp', board_id='board', permissions=CARD_REFERENCE_CONTEXT_PERMISSIONS)
    return card, parent, uow, actor


async def read(uow, actor):
    return await GetCardScenarioReferenceContextUseCase().execute(board_id='board', card_id='card', actor=actor, uow=uow)


@pytest.mark.parametrize('missing', CARD_REFERENCE_CONTEXT_PERMISSIONS)
async def test_denial_precedes_every_source_read(missing):
    _, _, uow, _ = source()
    actor = ActorContext('agent', 'mcp', board_id='board', permissions=tuple(
        flag for flag in CARD_REFERENCE_CONTEXT_PERMISSIONS if flag != missing))
    result = await read(uow, actor)
    assert result.status == 'not_authorized'
    assert result.finding_count is None and result.source_fingerprint is None and not result.findings
    uow.boards.get.assert_not_awaited()
    uow.services.list_application_records.assert_not_awaited()


@pytest.mark.parametrize('failure', ['card_board', 'realm', 'parent_board', 'incomplete', 'provider'])
async def test_unavailable_never_becomes_known_absence(failure):
    card, parent, uow, actor = source()
    if failure == 'card_board': card.board_id = 'foreign'
    if failure == 'realm': uow.boards.get.return_value.realm_id = 'foreign'
    if failure == 'parent_board': parent.board_id = 'foreign'
    if failure == 'incomplete': del parent.test_scenarios
    if failure == 'provider': uow.services.list_application_records.side_effect = RuntimeError('private provider details')
    result = await read(uow, actor)
    assert result.status == 'unavailable'
    assert result.finding_count is None and not result.findings
    assert 'foreign' not in result.model_dump_json() and 'private' not in result.model_dump_json()
    if failure == 'parent_board':
        assert all('test_scenarios' not in call.args[0].select_fields
                   for call in uow.services.list_application_records.await_args_list)
    uow.commit.assert_not_awaited()


async def test_current_source_closes_previous_finding_without_audit_or_graph():
    card, parent, uow, actor = source()
    absent = await read(uow, actor)
    assert absent.finding_count == 1 and absent.findings[0].reason_code == 'target_absent'
    parent.test_scenarios = [{'id': 'missing'}]
    resolved = await read(uow, actor)
    assert resolved.status == 'available' and resolved.finding_count == 0
    assert resolved.source_fingerprint != absent.source_fingerprint
    card.spec_id = None
    unlinked = await read(uow, actor)
    assert unlinked.findings[0].reason_code == 'parent_absent'
    assert unlinked.findings[0].target_ref == 'missing'
    card.test_scenario_ids = []
    assert (await read(uow, actor)).finding_count == 0
    uow.commit.assert_not_awaited()


async def test_ambiguous_identity_is_visible_without_choosing_a_target():
    _, parent, uow, actor = source()
    parent.test_scenarios = [{'id': 'missing'}, {'id': 'missing'}]
    result = await read(uow, actor)
    assert result.findings[0].reason_code == 'target_ambiguous'
    assert result.findings[0].correction_surface == 'spec_test_scenarios'


@pytest.mark.parametrize('oversize', [False, True])
async def test_bounded_view_keeps_exact_count_and_never_truncates_an_identity(oversize):
    card, _, uow, actor = source()
    card.test_scenario_ids = ['界' * 4000] if oversize else [f'missing-{index:03}' for index in range(25)]
    result = await read(uow, actor)
    assert result.finding_count == len(card.test_scenario_ids) and result.truncated
    assert len(result.findings) == (0 if oversize else 20)
    assert len(result.model_dump_json().encode()) < 8192


@pytest.mark.parametrize('profile,scope', [('summary', 'all'), ('detail', 'all'), ('full', 'gate')])
async def test_budget_preserves_exact_findings_and_explicit_unavailable(profile, scope):
    _, _, uow, actor = source()
    context = (await read(uow, actor)).model_dump(mode='json')
    payload = {'card': {'id': 'card'}, 'scenario_reference_context': context,
        'gate_readiness': {str(i): {str(j): 'x' * 1000 for j in range(12)} for i in range(30)}}
    before = copy.deepcopy(payload)
    projected = project_task_context(payload, card_id='card', profile=profile, context_scope=scope)
    assert projected['scenario_reference_context'] == context and payload == before
    assert _stable_payload_bytes(projected) <= projected['projection']['budget_bytes']
    assert projected['projection']['payload_bytes'] == _stable_payload_bytes(projected)
