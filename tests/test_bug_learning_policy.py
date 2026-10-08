"""KG7.4 policy/default authority and read-only request preconditions."""
# ruff: noqa: F811 -- imported fixtures are injected by pytest.
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from test_r01a_boards_uow import client  # noqa: F401
from test_default_board_config_api import _isolate_committed_global_templates  # noqa: F401
from test_default_cognitive_policy_authority import BASE, _agent
from okto_pulse.community.api.default_board_config import router
from okto_pulse.core.domain.bug_learning_policy import bug_learning_closeout_mode, requires_bug_learning_capture
from okto_pulse.core.domain.enums import CardStatus, CardType
from okto_pulse.core.models.schemas import BoardSettings


@pytest.mark.parametrize('raw', [None, {}, {'skip_cognitive_consolidation': True}, {'cognitive_readiness_policy': 'blocking'}])
def test_legacy_settings_do_not_activate_capture_requirement(raw):
    assert bug_learning_closeout_mode(raw) == 'advisory'


@pytest.mark.parametrize('value', [None, '', 'off', 'BLOCKING', True, {}])
def test_explicit_invalid_policy_is_not_an_implicit_waiver(value):
    with pytest.raises(ValueError, match='bug_learning_closeout_policy_invalid'):
        bug_learning_closeout_mode({'bug_learning_closeout': value})
    with pytest.raises(ValidationError):
        BoardSettings(bug_learning_closeout=value)


@pytest.mark.parametrize('card_type', list(CardType))
def test_capture_policy_only_applies_to_bugs_and_legacy_skip_cannot_waive_it(card_type):
    assert requires_bug_learning_capture({'bug_learning_closeout': 'blocking',
        'skip_cognitive_consolidation': True}, card_type) == (card_type is CardType.BUG)


@pytest.mark.parametrize('policy_key', ['bug_learning_closeout', 'missing_link_gate'])
@pytest.mark.parametrize('operation', ['create', 'activate', 'deactivate', 'import'])
def test_template_lifecycle_cannot_weaken_human_capture_policy(client, operation, policy_key):
    client.app.include_router(router, prefix='/api/v1')
    baseline = client.post(f'{BASE}/versions', json={
        'settings_payload': {policy_key: 'blocking'}, 'activate': True})
    assert baseline.status_code == 200, baseline.text
    target = client.post(f'{BASE}/versions', json={
        'settings_payload': {policy_key: 'advisory'}})
    assert target.status_code == 200, target.text
    before = client.get(f'{BASE}/versions').json()
    _agent(client)
    if operation == 'create':
        response = client.post(f'{BASE}/versions', json={
            'settings_payload': {policy_key: 'advisory'}, 'activate': True})
    elif operation == 'import':
        response = client.post(f'{BASE}/import', json={'schema_version': '1', 'kind': 'board_config',
            'items': [{'settings_payload': {'max_scenarios_per_card': 4}},
                {'settings_payload': {policy_key: 'advisory'}, 'is_active': True}]})
        assert response.json()['detail']['created'] == 0, response.text
    else:
        identity = target.json()['id'] if operation == 'activate' else baseline.json()['id']
        response = client.post(f'{BASE}/versions/{identity}/{operation}')
    assert response.status_code == (400 if operation == 'import' else 403), response.text
    assert client.get(f'{BASE}/versions').json() == before


@pytest.mark.parametrize('policy_key', ['bug_learning_closeout', 'missing_link_gate'])
def test_executor_new_template_inherits_omitted_human_capture_policy(client, policy_key):
    client.app.include_router(router, prefix='/api/v1')
    baseline = client.post(f'{BASE}/versions', json={
        'settings_payload': {policy_key: 'blocking'}, 'activate': True})
    assert baseline.status_code == 200, baseline.text
    _agent(client)
    result = client.post(f'{BASE}/versions', json={
        'settings_payload': {'max_scenarios_per_card': 4}, 'activate': True})
    assert result.status_code == 200, result.text
    assert result.json()['settings_payload'][policy_key] == 'blocking'


@pytest.mark.asyncio
@pytest.mark.parametrize('policy,required', [('advisory', False), ('blocking', True)])
async def test_preview_keeps_required_learning_as_request_input_without_writing(monkeypatch, policy, required):
    from okto_pulse.core.application.use_cases import allowed_transitions as module
    from okto_pulse.core.application.use_cases.base import ActorContext

    board = SimpleNamespace(id='board', settings={'bug_learning_closeout': policy})
    bug = SimpleNamespace(id='bug', board_id='board', card_type=CardType.BUG, status=CardStatus.IN_PROGRESS)
    monkeypatch.setattr(module, 'load_accessible_board', AsyncMock(return_value=board))
    monkeypatch.setattr(module, 'resolve_actor_permissions', AsyncMock(return_value={}))
    case = module.ListAllowedTransitionsUseCase()
    case._load_entity = AsyncMock(return_value=bug)
    async def independent_previews(services, entity_type, entity, transition, **kwargs):
        return transition
    case._preview_entity_transition = independent_previews
    # No writer/store or commit exists on this test UOW. Independent preview
    # gates are fixtures; this checks the additional request-local projection.
    result = await case.execute(module.ListAllowedTransitionsCommand(
        board_id='board', entity_type='card', entity_id='bug'),
        actor=ActorContext('owner', 'rest'), uow=SimpleNamespace(services=SimpleNamespace()))
    done = next(item for item in result.read_model.allowed_transitions if item.to_status == 'done')
    assert ('valid_durable_learning_capture' in done.preconditions) is required
    assert done.blocked_reason is None
