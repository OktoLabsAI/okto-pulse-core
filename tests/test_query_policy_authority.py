"""Query limits cannot be widened through default-template lifecycle routes."""
# ruff: noqa: F811
import pytest
from test_r01a_boards_uow import client  # noqa: F401
from test_default_board_config_api import _isolate_committed_global_templates  # noqa: F401
from test_default_cognitive_policy_authority import BASE, _agent
from okto_pulse.community.api.default_board_config import router


@pytest.mark.parametrize('operation', ['create', 'activate', 'deactivate', 'import'])
def test_template_lifecycle_preserves_human_query_limit(client, operation):
    client.app.include_router(router, prefix='/api/v1')
    current = client.post(BASE + '/versions', json={
        'settings_payload': {'kg_query_timeout_ms': 900}, 'activate': True,
    })
    assert current.status_code == 200, current.text
    target = client.post(BASE + '/versions', json={
        'settings_payload': {'kg_query_timeout_ms': 30000},
    })
    assert target.status_code == 200, target.text
    before = client.get(BASE + '/versions').json()
    _agent(client)
    if operation == 'create':
        response = client.post(BASE + '/versions', json={
            'settings_payload': {'kg_query_timeout_ms': 30000}, 'activate': True,
        })
    elif operation == 'import':
        response = client.post(BASE + '/import', json={
            'schema_version': '1', 'kind': 'board_config', 'items': [
                {'settings_payload': {'kg_query_timeout_ms': 30000}, 'is_active': True},
            ],
        })
    else:
        identity = target.json()['id'] if operation == 'activate' else current.json()['id']
        response = client.post(f'{BASE}/versions/{identity}/{operation}')
    assert response.status_code == (400 if operation == 'import' else 403), response.text
    assert 'human_control_required' in response.text
    assert client.get(BASE + '/versions').json() == before


def test_executor_template_edit_inherits_omitted_query_limit(client):
    client.app.include_router(router, prefix='/api/v1')
    result = client.post(BASE + '/versions', json={
        'settings_payload': {'kg_query_timeout_ms': 900}, 'activate': True,
    })
    assert result.status_code == 200, result.text
    _agent(client)
    result = client.post(BASE + '/versions', json={
        'settings_payload': {'max_scenarios_per_card': 4}, 'activate': True,
    })
    assert result.status_code == 200, result.text
    assert result.json()['settings_payload']['kg_query_timeout_ms'] == 900


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['create', 'activate', 'deactivate'])
async def test_mcp_template_lifecycle_cannot_widen_query_limit(client, operation):
    from test_default_board_config_api import _call, USER_ID
    from test_r01a_boards_uow import _seed_board

    client.app.include_router(router, prefix='/api/v1')
    current = client.post(BASE + '/versions', json={
        'settings_payload': {'kg_query_timeout_ms': 900}, 'activate': True,
    })
    target = client.post(BASE + '/versions', json={
        'settings_payload': {'kg_query_timeout_ms': 30000},
    })
    assert current.status_code == target.status_code == 200
    before = client.get(BASE + '/versions').json()
    kwargs = {'board_id': await _seed_board(owner=USER_ID)}
    if operation == 'create':
        kwargs.update(settings_payload={'kg_query_timeout_ms': 30000}, activate=True)
    else:
        kwargs['template_id'] = target.json()['id'] if operation == 'activate' else current.json()['id']
    result = await _call(f'okto_pulse_{operation}_default_board_config_version', **kwargs)
    assert result['code'] == 'human_control_required', result
    assert client.get(BASE + '/versions').json() == before
