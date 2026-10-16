"""Native MCP requirement authoring preserves identity and explicit empty patches."""

import pytest

from sqlalchemy_test_models import Spec
from test_r3_imp1_create_spec_effective import _board, _call
from okto_pulse.core.mcp import server


@pytest.mark.asyncio
async def test_mcp_authors_objects_updates_ids_and_clears_collection(db_factory):
    board = await _board(db_factory)
    created = await _call('okto_pulse_create_spec', board_id=board, title='Current objects',
        functional_requirements=[{'text': 'New FR'}],
        technical_requirements=[{'text': 'New TR'}],
        acceptance_criteria=[{'text': 'New AC'}])
    assert created['success'], created
    spec_id = created['spec']['id']
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        fr = dict(spec.functional_requirements[0])
        assert fr['id'].startswith('fr_')
        assert spec.technical_requirements[0]['id'].startswith('tr_')
        assert spec.acceptance_criteria[0]['id'].startswith('ac_')
    result = await _call('okto_pulse_update_spec', board_id=board, spec_id=spec_id,
        functional_requirements=[{**fr, 'text': 'Edited FR'}], technical_requirements=[])
    assert result.get('success'), result
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        assert spec.functional_requirements[0]['id'] == fr['id']
        assert spec.functional_requirements[0]['text'] == 'Edited FR'
        assert spec.technical_requirements == []
        assert spec.acceptance_criteria[0]['text'] == 'New AC'


@pytest.mark.asyncio
@pytest.mark.parametrize('name', ['okto_pulse_create_spec', 'okto_pulse_update_spec'])
async def test_mcp_schema_has_only_object_requirement_collections(name):
    tool = await server.mcp.get_tool(name)
    for field in ('functional_requirements', 'technical_requirements', 'acceptance_criteria'):
        variants = tool.parameters['properties'][field]['anyOf']
        assert {item['type'] for item in variants} == {'array', 'null'}
        assert next(item for item in variants if item['type'] == 'array')['items']['type'] == 'object'


@pytest.mark.asyncio
async def test_clearing_requirements_still_requires_content_edit_permission(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    import json

    monkeypatch.setattr(server, '_get_agent_ctx', AsyncMock(return_value=SimpleNamespace(permissions=object())))
    checked = []

    def deny(permissions, granular, *args):
        checked.append(granular)
        return 'Content edit denied'

    monkeypatch.setattr(server, '_mcp_check_permission', deny)
    raw = await server.okto_pulse_update_spec.fn(board_id='board', spec_id='spec',
        functional_requirements=[], labels=['label'])
    assert checked == ['spec.entity.edit_fields']
    assert 'error' in json.loads(raw)
