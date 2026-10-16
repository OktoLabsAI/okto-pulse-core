"""BASE T15: grouped dispatch preserves each variant's permission boundary."""
import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.mcp import server


VARIANTS = [
    ('scenario', 'card.link_to.scenario'),
    ('rule', 'card.link_to.rule'),
    ('fr', 'spec.structured_entity.functional_requirement.link_task'),
    ('decision', 'spec.structured_entity.decision.link_task'),
    ('tr', 'card.link_to.tr'),
    ('contract', 'card.link_to.contract'),
    ('ir', 'spec.integration_requirements.link_task'),
    ('ir', 'card.link_to.ir'),
    ('or', 'spec.observability_requirements.link_task'),
    ('or', 'card.link_to.or'),
    ('spec', 'card.entity.link_spec'),
]


@pytest.mark.asyncio
@pytest.mark.parametrize('variant,denied_leaf', VARIANTS)
async def test_grouped_link_task_denies_only_missing_variant_grant_before_uow(
    monkeypatch, variant, denied_leaf,
):
    flags = {}
    for _, leaf in VARIANTS:
        node = flags
        *parents, name = leaf.split('.')
        for parent in parents:
            node = node.setdefault(parent, {})
        node[name] = leaf != denied_leaf
    ctx = SimpleNamespace(agent_id='actor', agent_name='Actor', board_id='board',
                          permissions=PermissionSet(flags))

    async def context(board_id):
        assert board_id == 'board'
        return ctx

    class ReachedPersistenceBoundary(RuntimeError):
        pass

    def persistence_boundary():
        raise ReachedPersistenceBoundary

    monkeypatch.setattr(server, '_get_agent_ctx', context)
    monkeypatch.setattr(server, 'get_unit_of_work_factory_for_mcp', persistence_boundary)
    payload = dict(board_id='board', target_id='target', card_id='card', spec_id='spec')
    denied = json.loads(await server.okto_pulse_link_task.fn(target_type=variant, **payload))
    detail = json.loads(denied['error'])
    assert detail['reason'] == 'permission_missing'
    assert detail['required_permission'] == denied_leaf

    # The same actor can enter a different variant; this is not blanket denial.
    sibling = 'rule' if variant == 'scenario' else 'scenario'
    with pytest.raises(ReachedPersistenceBoundary):
        await server.okto_pulse_link_task.fn(target_type=sibling, **payload)

    # Granting the precise missing leaf lets this variant reach the same boundary.
    node = flags
    *parents, name = denied_leaf.split('.')
    for parent in parents:
        node = node[parent]
    node[name] = True
    ctx.permissions = PermissionSet(flags)
    with pytest.raises(ReachedPersistenceBoundary):
        await server.okto_pulse_link_task.fn(target_type=variant, **payload)
