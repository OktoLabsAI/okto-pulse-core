from types import SimpleNamespace
from unittest.mock import AsyncMock
from datetime import datetime, timezone
from dataclasses import replace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.application.use_cases.bug_clusters import BugClustersCommand, BugClustersUseCase
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.ports.bug_clusters import BugClustersQuery


def context(flags, *, realm=LOCAL_REALM_ID):
    board = SimpleNamespace(id='b', owner_id='owner', realm_id=realm, settings={'kg_query_timeout_ms': 700})
    operation = AsyncMock(return_value={'authority': 'informational'})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(analytics=SimpleNamespace(bug_clusters=operation)))
    actor = ActorContext('agent', 'mcp', actor_kind='agent', board_id='b', realm_id=LOCAL_REALM_ID, permissions=flags)
    return uow, actor, operation


def command(group_by='proxy'):
    request = BugClustersQuery.recent(board_id='b', actor_scope_ref='ignored', now=datetime.now(timezone.utc))
    return BugClustersCommand('b', request.window, group_by=group_by, timeout_ms=30000)


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', ['board.read', 'card.entity.read', 'spec.entity.read', 'kg.query.related_context'])
async def test_proxy_requires_each_existing_read_leaf_before_aggregation(missing):
    flags = ['board.read', 'card.entity.read', 'spec.entity.read', 'kg.query.related_context']
    uow, actor, operation = context([flag for flag in flags if flag != missing])
    with pytest.raises(PermissionDeniedError):
        await BugClustersUseCase().execute(command(), actor=actor, uow=uow)
    operation.assert_not_called()


@pytest.mark.asyncio
async def test_learning_does_not_borrow_related_context_grant():
    uow, actor, operation = context(['board.read', 'card.entity.read', 'kg.query.related_context'])
    with pytest.raises(PermissionDeniedError):
        await BugClustersUseCase().execute(command('learning'), actor=actor, uow=uow)
    operation.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(('group_by', 'extra'), [('severity', []), ('spec', ['spec.entity.read']),
    ('learning', ['kg.query.learning_from_bugs']), ('proxy', ['spec.entity.read', 'kg.query.related_context'])])
async def test_authorized_grouping_binds_actor_and_cannot_raise_board_timeout(group_by, extra):
    uow, actor, operation = context(['board.read', 'card.entity.read', *extra])
    await BugClustersUseCase().execute(command(group_by), actor=actor, uow=uow)
    query = operation.call_args.args[0]
    assert query.actor_scope_ref == f'{LOCAL_REALM_ID}:agent:mcp:agent'
    assert operation.call_args.kwargs == {'timeout_ms': 700}


@pytest.mark.asyncio
async def test_foreign_realm_is_not_enumerable():
    uow, actor, operation = context(['*'], realm='foreign')
    with pytest.raises(EntityNotFoundError):
        await BugClustersUseCase().execute(command(), actor=actor, uow=uow)
    operation.assert_not_called()


@pytest.mark.asyncio
async def test_revocation_is_checked_again_before_next_page():
    uow, actor, operation = context(['board.read', 'card.entity.read'])
    await BugClustersUseCase().execute(command('severity'), actor=actor, uow=uow)
    actor.permissions = ['board.read']
    with pytest.raises(PermissionDeniedError):
        await BugClustersUseCase().execute(replace(command('severity'), cursor='previous-page'), actor=actor, uow=uow)
    assert operation.await_count == 1
