from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.application.use_cases.spec_coverage_query import SpecCoverageCommand, SpecCoverageUseCase

FLAGS = ['board.read', 'spec.entity.read', 'card.entity.read', 'spec.tests.read']
COMMAND = SpecCoverageCommand('board', 'spec', timeout_ms=30000)


def context(flags):
    operation = AsyncMock(return_value={})
    board = SimpleNamespace(id='board', owner_id='owner', realm_id='local', settings={'kg_query_timeout_ms': 700})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(analytics=SimpleNamespace(spec_coverage=operation)))
    actor = ActorContext('agent', 'mcp', actor_kind='agent', realm_id='local', board_id='board', permissions=flags)
    return actor, uow, operation, board


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', FLAGS)
async def test_source_permissions_precede_all_aggregation(missing):
    actor, uow, operation, _ = context([flag for flag in FLAGS if flag != missing])
    with pytest.raises(PermissionDeniedError):
        await SpecCoverageUseCase().execute(COMMAND, actor=actor, uow=uow)
    operation.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('proof', [False, True])
async def test_code_proof_is_explicitly_scoped_without_borrowing_other_grants(proof):
    actor, uow, operation, _ = context(FLAGS + (['code_traceability.evidence.read'] if proof else ['kg.query.related_context']))
    await SpecCoverageUseCase().execute(COMMAND, actor=actor, uow=uow)
    query = operation.call_args.args[0]
    assert query.read_delivery is proof
    assert query.read_graph is (not proof)
    assert query.actor_scope_ref == 'local:agent:mcp:agent'
    assert operation.call_args.kwargs == {'timeout_ms': 700}


@pytest.mark.asyncio
async def test_revocation_is_observed_before_reusing_a_cursor():
    actor, uow, operation, _ = context(FLAGS + ['code_traceability.evidence.read'])
    await SpecCoverageUseCase().execute(COMMAND, actor=actor, uow=uow)
    actor.permissions = FLAGS
    await SpecCoverageUseCase().execute(replace(COMMAND, cursor='previous'), actor=actor, uow=uow)
    assert operation.call_args.args[0].read_delivery is False


@pytest.mark.asyncio
async def test_foreign_realm_is_not_enumerable():
    actor, uow, operation, board = context(FLAGS)
    board.realm_id = 'foreign'
    with pytest.raises(EntityNotFoundError):
        await SpecCoverageUseCase().execute(COMMAND, actor=actor, uow=uow)
    operation.assert_not_called()
