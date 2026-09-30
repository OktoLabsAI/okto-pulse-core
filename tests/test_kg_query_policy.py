"""KG6.5: legacy defaults, human-owned bounds and scoped policy reads."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.kg_query_policy import ReadKGQueryPolicyUseCase
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.ports.kg_query_policy import KGQueryPolicy, query_row_limit
from okto_pulse.core.models.schemas import BoardSettings


def test_defaults_and_per_call_reduction():
    assert BoardSettings().kg_query_timeout_ms == 15000
    assert KGQueryPolicy.from_settings({}).effective_timeout() == 15000
    policy = KGQueryPolicy.from_settings({'kg_query_timeout_ms': 1200})
    assert policy.effective_timeout(30000) == 1200
    assert policy.effective_timeout(5) == 5
    assert query_row_limit() == query_row_limit(0) == 200
    assert query_row_limit(1000) == 1000


@pytest.mark.parametrize('value', [True, '15000', 1.5, None, 0, -1, 30001])
def test_invalid_stored_policy_is_not_silently_defaulted(value):
    with pytest.raises(ValueError):
        KGQueryPolicy.from_settings({'kg_query_timeout_ms': value})
    with pytest.raises(ValidationError):
        BoardSettings(kg_query_timeout_ms=value)


@pytest.mark.parametrize('value', [True, '10', 1.5, 0, -1])
def test_invalid_call_timeout(value):
    with pytest.raises(ValueError):
        KGQueryPolicy().effective_timeout(value)


@pytest.mark.parametrize('value', [True, '10', 1.5, -1, 1001])
def test_invalid_rows(value):
    with pytest.raises(ValueError):
        query_row_limit(value)


@pytest.mark.asyncio
@pytest.mark.parametrize('realm,expected', [(LOCAL_REALM_ID, 800), ('another-realm', None)])
async def test_policy_read_uses_board_and_realm_scope(realm, expected):
    board = SimpleNamespace(id='b', owner_id='owner', realm_id=realm,
                            settings={'kg_query_timeout_ms': 800})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)))
    actor = ActorContext('agent', 'mcp', actor_kind='agent', board_id='b',
                         realm_id=LOCAL_REALM_ID, permissions=['board.read'])
    if expected is None:
        with pytest.raises(EntityNotFoundError):
            await ReadKGQueryPolicyUseCase().execute('b', actor=actor, uow=uow)
    else:
        assert (await ReadKGQueryPolicyUseCase().execute('b', actor=actor, uow=uow)).timeout_ms == expected
