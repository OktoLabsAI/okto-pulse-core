"""P08 compact board projections reuse a single authorized proof universe."""
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain import effective_delivery_coverage as coverage
from okto_pulse.core.ports.delivery_readiness import ReadinessActor, ReadinessAccessDenied
from okto_pulse.core.services.delivery_readiness import DeliveryReadinessService, EvaluatedDeliveryReadiness
from test_delivery_readiness_shared_evaluation import prepared, changed
from test_delivery_readiness_paging import Cursor

ACTOR = ReadinessActor('reader', 'agent', 'local', 'board')


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['current', 'missing_contribution', 'failed', 'waived', 'incomplete', 'open'])
async def test_board_uses_one_load_and_same_metrics_as_card_reads(monkeypatch, kind):
    batch = prepared(changed(kind))
    loader = SimpleNamespace(load=AsyncMock(return_value=batch))
    calls = []
    original = coverage.evaluate_effective_delivery_coverage
    def count(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)
    monkeypatch.setattr(coverage, 'evaluate_effective_delivery_coverage', count)
    result = await DeliveryReadinessService(loader, Cursor()).board_summary(
        board_id='board', spec_id='spec', actor=ACTOR)
    loader.load.assert_awaited_once()
    assert len(calls) == 2  # Proof and lifecycle views, never one evaluation per Card.
    expected = EvaluatedDeliveryReadiness(batch, actor_id='reader')
    assert result.delivery_readiness == expected.context()
    assert {card.card_id for card in result.cards} == {card.id for card in batch.cards}
    for row in result.cards:
        card = row.delivery_readiness
        assert card == expected.context(card.scope.card_id)
        assert card.snapshot_id == result.delivery_readiness.snapshot_id


@pytest.mark.asyncio
async def test_missing_authority_is_denied_not_empty_board():
    loader = SimpleNamespace(load=AsyncMock(return_value=replace(prepared(changed('current')), access=None)))
    with pytest.raises(ReadinessAccessDenied):
        await DeliveryReadinessService(loader, Cursor()).board_summary(board_id='board', spec_id='spec', actor=ACTOR)


def test_completed_card_uses_same_snapshot_history_without_granting_a_transition():
    batch = prepared(changed('current'))
    batch = replace(batch, cards=tuple(replace(card, status='done') for card in batch.cards))
    current = EvaluatedDeliveryReadiness(batch, actor_id='reader').context('ui')
    assert current.delivery_state == 'delivered'
    assert current.lifecycle.check_status == 'not_evaluated'
    assert current.lifecycle.reason_codes == ('native_completion_recorded',)
    assert current.mutation_reauthorization_required
    missing = replace(batch, facts=changed('missing_contribution'))
    other = EvaluatedDeliveryReadiness(missing, actor_id='reader').context('authorization')
    assert other.lifecycle.stored_status == 'done'
    assert other.lifecycle.qualification_is_current is False
