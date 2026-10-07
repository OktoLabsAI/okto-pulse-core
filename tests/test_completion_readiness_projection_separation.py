"""BASE T39/T40: projection diagnostics cannot mask substantive readiness."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import cognitive_readiness as readiness
from okto_pulse.core.kg.cognitive_closeout_gate import CognitiveCloseoutGate
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItem
from okto_pulse.core.ports.kg_operational import KGCanonicalDebtSignal, KGDeadLetterSignal
from okto_pulse.core.services.main import (
    GovernedCompletionBlocked, _evaluate_cognitive_readiness_or_raise,
)

NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)


class Store:
    def __init__(self, status):
        self.items = [] if status is None else [CognitiveConsolidationItem(
            item_id='item', board_id='board', kg_generation_id='generation',
            source_ref='bug:bug', artifact_type='bug', status=status,
            recorded_at=NOW.isoformat(),
            reason_code='evidence_insufficient' if status == 'skipped' else None,
            revisit_at=(NOW - timedelta(seconds=1)).isoformat() if status == 'skipped' else None,
        )]

    def latest_generation(self, board):
        return 'generation'

    def read_completion_snapshot(self, board, kg_generation_id=None):
        return 'generation', self.items

    def list_items(self, board, generation):
        return self.items


@pytest.mark.asyncio
@pytest.mark.parametrize('technical', ['dlq', 'projection_debt'])
@pytest.mark.parametrize('status,expected', [(None, None), ('pending', 'cognitive_active'),
                                          ('failed', 'cognitive_active'), ('skipped', 'skip_expired')])
async def test_completion_uses_substantive_verdict_while_diagnostics_keep_projection(
    monkeypatch, technical, status, expected,
):
    class Signals:
        async def list_dead_letter_signals(self, context, *, board_id):
            return [KGDeadLetterSignal('bug', 'bug')] if technical == 'dlq' else []

        async def list_canonical_debt_signals(self, context, *, board_id):
            return [KGCanonicalDebtSignal('bug', 'bug', 'bug:bug', 'pending', 'projection_pending')]

    monkeypatch.setattr(readiness, 'get_kg_operational_read_model_port', lambda: Signals())
    store = Store(status)
    before = [item.to_dict() for item in store.items]
    service = readiness.CognitiveReadinessService(store, now=lambda: NOW)
    diagnostic = await service.evaluate_artifact(None, board_id='board', source_ref='bug:bug')
    assert diagnostic.tier == ('technical_dlq' if technical == 'dlq' else 'canonical_debt_open')
    substantive = await service.evaluate_completion(None, board_id='board', source_ref='bug:bug')
    assert substantive.blocking is (expected is not None)
    if expected is not None:
        assert substantive.tier == expected
    assert diagnostic.precedence_explanation['completion_tier'] == substantive.tier
    for payload in (diagnostic, diagnostic.to_api()):
        assert readiness.completion_would_block_done(payload, True) is (expected == 'skip_expired')
        assert not readiness.completion_would_block_done(payload, False)
    if expected == 'cognitive_active':
        # Active items retain their existing authority in the first closeout
        # gate; the supplementary readiness gate owns expired skips.
        legacy = CognitiveCloseoutGate(store=store).evaluate(
            board_id='board', entity_type='bug', entity_id='bug', target_status='done',
        )
        assert not legacy.allowed and legacy.blocking_count == 1
    call = _evaluate_cognitive_readiness_or_raise(
        service_factory=lambda: service, db=None, board_id='board', entity_type='bug',
        entity_id='bug', entity=SimpleNamespace(id='bug'), target_label='Bug', policy_blocking=True,
    )
    if expected in {None, 'cognitive_active'}:
        await call
    else:
        with pytest.raises(GovernedCompletionBlocked) as failure:
            await call
        assert failure.value.code == expected
    assert [item.to_dict() for item in store.items] == before
