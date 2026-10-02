"""Recovery fairly revisits backend receipts without adopting manual work."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.learning_materialization_work import LearningCaptureWorkRef
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItem
from okto_pulse.core.kg.workers import cognitive_closeout


@pytest.mark.asyncio
async def test_recovery_is_bounded_fair_and_excludes_human_receipts(monkeypatch):
    items = [CognitiveConsolidationItem(item_id=str(index), board_id='board', kg_generation_id='gen',
        source_ref=LearningCaptureWorkRef('bug', f'learning-{index}', 0, 'a' * 64).encode(), artifact_type='bug',
        status='consolidated', recorded_at='2026-09-29T00:00:00+00:00',
        updated_by_agent_id=cognitive_closeout.AGENT_ID, reason='authored_capture_materialized',
        outcome_type='candidate_created', evidence_refs=(f'kg:learning-{index}',), content_hash='a' * 64)
        for index in range(3)]
    items.append(replace(items[0], item_id='human', updated_by_agent_id='human'))
    calls = []
    async def drain(_factory, _board, **kwargs):
        calls.append(kwargs['recovery_item_ids'])
        return []
    monkeypatch.setattr(cognitive_closeout, 'drain_cognitive_closeout_pending', drain)
    worker = cognitive_closeout.CognitiveCloseoutWorker(object(),
        store=SimpleNamespace(list_items=lambda *_args: items), recovery_batch_size=2,
        pending_work_provider=SimpleNamespace(list_records=lambda: [SimpleNamespace(board_id='board', kg_generation_id='gen')]))
    assert await worker.drain_once() == 0
    assert await worker.drain_once() == 0
    assert len(calls) == 2 and all(len(ids) == 2 for ids in calls)
    assert set.union(*(set(ids) for ids in calls)) == {'0', '1', '2'}
