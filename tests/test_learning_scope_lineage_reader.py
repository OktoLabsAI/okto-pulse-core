"""Public history distinguishes declared scope, durable linkage and currentness."""
from dataclasses import replace

import pytest

from okto_pulse.core.application.learning_supersedence import LearningScopeHistoryPageReader
from okto_pulse.core.ports.kg_cognitive_source import CognitiveSourceUnavailable
from test_learning_scope_history import records


class Reader:
    def __init__(self, target, successor):
        self.histories = {target[0].node_id: target, successor[0].node_id: successor}
        self.calls = []

    async def read_bounded_history_in_context(self, context, *, board_id, node_id, generation, max_records):
        self.calls.append((node_id, max_records))
        rows = self.histories.get(node_id, ())
        if len(rows) > max_records:
            raise CognitiveSourceUnavailable('cognitive_source_history_limit', board_id=board_id, node_id=node_id)
        return rows


@pytest.mark.asyncio
async def test_recorded_linkage_is_exact_historical_and_request_cached():
    previous, claimed, capture, successor = records()
    store = Reader((previous, claimed), (capture, successor))
    reader = LearningScopeHistoryPageReader(object(), store)
    lineage = await reader.lineage(capture)
    assert lineage['state'] == 'recorded' and lineage['scope'] == 'source_bug'
    assert lineage['bug_id'] == capture.payload['source']['bug_id']
    assert lineage['capture_fingerprint'] == capture.record_fingerprint
    assert lineage['target']['fingerprint'] == previous.record_fingerprint
    assert lineage['target_claim']['fingerprint'] == claimed.record_fingerprint
    assert lineage['successor_birth']['fingerprint'] == successor.record_fingerprint
    assert lineage['current_applicability'] == lineage['graph_projection'] == 'not_assessed'
    assert await reader.lineage(capture) == lineage and len(store.calls) == 2
    assert reader.remaining == 196


@pytest.mark.asyncio
async def test_pending_capture_is_not_reported_as_a_recorded_replacement():
    previous, _, capture, _ = records()
    lineage = await LearningScopeHistoryPageReader(None, Reader((previous,), (capture,))).lineage(capture)
    assert lineage['state'] == 'unverified' and lineage['limitation'] == 'not_recorded'
    assert 'target_claim' not in lineage and 'successor_birth' not in lineage
    unavailable = await LearningScopeHistoryPageReader(None, object()).lineage(capture)
    assert unavailable['limitation'] == 'history_capability_unavailable'


@pytest.mark.asyncio
async def test_page_budget_is_shared_and_exhaustion_stops_further_history_reads():
    previous, claimed, capture, successor = records()
    store = Reader((previous, claimed), (capture, successor))
    reader = LearningScopeHistoryPageReader(None, store)
    reader.remaining = 1
    assert (await reader.lineage(capture))['limitation'] == 'history_limit'
    assert (await reader.lineage(capture))['state'] == 'unverified'
    assert store.calls == [(previous.node_id, 1)] and reader.remaining == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['missing_claim', 'missing_successor', 'foreign_scope', 'changed_successor', 'capture_revision'])
async def test_partial_or_inconsistent_history_never_becomes_recorded_linkage(damage):
    previous, claimed, capture, successor = records()
    target, descendants = (previous, claimed), (capture, successor)
    if damage == 'missing_claim': target = (previous,)
    elif damage == 'missing_successor': descendants = (capture,)
    elif damage == 'foreign_scope':
        target = tuple(replace(row, board_id='foreign', record_fingerprint='') for row in target)
    elif damage == 'changed_successor':
        descendants = (capture, replace(successor, payload={**successor.payload, 'content': 'other'}, record_fingerprint=''))
    else: capture = replace(capture, source_revision=9)
    with pytest.raises(ValueError):
        await LearningScopeHistoryPageReader(None, Reader(target, descendants)).lineage(capture)


@pytest.mark.asyncio
async def test_unscoped_supersede_does_not_acquire_linkage_or_scope():
    _, _, capture, _ = records()
    capture = replace(capture, record_fingerprint='', payload={**capture.payload,
        'capture_format': 'learning-capture/v2', 'intent': {**capture.payload['intent'], 'scope': None}})
    assert await LearningScopeHistoryPageReader(None, object()).lineage(capture) is None
