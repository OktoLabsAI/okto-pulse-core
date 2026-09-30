"""Reuse provenance is an explicit revision chain, never a union of guesses."""
from dataclasses import replace

import pytest

from okto_pulse.core.application.learning_capture import resolve_learning_capture_projection
from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection
from test_learning_materialization_projection import projection, literal


def reuse(previous, original, *, bug_id='another-bug', identity='reuse-1'):
    capture = replace(original, record_fingerprint='', source_revision=previous.source_revision + 1,
        payload={**original.payload, 'capture_id': identity, 'author_id': 'another-author',
            'source': {**original.payload['source'], 'bug_id': bug_id},
            'context': 'New association context', 'applicability': 'Explicit new scope',
            'intent': {'kind': 'reuse', 'target_node_id': previous.node_id,
                'target_generation': previous.generation, 'expected_fingerprint': previous.record_fingerprint,
                'reason': 'Applies to the corrected new Bug'}})
    plan = CapturedLearningProjection(capture, capture, bug_id, previous)
    head = replace(capture, record_fingerprint='', source_revision=capture.source_revision + 1,
        payload=plan.literal_payload, evidence_refs=plan.evidence_refs)
    return plan, head


class History:
    def __init__(self, *records):
        self.records = {record.record_fingerprint: record for record in records}

    async def read_fingerprint_in_context(self, context, **selector):
        return self.records.get(selector['fingerprint'])

    async def read_history_in_context(self, context, **selector):
        return tuple(sorted((record for record in self.records.values()
            if (record.board_id, record.node_id, record.generation) == (
                selector['board_id'], selector['node_id'], selector['generation'])),
            key=lambda record: record.source_revision))


def test_reuse_keeps_curated_content_birth_generation_and_original_origin():
    original = projection()
    previous = literal(original, human_curated=True, generation=0, embedding=[0.25, 0.75])
    plan, head = reuse(previous, original.capture)
    plan.require_literal_head()
    replace(plan, head=head).require_literal_head()
    assert {key: value for key, value in head.payload.items() if key != 'source_content_hash'} == {
        key: value for key, value in previous.payload.items() if key != 'source_content_hash'}
    assert head.evidence_refs == (*previous.evidence_refs, 'bug:another-bug')
    assert head.payload['source_content_hash'] == plan.capture.record_fingerprint


@pytest.mark.asyncio
async def test_earlier_capture_recovery_proves_multiple_reuse_revisions_without_regressing_head():
    original = projection()
    birth = literal(original)
    first, one = reuse(birth, original.capture)
    second, two = reuse(one, original.capture, bug_id='third-bug', identity='reuse-2')
    store = History(original.capture, birth, first.capture, one, second.capture, two)
    for selected in (original.capture, first.capture, second.capture):
        plan = await resolve_learning_capture_projection(None, store, capture=selected,
            head=two, bug_id=selected.payload['source']['bug_id'])
        plan.require_literal_head()
        assert plan.literal_payload == two.payload
        assert plan.evidence_refs == two.evidence_refs
        assert plan.capture == selected


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['unrelated', 'content', 'birth', 'origin', 'evidence', 'missing_target', 'pending'])
async def test_recovery_does_not_treat_evidence_refs_as_revision_authority(damage):
    original = projection()
    birth = literal(original)
    reused, head = reuse(birth, original.capture)
    if damage in {'content', 'birth', 'origin'}:
        key = {'content': 'content', 'birth': 'created_by_agent', 'origin': 'source_artifact_ref'}[damage]
        head = replace(head, record_fingerprint='', payload={**head.payload, key: 'changed'})
    elif damage == 'evidence':
        head = replace(head, record_fingerprint='', evidence_refs=(*head.evidence_refs, 'bug:unproved'))
    elif damage == 'pending':
        head = reused.capture
    elif damage == 'unrelated':
        original = replace(original, capture=replace(original.capture, record_fingerprint='',
            payload={**original.capture.payload, 'capture_id': 'other-capture'}))
    store = History(original.capture, reused.capture, head, *(() if damage == 'missing_target' else (birth,)))
    with pytest.raises(ValueError, match='learning_materialization_projection_'):
        await resolve_learning_capture_projection(None, store, capture=original.capture,
            head=head, bug_id=original.bug_id)


def test_repeated_same_bug_association_advances_provenance_without_reauthoring_content():
    original = projection()
    birth = literal(original)
    reused, head = reuse(birth, original.capture, bug_id=original.bug_id)
    assert head.evidence_refs == birth.evidence_refs
    assert head.payload['content'] == birth.payload['content']
    assert head.record_fingerprint != birth.record_fingerprint
    assert head.payload['source_content_hash'] == reused.capture.record_fingerprint


@pytest.mark.asyncio
@pytest.mark.parametrize('reuse_after_claim', [False, True])
async def test_recovery_keeps_proved_scope_refs_without_changing_other_origins(reuse_after_claim):
    from test_learning_scope_history import records
    original = projection()
    birth = literal(original)
    _, claimed, replacement_capture, successor = records(predecessor=birth)
    history = [original.capture, birth, claimed, replacement_capture, successor]
    head = claimed
    if reuse_after_claim:
        reused, head = reuse(claimed, original.capture, bug_id='uncovered-bug')
        history.extend((reused.capture, head))
    plan = await resolve_learning_capture_projection(None, History(*history), capture=original.capture,
        head=head, bug_id=original.bug_id)
    assert plan.evidence_refs == head.evidence_refs
    assert plan.literal_payload == head.payload
    assert plan.capture == original.capture


@pytest.mark.asyncio
async def test_recovery_does_not_preserve_an_opaque_scope_ref_without_successor_proof():
    from test_learning_scope_history import records
    original = projection()
    birth = literal(original)
    _, claimed, replacement_capture, _ = records(predecessor=birth)
    with pytest.raises(ValueError, match='learning_scope_claim_invalid'):
        await resolve_learning_capture_projection(None, History(original.capture, birth, claimed, replacement_capture),
            capture=original.capture, head=claimed, bug_id=original.bug_id)
