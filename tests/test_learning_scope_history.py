"""A scoped replacement is a linked committed history, not an opaque marker."""
from dataclasses import replace

import pytest

from okto_pulse.core.application.learning_supersedence import read_learning_scope_replacements
from okto_pulse.core.domain.learning_supersedence import qualify_learning_scope_replacement, scope_reference_additions
from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection
from okto_pulse.core.ports.learning_capture import LearningCaptureSourceRef
from test_learning_materialization_projection import projection, literal


def records(board_id='board', suffix='one', bug_id='covered-bug', *, source=None, predecessor=None):
    original = projection()
    previous = predecessor or replace(literal(original), board_id=board_id, source_revision=0, record_fingerprint='')
    capture = replace(original.capture, board_id=board_id, node_id='replacement-' + suffix,
        source_revision=0, record_fingerprint='', payload={**original.capture.payload,
            'capture_format': 'learning-capture/v2', 'capture_id': 'scoped-' + suffix,
            'source': {**original.capture.payload['source'], 'board_id': board_id, 'bug_id': bug_id},
            'intent': {'kind': 'supersede', 'target_node_id': previous.node_id,
                'target_generation': previous.generation, 'expected_fingerprint': previous.record_fingerprint,
                'reason': 'Corrected deployment requires this replacement', 'scope': 'source_bug'}})
    if source is not None:
        capture = replace(capture, record_fingerprint='', payload={**capture.payload,
            'source': {**capture.payload['source'], 'digest': source.source_digest,
                'policy_version': source.source_policy_version}})
    plan = CapturedLearningProjection(capture, capture, bug_id)
    successor = literal(plan)
    reference = LearningCaptureSourceRef(capture.node_id, capture.generation, capture.record_fingerprint)
    claimed = replace(previous, source_revision=previous.source_revision + 1, record_fingerprint='',
        evidence_refs=(*previous.evidence_refs, reference.encode()))
    return previous, claimed, capture, successor


def test_scope_claim_preserves_literal_target_and_has_exact_committed_successor():
    previous, claimed, capture, successor = records()
    claim = qualify_learning_scope_replacement(previous=previous, claimed=claimed, capture=capture, successor=successor)
    assert claim.bug_id == 'covered-bug'
    assert claimed.payload == previous.payload
    assert claimed.record_fingerprint != previous.record_fingerprint
    assert claim.successor.node_id != claim.previous.node_id
    assert scope_reference_additions((previous, claimed))[0][:2] == (previous, claimed)


def test_prepared_scope_claim_changes_only_provenance_and_keeps_target_birth():
    from okto_pulse.core.domain.learning_supersedence import prepare_learning_scope_replacement
    previous, claimed, capture, successor = records()
    successor = replace(successor, source_session_id='governed-commit', committed_at='2026-09-29T16:00:00+00:00')
    prepared = prepare_learning_scope_replacement(previous=previous, capture=capture, successor=successor)
    assert prepared.claimed.record_fingerprint == claimed.record_fingerprint
    assert prepared.claimed.payload == previous.payload
    assert prepared.claimed.source_session_id == 'governed-commit'
    assert prepared.claimed.committed_at == successor.committed_at
    assert prepared.claimed.evidence_refs[:-1] == previous.evidence_refs


@pytest.mark.parametrize('damage', ['target_content', 'dropped_evidence', 'unknown_reference', 'target_revision',
    'successor_revision', 'successor_content', 'successor_evidence', 'successor_identity', 'v1', 'wrong_target'])
def test_claim_cannot_substitute_target_or_uncommitted_successor(damage):
    previous, claimed, capture, successor = records()
    if damage == 'target_content': claimed = replace(claimed, record_fingerprint='', payload={**claimed.payload, 'content': 'other'})
    elif damage == 'dropped_evidence': claimed = replace(claimed, record_fingerprint='', evidence_refs=claimed.evidence_refs[-1:])
    elif damage == 'unknown_reference': claimed = replace(claimed, record_fingerprint='', evidence_refs=(*previous.evidence_refs, 'bug:unrelated'))
    elif damage == 'target_revision': claimed = replace(claimed, source_revision=2)
    elif damage == 'successor_revision': successor = replace(successor, source_revision=2)
    elif damage == 'successor_content': successor = replace(successor, record_fingerprint='', payload={**successor.payload, 'content': 'other'})
    elif damage == 'successor_evidence': successor = replace(successor, record_fingerprint='', evidence_refs=())
    elif damage == 'successor_identity': successor = replace(successor, record_fingerprint='', node_id='foreign')
    elif damage == 'v1':
        capture = replace(capture, record_fingerprint='', payload={**capture.payload, 'capture_format': 'learning-capture/v1',
            'intent': {key: value for key, value in capture.payload['intent'].items() if key != 'scope'}})
    else:
        capture = replace(capture, record_fingerprint='', payload={**capture.payload,
            'intent': {**capture.payload['intent'], 'expected_fingerprint': 'f' * 64}})
    error = ('learning_capture_payload_invalid' if damage == 'v1'
             else 'learning_(scope_claim_invalid|materialization_projection_conflict)')
    with pytest.raises(ValueError, match=error):
        qualify_learning_scope_replacement(previous=previous, claimed=claimed, capture=capture, successor=successor)


def test_later_curation_cannot_silently_drop_prior_scope_claim():
    previous, claimed, _, _ = records()
    curated = replace(previous, record_fingerprint='', source_revision=2,
        payload={**previous.payload, 'content': 'A later human correction', 'human_curated': True})
    with pytest.raises(ValueError, match='scope_history_claim_lost'):
        scope_reference_additions((previous, claimed, curated))


def test_pending_reuse_is_not_a_literal_claim_deletion():
    previous, claimed, capture, _ = records()
    pending = replace(capture, node_id=previous.node_id, record_fingerprint='', source_revision=2,
        payload={**capture.payload, 'capture_format': 'learning-capture/v2', 'intent': {
            'kind': 'reuse', 'target_node_id': claimed.node_id, 'target_generation': claimed.generation,
            'expected_fingerprint': claimed.record_fingerprint, 'reason': 'Applies to another origin', 'scope': None}})
    assert len(scope_reference_additions((previous, claimed, pending))) == 1


class History:
    def __init__(self, target, successor):
        self.target, self.successor = target, successor

    async def read_history_in_context(self, context, *, node_id, **scope):
        return self.target if node_id == self.target[0].node_id else self.successor


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', [None, 'missing_capture', 'missing_successor', 'stale_head', 'foreign_board'])
async def test_scope_reader_requires_exact_target_and_successor_history(damage):
    previous, claimed, capture, successor = records()
    history = (capture, successor)
    head = claimed
    if damage == 'missing_capture': history = (successor,)
    elif damage == 'missing_successor': history = (capture,)
    elif damage == 'stale_head': head = previous
    elif damage == 'foreign_board': history = tuple(replace(row, board_id='foreign', record_fingerprint='') for row in history)
    store = History((previous, claimed), history)
    if damage is not None:
        with pytest.raises(ValueError, match='learning_scope_(claim_invalid|history_changed)'):
            await read_learning_scope_replacements(None, store, head=head)
    else:
        claim, = await read_learning_scope_replacements(None, store, head=head)
        assert claim.capture == capture and claim.successor == successor


@pytest.mark.parametrize('change,expected', [({}, True), ({'status': 'in_progress'}, False),
    ({'source_policy_version': 4}, False), ({'conclusions': ({'text': 'New correction'},)}, False),
    ({'bug_id': 'other-bug'}, False)])
def test_scope_applicability_requires_current_done_source_not_only_bug_identity(change, expected):
    from test_learning_closeout_binding import source
    from okto_pulse.core.domain.learning_supersedence import learning_scope_replacement_is_current
    current = source(status='done', bug_id='covered-bug')
    previous, claimed, capture, successor = records(source=current)
    claim = qualify_learning_scope_replacement(previous=previous, claimed=claimed, capture=capture, successor=successor)
    observed = source(**({'status': 'done', 'bug_id': 'covered-bug'} | change))
    assert learning_scope_replacement_is_current(claim, observed, None) is expected


@pytest.mark.parametrize('damage', [None, 'fingerprint', 'revision', 'before', 'closed', 'duplicate', 'corrupt'])
def test_scope_binding_matches_exact_capture_and_current_closed_basis(damage):
    from test_learning_closeout_binding import source, bind
    from okto_pulse.core.domain.learning_supersedence import learning_scope_replacement_is_current
    from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
    before = source(bug_id='covered-bug')
    closed = source(status='done', source_policy_version=4, bug_id='covered-bug')
    previous, claimed, capture, successor = records(source=before)
    claim = qualify_learning_scope_replacement(previous=previous, claimed=claimed, capture=capture, successor=successor)
    # Structural binding fixture; lifecycle admission is qualified by integration tests.
    value = bind(before, closed).model_dump(exclude={'sha256'})
    value.update(capture={'learning_id': capture.node_id, 'generation': capture.generation,
        'fingerprint': capture.record_fingerprint}, capture_revision=capture.source_revision)
    if damage == 'fingerprint': value['capture']['fingerprint'] = 'f' * 64
    elif damage == 'revision': value['capture_revision'] += 1
    elif damage == 'before': value['before_digest'] = 'f' * 64
    elif damage == 'closed': value['closed_digest'] = 'f' * 64
    value['sha256'] = canonical_sha256(value)
    if damage == 'corrupt': value['closed_digest'] = 'f' * 64
    history = [value, value] if damage == 'duplicate' else [value]
    if damage in {'duplicate', 'corrupt'}:
        with pytest.raises(ValueError, match='learning_closeout_(history_invalid|binding_integrity_invalid)'):
            learning_scope_replacement_is_current(claim, closed, history)
    else:
        assert learning_scope_replacement_is_current(claim, closed, history) is (damage is None)


@pytest.mark.asyncio
@pytest.mark.parametrize('same_basis', [False, True])
async def test_history_preserves_distinct_corrections_of_one_bug(same_basis):
    from test_learning_closeout_binding import source
    first = records(source=source(status='done'))
    second = records(suffix='two', predecessor=first[1],
        source=source(status='done', source_policy_version=3 if same_basis else 4))

    class MultipleHistory:
        async def read_history_in_context(self, context, *, node_id, **scope):
            if node_id == first[0].node_id:
                return first[:2] + (second[1],)
            return first[2:] if node_id == first[2].node_id else second[2:]

    if same_basis:
        with pytest.raises(ValueError, match='learning_scope_claim_ambiguous'):
            await read_learning_scope_replacements(None, MultipleHistory(), head=second[1])
    else:
        claims = await read_learning_scope_replacements(None, MultipleHistory(), head=second[1])
        assert len(claims) == 2 and claims[0].bug_id == claims[1].bug_id


@pytest.mark.asyncio
@pytest.mark.parametrize('authenticated', [True, False])
async def test_current_scope_requires_evidence_authentication_even_when_digest_matches(monkeypatch, authenticated):
    from types import SimpleNamespace
    from test_learning_closeout_binding import source
    from okto_pulse.core.application import learning_capture
    from okto_pulse.core.application.learning_supersedence import current_learning_scope_replacement
    from okto_pulse.core.ports import application_persistence
    current = source(status='done', bug_id='covered-bug')
    previous, claimed, capture, successor = records(source=current)
    claim = qualify_learning_scope_replacement(previous=previous, claimed=claimed, capture=capture, successor=successor)
    bug = SimpleNamespace(board_id=current.board_id, status=current.status, learning_closeout_bindings=[])
    class Persistence:
        async def get(self, *args, **kwargs): return bug
        async def refresh(self, context, value): return value
    monkeypatch.setattr(application_persistence, 'get_application_persistence_port', lambda: Persistence())
    checked = []
    def authenticate(observed, selected):
        checked.append((observed, selected))
        if not authenticated:
            raise ValueError('learning_capture_evidence_not_authenticated')
    monkeypatch.setattr(learning_capture, '_require_current_capture_evidence', authenticate)
    if authenticated:
        assert await current_learning_scope_replacement(None, (claim,), source=current) == claim
    else:
        with pytest.raises(ValueError, match='learning_capture_evidence_not_authenticated'):
            await current_learning_scope_replacement(None, (claim,), source=current)
    assert checked == [(current, capture)]
