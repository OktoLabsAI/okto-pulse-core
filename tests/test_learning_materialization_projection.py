"""Literal projection preserves authorship and refuses implicit semantic edits."""
from dataclasses import replace

import pytest

from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection
from okto_pulse.core.application.learning_materialization import authored_learning_candidate, require_capture_candidates
from okto_pulse.core.kg.schemas import EdgeCandidate
from test_learning_closeout_binding import source, captured


def projection():
    record = captured(source(status='done'))
    return CapturedLearningProjection(record, record, record.payload['source']['bug_id'])


def literal(plan, **changes):
    return replace(plan.capture, source_revision=plan.capture.source_revision + 1, record_fingerprint='',
        evidence_refs=plan.evidence_refs, payload={**plan.fields,
            'generation': plan.capture.generation,
            'created_at': plan.capture.payload['captured_at'],
            'created_by_agent': plan.capture.payload['author_id'], **changes})


def test_literal_projection_keeps_original_content_and_separate_history():
    plan = projection()
    authored = dict(plan.capture.payload)
    head = literal(plan)
    replace(plan, head=head).require_literal_head()
    assert head.payload['content'] == authored['content']
    assert authored['context'] in head.payload['context']
    assert authored['applicability'] in head.payload['context']
    assert plan.capture.payload == authored


@pytest.mark.parametrize('left,right,valid', [
    ('2026-09-29T15:00:00Z', '2026-09-29T15:00:00+00:00', True),
    ('2026-09-29T12:00:00-03:00', '2026-09-29T15:00:00Z', True),
    ('2026-09-29T15:00:00', '2026-09-29T15:00:00Z', False),
    ('2026-09-29T15:00:01Z', '2026-09-29T15:00:00Z', False),
    (None, None, False),
])
def test_authored_instant_comparison_preserves_timezone_and_precision(left, right, valid):
    from okto_pulse.core.domain.learning_materialization import same_authored_timestamp
    assert same_authored_timestamp(left, right) is valid


@pytest.mark.parametrize('change', [
    {'human_curated': True}, {'superseded_by': 'another'}, {'revocation_reason': 'withdrawn'},
    {'content': 'Changed content'}, {'created_by_agent': 'another'},
    {'created_at': '2026-09-29T00:00:00+00:00'}, {'generation': 1},
])
def test_recovery_refuses_curated_revoked_superseded_or_changed_projection(change):
    plan = projection()
    with pytest.raises(ValueError, match='projection_conflict'):
        replace(plan, head=literal(plan, **change)).require_literal_head()


@pytest.mark.parametrize('damage', ['other_board', 'missing_evidence', 'birth_revision'])
def test_literal_head_cannot_substitute_scope_evidence_or_history(damage):
    plan = projection()
    changes = {'other_board': {'board_id': 'another'},
        'missing_evidence': {'evidence_refs': ()}, 'birth_revision': {'source_revision': 0}}[damage]
    with pytest.raises(ValueError, match='projection_conflict'):
        replace(plan, head=replace(literal(plan), record_fingerprint='', **changes)).require_literal_head()


@pytest.mark.parametrize('damage', [None, 'content', 'edge', 'extra_candidate'])
def test_internal_commit_shape_is_exact_and_has_one_validates_edge(damage):
    plan = projection()
    candidate = authored_learning_candidate(plan.capture)
    edge = EdgeCandidate(candidate_id='edge', edge_type='validates',
        from_candidate_id=candidate.candidate_id, to_candidate_id='kg:canonical-bug')
    candidates = {candidate.candidate_id: candidate}
    if damage == 'content': candidate.content = 'Unadmitted content'
    elif damage == 'edge': edge.to_candidate_id = 'unresolved'
    elif damage == 'extra_candidate': candidates['extra'] = candidate
    if damage is None:
        require_capture_candidates(plan, candidates, {'edge': edge})
    else:
        with pytest.raises(ValueError, match='candidate_mismatch'):
            require_capture_candidates(plan, candidates, {'edge': edge})
