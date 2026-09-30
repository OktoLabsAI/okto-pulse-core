"""One work identity per capture, with the existing owning-Bug grouping."""
import pytest

from okto_pulse.core.domain.learning_materialization_work import LearningCaptureWorkRef, parse_learning_capture_work_ref
from okto_pulse.core.domain.learning_closeout import LearningCaptureSelection
from okto_pulse.core.events.types import LearningCaptureAdmitted, resolve_event_class
from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id


@pytest.mark.parametrize('bug,learning', [
    ('fef26310-f973-4828-8a71-9aee0c0e26e7', 'learning_123'),
    ('bug:with suffix/ç', 'learning:%colon:'),
])
def test_work_reference_round_trip_preserves_opaque_ids_and_bug_group(bug, learning):
    work = LearningCaptureWorkRef(bug, learning, 0)
    assert parse_learning_capture_work_ref(work.encode()) == work
    assert normalize_cognitive_artifact_id(work.encode()) == normalize_cognitive_artifact_id('bug:' + bug)
    assert LearningCaptureWorkRef(bug, learning + 'other', 0).encode() != work.encode()


@pytest.mark.parametrize('value', [
    'bug:b:learning:capture-v1:n:-1', 'bug:b:learning:capture-v1:n:01',
    'bug:b:learning:capture-v1::0', 'bug:b:learning:capture-v1:%ZZ:0',
])
def test_reserved_malformed_work_refs_fail_closed(value):
    with pytest.raises(ValueError):
        parse_learning_capture_work_ref(value)


def test_legacy_references_are_not_reinterpreted_as_capture_work():
    assert parse_learning_capture_work_ref('bug:b:learning:old-concept') is None
    assert normalize_cognitive_artifact_id('bug:b:learning:old-concept') == 'bug:b:learning:old-concept'


def test_capture_event_round_trips_only_identity_not_narrative_or_approval():
    event = LearningCaptureAdmitted(board_id='b', bug_id='bug', capture_author_id='author', actor_type='system',
        capture=LearningCaptureSelection(learning_id='learning', generation=0, fingerprint='a' * 64))
    assert resolve_event_class(event.event_type) is LearningCaptureAdmitted
    assert LearningCaptureAdmitted.model_validate(event.model_dump()) == event
    assert set(event.payload_for_storage()) == {'bug_id', 'capture_author_id', 'capture'}
    with pytest.raises(ValueError):
        LearningCaptureAdmitted.model_validate({**event.model_dump(), 'approved': True})


def test_distinct_capture_fingerprints_have_distinct_work_without_changing_bug_group():
    first = LearningCaptureWorkRef('bug:opaque', 'learning:opaque', 2, 'a' * 64)
    second = LearningCaptureWorkRef(first.bug_id, first.learning_id, first.generation, 'b' * 64)
    assert first.encode() != second.encode()
    assert parse_learning_capture_work_ref(first.encode()) == first
    assert parse_learning_capture_work_ref(second.encode()) == second
    assert normalize_cognitive_artifact_id(first.encode()) == normalize_cognitive_artifact_id(second.encode())
    assert LearningCaptureWorkRef('b', 'n', 0).encode() == 'bug:b:learning:capture-v1:n:0'


@pytest.mark.parametrize('suffix', ['0', '01:' + 'a' * 64, '0:' + 'A' * 64,
    '0:' + 'a' * 63, '0:' + 'a' * 65, '0:' + 'a' * 64 + ':extra'])
def test_malformed_v2_references_fail_closed(suffix):
    with pytest.raises(ValueError, match='learning_capture_work_reference_invalid'):
        parse_learning_capture_work_ref('bug:b:learning:capture-v2:n:' + suffix)


@pytest.mark.asyncio
async def test_v2_fingerprint_mismatch_never_invokes_source_or_graph():
    from okto_pulse.core.application.learning_materialization_worker import materialize_capture_work
    result = await materialize_capture_work(None, board_id='board',
        work=LearningCaptureWorkRef('b', 'n', 0, 'a' * 64), fingerprint='b' * 64, persister=None)
    assert result.outcome == 'materialization_failed'
    assert result.reason == 'learning_capture_work_source_mismatch'
