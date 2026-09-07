from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors.global_outbox import (
    DIGEST_VISIBILITY_BATCH_SIZE,
    GlobalOutboxProcessor,
)


@pytest.mark.parametrize('size', [0, 1, 512, 513, 1025, 2414])
def test_visibility_identity_payload_is_bounded_and_complete(size):
    calls = []
    active = {f'active-{i}' for i in range(size)}
    revoked = {'revoked-1', 'revoked-2'}

    def execute(statement, params):
        assert len(params['ids']) <= DIGEST_VISIBILITY_BATCH_SIZE
        assert params['bid'] == 'board'
        calls.append(params)
        return SimpleNamespace(rows=((len(params['ids']),),))

    count = GlobalOutboxProcessor._set_board_digest_source_visibility(
        SimpleNamespace(execute=execute), 'board',
        active_source_ids=active, revoked_source_ids=revoked,
    )
    assert count == size + 2
    for expected, flag in ((active, False), (revoked, True)):
        observed = [key for call in calls if call['revoked'] is flag for key in call['ids']]
        assert observed == sorted(expected)


def test_visibility_failure_propagates_and_retry_converges_without_rewriting():
    state = {}
    calls = 0
    fail = True

    def execute(statement, params):
        nonlocal calls
        calls += 1
        if fail and calls == 2:
            raise RuntimeError('second batch failed')
        changed = sum(state.get(key) is not params['revoked'] for key in params['ids'])
        state.update({key: params['revoked'] for key in params['ids']})
        return SimpleNamespace(rows=((changed,),))

    source_ids = {str(i) for i in range(1025)}
    runtime = SimpleNamespace(execute=execute)
    with pytest.raises(RuntimeError, match='second batch failed'):
        GlobalOutboxProcessor._set_board_digest_source_visibility(
            runtime, 'board', active_source_ids=source_ids, revoked_source_ids=set(),
        )
    assert len(state) == 512
    fail = False
    assert GlobalOutboxProcessor._set_board_digest_source_visibility(
        runtime, 'board', active_source_ids=source_ids, revoked_source_ids=set(),
    ) == 513
    assert state == dict.fromkeys(source_ids, False)
    assert GlobalOutboxProcessor._set_board_digest_source_visibility(
        runtime, 'board', active_source_ids=source_ids, revoked_source_ids=set(),
    ) == 0
