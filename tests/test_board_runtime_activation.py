"""Authorization and ownership contract for explicit Board runtime activation."""
import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
import threading

import pytest

from okto_pulse.core.application.use_cases import boards_crud as module
from okto_pulse.core.application.use_cases import authorization
from okto_pulse.core.application import kg_runtime_access
from okto_pulse.core.application.use_cases.base import EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.kg import guarded_write
from okto_pulse.core.kg.interfaces.graph_errors import GraphCapabilityUnavailable, GraphUnavailable
from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphHandle


@pytest.fixture
def activation(monkeypatch):
    events = []
    actor = SimpleNamespace(actor_id="authorized-user")
    uow = SimpleNamespace(commit=AsyncMock())
    access = AsyncMock(return_value=object())
    authorize = AsyncMock()
    monkeypatch.setattr(module, 'load_accessible_board', access)
    monkeypatch.setattr(authorization, 'require_all', authorize)
    lifecycle = SimpleNamespace(open=AsyncMock(side_effect=lambda board: handle()))
    monkeypatch.setattr(kg_runtime_access, 'resolve_graph_lifecycle', lambda: lifecycle)
    lease = SimpleNamespace(ensure_durable=lambda: events.append('durable'),
        ensure_owned=lambda **_: events.append('owned'))

    @contextmanager
    def boundary(board_id, **kwargs):
        events.append(('acquire', board_id, kwargs))
        try:
            yield lease
        finally:
            events.append('release')

    monkeypatch.setattr(guarded_write, 'guarded_board_write', boundary)
    return SimpleNamespace(actor=actor, uow=uow, access=access, authorize=authorize,
        lifecycle=lifecycle, events=events, lease=lease)


def handle(**overrides):
    fields = dict(board_id='board', storage_ref=None, opened=True,
        status='opened', locked=False, quarantined=False, readers_prepared=True)
    fields.update(overrides)
    return GraphHandle(**fields)


async def execute(fixture):
    return await module.ActivateBoardRuntimeUseCase().execute(
        module.ActivateBoardRuntimeCommand('board'), actor=fixture.actor, uow=fixture.uow)


@pytest.mark.asyncio
async def test_activation_authorizes_before_lifecycle_and_preserves_durability(activation):
    async def open_board(board):
        activation.authorize.assert_awaited_once()
        activation.events.append('open')
        return handle()
    activation.lifecycle.open.side_effect = open_board
    assert await execute(activation) == {'board_id': 'board', 'state': 'ready'}
    assert activation.events[1:] == ['open', 'durable', 'open', 'owned', 'release']
    assert activation.events[0][2]['owner_id'] == 'authorized-user'
    assert [r.operation for r in activation.authorize.call_args.args[1:]] == [
        'board.read', 'kg.query.related_context']
    activation.uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_reader_does_not_acquire_busy_writer_or_repeat_durability(activation):
    def observe(board):
        activation.authorize.assert_awaited_once()
        assert board == 'board'
        return handle()
    activation.lifecycle.observe_prepared_reader = observe
    def refused(*args, **kwargs):
        raise AssertionError('prepared reader must not acquire writer')
    from okto_pulse.core.kg import guarded_write as guards
    original = guards.guarded_board_write
    guards.guarded_board_write = refused
    try:
        assert await execute(activation) == {'board_id': 'board', 'state': 'ready'}
    finally:
        guards.guarded_board_write = original
    activation.lifecycle.open.assert_not_awaited()
    assert activation.events == []


@pytest.mark.asyncio
async def test_missing_prepared_reader_preserves_full_cold_activation(activation):
    activation.lifecycle.observe_prepared_reader = lambda board: None
    assert await execute(activation) == {'board_id': 'board', 'state': 'ready'}
    assert 'durable' in activation.events
    assert activation.lifecycle.open.await_count == 2


@pytest.mark.asyncio
async def test_cold_guard_admission_contention_is_busy_before_any_lifecycle(activation, monkeypatch):
    from okto_pulse.core.kg.interfaces.graph_errors import GraphActivationBusy
    activation.lifecycle.observe_prepared_reader = lambda board: None
    failure = guarded_write.GuardedWriteError('lock_contention', 'private owner details', retryable=True)

    @contextmanager
    def refused(*args, **kwargs):
        activation.authorize.assert_awaited_once()
        raise failure
        yield  # Make the failure occur at __enter__, as in the real guard.

    monkeypatch.setattr(guarded_write, 'guarded_board_write', refused)
    with pytest.raises(GraphActivationBusy) as caught:
        await execute(activation)
    assert caught.value.__cause__ is failure
    assert caught.value.details == {'phase': 'acquire_writer', 'cause_code': 'lock_contention'}
    assert 'private' not in str(caught.value)
    activation.lifecycle.open.assert_not_awaited()
    assert activation.events == []
    activation.uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['open', 'durable', 'owned', 'exit'])
async def test_cold_contention_after_guard_entry_is_never_replayable_busy(activation, monkeypatch, phase):
    from okto_pulse.core.kg.interfaces.graph_errors import GraphActivationBusy
    activation.lifecycle.observe_prepared_reader = lambda board: None
    failure = guarded_write.GuardedWriteError('lock_contention', 'private owner details', retryable=True)

    def fail(*args, **kwargs):
        raise failure

    if phase == 'open':
        activation.lifecycle.open.side_effect = failure
    elif phase == 'durable':
        activation.lease.ensure_durable = fail
    elif phase == 'owned':
        activation.lease.ensure_owned = fail
    else:
        @contextmanager
        def failing_exit(*args, **kwargs):
            yield activation.lease
            raise failure
        monkeypatch.setattr(guarded_write, 'guarded_board_write', failing_exit)
    with pytest.raises(guarded_write.GuardedWriteError) as caught:
        await execute(activation)
    assert caught.value is failure
    assert not isinstance(caught.value, GraphActivationBusy)
    assert activation.lifecycle.open.await_count == (1 if phase in ('open', 'durable') else 2)
    activation.uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('state', [{'opened': False}, {'locked': True}, {'quarantined': True},
                                  {'board_id': 'other'}, {'readers_prepared': False}])
async def test_invalid_prepared_handle_never_reports_ready_or_mutates(activation, state):
    activation.lifecycle.observe_prepared_reader = lambda board: handle(**state)
    with pytest.raises(GraphUnavailable):
        await execute(activation)
    assert activation.events == []
    activation.lifecycle.open.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_reader_refusal_is_not_cold_recovery(activation):
    def observe(board):
        raise GraphUnavailable('closing')
    activation.lifecycle.observe_prepared_reader = observe
    with pytest.raises(GraphUnavailable):
        await execute(activation)
    assert activation.events == []


@pytest.mark.asyncio
async def test_busy_observation_can_recover_without_replaying_durability(activation):
    from okto_pulse.core.kg.interfaces.graph_errors import GraphActivationBusy, GraphQueryTimeout
    observations = iter([GraphQueryTimeout('reader admission expired'), handle()])
    def observe(board):
        result = next(observations)
        if isinstance(result, Exception):
            raise result
        return result
    activation.lifecycle.observe_prepared_reader = observe
    with pytest.raises(GraphActivationBusy) as failure:
        await execute(activation)
    assert failure.value.retryable
    assert failure.value.details == {
        'phase': 'observe_prepared_reader', 'cause_code': 'graph_query_timeout'}
    assert activation.events == []
    assert await execute(activation) == {'board_id': 'board', 'state': 'ready'}
    assert activation.events == []
    activation.lifecycle.open.assert_not_awaited()


@pytest.mark.asyncio
async def test_cold_lifecycle_timeout_is_not_replayable_activation_busy(activation):
    from okto_pulse.core.kg.interfaces.graph_errors import GraphActivationBusy, GraphQueryTimeout
    activation.lifecycle.observe_prepared_reader = lambda board: None
    failure = GraphQueryTimeout('durability failed')
    def timeout():
        raise failure
    activation.lease.ensure_durable = timeout
    with pytest.raises(GraphQueryTimeout) as raised:
        await execute(activation)
    assert raised.value is failure
    assert not isinstance(raised.value, GraphActivationBusy)
    assert activation.events[-1] == 'release'


@pytest.mark.asyncio
@pytest.mark.parametrize('state', [{'opened': False}, {'locked': True}, {'quarantined': True}, {'board_id': 'other'}])
async def test_closed_or_refused_storage_never_enters_durability(activation, state):
    activation.lifecycle.open.side_effect = lambda board: handle(**state)
    with pytest.raises(GraphUnavailable):
        await execute(activation)
    assert 'durable' not in activation.events
    assert activation.events[-1] == 'release'


@pytest.mark.asyncio
async def test_missing_board_never_enters_native_lifecycle(activation):
    activation.access.return_value = None
    with pytest.raises(EntityNotFoundError):
        await execute(activation)
    assert activation.events == []
    activation.authorize.assert_not_awaited()


@pytest.mark.asyncio
async def test_unauthorized_activation_never_enters_native_lifecycle(activation):
    activation.authorize.side_effect = PermissionDeniedError('not allowed')
    with pytest.raises(PermissionDeniedError):
        await execute(activation)
    assert activation.events == []
    activation.lifecycle.open.assert_not_awaited()


@pytest.mark.asyncio
async def test_writer_open_does_not_manufacture_prepared_reader_availability(activation):
    activation.lifecycle.open.side_effect = lambda board: GraphHandle(
        board, None, True, 'opened', False, False)
    with pytest.raises(GraphCapabilityUnavailable) as failure:
        await execute(activation)
    assert failure.value.details == {'reason': 'prepared_reader_unavailable'}
    assert activation.events[-1] == 'release'


@pytest.mark.asyncio
async def test_reopened_handle_must_still_match_authorized_board(activation):
    activation.lifecycle.open.side_effect = [handle(), handle(board_id='other')]
    with pytest.raises(GraphCapabilityUnavailable):
        await execute(activation)
    assert 'owned' not in activation.events
    assert activation.events[-1] == 'release'


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['ensure_durable', 'ensure_owned'])
async def test_durability_or_ownership_failure_cannot_report_ready(activation, phase):
    failure = guarded_write.GuardedWriteError('writer_lease_lost', 'Lost owner', retryable=True)
    def fail(**kwargs):
        raise failure
    setattr(activation.lease, phase, fail)
    with pytest.raises(guarded_write.GuardedWriteError) as raised:
        await execute(activation)
    assert raised.value is failure
    assert activation.events[-1] == 'release'
    assert activation.lifecycle.open.await_count == (1 if phase == 'ensure_durable' else 2)
    activation.uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_activation_cancellation_drains_native_ownership_before_returning(activation):
    entered, release = threading.Event(), threading.Event()
    async def open_board(board):
        entered.set()
        assert release.wait(5)
        return handle()
    activation.lifecycle.open.side_effect = open_board
    task = asyncio.create_task(execute(activation))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert 'release' not in activation.events
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert activation.events[-2:] == ['owned', 'release']
