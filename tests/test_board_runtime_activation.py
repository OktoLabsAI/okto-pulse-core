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
