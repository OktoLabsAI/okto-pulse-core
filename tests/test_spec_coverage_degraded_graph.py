"""A graph timeout must not erase an already authorized source snapshot."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.service_catalog import CoreAnalyticsOperations
from okto_pulse.core.kg.interfaces.graph_errors import GraphQueryTimeout
from okto_pulse.core.models.spec_coverage_query import SpecCoverageResponse
from okto_pulse.core.ports.spec_coverage_query import SpecCoverageGraphFacts
from okto_pulse.core.services.spec_coverage_query import project_spec_coverage
from okto_pulse.core.services import spec_coverage_query as projection
from test_spec_coverage_query import QUERY, source


def catalog(monkeypatch, reader):
    monkeypatch.setattr(
        'okto_pulse.core.ports.relational_application.require_relational_application_adapter',
        lambda: SimpleNamespace(spec_coverage_read=lambda _: reader),
    )
    return CoreAnalyticsOperations(object())


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['before_source', 'final_source_fence', 'projection'])
async def test_required_deadline_phase_is_sanitized_without_changing_failure(monkeypatch, caplog, phase):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])
    failure = GraphQueryTimeout('SELECT private_secret /private/path')

    async def fence(*args, **kwargs):
        if phase == 'final_source_fence':
            raise failure
        if phase == 'projection':
            now[0] = 116.0

    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
                             verify_source=AsyncMock(side_effect=fence))
    with pytest.raises(GraphQueryTimeout) as caught:
        await catalog(monkeypatch, reader).spec_coverage(
            replace(QUERY, read_graph=False), timeout_ms=15000,
            deadline=99.0 if phase == 'before_source' else None)
    if phase == 'before_source':
        reader.read.assert_not_awaited()
    elif phase == 'final_source_fence':
        assert caught.value is failure
    messages = [r.getMessage() for r in caplog.records if 'spec_coverage.required_timeout' in r.getMessage()]
    assert len(messages) == 1 and f'stage={phase}' in messages[0]
    assert 'private' not in caplog.text and 'SELECT' not in caplog.text


@pytest.mark.asyncio
async def test_graph_timeout_keeps_source_and_exact_proof_without_false_graph_success(monkeypatch):
    query = replace(QUERY, read_graph=True)
    snapshot = source()
    reader = SimpleNamespace(read=AsyncMock(return_value=snapshot),
        read_graph=AsyncMock(side_effect=GraphQueryTimeout('native deadline exceeded')),
        verify_source=AsyncMock())
    result = await catalog(monkeypatch, reader).spec_coverage(query, timeout_ms=15000)
    SpecCoverageResponse.model_validate(result)
    expected = project_spec_coverage(query, snapshot)
    assert result['structure']['summary'] == expected['structure']['summary']
    assert result['delivery'] == expected['delivery']
    assert result['items'] == expected['items']
    assert result['data_source'] == 'relational'
    assert result['projection_freshness']['state'] == 'unavailable'
    assert result['structure']['graph']['observed_nodes'] is None
    assert result['structure']['graph']['missing_nodes'] is None
    assert not result['completeness']['complete_for_scope']
    assert reader.read_graph.await_count == 1  # no retry storm
    assert reader.verify_source.await_count == 1
    assert reader.read_graph.call_args.kwargs['timeout_ms'] <= 500
    assert reader.verify_source.call_args.args == (query, snapshot.source_revision)


@pytest.mark.asyncio
async def test_source_timeout_cannot_be_presented_as_available_coverage(monkeypatch):
    reader = SimpleNamespace(read=AsyncMock(side_effect=GraphQueryTimeout('source timeout')),
        read_graph=AsyncMock())
    with pytest.raises(GraphQueryTimeout):
        await catalog(monkeypatch, reader).spec_coverage(replace(QUERY, read_graph=True), timeout_ms=15000)
    reader.read_graph.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('error', [ValueError('spec_coverage_source_changed'), asyncio.CancelledError()])
async def test_changed_source_and_caller_cancellation_are_not_hidden(monkeypatch, error):
    reader = SimpleNamespace(read=AsyncMock(return_value=source()), read_graph=AsyncMock(side_effect=error))
    with pytest.raises(type(error)):
        await catalog(monkeypatch, reader).spec_coverage(replace(QUERY, read_graph=True), timeout_ms=15000)


@pytest.mark.asyncio
async def test_source_change_during_optional_timeout_is_still_refused(monkeypatch):
    changed = False

    async def graph(*args, **kwargs):
        nonlocal changed
        changed = True
        raise GraphQueryTimeout('optional phase expired')

    async def fence(*args, **kwargs):
        assert changed
        raise ValueError('spec_coverage_source_changed')

    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
        read_graph=AsyncMock(side_effect=graph), verify_source=AsyncMock(side_effect=fence))
    with pytest.raises(ValueError, match='spec_coverage_source_changed'):
        await catalog(monkeypatch, reader).spec_coverage(replace(QUERY, read_graph=True), timeout_ms=15000)
    reader.verify_source.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('graph_error', [None, GraphQueryTimeout('optional phase expired')])
async def test_mandatory_final_fence_timeout_never_becomes_available_data(monkeypatch, graph_error):
    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
        read_graph=AsyncMock(return_value=SpecCoverageGraphFacts(), side_effect=graph_error),
        verify_source=AsyncMock(side_effect=GraphQueryTimeout('mandatory final fence expired')))
    with pytest.raises(GraphQueryTimeout, match='mandatory final fence'):
        await catalog(monkeypatch, reader).spec_coverage(replace(QUERY, read_graph=True), timeout_ms=15000)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode,phase_ms', [('summary', 500), ('detail', 5000)])
async def test_graph_phase_is_capped_without_spending_the_whole_source_fence_budget(monkeypatch, mode, phase_ms):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])

    async def graph(*args, **kwargs):
        assert kwargs['timeout_ms'] == phase_ms
        assert kwargs['deadline'] == 100.0 + phase_ms / 1000
        now[0] = kwargs['deadline']
        raise GraphQueryTimeout('optional phase expired')

    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
        read_graph=AsyncMock(side_effect=graph), verify_source=AsyncMock())
    result = await catalog(monkeypatch, reader).spec_coverage(
        replace(QUERY, read_graph=True, graph_mode=mode), timeout_ms=15000)
    assert result['structure']['graph']['state'] == 'unavailable'
    assert reader.verify_source.call_args.kwargs == {'timeout_ms': 15000-phase_ms, 'deadline': 115.0}


@pytest.mark.asyncio
async def test_caller_deadline_clips_all_phases_and_never_resets(monkeypatch):
    monkeypatch.setattr('time.monotonic', lambda: 100.0)
    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
        read_graph=AsyncMock(return_value=SpecCoverageGraphFacts()), verify_source=AsyncMock())
    await catalog(monkeypatch, reader).spec_coverage(
        replace(QUERY, read_graph=True, graph_mode='detail'), timeout_ms=15000, deadline=100.125)
    for operation in (reader.read, reader.verify_source):
        assert operation.call_args.kwargs == {'timeout_ms': 125, 'deadline': 100.125}
    assert reader.read_graph.call_args.kwargs == {'timeout_ms': 62, 'deadline': 100.062}


@pytest.mark.asyncio
@pytest.mark.parametrize('mode,source_seconds', [('summary', 14.5), ('detail', 10.0)])
async def test_slow_source_cannot_let_optional_graph_starve_final_fence(monkeypatch, mode, source_seconds):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])
    snapshot = source()

    async def read(*args, **kwargs):
        now[0] += source_seconds
        return snapshot

    async def graph(*args, **kwargs):
        now[0] = kwargs['deadline']
        raise GraphQueryTimeout('optional phase expired')

    async def fence(*args, **kwargs):
        assert kwargs['deadline'] == 115.0
        assert kwargs['timeout_ms'] > 100
        now[0] += 0.1

    reader = SimpleNamespace(read=AsyncMock(side_effect=read),
        read_graph=AsyncMock(side_effect=graph), verify_source=AsyncMock(side_effect=fence))
    query = replace(QUERY, read_graph=True, graph_mode=mode)
    result = await catalog(monkeypatch, reader).spec_coverage(query, timeout_ms=15000)
    reader.verify_source.assert_awaited_once()
    assert result['structure']['graph']['state'] == 'unavailable'
    assert result['structure']['graph']['observed_nodes'] is None
    assert result['delivery'] == project_spec_coverage(query, snapshot)['delivery']
    assert result['items'] == project_spec_coverage(query, snapshot)['items']


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['summary', 'detail'])
async def test_required_proof_is_evaluated_once_before_budgeting_optional_graph(monkeypatch, mode):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])
    snapshot = source()
    query = replace(QUERY, read_graph=True, graph_mode=mode)
    expected_delivery = project_spec_coverage(query, snapshot)['delivery']
    events = []
    evaluate = projection.evaluate_delivery_coverage

    def proof(*args, **kwargs):
        events.append('proof')
        now[0] += 0.5
        return evaluate(*args, **kwargs)

    async def graph(*args, **kwargs):
        assert events == ['proof']
        events.append('graph')
        assert kwargs == {'timeout_ms': 250, 'deadline': 100.75}
        now[0] = kwargs['deadline']
        raise GraphQueryTimeout('optional graph expired')

    async def fence(*args, **kwargs):
        events.append('fence')
        assert kwargs == {'timeout_ms': 250, 'deadline': 101.0}
        now[0] += 0.125

    monkeypatch.setattr(projection, 'evaluate_delivery_coverage', proof)
    reader = SimpleNamespace(read=AsyncMock(return_value=snapshot),
        read_graph=AsyncMock(side_effect=graph), verify_source=AsyncMock(side_effect=fence))
    result = await catalog(monkeypatch, reader).spec_coverage(query, timeout_ms=1000)
    assert events == ['proof', 'graph', 'fence']
    assert result['delivery'] == expected_delivery
    assert result['structure']['graph']['state'] == 'unavailable'


@pytest.mark.asyncio
@pytest.mark.parametrize('changed_graph', [False, True])
async def test_prepared_projection_validates_cursor_only_against_final_graph(monkeypatch, changed_graph):
    from test_spec_coverage_graph import observed

    monkeypatch.setattr('time.monotonic', lambda: 100.0)
    snapshot = observed()
    first_query = replace(QUERY, read_graph=True, limit=1)
    cursor = project_spec_coverage(first_query, snapshot)['next_cursor']
    query = replace(first_query, cursor=cursor)
    graph = replace(snapshot.graph, generation='changed') if changed_graph else snapshot.graph
    reader = SimpleNamespace(
        read=AsyncMock(return_value=replace(snapshot, graph=None, graph_scope=None)),
        read_graph=AsyncMock(return_value=graph), verify_source=AsyncMock())
    operation = catalog(monkeypatch, reader)
    if changed_graph:
        with pytest.raises(ValueError, match='spec_coverage_cursor_stale'):
            await operation.spec_coverage(query, timeout_ms=15000)
    else:
        result = await operation.spec_coverage(query, timeout_ms=15000)
        assert result == project_spec_coverage(query, snapshot)
    reader.read_graph.assert_awaited_once()
    reader.verify_source.assert_awaited_once()


@pytest.mark.asyncio
async def test_last_millisecond_is_reserved_for_required_fence(monkeypatch):
    monkeypatch.setattr('time.monotonic', lambda: 100.0)
    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
        read_graph=AsyncMock(return_value=SpecCoverageGraphFacts()), verify_source=AsyncMock())
    result = await catalog(monkeypatch, reader).spec_coverage(
        replace(QUERY, read_graph=True, graph_mode='detail'), timeout_ms=15000, deadline=100.0015)
    reader.read_graph.assert_not_awaited()
    reader.verify_source.assert_awaited_once()
    assert reader.verify_source.call_args.kwargs == {'timeout_ms': 1, 'deadline': 100.0015}
    assert result['structure']['graph']['state'] == 'unavailable'
    assert result['structure']['graph']['expected_nodes'] is None
    assert result['structure']['graph']['observed_nodes'] is None


@pytest.mark.asyncio
async def test_expired_caller_never_opens_the_source_reader(monkeypatch):
    monkeypatch.setattr('time.monotonic', lambda: 100.0)
    reader = SimpleNamespace(read=AsyncMock(), read_graph=AsyncMock(), verify_source=AsyncMock())
    with pytest.raises(GraphQueryTimeout):
        await catalog(monkeypatch, reader).spec_coverage(QUERY, timeout_ms=15000, deadline=99.0)
    reader.read.assert_not_awaited()
    reader.read_graph.assert_not_awaited()
    reader.verify_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_whole_deadline_exhaustion_cannot_return_optional_fallback(monkeypatch):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])

    async def graph(*args, **kwargs):
        now[0] = 101.0
        raise GraphQueryTimeout('whole deadline expired')

    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
        read_graph=AsyncMock(side_effect=graph), verify_source=AsyncMock())
    with pytest.raises(GraphQueryTimeout):
        await catalog(monkeypatch, reader).spec_coverage(replace(QUERY, read_graph=True), timeout_ms=200)
    reader.verify_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_restricted_graph_still_requires_a_final_source_fence(monkeypatch):
    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
        read_graph=AsyncMock(), verify_source=AsyncMock(side_effect=ValueError('spec_coverage_source_changed')))
    with pytest.raises(ValueError, match='spec_coverage_source_changed'):
        await catalog(monkeypatch, reader).spec_coverage(QUERY, timeout_ms=15000)
    reader.read_graph.assert_not_awaited()


@pytest.mark.asyncio
async def test_unrelated_graph_failure_is_not_disguised_as_unavailable(monkeypatch):
    reader = SimpleNamespace(read=AsyncMock(return_value=source()),
        read_graph=AsyncMock(side_effect=RuntimeError('adapter defect')), verify_source=AsyncMock())
    with pytest.raises(RuntimeError, match='adapter defect'):
        await catalog(monkeypatch, reader).spec_coverage(replace(QUERY, read_graph=True), timeout_ms=15000)
