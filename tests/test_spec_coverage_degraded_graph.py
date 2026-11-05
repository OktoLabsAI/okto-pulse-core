"""A graph timeout must not erase an already authorized source snapshot."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.service_catalog import CoreAnalyticsOperations
from okto_pulse.core.kg.interfaces.graph_errors import GraphQueryTimeout
from okto_pulse.core.models.spec_coverage_query import SpecCoverageResponse
from okto_pulse.core.services.spec_coverage_query import project_spec_coverage
from test_spec_coverage_query import QUERY, source


def catalog(monkeypatch, reader):
    monkeypatch.setattr(
        'okto_pulse.core.ports.relational_application.require_relational_application_adapter',
        lambda: SimpleNamespace(spec_coverage_read=lambda _: reader),
    )
    return CoreAnalyticsOperations(object())


@pytest.mark.asyncio
async def test_graph_timeout_keeps_source_and_exact_proof_without_false_graph_success(monkeypatch):
    query = replace(QUERY, read_graph=True)
    snapshot = source()
    reader = SimpleNamespace(read=AsyncMock(return_value=snapshot),
        read_graph=AsyncMock(side_effect=GraphQueryTimeout('native deadline exceeded')))
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
