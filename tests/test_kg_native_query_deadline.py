"""Core forwards its bounded deadline without learning a graph mechanism."""

from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.interfaces import registry
from okto_pulse.core.kg.tier_power import execute_cypher_read_only


@pytest.mark.parametrize("include_working", [False, True])
@pytest.mark.parametrize("requested,expected", [(None, 15000), (1500, 1500), (99999, 30000)])
def test_core_passes_deadline_to_scalar_and_paired_read(monkeypatch, include_working, requested, expected):
    calls = []
    envelope = dict(rows=[], columns=[], row_count=0, truncated=False)

    class Executor:
        def execute_read_only(self, board, query, params, *, max_rows, timeout_ms):
            calls.append(("scalar", board, timeout_ms))
            return dict(envelope)

        def execute_read_only_pair(self, board, primary, comparison, params, *, max_rows, timeout_ms):
            calls.append(("pair", board, timeout_ms))
            return dict(primary=dict(envelope), comparison=dict(envelope))

    monkeypatch.setattr(registry, "get_kg_registry", lambda: SimpleNamespace(cypher_executor=Executor()))
    execute_cypher_read_only("b", "MATCH (n:Decision) RETURN n.id",
                            timeout_ms=requested, include_working=include_working)
    assert calls == [("scalar" if include_working else "pair", "b", expected)]
