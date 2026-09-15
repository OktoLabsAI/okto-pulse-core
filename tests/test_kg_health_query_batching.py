"""Optional backend-neutral Health batches preserve scalar degradation policy."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import interfaces, schema_contract
from okto_pulse.core.kg.interfaces.cypher_executor import (
    CypherExecutor, ReadOnlyBatchCypherExecutor,
)
from okto_pulse.core.services import kg_health_service as health


class Scalar:
    def __init__(self, failures=()):
        self.singles = []
        self.failures = failures

    def is_supported(self):
        return True

    def answer(self, query):
        label = "A" if "n:A" in query else "B"
        if label in self.failures:
            raise RuntimeError("injected per-type read failure")
        if "RETURN n.relevance_score" in query:
            return {"rows": [[None], [0.5], [0.9]] if label == "A" else [[0.2]]}
        return {"rows": [["canonical", "accepted", 3]] if label == "A"
                else [[None, None, 1]]}

    def execute_read_only(self, board_id, query, params=None, *, max_rows=1000):
        self.singles.append((board_id, query, params, max_rows))
        return self.answer(query)


class Batch(Scalar):
    def __init__(self, behavior="ok", failures=()):
        super().__init__(failures)
        self.behavior = behavior
        self.batches = []

    def execute_read_only_batch(self, board_id, statements):
        self.batches.append((board_id, list(statements)))
        if self.behavior == "raise":
            raise RuntimeError("injected complete-batch refusal")
        if self.behavior == "short":
            return [{"rows": [[99999]]}]  # must never contribute a prefix
        if self.behavior == "malformed":
            return [None for _ in statements]
        return [self.answer(query) for query, _params, _limit in statements]


@pytest.fixture(autouse=True)
def node_types(monkeypatch):
    monkeypatch.setattr(schema_contract, "NODE_TYPES", ("A", "B"))


def snapshot(monkeypatch, executor):
    monkeypatch.setattr(interfaces, "get_kg_registry",
                        lambda: SimpleNamespace(cypher_executor=executor))
    return (health._aggregate_graph_metrics("board"),
            health._aggregate_kg_layer_counts("board"))


def test_batch_preserves_queries_parameters_limits_order_and_answers(monkeypatch):
    scalar = Scalar()
    expected = snapshot(monkeypatch, scalar)
    batch = Batch()
    assert snapshot(monkeypatch, batch) == expected
    assert expected[0] == {"total_nodes": 4, "default_score_count": 1, "avg_relevance": 0.5333}
    assert len(scalar.singles) == 4
    assert len(batch.batches) == 2 and batch.singles == []
    flattened = [(board, query, params, limit) for board, group in batch.batches
                 for query, params, limit in group]
    assert flattened == scalar.singles


@pytest.mark.parametrize("behavior", ["raise", "short", "malformed"])
def test_failed_or_incomplete_batch_discards_prefix_and_retries_scalar(monkeypatch, behavior):
    expected = snapshot(monkeypatch, Scalar())
    batch = Batch(behavior)
    assert snapshot(monkeypatch, batch) == expected
    assert len(batch.singles) == 4 and len(batch.batches) == 2


@pytest.mark.parametrize("failures", [("B",), ("A", "B")])
def test_per_type_failures_keep_partial_or_unavailable_status(monkeypatch, failures):
    expected = snapshot(monkeypatch, Scalar(failures))
    batch = Batch(failures=failures)
    assert snapshot(monkeypatch, batch) == expected
    assert expected[1]["failed_node_types"] == len(failures)
    assert expected[1]["status"] == ("partial" if len(failures) == 1 else "unavailable")


def test_scalar_port_is_not_forced_to_implement_optional_batch():
    assert isinstance(Scalar(), CypherExecutor)
    assert not isinstance(Scalar(), ReadOnlyBatchCypherExecutor)
    assert isinstance(Batch(), ReadOnlyBatchCypherExecutor)


def test_empty_group_performs_no_read():
    executor = Batch()
    assert health._read_health_query_group(executor, "board", []) == []
    assert not executor.singles and not executor.batches
