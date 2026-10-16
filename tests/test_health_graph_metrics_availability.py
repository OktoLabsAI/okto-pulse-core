"""INV-13: failed, partial or capped graph observations are not known totals."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import interfaces
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.services import kg_health_service as health


@pytest.mark.parametrize("case", ["all_failed", "partial", "missing_rows", "truncated", "has_more", "cap", "empty", "complete"])
def test_graph_metrics_report_completeness_and_keep_query_bounds(monkeypatch, case):
    results = [{"rows": []} for _ in NODE_TYPES]
    if case == "all_failed":
        results = [RuntimeError("unavailable") for _ in NODE_TYPES]
    elif case == "partial":
        results[0] = {"rows": [[0.5], [0.8]]}
        results[1] = RuntimeError("unavailable")
    elif case == "missing_rows":
        results[0] = {}
    elif case in {"truncated", "has_more"}:
        results[0] = {"rows": [[0.5]], case: True}
    elif case == "cap":
        results[0] = {"rows": [[0.5]] * 10000}
    elif case == "complete":
        results[0] = {"rows": [[0.5], [0.8]]}

    def queries(executor, board_id, statements):
        assert board_id == "authorized-board"
        assert len(statements) == len(NODE_TYPES)
        assert all(limit == 10000 for _, _, limit in statements)
        return results

    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: SimpleNamespace(cypher_executor=object()))
    monkeypatch.setattr(health, "_read_health_query_group", queries)
    result = health._aggregate_graph_metrics("authorized-board")
    assert result["status"] == ("available" if case in {"empty", "complete"} else "unavailable")
    if case == "empty":
        assert result["total_nodes"] == result["default_score_count"] == 0
    elif case == "complete":
        assert result["total_nodes"] == 2
        assert result["default_score_count"] == 1
        assert result["avg_relevance"] == 0.65
    else:
        assert result["reason"] in {"graph_metrics_query_unavailable", "graph_metrics_limit_reached"}


def test_missing_provider_is_not_a_successful_empty_observation(monkeypatch):
    def missing():
        raise RuntimeError("provider unavailable")
    monkeypatch.setattr(interfaces, "get_kg_registry", missing)
    assert health._aggregate_graph_metrics("authorized-board")["status"] == "unavailable"
