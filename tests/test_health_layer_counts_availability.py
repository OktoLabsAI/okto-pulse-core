"""INV-13: incomplete layer observations must not masquerade as known totals."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import interfaces
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.services import kg_health_service as health


@pytest.mark.parametrize("case", [
    "all_failed", "partial", "missing_rows", "truncated", "has_more", "cap",
    "short", "negative", "null_count", "bool_count", "empty", "complete",
])
def test_layer_counts_require_complete_bounded_observation(monkeypatch, case):
    results = [{"rows": []} for _ in NODE_TYPES]
    if case == "all_failed":
        results = [RuntimeError("unavailable") for _ in NODE_TYPES]
    elif case == "partial":
        results[0] = {"rows": [["canonical", "accepted", 5]]}
        results[1] = RuntimeError("unavailable")
    elif case == "missing_rows":
        results[0] = {}
    elif case in {"truncated", "has_more"}:
        results[0] = {"rows": [["canonical", "accepted", 5]], case: True}
    elif case == "cap":
        results[0] = {"rows": [["canonical", "accepted", 1]] * 10000}
    elif case == "short":
        results.pop()
    elif case in {"negative", "null_count", "bool_count"}:
        value = {"negative": -1, "null_count": None, "bool_count": True}[case]
        results[0] = {"rows": [["canonical", "accepted", value]]}
    elif case == "complete":
        results[0] = {"rows": [["canonical", "accepted", 5], ["working", "draft", 2]]}

    def queries(executor, board_id, statements):
        assert board_id == "authorized-board"
        assert len(statements) == len(NODE_TYPES)
        assert all(limit == 10000 for _, _, limit in statements)
        return results

    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: SimpleNamespace(cypher_executor=object()))
    monkeypatch.setattr(health, "_read_health_query_group", queries)
    result = health._aggregate_kg_layer_counts("authorized-board")
    if case == "empty":
        assert result["status"] == "ok"
        assert result["by_layer"]["canonical"] == result["by_layer"]["working"] == 0
    elif case == "complete":
        assert result["status"] == "ok"
        assert result["by_layer"]["canonical"] == 5
        assert result["by_layer"]["working"] == 2
        assert result["by_maturity_status"] == {"accepted": 5, "draft": 2}
    else:
        assert result["status"] in {"partial", "unavailable"}
        assert result["by_layer"] == result["by_maturity_status"] == {}


def test_fallback_does_not_fabricate_zero_layer_counts():
    result = health._unavailable_kg_layer_counts("probe_timeout")
    assert result == {
        "status": "unavailable", "by_layer": {}, "by_maturity_status": {},
        "reason": "probe_timeout",
    }
