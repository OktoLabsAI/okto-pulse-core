"""KG60: source update windows are not projection creation or historical diffs."""
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.tier_power import execute_natural_query


def test_temporal_search_uses_latest_source_update_across_projection_recreation(monkeypatch):
    board = "temporal-current-source"
    bootstrap_board_graph(board)
    stamp = lambda day: datetime(2026, 10, day, tzinfo=timezone.utc)
    with open_board_connection(board) as (_, connection):
        for identity, updated in (("changed", stamp(5)), ("old", stamp(2)), ("unknown", None)):
            connection.execute(
                "CREATE (:Bug {id: $id, title: 'temporal needle', content: 'temporal needle', "
                "graph_layer: 'canonical', source_confidence: 0.9, relevance_score: 0.5, "
                "created_at: $projected, source_created_at: $created, source_updated_at: $updated})",
                {"id": identity, "projected": stamp(7), "created": stamp(1), "updated": updated})
    monkeypatch.setattr(get_kg_registry(), "embedding_provider", SimpleNamespace(encode=lambda query: None))

    def search():
        return execute_natural_query(board, "temporal needle",
                                     since="2026-10-04", until="2026-10-06")

    first = search()
    assert {row["node_id"] for row in first["nodes"]} == {"changed"}
    assert first["nodes"][0]["source_updated_at"].startswith("2026-10-05")
    assert first["temporal_filter"]["field"] == "source_updated_at"
    assert first["temporal_filter"]["interpretation"] == "latest_source_update_not_history"
    assert first["temporal_filter"]["history_complete"] is False
    assert first["temporal_filter"]["as_of_supported"] is False
    # Re-projecting changes only graph creation time; source-window selection is stable.
    with open_board_connection(board) as (_, connection):
        connection.execute("MATCH (n:Bug) SET n.created_at = $projected", {"projected": stamp(4)})
    repeated = search()
    assert {row["node_id"] for row in repeated["nodes"]} == {"changed"}
    # A second source edit supersedes the last-update timestamp. It does not
    # reconstruct the earlier event in the old window.
    with open_board_connection(board) as (_, connection):
        connection.execute("MATCH (n:Bug {id: 'changed'}) SET n.source_updated_at = $updated",
                           {"updated": stamp(7)})
    assert search()["nodes"] == []


@pytest.mark.parametrize("failure", ["no_provider", "read_error", "malformed"])
def test_temporal_lookup_never_hides_unavailability_as_empty(monkeypatch, failure):
    from okto_pulse.core.kg import tier_power
    from okto_pulse.core.kg.interfaces import registry

    def read(*args, **kwargs):
        if failure == "read_error":
            raise RuntimeError("private diagnostic")
        return {"rows": [["node", "not a timestamp"]]}

    monkeypatch.setattr(registry, "get_kg_registry", lambda: SimpleNamespace(
        cypher_executor=None if failure == "no_provider" else SimpleNamespace(execute_read_only=read)))
    with pytest.raises(tier_power.TierPowerError) as error:
        tier_power._batch_lookup_source_updated_at("board", [{"node_type": "Bug", "node_id": "node"}])
    assert error.value.code == "query_temporal_unavailable"
    assert "private diagnostic" not in str(error.value)
