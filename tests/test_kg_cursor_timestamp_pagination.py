"""Regression coverage for timestamp-typed KG cursor pagination.

The graph schema stores ``created_at`` as a TIMESTAMP, while the opaque cursor
codec deliberately transports its value as an ISO string.  The query template
must perform that conversion at the graph-language boundary.
"""

from __future__ import annotations

from okto_pulse.core.kg import cypher_templates as tpl
from okto_pulse.core.kg.cursor_codec import encode_cursor
from okto_pulse.core.kg.kg_service import KGService


def test_cursor_templates_cast_iso_cursor_to_graph_timestamp() -> None:
    """Both keyset variants compare TIMESTAMP values, never STRING parameters."""
    for template in (
        tpl.GET_ALL_NODES_AFTER_CURSOR,
        tpl.GET_ALL_NODES_BY_TYPE_AFTER_CURSOR,
    ):
        assert template.count("timestamp($cursor_ts)") == 2
        assert "n.created_at < $cursor_ts" not in template
        assert "n.created_at = $cursor_ts" not in template


def test_get_all_nodes_selects_timestamp_cast_cursor_template(monkeypatch) -> None:
    """The service preserves the decoded cursor pair and selects the keyset query."""
    observed: dict[str, object] = {}

    class Executor:
        def execute_read_only(self, board_id, cypher, params, *, max_rows):
            observed.update(
                board_id=board_id,
                cypher=cypher,
                params=params,
                max_rows=max_rows,
            )
            return {"rows": []}

    import okto_pulse.core.kg.kg_service as service_module

    monkeypatch.setattr(service_module, "_get_cypher_executor", lambda: Executor())
    service = KGService(emit_hit_events=False)
    # Avoid requiring a configured cache backend: this test targets service-to-executor
    # selection and parameter preservation, not cache behavior.
    monkeypatch.setattr(
        service,
        "_cached_call",
        lambda _tool_name, _board_id, _cache_params, query: query(),
    )

    timestamp = "2026-09-05T12:39:14.519749Z"
    node_id = "entity-anchor"
    service.get_all_nodes(
        "board-1",
        max_rows=5,
        cursor=encode_cursor(timestamp, node_id),
        node_type="Entity",
    )

    assert observed["board_id"] == "board-1"
    assert observed["max_rows"] == 5
    assert "timestamp($cursor_ts)" in observed["cypher"]
    params = observed["params"]
    assert params["cursor_ts"] == timestamp
    assert params["cursor_id"] == node_id
