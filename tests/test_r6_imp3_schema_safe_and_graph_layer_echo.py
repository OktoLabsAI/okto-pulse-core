"""R6-IMP3 (card 8ba9034f, FR3/AC3) — schema-safe introspection + real graph_layer
selectors with applied_graph_layer echo.

* schema_info exposes an additive per-label ``label_properties`` map so agents query
  only schema-safe ``stable_properties`` (never a universal/ad-hoc property like
  ``name``); ``has_vector_index`` carries the real per-label variation.
* okto_pulse_kg_query_global / okto_pulse_kg_get_related_context echo the
  ``applied_graph_layer`` top-level (additive) and fail closed on an invalid layer;
  canonical|working|all behaviour is unchanged.

Anti-test-theater: graph_layer behaviour is driven through the REAL MCP query tools
over a seeded board graph (reusing the test_kg_layer_propagation harness); the
schema-safe map is asserted from the REAL schema constants / get_schema_info.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg.global_discovery.schema import (
    bootstrap_global_discovery,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.schema import (
    NODE_TYPES,
    STABLE_NODE_PROPERTIES,
    VECTOR_INDEX_TYPES,
)
from okto_pulse.core.kg.tier_power import _label_properties_map, get_schema_info
from test_kg_layer_propagation import (
    CENTER_REF,
    QUERY_TEXT,
    _call,
    _register_query_tools,
    _seed_board_subgraph,
    _seed_global_digests,
)


@pytest.fixture(scope="module", autouse=True)
def _isolated_global_discovery_runtime():
    # R6-IMP3 rework: own the global-discovery graph lifecycle for this module
    # (mirrors test_kg_layer_propagation) so this file neither inherits nor leaks
    # cross-board global-discovery state — order-robust regardless of run order.
    reset_global_discovery_runtime_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_discovery_runtime_for_tests()


# ===========================================================================
# schema-safe per-label property map
# ===========================================================================


def test_stable_node_properties_are_schema_safe():
    # The stable set is the schema-guaranteed scalar properties; the 384-d vector
    # and any ad-hoc/universal name are NOT in it.
    assert "embedding" not in STABLE_NODE_PROPERTIES
    assert "name" not in STABLE_NODE_PROPERTIES
    for prop in ("id", "title", "content", "graph_layer", "source_confidence"):
        assert prop in STABLE_NODE_PROPERTIES, prop


def test_label_properties_map_per_label():
    m = _label_properties_map()
    # Every canonical label is enumerated.
    assert set(m) == set(NODE_TYPES)
    for label, entry in m.items():
        # stable_properties is the schema-safe set (no embedding, no universal name).
        assert "title" in entry["stable_properties"]
        assert "graph_layer" in entry["stable_properties"]
        assert "embedding" not in entry["stable_properties"]
        assert "name" not in entry["stable_properties"]
        # has_vector_index carries the REAL per-label variation.
        assert entry["has_vector_index"] == (label in VECTOR_INDEX_TYPES)
    # Spot-check the per-label vector-index discrimination.
    assert m["Decision"]["has_vector_index"] is True
    assert m["Alternative"]["has_vector_index"] is False


def test_schema_info_includes_label_properties():
    board_id = _seed_global_digests()
    info = get_schema_info(board_id)
    # Additive: existing keys preserved + new label_properties present.
    assert "stable_node_types" in info and "vector_indexes" in info
    assert "label_properties" in info
    assert "Decision" in info["label_properties"]
    assert "title" in info["label_properties"]["Decision"]["stable_properties"]


# ===========================================================================
# graph_layer selector — applied_graph_layer echo + fail-closed
# ===========================================================================


def test_query_global_echoes_applied_graph_layer(monkeypatch):
    board_id = _seed_global_digests()
    tool = _register_query_tools(board_id, monkeypatch)["okto_pulse_kg_query_global"]

    default = _call(tool, board_id=board_id, nl_query=QUERY_TEXT)
    assert default["applied_graph_layer"] == "canonical"           # default
    # per-result graph_layer still present (R6-IMP2 contract unchanged).
    assert "canonical" in {r.get("graph_layer") for r in default["results"]}

    both = _call(tool, board_id=board_id, nl_query=QUERY_TEXT, graph_layer="all")
    assert both["applied_graph_layer"] == "all"

    working = _call(tool, board_id=board_id, nl_query=QUERY_TEXT, graph_layer="working")
    assert working["applied_graph_layer"] == "working"


def test_query_global_invalid_graph_layer_fails_closed(monkeypatch):
    board_id = _seed_global_digests()
    tool = _register_query_tools(board_id, monkeypatch)["okto_pulse_kg_query_global"]

    bad = _call(tool, board_id=board_id, nl_query=QUERY_TEXT, graph_layer="bogus")
    assert bad["error"]["code"] == "invalid_param"
    assert "results" not in bad  # no partial/leaky result on invalid layer


def test_get_related_context_echoes_and_fails_closed(monkeypatch):
    board_id = _seed_board_subgraph()
    tool = _register_query_tools(board_id, monkeypatch)["okto_pulse_kg_get_related_context"]

    default = _call(tool, board_id=board_id, artifact_id=CENTER_REF)
    assert default["applied_graph_layer"] == "canonical"

    bad = _call(tool, board_id=board_id, artifact_id=CENTER_REF, graph_layer="bogus")
    assert bad["error"]["code"] == "invalid_param"
    assert "context" not in bad


def test_get_related_context_rejects_raw_uuid_before_graph_lookup(monkeypatch):
    board_id = _seed_board_subgraph()
    raw_uuid = str(uuid.uuid4())
    calls = {"get_related_context": 0}

    class FakeService:
        def check_board_access(self, boards, checked_board_id):
            assert checked_board_id == board_id

        def get_related_context(self, *args, **kwargs):
            calls["get_related_context"] += 1
            return []

    import okto_pulse.core.mcp.kg_query_tools as kg_query_tools

    monkeypatch.setattr(kg_query_tools, "get_kg_service", lambda: FakeService())
    tool = _register_query_tools(board_id, monkeypatch)["okto_pulse_kg_get_related_context"]

    result = _call(tool, board_id=board_id, artifact_id=raw_uuid)
    assert result["error"]["code"] == "invalid_artifact_ref"
    assert result["error"]["supported"] == ["spec:<uuid>", "card:<uuid>"]
    assert calls["get_related_context"] == 0


def test_get_related_context_accepts_typed_uuid_refs(monkeypatch):
    board_id = _seed_board_subgraph()
    artifact_ref = f"spec:{uuid.uuid4()}"
    calls = {"artifact_id": None}

    class FakeService:
        def check_board_access(self, boards, checked_board_id):
            assert checked_board_id == board_id

        def get_related_context(self, board_id_arg, artifact_id, **kwargs):
            calls["artifact_id"] = artifact_id
            return []

    import okto_pulse.core.mcp.kg_query_tools as kg_query_tools

    monkeypatch.setattr(kg_query_tools, "get_kg_service", lambda: FakeService())
    tool = _register_query_tools(board_id, monkeypatch)["okto_pulse_kg_get_related_context"]

    result = _call(tool, board_id=board_id, artifact_id=artifact_ref)
    assert result["context"] == []
    assert result["applied_graph_layer"] == "canonical"
    assert calls["artifact_id"] == artifact_ref


# ===========================================================================
# R6-IMP3 rework TEETH — query_global must fall back to the linear scan when the
# global HNSW page is incomplete for the target board (so a board's `working`
# digest is NOT lost under graph_layer=all). This test FAILS if the early-return
# condition reverts to `if filtered_hnsw:` (trust HNSW, skip fallback).
# ===========================================================================


class _FakeResult:
    """Minimal Kùzu QueryResult double."""

    def __init__(self, rows):
        self._rows = list(rows)

    def has_next(self):
        return bool(self._rows)

    def get_next(self):
        return self._rows.pop(0)

    def close(self):
        pass


def test_query_global_all_recovers_working_via_linear_fallback(monkeypatch):
    from okto_pulse.core.kg.embedding import get_embedding_provider
    from okto_pulse.core.kg import global_discovery as _gd  # noqa: F401
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.kg_service import get_kg_service
    from okto_pulse.core.kg.schema import bootstrap_board_graph, open_board_connection

    qtext = "teeth fallback query"
    board_id = f"teeth-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    emb = get_embedding_provider().encode(qtext)
    # Real board source nodes so _filter_global_results_to_existing_nodes (NOT
    # stubbed — kept intact) confirms BOTH digests' sources exist.
    with open_board_connection(board_id) as (_db, conn):
        for nid, layer in (("canon_src", "canonical"), ("work_src", "working")):
            conn.execute(
                "CREATE (n:Decision {id:$id, title:$t, graph_layer:$l, "
                "source_confidence:1.0})",
                {"id": nid, "t": f"{layer} src", "l": layer},
            )

    # HNSW returns an INCOMPLETE page for the board: only the canonical digest
    # (1 row < top_k) — simulating the global top-k crowded by other boards.
    canon_hnsw = [board_id, "dd_canon", "canon_src", "canonical digest",
                  qtext, "Decision", "canonical", 0.0]
    # The board+layer-scoped LINEAR fallback returns BOTH layers (complete).
    canon_lin = [board_id, "dd_canon", "canon_src", "canonical digest",
                 qtext, "Decision", "canonical", emb]
    work_lin = [board_id, "dd_work", "work_src", "working digest",
                qtext, "Decision", "working", emb]

    class _FakeGlobalConn:
        def execute(self, cypher, params=None):
            if "QUERY_VECTOR_INDEX" in cypher:        # HNSW page (incomplete)
                return _FakeResult([canon_hnsw])
            if "DecisionDigest" in cypher:            # linear fallback (complete)
                return _FakeResult([canon_lin, work_lin])
            return _FakeResult([])

        def close(self):
            pass

    runtime = get_kg_registry().global_discovery_runtime
    monkeypatch.setattr(runtime, "open_connection",
                        lambda: (object(), _FakeGlobalConn()))
    monkeypatch.setattr(runtime, "ensure_layer_schema", lambda: [])

    rows = get_kg_service().query_global(
        qtext, user_boards=[board_id], graph_layer="all", top_k=10, min_similarity=0.1,
    )
    layers = {r["graph_layer"] for r in rows}
    # TEETH: the board's `working` digest is recovered via the linear fallback even
    # though the HNSW page returned only canonical. Under `if filtered_hnsw:` the
    # function would early-return [canonical] and this assertion would FAIL.
    assert {"canonical", "working"} <= layers, rows
    ids = {r["id"] for r in rows}
    assert {"canon_src", "work_src"} <= ids
