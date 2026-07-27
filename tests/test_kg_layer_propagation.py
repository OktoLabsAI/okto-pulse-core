"""Behavioral tests for graph_layer propagation + working non-leakage (spec
849d6292, Batch 2 — impl card 667362af; test cards 3a387a14/ts_5f959349 AC5 and
d830bae4/ts_bafdaa59 AC6).

HARD requirement (validator + codex): prove the default scope (canonical) NEVER
leaks working nodes, across every read surface — `/subgraph?center`,
`get_related_context`, and `query_global` — and that `working`/`all` widen the
scope as requested. Every test runs the REAL board graph and/or REAL global
discovery graph (no source inspection — TR7).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import uuid
from dataclasses import dataclass
from typing import Any, Callable

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_layer_"))

import okto_pulse.core.mcp.kg_query_tools as qt
from okto_pulse.community.api.kg_routes import get_subgraph
from okto_pulse.core.kg.embedding import get_embedding_provider
from global_graph_testing import (
    bootstrap_global_discovery,
    execute_global_write,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.kg_service import get_kg_service
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.mcp.kg_query_tools import register_kg_query_tools


@pytest.fixture(scope="module", autouse=True)
def _bootstrap_global():
    reset_global_discovery_runtime_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_discovery_runtime_for_tests()


# ---------------------------------------------------------------------------
# Board-graph seeding (subgraph / get_related_context)
# ---------------------------------------------------------------------------

CENTER_REF = "spec:center"


def _seed_board_subgraph() -> str:
    """center (canonical) with one canonical + one working neighbor."""
    board_id = f"layerprop-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)

    def mk(conn, nid, ref, layer, title):
        conn.execute(
            "CREATE (n:Decision {id:$id, title:$t, source_artifact_ref:$r, "
            "source_confidence:1.0, graph_layer:$l})",
            {"id": nid, "t": title, "r": ref, "l": layer},
        )

    with open_board_connection(board_id) as (_db, conn):
        mk(conn, "center", CENTER_REF, "canonical", "CENTER")
        mk(conn, "hop_canon", "x:canon", "canonical", "CANON_NEIGHBOR")
        mk(conn, "hop_work", "x:work", "working", "WORKING_NEIGHBOR")
        for a, b in (("center", "hop_canon"), ("center", "hop_work")):
            conn.execute(
                "MATCH (a:Decision {id:$a}),(b:Decision {id:$b}) "
                "CREATE (a)-[:supersedes {confidence:1.0}]->(b)",
                {"a": a, "b": b},
            )
    return board_id


async def _subgraph(board_id, **kw):
    """Call the route handler directly, filling the params that FastAPI would
    otherwise inject as ``Query(...)`` default objects."""
    params = {"depth": 2, "limit": 100, "cursor": "", "min_relevance": 0.0,
              "type": ""}
    params.update(kw)
    return await get_subgraph(board_id, **params)


def _neighbor_titles(rows: list[dict]) -> set[str]:
    titles: set[str] = set()
    for r in rows:
        for key in ("hop1_title", "hop2_title"):
            if r.get(key):
                titles.add(r[key])
    return titles


# ---------------------------------------------------------------------------
# Global-graph seeding (query_global)
# ---------------------------------------------------------------------------

QUERY_TEXT = "caching strategy for the gateway"


def _seed_global_digests() -> str:
    """A Board with one CANONICAL + one WORKING DecisionDigest, both embedded on
    the same text so a vector query matches both — the layer filter is the only
    thing that can keep working out of a canonical query."""
    board_id = f"glob-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    emb = get_embedding_provider().encode(QUERY_TEXT)
    ts = "2026-06-15T00:00:00"
    with open_board_connection(board_id) as (_db, conn):
        for nid, layer in (("dd_canon", "canonical"), ("dd_work", "working")):
            conn.execute(
                "CREATE (n:Decision {id:$id, title:$title, "
                "source_confidence:1.0, graph_layer:$layer, embedding:$emb})",
                {
                    "id": nid,
                    "title": f"{layer} digest",
                    "layer": layer,
                    "emb": emb,
                },
            )
    execute_global_write(
        "CREATE (b:Board {board_id:$bid, name:$bid, summary:'', "
        "summary_embedding:$emb, topic_count:0, entity_count:0, "
        "decision_count:3, last_sync_at:timestamp($ts)})",
        {"bid": board_id, "emb": emb, "ts": ts},
        operation="test_layer_propagation_seed_board",
    )
    for did, layer in (
        ("dd_canon", "canonical"),
        ("dd_work", "working"),
        ("dd_stale", "canonical"),
    ):
        execute_global_write(
            "CREATE (d:DecisionDigest {id:$did, board_id:$bid, "
            "original_node_id:$oid, title:$title, one_line_summary:$s, "
            "node_type:'Decision', graph_layer:$layer, embedding:$emb, "
            "created_at:timestamp($ts)})",
            {
                "did": f"{did}_{board_id[:8]}",
                "bid": board_id,
                "oid": did,
                "title": f"{layer} digest",
                "s": QUERY_TEXT,
                "layer": layer,
                "emb": emb,
                "ts": ts,
            },
            operation="test_layer_propagation_seed_digest",
        )
        execute_global_write(
            "MATCH (b:Board {board_id:$bid}), "
            "(d:DecisionDigest {id:$did}) "
            "MERGE (b)-[:CONTAINS_DECISION]->(d)",
            {"bid": board_id, "did": f"{did}_{board_id[:8]}"},
            operation="test_layer_propagation_link_digest",
        )
    return board_id


def _global_layers(rows: list[dict]) -> set[str]:
    return {r.get("graph_layer") for r in rows}


# ===========================================================================
# AC5 (card 3a387a14 / ts_5f959349) — /subgraph?center respects graph_layer
# ===========================================================================


@pytest.mark.asyncio
async def test_subgraph_centered_default_and_canonical_excludes_working():
    board_id = _seed_board_subgraph()

    for layer in (None, "canonical"):
        kwargs = {"center": CENTER_REF}
        if layer is not None:
            kwargs["graph_layer"] = layer
        resp = await _subgraph(board_id, **kwargs)
        titles = _neighbor_titles(resp["nodes"])
        assert "CANON_NEIGHBOR" in titles
        assert "WORKING_NEIGHBOR" not in titles, (
            f"working node leaked into a {layer or 'default'} centered subgraph"
        )
        # AC5: the response reports the applied layer.
        assert resp["metadata"]["graph_layer"] == "canonical"


@pytest.mark.asyncio
async def test_subgraph_centered_working_returns_only_working():
    board_id = _seed_board_subgraph()
    resp = await _subgraph(board_id, center=CENTER_REF, graph_layer="working")
    titles = _neighbor_titles(resp["nodes"])
    assert titles == {"WORKING_NEIGHBOR"}
    assert resp["metadata"]["graph_layer"] == "working"


@pytest.mark.asyncio
async def test_subgraph_centered_all_returns_both():
    board_id = _seed_board_subgraph()
    resp = await _subgraph(board_id, center=CENTER_REF, graph_layer="all")
    titles = _neighbor_titles(resp["nodes"])
    assert {"CANON_NEIGHBOR", "WORKING_NEIGHBOR"} <= titles
    assert resp["metadata"]["graph_layer"] == "all"


WORKING_CENTER_REF = "spec:wcenter"


def _seed_board_working_center() -> str:
    """A WORKING anchor node with a canonical + a working neighbor — exercises
    the validator-mandated case center=<working>&graph_layer=canonical."""
    board_id = f"wcenter-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)

    def mk(conn, nid, ref, layer, title):
        conn.execute(
            "CREATE (n:Decision {id:$id, title:$t, source_artifact_ref:$r, "
            "source_confidence:1.0, graph_layer:$l})",
            {"id": nid, "t": title, "r": ref, "l": layer},
        )

    with open_board_connection(board_id) as (_db, conn):
        mk(conn, "wcenter", WORKING_CENTER_REF, "working", "WORKING_CENTER")
        mk(conn, "hop_canon", "x:canon", "canonical", "CANON_NEIGHBOR")
        mk(conn, "hop_work", "x:work", "working", "WORKING_NEIGHBOR")
        for a, b in (("wcenter", "hop_canon"), ("wcenter", "hop_work")):
            conn.execute(
                "MATCH (a:Decision {id:$a}),(b:Decision {id:$b}) "
                "CREATE (a)-[:supersedes {confidence:1.0}]->(b)",
                {"a": a, "b": b},
            )
    return board_id


@pytest.mark.asyncio
async def test_subgraph_centered_on_working_anchor_keeps_anchor_canonical_expansion():
    # Validator AC5 follow-up: an explicitly-requested anchor is exempt from the
    # layer filter (it was selected by id, not discovered), but the EXPANSION
    # must still be canonical-only — the working neighbor must not leak.
    board_id = _seed_board_working_center()
    resp = await _subgraph(
        board_id, center=WORKING_CENTER_REF, graph_layer="canonical"
    )
    rows = resp["nodes"]

    # The working anchor IS returned (as the center of every neighborhood row).
    assert {r.get("center_title") for r in rows} == {"WORKING_CENTER"}
    # The expansion is canonical-only — working neighbor excluded.
    titles = _neighbor_titles(rows)
    assert "CANON_NEIGHBOR" in titles
    assert "WORKING_NEIGHBOR" not in titles, (
        "working neighbor leaked into the canonical expansion of a working anchor"
    )
    assert resp["metadata"]["graph_layer"] == "canonical"


# ===========================================================================
# AC6 (card d830bae4 / ts_bafdaa59) — get_related_context + query_global layers
# ===========================================================================


def test_get_related_context_default_canonical_does_not_leak_working():
    board_id = _seed_board_subgraph()
    svc = get_kg_service()

    default_rows = svc.get_related_context(board_id, CENTER_REF)
    assert "CANON_NEIGHBOR" in _neighbor_titles(default_rows)
    assert "WORKING_NEIGHBOR" not in _neighbor_titles(default_rows)

    working_rows = svc.get_related_context(
        board_id, CENTER_REF, graph_layer="working"
    )
    assert _neighbor_titles(working_rows) == {"WORKING_NEIGHBOR"}

    all_rows = svc.get_related_context(board_id, CENTER_REF, graph_layer="all")
    assert {"CANON_NEIGHBOR", "WORKING_NEIGHBOR"} <= _neighbor_titles(all_rows)


def test_query_global_default_and_canonical_no_working_leak():
    board_id = _seed_global_digests()
    svc = get_kg_service()

    for layer in (None, "canonical"):
        kwargs = {"user_boards": [board_id], "min_similarity": 0.1}
        if layer is not None:
            kwargs["graph_layer"] = layer
        rows = svc.query_global(QUERY_TEXT, **kwargs)
        layers = _global_layers(rows)
        assert "canonical" in layers, f"canonical digest missing for {layer}"
        assert "working" not in layers, (
            f"working digest leaked into a {layer or 'default'} query_global"
        )


def test_query_global_working_only_and_all_both():
    board_id = _seed_global_digests()
    svc = get_kg_service()

    working = svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="working",
        min_similarity=0.1,
    )
    assert _global_layers(working) == {"working"}

    both = svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="all",
        min_similarity=0.1,
    )
    assert {"canonical", "working"} <= _global_layers(both)


def test_query_global_filters_stale_digest_ids():
    board_id = _seed_global_digests()
    svc = get_kg_service()

    rows = svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="all",
        min_similarity=0.1,
    )

    assert {r["id"] for r in rows} == {"dd_canon", "dd_work"}


# ---------------------------------------------------------------------------
# AC6 — the MCP surface (default canonical, accepts working/all)
# ---------------------------------------------------------------------------


@dataclass
class _FakeAgent:
    id: str = "agent-layer-test"


class _MCPDouble:
    def __init__(self) -> None:
        self.tools: dict[str, Callable[..., Any]] = {}

    def tool(self):
        def _decorator(fn):
            self.tools[fn.__name__] = fn
            return fn
        return _decorator


def _register_query_tools(board_id, monkeypatch):
    """Register the KG query MCP tools, stubbing only auth so the tool runs the
    REAL service against ``board_id`` (the logic under test is untouched)."""
    async def _fake_user_boards(*_a, **_k):
        return _FakeAgent(), [board_id]

    async def _board_agent(_board_id: str):
        return _FakeAgent()

    async def _global_agent():
        return _FakeAgent()

    monkeypatch.setattr(qt, "_get_user_boards", _fake_user_boards)
    double = _MCPDouble()
    register_kg_query_tools(
        double,
        get_agent=lambda: None,
        get_uow=lambda: None,
        get_board_agent=_board_agent,
        get_global_agent=_global_agent,
    )
    return double.tools


def _call(tool, **kwargs) -> dict:
    return json.loads(asyncio.run(tool(**kwargs)))


def test_mcp_query_global_default_canonical_no_leak(monkeypatch):
    board_id = _seed_global_digests()
    tools = _register_query_tools(board_id, monkeypatch)
    tool = tools["okto_pulse_kg_query_global"]

    # Default (no graph_layer) — must be canonical-only.
    default = _call(tool, board_id=board_id, nl_query=QUERY_TEXT)
    layers = {r.get("graph_layer") for r in default["results"]}
    assert "canonical" in layers
    assert "working" not in layers

    # Explicit working / all widen the scope.
    working = _call(
        tool, board_id=board_id, nl_query=QUERY_TEXT, graph_layer="working"
    )
    assert {r.get("graph_layer") for r in working["results"]} == {"working"}

    both = _call(
        tool, board_id=board_id, nl_query=QUERY_TEXT, graph_layer="all"
    )
    assert {"canonical", "working"} <= {
        r.get("graph_layer") for r in both["results"]
    }


def test_mcp_get_related_context_default_canonical_no_leak(monkeypatch):
    board_id = _seed_board_subgraph()
    tools = _register_query_tools(board_id, monkeypatch)
    tool = tools["okto_pulse_kg_get_related_context"]

    default = _call(tool, board_id=board_id, artifact_id=CENTER_REF)
    titles = {
        h.get("hop1_title") for h in default["context"]
    } | {h.get("hop2_title") for h in default["context"]}
    assert "CANON_NEIGHBOR" in titles
    assert "WORKING_NEIGHBOR" not in titles

    working = _call(
        tool, board_id=board_id, artifact_id=CENTER_REF, graph_layer="working"
    )
    wtitles = {h.get("hop1_title") for h in working["context"]}
    assert "WORKING_NEIGHBOR" in wtitles
    assert "CANON_NEIGHBOR" not in wtitles


def test_mcp_invalid_graph_layer_returns_structured_error(monkeypatch):
    board_id = _seed_board_subgraph()
    tools = _register_query_tools(board_id, monkeypatch)
    tool = tools["okto_pulse_kg_get_related_context"]

    out = _call(tool, board_id=board_id, artifact_id=CENTER_REF,
                graph_layer="bogus")
    # TR5: invalid layer → structured error, not a silent default or crash.
    assert "error" in out or out.get("code") in ("invalid_param", "invalid_argument")
