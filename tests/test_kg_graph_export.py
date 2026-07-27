"""MKG-E C4/C5 — JSON-LD export (scenarios S4-S7).

Structure + PROV-O vocabulary, byte determinism, safe failure (no partial
output, read-only), stable pagination and the surface contracts.
"""

from __future__ import annotations

import gc
import json
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.graph_export import (
    GraphExportError,
    export_board_jsonld,
)

from kg_registry_testing import configure_real_graph_test_kg_registry
from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def export_board(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_export_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    _seed(board_id)
    yield board_id
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _seed(board_id: str) -> None:
    with open_board_connection(board_id) as (_kdb, kconn):
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_a', title: 'Alfa', content: 'ca',"
            " source_confidence: 0.9, graph_layer: 'canonical',"
            " source_artifact_ref: 'spec:alpha', kind_of: 'security_control',"
            " source_session_id: 'ses1', created_by_agent: 'agent-x',"
            " created_at: timestamp('2026-01-01T00:00:00')})"
        )
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_old', title: 'Velho',"
            " source_confidence: 0.9, graph_layer: 'canonical',"
            " source_artifact_ref: 'spec:beta', superseded_by: 'entity_b',"
            " created_at: timestamp('2026-01-02T00:00:00')})"
        )
        kconn.execute(
            "CREATE (n:Entity {id: 'entity_b', title: 'Beta',"
            " source_confidence: 0.9, graph_layer: 'canonical',"
            " source_artifact_ref: 'spec:beta',"
            " created_at: timestamp('2026-01-03T00:00:00')})"
        )
        kconn.execute(
            "MATCH (a:Entity {id: 'entity_a'}), (b:Entity {id: 'entity_b'}) "
            "CREATE (a)-[r:belongs_to {confidence: 0.8, layer: 'cognitive',"
            " created_by_session_id: 's',"
            " created_at: timestamp('2026-01-04T00:00:00'), rule_id: '',"
            " created_by: 's', fallback_reason: ''}]->(b)"
        )


def _entries(document, type_key):
    return [e for e in document["@graph"] if e.get("@type") == type_key]


def test_s4_jsonld_structure_and_vocabulary(export_board):
    document = export_board_jsonld(export_board)

    assert document["@context"] == {
        "pulse": "https://oktolabs.ai/pulse/kg#",
        "prov": "http://www.w3.org/ns/prov#",
    }
    assert document["pulse:board"] == export_board
    nodes = {e["@id"]: e for e in _entries(document, "prov:Entity")}
    assert document["nodes_exported"] == 3

    alfa = nodes["pulse:entity_a"]
    assert alfa["pulse:nodeType"] == "Entity"
    assert alfa["pulse:kindOf"] == "security_control"
    assert alfa["prov:wasDerivedFrom"] == "spec:alpha"
    assert alfa["prov:wasGeneratedBy"] == "pulse:session/ses1"
    assert alfa["prov:wasAttributedTo"] == "pulse:agent/agent-x"

    old = nodes["pulse:entity_old"]
    assert old["pulse:supersededBy"] == "pulse:entity_b"
    successor = nodes["pulse:entity_b"]
    assert successor["prov:wasRevisionOf"] == "pulse:entity_old"

    edges = _entries(document, "pulse:Edge")
    assert {
        (e["pulse:relType"], e["pulse:from"], e["pulse:to"]) for e in edges
    } >= {("belongs_to", "pulse:entity_a", "pulse:entity_b")}
    assert document["edges_exported"] == len(edges)


def test_s5_export_is_byte_deterministic(export_board):
    a = json.dumps(export_board_jsonld(export_board), sort_keys=True, default=str)
    b = json.dumps(export_board_jsonld(export_board), sort_keys=True, default=str)
    assert a == b


def test_s6_unreadable_graph_fails_structured_no_partial(export_board, monkeypatch):
    import okto_pulse.core.kg.graph_export as ge

    def _boom(*args, **kwargs):
        raise RuntimeError("graph unreadable (injected)")

    monkeypatch.setattr(ge, "_fetch_nodes", _boom)
    with pytest.raises(GraphExportError) as excinfo:
        export_board_jsonld(export_board)
    assert excinfo.value.code == "kg_export_failed"
    assert "injected" in excinfo.value.reason


def test_s6_export_is_read_only(export_board):
    def _snapshot():
        with open_board_connection(export_board) as (_kdb, kconn):
            res = kconn.execute(
                "MATCH (n:Entity) RETURN n.id, n.title, n.kind_of, "
                "n.superseded_by ORDER BY n.id"
            )
            rows = []
            while res.has_next():
                rows.append(tuple(res.get_next()))
            res.close()
            return rows

    before = _snapshot()
    export_board_jsonld(export_board)
    assert _snapshot() == before


def test_s7_pagination_is_stable_and_reconstructs_full_export(export_board):
    full = export_board_jsonld(export_board)
    full_node_ids = {e["@id"] for e in _entries(full, "prov:Entity")}

    page1 = export_board_jsonld(export_board, page_size=2)
    assert page1["last_page"] is False
    assert page1["next_cursor"]
    # Stability: the same call yields the same page.
    page1_again = export_board_jsonld(export_board, page_size=2)
    assert [e["@id"] for e in _entries(page1, "prov:Entity")] == [
        e["@id"] for e in _entries(page1_again, "prov:Entity")
    ]

    page2 = export_board_jsonld(
        export_board, cursor=page1["next_cursor"], page_size=2
    )
    assert page2["last_page"] is True

    paged_ids = {e["@id"] for e in _entries(page1, "prov:Entity")} | {
        e["@id"] for e in _entries(page2, "prov:Entity")
    }
    assert paged_ids == full_node_ids

    # Edges travel with the page of their FROM endpoint — concatenation
    # reconstructs the full edge set.
    full_edges = {
        (e["pulse:relType"], e["pulse:from"], e["pulse:to"])
        for e in _entries(full, "pulse:Edge")
    }
    paged_edges = {
        (e["pulse:relType"], e["pulse:from"], e["pulse:to"])
        for e in _entries(page1, "pulse:Edge")
    } | {
        (e["pulse:relType"], e["pulse:from"], e["pulse:to"])
        for e in _entries(page2, "pulse:Edge")
    }
    assert paged_edges == full_edges


def test_s7_surfaces_contract():
    import asyncio as _asyncio
    import importlib
    import inspect

    # MCP tool registered, within the R1.1 budget.
    mod = importlib.import_module("okto_pulse.core.mcp.server")
    tools = _asyncio.run(mod.mcp.get_tools())
    tool = tools.get("okto_pulse_kg_export_jsonld")
    assert tool is not None
    assert len(tool.description or "") <= 900

    # CLI export exists offline with the single-writer guard (D5/R7).
    from okto_pulse.community import cli as community_cli

    src = inspect.getsource(community_cli.cmd_kg_export)
    assert "_fail_fast_if_server_running" in src
    assert "os.replace" in src  # atomic tmp+rename write (BR4)

    # D7: REST deliberately absent — no export route in the REST module.
    from okto_pulse.community.api import kg_routes

    routes_src = inspect.getsource(kg_routes)
    assert "export_jsonld" not in routes_src
