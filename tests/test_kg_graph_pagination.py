"""Integration tests for GET /kg/boards/{board_id}/graph pagination.

Covers Spec 8 cards S1.6 and S1.7:

- S1.6 / AC-10: limit=50 returns at most 50 nodes + next_cursor.
- S1.6 / AC-11: limit>1000, limit<1 return 400 Bad Request.
- S1.7 / AC-12: identical cursor returns identical page (stability).
- S1.7 / AC-12: corrupted cursor returns 410 Gone.
- S1.7 / AC-12: walking all pages visits every seeded node exactly once.

Strategy: mock :func:`get_kg_service` so the route logic is exercised without
needing a live Kùzu process. The route contract — param validation, cursor
handling, response shape — is what these tests verify; the Kùzu query itself
is covered by ``test_kg_tier_primary.py`` and the e2e suite.
"""

from __future__ import annotations

import os
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.api import kg_routes
from okto_pulse.core.api.kg_routes import router as kg_router
from okto_pulse.core.application.use_cases import ActorContext


SEED_BOARD = "board-s8-pagination"
SEED_COUNT = 120


def _seed_rows(count: int = SEED_COUNT) -> list[dict]:
    """Build an ordered list of node dicts shaped like ``get_all_nodes`` output.

    Ordering: created_at DESC, id DESC — matches what S1.3 will produce.
    A handful share the same created_at to exercise the id tiebreak.
    """
    rows = []
    for i in range(count):
        minute = (i // 5) % 60
        second = (i % 5) * 10
        ts = f"2026-04-15T10:{minute:02d}:{second:02d}"
        rows.append({
            "id": f"node-{i:03d}",
            "node_type": "Decision",
            "title": f"Decision {i}",
            "content": f"Seeded node {i}",
            "created_at": ts,
            "source_confidence": 0.85,
            "relevance_score": 0.8,
            "source_artifact_ref": "spec-s8",
            "graph_layer": "canonical",
        })
    # Sort by created_at DESC, id DESC — the stable order the endpoint must honour.
    rows.sort(key=lambda r: (r["created_at"], r["id"]), reverse=True)
    return rows


class _FakeKGService:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def get_all_nodes(
        self,
        board_id: str,
        *,
        min_confidence: float = 0.0,
        min_relevance: float | None = None,
        max_rows: int | None = None,
        cursor: str | None = None,
        node_type: str | None = None,
        graph_layer: str = "canonical",
    ) -> list[dict]:
        # Stable order (created_at DESC, id DESC). AC-12 requires determinism.
        # The real KGService applies the ORDER BY in Cypher; we replicate it
        # here so pagination slicing is identical to production behaviour.
        ordered = sorted(
            self._rows,
            key=lambda r: (r["created_at"], r["id"]),
            reverse=True,
        )
        if node_type:
            ordered = [r for r in ordered if r.get("node_type") == node_type]
        if graph_layer != "all":
            ordered = [
                r for r in ordered
                if r.get("graph_layer", "canonical") == graph_layer
            ]
        if cursor:
            # Mimic the production decode_cursor contract. The helper lives
            # in kg_routes and raises ValueError on invalid input.
            ts, node_id = kg_routes.decode_cursor(cursor)
            ordered = [
                r for r in ordered
                if (r["created_at"], r["id"]) < (ts, node_id)
            ]
        if max_rows:
            ordered = ordered[:max_rows]
        return ordered

    def count_all_nodes(
        self,
        board_id: str,
        *,
        min_confidence: float = 0.0,
        min_relevance: float | None = None,
        node_type: str | None = None,
        graph_layer: str = "canonical",
    ) -> int:
        rows = self._rows
        if node_type:
            rows = [r for r in rows if r.get("node_type") == node_type]
        if graph_layer != "all":
            rows = [
                r for r in rows
                if r.get("graph_layer", "canonical") == graph_layer
            ]
        return len(rows)

    def get_related_context(self, *_, **__):
        return []

    def query_global(self, *_, **__):
        raise AssertionError("board graph routes must not use global discovery fallback")

    def get_schema_version(self, *_):
        return "0.3.5"


@pytest.fixture
def client(monkeypatch):
    fake = _FakeKGService(_seed_rows())
    monkeypatch.setattr(kg_routes, "get_kg_service", lambda: fake)
    monkeypatch.setattr(
        kg_routes,
        "_fetch_edges_for_nodes",
        lambda _board, _ids: (
            [],
            {
                "edge_read_status": "ok",
                "edge_tables_scanned": 0,
                "edge_tables_failed": 0,
                "edge_errors": [],
                "edges_returned": 0,
            },
        ),
    )
    monkeypatch.setattr(
        kg_routes,
        "_count_edges_by_type",
        lambda _board: (
            {"belongs_to": 3},
            {
                "edge_count_status": "ok",
                "edge_count_tables_scanned": 1,
                "edge_count_tables_failed": 0,
                "edge_count_errors": [],
            },
        ),
    )
    app = FastAPI()
    app.include_router(kg_router, prefix="/api/v1")
    async def _override_actor():
        return ActorContext("test-user", "rest", board_id=SEED_BOARD, roles=("admin",))

    app.dependency_overrides[kg_routes.require_kg_actor] = _override_actor
    app.dependency_overrides[kg_routes.require_kg_board_actor] = _override_actor
    return TestClient(app)


def _get_graph(client: TestClient, **params) -> tuple[int, dict]:
    resp = client.get(f"/api/v1/kg/boards/{SEED_BOARD}/graph", params=params)
    try:
        body = resp.json()
    except Exception:
        body = {}
    return resp.status_code, body


def _get_nodes(client: TestClient, **params) -> tuple[int, dict]:
    resp = client.get(f"/api/v1/kg/boards/{SEED_BOARD}/nodes", params=params)
    try:
        body = resp.json()
    except Exception:
        body = {}
    return resp.status_code, body


# ---------------------------------------------------------------------------
# S1.6 — AC-10: limit query param honored, next_cursor returned
# ---------------------------------------------------------------------------


class TestLimitContract:
    def test_limit_50_returns_at_most_50_nodes_plus_cursor(self, client):
        code, body = _get_graph(client, limit=50)
        assert code == 200, body
        assert "nodes" in body and "edges" in body
        assert len(body["nodes"]) <= 50
        assert len(body["nodes"]) == 50, (
            "Seed has >50 nodes so limit=50 must return exactly 50"
        )
        assert body.get("next_cursor"), "next_cursor must be non-empty mid-pagination"

    def test_limit_1000_hard_cap_accepted(self, client):
        code, _body = _get_graph(client, limit=1000)
        assert code == 200

    def test_limit_default_is_100_when_omitted(self, client):
        code, body = _get_graph(client)
        assert code == 200, body
        assert len(body["nodes"]) == 100
        assert body.get("next_cursor")

    def test_nodes_shape_matches_contract(self, client):
        code, body = _get_graph(client, limit=5)
        assert code == 200
        for n in body["nodes"]:
            assert {"id", "node_type", "title", "created_at"}.issubset(n.keys())

    def test_type_filter_is_applied_to_graph_page(self, client):
        code, body = _get_graph(client, limit=50, type="Bug")
        assert code == 200, body
        assert body["nodes"] == []
        assert body["edges"] == []
        assert body.get("next_cursor") in (None, "")


# ---------------------------------------------------------------------------
# S1.6 — AC-11: invalid limit values return 400
# ---------------------------------------------------------------------------


class TestLimitValidation:
    def test_limit_above_1000_rejected_with_400(self, client):
        code, body = _get_graph(client, limit=1001)
        # FastAPI Query(..., le=1000) yields 422 by default; the spec wants a
        # 400 with a specific detail. S1.1 must translate the validation error.
        assert code == 400, (code, body)
        detail = (body.get("detail") or "").lower() if isinstance(body, dict) else ""
        assert "limit" in detail and ("1000" in detail or "range" in detail), body

    def test_limit_zero_rejected_with_400(self, client):
        code, _body = _get_graph(client, limit=0)
        assert code == 400

    def test_limit_negative_rejected_with_400(self, client):
        code, _body = _get_graph(client, limit=-1)
        assert code == 400

    def test_limit_non_numeric_rejected_with_422(self, client):
        # Type-invalid remains FastAPI's default 422 — per ts_750e356f.
        code, _body = _get_graph(client, limit="abc")
        assert code == 422


# ---------------------------------------------------------------------------
# S1.7 — AC-12: cursor stability + corrupted cursor + pagination coverage
# ---------------------------------------------------------------------------


class TestCursorStability:
    def test_same_cursor_returns_same_page_twice(self, client):
        _code1, page1 = _get_graph(client, limit=50)
        cursor = page1.get("next_cursor")
        assert cursor

        _code_a, resp_a = _get_graph(client, limit=50, cursor=cursor)
        _code_b, resp_b = _get_graph(client, limit=50, cursor=cursor)

        assert resp_a["nodes"] == resp_b["nodes"]
        assert resp_a["edges"] == resp_b["edges"]
        assert resp_a.get("next_cursor") == resp_b.get("next_cursor")

    def test_corrupted_cursor_returns_410_gone(self, client):
        code, body = _get_graph(client, limit=50, cursor="not-valid-base64!!!")
        assert code == 410, (code, body)

    def test_cursor_wrong_base64_contents_returns_410(self, client):
        import base64

        # Valid base64 but missing the semicolon separator — decode_cursor
        # must raise ValueError and the route must translate to 410.
        bad = base64.b64encode(b"no-separator").decode()
        code, _body = _get_graph(client, limit=50, cursor=bad)
        assert code == 410

    def test_walk_all_pages_yields_full_seed_without_duplicates(self, client):
        collected: list[str] = []
        cursor: str | None = None
        while True:
            params = {"limit": 50}
            if cursor:
                params["cursor"] = cursor
            code, body = _get_graph(client, **params)
            assert code == 200, body
            collected.extend(n["id"] for n in body["nodes"])
            cursor = body.get("next_cursor")
            if not cursor:
                break

        assert len(collected) == len(set(collected)), "pagination produced duplicates"
        assert len(collected) == SEED_COUNT


# ---------------------------------------------------------------------------
# S1.4 — Response shape always has next_cursor (null on last page)
# ---------------------------------------------------------------------------


class TestResponseShape:
    def test_last_page_has_null_next_cursor(self, client):
        # limit=1000 >= SEED_COUNT → single page → next_cursor must be None
        code, body = _get_graph(client, limit=1000)
        assert code == 200
        assert body.get("next_cursor") in (None, "")

    def test_response_always_has_nodes_edges_next_cursor(self, client):
        code, body = _get_graph(client, limit=10)
        assert code == 200
        assert {"nodes", "edges", "next_cursor"}.issubset(body.keys())

    def test_response_includes_edge_read_diagnostics(self, client):
        code, body = _get_graph(client, limit=10)
        assert code == 200
        metadata = body.get("metadata") or {}
        assert metadata.get("edge_read_status") == "ok"
        assert metadata.get("min_relevance") == 0.0

    def test_response_surfaces_edge_read_partial_failure(self, client, monkeypatch):
        monkeypatch.setattr(
            kg_routes,
            "_fetch_edges_for_nodes",
            lambda _board, _ids: (
                [],
                {
                    "edge_read_status": "partial_failure",
                    "edge_tables_scanned": 2,
                    "edge_tables_failed": 1,
                    "edge_errors": [{
                        "relationship": "belongs_to",
                        "error": "read failed",
                    }],
                    "edges_returned": 0,
                },
            ),
        )

        code, body = _get_graph(client, limit=10)

        assert code == 200
        metadata = body.get("metadata") or {}
        assert metadata["edge_read_status"] == "partial_failure"
        assert metadata["edge_tables_failed"] == 1
        assert metadata["edge_errors"][0]["relationship"] == "belongs_to"


class TestNodesAndStats:
    def test_nodes_endpoint_lists_all_nodes_not_decision_history(self, client):
        code, body = _get_nodes(client, limit=5)
        assert code == 200
        assert len(body["nodes"]) == 5
        assert body["nodes"][0]["id"].startswith("node-")
        assert body["page_count"] == 5
        assert body["total_hint"] == SEED_COUNT

    def test_nodes_total_hint_is_total_filtered_count_not_page_size(self, client):
        code, body = _get_nodes(client, limit=10)
        assert code == 200
        assert len(body["nodes"]) == 10
        assert body["page_count"] == 10
        assert body["total_hint"] == SEED_COUNT

        code, larger_page = _get_nodes(client, limit=50)
        assert code == 200
        assert len(larger_page["nodes"]) == 50
        assert larger_page["page_count"] == 50
        assert larger_page["total_hint"] == SEED_COUNT

    def test_stats_reports_graph_schema_and_edge_counts(self, client):
        resp = client.get(f"/api/v1/kg/boards/{SEED_BOARD}/stats")
        body = resp.json()
        assert resp.status_code == 200, body
        assert body["schema_version"] == "0.3.5"
        assert body["graph_schema_version"] == "0.3.5"
        assert body["node_counts_by_type"]["Decision"] == SEED_COUNT
        assert body["node_counts_by_type"]["Learning"] == 0
        assert body["edge_counts_by_type"] == {"belongs_to": 3}
        assert body["edge_count_status"] == "ok"

    def test_graph_layer_filters_graph_nodes_and_stats(self, client, monkeypatch):
        rows = _seed_rows(4)
        rows[0]["graph_layer"] = "canonical"
        rows[1]["graph_layer"] = "working"
        rows[2]["graph_layer"] = "working"
        rows[3]["graph_layer"] = "canonical"
        fake = _FakeKGService(rows)
        monkeypatch.setattr(kg_routes, "get_kg_service", lambda: fake)

        code, graph_body = _get_graph(client, limit=10, graph_layer="working")
        assert code == 200, graph_body
        assert graph_body["metadata"]["graph_layer"] == "working"
        assert {n["graph_layer"] for n in graph_body["nodes"]} == {"working"}
        assert len(graph_body["nodes"]) == 2

        nodes_code, nodes_body = _get_nodes(client, limit=10, graph_layer="working")
        assert nodes_code == 200, nodes_body
        assert nodes_body["graph_layer"] == "working"
        assert nodes_body["total_hint"] == 2

        stats_resp = client.get(
            f"/api/v1/kg/boards/{SEED_BOARD}/stats",
            params={"graph_layer": "working"},
        )
        stats_body = stats_resp.json()
        assert stats_resp.status_code == 200, stats_body
        assert stats_body["graph_layer"] == "working"
        assert stats_body["node_counts_by_type"]["Decision"] == 2

    def test_invalid_graph_layer_is_rejected(self, client):
        code, body = _get_graph(client, limit=10, graph_layer="draft")
        assert code == 400, body
        assert "graph_layer" in body["detail"]

    def test_stats_counts_learning_nodes_beyond_first_thousand(self, monkeypatch):
        many_decisions = _seed_rows(1005)
        learning = {
            "id": "learning-old-001",
            "node_type": "Learning",
            "title": "Late-page Learning",
            "content": "A learning node older than the first 1000 rows.",
            "created_at": "2020-01-01T00:00:00",
            "source_confidence": 0.9,
            "relevance_score": 0.75,
            "source_artifact_ref": "bug-1",
            "graph_layer": "canonical",
        }
        fake = _FakeKGService([*many_decisions, learning])
        monkeypatch.setattr(kg_routes, "get_kg_service", lambda: fake)
        monkeypatch.setattr(
            kg_routes,
            "_count_edges_by_type",
            lambda _board: (
                {},
                {
                    "edge_count_status": "ok",
                    "edge_count_tables_scanned": 0,
                    "edge_count_tables_failed": 0,
                    "edge_count_errors": [],
                },
            ),
        )
        app = FastAPI()
        app.include_router(kg_router, prefix="/api/v1")
        async def _override_actor():
            return ActorContext("test-user", "rest", board_id=SEED_BOARD, roles=("admin",))

        app.dependency_overrides[kg_routes.require_kg_actor] = _override_actor
        app.dependency_overrides[kg_routes.require_kg_board_actor] = _override_actor
        local_client = TestClient(app)

        resp = local_client.get(f"/api/v1/kg/boards/{SEED_BOARD}/stats")
        body = resp.json()

        assert resp.status_code == 200, body
        assert body["node_counts_by_type"]["Decision"] == 1005
        assert body["node_counts_by_type"]["Learning"] == 1

    def test_board_routes_do_not_use_global_discovery_fallback(self, client):
        graph_code, graph_body = _get_graph(client, limit=1)
        nodes_code, nodes_body = _get_nodes(client, limit=1)
        stats_resp = client.get(f"/api/v1/kg/boards/{SEED_BOARD}/stats")

        assert graph_code == 200, graph_body
        assert nodes_code == 200, nodes_body
        assert stats_resp.status_code == 200, stats_resp.json()


# ---------------------------------------------------------------------------
# S1.2 — Cursor encode/decode direct unit coverage
# ---------------------------------------------------------------------------


class TestCursorCodec:
    def test_encode_decode_roundtrip_tuple_format(self):
        encoded = kg_routes.encode_cursor("2026-04-15T10:05:00", "node-042")
        ts, node_id = kg_routes.decode_cursor(encoded)
        assert ts == "2026-04-15T10:05:00"
        assert node_id == "node-042"

    def test_decode_raises_on_invalid_base64(self):
        with pytest.raises(ValueError):
            kg_routes.decode_cursor("not-base64!!!")

    def test_decode_raises_on_missing_separator(self):
        import base64
        bad = base64.b64encode(b"no-separator").decode()
        with pytest.raises(ValueError):
            kg_routes.decode_cursor(bad)

    def test_decode_raises_on_empty_parts(self):
        import base64
        bad = base64.b64encode(b";").decode()
        with pytest.raises(ValueError):
            kg_routes.decode_cursor(bad)


class TestCrossPageEdges:
    """Investigação 2026-06-10: a projeção paginada exigia AMBAS as pontas
    da edge na mesma página — com 100 nós/página, quase toda edge cruzava
    páginas e era omitida, e a Graph view mostrava milhares de nós
    conectados como se fossem órfãos."""

    def test_fetch_edges_includes_edges_with_one_endpoint_in_page(self, monkeypatch):
        from okto_pulse.core.api import kg_routes
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        class _CypherExecutor:
            def execute_read_only(self, _board_id, query, _params=None, **_kwargs):
                if ":belongs_to" in query and "(a:Requirement)" in query:
                    return {
                        "rows": [
                            ("req-in-page", "entity-OUT-of-page", 0.9),
                            ("req-other-page", "entity-in-page", 0.8),
                            ("req-out", "entity-out", 0.7),
                        ]
                    }
                return {"rows": []}

        monkeypatch.setattr(get_kg_registry(), "cypher_executor", _CypherExecutor())
        monkeypatch.setattr(
            kg_routes,
            "_relation_pairs",
            lambda *_a, **_k: [("belongs_to", "Requirement", "Entity")],
        )

        edges, diagnostics = kg_routes._fetch_edges_for_nodes(
            "board-x", {"req-in-page", "entity-in-page"}
        )

        ids = {(e["source"], e["target"]) for e in edges}
        # uma ponta na página basta (as duas edges abaixo eram omitidas):
        assert ("req-in-page", "entity-OUT-of-page") in ids
        assert ("req-other-page", "entity-in-page") in ids
        # edges sem NENHUMA ponta na página continuam fora:
        assert ("req-out", "entity-out") not in ids
        assert diagnostics["edge_read_status"] == "ok"
