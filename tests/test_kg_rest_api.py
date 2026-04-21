"""Tests for REST API /api/kg/ endpoints."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.api.kg_routes import (
    ProblemDetail,
    decode_cursor,
    encode_cursor,
    router,
)


class TestEndpointRegistration:
    def test_endpoint_count(self):
        routes = [r for r in router.routes if hasattr(r, "methods")]
        assert len(routes) >= 21

    def test_all_under_kg_prefix(self):
        routes = [r for r in router.routes if hasattr(r, "path")]
        for r in routes:
            assert r.path.startswith("/kg"), f"Route {r.path} not under /kg"


class TestCursorPagination:
    """Cursor codec contract — updated per Spec 8 / S1.2 to use an
    ``iso;id`` tuple-encoded payload that raises ``ValueError`` on any
    corruption (so route handlers translate to 410 Gone)."""

    def test_encode_decode_roundtrip(self):
        cursor = encode_cursor("2026-04-15T10:00:00", "id-123")
        ts, node_id = decode_cursor(cursor)
        assert ts == "2026-04-15T10:00:00"
        assert node_id == "id-123"

    def test_decode_invalid(self):
        with pytest.raises(ValueError):
            decode_cursor("invalid-base64!!!")

    def test_decode_empty(self):
        with pytest.raises(ValueError):
            decode_cursor("")


class TestProblemDetail:
    def test_rfc7807_model(self):
        p = ProblemDetail(
            type="/errors/not-found",
            title="Not Found",
            status=404,
            detail="Board not found",
        )
        d = p.model_dump()
        assert d["type"] == "/errors/not-found"
        assert d["status"] == 404
        assert d["title"] == "Not Found"


class TestStreamingExport:
    def test_export_endpoint_exists(self):
        paths = [r.path for r in router.routes if hasattr(r, "path")]
        assert "/kg/boards/{board_id}/audit/export" in paths


class TestCypherDelegate:
    def test_cypher_endpoint_exists(self):
        paths = [r.path for r in router.routes if hasattr(r, "path")]
        assert "/kg/boards/{board_id}/cypher" in paths


class TestSchemaEndpoint:
    def test_schema_endpoint_exists(self):
        paths = [r.path for r in router.routes if hasattr(r, "path")]
        assert "/kg/schema" in paths


class TestDeleteKG:
    def test_delete_endpoint_exists(self):
        routes = [(r.path, r.methods) for r in router.routes if hasattr(r, "methods")]
        delete_routes = [(p, m) for p, m in routes if "DELETE" in m]
        assert len(delete_routes) >= 1
        assert any("/kg" in p for p, m in delete_routes)


class TestHistoricalEndpoints:
    def test_start_cancel_progress_exist(self):
        paths = [r.path for r in router.routes if hasattr(r, "path")]
        assert "/kg/boards/{board_id}/historical-consolidation/start" in paths
        assert "/kg/boards/{board_id}/historical-consolidation/cancel" in paths
        assert "/kg/boards/{board_id}/historical-consolidation/progress" in paths


class TestNewEndpoints:
    def test_similar_endpoint_exists(self):
        paths = [r.path for r in router.routes if hasattr(r, "path")]
        assert "/kg/boards/{board_id}/similar" in paths

    def test_supersedence_endpoint_exists(self):
        paths = [r.path for r in router.routes if hasattr(r, "path")]
        assert "/kg/boards/{board_id}/supersedence/{decision_id}" in paths

    def test_contradictions_endpoint_exists(self):
        paths = [r.path for r in router.routes if hasattr(r, "path")]
        assert "/kg/boards/{board_id}/contradictions" in paths
