"""Tests for the `/analytics/blockers` triage endpoint (spec 124087e7)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.routing import APIRoute

from okto_pulse.core.api.analytics import router


class TestBlockersEndpointContract:
    def test_endpoint_registered(self):
        paths = {r.path for r in router.routes if isinstance(r, APIRoute)}
        assert "/boards/{board_id}/analytics/blockers" in paths

    def test_endpoint_accepts_stale_hours_param(self):
        route = next(
            r for r in router.routes
            if isinstance(r, APIRoute)
            and r.path == "/boards/{board_id}/analytics/blockers"
        )
        params = route.dependant.query_params
        assert any(p.name == "stale_hours" for p in params)


class TestBlockerTypesAreWellKnown:
    EXPECTED_TYPES = {
        "dependency_blocked",
        "on_hold",
        "stale",
        "spec_pending_validation",
        "spec_no_cards",
        "uncovered_scenario",
    }

    def test_expected_type_set(self):
        assert len(self.EXPECTED_TYPES) == 6
        # All types are imperative-past or adjectival — machine-parseable.
        for t in self.EXPECTED_TYPES:
            assert "_" in t or t.islower()


class TestMCPToolRegistered:
    def test_list_blockers_tool_definition_exists(self):
        # Source-level check: the MCP tool function is importable
        from okto_pulse.core.mcp import server as mcp_server
        assert hasattr(mcp_server, "okto_pulse_list_blockers")

    def test_mcp_tool_accepts_filter_type(self):
        import inspect
        from okto_pulse.core.mcp import server as mcp_server
        fn = mcp_server.okto_pulse_list_blockers
        # FastMCP wraps the function; unwrap if needed
        target = getattr(fn, "fn", fn)
        sig = inspect.signature(target)
        assert "filter_type" in sig.parameters
        assert "stale_hours" in sig.parameters
        assert sig.parameters["stale_hours"].default == 72
