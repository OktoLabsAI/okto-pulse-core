"""Tests for the active-decisions-default policy on get_spec_context
(spec 53c557b6)."""

import inspect
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.mcp.server import _decisions_stats, _filter_decisions_by_status


FIXTURE = [
    {"id": "d1", "title": "A", "status": "active"},
    {"id": "d2", "title": "B", "status": "superseded"},
    {"id": "d3", "title": "C", "status": "revoked"},
    {"id": "d4", "title": "D"},  # legacy row — no status field
    {"id": "d5", "title": "E", "status": "active"},
]


class TestFilterDefaults:
    def test_default_returns_active_plus_legacy(self):
        out = _filter_decisions_by_status(FIXTURE)
        ids = [d["id"] for d in out]
        # d1 + d5 active; d4 has no status → treated as active so legacy rows
        # don't silently vanish.
        assert ids == ["d1", "d4", "d5"]

    def test_include_superseded_returns_all(self):
        out = _filter_decisions_by_status(FIXTURE, include_superseded=True)
        assert len(out) == 5

    def test_none_input(self):
        assert _filter_decisions_by_status(None) == []

    def test_empty_input(self):
        assert _filter_decisions_by_status([]) == []

    def test_drops_non_dict_entries(self):
        messy = FIXTURE + ["stringy-entry", 42]
        out = _filter_decisions_by_status(messy)
        # Non-dict entries dropped in both modes
        assert all(isinstance(d, dict) for d in out)


class TestStats:
    def test_full_breakdown(self):
        stats = _decisions_stats(FIXTURE)
        assert stats["total"] == 5
        assert stats["active"] == 3  # d1 + d5 + d4 (legacy no-status)
        assert stats["superseded"] == 1
        assert stats["revoked"] == 1
        assert stats["other"] == 0

    def test_empty_stats(self):
        stats = _decisions_stats([])
        assert stats == {
            "total": 0, "active": 0, "superseded": 0, "revoked": 0, "other": 0
        }

    def test_unknown_status_goes_to_other(self):
        stats = _decisions_stats([{"status": "weird"}])
        assert stats["other"] == 1
        assert stats["active"] == 0


class TestMCPToolSignature:
    def test_get_spec_context_has_include_superseded(self):
        from okto_pulse.core.mcp import server as mcp_server
        fn = mcp_server.okto_pulse_get_spec_context
        target = getattr(fn, "fn", fn)
        sig = inspect.signature(target)
        assert "include_superseded" in sig.parameters
        # Default MUST be "false" — this is the whole point of the change.
        assert sig.parameters["include_superseded"].default == "false"

    def test_get_task_context_has_include_superseded(self):
        from okto_pulse.core.mcp import server as mcp_server
        fn = mcp_server.okto_pulse_get_task_context
        target = getattr(fn, "fn", fn)
        sig = inspect.signature(target)
        assert "include_superseded" in sig.parameters
        assert sig.parameters["include_superseded"].default == "false"
