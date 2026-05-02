"""Tests for `since`/`until` temporal filter on kg_query_natural (spec 4b6dfe97)."""

import inspect
import os
import sys
from datetime import timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.kg.tier_power import _parse_iso_ts, execute_natural_query


class TestIsoTimestampParser:
    def test_parses_bare_date(self):
        dt = _parse_iso_ts("2026-04-19")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.year == 2026 and dt.month == 4 and dt.day == 19

    def test_parses_full_iso(self):
        dt = _parse_iso_ts("2026-04-19T12:30:45")
        assert dt is not None
        assert dt.hour == 12

    def test_parses_z_suffix_as_utc(self):
        dt = _parse_iso_ts("2026-04-19T00:00:00Z")
        assert dt is not None
        assert dt.tzinfo == timezone.utc

    def test_none_passes_through(self):
        assert _parse_iso_ts(None) is None

    def test_empty_string_passes_through(self):
        assert _parse_iso_ts("") is None

    def test_invalid_returns_none(self):
        # Best-effort contract: bad input shouldn't crash the tool
        assert _parse_iso_ts("not-a-date") is None
        assert _parse_iso_ts("yesterday") is None


class TestSignatureContract:
    def test_execute_natural_query_accepts_since_until(self):
        sig = inspect.signature(execute_natural_query)
        assert "since" in sig.parameters
        assert "until" in sig.parameters
        assert sig.parameters["since"].default is None
        assert sig.parameters["until"].default is None


class TestMCPToolRegistration:
    def test_mcp_natural_tool_accepts_temporal_params(self):
        import inspect as _inspect
        from okto_pulse.core.mcp.kg_power_tools import register_kg_power_tools
        # register_kg_power_tools must define the tool with since/until
        src = _inspect.getsource(register_kg_power_tools)
        assert "since: str" in src
        assert "until: str" in src
        assert "temporal_filter" in src or "since=" in src
