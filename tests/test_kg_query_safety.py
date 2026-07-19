"""Unit tests for the MCP KG query-safety quick-wins (spec R3, card R3.1).

Covers the spec test scenarios at the component level:
- ts_27ecfd9d: oversize natural query rejects before any embedding call.
- ts_5f85add5: Cypher sanitizer strips vector fields from every shape.
- ts_2b45d669: Cypher rows are bounded; full mode respects the hard cap.
- ts_000d98bc: numeric rounding is response-only and order-preserving.
"""

from __future__ import annotations

import json as _json
from unittest.mock import patch

import pytest

from okto_pulse.core.mcp.kg_query_safety import (
    CYPHER_DEFAULT_ROWS,
    CYPHER_HARD_CAP_ROWS,
    KGCypherResponseSanitizer,
    KGHealthMCPProjection,
    KGNaturalQueryBoundaryGuard,
    annotate_truncation,
    resolve_cypher_max_rows,
    round_kg_numbers,
)


# ---------------------------------------------------------------------------
# FR0 — ts_27ecfd9d: oversize natural query rejects before embedding
# ---------------------------------------------------------------------------
def test_guard_passes_normal_query():
    guard = KGNaturalQueryBoundaryGuard(max_chars=2000)
    assert guard.check("which decisions touch caching?") is None


def test_guard_rejects_oversize_before_embedding():
    guard = KGNaturalQueryBoundaryGuard(max_chars=100)
    rejection = guard.check("x" * 250)
    assert rejection is not None
    assert rejection["error"] == "query_too_large"
    # Safe rejection metadata + the embedding was never invoked.
    assert rejection["rejection"]["embedding_invoked"] is False
    assert rejection["rejection"]["query_chars"] == 250
    assert rejection["rejection"]["max_chars"] == 100


def test_guard_boundary_is_inclusive():
    guard = KGNaturalQueryBoundaryGuard(max_chars=10)
    assert guard.check("0123456789") is None  # exactly max -> allowed
    assert guard.check("0123456789x") is not None  # one over -> rejected


# ---------------------------------------------------------------------------
# FR1 — ts_5f85add5: sanitizer strips vector fields from all supported shapes
# ---------------------------------------------------------------------------
def _vec(n: int = 384) -> list[float]:
    return [0.0123456789 + i for i in range(n)]


def test_sanitizer_strips_columns_dotted_and_node_dict():
    # Columns named `embedding` and `n.embedding`, plus a node-like dict
    # carrying an `embedding` key with 384 floats; a real id/title preserved.
    result = {
        "columns": ["embedding", "n.embedding", "node"],
        "rows": [
            [
                _vec(),  # column 0: embedding
                _vec(),  # column 1: n.embedding
                {"id": "dec_1", "title": "Cache writes", "embedding": _vec()},
            ]
        ],
        "row_count": 1,
    }
    out = KGCypherResponseSanitizer().sanitize(result)

    row = out["rows"][0]
    # No vector values remain anywhere.
    assert row[0] is None  # embedding column stripped
    assert row[1] is None  # n.embedding column stripped
    assert "embedding" not in row[2]  # node-dict embedding key stripped
    # Non-vector data preserved.
    assert row[2]["id"] == "dec_1"
    assert row[2]["title"] == "Cache writes"
    # stripped_fields lists each removed path.
    stripped = out["sanitization"]["stripped_fields"]
    assert any("columns[0]" in s for s in stripped)
    assert any("columns[1]" in s for s in stripped)
    assert any(s.endswith(".embedding") for s in stripped)
    assert out["sanitization"]["stripped_count"] == len(stripped) >= 3


def test_sanitizer_strips_bare_vector_without_columns():
    # RETURN n.embedding with no column names: a bare 384-float vector cell.
    result = {"rows": [["dec_1", _vec()]], "row_count": 1}
    out = KGCypherResponseSanitizer().sanitize(result)
    assert out["rows"][0][0] == "dec_1"  # scalar id kept
    assert out["rows"][0][1] is None  # bare vector stripped
    assert out["sanitization"]["stripped_count"] == 1


def test_sanitizer_keeps_short_numeric_lists():
    # A short numeric list (e.g. a degree pair) is NOT a vector — keep it.
    result = {"rows": [[{"id": "n1", "degrees": [0, 2]}]], "row_count": 1}
    out = KGCypherResponseSanitizer().sanitize(result)
    assert out["rows"][0][0]["degrees"] == [0, 2]
    assert out["sanitization"]["stripped_count"] == 0


# ---------------------------------------------------------------------------
# FR2 — ts_2b45d669: bounded default rows, hard cap on full mode
# ---------------------------------------------------------------------------
def test_default_rows_use_agent_safe_limit():
    effective, err = resolve_cypher_max_rows(0)
    assert err is None
    assert effective == CYPHER_DEFAULT_ROWS == 50


def test_explicit_bounded_request_is_honored():
    effective, err = resolve_cypher_max_rows(200)
    assert err is None and effective == 200


def test_above_hard_cap_is_rejected():
    effective, err = resolve_cypher_max_rows(CYPHER_HARD_CAP_ROWS + 1)
    assert effective is None
    assert err is not None
    assert err["error"] == "max_rows_exceeds_hard_cap"
    assert err["hard_cap"] == CYPHER_HARD_CAP_ROWS == 1000
    assert err["requested_max_rows"] == CYPHER_HARD_CAP_ROWS + 1


def test_annotate_truncation_records_bounds():
    result = annotate_truncation({"rows": [], "truncated": True}, 50)
    assert result["row_bounds"]["effective_limit"] == 50
    assert result["row_bounds"]["hard_cap"] == CYPHER_HARD_CAP_ROWS
    assert result["row_bounds"]["truncated"] is True


# ---------------------------------------------------------------------------
# FR3 — ts_000d98bc: rounding is response-only and order-preserving
# ---------------------------------------------------------------------------
def test_rounding_low_precision_deterministic():
    out = round_kg_numbers({"similarity": 0.8472290039062500, "score": 1.0 / 3})
    assert out["similarity"] == 0.8472
    assert out["score"] == 0.3333


def test_rounding_preserves_order():
    # Rows sorted by precise descending score; rounding must not reorder.
    rows = [
        {"id": "a", "score": 0.900011},
        {"id": "b", "score": 0.900009},
        {"id": "c", "score": 0.5000001},
    ]
    out = round_kg_numbers(rows)
    assert [r["id"] for r in out] == ["a", "b", "c"]  # order intact
    # Values rounded (note a/b collapse to the same displayed value but order
    # is preserved because we never re-sort).
    assert out[0]["score"] == 0.9
    assert out[1]["score"] == 0.9
    assert out[2]["score"] == 0.5


def test_rounding_leaves_ints_and_strings():
    out = round_kg_numbers({"count": 7, "name": "n1", "ratio": 0.123456})
    assert out["count"] == 7 and out["name"] == "n1"
    assert out["ratio"] == 0.1235


# ---------------------------------------------------------------------------
# FR4 — ts_860cb8a4: slim KG health projection keeps the stop-rule fields
# ---------------------------------------------------------------------------
def _full_health_payload() -> dict:
    return {
        # operational stop-rule fields
        "board_id": "b1",
        "graph_state": "healthy",
        "discovery_state": "healthy",
        "overall_state": "healthy",
        "metric_status": "available",
        "classification_reason": "metric.ok",
        "correlation_id": "corr-1",
        "memory_pressure_status": "normal",
        "recent_events": [
            {"event_type": "kg.tick.daily", "occurred_at": "2026-06-02T03:00:00Z"}
        ],
        "checked_at": "2026-06-02T10:00:00Z",
        # small operational scalars (kept in slim)
        "queue_depth": 0,
        "dead_letter_count": 0,
        "total_nodes": 50,
        "default_score_ratio": 0.1,
        "avg_relevance": 0.62,
        "contradict_warn_count": 0,
        "last_tick_status": "ok",
        "last_decay_tick_at": "2026-06-02T03:00:00Z",
        "decay_scheduler_diagnostics": {
            "status": "stale",
            "severity": "warning",
            "operational_debt": True,
            "graph_recovery_required": False,
            "recommended_action": "check_decay_scheduler",
            "reason": "latest_success_too_old",
        },
        "storage_footprint_proxy": {
            "source": "runtime_capability",
            "status": "ok",
            "percentage": 12.5,
            "high_water_mark_pct": 12.5,
            "is_direct_memory_telemetry": False,
        },
        # verbose diagnostics / aliases (dropped in slim)
        "state": "healthy",  # alias of overall_state
        "classification_reasons": ["metric.ok"],  # dup of classification_reason
        "schema_version": "1.0",
        "health_schema_version": "1.1",
        "materialization_state": "materialized",
        "materialization_generation": "generation-1",
        "probe_reason_codes": {"board_graph": "board_graph_present"},
        "global_outbox_dead_letter_count": 0,
        "health_issues": [
            {"code": "x", "description": "long human-readable prose ..."}
        ],
        "diagnostics": {"graph_read_status": "ok", "primary_health_cause": "none"},
    }


def test_health_slim_keeps_stop_fields_and_drops_verbose():
    out = KGHealthMCPProjection().project(_full_health_payload(), profile="summary")
    # Every operational stop-rule field survives.
    for field in (
        "graph_state",
        "discovery_state",
        "overall_state",
        "metric_status",
        "classification_reason",
        "correlation_id",
        "memory_pressure_status",
        "recent_events",
    ):
        assert field in out, field
    # Decision-relevant scalars survive.
    assert out["queue_depth"] == 0 and out["total_nodes"] == 50
    # Scheduler debt and storage footprint survive the MCP summary so agents
    # do not infer rebuild/recovery from stale scheduler operations alone.
    assert out["decay_scheduler_diagnostics"]["status"] == "stale"
    assert out["decay_scheduler_diagnostics"]["operational_debt"] is True
    assert out["decay_scheduler_diagnostics"]["graph_recovery_required"] is False
    assert out["storage_footprint_proxy"]["source"] == "runtime_capability"
    assert out["storage_footprint_proxy"]["is_direct_memory_telemetry"] is False
    # Verbose diagnostics and aliases are omitted until full is requested.
    for field in (
        "state",
        "classification_reasons",
        "health_issues",
        "diagnostics",
    ):
        assert field not in out, field
    assert out["health_schema_version"] == "1.1"
    assert out["materialization_state"] == "materialized"
    assert out["materialization_generation"] == "generation-1"
    assert out["probe_reason_codes"] == {"board_graph": "board_graph_present"}
    assert out["global_outbox_dead_letter_count"] == 0
    assert out["profile"] == "summary"
    assert out["full_profile_available"] is True
    assert out["omitted_count"] >= 5


def test_health_full_profile_returns_everything_untouched():
    full = _full_health_payload()
    out = KGHealthMCPProjection().project(full, profile="full")
    assert out is full  # full diagnostics preserved verbatim
    assert "health_issues" in out and "diagnostics" in out


def test_health_legacy_profile_is_also_full():
    out = KGHealthMCPProjection().project(_full_health_payload(), profile="legacy")
    assert "diagnostics" in out and "health_issues" in out


def test_health_projection_rejects_unknown_profile_consistently():
    out = KGHealthMCPProjection().project(_full_health_payload(), profile="verbose")
    assert out == {
        "outcome": "error",
        "error": "Unsupported projection profile: 'verbose'",
        "error_code": "unsupported_projection",
        "supported_profiles": ["summary", "detail", "full", "legacy"],
    }


def test_health_detail_keeps_its_requested_profile_metadata():
    out = KGHealthMCPProjection().project(_full_health_payload(), profile="detail")
    assert out["profile"] == "detail"


# ===========================================================================
# INTEGRATION — the guard/sanitizer/bounds are WIRED into the real MCP handlers
# (not just pure components). These prove the runtime path, which the
# component tests above cannot. ts_27ecfd9d's "embedding spy records zero
# calls" is realised here as "the executor is never invoked".
# ===========================================================================
class _StubAgent:
    id = "agent-test"


async def _stub_get_agent():
    return _StubAgent()


def _fresh_mcp(name: str):
    from okto_pulse.core.mcp.catalog import CoreMcpCatalog

    return CoreMcpCatalog(name=name, version="test")


@pytest.mark.asyncio
async def test_natural_handler_rejects_oversize_before_executor():
    """FR0 wired: an oversize query is rejected and the executor (which resolves
    the embedding provider) is NEVER called."""
    import okto_pulse.core.mcp.kg_power_tools as kpt

    calls = {"executor": 0}

    def _spy_execute(*args, **kwargs):
        calls["executor"] += 1
        return {"nodes": [], "total_matches": 0}

    mcp = _fresh_mcp("test-kg-safety-natural-reject")
    with (
        patch.object(kpt, "execute_natural_query", _spy_execute),
        patch.object(kpt, "check_rate_limit", lambda *_a, **_k: None),
    ):
        kpt.register_kg_power_tools(mcp, get_agent=_stub_get_agent)
        tool = await mcp.get_tool("okto_pulse_kg_query_natural")
        raw = await tool.fn(board_id="b1", nl_query="x" * 5000)

    payload = _json.loads(raw)
    assert payload["error"]["code"] == "query_too_large"
    assert payload["error"]["embedding_invoked"] is False
    assert payload["error"]["query_chars"] == 5000
    assert calls["executor"] == 0  # embedding provider never resolved


@pytest.mark.asyncio
async def test_natural_handler_runs_executor_for_normal_query():
    """A within-bounds query reaches the executor (the guard does not over-block)
    and FR3 rounding is applied to the response."""
    import okto_pulse.core.mcp.kg_power_tools as kpt

    calls = {"executor": 0}

    def _spy_execute(*args, **kwargs):
        calls["executor"] += 1
        return {"nodes": [{"id": "n1", "similarity": 0.123456789}], "total_matches": 1}

    mcp = _fresh_mcp("test-kg-safety-natural-ok")
    with (
        patch.object(kpt, "execute_natural_query", _spy_execute),
        patch.object(kpt, "check_rate_limit", lambda *_a, **_k: None),
    ):
        kpt.register_kg_power_tools(mcp, get_agent=_stub_get_agent)
        tool = await mcp.get_tool("okto_pulse_kg_query_natural")
        raw = await tool.fn(board_id="b1", nl_query="which decisions touch caching?")

    payload = _json.loads(raw)
    assert calls["executor"] == 1
    assert payload["nodes"][0]["similarity"] == 0.1235  # FR3 rounding applied


@pytest.mark.asyncio
async def test_cypher_handler_sanitizes_bounds_and_rounds():
    """FR1/FR2/FR3 wired: the cypher handler strips embeddings, applies the
    agent-safe default limit, and rounds — through the real tool path."""
    import okto_pulse.core.mcp.kg_power_tools as kpt

    seen = {"max_rows": None}

    def _spy_cypher(
        board_id,
        cypher,
        params,
        *,
        max_rows,
        timeout_ms,
        include_working=False,
    ):
        seen["max_rows"] = max_rows  # agent-safe default must reach the executor
        return {
            "rows": [["dec_1", [0.1 + i for i in range(384)]]],  # bare vector cell
            "row_count": 1,
            "truncated": False,
        }

    mcp = _fresh_mcp("test-kg-safety-cypher")
    with (
        patch.object(kpt, "execute_cypher_read_only", _spy_cypher),
        patch.object(kpt, "check_rate_limit", lambda *_a, **_k: None),
    ):
        kpt.register_kg_power_tools(mcp, get_agent=_stub_get_agent)
        tool = await mcp.get_tool("okto_pulse_kg_query_cypher")
        raw = await tool.fn(board_id="b1", cypher="MATCH (n) RETURN n.embedding")

    payload = _json.loads(raw)
    assert seen["max_rows"] == CYPHER_DEFAULT_ROWS == 50  # FR2 default reached executor
    assert payload["rows"][0][0] == "dec_1"  # scalar preserved
    assert payload["rows"][0][1] is None  # FR1 bare vector stripped
    assert payload["sanitization"]["stripped_count"] == 1
    assert payload["row_bounds"]["effective_limit"] == 50  # FR2 metadata


@pytest.mark.asyncio
async def test_cypher_handler_rejects_above_hard_cap():
    """FR2 wired: a request above the hard cap is rejected before execution."""
    import okto_pulse.core.mcp.kg_power_tools as kpt

    calls = {"executor": 0}

    def _spy_cypher(*args, **kwargs):
        calls["executor"] += 1
        return {"rows": [], "row_count": 0, "truncated": False}

    mcp = _fresh_mcp("test-kg-safety-cypher-cap")
    with (
        patch.object(kpt, "execute_cypher_read_only", _spy_cypher),
        patch.object(kpt, "check_rate_limit", lambda *_a, **_k: None),
    ):
        kpt.register_kg_power_tools(mcp, get_agent=_stub_get_agent)
        tool = await mcp.get_tool("okto_pulse_kg_query_cypher")
        raw = await tool.fn(board_id="b1", cypher="MATCH (n) RETURN n", max_rows=5000)

    payload = _json.loads(raw)
    # Structured error via the _err helper: {"error": {"code", "message", ...}}.
    assert payload["error"]["code"] == "max_rows_exceeds_hard_cap"
    assert payload["error"]["hard_cap"] == CYPHER_HARD_CAP_ROWS
    assert calls["executor"] == 0  # rejected before execution
