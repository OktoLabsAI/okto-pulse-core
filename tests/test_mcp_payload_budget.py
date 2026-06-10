"""R5.2 — char-based MCPPayloadBudgetScanner, MCPProjectionTelemetrySink, and
projection guidance guardrails.

Covers spec ``MCP Projection Envelope and Token Budget Guardrails`` scenarios:

- ``ts_d8a4ad4e`` — scanner fails with actionable over-budget paths.
- ``ts_2717dde6`` — projection telemetry records safe labels only.
- ``ts_64c98201`` — agent guidance preserves the full-context invariant.
- ``ts_236b6194`` — R1–R4 adoption of the projection helper preserves semantics.
- ``ts_86563ed2`` — envelope requires outcome; status is optional compat.
- ``ts_c4991939`` — unified scanner reports char-based violations (no tokens).
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.payload_budget import (
    METRIC_BUDGET_VIOLATION,
    METRIC_PAYLOAD_BUDGET_VIOLATION,
    METRIC_PROJECTION_LEGACY_FULL,
    METRIC_PROJECTION_RESPONSE,
    METRIC_UNSAFE_LABEL_REJECTED,
    UNSAFE_LABEL_REASON,
    BudgetViolation,
    MCPPayloadBudgetScanner,
    MCPProjectionTelemetrySink,
    is_safe_label_value,
    load_budget_profile,
    snapshot_instructions,
    snapshot_resources,
    snapshot_tool_descriptions,
)
from okto_pulse.core.mcp.projection_envelope import (
    ENVELOPE_METADATA_KEYS,
    project_response,
)

FIXTURES = Path(__file__).parent / "fixtures" / "mcp_payload_budgets"
AGENT_INSTRUCTIONS = (
    Path(__file__).parent.parent
    / "src"
    / "okto_pulse"
    / "core"
    / "mcp"
    / "agent_instructions.md"
)


def _fixture_profile():
    return load_budget_profile(FIXTURES / "budget_manifest.json", name="r5_fixture")


def _payload(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# ts_d8a4ad4e / ts_c4991939 — scanner over-budget paths, char-based, no tokens
# ---------------------------------------------------------------------------


def test_scanner_fails_with_actionable_over_budget_paths():
    # Covers all four required surfaces over budget: tool description,
    # always-loaded instruction, resource, AND payload fixture.
    scanner = MCPPayloadBudgetScanner(_fixture_profile())  # per_resource_chars=20000
    report = scanner.scan(
        tool_descriptions={"okto_pulse_big": "x" * 1200, "okto_pulse_small": "ok"},
        instructions={"big_instructions.md": "y" * 9000, "small.md": "tiny"},
        resources={"reference/big.md": "z" * 21000, "reference/small.md": "tiny"},
        payload_fixtures={
            "within_budget.json": _payload("within_budget.json"),
            "oversized.json": _payload("oversized.json"),
        },
    )
    assert report.passed is False
    assert report.unit == "chars"

    paths = {v.path for v in report.violations}
    assert "tool:okto_pulse_big" in paths
    assert "instruction:big_instructions.md" in paths
    assert "resource:reference/big.md" in paths
    assert "payload:oversized.json" in paths
    # within-budget items did not violate
    assert "tool:okto_pulse_small" not in paths
    assert "resource:reference/small.md" not in paths
    assert "payload:within_budget.json" not in paths

    # the resource violation carries the full actionable shape
    res_v = next(v for v in report.violations if v.path == "resource:reference/big.md")
    rd = res_v.to_dict()
    assert rd["actual_chars"] == 21000
    assert rd["budget_chars"] == 20000
    assert rd["reason"] == "resource_over_budget"
    assert rd["budget_profile"] == "r5_fixture"
    assert rd["unit"] == "chars"

    for v in report.violations:
        d = v.to_dict()
        for key in ("path", "actual_chars", "budget_chars", "reason", "budget_profile", "unit"):
            assert key in d
        assert d["unit"] == "chars"
        assert d["budget_profile"] == "r5_fixture"
        assert d["actual_chars"] > d["budget_chars"]


def test_unified_scanner_char_based_violations_emit_no_token_fields():
    scanner = MCPPayloadBudgetScanner(_fixture_profile())
    report = scanner.scan(
        tool_descriptions={"okto_pulse_x": "z" * 1500},
        payload_fixtures={"oversized.json": _payload("oversized.json")},
    )
    assert report.passed is False
    assert report.unit == "chars"

    tool_violation = next(v for v in report.violations if v.path.startswith("tool:"))
    assert tool_violation.budget_chars == 900  # concrete R5 budget

    blob = json.dumps(report.to_dict()).lower()
    assert "actual_tokens" not in blob
    assert "max_tokens" not in blob
    assert "token" not in blob  # no token-based units anywhere


def test_scanner_passes_within_budget_and_reports_aggregate_measurements():
    scanner = MCPPayloadBudgetScanner(_fixture_profile())
    report = scanner.scan(
        tool_descriptions={"okto_pulse_a": "short desc", "okto_pulse_b": "another"},
        instructions={"x.md": "small"},
        payload_fixtures={"within_budget.json": _payload("within_budget.json")},
    )
    assert report.passed is True
    assert report.violations == []
    assert report.measurements["tool_count"] == 2
    assert report.measurements["aggregate_tool_description_chars"] == len("short desc") + len(
        "another"
    )
    assert "payload_chars:within_budget.json" in report.measurements


# ---------------------------------------------------------------------------
# ts_2717dde6 — telemetry records safe labels only; rejects unsafe fail-closed
# ---------------------------------------------------------------------------


def test_telemetry_records_safe_projection_labels_only():
    sink = MCPProjectionTelemetrySink()
    for profile in ("summary", "detail"):
        env = project_response(
            profile=profile, body={"id": "c1"}, omitted_count=2, deduped_count=1
        )
        assert sink.record_projection_envelope(env).accepted is True
    full_env = project_response(profile="full", body={"id": "c1", "success": True}, truncated=True)
    assert sink.record_projection_envelope(full_env).accepted is True
    legacy_env = project_response(profile="legacy", body={"id": "c1", "success": True})
    assert sink.record_projection_envelope(legacy_env).accepted is True

    assert sink.metrics[METRIC_PROJECTION_RESPONSE] == 4
    assert sink.metrics[METRIC_PROJECTION_LEGACY_FULL] == 2  # full + legacy only
    # No event carries a body field — only safe labels — and outcome is recorded
    # on every projected response (or_13c4906b).
    for event in sink.events:
        assert "outcome" in event
        assert event["outcome"] in ("ok", "error")
        assert set(event.keys()) <= {
            "type",
            "tool_name",
            "profile",
            "outcome",
            "truncated",
            "omitted_count",
            "deduped_count",
            "payload_bytes",
            "legacy_full",
        }


def test_record_projection_accepts_api_contract_shape_with_tool_name_and_outcome():
    # api_71a31d9f request shape must be accepted and recorded with outcome.
    sink = MCPProjectionTelemetrySink()
    result = sink.record_projection(
        {
            "tool_name": "okto_pulse_x",
            "profile": "summary",
            "payload_bytes": 12,
            "truncated": False,
            "omitted_count": 0,
            "deduped_count": 0,
            "outcome": "ok",
        }
    )
    assert result.accepted is True
    assert sink.metrics[METRIC_PROJECTION_RESPONSE] == 1
    event = sink.events[0]
    assert event["tool_name"] == "okto_pulse_x"
    assert event["outcome"] == "ok"


def test_record_projection_envelope_records_outcome():
    sink = MCPProjectionTelemetrySink()
    env = project_response(profile="summary", body={"id": "c1"})
    assert env["outcome"] == "ok"
    assert sink.record_projection_envelope(env, tool_name="okto_pulse_x").accepted is True
    event = sink.events[0]
    assert event["outcome"] == "ok"
    assert event["tool_name"] == "okto_pulse_x"

    # failure envelope propagates outcome=error
    fail_env = project_response(profile="summary", body={"success": False, "error": "e", "error_code": "c"})
    assert sink.record_projection_envelope(fail_env).accepted is True
    assert sink.events[1]["outcome"] == "error"


def test_telemetry_rejects_unsafe_label_with_body_or_email():
    sink = MCPProjectionTelemetrySink()
    body_leak = {"profile": "summary", "payload": '{"id":"c1","secret":"leak"}'}
    res = sink.record_projection(body_leak)
    assert res.accepted is False
    assert res.reason == UNSAFE_LABEL_REASON

    email_leak = {"profile": "summary", "user": "jp.am.braga@gmail.com"}
    assert sink.record_projection(email_leak).accepted is False

    assert sink.metrics[METRIC_UNSAFE_LABEL_REJECTED] == 2
    # fail-closed: nothing was recorded for the unsafe events
    assert sink.metrics.get(METRIC_PROJECTION_RESPONSE) is None
    assert sink.events == []


def test_telemetry_rejects_forbidden_label_keys_even_when_value_is_short():
    # Rework (Codex val_ed0099ed): a forbidden KEY leaks board content/body/
    # query/PII regardless of how short/format-safe the value looks. Each must
    # be rejected fail-closed, not recorded.
    forbidden_cases = [
        {"profile": "summary", "payload": "c1"},
        {"profile": "summary", "query": "board_id"},
        {"profile": "summary", "description": "title"},
        {"profile": "summary", "content": "abc"},
        {"profile": "summary", "email": "redacted"},
    ]
    sink = MCPProjectionTelemetrySink()
    for case in forbidden_cases:
        result = sink.record_projection(case)
        assert result.accepted is False, f"should reject {case!r}"
        assert result.reason == UNSAFE_LABEL_REASON

    assert sink.metrics[METRIC_UNSAFE_LABEL_REJECTED] == len(forbidden_cases)
    # fail-closed: no normal projection events recorded, no response metric
    assert sink.events == []
    assert sink.metrics.get(METRIC_PROJECTION_RESPONSE) is None


def test_telemetry_rejects_unknown_label_key_fail_closed():
    # Allowlist posture: a key that is neither allowlisted nor obviously safe is
    # rejected, so new/unexpected labels cannot silently leak content.
    sink = MCPProjectionTelemetrySink()
    result = sink.record_projection({"profile": "summary", "some_new_field": "x"})
    assert result.accepted is False
    assert result.reason == UNSAFE_LABEL_REASON
    assert sink.events == []


def test_budget_violation_emits_canonical_and_compat_metrics():
    sink = MCPProjectionTelemetrySink()
    violation = BudgetViolation(
        path="tool:okto_pulse_x",
        actual_chars=1200,
        budget_chars=900,
        reason="per_tool_description_over_budget",
        budget_profile="r5_default",
    )
    assert sink.record_budget_violation(violation).accepted is True
    assert sink.metrics[METRIC_BUDGET_VIOLATION] == 1
    assert sink.metrics[METRIC_PAYLOAD_BUDGET_VIOLATION] == 1


def test_is_safe_label_value_fails_closed():
    assert is_safe_label_value("summary")
    assert is_safe_label_value(42)
    assert is_safe_label_value(True)
    assert is_safe_label_value("tool:okto_pulse_get_card")
    assert is_safe_label_value("per_tool_description_over_budget")
    assert not is_safe_label_value("a@b.com")
    assert not is_safe_label_value('{"x":1}')
    assert not is_safe_label_value("<xml>body</xml>")
    assert not is_safe_label_value("has spaces and text")
    assert not is_safe_label_value("x" * 200)


# ---------------------------------------------------------------------------
# ts_64c98201 — agent guidance preserves the full-context invariant
# ---------------------------------------------------------------------------


def test_agent_guidance_references_projection_and_keeps_full_context_rule():
    text = AGENT_INSTRUCTIONS.read_text(encoding="utf-8")
    low = text.lower()
    # references the lazy resource
    assert "okto-pulse://reference/projection-profiles" in text
    # recommends summary-first exploration
    assert "summary-first" in low
    # still REQUIRES full get_*_context before status-changing moves
    assert "get_*_context" in text
    assert "status-changing move" in low
    assert "before" in low

    resource = mcp_server._load_resource_file("reference/projection_profiles.md")
    assert "full context" in resource.lower()
    assert "before" in resource.lower()


# ---------------------------------------------------------------------------
# ts_236b6194 — R1–R4 adoption wraps without changing source/permission semantics
# ---------------------------------------------------------------------------


def test_r1_r4_outputs_wrap_in_summary_and_full_without_mutation():
    fixtures = {
        "r1_docs": {"tool": "okto_pulse_x", "description": "trimmed doc"},
        "r2_context": {"id": "task-1", "spec": {"id": "s1"}, "success": True},
        "r3_compaction": {"id": "card-1", "comments": [{"id": "c"}]},
        "r4_alias": {"alias": "list", "resolved": "okto_pulse_list_by_board", "success": True},
    }
    for src in fixtures.values():
        snapshot = copy.deepcopy(src)
        summary = project_response(profile="summary", body=src)
        full = project_response(profile="full", body=src)
        # summary adds the envelope metadata
        for key in ENVELOPE_METADATA_KEYS:
            assert key in summary
        # full preserves every source field exactly
        for key, value in snapshot.items():
            assert full[key] == value
        # no mutation of the source payload (no domain/permission change)
        assert src == snapshot


# ---------------------------------------------------------------------------
# ts_86563ed2 — envelope requires outcome; status is optional compatibility
# ---------------------------------------------------------------------------


def test_envelope_requires_outcome_and_treats_status_as_optional():
    for profile in ("summary", "detail", "full", "legacy"):
        ok = project_response(
            profile=profile, body={"id": "c1", "success": True, "status": "active"}
        )
        assert ok["outcome"] == "ok"  # canonical success key always present
        assert ok.get("status") == "active"  # status passes through as metadata
        if profile in ("full", "legacy"):
            assert ok["success"] is True  # compat success preserved
        else:
            assert "success" not in ok  # slim omits positive success

        fail = project_response(
            profile=profile,
            body={"id": "c1", "success": False, "error": "e", "error_code": "c"},
        )
        assert fail["outcome"] == "error"
        assert fail["error"] == "e" and fail["error_code"] == "c"


# ---------------------------------------------------------------------------
# ir_0e6ac2bc — live registry snapshot scan is deterministic + char-based
# ---------------------------------------------------------------------------


def test_live_registry_snapshot_is_char_based_and_token_free():
    scanner = MCPPayloadBudgetScanner()
    descriptions = snapshot_tool_descriptions(mcp_server.mcp)
    instructions = snapshot_instructions(mcp_server)
    resources = snapshot_resources(mcp_server)
    assert descriptions and instructions and resources  # non-empty live surface

    report = scanner.scan(
        tool_descriptions=descriptions, instructions=instructions, resources=resources
    )
    assert report.unit == "chars"
    assert "aggregate_tool_description_chars" in report.measurements
    # deterministic: the measured aggregate equals a recomputed char sum
    assert report.measurements["aggregate_tool_description_chars"] == sum(
        len(d) for d in descriptions.values()
    )
    blob = json.dumps(report.to_dict()).lower()
    assert "actual_tokens" not in blob and "max_tokens" not in blob
    # NOTE: passed is intentionally NOT asserted — the live surface is the
    # pre-R1–R4 "before" state and exceeds the target budgets by design.
