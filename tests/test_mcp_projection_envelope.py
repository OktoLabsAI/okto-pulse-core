"""R5.1 — MCPProjectionEnvelopeHelper and projection profile semantics.

Covers spec ``MCP Projection Envelope and Token Budget Guardrails`` scenarios:

- ``ts_a284663c`` — projection helper emits complete envelope metadata.
- ``ts_68dc1a8d`` — profiles have deterministic shapes; unsupported errors.
- ``ts_d9a55d11`` — legacy/full preserve success true and success false.
- ``ts_1d78055d`` — slim/default success omission still exposes outcome + error.

Plus card R5.1 required units: response-only (no input mutation), deterministic
``payload_bytes``, R1–R4 fixture wrapping without changing full/legacy
semantics, and the lazy ``okto-pulse://reference/projection-profiles`` resource.
"""

from __future__ import annotations

import copy
import json

from okto_pulse.core.mcp.projection_envelope import (
    DEFAULT_PROFILE,
    ENVELOPE_METADATA_KEYS,
    OUTCOME_ERROR,
    OUTCOME_OK,
    PROJECTION_CONTRACT_VIOLATION_CODE,
    SUPPORTED_PROFILES,
    UNSUPPORTED_PROFILE_CODE,
    MCPProjectionEnvelopeHelper,
    _stable_payload_bytes,
    is_supported_profile,
    project_response,
    resolve_profile,
)

# Distinct per-profile bodies for the determinism scenario.
BODIES = {
    "summary": {"id": "c1", "count": 3},
    "detail": {"id": "c1", "count": 3, "titles": ["a", "b", "c"]},
    "full": {"id": "c1", "count": 3, "items": [{"id": "a", "body": "x" * 40}]},
    "legacy": {"id": "c1", "success": True, "data": {"legacy_field": "kept"}},
}


# ---------------------------------------------------------------------------
# ts_a284663c — complete envelope metadata
# ---------------------------------------------------------------------------


def test_summary_envelope_has_all_sc1_metadata():
    result = project_response(
        profile="summary",
        body={"id": "c1", "count": 3},
        follow_up=[{"rel": "read_full_context", "target_ref": "okto_pulse_get_task_context"}],
    )
    for key in ENVELOPE_METADATA_KEYS:
        assert key in result, f"missing envelope key: {key}"
    assert result["profile"] == "summary"
    assert result["outcome"] == OUTCOME_OK
    assert isinstance(result["payload_bytes"], int) and result["payload_bytes"] > 0
    assert result["truncated"] is False
    assert result["omitted_count"] == 0
    assert result["deduped_count"] == 0
    assert result["follow_up"] == [
        {"rel": "read_full_context", "target_ref": "okto_pulse_get_task_context"}
    ]
    # outcome is the canonical success key — status must not be the success key.
    assert "status" not in result
    # summary omits positive success entirely.
    assert "success" not in result


def test_default_profile_is_summary():
    assert resolve_profile(None) == DEFAULT_PROFILE == "summary"
    assert resolve_profile("") == "summary"
    assert project_response(profile=None, body={"x": 1})["profile"] == "summary"


# ---------------------------------------------------------------------------
# ts_68dc1a8d — deterministic shapes + unsupported error
# ---------------------------------------------------------------------------


def test_profiles_return_distinct_deterministic_shapes():
    helper = MCPProjectionEnvelopeHelper()
    out = {p: helper.project(profile=p, bodies=BODIES) for p in SUPPORTED_PROFILES}

    # Each profile surfaced its own body fields.
    assert "titles" not in out["summary"] and "items" not in out["summary"]
    assert out["detail"]["titles"] == ["a", "b", "c"]
    assert out["full"]["items"][0]["body"] == "x" * 40
    assert out["legacy"]["data"] == {"legacy_field": "kept"}

    # Each labels its own profile, and the four shapes are pairwise distinct.
    for p in SUPPORTED_PROFILES:
        assert out[p]["profile"] == p
    serialized = {json.dumps(v, sort_keys=True, default=str) for v in out.values()}
    assert len(serialized) == 4

    # Determinism: same input → byte-identical output.
    again = helper.project(profile="full", bodies=BODIES)
    assert json.dumps(again, sort_keys=True, default=str) == json.dumps(
        out["full"], sort_keys=True, default=str
    )


def test_unsupported_profile_returns_structured_error():
    result = project_response(profile="verbose", body={"id": "c1"})
    assert result["error_code"] == UNSUPPORTED_PROFILE_CODE
    assert result["outcome"] == OUTCOME_ERROR
    assert result["supported_profiles"] == list(SUPPORTED_PROFILES)
    assert "verbose" in result["error"]
    assert is_supported_profile("verbose") is False
    assert is_supported_profile("summary") is True


# ---------------------------------------------------------------------------
# ts_d9a55d11 — legacy/full preserve success true AND success false
# ---------------------------------------------------------------------------


def test_legacy_and_full_preserve_positive_and_negative_success():
    ok_body = {"id": "c1", "success": True, "data": {"k": "v"}}
    err_body = {"id": "c1", "success": False, "error": "boom", "error_code": "x"}

    for profile in ("legacy", "full"):
        ok = project_response(profile=profile, body=ok_body)
        assert ok["success"] is True
        assert ok["profile"] == profile

        err = project_response(
            profile=profile, body=err_body, outcome=OUTCOME_ERROR
        )
        assert err["success"] is False  # negative success preserved exactly
        assert err["outcome"] == OUTCOME_ERROR


def test_legacy_success_override_sets_value():
    out = project_response(profile="legacy", body={"id": "c1"}, legacy_success=False)
    assert out["success"] is False
    out2 = project_response(profile="full", body={"id": "c1"}, outcome=OUTCOME_OK)
    # derived from outcome when body has no success
    assert out2["success"] is True


# ---------------------------------------------------------------------------
# ts_1d78055d — slim/default success omission + explicit outcome/error contract
# ---------------------------------------------------------------------------


def test_summary_success_omits_positive_success_uses_outcome():
    out = project_response(profile="summary", body={"id": "c1", "success": True})
    assert "success" not in out  # positive success omitted
    assert out["outcome"] == OUTCOME_OK


def test_summary_failure_exposes_outcome_error_and_error_contract():
    out = project_response(
        profile="summary",
        body={"id": "c1"},
        outcome=OUTCOME_ERROR,
        error="not found",
        error_code="not_found",
    )
    assert out["outcome"] == OUTCOME_ERROR
    assert out["error"] == "not found"
    assert out["error_code"] == "not_found"
    assert "success" not in out  # no truthiness requirement


def test_detail_also_omits_positive_success():
    out = project_response(profile="detail", body={"id": "c1", "success": True})
    assert "success" not in out
    assert out["outcome"] == OUTCOME_OK


# ---------------------------------------------------------------------------
# Rework (Codex val_138e87fa) — a legacy failure must never become a canonical
# success: outcome is INFERRED from failure signals; explicit ok over a failure
# is refused; outcome=error always carries error + error_code.
# ---------------------------------------------------------------------------


def test_summary_infers_error_from_legacy_failure_without_explicit_outcome():
    # The exact reproduction from the reject: no explicit outcome, legacy failure.
    out = project_response(
        profile="summary", body={"success": False, "error": "boom", "error_code": "x"}
    )
    assert out["outcome"] == OUTCOME_ERROR  # was wrongly "ok" before the fix
    assert out["error"] == "boom"
    assert out["error_code"] == "x"
    assert "success" not in out  # summary still omits success


def test_detail_infers_error_from_legacy_failure_without_explicit_outcome():
    out = project_response(
        profile="detail", body={"success": False, "error": "nope", "error_code": "e1"}
    )
    assert out["outcome"] == OUTCOME_ERROR
    assert out["error"] == "nope"
    assert out["error_code"] == "e1"
    assert "success" not in out


def test_error_or_error_code_alone_infers_error_outcome():
    only_error = project_response(profile="summary", body={"id": "c1", "error": "boom"})
    assert only_error["outcome"] == OUTCOME_ERROR
    assert only_error["error"] == "boom"
    assert only_error["error_code"]  # present (synthesized code)

    only_code = project_response(profile="summary", body={"id": "c1", "error_code": "bad"})
    assert only_code["outcome"] == OUTCOME_ERROR
    assert only_code["error_code"] == "bad"
    assert only_code["error"]  # present (derived)


def test_outcome_error_without_detail_synthesizes_error_fields():
    out = project_response(profile="summary", body={"id": "c1"}, outcome=OUTCOME_ERROR)
    assert out["outcome"] == OUTCOME_ERROR
    assert out["error"]  # synthesized safe value, never empty
    assert out["error_code"] == "unspecified_error"


def test_explicit_ok_over_failure_body_returns_contract_violation():
    out = project_response(
        profile="summary", body={"success": False, "error": "boom"}, outcome=OUTCOME_OK
    )
    assert out["error_code"] == PROJECTION_CONTRACT_VIOLATION_CODE
    assert out["outcome"] == OUTCOME_ERROR
    assert "boom" not in out.get("error", "")  # it's the violation message, not the body


def test_legacy_success_false_infers_error_outcome_and_keeps_failure_contract():
    out = project_response(profile="legacy", body={"id": "c1"}, legacy_success=False)
    assert out["success"] is False
    assert out["outcome"] == OUTCOME_ERROR
    assert out["error"] and out["error_code"]  # failure contract satisfied


def test_full_failure_body_infers_error_and_preserves_success_false():
    out = project_response(
        profile="full", body={"id": "c1", "success": False, "error": "x", "error_code": "y"}
    )
    assert out["success"] is False  # preserved exactly
    assert out["outcome"] == OUTCOME_ERROR
    assert out["error"] == "x" and out["error_code"] == "y"


# ---------------------------------------------------------------------------
# Card R5.1 required units: response-only, deterministic bytes, R1–R4 wrap
# ---------------------------------------------------------------------------


def test_helper_does_not_mutate_input_payload():
    body = {"id": "c1", "success": True, "nested": {"a": [1, 2, 3]}}
    snapshot = copy.deepcopy(body)
    result = project_response(profile="summary", body=body)
    assert body == snapshot  # input untouched
    assert result is not body


def test_payload_bytes_is_deterministic_over_body():
    body = {"id": "c1", "count": 3, "items": ["a", "b"]}
    out = project_response(profile="full", body=body)
    assert out["payload_bytes"] == _stable_payload_bytes(body)
    assert out["payload_bytes"] == project_response(profile="full", body=body)[
        "payload_bytes"
    ]


def test_non_mapping_body_is_wrapped_under_data():
    out = project_response(profile="summary", body=[{"id": "a"}, {"id": "b"}])
    assert out["data"] == [{"id": "a"}, {"id": "b"}]
    assert out["profile"] == "summary"
    assert out["outcome"] == OUTCOME_OK


def test_r1_r4_fixture_wraps_without_changing_full_semantics():
    """ir_a5e22ea0 — a representative R1–R4 'full' payload (e.g. a get_card body)
    wraps without losing/altering any domain field or its success flag."""
    source = {
        "id": "card-1",
        "board_id": "b1",
        "status": "in_progress",
        "title": "Implement X",
        "success": True,
        "comments": [{"id": "cm1", "content": "hi"}],
    }
    snapshot = copy.deepcopy(source)
    wrapped = project_response(profile="full", body=source)
    # Every domain field preserved unchanged.
    for key, value in snapshot.items():
        assert wrapped[key] == value
    # Envelope added; input untouched.
    assert wrapped["profile"] == "full"
    assert wrapped["outcome"] == OUTCOME_OK
    assert source == snapshot


def test_follow_up_entries_are_normalized_and_invalid_dropped():
    out = project_response(
        profile="summary",
        body={"id": "c1"},
        follow_up=[
            {"rel": "read_full_context", "target_ref": "okto_pulse_get_card"},
            {"rel": "missing_target"},  # invalid → dropped
            {"target_ref": "missing_rel"},  # invalid → dropped
        ],
    )
    assert out["follow_up"] == [
        {"rel": "read_full_context", "target_ref": "okto_pulse_get_card"}
    ]


# ---------------------------------------------------------------------------
# Resource: okto-pulse://reference/projection-profiles registered + readable
# ---------------------------------------------------------------------------


def test_projection_profiles_resource_registered_and_readable():
    from okto_pulse.core.mcp import server as _srv

    uris = {entry[0] for entry in _srv._RESOURCE_REGISTRY}
    assert "okto-pulse://reference/projection-profiles" in uris

    content = _srv._load_resource_file("reference/projection_profiles.md")
    assert content.startswith("---")  # frontmatter
    assert 'version: "1.0"' in content
    # Documents the canonical key + the four profiles + the safety invariant.
    assert "outcome" in content
    for profile in SUPPORTED_PROFILES:
        assert profile in content
    assert "full context" in content.lower()
