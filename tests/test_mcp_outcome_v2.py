"""Transport-neutral MCP Outcome V2 contract."""

from __future__ import annotations

import json

import pytest

from okto_pulse.core.mcp.outcome import (
    McpToolOutcome,
    coerce_mcp_tool_outcome,
)


@pytest.mark.parametrize(
    ("payload", "code", "retryable"),
    [
        ({"error": "API key authentication required"}, "authentication_required", False),
        ({"error": "Invalid status; value is required"}, "validation_failed", False),
        ({"error": "Card not found"}, "not_found", False),
        ({"error": "Spec is locked"}, "spec_locked", True),
        ({"error": "Validation gate blocked"}, "gate_blocked", False),
        ({"error": "Expected version conflict"}, "version_conflict", True),
    ],
)
def test_legacy_failures_map_to_stable_error_codes(payload, code, retryable):
    outcome = coerce_mcp_tool_outcome(json.dumps(payload), tool_name="probe")
    assert outcome.is_error is True
    assert outcome.code == code
    assert outcome.retryable is retryable
    assert outcome.structured_content(tool_name="probe")["data"] == payload


def test_nested_structured_error_preserves_domain_code_and_message():
    payload = {
        "error": {
            "code": "invalid_artifact_ref",
            "message": "Use spec:<uuid> or card:<uuid>.",
        }
    }

    outcome = coerce_mcp_tool_outcome(payload, tool_name="related_context")

    assert outcome.is_error is True
    assert outcome.code == "invalid_artifact_ref"
    assert outcome.message == "Use spec:<uuid> or card:<uuid>."


@pytest.mark.parametrize(
    "payload",
    [
        {
            "code": "invalid_lane_type",
            "field": "lane_type",
            "received_value": "release_validation",
            "accepted_values": ["normal", "hotfix"],
            "mutation_applied": False,
        },
        {
            "code": "knowledge_governance_invalid_metadata",
            "issues": [{"path": "governance_metadata", "code": "invalid_json"}],
        },
    ],
)
def test_code_only_canonical_envelopes_are_protocol_errors(payload):
    outcome = coerce_mcp_tool_outcome(json.dumps(payload), tool_name="probe")

    assert outcome.is_error is True
    assert outcome.code == payload["code"]
    assert outcome.structured_content(tool_name="probe")["outcome"] == "error"
    assert outcome.legacy_content() == json.dumps(payload)


def test_explicit_success_with_informational_code_remains_success():
    outcome = coerce_mcp_tool_outcome(
        {"success": True, "code": "accepted", "value": 1},
        tool_name="probe",
    )

    assert outcome.is_error is False
    assert outcome.payload["value"] == 1


def test_explicit_retryable_false_is_not_overridden_by_conflict_suffix():
    payload = {
        "error": "knowledge_propagation_revision_conflict",
        "code": "knowledge_propagation_revision_conflict",
        "detail": "read the latest assignment revision and reformulate",
        "retryable": False,
    }

    outcome = coerce_mcp_tool_outcome(payload, tool_name="knowledge_mutation")

    assert outcome.is_error is True
    assert outcome.code == "knowledge_propagation_revision_conflict"
    assert outcome.retryable is False
    assert outcome.structured_content()["retryable"] is False


def test_action_required_is_not_protocol_error_and_has_replay_instruction():
    outcome = coerce_mcp_tool_outcome(
        json.dumps(
            {
                "error": "architecture_warning_acknowledgement_required",
                "ack_token": "ack-1",
            }
        ),
        tool_name="okto_pulse_update_architecture",
    )
    body = outcome.structured_content(tool_name="okto_pulse_update_architecture")
    assert outcome.is_error is False
    assert body["outcome"] == "action_required"
    assert body["retryable"] is True
    assert body["next_action"] == {
        "rel": "retry_with_confirmation",
        "tool": "okto_pulse_update_architecture",
        "arguments": {"ack_token": "ack-1"},
    }


def test_direct_action_outcome_derives_next_action_at_projection_time():
    outcome = McpToolOutcome.action_required(
        {"confirmation_id": "confirm-1"},
        code="confirmation_required",
    )
    assert outcome.structured_content(tool_name="destructive_tool")["next_action"] == {
        "rel": "retry_with_confirmation",
        "tool": "destructive_tool",
        "arguments": {"confirmation_id": "confirm-1"},
    }


def test_plain_text_and_malformed_json_remain_success_data():
    for raw in ("ordinary prose", "{not-json"):
        outcome = coerce_mcp_tool_outcome(raw)
        assert outcome.is_error is False
        assert outcome.payload == raw


def test_explicit_legacy_text_is_preserved_byte_for_byte():
    raw = '{"success":true,"value":1}'
    outcome = coerce_mcp_tool_outcome(raw)
    assert outcome.legacy_content() == raw
