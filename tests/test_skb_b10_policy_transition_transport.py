"""SK-B B10 stable Policy Compliance rejection envelopes."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from okto_pulse.core.domain.guideline_compliance import (
    PolicyCurrentnessReason,
)
from okto_pulse.core.domain.guideline_policy import (
    PolicyCurrentness,
    PolicyEntityType,
)
from okto_pulse.core.domain.guideline_policy_transition import (
    PolicyTransitionDecision,
    PolicyTransitionReasonCode,
    PolicyTransitionRejected,
)
from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
from okto_pulse.core.inbound.policy_transition_error import (
    project_policy_transition_rejection,
)


SERVER_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "mcp"
    / "server.py"
)
MCP_MUTATION_HANDLERS = (
    "okto_pulse_move_ideation",
    "okto_pulse_move_refinement",
    "okto_pulse_move_spec",
    "okto_pulse_move_sprint",
    "okto_pulse_move_card",
    "okto_pulse_submit_task_validation",
    "okto_pulse_submit_spec_validation",
    "okto_pulse_update_test_scenario_status",
)


def _rejection() -> PolicyTransitionRejected:
    return PolicyTransitionRejected(
        PolicyTransitionDecision(
            entity_type=PolicyEntityType.SPEC,
            subject_id="spec-1",
            from_status="approved",
            to_status="validated",
            transition_allowed=True,
            policy_compliance_required=True,
            allowed=False,
            reason_codes=(
                PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_STALE,
                PolicyTransitionReasonCode.POLICY_COMPLIANCE_BLOCKED,
            ),
            receipt_id="receipt-1",
            currentness=PolicyCurrentness.STALE,
            currentness_reasons=(PolicyCurrentnessReason.POLICY_SET_CHANGED,),
            applicable_rule_count=5,
            applicable_blocking_rule_count=3,
            blocking_rule_count=2,
            waived_rule_count=1,
            advisory_issue_count=1,
            fence_digest="f" * 64,
        )
    )


def _caught_names(node: ast.expr | None) -> set[str]:
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {node.attr}
    if isinstance(node, ast.Tuple):
        names: set[str] = set()
        for element in node.elts:
            names.update(_caught_names(element))
        return names
    return set()


def _function(tree: ast.Module, name: str) -> ast.AsyncFunctionDef:
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"handler_not_found:{name}")


def test_policy_transition_rejection_projection_is_complete_and_stable() -> None:
    error = _rejection()

    assert project_policy_transition_rejection(error) == {
        "outcome": "error",
        "error": "policy_compliance_receipt_stale",
        "code": "policy_compliance_receipt_stale",
        "message": "policy_compliance_receipt_stale",
        "reason_codes": [
            "policy_compliance_receipt_stale",
            "policy_compliance_blocked",
        ],
        "decision_digest": error.decision_digest,
        "fence_digest": "f" * 64,
        "receipt_id": "receipt-1",
        "currentness": "stale",
        "currentness_reasons": ["policy_set_changed"],
        "counts": {
            "applicable_rules": 5,
            "applicable_blocking_rules": 3,
            "blocking_rules": 2,
            "waived_rules": 1,
            "advisory_issues": 1,
        },
        "transition": {
            "entity_type": "spec",
            "subject_id": "spec-1",
            "from_status": "approved",
            "to_status": "validated",
        },
        "policy_compliance_required": True,
    }


def test_mcp_adapter_uses_the_shared_policy_rejection_projection() -> None:
    error = _rejection()

    assert json.loads(MCPAdapterContract.error(error)) == (
        project_policy_transition_rejection(error)
    )


@pytest.mark.parametrize("handler_name", MCP_MUTATION_HANDLERS)
def test_mcp_mutations_catch_policy_rejection_before_value_error(
    handler_name: str,
) -> None:
    tree = ast.parse(SERVER_PATH.read_text(encoding="utf-8"))
    handler = _function(tree, handler_name)
    matching_tries = []
    for candidate in ast.walk(handler):
        if not isinstance(candidate, ast.Try):
            continue
        caught = [_caught_names(item.type) for item in candidate.handlers]
        if any("PolicyTransitionRejected" in names for names in caught):
            matching_tries.append((candidate, caught))

    assert len(matching_tries) == 1
    candidate, caught = matching_tries[0]
    policy_index = next(
        index
        for index, names in enumerate(caught)
        if "PolicyTransitionRejected" in names
    )
    value_index = next(
        index for index, names in enumerate(caught) if "ValueError" in names
    )
    assert policy_index < value_index
    policy_handler = candidate.handlers[policy_index]
    assert any(
        isinstance(node, ast.Attribute)
        and node.attr == "error"
        and isinstance(node.value, ast.Name)
        and node.value.id == "MCPAdapterContract"
        for node in ast.walk(policy_handler)
    )
    assert any(
        isinstance(node, ast.ImportFrom)
        and node.module == "okto_pulse.core.domain.guideline_policy_transition"
        and any(alias.name == "PolicyTransitionRejected" for alias in node.names)
        for node in ast.walk(handler)
    )
