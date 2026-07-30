"""SK-B B10 Policy Compliance projection on allowed transitions."""

from __future__ import annotations

from types import SimpleNamespace
import uuid
from unittest.mock import AsyncMock

import pytest

from okto_pulse.community.api.allowed_transitions import (
    AllowedTransitionsResponse,
)
from okto_pulse.core.application.use_cases.allowed_transitions import (
    POLICY_SUBJECT_REQUIRED,
    ListAllowedTransitionsCommand,
    ListAllowedTransitionsUseCase,
    allowed_transitions_for_status,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.guideline_policy import PolicyEntityType
from okto_pulse.core.domain.guideline_policy_transition import (
    PolicyTransitionReasonCode,
    PolicyTransitionSnapshot,
    evaluate_policy_transition,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import (
    get_session_factory,
)
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from sqlalchemy_test_models import Board


USER = "skb-b10-allowed-transition"


def _decision(*, blocking_rules: int):
    snapshot = PolicyTransitionSnapshot(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        expected_from_status="approved",
        applicable_rule_count=blocking_rules,
        applicable_blocking_rule_count=blocking_rules,
        receipt=None,
        current_snapshot=None,
    )
    return evaluate_policy_transition(snapshot, "validated")


def test_static_projection_preserves_additive_registry_metadata() -> None:
    transitions = allowed_transitions_for_status("spec", "approved")
    validated = next(item for item in transitions if item.to_status == "validated")
    recovery = next(item for item in transitions if item.to_status == "draft")
    scenario_edges = allowed_transitions_for_status(
        "test_scenario",
        "ready",
    )

    assert validated.policy_compliance is True
    assert validated.policy_compliance_decision is None
    assert recovery.policy_compliance is False
    assert all(
        item.policy_compliance
        for item in scenario_edges
        if item.to_status in {"automated", "passed", "failed"}
    )
    assert all(
        item.gate == "test_scenario_progression"
        for item in scenario_edges
        if item.policy_compliance
    )


class _PreviewUseCase(ListAllowedTransitionsUseCase):
    def __init__(self, existing_blocker: str | None) -> None:
        self.existing_blocker = existing_blocker

    async def _blocked_reason(self, *args, **kwargs) -> str | None:
        return self.existing_blocker


@pytest.mark.asyncio
@pytest.mark.parametrize("blocking_rules", (0, 1))
async def test_entity_scoped_projection_uses_exact_guideline_service_decision(
    blocking_rules: int,
) -> None:
    decision = _decision(blocking_rules=blocking_rules)
    guidelines = SimpleNamespace(
        preview_policy_transition=AsyncMock(return_value=decision)
    )
    services = SimpleNamespace(guidelines=guidelines)
    entity = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        status=SpecStatus.APPROVED,
    )
    transition = next(
        item
        for item in allowed_transitions_for_status("spec", "approved")
        if item.to_status == "validated"
    )
    use_case = _PreviewUseCase("spec_validation_required: existing gate")

    projected = await use_case._preview_entity_transition(
        services,
        "spec",
        entity,
        transition,
    )

    guidelines.preview_policy_transition.assert_awaited_once_with(
        board_id="board-1",
        entity_type="spec",
        subject_id="spec-1",
        from_status="approved",
        to_status="validated",
    )
    assert projected.policy_compliance
    assert projected.policy_compliance_decision is not None
    assert (
        projected.policy_compliance_decision.decision_digest == decision.decision_digest
    )
    assert projected.policy_compliance_decision.fence_digest == decision.fence_digest
    assert projected.policy_compliance_decision.policy_compliance_required is True
    assert projected.policy_compliance_decision.allowed is decision.allowed
    assert projected.policy_compliance_decision.state == decision.reason_code.value
    if blocking_rules:
        assert not decision.allowed
        assert "spec_validation_required" in (projected.blocked_reason or "")
        assert PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_MISSING.value in (
            projected.blocked_reason or ""
        )
    else:
        assert decision.allowed
        assert projected.blocked_reason == ("spec_validation_required: existing gate")


@pytest.mark.asyncio
async def test_current_status_only_returns_structural_subject_required_state() -> None:
    board_id = f"skb-b10-board-{uuid.uuid4().hex[:10]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="SK-B B10",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
                settings={},
            )
        )
        await db.commit()
        result = await ListAllowedTransitionsUseCase().execute(
            ListAllowedTransitionsCommand(
                board_id,
                "spec",
                current_status="approved",
            ),
            actor=ActorContext(
                USER,
                "test",
                board_id=board_id,
                realm_id=LOCAL_REALM_ID,
            ),
            uow=resolve_unit_of_work_factory().wrap(db),
        )

    payload = result.read_model.to_dict()
    validated = next(
        item
        for item in payload["allowed_transitions"]
        if item["to_status"] == "validated"
    )
    recovery = next(
        item for item in payload["allowed_transitions"] if item["to_status"] == "draft"
    )

    assert validated["policy_compliance"] is True
    assert validated["policy_compliance_decision"] == {
        "state": POLICY_SUBJECT_REQUIRED,
        "allowed": None,
        "policy_compliance_required": True,
        "reason_codes": [POLICY_SUBJECT_REQUIRED],
        "decision_digest": None,
        "fence_digest": None,
        "receipt_id": None,
        "currentness": None,
        "currentness_reasons": [],
        "applicable_rule_count": None,
        "applicable_blocking_rule_count": None,
        "blocking_rule_count": None,
        "waived_rule_count": None,
        "advisory_issue_count": None,
    }
    assert POLICY_SUBJECT_REQUIRED in validated["blocked_reason"]
    assert recovery["policy_compliance"] is False
    assert recovery["policy_compliance_decision"] is None
    assert recovery["blocked_reason"] is None
    # The Community response schema must preserve the same additive contract.
    assert (
        AllowedTransitionsResponse.model_validate(payload).model_dump(mode="json")
        == payload
    )


@pytest.mark.asyncio
async def test_mcp_tool_documents_test_scenario_and_scoped_policy_semantics() -> None:
    tool = await mcp_server.mcp.get_tool("okto_pulse_get_allowed_transitions")
    documentation = tool.description

    assert documentation is not None
    assert "test_scenario" in documentation
    assert "Policy Compliance" in documentation
    assert "policy_subject_required" in documentation
