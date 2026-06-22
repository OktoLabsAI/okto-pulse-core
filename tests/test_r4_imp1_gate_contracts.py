"""R4-IMP1 (card 8fe04490) — normalized operational gate / transition contracts.

The gates BLOCK exactly what they blocked before (no state-machine change, no
auto-promotion); R4-IMP1 only normalizes the error/success envelope so MCP/REST/UI
get machine-readable gate_type / required_tool / blocked_transition / next_action.

Anti-test-theater: gates are exercised through the REAL services + REAL MCP tools;
the "no auto-promotion" teeth assert the entity status is UNCHANGED after a block.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.gate_contracts import (
    GATE_SPEC_VALIDATION,
    GATE_TEST_CARD_COMPLETION,
    GateContractError,
    incomplete_test_card_completion_error,
    spec_validation_gate_error,
    task_validation_unsupported_for_test_card_error,
)
from okto_pulse.core.services.main import CardService, SpecService

USER_ID = "user-r4-imp1"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r4 tester"
        self.permissions = object()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


# ===========================================================================
# Builders — envelope shape (deterministic, codex-approved contract)
# ===========================================================================


def test_builders_emit_normalized_envelope():
    spec_err = spec_validation_gate_error(spec_id="s1", current_status="approved")
    assert isinstance(spec_err, ValueError)  # legacy compatibility
    env = spec_err.to_dict()
    assert env["code"] == "spec_validation_gate_required"
    d = env["details"]
    assert d["gate_type"] == GATE_SPEC_VALIDATION
    assert d["blocked_transition"] == "approved->validated"
    assert d["required_status"] == "validated"
    assert d["required_tool"] == "okto_pulse_submit_spec_validation"
    assert d["operator_action"] and d["next_action"]["tool"] == "okto_pulse_submit_spec_validation"

    tv_err = task_validation_unsupported_for_test_card_error(card_id="c1")
    d2 = tv_err.to_dict()["details"]
    assert tv_err.to_dict()["code"] == "test_card_not_subject_to_task_validation"
    assert d2["gate_type"] == GATE_TEST_CARD_COMPLETION
    assert d2["required_tool"] == "okto_pulse_update_test_scenario_status"
    assert d2["follow_up_tool"] == "okto_pulse_move_card"

    tc_err = incomplete_test_card_completion_error(
        card_id="c2", current_status="in_progress", board_id="b1", spec_id="sp1",
        pending_scenarios=[
            {"id": "ts1", "title": "A", "status": "draft"},
            {"id": "ts2", "title": "B", "status": "ready"},
        ],
    )
    d3 = tc_err.to_dict()["details"]
    assert tc_err.to_dict()["code"] == "test_card_completion_blocked"
    assert d3["gate_type"] == GATE_TEST_CARD_COMPLETION
    assert d3["blocked_transition"] == "in_progress->done"
    assert d3["required_tool"] == "okto_pulse_update_test_scenario_status"
    assert d3["unready_scenario_count"] == 2
    # Actionable details: pending scenarios + params_template faithful to the REAL
    # update tool signature (singular scenario_id; NO card_id / plural scenario_ids).
    assert [s["id"] for s in d3["pending_scenarios"]] == ["ts1", "ts2"]
    na = d3["next_action"]
    pt = na["params_template"]
    assert "scenario_ids" not in pt and "card_id" not in pt
    assert pt["spec_id"] == "sp1" and pt["board_id"] == "b1" and "scenario_id" in pt
    assert na["scenario_ids"] == ["ts1", "ts2"]  # separate pending-list metadatum
    assert na["follow_up"]["params"]["board_id"] == "b1"
    assert na["follow_up"]["params"]["card_id"] == "c2"


# ===========================================================================
# move_spec(validated) direct — spec_validation gate (MCP + no auto-promotion)
# ===========================================================================


async def _seed_board_spec(db_factory, *, spec_status, require_validation=True):
    board_id = _id("board")
    spec_id = _id("spec")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4", owner_id=USER_ID,
                     settings={"require_spec_validation": require_validation}))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=spec_status,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[],
                    technical_requirements=[], decisions=[]))
        await db.commit()
    return board_id, spec_id


@pytest.mark.asyncio
async def test_move_spec_validated_direct_returns_gate_envelope(db_factory):
    board_id, spec_id = await _seed_board_spec(db_factory, spec_status=SpecStatus.APPROVED)

    result = await _call("okto_pulse_move_spec", board_id=board_id, spec_id=spec_id,
                         status="validated")

    assert result.get("code") == "spec_validation_gate_required", result
    d = result["details"]
    assert d["gate_type"] == "spec_validation"
    assert d["blocked_transition"] == "approved->validated"
    assert d["required_tool"] == "okto_pulse_submit_spec_validation"
    # TEETH: no auto-promotion / state-machine change — spec is still approved.
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
    assert spec.status == SpecStatus.APPROVED


@pytest.mark.asyncio
async def test_move_spec_validated_raises_gatecontracterror_at_service(db_factory):
    """Service layer raises the typed error (subclass of ValueError, so legacy
    `except ValueError` keeps working)."""
    board_id, spec_id = await _seed_board_spec(db_factory, spec_status=SpecStatus.APPROVED)
    from okto_pulse.core.models.schemas import SpecMove

    async with db_factory() as db:
        with pytest.raises(GateContractError) as exc_info:
            await SpecService(db).move_spec(spec_id, USER_ID, SpecMove(status=SpecStatus.VALIDATED))
    assert exc_info.value.gate_type == GATE_SPEC_VALIDATION


# ===========================================================================
# submit_task_validation on a test card — not subject to the task gate
# ===========================================================================


@pytest.mark.asyncio
async def test_submit_task_validation_on_test_card_returns_redirect_contract(db_factory):
    board_id = _id("board")
    card_id = _id("card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4", owner_id=USER_ID))
        db.add(Card(id=card_id, board_id=board_id, title="test card",
                    status=CardStatus.VALIDATION, card_type=CardType.TEST, created_by=USER_ID))
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(GateContractError) as exc_info:
            await CardService(db).submit_task_validation(
                card_id, USER_ID, "r4 tester",
                {"confidence": 90, "confidence_justification": "x",
                 "estimated_completeness": 90, "completeness_justification": "x",
                 "estimated_drift": 5, "drift_justification": "x",
                 "general_justification": "x", "recommendation": "approve"},
            )
    env = exc_info.value.to_dict()
    assert env["code"] == "test_card_not_subject_to_task_validation"
    assert env["details"]["required_tool"] == "okto_pulse_update_test_scenario_status"
    assert env["details"]["follow_up_tool"] == "okto_pulse_move_card"


# ===========================================================================
# move_card(done) test card with draft/ready scenarios — test_card_completion
# ===========================================================================


@pytest.mark.asyncio
async def test_move_card_done_test_card_with_draft_scenario_blocks_with_contract(db_factory):
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")
    scenario_id = "ts_draft_one"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=SpecStatus.IN_PROGRESS,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[{"id": scenario_id, "title": "Draft scenario", "given": "g",
                                     "when": "w", "then": "t", "status": "draft"}],
                    business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="test card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.TEST, created_by=USER_ID,
                    test_scenario_ids=[scenario_id]))
        await db.commit()

    from okto_pulse.core.models.schemas import CardMove

    async with db_factory() as db:
        with pytest.raises(GateContractError) as exc_info:
            await CardService(db).move_card(card_id, USER_ID, CardMove(status=CardStatus.DONE))
    env = exc_info.value.to_dict()
    assert env["code"] == "test_card_completion_blocked"
    assert env["details"]["gate_type"] == "test_card_completion"
    assert env["details"]["required_tool"] == "okto_pulse_update_test_scenario_status"
    # Actionable: the blocking scenario id is surfaced + threaded into next_action.
    pending = env["details"]["pending_scenarios"]
    assert any(s["id"] == scenario_id and s["status"] == "draft" for s in pending)
    na = env["details"]["next_action"]
    # params_template mirrors the real singular tool signature (no card_id / no
    # plural scenario_ids inside the update tool params).
    assert "scenario_ids" not in na["params_template"] and "card_id" not in na["params_template"]
    assert scenario_id in na["scenario_ids"]
    assert na["params_template"]["spec_id"] == spec_id
    assert na["follow_up"]["params"]["board_id"] == board_id

    # TEETH: no auto-promotion — the test card is still in_progress.
    async with db_factory() as db:
        card = await db.get(Card, card_id)
    assert card.status == CardStatus.IN_PROGRESS


# ===========================================================================
# submit_spec_evaluation — append-only success envelope (state_changed=false)
# ===========================================================================


@pytest.mark.asyncio
async def test_submit_spec_evaluation_success_envelope_is_append_only(db_factory):
    board_id, spec_id = await _seed_board_spec(db_factory, spec_status=SpecStatus.VALIDATED)

    result = await _call(
        "okto_pulse_submit_spec_evaluation", board_id=board_id, spec_id=spec_id,
        breakdown_completeness=80, breakdown_justification="j",
        granularity=80, granularity_justification="j",
        dependency_coherence=80, dependency_justification="j",
        test_coverage_quality=80, test_coverage_justification="j",
        overall_score=80, overall_justification="ok", recommendation="approve",
    )

    assert result.get("success") is True, result
    assert result["state_changed"] is False
    assert result["status_before"] == "validated" and result["status_after"] == "validated"
    assert result["next_action"]["tool"] == "okto_pulse_move_spec"
    assert result["next_action"]["params"]["status"] == "in_progress"
    assert result["evaluation"]["overall_score"] == 80
    # TEETH: append-only, no transition — spec is still validated.
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
    assert spec.status == SpecStatus.VALIDATED
