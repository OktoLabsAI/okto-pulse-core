"""R4-TEST2 (card f64e397d) — behavioral teeth for the test-card completion gate
through the REAL MCP tool surface.

Maps the spec R4 scenarios:
* ts_154b86fb: a test card whose linked scenarios are still draft/ready BLOCKS
  ``okto_pulse_move_card(status='done')``; the returned envelope lists the pending
  scenarios and points at ``okto_pulse_update_test_scenario_status`` with a
  follow-up ``okto_pulse_move_card(status='done')`` — and once the scenarios are
  passed the test_card_completion gate RELEASES.
* ts_ab364e51: ``okto_pulse_submit_task_validation`` on a ``card_type='test'`` card
  rejects the NORMAL task-validation gate and redirects to the scenario-status +
  move-to-done flow (never suggests a normal task validation).

Anti-test-theater: every assertion is over the REAL MCP tool RETURN (the wrapper
catches ``GateContractError`` and serializes ``to_dict()``), with structured-field
assertions (gate_type / required_tool / follow_up_tool / blocked_transition /
pending_scenarios), and the "no auto-promotion" teeth assert the entity status is
UNCHANGED after a block. R4 does not change the state machine.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)

USER_ID = "user-r4-test2"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r4 test2"
        self.permissions = set()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _seed_test_card(db_factory, *, scenarios, card_status=CardStatus.IN_PROGRESS):
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4 test2", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=SpecStatus.IN_PROGRESS,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=scenarios, business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="test card",
                    status=card_status, card_type=CardType.TEST, created_by=USER_ID,
                    test_scenario_ids=[s["id"] for s in scenarios]))
        await db.commit()
    return board_id, spec_id, card_id


# ===========================================================================
# ts_154b86fb — move_card(done) blocked by draft/ready scenarios + release
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_154b86fb_move_card_done_blocked_lists_pending_and_points_to_update(db_factory):
    # Both a draft AND a ready scenario must block (the gate keys on draft/ready).
    board_id, spec_id, card_id = await _seed_test_card(db_factory, scenarios=[
        {"id": "ts_draft", "title": "Draft scenario", "given": "g", "when": "w",
         "then": "t", "status": "draft"},
        {"id": "ts_ready", "title": "Ready scenario", "given": "g", "when": "w",
         "then": "t", "status": "ready"},
    ])

    result = await _call("okto_pulse_move_card", board_id=board_id, card_id=card_id,
                         status="done")

    # Returned envelope (MCP wrapper serialized the GateContractError).
    assert result.get("code") == "test_card_completion_blocked", result
    d = result["details"]
    assert d["gate_type"] == "test_card_completion"
    assert d["required_tool"] == "okto_pulse_update_test_scenario_status"
    assert d["follow_up_tool"] == "okto_pulse_move_card"
    assert d["blocked_transition"] == "in_progress->done"
    assert d["required_status"] == "done"
    assert d["unready_scenario_count"] == 2
    # Both pending scenarios listed with id + status (draft AND ready).
    pend = {s["id"]: s["status"] for s in d["pending_scenarios"]}
    assert pend == {"ts_draft": "draft", "ts_ready": "ready"}
    # next_action: update each scenario, then move_card(done). params_template is
    # faithful to the real singular tool signature (no card_id / no plural ids).
    na = d["next_action"]
    assert na["tool"] == "okto_pulse_update_test_scenario_status"
    assert "card_id" not in na["params_template"] and "scenario_ids" not in na["params_template"]
    assert na["params_template"]["spec_id"] == spec_id
    assert set(na["scenario_ids"]) == {"ts_draft", "ts_ready"}
    assert na["follow_up"]["tool"] == "okto_pulse_move_card"
    assert na["follow_up"]["params"] == {"board_id": board_id, "card_id": card_id, "status": "done"}

    # TEETH: no auto-promotion — the test card is STILL in_progress.
    async with db_factory() as db:
        card = await db.get(Card, card_id)
    assert card.status == CardStatus.IN_PROGRESS


@pytest.mark.asyncio
async def test_ts_154b86fb_gate_releases_once_scenarios_passed(db_factory):
    # Same card; the gate keys on linked scenario status. After the documented
    # remediation (scenarios -> passed) the test_card_completion gate RELEASES.
    board_id, spec_id, card_id = await _seed_test_card(db_factory, scenarios=[
        {"id": "ts_draft", "title": "Draft scenario", "given": "g", "when": "w",
         "then": "t", "status": "draft"},
        {"id": "ts_ready", "title": "Ready scenario", "given": "g", "when": "w",
         "then": "t", "status": "ready"},
    ])

    blocked = await _call("okto_pulse_move_card", board_id=board_id, card_id=card_id,
                          status="done")
    assert blocked.get("code") == "test_card_completion_blocked"

    # Remediate: mark both linked scenarios passed (the authoritative state the gate
    # reads). Done at the spec level to keep this test focused on the move_card gate.
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.test_scenarios = [
            {**s, "status": "passed", "last_run_at": "2026-06-18T00:00:00Z",
             "test_run_id": "r4-test2"}
            for s in spec.test_scenarios
        ]
        await db.commit()

    released = await _call("okto_pulse_move_card", board_id=board_id, card_id=card_id,
                           status="done", conclusion="scenarios passed",
                           completeness=100, completeness_justification="all passed",
                           drift=0, drift_justification="none")
    # TEETH: the test_card_completion gate RELEASED — it no longer fires once the
    # linked scenarios are passed. Any subsequent block is a DIFFERENT, independent
    # completion gate (e.g. the resource N/A gate), which proves the SUT gate cleared
    # rather than masking it. We assert specifically on the test-card gate.
    assert released.get("code") != "test_card_completion_blocked", released


# ===========================================================================
# ts_ab364e51 — submit_task_validation on a test card is redirected (not the
# normal task-validation gate)
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_ab364e51_submit_task_validation_on_test_card_redirects_not_normal_gate(db_factory):
    # submit_task_validation requires the card in 'validation' status before the
    # test-card redirect fires.
    board_id, spec_id, card_id = await _seed_test_card(
        db_factory,
        scenarios=[{"id": "ts_x", "title": "S", "given": "g", "when": "w", "then": "t",
                    "status": "draft"}],
        card_status=CardStatus.VALIDATION,
    )

    result = await _call(
        "okto_pulse_submit_task_validation", board_id=board_id, card_id=card_id,
        confidence=90, confidence_justification="x",
        estimated_completeness=90, completeness_justification="x",
        estimated_drift=5, drift_justification="x",
        general_justification="x", recommendation="approve",
    )

    assert result.get("code") == "test_card_not_subject_to_task_validation", result
    d = result["details"]
    assert d["gate_type"] == "test_card_completion"
    assert d["required_tool"] == "okto_pulse_update_test_scenario_status"
    assert d["follow_up_tool"] == "okto_pulse_move_card"
    # NEGATIVE: the redirect must NOT steer the operator into a normal task
    # validation — the actionable tool/next_action is the scenario-status flow.
    assert d["required_tool"] != "okto_pulse_submit_task_validation"
    assert d["next_action"]["tool"] == "okto_pulse_update_test_scenario_status"
    assert d["next_action"]["follow_up"]["tool"] == "okto_pulse_move_card"
    assert d["next_action"]["follow_up"]["params"]["status"] == "done"
    # Operator guidance points at scenarios + moving to done, not a validation submit.
    assert "scenario" in d["operator_action"].lower()
    assert "done" in d["operator_action"].lower()

    # TEETH: no validation recorded, card unchanged (still in validation).
    async with db_factory() as db:
        card = await db.get(Card, card_id)
    assert card.status == CardStatus.VALIDATION
    assert not (card.validations or [])
