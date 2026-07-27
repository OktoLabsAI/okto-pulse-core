"""R4-IMP3 (card ca87ae8d) — explicit operational flow for test cards.

get_task_context for a test card exposes a read-only ``test_card_operational_flow``
block (additive to R4-IMP1): proactive completion guidance — which scenarios are
pending, would_block_done, and the operational next_action (update scenarios ->
move done) — BEFORE the gate fires. Test cards are NOT subject to task validation.

Anti-test-theater: driven through the REAL ``okto_pulse_get_task_context`` tool
over a REAL spec/test-card; the block reflects the live scenario statuses.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import inspect
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
from okto_pulse.core.services.gate_contracts import operational_flow_for_test_card

USER_ID = "user-r4-imp3"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r4 tester"
        self.permissions = set()  # iterable; granular checks patched to allow


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _seed_test_card(db_factory, *, scenarios):
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")
    scenario_ids = [s["id"] for s in scenarios]
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4 imp3", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=SpecStatus.IN_PROGRESS,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=scenarios, business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="test card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.TEST, created_by=USER_ID,
                    test_scenario_ids=scenario_ids))
        await db.commit()
    return board_id, card_id


# ===========================================================================
# Unit — the operational flow block (blocked vs ready)
# ===========================================================================


def test_operational_flow_block_blocked_and_ready():
    blocked = operational_flow_for_test_card(
        card_id="c1", board_id="b1", spec_id="s1", current_status="in_progress",
        linked_scenarios=[
            {"id": "ts1", "title": "A", "status": "draft"},
            {"id": "ts2", "title": "B", "status": "passed", "evidence": {
                "evidence_class": "automated_test_pointer",
                "test_file_path": "tests/test_x.py",
                "test_function": "test_b",
            }},
        ],
    )
    assert blocked["card_type"] == "test"
    assert blocked["applies_to_task_validation"] is False
    assert blocked["gate_type"] == "test_card_completion"
    assert blocked["would_block_done"] is True
    assert [p["id"] for p in blocked["pending_scenarios"]] == ["ts1"]
    na = blocked["next_action"]
    assert na["tool"] == "okto_pulse_update_test_scenario_status"
    # params_template mirrors the REAL singular tool signature; NO plural
    # scenario_ids inside the tool params (R4-IMP3 rework).
    pt = na["params_template"]
    assert "scenario_id" in pt and "scenario_ids" not in pt
    assert pt["board_id"] == "b1" and pt["spec_id"] == "s1"
    assert "status" in pt and "evidence" in pt
    assert "card_id" not in pt  # update_test_scenario_status takes no card_id
    # scenario_ids is a SEPARATE pending-list metadatum.
    assert na["scenario_ids"] == ["ts1"]
    # follow_up move_card carries board_id + card_id + status.
    assert na["follow_up"]["tool"] == "okto_pulse_move_card"
    assert na["follow_up"]["params"]["board_id"] == "b1"
    assert na["follow_up"]["params"]["card_id"] == "c1"
    # evidence_present surfaced per scenario.
    ev = {s["id"]: s["evidence_present"] for s in blocked["linked_scenarios"]}
    assert ev["ts2"] is True and ev["ts1"] is False

    # Nested evidence object (Pulse stores evidence under scenario["evidence"]).
    nested = operational_flow_for_test_card(
        card_id="c1", board_id="b1", spec_id="s1", current_status="in_progress",
        linked_scenarios=[{"id": "ts3", "title": "C", "status": "passed",
                           "evidence": {"last_run_at": "2026-01-01", "test_run_id": "r1"}}],
    )
    assert nested["linked_scenarios"][0]["evidence_present"] is True

    ready = operational_flow_for_test_card(
        card_id="c1", board_id="b1", spec_id="s1", current_status="in_progress",
        linked_scenarios=[{"id": "ts1", "title": "A", "status": "automated",
                           "evidence": {"evidence_class": "automated_test_pointer",
                                        "test_file_path": "tests/test_x.py",
                                        "test_function": "test_a"}}],
    )
    assert ready["would_block_done"] is False
    assert ready["pending_scenarios"] == []
    assert ready["next_action"]["tool"] == "okto_pulse_move_card"
    assert ready["next_action"]["params"]["board_id"] == "b1"
    assert ready["next_action"]["params"]["status"] == "done"
    assert ready["mutation_allowed"] is False


# ===========================================================================
# Contract cross-check — next_action params vs the REAL MCP tool signatures
# (non-tautological: allowed keys are derived from inspect.signature of the
# actual tools, so a regression that puts scenario_ids/card_id back into the
# update params, or drops board_id, FAILS here).
# ===========================================================================


async def _real_tool_params(name: str) -> set[str]:
    tool = await mcp_server.mcp.get_tool(name)
    return set(inspect.signature(tool.fn).parameters.keys())


@pytest.mark.asyncio
async def test_next_action_params_cross_check_real_mcp_signatures():
    update_params = await _real_tool_params("okto_pulse_update_test_scenario_status")
    move_params = await _real_tool_params("okto_pulse_move_card")
    # Ground truth: the real signatures we cross-check against.
    assert {"board_id", "spec_id", "scenario_id", "status", "evidence"} <= update_params
    assert "card_id" not in update_params and "scenario_ids" not in update_params
    assert {"board_id", "card_id", "status"} <= move_params

    blocked = operational_flow_for_test_card(
        card_id="c1", board_id="b1", spec_id="s1", current_status="in_progress",
        linked_scenarios=[{"id": "ts1", "title": "A", "status": "draft"}],
    )
    na = blocked["next_action"]
    pt_keys = set(na["params_template"].keys())
    # Every templated param MUST be a real param of the update tool.
    assert pt_keys <= update_params, pt_keys - update_params
    # Required keys present (fails if board_id/scenario_id dropped).
    assert {"board_id", "spec_id", "scenario_id", "status"} <= pt_keys
    # follow_up move_card params cross-checked against move_card signature.
    fu_keys = set(na["follow_up"]["params"].keys())
    assert fu_keys <= move_params and {"board_id", "card_id", "status"} <= fu_keys

    ready = operational_flow_for_test_card(
        card_id="c1", board_id="b1", spec_id="s1", current_status="in_progress",
        linked_scenarios=[{"id": "ts1", "title": "A", "status": "passed",
                           "evidence": {"evidence_class": "automated_test_pointer",
                                        "test_file_path": "tests/test_x.py",
                                        "test_function": "test_a"}}],
    )
    rp_keys = set(ready["next_action"]["params"].keys())
    assert rp_keys <= move_params and {"board_id", "card_id", "status"} <= rp_keys


# ===========================================================================
# get_task_context integration — blocked test card
# ===========================================================================


@pytest.mark.asyncio
async def test_get_task_context_test_card_blocked_flow(db_factory):
    board_id, card_id = await _seed_test_card(db_factory, scenarios=[
        {"id": "ts1", "title": "Draft scenario", "given": "g", "when": "w", "then": "t",
         "status": "draft"},
        {"id": "ts2", "title": "Ready scenario", "given": "g", "when": "w", "then": "t",
         "status": "ready"},
    ])

    result = await _call("okto_pulse_get_task_context", board_id=board_id, card_id=card_id,
                         profile="full")
    flow = result["test_card_operational_flow"]
    assert flow["card_type"] == "test"
    assert flow["would_block_done"] is True
    assert {p["id"] for p in flow["pending_scenarios"]} == {"ts1", "ts2"}
    assert flow["required_tool"] == "okto_pulse_update_test_scenario_status"
    assert flow["follow_up_tool"] == "okto_pulse_move_card"
    assert flow["applies_to_task_validation"] is False


@pytest.mark.asyncio
async def test_get_task_context_test_card_ready_flow(db_factory):
    board_id, card_id = await _seed_test_card(db_factory, scenarios=[
        {"id": "ts1", "title": "Done scenario", "given": "g", "when": "w", "then": "t",
         "status": "passed", "evidence": {
             "evidence_class": "automated_test_pointer",
             "test_file_path": "tests/test_x.py",
             "test_function": "test_done",
         }},
    ])

    result = await _call("okto_pulse_get_task_context", board_id=board_id, card_id=card_id,
                         profile="summary")
    flow = result["test_card_operational_flow"]
    assert flow["would_block_done"] is False
    assert flow["pending_scenarios"] == []
    assert flow["next_action"]["tool"] == "okto_pulse_move_card"
    assert flow["next_action"]["params"]["status"] == "done"
    assert flow["next_action"]["params"]["board_id"] == board_id
    assert flow["linked_scenarios"][0]["evidence_present"] is True


@pytest.mark.asyncio
async def test_get_task_context_normal_card_has_no_flow_block(db_factory):
    board_id = _id("board")
    card_id = _id("card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4 imp3", owner_id=USER_ID))
        db.add(Card(id=card_id, board_id=board_id, title="normal card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()

    result = await _call("okto_pulse_get_task_context", board_id=board_id, card_id=card_id,
                         profile="full")
    # The block is test-card specific; a normal card must not carry it.
    assert "test_card_operational_flow" not in result
