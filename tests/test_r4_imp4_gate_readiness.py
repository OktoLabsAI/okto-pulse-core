"""R4-IMP4 (card a0e1d167) — read-only gate/readiness block in get_spec_context
and get_task_context.

Both context tools expose a ``gate_readiness`` block that surfaces the SAME fields
the gates enforce (parity by construction): board cognitive enforcement posture,
the card's OWN cognitive verdict (enforcement-aware ``would_block_done``), the
single active transition gate + its required tool, and an auditable ``consistency``
self-check. Spec context does NOT aggregate per-card cognitive verdicts (codex Q1).

Anti-test-theater:
* the pure builders are unit-tested across the advisory/blocking/enforcement matrix
  (proving advisory != blocking and enforced+blocking => would_block_done);
* the block is also exercised through the REAL ``okto_pulse_get_spec_context`` /
  ``okto_pulse_get_task_context`` tools over a real board/spec/card, and the test
  card's ``active_gate.would_block_done`` is cross-checked against the live
  ``test_card_operational_flow`` (single source).
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
    GATE_COGNITIVE_READINESS,
    GATE_SPEC_VALIDATION,
    GATE_TEST_CARD_COMPLETION,
    operational_flow_for_test_card,
    spec_gate_readiness,
    task_gate_readiness,
)

USER_ID = "user-r4-imp4"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r4 imp4 tester"
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


# ===========================================================================
# Pure builder — task_gate_readiness: the advisory / blocking / enforcement matrix
# ===========================================================================


def _cognitive_verdict(*, tier, blocking, would_block_done):
    return {
        "source_ref": "card:c1",
        "readiness_effect": "block" if would_block_done else "advise",
        "readiness_signal": "consolidation_pending",
        "tier": tier,
        "reason_code": "pending_consolidation",
        "blocking": blocking,
        "revisit_at": None,
        "would_block_done": would_block_done,
    }


def test_task_advisory_when_enforcement_off_does_not_block_or_gate():
    # A blocking verdict (blocking=True) but the board policy is OFF -> the verdict
    # is ADVISORY: would_block_done is False and NO cognitive gate is surfaced.
    verdict = _cognitive_verdict(tier="consolidation", blocking=True, would_block_done=False)
    block = task_gate_readiness(
        card_status="in_progress",
        cognitive_enforcement_active=False,
        cognitive_verdict=verdict,
        operational_flow=None,
    )
    assert block["cognitive_enforcement_active"] is False
    assert block["cognitive_enforcement_mode"] == "advisory"
    # blocking (raw) is True but would_block_done (enforcement-aware) is False:
    assert block["cognitive_readiness"]["blocking"] is True
    assert block["cognitive_readiness"]["would_block_done"] is False
    assert block["active_gate"] is None  # advisory != blocking
    assert block["mutation_allowed"] is False
    assert block["consistency"]["mismatch"] is False


def test_task_enforced_and_blocking_blocks_done_and_surfaces_cognitive_gate():
    verdict = _cognitive_verdict(tier="consolidation", blocking=True, would_block_done=True)
    block = task_gate_readiness(
        card_status="in_progress",
        cognitive_enforcement_active=True,
        cognitive_verdict=verdict,
        operational_flow=None,
    )
    assert block["cognitive_enforcement_mode"] == "enforced"
    assert block["cognitive_readiness"]["would_block_done"] is True
    gate = block["active_gate"]
    assert gate is not None
    assert gate["gate_type"] == GATE_COGNITIVE_READINESS
    assert gate["would_block_done"] is True
    assert gate["blocked_transition"] == "in_progress->done"
    assert gate["required_status"] == "done"
    # The required tool is the EVALUATE tool — never a skip tool (codex restriction).
    assert gate["required_tool"] == "okto_pulse_kg_evaluate_cognitive_readiness"
    assert "skip" not in gate["required_tool"]
    assert block["consistency"]["mismatch"] is False


def test_task_no_verdict_means_no_gate():
    block = task_gate_readiness(
        card_status="in_progress",
        cognitive_enforcement_active=True,
        cognitive_verdict=None,
        operational_flow=None,
    )
    assert block["cognitive_readiness"] is None
    assert block["active_gate"] is None
    assert block["consistency"]["mismatch"] is False


def test_task_test_card_reuses_operational_flow_single_source():
    flow = operational_flow_for_test_card(
        card_id="c1", board_id="b1", spec_id="s1", current_status="in_progress",
        linked_scenarios=[{"id": "ts1", "title": "A", "status": "draft"}],
    )
    block = task_gate_readiness(
        card_status="in_progress",
        cognitive_enforcement_active=False,
        cognitive_verdict=None,
        operational_flow=flow,
    )
    gate = block["active_gate"]
    assert gate["gate_type"] == GATE_TEST_CARD_COMPLETION
    # Derived from the SAME operational flow — must match exactly.
    assert gate["would_block_done"] == flow["would_block_done"] is True
    assert gate["required_tool"] == flow["required_tool"]
    assert gate["follow_up_tool"] == flow["follow_up_tool"]
    # blocked_transition derived from the flow's own current_status; required_status
    # mirrors the R4-IMP1 envelope shape.
    assert gate["blocked_transition"] == "in_progress->done"
    assert gate["required_status"] == "done"
    assert block["consistency"]["mismatch"] is False


def test_task_test_card_blocked_transition_none_without_status():
    # If the operational flow carries no current_status, blocked_transition is None
    # (acceptable per codex) — but required_status still anchors the gate.
    flow = operational_flow_for_test_card(
        card_id="c1", board_id="b1", spec_id="s1", current_status=None,
        linked_scenarios=[{"id": "ts1", "title": "A", "status": "draft"}],
    )
    block = task_gate_readiness(
        card_status="in_progress",
        cognitive_enforcement_active=False,
        cognitive_verdict=None,
        operational_flow=flow,
    )
    gate = block["active_gate"]
    assert gate["blocked_transition"] is None
    assert gate["required_status"] == "done"
    assert block["consistency"]["mismatch"] is False


def test_task_consistency_detects_non_enforcement_aware_would_block_done():
    # An impossible-by-construction verdict (would_block_done True while the policy is
    # OFF) MUST be flagged as a mismatch with expected/observed/reason — this is the
    # enforcement-awareness invariant codex required.
    bad = _cognitive_verdict(tier="consolidation", blocking=True, would_block_done=True)
    block = task_gate_readiness(
        card_status="in_progress",
        cognitive_enforcement_active=False,  # policy OFF ...
        cognitive_verdict=bad,               # ... but verdict claims it blocks done
        operational_flow=None,
    )
    c = block["consistency"]
    assert c["mismatch"] is True
    assert c["expected"]["would_block_done"] is False
    assert c["observed"]["would_block_done"] is True
    assert "enforcement-aware" in c["reason"]


# ===========================================================================
# Pure builder — spec_gate_readiness: spec_validation gate + NO cognitive aggregation
# ===========================================================================


def test_spec_validation_gate_surfaced_when_approved_and_required():
    block = spec_gate_readiness(
        spec_id="s1",
        spec_status="approved",
        require_spec_validation=True,
        cognitive_enforcement_active=False,
    )
    gates = block["active_gates"]
    assert len(gates) == 1
    g = gates[0]
    assert g["gate_type"] == GATE_SPEC_VALIDATION
    assert g["blocked_transition"] == "approved->validated"
    assert g["required_tool"] == "okto_pulse_submit_spec_validation"
    assert g["follow_up_tool"] == "okto_pulse_move_spec"
    # Q1: spec context never carries a per-card cognitive verdict.
    assert "cognitive_readiness" not in block
    assert "aggregate" in block["note"].lower() or "does not" in block["note"].lower()
    assert block["consistency"]["mismatch"] is False


def test_spec_validation_gate_absent_when_not_required():
    block = spec_gate_readiness(
        spec_id="s1",
        spec_status="approved",
        require_spec_validation=False,
        cognitive_enforcement_active=True,
    )
    assert block["active_gates"] == []
    assert block["cognitive_enforcement_mode"] == "enforced"
    assert block["consistency"]["mismatch"] is False


def test_spec_validation_gate_absent_when_not_approved():
    for status in ("draft", "review", "validated", "in_progress", "done"):
        block = spec_gate_readiness(
            spec_id="s1",
            spec_status=status,
            require_spec_validation=True,
            cognitive_enforcement_active=False,
        )
        assert block["active_gates"] == [], status


def test_spec_does_not_aggregate_card_cognitive_verdicts():
    # No field/path in the spec block ever exposes a per-card cognitive verdict.
    block = spec_gate_readiness(
        spec_id="s1",
        spec_status="in_progress",
        require_spec_validation=True,
        cognitive_enforcement_active=True,
    )
    serialized = json.dumps(block)
    assert "readiness_effect" not in serialized
    assert "would_block_done" not in serialized
    assert block["cognitive_enforcement_active"] is True


# ===========================================================================
# Integration — through the REAL MCP context tools
# ===========================================================================


async def _seed_spec(db_factory, *, status: str, settings=None):
    board_id = _id("board")
    spec_id = _id("spec")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4 imp4", owner_id=USER_ID, settings=settings or {}))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec",
                    status=SpecStatus(status), created_by=USER_ID,
                    functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[]))
        await db.commit()
    return board_id, spec_id


@pytest.mark.asyncio
async def test_get_spec_context_exposes_spec_validation_gate(db_factory):
    board_id, spec_id = await _seed_spec(db_factory, status="approved")
    result = await _call("okto_pulse_get_spec_context", board_id=board_id,
                         spec_id=spec_id, profile="full")
    gr = result["gate_readiness"]
    assert gr["mutation_allowed"] is False
    assert [g["gate_type"] for g in gr["active_gates"]] == [GATE_SPEC_VALIDATION]
    assert gr["active_gates"][0]["required_tool"] == "okto_pulse_submit_spec_validation"
    # Q1: no per-card cognitive aggregation in the spec context.
    assert "cognitive_readiness" not in gr
    assert gr["consistency"]["mismatch"] is False


@pytest.mark.asyncio
async def test_get_spec_context_no_gate_when_validation_disabled(db_factory):
    board_id, spec_id = await _seed_spec(
        db_factory, status="approved", settings={"require_spec_validation": False},
    )
    result = await _call("okto_pulse_get_spec_context", board_id=board_id,
                         spec_id=spec_id, profile="summary")
    assert result["gate_readiness"]["active_gates"] == []


@pytest.mark.asyncio
async def test_get_task_context_test_card_gate_matches_operational_flow(db_factory):
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")
    scenarios = [
        {"id": "ts1", "title": "Draft", "given": "g", "when": "w", "then": "t",
         "status": "draft"},
    ]
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4 imp4", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec",
                    status=SpecStatus.IN_PROGRESS, created_by=USER_ID,
                    functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=scenarios, business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="test card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.TEST,
                    created_by=USER_ID, test_scenario_ids=["ts1"]))
        await db.commit()

    result = await _call("okto_pulse_get_task_context", board_id=board_id,
                         card_id=card_id, profile="full")
    gr = result["gate_readiness"]
    flow = result["test_card_operational_flow"]
    # Single source: the gate's would_block_done MUST equal the operational flow's.
    assert gr["active_gate"]["gate_type"] == GATE_TEST_CARD_COMPLETION
    assert gr["active_gate"]["would_block_done"] == flow["would_block_done"] is True
    assert gr["active_gate"]["required_tool"] == flow["required_tool"]
    # blocked_transition derived from the card's live status in the real tool payload.
    assert gr["active_gate"]["blocked_transition"] == "in_progress->done"
    assert gr["active_gate"]["required_status"] == "done"
    assert gr["mutation_allowed"] is False
    assert gr["consistency"]["mismatch"] is False


@pytest.mark.asyncio
async def test_get_task_context_normal_card_advisory_by_default(db_factory):
    board_id = _id("board")
    card_id = _id("card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4 imp4", owner_id=USER_ID))
        db.add(Card(id=card_id, board_id=board_id, title="normal card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()

    result = await _call("okto_pulse_get_task_context", board_id=board_id,
                         card_id=card_id, profile="full")
    gr = result["gate_readiness"]
    # Cognitive readiness defaults to advisory for a fresh board (policy off).
    assert gr["cognitive_enforcement_active"] is False
    assert gr["cognitive_enforcement_mode"] == "advisory"
    # No test-card flow for a normal card.
    assert "test_card_operational_flow" not in result
    assert gr["consistency"]["mismatch"] is False
