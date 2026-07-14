"""R4-TEST3 (card 2e3c0b1a) — readiness/gate transparency policy behavior, proven
through the REAL MCP context tools (R4-IMP4 surface).

Maps the spec R4 scenarios:
* ts_2828f2d6: get_spec_context / get_task_context expose enforcement_active,
  enforcement_mode (advisory|enforced), would_block_done and the central
  CognitiveReadinessService reason — coherent with the runtime policy. Advisory
  never blocks done; enforced blocks ONLY when the tier is in GATE_BLOCKING_TIERS.
* ts_6b3a8048: the transparency creates NO artificial cognitive pending, does NOT
  make cognitive blocking by default, does NOT change the state machine and does
  NOT auto-promote a spec/card.

Anti-test-theater: the blocking verdict is a REAL one — an OPEN ``CanonicalDebt``
for the card's artifact (a technical tier that blocks regardless of the task/test
advisory carve-out). ``would_block_done`` then flips solely with the board's
two-key enforcement policy (board ``cognitive_readiness_policy=blocking`` + global
flag), exactly like the done-gate. No verdict is mocked.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import os
import sys
import tempfile
import uuid
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r4t3_"))

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
)
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

USER_ID = "user-r4-test3"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r4 test3"
        self.permissions = set()


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    # Isolate the cognitive ledger base dir so we can assert no pending is created.
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


def _enable_enforcement(monkeypatch):
    """Two-key rollout: board policy=blocking (per-board) + global flag (here)."""
    from okto_pulse.core.infra import config as config_mod

    monkeypatch.setattr(
        config_mod.get_settings(), "cognitive_readiness_blocking_enabled", True,
        raising=False,
    )


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _seed(db_factory, *, board_settings=None, spec_status=SpecStatus.IN_PROGRESS,
                with_open_debt=False):
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r4 test3", owner_id=USER_ID,
                     settings=board_settings or {}))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=spec_status,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()
        if with_open_debt:
            # A normal card resolves to source_ref "task:<id>" (see
            # resolve_cognitive_source_refs + _card_cognitive_entity_type); the debt
            # ref must normalize to the SAME artifact_id, so use the task: prefix.
            await upsert_canonical_debt(
                db, board_id=board_id, artifact_type="card", artifact_id=card_id,
                source_ref=f"task:{card_id}", content_hash="h",
                target_status="canonical", canonical_state="pending",
                failure_reason="open debt for readiness test",
            )
            await db.commit()
    return board_id, spec_id, card_id


# ===========================================================================
# ts_2828f2d6 — enforcement coherence (advisory vs enforced) over a REAL verdict
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_2828f2d6_advisory_policy_blocking_verdict_does_not_block_done(db_factory):
    # Policy OFF (default board): a genuinely blocking verdict stays ADVISORY.
    board_id, spec_id, card_id = await _seed(db_factory, with_open_debt=True)

    result = await _call("okto_pulse_get_task_context", board_id=board_id,
                         card_id=card_id, profile="full")
    gr = result["gate_readiness"]
    assert gr["cognitive_enforcement_active"] is False
    assert gr["cognitive_enforcement_mode"] == "advisory"
    cog = gr["cognitive_readiness"]
    assert cog is not None, gr
    # REAL verdict from the central service: an open canonical debt is a blocking
    # technical tier.
    assert cog["blocking"] is True
    assert cog["tier"] == "canonical_debt_open"
    assert cog["readiness_effect"] == "blocking_technical"
    # ... but with the policy OFF it does NOT block done (enforcement-aware).
    assert cog["would_block_done"] is False
    assert gr["active_gate"] is None
    assert gr["mutation_allowed"] is False
    assert gr["consistency"]["mismatch"] is False


@pytest.mark.asyncio
async def test_ts_2828f2d6_enforced_policy_blocking_tier_blocks_done(db_factory, monkeypatch):
    _enable_enforcement(monkeypatch)
    board_id, spec_id, card_id = await _seed(
        db_factory, board_settings={"cognitive_readiness_policy": "blocking"},
        with_open_debt=True,
    )

    result = await _call("okto_pulse_get_task_context", board_id=board_id,
                         card_id=card_id, profile="full")
    gr = result["gate_readiness"]
    assert gr["cognitive_enforcement_active"] is True
    assert gr["cognitive_enforcement_mode"] == "enforced"
    cog = gr["cognitive_readiness"]
    assert cog["blocking"] is True
    assert cog["tier"] == "canonical_debt_open"
    # Enforced + blocking tier -> would_block_done True.
    assert cog["would_block_done"] is True
    gate = gr["active_gate"]
    assert gate is not None
    assert gate["gate_type"] == "cognitive_readiness"
    assert gate["would_block_done"] is True
    assert gate["blocked_transition"] == "in_progress->done"
    assert gate["required_status"] == "done"
    # The actionable tool is EVALUATE — never a skip/no_action (codex restriction).
    assert gate["required_tool"] == "okto_pulse_kg_evaluate_cognitive_readiness"
    assert "skip" not in gate["required_tool"] and "no_action" not in gate["required_tool"]
    assert gr["consistency"]["mismatch"] is False


@pytest.mark.asyncio
async def test_ts_2828f2d6_enforced_policy_clean_card_does_not_block(db_factory, monkeypatch):
    # Enforced policy, but NO blocking tier (no debt) -> enforced does NOT block.
    _enable_enforcement(monkeypatch)
    board_id, spec_id, card_id = await _seed(
        db_factory, board_settings={"cognitive_readiness_policy": "blocking"},
        with_open_debt=False,
    )

    result = await _call("okto_pulse_get_task_context", board_id=board_id,
                         card_id=card_id, profile="full")
    gr = result["gate_readiness"]
    assert gr["cognitive_enforcement_active"] is True
    cog = gr["cognitive_readiness"]
    # A clean card carries no blocking tier; would_block_done False even when enforced.
    if cog is not None:
        assert cog["tier"] not in ("technical_dlq", "canonical_debt_open", "skip_expired")
        assert cog["would_block_done"] is False
    assert gr["active_gate"] is None
    assert gr["consistency"]["mismatch"] is False


# ===========================================================================
# ts_6b3a8048 — no artificial pending / no blocking default / no state change /
# no auto-promotion / spec does not aggregate card cognitive
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_6b3a8048_context_calls_no_pending_no_status_change(
    db_factory, _tmp_rebuild_dir
):
    board_id, spec_id, card_id = await _seed(
        db_factory, spec_status=SpecStatus.APPROVED, with_open_debt=True,
    )
    # Same base_dir the readiness service uses (resolves OKTO_PULSE_REBUILD_BASE_DIR).
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    # No consolidation has run -> no ledger generation for this board.
    assert store.latest_generation(board_id) is None

    spec_ctx = await _call("okto_pulse_get_spec_context", board_id=board_id,
                           spec_id=spec_id, profile="full")
    task_ctx = await _call("okto_pulse_get_task_context", board_id=board_id,
                           card_id=card_id, profile="full")

    # Blocking NOT default: even with an open debt, advisory by default.
    assert spec_ctx["gate_readiness"]["cognitive_enforcement_active"] is False
    assert task_ctx["gate_readiness"]["cognitive_enforcement_active"] is False
    # No artificial cognitive pending created by reading context.
    assert store.latest_generation(board_id) is None
    # No state-machine change / auto-promotion.
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        card = await db.get(Card, card_id)
    assert spec.status == SpecStatus.APPROVED
    assert card.status == CardStatus.IN_PROGRESS


@pytest.mark.asyncio
async def test_ts_6b3a8048_spec_context_does_not_aggregate_card_cognitive(db_factory, monkeypatch):
    # Even under enforced policy with a blocking card, the SPEC context never
    # aggregates a per-card cognitive verdict (codex Q1).
    _enable_enforcement(monkeypatch)
    board_id, spec_id, card_id = await _seed(
        db_factory, board_settings={"cognitive_readiness_policy": "blocking"},
        with_open_debt=True,
    )

    spec_ctx = await _call("okto_pulse_get_spec_context", board_id=board_id,
                           spec_id=spec_id, profile="full")
    gr = spec_ctx["gate_readiness"]
    assert "cognitive_readiness" not in gr
    serialized = json.dumps(gr)
    assert "readiness_effect" not in serialized
    assert "would_block_done" not in serialized
    # Enforcement posture is still surfaced, and the block is read-only.
    assert gr["cognitive_enforcement_active"] is True
    assert gr["mutation_allowed"] is False
    assert all("skip" not in json.dumps(g) for g in gr["active_gates"])
    assert gr["consistency"]["mismatch"] is False
