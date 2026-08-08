"""R5-IMP1 (card e9a6b251) — agent-facing MCP skip / no_action mutations are
HUMAN-only and fail closed.

The four skip-mutation MCP tools refuse with ``human_control_required`` BEFORE any
write-path call, so no ``skip_ambiguity_gate`` / cognitive ledger / override is
touched (``mutation_allowed=false``, ``state_changed=false``,
``required_surface='ui|human_rest'``, ``required_actor='human'``). The
``evaluate_bug_cognitive_closure`` escape hatch is blocked ONLY for
``requested_action`` in {skip, no_action}; ``evaluate`` / ``create_learning`` (which
never persist a skip) stay agent-facing.

Anti-test-theater: every refusal is driven through the REAL MCP tool, and the
"no write" teeth read the actual DB / ledger AFTER the refused call.
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
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r5i1_"))

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    compute_cognitive_item_id,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Ideation

USER_ID = "user-r5-imp1"
UUID_A = "11111111-1111-4111-8111-111111111111"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = "mcp-agent"
        self.agent_name = "r5 imp1 agent"
        self.permissions = ["*"]


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


def _seed_item(base_dir, board, gen, *, source_ref, status):
    """Write a generation record with one cognitive ledger item at ``status``."""
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    item = {
        "item_id": compute_cognitive_item_id(board, gen, source_ref),
        "board_id": board, "kg_generation_id": gen, "source_ref": source_ref,
        "artifact_type": source_ref.split(":", 1)[0], "status": status,
        "recorded_at": "2026-06-17T00:00:00+00:00",
    }
    if status == CognitiveItemStatus.SKIPPED.value:
        item["reason_code"] = "trivial_fix"
        item["outcome_type"] = "no_action_required"
    record = {"board_id": board, "kg_generation_id": gen,
              "pending_count": 0, "pending_refs": [], "status": "complete",
              "recorded_at": "2026-06-17T00:00:00+00:00", "items": [item]}
    store.artifact_store.write_json_atomic(
        RebuildAuditKey(
            namespace="cognitive_pending",
            board_id=board,
            kg_generation_id=gen,
        ),
        record,
    )
    return store


def _assert_human_control(out: dict, *, blocked_tool: str, blocked_action_contains: str = ""):
    assert out["code"] == "human_control_required", out
    assert out["error"] == "human_control_required"
    d = out["details"]
    assert d["mutation_allowed"] is False
    assert d["state_changed"] is False
    assert d["required_surface"] == "ui|human_rest"
    assert d["required_actor"] == "human"
    assert d["blocked_tool"] == blocked_tool
    if blocked_action_contains:
        assert blocked_action_contains in d["blocked_action"]
    # The refusal must NOT instruct the agent to apply a skip.
    assert "skip" not in out["message"].lower() or "human" in out["message"].lower()


# ===========================================================================
# 1. ambiguity-gate skip — human-only, skip_ambiguity_gate untouched
# ===========================================================================


@pytest.mark.asyncio
async def test_ambiguity_gate_skip_mcp_is_human_only(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp1", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="idea",
                        created_by=USER_ID))
        await db.commit()

    out = await _call("okto_pulse_set_ideation_ambiguity_gate_skip",
                      board_id=board_id, ideation_id=ideation_id,
                      skip_ambiguity_gate=True)
    _assert_human_control(out, blocked_tool="okto_pulse_set_ideation_ambiguity_gate_skip",
                          blocked_action_contains="ambiguity")
    assert out["details"]["target_ref"] == f"ideation:{ideation_id}"

    # TEETH: skip_ambiguity_gate was NOT mutated.
    async with db_factory() as db:
        ideation = await db.get(Ideation, ideation_id)
    assert ideation.skip_ambiguity_gate is False


# ===========================================================================
# 2. record cognitive skip — human-only, ledger untouched
# ===========================================================================


@pytest.mark.asyncio
async def test_record_cognitive_skip_mcp_is_human_only(db_factory, _tmp_rebuild_dir):
    board_id, gen = _id("board"), "gen-1"
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp1", owner_id=USER_ID))
        await db.commit()
    store = _seed_item(_tmp_rebuild_dir, board_id, gen, source_ref=source_ref,
                       status=CognitiveItemStatus.PENDING.value)

    out = await _call("okto_pulse_kg_record_cognitive_skip",
                      board_id=board_id, source_ref=source_ref, reason_code="trivial_fix")
    _assert_human_control(out, blocked_tool="okto_pulse_kg_record_cognitive_skip")
    assert out["details"]["target_ref"] == source_ref

    # TEETH: the ledger item is untouched — still PENDING, never skipped.
    iid = compute_cognitive_item_id(board_id, gen, source_ref)
    persisted = {i.item_id: i for i in store.list_items(board_id, gen)}[iid]
    assert persisted.status == CognitiveItemStatus.PENDING.value


# ===========================================================================
# 3. clear cognitive skip — human-only, ledger untouched
# ===========================================================================


@pytest.mark.asyncio
async def test_clear_cognitive_skip_mcp_is_human_only(db_factory, _tmp_rebuild_dir):
    board_id, gen = _id("board"), "gen-1"
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp1", owner_id=USER_ID))
        await db.commit()
    store = _seed_item(_tmp_rebuild_dir, board_id, gen, source_ref=source_ref,
                       status=CognitiveItemStatus.SKIPPED.value)

    out = await _call("okto_pulse_kg_clear_cognitive_skip",
                      board_id=board_id, source_ref=source_ref)
    _assert_human_control(out, blocked_tool="okto_pulse_kg_clear_cognitive_skip")

    # TEETH: the skipped item was NOT reopened.
    iid = compute_cognitive_item_id(board_id, gen, source_ref)
    persisted = {i.item_id: i for i in store.list_items(board_id, gen)}[iid]
    assert persisted.status == CognitiveItemStatus.SKIPPED.value


# ===========================================================================
# 4. evaluate_bug escape hatch — skip / no_action are human-only
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["skip", "no_action"])
async def test_evaluate_bug_skip_actions_are_human_only(db_factory, _tmp_rebuild_dir, action):
    board_id = _id("board")
    bug_id = UUID_A
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp1", owner_id=USER_ID))
        await db.commit()

    out = await _call("okto_pulse_kg_evaluate_bug_cognitive_closure",
                      board_id=board_id, bug_id=bug_id,
                      evidence={"root_cause": "rc", "fix_narrative": "fix"},
                      requested_action=action, reason_code="trivial_fix")
    _assert_human_control(out, blocked_tool="okto_pulse_kg_evaluate_bug_cognitive_closure",
                          blocked_action_contains=action)
    assert out["details"]["target_ref"] == f"bug:{bug_id}"

    # TEETH: no cognitive generation/ledger was created by the refused mutation.
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    assert store.latest_generation(board_id) is None


# ===========================================================================
# 5. evaluate_bug read-only branches stay agent-facing (no skip persisted)
# ===========================================================================


@pytest.mark.asyncio
async def test_evaluate_bug_evaluate_branch_stays_agent_facing(db_factory, _tmp_rebuild_dir):
    board_id = _id("board")
    bug_id = UUID_A
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp1", owner_id=USER_ID))
        await db.commit()

    out = await _call("okto_pulse_kg_evaluate_bug_cognitive_closure",
                      board_id=board_id, bug_id=bug_id,
                      evidence={"root_cause": "rc", "fix_narrative": "fix"},
                      requested_action="evaluate")
    # Not refused — evaluation stays agent-facing.  Without the edition-owned
    # canonical context assembler this test harness now fails closed instead of
    # treating caller evidence as the product record.
    assert out.get("code") != "human_control_required"
    assert out["status"] in {
        "bug_cognitive_context_unavailable",
        "bug_source_not_found",
    }
    assert out["blocking"] is True
    assert out["evidence_readiness"]["context_verified"] is False
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    assert store.latest_generation(board_id) is None


@pytest.mark.asyncio
async def test_evaluate_bug_create_learning_branch_stays_agent_facing(db_factory, _tmp_rebuild_dir):
    board_id = _id("board")
    bug_id = UUID_A
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp1", owner_id=USER_ID))
        await db.commit()

    out = await _call("okto_pulse_kg_evaluate_bug_cognitive_closure",
                      board_id=board_id, bug_id=bug_id,
                      evidence={"root_cause": "rc", "fix_narrative": "fix"},
                      requested_action="create_learning")
    # Not refused — create_learning never persists a skip/no_action.
    assert out.get("code") != "human_control_required"
    assert "status" in out
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    assert store.latest_generation(board_id) is None
