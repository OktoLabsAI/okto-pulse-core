"""R5-TEST1 (card bdfe0b7b, AC1) — agent-facing MCP skip/no_action mutations fail
closed with an explicit BEFORE/AFTER proof that no state changed.

Scenarios ts_c9d10fb7 / ts_4eee7e88: through the REAL MCP boundary, each of the
four skip tools returns ``human_control_required`` (mutation_allowed=false,
state_changed=false), and a before/after snapshot proves ``skip_ambiguity_gate``
and the ``CognitiveConsolidationItemStore`` ledger are UNCHANGED. The allowed
``evaluate`` / ``create_learning`` branches stay agent-facing.

Teeth: each before/after assertion FAILS if the human-only guard is removed — the
tool would then mutate the flag / ledger and the AFTER state would differ from
BEFORE.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r5t1_"))

from okto_pulse.core.kg.bug_cognitive_closure import bug_cognitive_source_ref
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    compute_cognitive_item_id,
)
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Ideation, IdeationStatus

USER_ID = "r5-test1-user"
UUID_A = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
GEN = "gen-1"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = "mcp-agent"
        self.agent_name = "r5 test1 agent"
        self.permissions = set()


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


def _assert_fail_closed(out: dict) -> None:
    assert out["code"] == "human_control_required", out
    d = out["details"]
    assert d["mutation_allowed"] is False
    assert d["state_changed"] is False
    assert d["required_actor"] == "human"


def _seed_item(base_dir, board, source_ref, status):
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    path = store._record_path(board, GEN)
    path.parent.mkdir(parents=True, exist_ok=True)
    item = {
        "item_id": compute_cognitive_item_id(board, GEN, source_ref),
        "board_id": board, "kg_generation_id": GEN, "source_ref": source_ref,
        "artifact_type": source_ref.split(":", 1)[0], "status": status,
        "recorded_at": "2026-06-17T00:00:00+00:00",
    }
    if status == CognitiveItemStatus.SKIPPED.value:
        item["reason_code"] = "trivial_fix"
        item["outcome_type"] = "no_action_required"
    record = {"board_id": board, "kg_generation_id": GEN,
              "pending_count": 0, "pending_refs": [], "status": "complete",
              "recorded_at": "2026-06-17T00:00:00+00:00", "items": [item]}
    path.write_text(json.dumps(record), encoding="utf-8")
    return store


def _item_status(store, board, source_ref) -> str:
    iid = compute_cognitive_item_id(board, GEN, source_ref)
    return {i.item_id: i for i in store.list_items(board, GEN)}[iid].status


# ===========================================================================
# ts_c9d10fb7 — ambiguity gate skip MCP fails closed (before/after unchanged)
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_c9d10fb7_ambiguity_skip_mcp_fails_closed_no_state_change(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test1", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="i",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        await db.commit()

    # BEFORE
    async with db_factory() as db:
        before = (await db.get(Ideation, ideation_id)).skip_ambiguity_gate
    assert before is False

    out = await _call("okto_pulse_set_ideation_ambiguity_gate_skip",
                      board_id=board_id, ideation_id=ideation_id, skip_ambiguity_gate=True)
    _assert_fail_closed(out)

    # AFTER — unchanged (teeth: removing the guard would flip this to True).
    async with db_factory() as db:
        after = (await db.get(Ideation, ideation_id)).skip_ambiguity_gate
    assert after == before is False


# ===========================================================================
# ts_4eee7e88 — cognitive skip mutations (record / clear / evaluate_bug) fail closed
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_4eee7e88_record_cognitive_skip_mcp_fails_closed_ledger_unchanged(
    db_factory, _tmp_rebuild_dir
):
    board_id = _id("board")
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test1", owner_id=USER_ID))
        await db.commit()
    store = _seed_item(_tmp_rebuild_dir, board_id, source_ref, CognitiveItemStatus.PENDING.value)
    assert _item_status(store, board_id, source_ref) == CognitiveItemStatus.PENDING.value

    out = await _call("okto_pulse_kg_record_cognitive_skip",
                      board_id=board_id, source_ref=source_ref, reason_code="trivial_fix")
    _assert_fail_closed(out)

    # Ledger unchanged (teeth: removing the guard would flip PENDING -> SKIPPED).
    assert _item_status(store, board_id, source_ref) == CognitiveItemStatus.PENDING.value


@pytest.mark.asyncio
async def test_ts_4eee7e88_clear_cognitive_skip_mcp_fails_closed_ledger_unchanged(
    db_factory, _tmp_rebuild_dir
):
    board_id = _id("board")
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test1", owner_id=USER_ID))
        await db.commit()
    store = _seed_item(_tmp_rebuild_dir, board_id, source_ref, CognitiveItemStatus.SKIPPED.value)
    assert _item_status(store, board_id, source_ref) == CognitiveItemStatus.SKIPPED.value

    out = await _call("okto_pulse_kg_clear_cognitive_skip",
                      board_id=board_id, source_ref=source_ref)
    _assert_fail_closed(out)

    # Ledger unchanged (teeth: removing the guard would reopen SKIPPED -> PENDING).
    assert _item_status(store, board_id, source_ref) == CognitiveItemStatus.SKIPPED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["skip", "no_action"])
async def test_ts_4eee7e88_evaluate_bug_skip_actions_fail_closed_ledger_unchanged(
    db_factory, _tmp_rebuild_dir, action
):
    board_id = _id("board")
    source_ref = bug_cognitive_source_ref(UUID_A)
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test1", owner_id=USER_ID))
        await db.commit()
    # Seed a PENDING item the skip WOULD target — so removing the guard would mutate it.
    store = _seed_item(_tmp_rebuild_dir, board_id, source_ref, CognitiveItemStatus.PENDING.value)

    out = await _call("okto_pulse_kg_evaluate_bug_cognitive_closure",
                      board_id=board_id, bug_id=UUID_A,
                      evidence={"root_cause": "r"}, requested_action=action,
                      reason_code="trivial_fix")
    _assert_fail_closed(out)

    # Ledger unchanged (teeth: removing the guard would flip PENDING -> SKIPPED).
    assert _item_status(store, board_id, source_ref) == CognitiveItemStatus.PENDING.value


# ===========================================================================
# Allowed branches still agent-facing (not broken by the refusal)
# ===========================================================================


@pytest.mark.asyncio
async def test_evaluate_bug_evaluate_and_create_learning_not_refused(
    db_factory, _tmp_rebuild_dir
):
    board_id = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test1", owner_id=USER_ID))
        await db.commit()

    ev = await _call("okto_pulse_kg_evaluate_bug_cognitive_closure",
                     board_id=board_id, bug_id=UUID_A,
                     evidence={"root_cause": "r", "fix_narrative": "f"},
                     requested_action="evaluate")
    cl = await _call("okto_pulse_kg_evaluate_bug_cognitive_closure",
                     board_id=board_id, bug_id=UUID_A,
                     evidence={"root_cause": "r", "fix_narrative": "f"},
                     requested_action="create_learning")
    assert ev.get("code") != "human_control_required"
    assert cl.get("code") != "human_control_required"
    assert "status" in ev and "status" in cl
    # No ledger generation fabricated by the read-only branches.
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    assert store.latest_generation(board_id) is None
