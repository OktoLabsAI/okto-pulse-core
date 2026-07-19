"""R5-TEST3 (card d6f8ff2f) — human UI/REST controls are audited (AC3) and
technical blockers stay visible + blocking even with an active cognitive skip (AC5).

* ts_9455d0bf / AC3: the HUMAN path applies AND removes the ambiguity skip and the
  cognitive skip, with an audit trail (old/new value, user, timestamp, justification
  when applicable) and the persisted state changing accordingly.
* ts_85e18262 / AC5: an active cognitive skip is read-only context only — a DLQ /
  open CanonicalDebt for the same artifact stays VISIBLE in the read-model and
  BLOCKING in the verdict; the skip never marks the technical debt resolved/skipped.

Anti-test-theater: human writes run through the REAL service / REST endpoints with a
human actor and the audit is read back from the DB / ledger; the non-masking teeth
drive the REAL MCP evaluate/list tools over a seeded skip + open debt / DLQ.
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

from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWork
from sqlalchemy import select

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r5t3_"))

import okto_pulse.community.api.cognitive_action_center as ac_api
from okto_pulse.community.api.cognitive_action_center import (
    CognitiveClearRequest,
    CognitiveSkipRequest,
    clear_cognitive_skip_endpoint,
    record_cognitive_skip_endpoint,
)
from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    compute_cognitive_item_id,
    require_rebuild_audit_artifact_store,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    ConsolidationDeadLetter,
    Ideation,
    IdeationStatus,
)
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt
from okto_pulse.core.services.main import IdeationService

USER_ID = "r5-test3-human"
UUID_A = "aaaaaaaa-3333-4333-8333-aaaaaaaaaaaa"
GEN = "gen-1"
SKIP_ACTION = "ideation.ambiguity_gate_skip_updated"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


class _Ctx:
    def __init__(self):
        self.agent_id = "mcp-agent"
        self.agent_name = "r5 test3 agent"
        self.permissions = set()


async def _mcp(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


async def _skip_logs(db, ideation_id):
    rows = (await db.execute(
        select(ActivityLog).where(ActivityLog.action == SKIP_ACTION))).scalars().all()
    return [r for r in rows if (r.details or {}).get("ideation_id") == ideation_id]


def _seed_item(store, board, source_ref, status):
    item = {
        "item_id": compute_cognitive_item_id(board, GEN, source_ref),
        "board_id": board, "kg_generation_id": GEN, "source_ref": source_ref,
        "artifact_type": source_ref.split(":", 1)[0], "status": status,
        "recorded_at": "2026-06-17T00:00:00+00:00",
    }
    if status == CognitiveItemStatus.SKIPPED.value:
        item["reason_code"] = "duplicate_bug"
        item["outcome_type"] = "no_action_required"
    record = {"board_id": board, "kg_generation_id": GEN,
              "pending_count": 1 if status == CognitiveItemStatus.PENDING.value else 0,
              "pending_refs": [source_ref] if status == CognitiveItemStatus.PENDING.value else [],
              "status": "pending" if status == CognitiveItemStatus.PENDING.value else "complete",
              "recorded_at": "2026-06-17T00:00:00+00:00", "items": [item]}
    store.artifact_store.write_json_atomic(
        RebuildAuditKey(
            namespace="cognitive_pending",
            board_id=board,
            kg_generation_id=GEN,
        ),
        record,
    )


# ===========================================================================
# ts_9455d0bf / AC3 — human apply + remove, audited
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_9455d0bf_human_ambiguity_apply_and_remove_audited(db_factory):
    board_id, ideation_id = _id("board"), _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 test3", owner_id=USER_ID,
                     settings={"require_ideation_ambiguity_gate": True,
                               "max_ideation_ambiguity": 3}))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="i",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        await db.commit()

    async with db_factory() as db:
        await IdeationService(db).set_ambiguity_gate_skip(ideation_id, USER_ID, True, source="rest")
        await db.commit()
    async with db_factory() as db:
        res = await IdeationService(db).set_ambiguity_gate_skip(ideation_id, USER_ID, False, source="rest")
        await db.commit()
        assert res.skip_ambiguity_gate is False  # persisted removal

    async with db_factory() as db:
        logs = sorted(await _skip_logs(db, ideation_id), key=lambda r: r.created_at)
    assert [(g.details["old_value"], g.details["new_value"]) for g in logs] == [(False, True), (True, False)]
    for g in logs:
        assert g.actor_id == USER_ID and g.actor_type == "user"
        assert g.created_at is not None


@pytest.mark.asyncio
async def test_ts_9455d0bf_human_cognitive_apply_and_remove_audited(db_factory, tmp_path, monkeypatch):
    board, source_ref = _id("board"), f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 test3", owner_id=USER_ID))
        await db.commit()
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_item(store, board, source_ref, CognitiveItemStatus.PENDING.value)
    monkeypatch.setattr(ac_api, "build_default_readiness_service",
                        lambda: CognitiveReadinessService(store), raising=True)

    async with db_factory() as db:
        applied = await record_cognitive_skip_endpoint(
            board, CognitiveSkipRequest(source_ref=source_ref, reason_code="duplicate_bug",
                                        justification="dup of X", evidence_refs=["card:other"]),
            SQLAlchemyUnitOfWork(db), actor=USER_ID)
    assert applied["status"] == CognitiveItemStatus.SKIPPED.value
    assert applied["actor"] == USER_ID and applied["reason_code"] == "duplicate_bug"
    assert applied["justification"] == "dup of X" and applied["evidence_refs"] == ["card:other"]
    assert applied["updated_at"] is not None

    async with db_factory() as db:
        cleared = await clear_cognitive_skip_endpoint(
            board,
            CognitiveClearRequest(source_ref=source_ref),
            SQLAlchemyUnitOfWork(db),
            actor=USER_ID,
        )
    assert cleared["status"] == CognitiveItemStatus.PENDING.value
    assert cleared["actor"] == USER_ID and cleared["updated_at"] is not None
    # durable ledger reflects the reopen.
    iid = compute_cognitive_item_id(board, GEN, source_ref)
    assert {i.item_id: i for i in store.list_items(board, GEN)}[iid].status == CognitiveItemStatus.PENDING.value


# ===========================================================================
# ts_85e18262 / AC5 — technical blockers visible + blocking with active skip
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_85e18262_open_debt_visible_and_blocking_with_active_skip(db_factory):
    board, source_ref = _id("board"), f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 test3", owner_id=USER_ID))
        await db.commit()
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="canonical", canonical_state="blocked")
        await db.commit()
    # An ACTIVE cognitive skip exists for the SAME artifact (read-only context).
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    _seed_item(store, board, source_ref, CognitiveItemStatus.SKIPPED.value)

    # BLOCKING: the verdict is the technical tier, NOT ready/skip.
    verdict = await _mcp("okto_pulse_kg_evaluate_cognitive_readiness",
                         board_id=board, source_ref=source_ref)
    assert verdict["tier"] == "canonical_debt_open"
    assert verdict["blocking"] is True

    # VISIBLE: the open debt still surfaces in the read-model (not hidden by the skip).
    listed = await _mcp("okto_pulse_kg_list_cognitive_readiness_items",
                        board_id=board, signal="open_canonical_debt")
    debt_artifacts = {it["artifact_id"] for it in listed["items"]}
    assert any(UUID_A in a for a in debt_artifacts), listed
    assert all(it["blocking"] for it in listed["items"])


@pytest.mark.asyncio
async def test_ts_85e18262_dlq_visible_and_blocking_with_active_skip(db_factory):
    board, source_ref = _id("board"), f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 test3", owner_id=USER_ID))
        await db.flush()
        db.add(ConsolidationDeadLetter(
            id=_id("dlq"), board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}]))
        await db.commit()
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    _seed_item(store, board, source_ref, CognitiveItemStatus.SKIPPED.value)

    verdict = await _mcp("okto_pulse_kg_evaluate_cognitive_readiness",
                         board_id=board, source_ref=source_ref)
    assert verdict["tier"] == "technical_dlq"
    assert verdict["blocking"] is True

    listed = await _mcp("okto_pulse_kg_list_cognitive_readiness_items",
                        board_id=board, signal="dlq")
    assert listed["items"], listed
    assert all(it["blocking"] for it in listed["items"])


@pytest.mark.asyncio
async def test_ts_85e18262_human_skip_cannot_mark_debt_resolved(db_factory, tmp_path, monkeypatch):
    # Even the HUMAN skip path cannot skip an artifact with open technical debt — the
    # blocker is never marked resolved/skipped/ignored.
    from fastapi import HTTPException

    board, source_ref = _id("board"), f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 test3", owner_id=USER_ID))
        await db.commit()
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="canonical", canonical_state="pending")
        await db.commit()
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_item(store, board, source_ref, CognitiveItemStatus.PENDING.value)
    monkeypatch.setattr(ac_api, "build_default_readiness_service",
                        lambda: CognitiveReadinessService(store), raising=True)

    async with db_factory() as db:
        with pytest.raises(HTTPException) as exc:
            await record_cognitive_skip_endpoint(
                board, CognitiveSkipRequest(source_ref=source_ref, reason_code="duplicate_bug"),
                SQLAlchemyUnitOfWork(db), actor=USER_ID)
    assert exc.value.status_code == 409
    assert exc.value.detail["error"] == "technical_debt_cannot_be_skipped"
    # the item was NOT skipped — debt not masked.
    iid = compute_cognitive_item_id(board, GEN, source_ref)
    assert {i.item_id: i for i in store.list_items(board, GEN)}[iid].status == CognitiveItemStatus.PENDING.value
