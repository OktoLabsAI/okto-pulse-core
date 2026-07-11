"""R5-IMP3 (card 11409761) — the HUMAN UI/REST controls to apply AND remove a
skip (ambiguity gate + cognitive consolidation) are preserved and audited, while
the agent-facing MCP surface keeps refusing (R5-IMP1 contract).

Audit requirements (user, gate, old/new value, timestamp, justification when
applicable):
  * ambiguity gate skip -> ActivityLog ``ideation.ambiguity_gate_skip_updated``
    carries actor_id + source + old_value/new_value, with a created_at timestamp,
    on BOTH apply (False->True) and remove (True->False);
  * cognitive skip -> the central ledger item carries actor + reason_code +
    justification + evidence_refs + updated_at and the status transition
    (PENDING->SKIPPED on apply, SKIPPED->PENDING on remove).

Anti-test-theater: the human flows run through the REAL service / REST endpoints
(actor_is_human=True) and the audit is read back from the DB / ledger; the
coexistence teeth drive the REAL MCP tools and assert they still refuse.
"""

from __future__ import annotations

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
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r5i3_"))

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
)
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import ActivityLog, Board, Ideation, IdeationStatus
from okto_pulse.core.services.main import IdeationService

USER_ID = "r5-imp3-human"
UUID_A = "aaaaaaaa-3333-4333-8333-aaaaaaaaaaaa"
SKIP_ACTION = "ideation.ambiguity_gate_skip_updated"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


async def _skip_logs(db, ideation_id: str) -> list[ActivityLog]:
    rows = (await db.execute(
        select(ActivityLog).where(ActivityLog.action == SKIP_ACTION)
    )).scalars().all()
    return [r for r in rows if (r.details or {}).get("ideation_id") == ideation_id]


# ===========================================================================
# Ambiguity gate skip — human apply AND remove, both audited
# ===========================================================================


@pytest.mark.asyncio
async def test_ambiguity_human_apply_and_remove_are_audited(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp3", owner_id=USER_ID,
                     settings={"require_ideation_ambiguity_gate": True,
                               "max_ideation_ambiguity": 3}))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="idea",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        await db.commit()

    # APPLY (False -> True) via the human service path.
    async with db_factory() as db:
        await IdeationService(db).set_ambiguity_gate_skip(
            ideation_id, USER_ID, True, source="rest",
        )
        await db.commit()
    # REMOVE (True -> False) — the un-skip must also be audited.
    async with db_factory() as db:
        result = await IdeationService(db).set_ambiguity_gate_skip(
            ideation_id, USER_ID, False, source="rest",
        )
        await db.commit()
        assert result.skip_ambiguity_gate is False

    async with db_factory() as db:
        logs = sorted(await _skip_logs(db, ideation_id), key=lambda r: r.created_at)
    assert len(logs) == 2, logs
    apply_log, remove_log = logs
    # APPLY audit: user, gate (action), old/new, timestamp.
    assert apply_log.action == SKIP_ACTION
    assert apply_log.actor_id == USER_ID and apply_log.actor_type == "user"
    assert apply_log.details["old_value"] is False
    assert apply_log.details["new_value"] is True
    assert apply_log.details["source"] == "rest"
    assert apply_log.created_at is not None
    # REMOVE audit: the un-skip carries old=True -> new=False.
    assert remove_log.details["old_value"] is True
    assert remove_log.details["new_value"] is False
    assert remove_log.actor_id == USER_ID
    assert remove_log.created_at is not None


@pytest.mark.asyncio
async def test_ambiguity_rest_endpoint_is_human_authorized(db_factory):
    # The human REST endpoint stays authorized (R5-IMP1 only blocks the agent MCP).
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okto_pulse.community.api.ideations import router as ideations_router
    from okto_pulse.core.infra import auth as _auth_mod
    from okto_pulse.core.infra.database import get_db, get_session_factory

    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp3", owner_id=USER_ID,
                     settings={"require_ideation_ambiguity_gate": True,
                               "max_ideation_ambiguity": 3}))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="idea",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        await db.commit()

    app = FastAPI()
    app.include_router(ideations_router, prefix="/api/v1")
    df = get_session_factory()

    async def _odb():
        async with df() as session:
            yield session

    app.dependency_overrides[get_db] = _odb
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    client = TestClient(app)

    resp = client.patch(f"/api/v1/ideations/{ideation_id}/ambiguity-gate-skip",
                        json={"skip_ambiguity_gate": True})
    assert resp.status_code == 200
    assert resp.json()["skip_ambiguity_gate"] is True


# ===========================================================================
# Cognitive skip — human REST apply AND remove, audited via the central ledger
# ===========================================================================


def _seed_pending(store, board, gen, source_ref):
    path = store._record_path(board, gen)
    path.parent.mkdir(parents=True, exist_ok=True)
    item = {
        "item_id": compute_cognitive_item_id(board, gen, source_ref),
        "board_id": board, "kg_generation_id": gen, "source_ref": source_ref,
        "artifact_type": source_ref.split(":", 1)[0],
        "status": CognitiveItemStatus.PENDING.value,
        "recorded_at": "2026-06-17T00:00:00+00:00",
    }
    record = {"pending_count": 1, "pending_refs": [source_ref], "status": "pending",
              "recorded_at": "2026-06-17T00:00:00+00:00", "items": [item]}
    path.write_text(json.dumps(record), encoding="utf-8")


@pytest.mark.asyncio
async def test_cognitive_human_apply_and_remove_are_audited(db_factory, tmp_path, monkeypatch):
    board, gen = _id("board"), "gen-1"
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 imp3", owner_id=USER_ID))
        await db.commit()
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_pending(store, board, gen, source_ref)
    monkeypatch.setattr(
        ac_api, "build_default_readiness_service",
        lambda: CognitiveReadinessService(store), raising=True,
    )

    # APPLY skip via the HUMAN REST endpoint (actor_is_human=True inside).
    async with db_factory() as db:
        applied = await record_cognitive_skip_endpoint(
            board,
            CognitiveSkipRequest(source_ref=source_ref, reason_code="trivial_fix",
                                 justification="no reusable learning",
                                 evidence_refs=["card:other"]),
            SQLAlchemyUnitOfWork(db), actor=USER_ID,
        )
    # Audit on the response + the durable ledger: user, reason, justification, ts.
    assert applied["status"] == CognitiveItemStatus.SKIPPED.value
    assert applied["actor"] == USER_ID
    assert applied["reason_code"] == "trivial_fix"
    assert applied["justification"] == "no reusable learning"
    assert applied["evidence_refs"] == ["card:other"]
    assert applied["updated_at"] is not None
    iid = compute_cognitive_item_id(board, gen, source_ref)
    persisted = {i.item_id: i for i in store.list_items(board, gen)}[iid]
    assert persisted.status == CognitiveItemStatus.SKIPPED.value
    assert persisted.actor == USER_ID and persisted.reason_code == "trivial_fix"

    # REMOVE (clear/reopen) via the HUMAN REST endpoint.
    async with db_factory() as db:
        cleared = await clear_cognitive_skip_endpoint(
            board,
            CognitiveClearRequest(source_ref=source_ref),
            SQLAlchemyUnitOfWork(db),
            actor=USER_ID,
        )
    assert cleared["status"] == CognitiveItemStatus.PENDING.value
    assert cleared["actor"] == USER_ID
    assert cleared["updated_at"] is not None
    persisted2 = {i.item_id: i for i in store.list_items(board, gen)}[iid]
    assert persisted2.status == CognitiveItemStatus.PENDING.value
    # The clear drops the stale skip reason_code (audit trail of the reopen).
    assert persisted2.reason_code is None


# ===========================================================================
# Coexistence (R5-IMP1 preserved) — the agent MCP surface still refuses
# ===========================================================================


class _Ctx:
    def __init__(self):
        self.agent_id = "mcp-agent"
        self.agent_name = "r5 imp3 agent"
        self.permissions = set()


async def _mcp_call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


@pytest.mark.asyncio
async def test_mcp_skip_surfaces_still_refuse_after_imp1(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp3", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="idea",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        await db.commit()

    amb = await _mcp_call("okto_pulse_set_ideation_ambiguity_gate_skip",
                          board_id=board_id, ideation_id=ideation_id,
                          skip_ambiguity_gate=True)
    rec = await _mcp_call("okto_pulse_kg_record_cognitive_skip",
                          board_id=board_id, source_ref=f"bug:{UUID_A}",
                          reason_code="trivial_fix")
    clr = await _mcp_call("okto_pulse_kg_clear_cognitive_skip",
                          board_id=board_id, source_ref=f"bug:{UUID_A}")
    for out in (amb, rec, clr):
        assert out["code"] == "human_control_required"
        assert out["details"]["mutation_allowed"] is False

    # TEETH: the agent MCP call did NOT mutate the ambiguity skip.
    async with db_factory() as db:
        ideation = await db.get(Ideation, ideation_id)
    assert ideation.skip_ambiguity_gate is False
