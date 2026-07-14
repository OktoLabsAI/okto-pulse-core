"""R5-IMP5 (card 22b27654) — technical blockers stay human-only-proof and the
agent-facing ``human_control_required`` rejection is counted.

* A cognitive skip / ambiguity skip can NEVER clear, hide, or mask a technical
  blocker (DLQ / open CanonicalDebt): the technical tier wins in the readiness
  verdict, and the human REST skip still fails with ``technical_debt_cannot_be_skipped``
  (not converted to human_control).
* A bounded-label in-process counter records each agent-facing skip/no_action
  refusal (the 4 R5-IMP1 tools); it never increments on evaluate / create_learning
  nor on the human REST path.

Anti-test-theater: the counter is read from the REAL module after driving the REAL
MCP tools; the technical-blocker teeth use the REAL CognitiveReadinessService /
human REST endpoint over a seeded open debt.
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r5i5_"))

import okto_pulse.community.api.cognitive_action_center as ac_api
from okto_pulse.community.api.cognitive_action_center import (
    CognitiveSkipRequest,
    record_cognitive_skip_endpoint,
)
from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    compute_cognitive_item_id,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Ideation, IdeationStatus
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt
from okto_pulse.core.services.human_control_metrics import (
    get_human_control_required_count,
    get_human_control_required_labels,
    get_human_control_required_samples,
    reset_human_control_required_counter,
)

USER_ID = "r5-imp5-user"
UUID_A = "aaaaaaaa-5555-4555-8555-aaaaaaaaaaaa"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.fixture(autouse=True)
def _reset(tmp_path, monkeypatch):
    reset_human_control_required_counter()
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    yield
    reset_human_control_required_counter()


class _Ctx:
    def __init__(self):
        self.agent_id = "mcp-agent"
        self.agent_name = "r5 imp5 agent"
        self.permissions = set()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


def _seed_item(store, board, gen, source_ref, status):
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


# ===========================================================================
# Counter — increments on the 4 MCP refusals, bounded labels
# ===========================================================================


@pytest.mark.asyncio
async def test_counter_increments_on_four_mcp_refusals_with_bounded_labels(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp5", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="i",
                        created_by=USER_ID, status=IdeationStatus.EVALUATING))
        await db.commit()

    await _call("okto_pulse_set_ideation_ambiguity_gate_skip",
                board_id=board_id, ideation_id=ideation_id, skip_ambiguity_gate=True)
    await _call("okto_pulse_kg_record_cognitive_skip",
                board_id=board_id, source_ref=f"bug:{UUID_A}", reason_code="trivial_fix")
    await _call("okto_pulse_kg_clear_cognitive_skip",
                board_id=board_id, source_ref=f"bug:{UUID_A}")
    await _call("okto_pulse_kg_evaluate_bug_cognitive_closure",
                board_id=board_id, bug_id=UUID_A, evidence={"root_cause": "r"},
                requested_action="skip", reason_code="trivial_fix")

    assert get_human_control_required_count() == 4
    assert get_human_control_required_count(board_id=board_id) == 4
    # one per tool
    for tool in (
        "okto_pulse_set_ideation_ambiguity_gate_skip",
        "okto_pulse_kg_record_cognitive_skip",
        "okto_pulse_kg_clear_cognitive_skip",
        "okto_pulse_kg_evaluate_bug_cognitive_closure",
    ):
        assert get_human_control_required_count(blocked_tool=tool) == 1, tool
    # bounded labels only — no target_ref / artifact id in the sample.
    assert get_human_control_required_labels() == ("board_id", "blocked_tool", "blocked_action")
    for sample in get_human_control_required_samples():
        assert set(sample.keys()) == {"board_id", "blocked_tool", "blocked_action"}
    # evaluate_bug action label carries the bounded requested_action.
    assert get_human_control_required_count(
        blocked_action="evaluate_bug_cognitive_closure:skip") == 1


# ===========================================================================
# Counter — NOT incremented by evaluate / create_learning (no skip)
# ===========================================================================


@pytest.mark.asyncio
async def test_counter_not_incremented_by_evaluate_or_create_learning(db_factory):
    board_id = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r5 imp5", owner_id=USER_ID))
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
    assert get_human_control_required_count() == 0


# ===========================================================================
# Counter — NOT incremented on the human REST path
# ===========================================================================


@pytest.mark.asyncio
async def test_counter_not_incremented_on_human_rest_skip(db_factory, tmp_path, monkeypatch):
    board, gen = _id("board"), "gen-1"
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 imp5", owner_id=USER_ID))
        await db.commit()
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_item(store, board, gen, source_ref, CognitiveItemStatus.PENDING.value)
    monkeypatch.setattr(ac_api, "build_default_readiness_service",
                        lambda: CognitiveReadinessService(store), raising=True)

    async with db_factory() as db:
        resp = await record_cognitive_skip_endpoint(
            board, CognitiveSkipRequest(source_ref=source_ref, reason_code="trivial_fix"),
            SQLAlchemyUnitOfWork(db), actor=USER_ID,
        )
    # Human REST skip succeeds (not a refusal) and never touches the counter.
    assert resp["status"] == CognitiveItemStatus.SKIPPED.value
    assert get_human_control_required_count() == 0


# ===========================================================================
# Technical blockers — never masked / never converted to human_control
# ===========================================================================


@pytest.mark.asyncio
async def test_human_rest_skip_blocked_by_open_debt_is_technical_not_human_control(db_factory, tmp_path, monkeypatch):
    board, gen = _id("board"), "gen-1"
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 imp5", owner_id=USER_ID))
        await db.commit()
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="canonical", canonical_state="blocked",
        )
        await db.commit()
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_item(store, board, gen, source_ref, CognitiveItemStatus.PENDING.value)
    monkeypatch.setattr(ac_api, "build_default_readiness_service",
                        lambda: CognitiveReadinessService(store), raising=True)

    # The human REST skip is refused as TECHNICAL debt, NOT human_control, and the
    # debt is never masked. The endpoint surfaces it as HTTP 409.
    from fastapi import HTTPException

    async with db_factory() as db:
        with pytest.raises(HTTPException) as exc:
            await record_cognitive_skip_endpoint(
                board, CognitiveSkipRequest(source_ref=source_ref, reason_code="trivial_fix"),
                SQLAlchemyUnitOfWork(db), actor=USER_ID,
            )
    assert exc.value.status_code == 409
    assert exc.value.detail["error"] == "technical_debt_cannot_be_skipped"
    assert exc.value.detail["error"] != "human_control_required"
    assert get_human_control_required_count() == 0


@pytest.mark.asyncio
async def test_skip_never_masks_open_debt_in_verdict(db_factory, tmp_path):
    board, gen = _id("board"), "gen-1"
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 imp5", owner_id=USER_ID))
        await db.commit()
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="canonical", canonical_state="pending",
        )
        await db.commit()
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    # Even with a SKIPPED ledger item present, the technical tier WINS.
    _seed_item(store, board, gen, source_ref, CognitiveItemStatus.SKIPPED.value)
    service = CognitiveReadinessService(store)

    async with db_factory() as db:
        verdict = await service.evaluate_artifact(db, board_id=board, source_ref=source_ref)
    assert verdict.tier == "canonical_debt_open"
    assert verdict.blocking is True


@pytest.mark.asyncio
async def test_ambiguity_skip_does_not_mask_cognitive_technical_blocker(db_factory, tmp_path):
    # Domain isolation: an ambiguity-gate skip on an ideation must NOT affect a
    # card's cognitive/technical readiness (separate domains).
    from okto_pulse.core.services.main import IdeationService

    board, gen = _id("board"), "gen-1"
    ideation_id = _id("idea")
    source_ref = f"bug:{UUID_A}"
    async with db_factory() as db:
        db.add(Board(id=board, name="r5 imp5", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board, title="i", created_by=USER_ID,
                        status=IdeationStatus.EVALUATING))
        await db.commit()
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="canonical", canonical_state="blocked",
        )
        await db.commit()
        # Human applies the ambiguity skip (legitimate, separate domain).
        await IdeationService(db).set_ambiguity_gate_skip(
            ideation_id, USER_ID, True, source="rest")
        await db.commit()

    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    _seed_item(store, board, gen, source_ref, CognitiveItemStatus.PENDING.value)
    service = CognitiveReadinessService(store)
    async with db_factory() as db:
        verdict = await service.evaluate_artifact(db, board_id=board, source_ref=source_ref)
    # The open debt still blocks — the ambiguity skip did not touch it.
    assert verdict.tier == "canonical_debt_open"
    assert verdict.blocking is True
