"""R6-IMP2 (card eb48339e, FR2/AC2) — active operational-queue drill-down in
KG Health / MCP.

The drill-down exposes worker_mode, separate sources (ConsolidationQueue +
global_update_outbox), counts by status/category, oldest_age_seconds and a
transient|stuck|backpressure|idle classification. ACTIVE = ConsolidationQueue
pending/claimed + GlobalUpdateOutbox retry-window rows; DLQ / dead_letter /
canonical debt are TERMINAL and NOT counted (that separation is R6-IMP5).

Anti-test-theater: the drill-down runs over REAL seeded ConsolidationQueue /
GlobalUpdateOutbox rows; the KG Health issue + MCP tool are exercised through the
real composition / tool boundary; the separation teeth seed DLQ + outbox
dead_letter rows and assert they do NOT inflate the active depth.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import delete

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    GlobalUpdateOutbox,
)
from okto_pulse.core.services.queue_health_service import (
    classify_active_queue,
    get_active_queue_drilldown,
)

USER_ID = "r6-imp2-user"
BOARD_PREFIX = "r6imp2-board"


def _now() -> datetime:
    # Captured per call, NOT at import: this module is imported during pytest
    # COLLECTION, so a module-level NOW would make every seeded age drift by
    # the collection→execution gap — in a full-suite run the "30s-old" outbox
    # row arrives minutes old and classify_active_queue flips transient→stuck.
    return datetime.now(timezone.utc)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest_asyncio.fixture(autouse=True)
async def _clean_r6_imp2_rows(db_factory):
    async def _cleanup() -> None:
        async with db_factory() as db:
            pattern = f"{BOARD_PREFIX}-%"
            for table in (
                ConsolidationQueue,
                ConsolidationDeadLetter,
                GlobalUpdateOutbox,
                Board,
            ):
                await db.execute(delete(table).where(table.board_id.like(pattern))
                                 if table is not Board
                                 else delete(Board).where(Board.id.like(pattern)))
            await db.commit()

    await _cleanup()
    yield
    await _cleanup()


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r6 imp2 agent"
        self.permissions = set()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


def _cq(board, *, artifact_type, status, age_s=10):
    return ConsolidationQueue(
        id=_id("cq"), board_id=board, artifact_type=artifact_type,
        artifact_id=_id("art"), status=status,
        triggered_at=_now() - timedelta(seconds=age_s),
    )


def _outbox(board, *, retry_count, processed=False, age_s=10):
    return GlobalUpdateOutbox(
        id=_id("ob"), event_id=_id("ev"), board_id=board, session_id="s",
        event_type="node_upsert", payload={"x": 1}, retry_count=retry_count,
        processed_at=(_now() if processed else None),
        created_at=_now() - timedelta(seconds=age_s),
    )


# ===========================================================================
# Unit — classification thresholds
# ===========================================================================


def test_classify_active_queue_thresholds():
    assert classify_active_queue(depth=0, oldest_age_s=999, alert_threshold=5000, stuck_age_s=300) == "idle"
    assert classify_active_queue(depth=3, oldest_age_s=10, alert_threshold=5000, stuck_age_s=300) == "transient"
    assert classify_active_queue(depth=3, oldest_age_s=400, alert_threshold=5000, stuck_age_s=300) == "stuck"
    assert classify_active_queue(depth=5000, oldest_age_s=10, alert_threshold=5000, stuck_age_s=300) == "backpressure"
    # backpressure wins over stuck when both apply.
    assert classify_active_queue(depth=5000, oldest_age_s=400, alert_threshold=5000, stuck_age_s=300) == "backpressure"


# ===========================================================================
# Integration — drill-down over real rows; sources separated; DLQ excluded
# ===========================================================================


@pytest.mark.asyncio
async def test_drilldown_separates_sources_and_excludes_terminal(db_factory):
    board = _id(BOARD_PREFIX)
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 imp2", owner_id=USER_ID))
        # ConsolidationQueue: 2 pending (spec, card) + 1 claimed (card) ACTIVE; an
        # oldest pending at 400s (=> stuck); 1 done is NOT active.
        db.add(_cq(board, artifact_type="spec", status="pending", age_s=400))
        db.add(_cq(board, artifact_type="card", status="pending", age_s=20))
        db.add(_cq(board, artifact_type="card", status="claimed", age_s=15))
        db.add(_cq(board, artifact_type="card", status="done", age_s=5))
        # Outbox: 1 active pending + 1 dead_letter (retry_count==MAX) + 1 processed.
        db.add(_outbox(board, retry_count=0, age_s=30))
        db.add(_outbox(board, retry_count=5))          # dead_letter — excluded
        db.add(_outbox(board, retry_count=0, processed=True))  # processed — excluded
        # A consolidation dead-letter row (terminal DLQ) — must NOT be counted.
        db.add(ConsolidationDeadLetter(
            id=_id("dlq"), board_id=board, artifact_type="card",
            artifact_id=_id("art"), original_queue_id="q1", attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}]))
        await db.commit()

        dd = await get_active_queue_drilldown(db, board)

    assert dd["worker_mode"] in ("running", "stopped", "unknown")
    # total active = 3 (cq pending+claimed) + 1 (outbox pending) = 4. DLQ/dead_letter
    # /processed/done are EXCLUDED.
    assert dd["total_active_depth"] == 4
    by_source = {s["source"]: s for s in dd["sources"]}
    assert set(by_source) == {"consolidation_queue", "global_update_outbox"}
    cq = by_source["consolidation_queue"]
    assert cq["queue_depth"] == 3
    assert cq["by_status"] == {"pending": 2, "claimed": 1}
    assert cq["by_category"] == {"spec": 1, "card": 2}
    assert cq["oldest_age_seconds"] >= 400
    assert cq["classification"] == "stuck"   # oldest active item older than 300s
    ob = by_source["global_update_outbox"]
    assert ob["queue_depth"] == 1            # dead_letter + processed excluded
    assert ob["classification"] == "transient"
    # overall worst-wins = stuck.
    assert dd["classification"] == "stuck"
    assert dd["drill_down_tool"] == "okto_pulse_kg_queue_drilldown"


@pytest.mark.asyncio
async def test_drilldown_idle_when_no_active_work(db_factory):
    board = _id(BOARD_PREFIX)
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 imp2", owner_id=USER_ID))
        db.add(_cq(board, artifact_type="card", status="done", age_s=5))
        db.add(_outbox(board, retry_count=5))  # dead_letter only
        await db.commit()
        dd = await get_active_queue_drilldown(db, board)
    assert dd["total_active_depth"] == 0
    assert dd["classification"] == "idle"


# ===========================================================================
# KG Health integration — issue + drill_down_tool + inline counts
# ===========================================================================


@pytest.mark.asyncio
async def test_kg_health_surfaces_active_queue_issue_and_counts(db_factory):
    from okto_pulse.core.services.kg_health_service import get_kg_health

    board = _id(BOARD_PREFIX)
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 imp2", owner_id=USER_ID))
        db.add(_cq(board, artifact_type="spec", status="pending", age_s=400))
        db.add(_outbox(board, retry_count=0, age_s=30))
        await db.commit()

    async with db_factory() as db:
        health = await get_kg_health(board, db)

    aq = health["active_queue"]
    assert aq["total_active_depth"] == 2
    assert aq["classification"] == "stuck"
    issues = [i for i in health["health_issues"] if i.get("component") == "operational_queue"]
    assert len(issues) == 1, health["health_issues"]
    issue = issues[0]
    assert issue["code"] == "active_queue_stuck"
    assert issue["drill_down_tool"] == "okto_pulse_kg_queue_drilldown"
    assert issue["counts"]["consolidation_queue"] == 1
    assert issue["counts"]["global_update_outbox"] == 1
    # stuck/backpressure may become primary when nothing else is the cause.
    assert health["primary_health_cause"] in ("active_queue_stuck", "decay_scheduler_debt") \
        or health["primary_health_cause"] != "none"


# ===========================================================================
# MCP tool — read-only boundary
# ===========================================================================


@pytest.mark.asyncio
async def test_mcp_queue_drilldown_tool(db_factory):
    board = _id(BOARD_PREFIX)
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 imp2", owner_id=USER_ID))
        db.add(_cq(board, artifact_type="card", status="pending", age_s=20))
        await db.commit()

    out = await _call("okto_pulse_kg_queue_drilldown", board_id=board)
    assert out["board_id"] == board
    assert out["total_active_depth"] == 1
    assert {s["source"] for s in out["sources"]} == {"consolidation_queue", "global_update_outbox"}
    assert out["classification"] in ("transient", "stuck", "backpressure", "idle")
    assert "worker_mode" in out
