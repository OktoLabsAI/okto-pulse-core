"""R6-TEST2 (card f1e35a3a) — KG Health distinguishes the ACTIVE queue from the
dead-letter backlog and canonical debt, via the REAL MCP tools.

* ts_73b89491: okto_pulse_kg_queue_drilldown exposes worker_mode, separate sources
  (consolidation_queue + global_update_outbox), counts by status/category,
  oldest_age_seconds, a transient|stuck|backpressure|idle classification and a
  drill_down_tool — and the ACTIVE depth never folds in DLQ or CanonicalDebt.
* ts_096b61b1: okto_pulse_kg_health surfaces dead_letter_backlog AT MOST ONCE per
  evaluation (full keeps the single issue with severity/operator_action/drill-down;
  summary omits the prose issue but keeps the dead_letter_count scalar), and the
  three operational domains stay separate counters.

Anti-test-theater: every case runs through the REAL MCP tools over seeded
ConsolidationQueue / GlobalUpdateOutbox / ConsolidationDeadLetter / CanonicalDebt
rows; the dedup teeth COUNT the dead_letter_backlog occurrences in each profile.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    GlobalUpdateOutbox,
)
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

USER_ID = "r6-test2-user"
NOW = datetime.now(timezone.utc)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r6 test2 agent"
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
        triggered_at=NOW - timedelta(seconds=age_s),
    )


def _outbox(board, *, retry_count=0, age_s=10):
    return GlobalUpdateOutbox(
        id=_id("ob"), event_id=_id("ev"), board_id=board, session_id="s",
        event_type="node_upsert", payload={"x": 1}, retry_count=retry_count,
        processed_at=None, created_at=NOW - timedelta(seconds=age_s),
    )


def _dlq(board):
    return ConsolidationDeadLetter(
        id=_id("dlq"), board_id=board, artifact_type="card", artifact_id=_id("art"),
        original_queue_id=_id("q"), attempts=5,
        errors=[{"attempt": 1, "error_type": "X", "message": "boom"}])


async def _seed_all(db_factory):
    """Active CQ (pending+claimed) + active outbox + DLQ + canonical debt."""
    board = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 test2", owner_id=USER_ID))
        db.add(_cq(board, artifact_type="spec", status="pending", age_s=20))
        db.add(_cq(board, artifact_type="card", status="claimed", age_s=15))
        db.add(_cq(board, artifact_type="card", status="done", age_s=5))   # NOT active
        db.add(_outbox(board, retry_count=0, age_s=30))                    # active
        db.add(_dlq(board))                                                # DLQ (terminal)
        await db.commit()
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=_id("a"),
            source_ref=f"card:{_id('a')}", content_hash="h",
            target_status="canonical", canonical_state="blocked")            # debt
        await db.commit()
    return board


def _dl_issue_count(health: dict) -> int:
    return sum(1 for i in (health.get("health_issues") or [])
               if i.get("code") == "dead_letter_backlog")


# ===========================================================================
# ts_73b89491 — active-queue drill-down; never folds in DLQ / canonical debt
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_73b89491_queue_drilldown_active_only(db_factory):
    board = await _seed_all(db_factory)
    dd = await _call("okto_pulse_kg_queue_drilldown", board_id=board)

    assert dd["worker_mode"] in ("running", "stopped", "unknown")
    by_source = {s["source"]: s for s in dd["sources"]}
    assert set(by_source) == {"consolidation_queue", "global_update_outbox"}
    cq = by_source["consolidation_queue"]
    assert cq["by_status"] == {"pending": 1, "claimed": 1}     # `done` excluded
    assert cq["by_category"] == {"spec": 1, "card": 1}
    assert "oldest_age_seconds" in cq
    assert cq["classification"] in ("transient", "stuck", "backpressure", "idle")
    ob = by_source["global_update_outbox"]
    assert ob["queue_depth"] == 1
    # ACTIVE = CQ pending/claimed (2) + outbox pending (1) = 3. DLQ + canonical
    # debt + `done` are NOT folded into the active queue.
    assert dd["total_active_depth"] == 3
    assert dd["drill_down_tool"] == "okto_pulse_kg_queue_drilldown"


# ===========================================================================
# ts_096b61b1 — dead_letter_backlog at most once per evaluation (summary/full)
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_096b61b1_dead_letter_backlog_once_in_full(db_factory):
    board = await _seed_all(db_factory)
    health = await _call("okto_pulse_kg_health", board_id=board, profile="full")

    # Exactly ONE dead_letter_backlog issue, with its operational fields intact.
    issues = [i for i in health["health_issues"] if i["code"] == "dead_letter_backlog"]
    assert len(issues) == 1, health["health_issues"]
    issue = issues[0]
    assert issue["severity"] == "warning"
    assert issue["operator_action"] == "inspect_dead_letters"
    assert issue["drill_down_tool"] == "okto_pulse_kg_dead_letter_list"

    # The three operational domains are SEPARATE counters (no mixing).
    dom = health["operational_domains"]
    assert dom["dead_letter"]["count"] == 1
    assert dom["active_queue"]["count"] == 3      # CQ active + outbox pending
    assert dom["canonical_debt"]["count"] >= 1
    assert dom["dead_letter"]["drill_down_tool"] == "okto_pulse_kg_dead_letter_list"
    assert dom["active_queue"]["drill_down_tool"] == "okto_pulse_kg_queue_drilldown"


@pytest.mark.asyncio
async def test_ts_096b61b1_dead_letter_not_duplicated_in_summary(db_factory):
    board = await _seed_all(db_factory)
    summary = await _call("okto_pulse_kg_health", board_id=board, profile="summary")

    # Summary keeps the dead_letter signal as the scalar count (the prose issue is
    # omitted in the slim profile) and NEVER duplicates the dead_letter_backlog.
    assert summary["profile"] == "summary"
    assert summary["dead_letter_count"] == 1
    assert _dl_issue_count(summary) <= 1   # omitted -> 0; never 2+


@pytest.mark.asyncio
async def test_ts_096b61b1_active_queue_distinct_from_dlq_and_debt(db_factory):
    board = await _seed_all(db_factory)
    health = await _call("okto_pulse_kg_health", board_id=board, profile="full")
    dom = health["operational_domains"]
    # Distinct domains, distinct semantics, distinct drill-downs — no conflation.
    assert dom["active_queue"]["semantics"] == "transient_operational"
    assert dom["dead_letter"]["semantics"] == "terminal_failure"
    assert dom["canonical_debt"]["semantics"] == "semantic_canonicality_pending"
    tools = {dom[d]["drill_down_tool"] for d in ("active_queue", "dead_letter", "canonical_debt")}
    assert len(tools) == 3
    # active_queue.count (3) reflects ONLY the active queue, not the DLQ (1) or debt.
    assert health["active_queue"]["total_active_depth"] == 3
    assert health["dead_letter_count"] == 1
