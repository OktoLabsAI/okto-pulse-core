"""R6-IMP5 (card af7b7c9e, FR5/AC5) — DLQ, canonical debt and the active queue are
SEPARATE, deduplicated operational domains in KG Health.

Each domain is its own counter + drill-down: active_queue (transient operational),
dead_letter (terminal failure), canonical_debt (semantic canonicality pendency).
DLQ is never summed into the active queue; canonical debt is never reported as DLQ;
and a dead-letter backlog yields EXACTLY ONE ``dead_letter_backlog`` health issue
(no duplicate on retry/re-evaluation).

Anti-test-theater: every case runs through the REAL ``get_kg_health`` composition
over seeded ConsolidationQueue / ConsolidationDeadLetter / CanonicalDebt rows; the
dedup teeth COUNT the ``dead_letter_backlog`` issue occurrences.
"""

from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
)
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt
from okto_pulse.core.services.kg_health_service import get_kg_health

USER_ID = "r6-imp5-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


async def _seed(db_factory, *, active=False, dlq=False, debt=False):
    board = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 imp5", owner_id=USER_ID))
        if active:
            db.add(ConsolidationQueue(
                id=_id("cq"), board_id=board, artifact_type="spec",
                artifact_id=_id("art"), status="pending"))
        if dlq:
            db.add(ConsolidationDeadLetter(
                id=_id("dlq"), board_id=board, artifact_type="card",
                artifact_id=_id("art"), original_queue_id="q1", attempts=3,
                errors=[{"attempt": 1, "error_type": "X", "message": "boom"}]))
        await db.commit()
        if debt:
            await upsert_canonical_debt(
                db, board_id=board, artifact_type="card", artifact_id=_id("art"),
                source_ref=f"card:{_id('a')}", content_hash="h",
                target_status="canonical", canonical_state="blocked")
            await db.commit()
    return board


def _codes(health):
    return [i["code"] for i in health["health_issues"]]


# ===========================================================================
# Single-domain cases — only the relevant domain surfaces
# ===========================================================================


@pytest.mark.asyncio
async def test_active_queue_only(db_factory):
    board = await _seed(db_factory, active=True)
    async with db_factory() as db:
        health = await get_kg_health(board, db)
    dom = health["operational_domains"]
    assert dom["active_queue"]["count"] == 1
    assert dom["dead_letter"]["count"] == 0
    assert dom["canonical_debt"]["count"] == 0
    codes = _codes(health)
    assert any(c.startswith("active_queue_") for c in codes)
    assert "dead_letter_backlog" not in codes
    assert "canonical_debt_open" not in codes


@pytest.mark.asyncio
async def test_dead_letter_only(db_factory):
    board = await _seed(db_factory, dlq=True)
    async with db_factory() as db:
        health = await get_kg_health(board, db)
    dom = health["operational_domains"]
    assert dom["dead_letter"]["count"] == 1
    assert dom["active_queue"]["count"] == 0
    assert dom["canonical_debt"]["count"] == 0
    codes = _codes(health)
    assert "dead_letter_backlog" in codes
    assert not any(c.startswith("active_queue_") for c in codes)
    assert "canonical_debt_open" not in codes


@pytest.mark.asyncio
async def test_canonical_debt_only(db_factory):
    board = await _seed(db_factory, debt=True)
    async with db_factory() as db:
        health = await get_kg_health(board, db)
    dom = health["operational_domains"]
    assert dom["canonical_debt"]["count"] >= 1
    assert dom["active_queue"]["count"] == 0
    assert dom["dead_letter"]["count"] == 0
    codes = _codes(health)
    assert "canonical_debt_open" in codes
    assert "dead_letter_backlog" not in codes
    assert not any(c.startswith("active_queue_") for c in codes)


# ===========================================================================
# Mixed — each domain appears once, separate counts + distinct drill-downs
# ===========================================================================


@pytest.mark.asyncio
async def test_mixed_each_domain_once_with_distinct_drilldowns(db_factory):
    board = await _seed(db_factory, active=True, dlq=True, debt=True)
    async with db_factory() as db:
        health = await get_kg_health(board, db)
    dom = health["operational_domains"]
    # Separate, non-conflated counters.
    assert dom["active_queue"]["count"] == 1
    assert dom["dead_letter"]["count"] == 1
    assert dom["canonical_debt"]["count"] >= 1
    # DLQ is its OWN count, never folded into the active queue: active_queue.count
    # reflects only ConsolidationQueue pending/claimed (1), not the DLQ row.
    assert dom["active_queue"]["count"] == 1 and dom["dead_letter"]["count"] == 1
    # Distinct drill-down tools per domain.
    tools = {dom[d]["drill_down_tool"] for d in ("active_queue", "dead_letter", "canonical_debt")}
    assert tools == {
        "okto_pulse_kg_queue_drilldown",
        "okto_pulse_kg_dead_letter_list",
        "okto_pulse_kg_canonical_debt_list",
    }
    # Distinct semantics per domain.
    assert dom["active_queue"]["semantics"] == "transient_operational"
    assert dom["dead_letter"]["semantics"] == "terminal_failure"
    assert dom["canonical_debt"]["semantics"] == "semantic_canonicality_pending"
    # Each domain surfaces EXACTLY ONE health issue.
    codes = _codes(health)
    assert sum(1 for c in codes if c.startswith("active_queue_")) == 1
    assert codes.count("dead_letter_backlog") == 1
    assert codes.count("canonical_debt_open") == 1


# ===========================================================================
# Dedup teeth — dead_letter_backlog appears EXACTLY once (no duplicate)
# ===========================================================================


@pytest.mark.asyncio
async def test_dead_letter_backlog_is_not_duplicated(db_factory):
    # Several dead-letter rows (as a retry/re-eval backlog would produce) must still
    # yield exactly ONE dead_letter_backlog health issue.
    board = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 imp5", owner_id=USER_ID))
        for _ in range(3):
            db.add(ConsolidationDeadLetter(
                id=_id("dlq"), board_id=board, artifact_type="card",
                artifact_id=_id("art"), original_queue_id=_id("q"), attempts=5,
                errors=[{"attempt": 1, "error_type": "X", "message": "boom"}]))
        await db.commit()
        health = await get_kg_health(board, db)

    codes = _codes(health)
    # The duplicate append was removed (R6-IMP5): EXACTLY one issue, and it carries
    # the drill-down tool (the FR7 canonical one was kept).
    dl_issues = [i for i in health["health_issues"] if i["code"] == "dead_letter_backlog"]
    assert len(dl_issues) == 1, codes
    assert dl_issues[0]["drill_down_tool"] == "okto_pulse_kg_dead_letter_list"
    assert health["operational_domains"]["dead_letter"]["count"] == 3
