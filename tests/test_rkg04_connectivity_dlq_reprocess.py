"""RKG-04 — safe diagnosis + reprocess of the connectivity-guard technical_dlq class.

Real tests (no fakes): seed real ConsolidationDeadLetter rows, gate via the
service, and prove the chain DLQ -> ConsolidationQueue -> worker -> graph.lbug on
a controlled fixture. The LIVE board DLQs are NOT mutated here (that would be a
destructive change to real KG state under NO-COMMIT) — the live inventory is
reported separately; this fixture proves the same code path safely.

Coverage:
  TS1 ts_a71681db (negative): reprocess blocked without preconditions, no mutation
     - selected DLQ missing            -> block
     - selected DLQ out-of-class       -> block, untouched
     - KG quarantined                  -> block, untouched
     - RKG-02/03 fix absent            -> block, untouched
  TS2 ts_c05ca560 (e2e): selected connectivity DLQ -> requeue (no dup) -> drain -> graph.lbug
  TS3 ts_317b11ef (negative): persistent failure stays an actionable technical_dlq
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.schema import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.workers.consolidation import _process_queue_entry
from okto_pulse.core.kg.workers.dead_letter import route_to_dead_letter
from okto_pulse.core.models.db import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.connectivity_dlq_reprocess_service import (
    CONNECTIVITY_GUARD_SIGNATURE,
    check_reprocess_preconditions,
    diagnose_connectivity_guard_dlq,
    reprocess_connectivity_guard_dlq,
    verify_connectivity_class_cleared,
)


async def _not_quarantined(_board_id, _db) -> bool:
    return False


async def _quarantined(_board_id, _db) -> bool:
    return True


async def _seed_dlq(
    db_factory, board_id, *, artifact_type="spec", artifact_id=None,
    error=CONNECTIVITY_GUARD_SIGNATURE, attempts=5,
) -> tuple[str, str]:
    aid = artifact_id or uuid.uuid4().hex
    async with db_factory() as db:
        row = ConsolidationDeadLetter(
            board_id=board_id, artifact_type=artifact_type, artifact_id=aid,
            attempts=attempts,
            errors=[{
                "attempt": attempts, "occurred_at": "2026-06-25T00:00:00+00:00",
                "error_type": "KGPrimitiveError", "message": error, "traceback": None,
            }],
            dead_lettered_at=datetime.now(timezone.utc),
        )
        db.add(row)
        await db.flush()
        dlq_id = row.id
        await db.commit()
    return dlq_id, aid


async def _dlq_ids(db_factory, board_id) -> set[str]:
    async with db_factory() as db:
        rows = (await db.execute(
            select(ConsolidationDeadLetter).where(
                ConsolidationDeadLetter.board_id == board_id))).scalars().all()
    return {r.id for r in rows}


def _find_by_artifact_rows(board_id: str, artifact_ref: str) -> list:
    """Query through the registered graph_store port (production surface).

    Unlike ``_count_nodes_containing`` (raw Ladybug connection, ts2-only),
    this runs against whichever provider the registry serves: the real
    Community adapter or the sanctioned core in-memory provider. Both match
    ``source_artifact_ref`` by equality, so an absence assert is equivalent
    in either environment. Filters are wide open so nothing masks a
    falsely-materialised node.
    """
    store = get_kg_registry().graph_store
    return store.find_by_artifact(
        board_id, artifact_ref,
        QueryFilters(min_confidence=0.0, max_rows=10, min_relevance=0.0),
    )


def _count_nodes_containing(board_id: str, needle: str) -> int:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            "MATCH (n) WHERE n.source_artifact_ref CONTAINS $needle RETURN count(n)",
            {"needle": needle},
        )
        try:
            return int(res.get_next()[0]) if res.has_next() else 0
        finally:
            try:
                res.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# TS1 ts_a71681db — reprocess blocked without preconditions; NOTHING mutated.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts1_block_when_selected_dlq_missing(db_factory):
    board_id = f"rkg04-{uuid.uuid4().hex[:10]}"
    async with db_factory() as db:
        pre = await check_reprocess_preconditions(
            db, board_id, ["does-not-exist"], quarantine_probe=_not_quarantined)
    assert pre["allowed"] is False
    assert "selected_dlq_missing" in pre["reasons"]


@pytest.mark.asyncio
async def test_ts1_block_when_selected_dlq_out_of_class(db_factory):
    board_id = f"rkg04-{uuid.uuid4().hex[:10]}"
    # A DLQ that exists but is NOT the connectivity-guard class.
    other_id, _ = await _seed_dlq(
        db_factory, board_id, error="some unrelated consolidation failure")
    async with db_factory() as db:
        res = await reprocess_connectivity_guard_dlq(
            db, board_id, [other_id], quarantine_probe=_not_quarantined)
        await db.commit()
    assert res["blocked"] is True
    assert "selected_dlq_out_of_class" in res["reasons"]
    assert res["removed_dlq"] is False
    assert other_id in await _dlq_ids(db_factory, board_id)  # untouched


@pytest.mark.asyncio
async def test_ts1_block_when_quarantined(db_factory):
    board_id = f"rkg04-{uuid.uuid4().hex[:10]}"
    dlq_id, _ = await _seed_dlq(db_factory, board_id)
    async with db_factory() as db:
        res = await reprocess_connectivity_guard_dlq(
            db, board_id, [dlq_id], quarantine_probe=_quarantined)
        await db.commit()
    assert res["blocked"] is True
    assert "kg_quarantined" in res["reasons"]
    assert dlq_id in await _dlq_ids(db_factory, board_id)  # untouched


@pytest.mark.asyncio
async def test_ts1_block_when_rkg_fix_absent(db_factory):
    board_id = f"rkg04-{uuid.uuid4().hex[:10]}"
    dlq_id, _ = await _seed_dlq(db_factory, board_id)
    async with db_factory() as db:
        res = await reprocess_connectivity_guard_dlq(
            db, board_id, [dlq_id],
            fixes_applied_probe=lambda: False, quarantine_probe=_not_quarantined)
        await db.commit()
    assert res["blocked"] is True
    assert "rkg02_rkg03_not_applied" in res["reasons"]
    assert dlq_id in await _dlq_ids(db_factory, board_id)  # untouched


@pytest.mark.asyncio
async def test_ts1_block_when_no_selection(db_factory):
    board_id = f"rkg04-{uuid.uuid4().hex[:10]}"
    await _seed_dlq(db_factory, board_id)
    async with db_factory() as db:
        res = await reprocess_connectivity_guard_dlq(
            db, board_id, [], quarantine_probe=_not_quarantined)
        await db.commit()
    assert res["blocked"] is True
    assert "no_dlq_selected" in res["reasons"]  # TR1: never broad reprocess


# ---------------------------------------------------------------------------
# TS2 ts_c05ca560 — selected connectivity DLQ -> requeue (no dup) -> drain -> graph.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts2_connectivity_dlq_drains_to_graph(db_factory):
    board_id = f"rkg04-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        db.add(Board(id=board_id, name="rkg04", owner_id="owner"))
        db.add(Spec(
            id=spec_id, board_id=board_id, title="RKG-04 controlled spec",
            status=SpecStatus.DONE, created_by="owner",
            functional_requirements=["FR1: the system shall reprocess safely"],
            acceptance_criteria=["AC1: given a DLQ then it drains to the graph"],
            test_scenarios=[], business_rules=[], api_contracts=[],
            technical_requirements=[], decisions=[],
        ))
        await db.commit()

    dlq_id, _ = await _seed_dlq(db_factory, board_id, artifact_type="spec", artifact_id=spec_id)

    # diagnose: the DLQ is in the connectivity-guard class.
    async with db_factory() as db:
        diag = await diagnose_connectivity_guard_dlq(db, board_id)
    assert dlq_id in diag["dead_letter_ids"]

    # reprocess (preconditions pass) -> DLQ removed, queue row created, NO duplicate.
    async with db_factory() as db:
        res = await reprocess_connectivity_guard_dlq(
            db, board_id, [dlq_id], quarantine_probe=_not_quarantined)
        await db.commit()
    assert res["blocked"] is False and res["success"] is True

    assert dlq_id not in await _dlq_ids(db_factory, board_id)  # removed
    async with db_factory() as db:
        q_rows = (await db.execute(select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.artifact_type == "spec",
            ConsolidationQueue.artifact_id == spec_id))).scalars().all()
    assert len(q_rows) == 1  # ConsolidationQueue dedup (TR1)

    # drain via the REAL worker -> graph.lbug.
    async with db_factory() as db:
        entry = (await db.execute(select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.artifact_id == spec_id))).scalars().first()
        ok = await _process_queue_entry(db, entry)
        await db.commit()
    assert ok is True

    # graph.lbug queryable for the artifact + class cleared (no return to DLQ).
    assert _count_nodes_containing(board_id, spec_id) > 0
    async with db_factory() as db:
        verify = await verify_connectivity_class_cleared(
            db, board_id, artifact_refs=[f"spec:{spec_id}"])
    assert verify["class_cleared"] is True


# ---------------------------------------------------------------------------
# TS3 ts_317b11ef — persistent failure stays an actionable technical_dlq.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts3_persistent_failure_stays_visible(db_factory):
    board_id = f"rkg04-{uuid.uuid4().hex[:10]}"
    missing_spec_id = f"spec-{uuid.uuid4().hex[:8]}"  # spec intentionally NOT created
    bootstrap_board_graph(board_id)

    dlq_id, _ = await _seed_dlq(
        db_factory, board_id, artifact_type="spec", artifact_id=missing_spec_id)

    async with db_factory() as db:
        res = await reprocess_connectivity_guard_dlq(
            db, board_id, [dlq_id], quarantine_probe=_not_quarantined)
        await db.commit()
    assert res["blocked"] is False

    # drain: the artifact cannot be loaded -> worker fails (no false success).
    async with db_factory() as db:
        entry = (await db.execute(select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.artifact_id == missing_spec_id))).scalars().first()
        ok = await _process_queue_entry(db, entry)
        if not ok:
            # production path on terminal failure: route back to a visible DLQ.
            await route_to_dead_letter(
                db, entry, error_text=CONNECTIVITY_GUARD_SIGNATURE,
                error_type="KGPrimitiveError")
        await db.commit()
    assert ok is False  # failure surfaced, not masked as success

    # no false materialisation for the failing artifact — through the
    # registered graph_store port so the assert runs in core-only AND with
    # the real Community runtime (ts2 keeps the raw-connection coverage).
    assert _find_by_artifact_rows(board_id, f"spec:{missing_spec_id}") == []
    # the failure remains an actionable connectivity-guard technical_dlq.
    async with db_factory() as db:
        diag = await diagnose_connectivity_guard_dlq(db, board_id)
        verify = await verify_connectivity_class_cleared(
            db, board_id, artifact_refs=[f"spec:{missing_spec_id}"])
    assert any(i["artifact_id"] == missing_spec_id for i in diag["items"])
    assert all(i["next_action"] for i in diag["items"])  # actionable
    assert verify["class_cleared"] is False  # NOT masked
