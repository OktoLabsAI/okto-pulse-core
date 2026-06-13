"""CanonicalDebt service tests."""

from __future__ import annotations

import pytest

from okto_pulse.core.models.db import Board, CanonicalDebt
from okto_pulse.core.models.db import ConsolidationQueue
from okto_pulse.core.kg.workers.consolidation import ConsolidationWorker
from okto_pulse.core.services.canonical_debt_service import (
    list_canonical_debt,
    reconcile_canonical_debt_with_evidence,
    schedule_canonical_debt_retry,
    summarize_canonical_debt,
    upsert_canonical_debt,
)


BOARD_ID = "board-canonical-debt-test"
USER_ID = "user-canonical-debt-test"


@pytest.mark.asyncio
async def test_canonical_debt_summary_counts_open_states(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        session.add_all([
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s1",
                source_ref="spec:s1",
                content_hash="h1",
                target_status="validated",
                canonical_state="failed",
            ),
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="task",
                artifact_id="t1",
                source_ref="task:t1",
                content_hash="h2",
                target_status="done",
                canonical_state="committed",
            ),
        ])
        await session.commit()

        summary = await summarize_canonical_debt(session, BOARD_ID)
        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert summary["open_count"] == 1
    assert summary["retryable_count"] == 1
    assert summary["terminal_count"] == 1
    assert listed.total == 2
    assert listed.counts["open_count"] == 1


@pytest.mark.asyncio
async def test_upsert_canonical_debt_is_idempotent_by_artifact_target_hash(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        first = await upsert_canonical_debt(
            session,
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id="s2",
            source_ref="spec:s2",
            content_hash="h-same",
            target_status="validated",
            canonical_state="failed",
            failure_reason="connectivity_guard",
            last_error="first",
        )
        second = await upsert_canonical_debt(
            session,
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id="s2",
            source_ref="spec:s2",
            content_hash="h-same",
            target_status="validated",
            canonical_state="deferred",
            failure_reason="kg_health_backpressure",
            last_error="second",
        )
        await session.commit()

        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert second.id == first.id
    assert listed.total == 1
    assert listed.items[0]["canonical_state"] == "deferred"
    assert listed.items[0]["last_error"] == "second"


@pytest.mark.asyncio
async def test_upsert_canonical_debt_requires_content_hash_and_persists_no_partial_row(
    db_factory,
):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        await session.commit()

        with pytest.raises(ValueError, match="content_hash"):
            await upsert_canonical_debt(
                session,
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s-no-hash",
                source_ref="spec:s-no-hash",
                content_hash="",
                target_status="validated",
                canonical_state="failed",
                failure_reason="connectivity_guard",
                last_error="hash missing",
            )
        await session.rollback()

        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert listed.total == 0


@pytest.mark.asyncio
async def test_schedule_canonical_debt_retry_respects_health_state(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        debt = CanonicalDebt(
            board_id=BOARD_ID,
            artifact_type="refinement",
            artifact_id="r1",
            source_ref="refinement:r1",
            content_hash="h3",
            target_status="done",
            canonical_state="failed",
        )
        session.add(debt)
        await session.commit()
        await session.refresh(debt)

        blocked = await schedule_canonical_debt_retry(
            session,
            board_id=BOARD_ID,
            debt_id=debt.id,
            actor_id="agent-x",
            kg_health_state="at_risk",
        )
        scheduled = await schedule_canonical_debt_retry(
            session,
            board_id=BOARD_ID,
            debt_id=debt.id,
            actor_id="agent-x",
            kg_health_state="healthy",
        )

    assert blocked["ok"] is False
    assert blocked["attempt_consumed"] is False
    assert blocked["error"] == "kg_health_blocks_retry"
    assert scheduled["ok"] is True
    assert scheduled["attempt_consumed"] is False
    assert scheduled["debt"]["canonical_state"] == "retry_scheduled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "health_state",
    ["at_risk", "backpressure", "recovery_needed", "quarantined"],
)
async def test_schedule_canonical_debt_retry_blocks_without_consuming_attempt_for_degraded_health(
    db_factory,
    health_state: str,
):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        debt = CanonicalDebt(
            board_id=BOARD_ID,
            artifact_type="refinement",
            artifact_id=f"r-{health_state}",
            source_ref=f"refinement:r-{health_state}",
            content_hash=f"h-{health_state}",
            target_status="done",
            canonical_state="failed",
            retry_count=2,
        )
        session.add(debt)
        await session.commit()
        await session.refresh(debt)

        result = await schedule_canonical_debt_retry(
            session,
            board_id=BOARD_ID,
            debt_id=debt.id,
            actor_id="agent-x",
            kg_health_state=health_state,
        )
        await session.refresh(debt)

    assert result["ok"] is False
    assert result["attempt_consumed"] is False
    assert result["error"] == "kg_health_blocks_retry"
    assert debt.retry_count == 2
    assert debt.canonical_state == "blocked"
    assert debt.failure_reason == f"kg_health_{health_state}"


@pytest.mark.asyncio
async def test_reconcile_canonical_debt_commits_only_matching_evidence(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        session.add_all([
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s3",
                source_ref="spec:s3",
                source_version="v1",
                content_hash="hash-match",
                target_status="validated",
                canonical_state="failed",
            ),
            CanonicalDebt(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id="s4",
                source_ref="spec:s4",
                source_version="v1",
                content_hash="hash-open",
                target_status="validated",
                canonical_state="failed",
            ),
        ])
        await session.commit()

        result = await reconcile_canonical_debt_with_evidence(
            session,
            board_id=BOARD_ID,
            actor_id="agent-x",
            report_ref="report:1",
            canonical_evidence=[
                {
                    "source_ref": "spec:s3",
                    "source_version": "v1",
                    "content_hash": "hash-match",
                    "node_ref": "Spec:s3",
                },
                {
                    "source_ref": "spec:s4",
                    "source_version": "v2",
                    "content_hash": "hash-open",
                    "node_ref": "Spec:s4",
                },
            ],
        )
        await session.commit()
        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    states = {item["source_ref"]: item["canonical_state"] for item in listed.items}
    assert result["committed_count"] == 1
    assert states["spec:s3"] == "committed"
    assert states["spec:s4"] == "failed"


@pytest.mark.asyncio
async def test_consolidation_failure_marks_canonical_debt(db_factory):
    async with db_factory() as session:
        board = await session.get(Board, BOARD_ID)
        if board is None:
            session.add(Board(id=BOARD_ID, name="debt", owner_id=USER_ID))
        await session.execute(
            CanonicalDebt.__table__.delete().where(
                CanonicalDebt.board_id == BOARD_ID
            )
        )
        await session.execute(
            ConsolidationQueue.__table__.delete().where(
                ConsolidationQueue.board_id == BOARD_ID
            )
        )
        entry = ConsolidationQueue(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id="spec-failed",
            status="claimed",
            worker_id="worker-test",
        )
        session.add(entry)
        await session.flush()

        worker = ConsolidationWorker(lambda: None)
        await worker._mark_failed(
            session,
            entry,
            error_text="KG node connectivity guard rejected the commit",
            max_attempts=3,
        )
        await session.commit()

        listed = await list_canonical_debt(session, board_id=BOARD_ID)

    assert listed.total == 1
    debt = listed.items[0]
    assert debt["artifact_type"] == "spec"
    assert debt["artifact_id"] == "spec-failed"
    assert debt["canonical_state"] == "failed"
    assert debt["failure_reason"] == "consolidation_failed"
    assert debt["queue_ref"] == entry.id
