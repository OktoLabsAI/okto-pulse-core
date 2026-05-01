"""Tests for Governance module — historical opt-in, ACL, undo, retention, erasure."""

import asyncio
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

tmpdb = tempfile.mktemp(suffix=".db")
os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{tmpdb}")
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_gov_"))

from sqlalchemy import select

from okto_pulse.core.models import db as _models  # noqa: F401
from okto_pulse.core.models.db import Board, Spec, SpecStatus
from okto_pulse.core.infra.database import create_database, get_session_factory, init_db
from okto_pulse.core.kg.governance import (
    cancel_historical,
    clear_acl_violations_for_tests,
    get_acl_violations,
    get_historical_progress,
    log_acl_violation,
    pause_historical,
    purge_expired_audit,
    resume_historical,
    right_to_erasure,
    start_historical_consolidation,
    undo_session,
)

_initialized = False


@pytest.fixture(scope="module", autouse=True)
async def _db():
    global _initialized
    if not _initialized:
        create_database(f"sqlite+aiosqlite:///{tmpdb}", echo=False)
        await init_db()
        _initialized = True


@pytest.fixture(autouse=True)
def _reset_acl():
    clear_acl_violations_for_tests()


@pytest.fixture
def db_factory():
    return get_session_factory()


async def _seed_board_with_spec(db_factory, board_id: str) -> None:
    """Insert a Board + done Spec so start_historical_consolidation finds artifacts."""
    import uuid

    async with db_factory() as db:
        db.add(Board(id=board_id, name=f"Test {board_id}", owner_id="test-owner"))
        db.add(Spec(
            id=str(uuid.uuid4()),
            board_id=board_id,
            title="Seed spec",
            status=SpecStatus.DONE,
            archived=False,
            created_by="test-user",
        ))
        await db.commit()


class TestHistoricalOptIn:
    @pytest.mark.asyncio
    async def test_start_creates_queue_entry(self, db_factory):
        await _seed_board_with_spec(db_factory, "board-hist-1")
        async with db_factory() as db:
            result = await start_historical_consolidation(db, "board-hist-1")
            assert result["status"] == "queueing"
            assert result["total_artifacts"] >= 1

    @pytest.mark.asyncio
    async def test_start_twice_returns_in_progress(self, db_factory):
        await _seed_board_with_spec(db_factory, "board-hist-2")
        async with db_factory() as db:
            await start_historical_consolidation(db, "board-hist-2")
        async with db_factory() as db:
            result = await start_historical_consolidation(db, "board-hist-2")
            assert result["status"] == "already_in_progress"

    @pytest.mark.asyncio
    async def test_pause_and_resume(self, db_factory):
        await _seed_board_with_spec(db_factory, "board-hist-3")
        async with db_factory() as db:
            await start_historical_consolidation(db, "board-hist-3")
        async with db_factory() as db:
            p = await pause_historical(db, "board-hist-3")
            assert p["status"] == "paused"
        async with db_factory() as db:
            r = await resume_historical(db, "board-hist-3")
            assert r["status"] == "resumed"

    @pytest.mark.asyncio
    async def test_cancel_removes_pending(self, db_factory):
        await _seed_board_with_spec(db_factory, "board-hist-4")
        async with db_factory() as db:
            await start_historical_consolidation(db, "board-hist-4")
        async with db_factory() as db:
            c = await cancel_historical(db, "board-hist-4")
            assert c["status"] == "cancelled"
            assert c["removed"] >= 1

    @pytest.mark.asyncio
    async def test_progress_tracking(self, db_factory):
        await _seed_board_with_spec(db_factory, "board-hist-5")
        async with db_factory() as db:
            await start_historical_consolidation(db, "board-hist-5")
        async with db_factory() as db:
            prog = await get_historical_progress(db, "board-hist-5")
            assert prog["total"] >= 1
            assert prog["progress"] == 0

    @pytest.mark.asyncio
    async def test_progress_total_stays_stable_when_processed_rows_are_deleted(self, db_factory):
        """DELETE-on-ack removes queue rows, so progress must use the run total."""
        import uuid
        from okto_pulse.core.models.db import ConsolidationQueue

        board_id = "board-hist-stable-progress"
        async with db_factory() as db:
            db.add(Board(id=board_id, name="Stable Progress", owner_id="owner"))
            for idx in range(3):
                db.add(Spec(
                    id=str(uuid.uuid4()),
                    board_id=board_id,
                    title=f"Spec {idx}",
                    status=SpecStatus.DONE,
                    archived=False,
                    created_by="test-user",
                ))
            await db.commit()

        async with db_factory() as db:
            await start_historical_consolidation(db, board_id)
            initial = await get_historical_progress(db, board_id)
            assert initial["total"] == 3
            assert initial["progress"] == 0

            row = (await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.source == "historical_backfill",
                ).limit(1)
            )).scalars().one()
            await db.delete(row)
            await db.commit()

        async with db_factory() as db:
            prog = await get_historical_progress(db, board_id)
            assert prog["total"] == 3
            assert prog["progress"] == 1
            assert prog["pending"] == 2


class TestUndo:
    @pytest.mark.asyncio
    async def test_undo_not_found(self, db_factory):
        async with db_factory() as db:
            result = await undo_session(db, "board-x", "nonexistent")
            assert result["error"] == "not_found"


class TestAuditRetention:
    @pytest.mark.asyncio
    async def test_purge_unlimited_skips(self, db_factory):
        async with db_factory() as db:
            result = await purge_expired_audit(db, "board-x", retention_days=None)
            assert result["retention"] == "unlimited"
            assert result["purged"] == 0

    @pytest.mark.asyncio
    async def test_purge_with_retention(self, db_factory):
        async with db_factory() as db:
            result = await purge_expired_audit(db, "board-x", retention_days=30)
            assert result["retention_days"] == 30


class TestACLViolations:
    def test_log_and_retrieve(self):
        log_acl_violation("user-1", "board-a", "get_decision_history")
        log_acl_violation("user-1", "board-b", "find_contradictions")
        violations = get_acl_violations("user-1")
        assert len(violations) == 2
        assert violations[0]["user_id"] == "user-1"

    def test_empty_without_violations(self):
        assert get_acl_violations("user-nobody") == []


class TestRightToErasure:
    @pytest.mark.asyncio
    async def test_erasure_completes(self, db_factory):
        async with db_factory() as db:
            result = await right_to_erasure(db, "board-erasure-test")
            assert result["board_id"] == "board-erasure-test"
            assert "global_cascade" in result or "global_cascade_error" in result


class TestHistoricalDedupFilter:
    """Regression tests for the governance dedup fix.

    Before the fix, the DELETE in start_historical_consolidation only
    cleared terminal rows with source='historical_backfill', but the
    dedup SELECT scanned ALL rows regardless of status. Terminal rows
    from event-driven enqueues (event:card.created, retry_from_ui, …)
    silently poisoned the dedup set and caused every matching artifact
    to be skipped.

    Fix:
      • DELETE no longer filters by source (all terminal rows cleared)
      • SELECT restricts to live statuses: pending / claimed / paused
    """

    @pytest.mark.asyncio
    async def test_terminal_event_rows_do_not_block_historical_requeue(self, db_factory):
        """Primary regression: a terminal row from event:spec.moved must NOT
        poison the dedup set so the historical pass can re-enqueue the spec."""
        import uuid
        from okto_pulse.core.models.db import ConsolidationQueue

        board_id = "board-dedup-event-done"
        await _seed_board_with_spec(db_factory, board_id)

        # Fetch the seeded spec id so we can register a terminal event row
        # pointing at the SAME artifact. UNIQUE(board_id, artifact_type,
        # artifact_id) makes this the only possible row for that artifact.
        async with db_factory() as db:
            result = await db.execute(
                select(Spec).where(Spec.board_id == board_id)
            )
            spec = result.scalars().first()
            assert spec is not None

            db.add(ConsolidationQueue(
                id=str(uuid.uuid4()),
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec.id,
                priority="high",
                source="event:spec.moved",
                status="done",
            ))
            await db.commit()

        # Run the historical backfill — the terminal event row should be
        # cleared and the spec re-queued as historical_backfill/pending.
        async with db_factory() as db:
            result = await start_historical_consolidation(db, board_id)
            assert result["status"] == "queueing"
            assert result["total_artifacts"] >= 1

        async with db_factory() as db:
            rows = (await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_id == spec.id,
                )
            )).scalars().all()
            # UNIQUE constraint means only one row exists. It must be the
            # newly-inserted historical_backfill row, not the poisoned one.
            assert len(rows) == 1
            row = rows[0]
            assert row.status == "pending"
            assert row.source == "historical_backfill"

    @pytest.mark.asyncio
    async def test_failed_rows_are_cleared_regardless_of_source(self, db_factory):
        """retry_from_ui failed rows must be cleared on historical start
        so the next attempt gets a clean slot."""
        import uuid
        from okto_pulse.core.models.db import ConsolidationQueue

        board_id = "board-dedup-failed-retry"
        await _seed_board_with_spec(db_factory, board_id)

        async with db_factory() as db:
            spec = (await db.execute(
                select(Spec).where(Spec.board_id == board_id)
            )).scalars().first()
            assert spec is not None

            db.add(ConsolidationQueue(
                id=str(uuid.uuid4()),
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec.id,
                priority="high",
                source="retry_from_ui",
                status="failed",
                last_error="simulated prior failure",
            ))
            await db.commit()

        async with db_factory() as db:
            result = await start_historical_consolidation(db, board_id)
            assert result["status"] == "queueing"

        async with db_factory() as db:
            rows = (await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_id == spec.id,
                )
            )).scalars().all()
            assert len(rows) == 1
            row = rows[0]
            assert row.status == "pending"
            assert row.source == "historical_backfill"
            assert row.last_error is None

    @pytest.mark.asyncio
    async def test_pending_event_row_is_preserved(self, db_factory):
        """If a pending row already exists (e.g. event:card.created waiting
        to be processed), the historical pass must not try to insert a
        duplicate — the UNIQUE constraint would reject it anyway, but the
        dedup filter must skip it cleanly."""
        import uuid
        from okto_pulse.core.models.db import ConsolidationQueue

        board_id = "board-dedup-pending-preserved"
        await _seed_board_with_spec(db_factory, board_id)

        async with db_factory() as db:
            spec = (await db.execute(
                select(Spec).where(Spec.board_id == board_id)
            )).scalars().first()
            assert spec is not None

            db.add(ConsolidationQueue(
                id=str(uuid.uuid4()),
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec.id,
                priority="high",
                source="event:spec.moved",
                status="pending",
            ))
            await db.commit()

        async with db_factory() as db:
            result = await start_historical_consolidation(db, board_id)
            assert result["status"] == "queueing"

        async with db_factory() as db:
            rows = (await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_id == spec.id,
                )
            )).scalars().all()
            # Exactly one row survives (UNIQUE constraint). It must be the
            # pre-existing pending event row, NOT a new historical row.
            assert len(rows) == 1
            row = rows[0]
            assert row.status == "pending"
            assert row.source == "event:spec.moved"

    @pytest.mark.asyncio
    async def test_paused_rows_are_preserved(self, db_factory):
        """A paused historical row from a prior run must survive the dedup
        pass — start after pause should observe 'already_in_progress' via
        the pre-check, but even if that didn't trigger, the dedup must
        treat paused rows as live."""
        import uuid
        from okto_pulse.core.models.db import ConsolidationQueue

        board_id = "board-dedup-paused-preserved"
        await _seed_board_with_spec(db_factory, board_id)

        async with db_factory() as db:
            spec = (await db.execute(
                select(Spec).where(Spec.board_id == board_id)
            )).scalars().first()
            assert spec is not None

            db.add(ConsolidationQueue(
                id=str(uuid.uuid4()),
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec.id,
                priority="low",
                source="historical_backfill",
                status="paused",
            ))
            await db.commit()

        # Paused status is not in {pending, claimed}, so the "already in
        # progress" pre-check won't trigger and start_historical_consolidation
        # will proceed to the dedup / enqueue path.
        async with db_factory() as db:
            result = await start_historical_consolidation(db, board_id)
            assert result["status"] == "queueing"

        async with db_factory() as db:
            rows = (await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_id == spec.id,
                )
            )).scalars().all()
            # Exactly one row must survive — the pre-existing paused row.
            # The dedup set must include paused statuses so the historical
            # pass skips this artifact instead of trying to duplicate it.
            assert len(rows) == 1
            row = rows[0]
            assert row.status == "paused"
            assert row.source == "historical_backfill"

    @pytest.mark.asyncio
    async def test_mixed_terminal_rows_all_cleared(self, db_factory):
        """Multiple artifacts with terminal rows from DIFFERENT sources
        must all be cleared and re-queued in a single pass."""
        import uuid
        from okto_pulse.core.models.db import Board, ConsolidationQueue

        board_id = "board-dedup-mixed"

        # Seed board with THREE done specs
        async with db_factory() as db:
            db.add(Board(id=board_id, name="Mixed", owner_id="owner"))
            spec_ids = []
            for i in range(3):
                sid = str(uuid.uuid4())
                spec_ids.append(sid)
                db.add(Spec(
                    id=sid, board_id=board_id,
                    title=f"Spec {i}", status=SpecStatus.DONE,
                    archived=False, created_by="test",
                ))
            await db.commit()

        # Pollute the queue with terminal rows from different sources
        terminal_rows = [
            (spec_ids[0], "event:spec.moved", "done"),
            (spec_ids[1], "retry_from_ui", "failed"),
            (spec_ids[2], "historical_backfill", "done"),
        ]
        async with db_factory() as db:
            for artifact_id, source, status in terminal_rows:
                db.add(ConsolidationQueue(
                    id=str(uuid.uuid4()),
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=artifact_id,
                    priority="low",
                    source=source,
                    status=status,
                ))
            await db.commit()

        async with db_factory() as db:
            result = await start_historical_consolidation(db, board_id)
            assert result["status"] == "queueing"
            # All three specs must be re-queued. Other fields that
            # historical_backfill also seeds (sprints, cards) may or may
            # not bump this count; we only assert the specs are included.
            assert result["total_artifacts"] >= 3

        async with db_factory() as db:
            rows = (await db.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == "spec",
                )
            )).scalars().all()
            assert len(rows) == 3
            for row in rows:
                assert row.status == "pending"
                assert row.source == "historical_backfill"
