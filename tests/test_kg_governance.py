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

from okto_pulse.core.models import db as _models  # noqa: F401
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


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


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


class TestHistoricalOptIn:
    @pytest.mark.asyncio
    async def test_start_creates_queue_entry(self, db_factory):
        async with db_factory() as db:
            result = await start_historical_consolidation(db, "board-hist-1")
            assert result["status"] == "queueing"

    @pytest.mark.asyncio
    async def test_start_twice_returns_in_progress(self, db_factory):
        async with db_factory() as db:
            await start_historical_consolidation(db, "board-hist-2")
        async with db_factory() as db:
            result = await start_historical_consolidation(db, "board-hist-2")
            assert result["status"] == "already_in_progress"

    @pytest.mark.asyncio
    async def test_pause_and_resume(self, db_factory):
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
        async with db_factory() as db:
            await start_historical_consolidation(db, "board-hist-4")
        async with db_factory() as db:
            c = await cancel_historical(db, "board-hist-4")
            assert c["status"] == "cancelled"
            assert c["removed"] >= 1

    @pytest.mark.asyncio
    async def test_progress_tracking(self, db_factory):
        async with db_factory() as db:
            await start_historical_consolidation(db, "board-hist-5")
        async with db_factory() as db:
            prog = await get_historical_progress(db, "board-hist-5")
            assert prog["total"] >= 1
            assert prog["progress"] == 0


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
