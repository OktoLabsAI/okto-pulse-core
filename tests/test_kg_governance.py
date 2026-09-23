"""Tests for Governance module — historical opt-in, ACL, undo, retention, erasure."""

# ruff: noqa: E402

import os
import sys
import tempfile

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

tmpdb = tempfile.mktemp(suffix=".db")
os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{tmpdb}")
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_gov_"))


import sqlalchemy_test_models as _models  # noqa: F401
from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.infra.database import create_database, get_session_factory, init_db
from okto_pulse.core.kg.governance import (
    clear_acl_violations_for_tests,
    get_acl_violations,
    log_acl_violation,
    purge_expired_audit,
    right_to_erasure,
    undo_session,
)

_initialized = False


@pytest_asyncio.fixture(
    scope="module",
    autouse=True,
    loop_scope="module",
)
async def _db():
    global _initialized
    from okto_pulse.core.ports.relational_runtime import (
        configure_database_runtime,
        resolve_database_runtime,
    )

    original_runtime = resolve_database_runtime()
    if not _initialized:
        create_database(f"sqlite+aiosqlite:///{tmpdb}", echo=False)
        await init_db()
        _initialized = True
    yield
    current_runtime = resolve_database_runtime()
    if current_runtime is not original_runtime:
        try:
            await current_runtime.close()
        finally:
            configure_database_runtime(runtime=original_runtime)


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
        db.add(
            Spec(
                id=str(uuid.uuid4()),
                board_id=board_id,
                title="Seed spec",
                status=SpecStatus.DONE,
                archived=False,
                created_by="test-user",
            )
        )
        await db.commit()







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
    def test_erasure_lease_does_not_extend_writer_after_reservation_loss(self):
        from okto_pulse.core.kg.governance import BoardErasureLease

        calls: list[str] = []

        class _Reservation:
            def renew(self, **_kwargs):
                calls.append("reservation")
                return False

        class _Writer:
            def renew(self, **_kwargs):
                calls.append("writer")
                return True

        lease = BoardErasureLease(
            board_id="board-reservation-lost",
            writer_lock=_Writer(),
            owner_token="writer-token",
            ttl_seconds=60,
            operation_reservation=_Reservation(),
            reservation_token="reservation-token",
        )

        assert lease.renew() is False
        assert calls == ["reservation"]

    @pytest.mark.asyncio
    async def test_erasure_completes(self, db_factory):
        async with db_factory() as db:
            result = await right_to_erasure(db, "board-erasure-test")
            assert result["board_id"] == "board-erasure-test"
            assert "global_cascade" in result or "global_cascade_error" in result

    @pytest.mark.asyncio
    async def test_strict_erasure_propagates_global_cascade_failure(
        self,
        monkeypatch,
    ):
        from okto_pulse.core.kg.global_discovery import clustering

        def _fail_cascade(*_args, **_kwargs):
            raise RuntimeError("cascade failed")

        monkeypatch.setattr(clustering, "board_delete_cascade", _fail_cascade)
        with pytest.raises(RuntimeError, match="cascade failed"):
            await right_to_erasure(
                object(),
                "board-strict-erasure",
                strict=True,
                commit=False,
            )

    @pytest.mark.asyncio
    async def test_board_erasure_scope_contention_is_fail_closed(
        self,
        monkeypatch,
    ):
        from types import SimpleNamespace

        from okto_pulse.core.kg import governance
        from okto_pulse.core.kg import single_writer_lock

        released: list[tuple[str, str]] = []

        class _ContendedLock:
            def __init__(self, **_kwargs):
                pass

            def bind_write_lock_port(self):
                return object()

            def acquire(self, **_kwargs):
                return SimpleNamespace(
                    acquired=False,
                    owner_token=None,
                    current_owner="worker-1",
                )

        class _ReservationLock:
            def __init__(self, **_kwargs):
                pass

            def acquire(self, **_kwargs):
                return SimpleNamespace(
                    acquired=True,
                    owner_token="reservation-token",
                    current_owner=None,
                )

            def release(self, *, board_id, owner_token):
                released.append((board_id, owner_token))
                return True

        monkeypatch.setattr(
            single_writer_lock,
            "KGSingleWriterLock",
            _ContendedLock,
        )
        monkeypatch.setattr(
            single_writer_lock,
            "KGAdministrativeOperationReservation",
            _ReservationLock,
        )
        with pytest.raises(
            governance.BoardErasureLockContention,
            match="current_owner=worker-1",
        ):
            async with governance.board_erasure_scope(
                "board-contended",
                actor_id="owner",
            ):
                pytest.fail("a contended erasure scope must never yield")
        assert released == [("board-contended", "reservation-token")]

    @pytest.mark.asyncio
    async def test_board_erasure_contention_is_not_masked_by_release_crash(
        self,
        monkeypatch,
    ):
        from types import SimpleNamespace

        from okto_pulse.core.kg import governance, single_writer_lock

        class CleanupCrash(BaseException):
            pass

        class _ContendedLock:
            def __init__(self, **_kwargs):
                pass

            def bind_write_lock_port(self):
                return object()

            def acquire(self, **_kwargs):
                return SimpleNamespace(
                    acquired=False,
                    owner_token=None,
                    current_owner="worker-1",
                )

        class _ReservationLock:
            def __init__(self, **_kwargs):
                pass

            def acquire(self, **_kwargs):
                return SimpleNamespace(
                    acquired=True,
                    owner_token="reservation-token",
                    current_owner=None,
                )

            def release(self, **_kwargs):
                raise CleanupCrash("cleanup failed")

        monkeypatch.setattr(single_writer_lock, "KGSingleWriterLock", _ContendedLock)
        monkeypatch.setattr(
            single_writer_lock,
            "KGAdministrativeOperationReservation",
            _ReservationLock,
        )

        with pytest.raises(
            governance.BoardErasureLockContention,
            match="current_owner=worker-1",
        ):
            async with governance.board_erasure_scope(
                "board-contended-cleanup",
                actor_id="owner",
            ):
                pytest.fail("contended scope must not yield")

    @pytest.mark.asyncio
    async def test_board_erasure_scope_releases_lease_on_cancellation(
        self,
        monkeypatch,
    ):
        import asyncio
        from types import SimpleNamespace

        from okto_pulse.core.kg import governance
        from okto_pulse.core.kg import global_discovery_writer
        from okto_pulse.core.kg import single_writer_lock

        released: list[tuple[str, str]] = []

        class _Lock:
            def __init__(self, **_kwargs):
                pass

            def bind_write_lock_port(self):
                return object()

            def acquire(self, **_kwargs):
                return SimpleNamespace(
                    acquired=True,
                    owner_token="lease-token",
                    current_owner=None,
                )

            def renew(self, **_kwargs):
                return True

            def is_owner(self, _board_id, _owner_token):
                return True

            def release(self, *, board_id, owner_token):
                released.append((board_id, owner_token))
                return True

        monkeypatch.setattr(single_writer_lock, "KGSingleWriterLock", _Lock)
        monkeypatch.setattr(
            single_writer_lock,
            "KGAdministrativeOperationReservation",
            _Lock,
        )
        monkeypatch.setattr(global_discovery_writer, "KGSingleWriterLock", _Lock)
        with pytest.raises(asyncio.CancelledError):
            async with governance.board_erasure_scope(
                "board-cancelled",
                actor_id="owner",
            ):
                raise asyncio.CancelledError

        assert released == [
            ("_global", "lease-token"),
            ("board-cancelled", "lease-token"),
            ("board-cancelled", "lease-token"),
        ]

    @pytest.mark.asyncio
    async def test_strict_erasure_waits_for_destructive_thread_on_cancellation(
        self,
        monkeypatch,
    ):
        import asyncio
        import threading

        from okto_pulse.core.kg.global_discovery import clustering

        started = threading.Event()
        finish = threading.Event()

        def _slow_cascade(*_args, **_kwargs):
            started.set()
            assert finish.wait(timeout=5)
            return {"verified_absent": True}

        monkeypatch.setattr(clustering, "board_delete_cascade", _slow_cascade)
        erasure = asyncio.create_task(
            right_to_erasure(
                object(),
                "board-cancelled-thread",
                strict=True,
                commit=False,
            )
        )
        assert await asyncio.to_thread(started.wait, 2)
        erasure.cancel()
        await asyncio.sleep(0)
        assert not erasure.done()

        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await erasure

    def test_board_erasure_serializes_same_token_renewals(self):
        import time
        from concurrent.futures import ThreadPoolExecutor
        from threading import Lock

        from okto_pulse.core.kg.governance import BoardErasureLease

        state_lock = Lock()
        active = 0
        max_active = 0

        class _WriterLock:
            def renew(self, **_kwargs):
                nonlocal active, max_active
                with state_lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.02)
                with state_lock:
                    active -= 1
                return True

        lease = BoardErasureLease(
            board_id="board-renew",
            writer_lock=_WriterLock(),
            owner_token="token",
            ttl_seconds=300,
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _index: lease.renew(), range(2)))

        assert results == [True, True]
        assert max_active == 1
