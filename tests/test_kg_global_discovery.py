"""Tests for Global Discovery Layer — schema, cascade, clustering, GC."""

from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
import os
import sys
import tempfile

import pytest
from sqlalchemy import delete, select

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_gdt_"))

from global_graph_testing import (
    GLOBAL_SCHEMA_VERSION,
    bootstrap_global_discovery,
    execute_global_read,
    execute_global_write,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.global_discovery.clustering import (
    ENTITY_CANONICALIZATION_THRESHOLD,
    TOPIC_SIMILARITY_THRESHOLD,
    board_delete_cascade,
    cosine_similarity,
    entity_combined_score,
    gc_orphans,
    normalize_name,
    string_fuzzy_ratio,
)
from okto_pulse.core.application.processors.global_outbox import (
    DEAD_LETTER_SENTINEL,
    MAX_RETRIES,
    GlobalOutboxProcessor,
    _is_retryable_global_open_error,
)
from okto_pulse.core.kg.embedding import get_embedding_provider
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from sqlalchemy_test_models import (
    Board,
    ConsolidationAudit,
    GlobalUpdateOutbox,
    KuzuNodeRef,
)
from kg_registry_testing import configure_real_graph_test_kg_registry


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_real_graph_test_kg_registry()


@pytest.fixture(scope="module", autouse=True)
def _bootstrap():
    reset_global_discovery_runtime_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_discovery_runtime_for_tests()


class TestGlobalSchema:
    def test_bootstrap_creates_tables(self):
        tables = execute_global_read("CALL SHOW_TABLES() RETURN *").rows
        node_count = sum(1 for t in tables if t[2] == "NODE")
        rel_count = sum(1 for t in tables if t[2] == "REL")
        assert node_count == 4
        assert rel_count == 7

    def test_schema_version(self):
        assert GLOBAL_SCHEMA_VERSION == "0.1.2"

    def test_decision_digest_carries_graph_layer(self):
        columns = set()
        for row in execute_global_read(
            "CALL TABLE_INFO('DecisionDigest') RETURN *"
        ).rows:
            for cell in row:
                if isinstance(cell, str):
                    columns.add(cell)
                    break
        assert "graph_layer" in columns

    def test_corrupt_global_discovery_wal_is_preserved_and_blocks_rebootstrap(
        self, monkeypatch, tmp_path
    ):
        from okto_pulse.core.kg.interfaces import get_kg_registry

        reset_global_discovery_runtime_for_tests()
        path = tmp_path / "global" / "discovery.lbug"
        path.parent.mkdir(parents=True)
        path.write_text("bad-db", encoding="utf-8")
        wal = path.with_name("discovery.lbug.wal")
        wal.write_text("bad-wal", encoding="utf-8")

        class FakeDB:
            def close(self):
                pass

        class FakeConn:
            def execute(self, *_args, **_kwargs):
                return None

            def close(self):
                pass

        calls = {"open": 0}

        def fake_open(_path, *, on_corruption=None):
            calls["open"] += 1
            if calls["open"] == 1:
                exc = RuntimeError(
                    "Storage exception: Checksum verification failed, "
                    "the WAL file is corrupted."
                )
                if on_corruption is not None:
                    on_corruption(exc)
                raise exc
            return FakeDB()

        global_runtime = get_kg_registry().global_discovery_runtime
        monkeypatch.setattr(global_runtime, "_graph_path_provider", lambda: path)
        board_runtime = global_runtime._runtime()
        monkeypatch.setattr(board_runtime, "open_global_kuzu_db", fake_open)
        monkeypatch.setattr(board_runtime, "new_connection", lambda _db: FakeConn())
        monkeypatch.setattr(board_runtime, "load_vector_extension", lambda _conn: None)

        with pytest.raises(RuntimeError, match="refusing automatic bootstrap"):
            bootstrap_global_discovery()

        assert calls["open"] == 1
        assert path.exists()
        assert wal.exists()

    def test_board_insert_and_query(self):
        emb = get_embedding_provider().encode("test board")
        execute_global_write(
            "CREATE (b:Board {board_id: $bid, name: $n, summary: $s, "
            "summary_embedding: $emb, topic_count: 0, entity_count: 0, "
            "decision_count: 1, last_sync_at: timestamp($ts)})",
            {
                "bid": "test-schema-b",
                "n": "Test",
                "s": "",
                "emb": emb,
                "ts": "2026-04-15T10:00:00",
            },
            operation="test_global_schema_insert_board",
        )
        r = execute_global_read(
            "MATCH (b:Board {board_id: $bid}) RETURN b.decision_count",
            {"bid": "test-schema-b"},
        )
        assert r.rows[0][0] == 1
        execute_global_write(
            "MATCH (b:Board {board_id: 'test-schema-b'}) DETACH DELETE b",
            operation="test_global_schema_delete_board",
        )


class TestBoardCascade:
    def test_cascade_removes_board(self):
        emb = get_embedding_provider().encode("cascade board")
        execute_global_write(
            "CREATE (b:Board {board_id: $bid, name: $n, summary: $s, "
            "summary_embedding: $emb, topic_count: 0, entity_count: 0, "
            "decision_count: 3, last_sync_at: timestamp($ts)})",
            {
                "bid": "cascade-b",
                "n": "CB",
                "s": "",
                "emb": emb,
                "ts": "2026-04-15T10:00:00",
            },
            operation="test_global_cascade_seed_board",
        )
        counts = board_delete_cascade("cascade-b")
        assert counts["board_removed"] is True
        r = execute_global_read(
            "MATCH (b:Board {board_id: 'cascade-b'}) RETURN count(b)"
        )
        assert r.rows[0][0] == 0


class TestClustering:
    def test_normalize_name(self):
        assert normalize_name("OAuth 2.0!") == "oauth 20"
        assert normalize_name("Hello World") == "hello world"

    def test_string_fuzzy_identical(self):
        assert string_fuzzy_ratio("test", "test") == 1.0

    def test_string_fuzzy_different(self):
        r = string_fuzzy_ratio("hello", "world")
        assert r < 0.5

    def test_cosine_identical(self):
        v = [1.0, 0.0, 0.0]
        assert abs(cosine_similarity(v, v) - 1.0) < 0.001

    def test_cosine_orthogonal(self):
        a = [1.0, 0.0, 0.0]
        b = [0.0, 1.0, 0.0]
        assert abs(cosine_similarity(a, b)) < 0.001

    def test_entity_combined_score(self):
        s = entity_combined_score(0.9, 0.8, 1.0)
        expected = 0.6 * 0.9 + 0.3 * 0.8 + 0.1 * 1.0
        assert abs(s - expected) < 0.001

    def test_thresholds(self):
        assert TOPIC_SIMILARITY_THRESHOLD == 0.75
        assert ENTITY_CANONICALIZATION_THRESHOLD == 0.85


class TestGC:
    def test_gc_dry_run_no_modify(self):
        counts = gc_orphans(dry_run=True)
        assert counts["dry_run"] is True
        assert isinstance(counts["topics_removed"], int)
        assert isinstance(counts["entities_removed"], int)


class TestGlobalOutboxProcessor:
    def test_max_retries(self):
        assert MAX_RETRIES == 5
        assert DEAD_LETTER_SENTINEL == -1

    def test_global_graph_error_codes_are_retryable(self):
        assert _is_retryable_global_open_error(
            "graph_corruption:global graph could not be opened"
        )
        assert _is_retryable_global_open_error(
            "graph_unavailable:global graph temporarily unavailable"
        )
        assert _is_retryable_global_open_error(
            "graph_lock_contention:global lifecycle reader still active"
        )
        assert _is_retryable_global_open_error(
            "graph_memory_pressure:global allocator is cooling down"
        )

    @pytest.mark.asyncio
    async def _case_post_flush_verification_isolated_per_board(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid
        import okto_pulse.core.application.processors.global_outbox as worker_mod

        healthy_board = f"board-healthy-{uuid.uuid4().hex[:8]}"
        toxic_board = f"board-toxic-{uuid.uuid4().hex[:8]}"
        event_ids = {
            healthy_board: str(uuid.uuid4()),
            toxic_board: str(uuid.uuid4()),
        }
        async with db_factory() as db:
            await db.execute(delete(GlobalUpdateOutbox))
            for board_id in event_ids:
                db.add(Board(id=board_id, name=board_id, owner_id="owner"))
                db.add(
                    GlobalUpdateOutbox(
                        event_id=event_ids[board_id],
                        board_id=board_id,
                        session_id=f"kgses_{uuid.uuid4().hex[:16]}",
                        event_type="consolidation_committed",
                        payload={"session_id": "", "nodes_added": 0},
                    )
                )
            await db.commit()

        class _Runtime:
            @contextmanager
            def post_write_verification_scope(self):
                yield

            def close(self):
                return None

        runtime = _Runtime()
        source_rechecks: list[tuple[str, dict[str, str]]] = []

        async def fake_apply_event(self, event, db):
            del self, db
            return {f"source-{event.board_id}": ("canonical", "Decision")}

        def fake_verify(self, _runtime, board_id, _expected):
            del self, _runtime
            if board_id == toxic_board:
                raise RuntimeError(
                    "outbox.digest_reconcile_verification_failed: toxic-board"
                )

        monkeypatch.setattr(worker_mod, "_global_discovery_runtime", lambda: runtime)
        monkeypatch.setattr(GlobalOutboxProcessor, "_apply_event", fake_apply_event)
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_flush_global_discovery_storage_after_batch",
            lambda self: None,
        )
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_assert_source_inventory_unchanged",
            staticmethod(
                lambda selected_board, types: source_rechecks.append(
                    (selected_board, dict(types))
                )
            ),
        )
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_verify_reconciled_digest_layers",
            fake_verify,
        )

        assert await GlobalOutboxProcessor(db_factory).process_once() == 1
        assert {board for board, _types in source_rechecks} == {
            healthy_board,
            toxic_board,
        }

        async with db_factory() as db:
            rows = (
                (
                    await db.execute(
                        select(GlobalUpdateOutbox).where(
                            GlobalUpdateOutbox.event_id.in_(event_ids.values())
                        )
                    )
                )
                .scalars()
                .all()
            )
            by_board = {row.board_id: row for row in rows}
            assert by_board[healthy_board].processed_at is not None
            assert by_board[healthy_board].retry_count == 0
            assert by_board[toxic_board].processed_at is None
            assert by_board[toxic_board].retry_count == 1

    @pytest.mark.asyncio
    async def _case_terminal_source_inventory_churn_requires_operator_reprocess(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid
        import okto_pulse.core.application.processors.global_outbox as worker_mod

        board_id = f"board-churn-recover-{uuid.uuid4().hex[:8]}"
        event_id = str(uuid.uuid4())
        async with db_factory() as db:
            await db.execute(delete(GlobalUpdateOutbox))
            db.add(Board(id=board_id, name=board_id, owner_id="owner"))
            db.add(
                GlobalUpdateOutbox(
                    event_id=event_id,
                    board_id=board_id,
                    session_id=f"kgses_{uuid.uuid4().hex[:16]}",
                    event_type="consolidation_committed",
                    payload={"session_id": "", "nodes_added": 0},
                    retry_count=DEAD_LETTER_SENTINEL,
                    last_error=(
                        "global_discovery_post_flush_verification_failed: "
                        "outbox.source_inventory_changed: board churned"
                    ),
                )
            )
            await db.commit()

        class _Runtime:
            @contextmanager
            def post_write_verification_scope(self):
                yield

            def close(self):
                return None

        async def fake_apply_event(self, event, db):
            del self, event, db
            return {}

        class _Clock:
            def __init__(self):
                self.current = datetime(2026, 7, 15, tzinfo=timezone.utc)

            def now(self):
                return self.current

            def advance(self, seconds):
                self.current += timedelta(seconds=seconds)

        clock = _Clock()

        monkeypatch.setattr(worker_mod, "_global_discovery_runtime", _Runtime)
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_read_board_digestable_node_types",
            staticmethod(lambda _board_id: {"stable-source": "Decision"}),
        )
        monkeypatch.setattr(GlobalOutboxProcessor, "_apply_event", fake_apply_event)
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_flush_global_discovery_storage_after_batch",
            lambda self: None,
        )
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_assert_source_inventory_unchanged",
            staticmethod(lambda _board_id, _types: None),
        )
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_verify_reconciled_digest_layers",
            lambda self, runtime, selected_board, expected: None,
        )

        worker = GlobalOutboxProcessor(db_factory, clock=clock)
        # Backend health and elapsed stability never authorize an implicit
        # terminal-DLQ mutation.  Only the explicit operator service may
        # reopen an immutable selected ID.
        assert await worker.process_once() == 0
        clock.advance(31)
        assert await worker.process_once() == 0
        async with db_factory() as db:
            row = (
                await db.execute(
                    select(GlobalUpdateOutbox).where(
                        GlobalUpdateOutbox.event_id == event_id
                    )
                )
            ).scalar_one()
            assert row.processed_at is None
            assert row.retry_count == DEAD_LETTER_SENTINEL
            assert row.last_error == (
                "global_discovery_post_flush_verification_failed: "
                "outbox.source_inventory_changed: board churned"
            )
            await db.execute(delete(GlobalUpdateOutbox))
            await db.commit()

    @pytest.mark.asyncio
    async def test_recovered_global_backend_does_not_implicitly_requeue_terminal(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid
        import okto_pulse.core.application.processors.global_outbox as worker_mod

        board_id = f"board-outbox-recover-{uuid.uuid4().hex[:8]}"
        event_id = str(uuid.uuid4())
        session_id = f"kgses_{uuid.uuid4().hex[:16]}"
        async with db_factory() as db:
            await db.execute(delete(GlobalUpdateOutbox))
            db.add(
                GlobalUpdateOutbox(
                    event_id=event_id,
                    board_id=board_id,
                    session_id=session_id,
                    event_type="consolidation_committed",
                    payload={"session_id": session_id, "nodes_added": 1},
                    retry_count=DEAD_LETTER_SENTINEL,
                    last_error="graph_corruption:global graph could not be opened",
                )
            )
            await db.commit()

        calls = {"open": 0, "apply": 0}

        class FakeConn:
            pass

        def fake_global_execute(*_args, **_kwargs):
            calls["open"] += 1
            return GraphStatementResult()

        async def fake_apply_event(self, event, db):
            calls["apply"] += 1
            return {}

        monkeypatch.setattr(
            worker_mod._global_discovery_runtime(),
            "execute",
            fake_global_execute,
        )
        monkeypatch.setattr(GlobalOutboxProcessor, "_apply_event", fake_apply_event)
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_flush_global_discovery_storage_after_batch",
            lambda self: None,
        )

        worker = GlobalOutboxProcessor(db_factory, interval_seconds=5)
        processed = await worker.process_once()

        assert processed == 0
        assert calls == {"open": 0, "apply": 0}

        async with db_factory() as db:
            row = (
                await db.execute(
                    select(GlobalUpdateOutbox).where(
                        GlobalUpdateOutbox.event_id == event_id
                    )
                )
            ).scalar_one()
            assert row.processed_at is None
            assert row.retry_count == DEAD_LETTER_SENTINEL
            assert row.last_error == "graph_corruption:global graph could not be opened"
            await db.execute(delete(GlobalUpdateOutbox))
            await db.commit()

    @pytest.mark.asyncio
    async def test_terminal_board_read_failure_requires_explicit_operator_reprocess(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid

        board_id = f"board-outbox-board-recover-{uuid.uuid4().hex[:8]}"
        event_id = str(uuid.uuid4())
        session_id = f"kgses_{uuid.uuid4().hex[:16]}"
        async with db_factory() as db:
            await db.execute(delete(GlobalUpdateOutbox))
            db.add(
                GlobalUpdateOutbox(
                    event_id=event_id,
                    board_id=board_id,
                    session_id=session_id,
                    event_type="consolidation_committed",
                    payload={"session_id": session_id, "nodes_added": 1},
                    retry_count=DEAD_LETTER_SENTINEL,
                    last_error=(
                        "outbox.read_board_failed: could not read source graph nodes"
                    ),
                )
            )
            await db.commit()

        async def fail_if_applied(self, event, db):
            raise AssertionError("terminal event requires explicit operator selection")

        monkeypatch.setattr(GlobalOutboxProcessor, "_apply_event", fail_if_applied)

        worker = GlobalOutboxProcessor(db_factory, interval_seconds=5)
        processed = await worker.process_once()

        assert processed == 0

        async with db_factory() as db:
            row = (
                await db.execute(
                    select(GlobalUpdateOutbox).where(
                        GlobalUpdateOutbox.event_id == event_id
                    )
                )
            ).scalar_one()
            assert row.processed_at is None
            assert row.retry_count == DEAD_LETTER_SENTINEL
            assert row.last_error == (
                "outbox.read_board_failed: could not read source graph nodes"
            )
            await db.execute(delete(GlobalUpdateOutbox))
            await db.commit()

    @pytest.mark.asyncio
    async def test_terminal_dead_letter_requires_explicit_operator_reprocess(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid

        terminal_event_id = str(uuid.uuid4())
        active_event_id = str(uuid.uuid4())
        async with db_factory() as db:
            await db.execute(delete(GlobalUpdateOutbox))
            db.add(
                GlobalUpdateOutbox(
                    event_id=terminal_event_id,
                    board_id=f"board-outbox-terminal-{uuid.uuid4().hex[:8]}",
                    session_id=f"kgses_{uuid.uuid4().hex[:16]}",
                    event_type="consolidation_committed",
                    payload={"session_id": "ignored"},
                    retry_count=DEAD_LETTER_SENTINEL,
                    last_error="graph_unavailable: backend is queryable again",
                )
            )
            db.add(
                GlobalUpdateOutbox(
                    event_id=active_event_id,
                    board_id=f"board-outbox-active-{uuid.uuid4().hex[:8]}",
                    session_id=f"kgses_{uuid.uuid4().hex[:16]}",
                    event_type="consolidation_committed",
                    payload={"session_id": "active-retry"},
                    retry_count=1,
                    last_error="graph_unavailable: prior transient failure",
                )
            )
            await db.commit()

        applied_event_ids: list[str] = []

        async def apply_active_retry(self, event, db):
            assert event.event_id == active_event_id
            applied_event_ids.append(event.event_id)
            return {}

        def verify_active_retry(self, processed_events):
            assert [event.event_id for event, _expected in processed_events] == [
                active_event_id
            ]
            return {}, None

        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_apply_event",
            apply_active_retry,
        )
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_verify_processed_batch",
            verify_active_retry,
        )

        worker = GlobalOutboxProcessor(db_factory, interval_seconds=5)
        processed = await worker.process_once()

        assert processed == 1
        assert applied_event_ids == [active_event_id]
        async with db_factory() as db:
            terminal_row = (
                await db.execute(
                    select(GlobalUpdateOutbox).where(
                        GlobalUpdateOutbox.event_id == terminal_event_id
                    )
                )
            ).scalar_one()
            active_row = (
                await db.execute(
                    select(GlobalUpdateOutbox).where(
                        GlobalUpdateOutbox.event_id == active_event_id
                    )
                )
            ).scalar_one()
            assert terminal_row.processed_at is None
            assert terminal_row.retry_count == DEAD_LETTER_SENTINEL
            assert (
                terminal_row.last_error
                == "graph_unavailable: backend is queryable again"
            )
            assert active_row.processed_at is not None
            assert active_row.retry_count == 1
            assert active_row.last_error is None
            await db.execute(delete(GlobalUpdateOutbox))
            await db.commit()

    @pytest.mark.asyncio
    async def test_process_once_uses_global_guard_and_flushes_after_batch(
        self,
        db_factory,
        monkeypatch,
    ):
        """Global discovery writes must run under the global guard and the
        worker must run the post-batch flush/probe before marking success
        durable.
        """
        import uuid
        from okto_pulse.core.kg.write_barrier import require_global_write_token

        event_id = str(uuid.uuid4())
        async with db_factory() as db:
            await db.execute(delete(GlobalUpdateOutbox))
            db.add(
                GlobalUpdateOutbox(
                    event_id=event_id,
                    board_id=f"board-outbox-guard-{uuid.uuid4().hex[:8]}",
                    session_id=f"kgses_{uuid.uuid4().hex[:16]}",
                    event_type="consolidation_committed",
                    payload={"session_id": "ignored", "nodes_added": 0},
                )
            )
            await db.commit()

        calls = {"apply": 0, "flush": 0}

        async def fake_apply_event(self, event, db):
            require_global_write_token()
            calls["apply"] += 1
            return {}

        def fake_flush(self):
            calls["flush"] += 1

        monkeypatch.setattr(GlobalOutboxProcessor, "_apply_event", fake_apply_event)
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_flush_global_discovery_storage_after_batch",
            fake_flush,
        )

        worker = GlobalOutboxProcessor(db_factory, interval_seconds=5)
        processed = await worker.process_once()

        assert processed == 1
        assert calls == {"apply": 1, "flush": 1}

        async with db_factory() as db:
            row = (
                await db.execute(
                    select(GlobalUpdateOutbox).where(
                        GlobalUpdateOutbox.event_id == event_id
                    )
                )
            ).scalar_one()
            assert row.processed_at is not None
            await db.execute(delete(GlobalUpdateOutbox))
            await db.commit()

    @pytest.mark.asyncio
    async def test_board_read_failure_keeps_event_retryable(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid
        import okto_pulse.core.application.processors.global_outbox as worker_mod

        board_id = f"board-outbox-read-fail-{uuid.uuid4().hex[:8]}"
        event_id = str(uuid.uuid4())
        session_id = f"kgses_{uuid.uuid4().hex[:16]}"
        async with db_factory() as db:
            db.add(
                Board(
                    id=board_id,
                    name=f"Global Outbox Read Failure {board_id}",
                    owner_id="global-outbox-test",
                )
            )
            await db.flush()
            now = datetime.now(timezone.utc)
            db.add(
                ConsolidationAudit(
                    session_id=session_id,
                    board_id=board_id,
                    artifact_id="global-outbox-read-failure",
                    artifact_type="test",
                    agent_id="global-outbox-test",
                    started_at=now,
                    committed_at=now,
                )
            )
            await db.flush()
            db.add(
                KuzuNodeRef(
                    session_id=session_id,
                    board_id=board_id,
                    kuzu_node_id="entity_source",
                    kuzu_node_type="Entity",
                    operation="add",
                )
            )
            db.add(
                GlobalUpdateOutbox(
                    event_id=event_id,
                    board_id=board_id,
                    session_id=session_id,
                    event_type="consolidation_committed",
                    payload={"session_id": session_id, "nodes_added": 1},
                )
            )
            await db.commit()

        class FakeResult:
            def has_next(self):
                return False

        class FakeConn:
            def execute(self, *_args, **_kwargs):
                return FakeResult()

        monkeypatch.setattr(
            worker_mod._global_discovery_runtime(),
            "execute",
            lambda *_args, **_kwargs: GraphStatementResult(),
        )
        monkeypatch.setattr(
            GlobalOutboxProcessor,
            "_read_board_nodes_for_refs",
            staticmethod(lambda _board_id, _refs: None),
        )

        worker = GlobalOutboxProcessor(db_factory, interval_seconds=5)
        processed = await worker.process_once()

        assert processed == 0
        async with db_factory() as db:
            row = (
                await db.execute(
                    select(GlobalUpdateOutbox).where(
                        GlobalUpdateOutbox.event_id == event_id
                    )
                )
            ).scalar_one()
            assert row.processed_at is None
            assert row.retry_count == 1
            assert "outbox.read_board_failed" in (row.last_error or "")


@pytest.mark.asyncio
async def test_post_flush_verification_isolated_per_board(
    db_factory,
    monkeypatch,
):
    await TestGlobalOutboxProcessor()._case_post_flush_verification_isolated_per_board(
        db_factory,
        monkeypatch,
    )


@pytest.mark.asyncio
async def test_terminal_source_inventory_churn_requires_operator_reprocess(
    db_factory,
    monkeypatch,
):
    await TestGlobalOutboxProcessor()._case_terminal_source_inventory_churn_requires_operator_reprocess(
        db_factory,
        monkeypatch,
    )


@pytest.mark.asyncio
async def test_post_flush_source_inventory_change_prevents_ack(
    db_factory,
    monkeypatch,
):
    import uuid
    import okto_pulse.core.application.processors.global_outbox as worker_mod

    board_id = f"board-postflush-churn-{uuid.uuid4().hex[:8]}"
    event_id = str(uuid.uuid4())
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        db.add(Board(id=board_id, name=board_id, owner_id="owner"))
        db.add(
            GlobalUpdateOutbox(
                event_id=event_id,
                board_id=board_id,
                session_id=f"kgses_{uuid.uuid4().hex[:16]}",
                event_type="consolidation_committed",
                payload={"session_id": "", "nodes_added": 0},
            )
        )
        await db.commit()

    class _Runtime:
        @contextmanager
        def post_write_verification_scope(self):
            yield

        def close(self):
            return None

    async def fake_apply_event(self, event, db):
        del self, event, db
        return {"source-a": ("canonical", "Decision")}

    def inventory_changed(_board_id, _types):
        raise RuntimeError("outbox.source_inventory_changed: concurrent remove/insert")

    monkeypatch.setattr(worker_mod, "_global_discovery_runtime", _Runtime)
    monkeypatch.setattr(GlobalOutboxProcessor, "_apply_event", fake_apply_event)
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_flush_global_discovery_storage_after_batch",
        lambda self: None,
    )
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_assert_source_inventory_unchanged",
        staticmethod(inventory_changed),
    )
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_verify_reconciled_digest_layers",
        lambda *_args: pytest.fail("global verify must follow source revalidation"),
    )

    assert await GlobalOutboxProcessor(db_factory).process_once() == 0
    async with db_factory() as db:
        row = (
            await db.execute(
                select(GlobalUpdateOutbox).where(
                    GlobalUpdateOutbox.event_id == event_id
                )
            )
        ).scalar_one()
        assert row.processed_at is None
        assert row.retry_count == 1
        assert "outbox.source_inventory_changed" in (row.last_error or "")
