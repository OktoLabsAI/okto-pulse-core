"""Tests for Global Discovery Layer — schema, cascade, clustering, GC."""

import os
import sys
import tempfile
import types

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_gdt_"))

from okto_pulse.core.kg.global_discovery.schema import (
    GLOBAL_SCHEMA_VERSION,
    bootstrap_global_discovery,
    open_global_connection,
    reset_global_db_for_tests,
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
from okto_pulse.core.kg.global_discovery.outbox_worker import (
    DEAD_LETTER_SENTINEL,
    MAX_RETRIES,
    OutboxWorker,
    _is_retryable_global_open_error,
)
from okto_pulse.core.kg.embedding import get_embedding_provider
from okto_pulse.core.models.db import GlobalUpdateOutbox, KuzuNodeRef


@pytest.fixture(scope="module", autouse=True)
def _bootstrap():
    reset_global_db_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_db_for_tests()


class TestGlobalSchema:
    def test_bootstrap_creates_tables(self):
        db, conn = open_global_connection()
        r = conn.execute("CALL SHOW_TABLES() RETURN *")
        tables = []
        while r.has_next():
            tables.append(r.get_next())
        node_count = sum(1 for t in tables if t[2] == "NODE")
        rel_count = sum(1 for t in tables if t[2] == "REL")
        assert node_count == 4
        assert rel_count == 7
        del conn

    def test_schema_version(self):
        assert GLOBAL_SCHEMA_VERSION == "0.1.0"

    def test_corrupt_global_discovery_wal_is_purged_before_rebootstrap(self, monkeypatch, tmp_path):
        from okto_pulse.core.kg import schema as kg_schema
        from okto_pulse.core.kg.global_discovery import schema as global_schema

        reset_global_db_for_tests()
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

        def fake_open(_path):
            calls["open"] += 1
            if calls["open"] == 1:
                raise RuntimeError(
                    "Runtime exception: Corrupted wal file. "
                    "Read out invalid WAL record type."
                )
            return FakeDB()

        monkeypatch.setattr(global_schema, "_global_kuzu_path", lambda: path)
        monkeypatch.setattr(kg_schema, "_open_kuzu_db", fake_open)
        monkeypatch.setattr(kg_schema, "load_vector_extension", lambda _conn: None)
        monkeypatch.setitem(
            sys.modules,
            "ladybug",
            types.SimpleNamespace(Connection=lambda _db: FakeConn()),
        )

        bootstrap_global_discovery()

        assert calls["open"] == 2
        assert not wal.exists()

    def test_board_insert_and_query(self):
        db, conn = open_global_connection()
        emb = get_embedding_provider().encode("test board")
        conn.execute(
            "CREATE (b:Board {board_id: $bid, name: $n, summary: $s, "
            "summary_embedding: $emb, topic_count: 0, entity_count: 0, "
            "decision_count: 1, last_sync_at: timestamp($ts)})",
            {"bid": "test-schema-b", "n": "Test", "s": "", "emb": emb,
             "ts": "2026-04-15T10:00:00"},
        )
        r = conn.execute(
            "MATCH (b:Board {board_id: $bid}) RETURN b.decision_count",
            {"bid": "test-schema-b"},
        )
        assert r.get_next()[0] == 1
        conn.execute(
            "MATCH (b:Board {board_id: 'test-schema-b'}) DETACH DELETE b"
        )
        del conn


class TestBoardCascade:
    def test_cascade_removes_board(self):
        db, conn = open_global_connection()
        emb = get_embedding_provider().encode("cascade board")
        conn.execute(
            "CREATE (b:Board {board_id: $bid, name: $n, summary: $s, "
            "summary_embedding: $emb, topic_count: 0, entity_count: 0, "
            "decision_count: 3, last_sync_at: timestamp($ts)})",
            {"bid": "cascade-b", "n": "CB", "s": "", "emb": emb,
             "ts": "2026-04-15T10:00:00"},
        )
        del conn
        counts = board_delete_cascade("cascade-b")
        assert counts["board_removed"] is True
        db, conn = open_global_connection()
        r = conn.execute(
            "MATCH (b:Board {board_id: 'cascade-b'}) RETURN count(b)"
        )
        assert r.get_next()[0] == 0
        del conn


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


class TestOutboxWorker:
    def test_max_retries(self):
        assert MAX_RETRIES == 5
        assert DEAD_LETTER_SENTINEL == -1

    def test_global_open_wal_error_is_retryable(self):
        assert _is_retryable_global_open_error(
            "Failed to open LadybugDB database at "
            "C:/Users/me/.okto-pulse/global/discovery.lbug: "
            "RuntimeError: Assertion failed in file "
            "wal_record.cpp on line 76: UNREACHABLE_CODE"
        )

    @pytest.mark.asyncio
    async def test_dead_lettered_global_open_failure_is_requeued_and_processed(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid
        import okto_pulse.core.kg.global_discovery.outbox_worker as worker_mod

        board_id = f"board-outbox-recover-{uuid.uuid4().hex[:8]}"
        event_id = str(uuid.uuid4())
        session_id = f"kgses_{uuid.uuid4().hex[:16]}"
        async with db_factory() as db:
            db.add(GlobalUpdateOutbox(
                event_id=event_id,
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"session_id": session_id, "nodes_added": 1},
                retry_count=DEAD_LETTER_SENTINEL,
                last_error=(
                    "Failed to open LadybugDB database at "
                    "C:/Users/me/.okto-pulse/global/discovery.lbug: "
                    "RuntimeError: Assertion failed in file "
                    "wal_record.cpp on line 76: UNREACHABLE_CODE"
                ),
            ))
            await db.commit()

        calls = {"open": 0, "apply": 0}

        class FakeConn:
            pass

        def fake_open_global_connection():
            calls["open"] += 1
            return object(), FakeConn()

        async def fake_apply_event(self, event, db):
            calls["apply"] += 1

        monkeypatch.setattr(
            worker_mod,
            "open_global_connection",
            fake_open_global_connection,
        )
        monkeypatch.setattr(OutboxWorker, "_apply_event", fake_apply_event)

        worker = OutboxWorker(db_factory, interval_seconds=5)
        processed = await worker.process_once()

        assert processed == 1
        assert calls == {"open": 1, "apply": 1}

        async with db_factory() as db:
            row = (
                await db.execute(
                    worker_mod.select(GlobalUpdateOutbox)
                    .where(GlobalUpdateOutbox.event_id == event_id)
                )
            ).scalar_one()
            assert row.processed_at is not None
            assert row.retry_count == 0
            assert row.last_error is None

    @pytest.mark.asyncio
    async def test_non_global_dead_letter_is_not_requeued(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid
        import okto_pulse.core.kg.global_discovery.outbox_worker as worker_mod

        event_id = str(uuid.uuid4())
        async with db_factory() as db:
            db.add(GlobalUpdateOutbox(
                event_id=event_id,
                board_id=f"board-outbox-non-recover-{uuid.uuid4().hex[:8]}",
                session_id=f"kgses_{uuid.uuid4().hex[:16]}",
                event_type="consolidation_committed",
                payload={"session_id": "ignored"},
                retry_count=DEAD_LETTER_SENTINEL,
                last_error="invalid payload shape",
            ))
            await db.commit()

        def fail_if_called():
            raise AssertionError("global recovery should not run")

        monkeypatch.setattr(
            worker_mod,
            "open_global_connection",
            fail_if_called,
        )

        worker = OutboxWorker(db_factory, interval_seconds=5)
        processed = await worker.process_once()

        assert processed == 0
        async with db_factory() as db:
            row = (
                await db.execute(
                    worker_mod.select(GlobalUpdateOutbox)
                    .where(GlobalUpdateOutbox.event_id == event_id)
                )
            ).scalar_one()
            assert row.processed_at is None
            assert row.retry_count == DEAD_LETTER_SENTINEL
            assert row.last_error == "invalid payload shape"

    @pytest.mark.asyncio
    async def test_board_read_failure_keeps_event_retryable(
        self,
        db_factory,
        monkeypatch,
    ):
        import uuid
        import okto_pulse.core.kg.global_discovery.outbox_worker as worker_mod

        board_id = f"board-outbox-read-fail-{uuid.uuid4().hex[:8]}"
        event_id = str(uuid.uuid4())
        session_id = f"kgses_{uuid.uuid4().hex[:16]}"
        async with db_factory() as db:
            db.add(KuzuNodeRef(
                session_id=session_id,
                board_id=board_id,
                kuzu_node_id="entity_source",
                kuzu_node_type="Entity",
                operation="add",
            ))
            db.add(GlobalUpdateOutbox(
                event_id=event_id,
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"session_id": session_id, "nodes_added": 1},
            ))
            await db.commit()

        class FakeResult:
            def has_next(self):
                return False

        class FakeConn:
            def execute(self, *_args, **_kwargs):
                return FakeResult()

        monkeypatch.setattr(
            worker_mod,
            "open_global_connection",
            lambda: (object(), FakeConn()),
        )
        monkeypatch.setattr(
            OutboxWorker,
            "_read_board_nodes_for_refs",
            staticmethod(lambda _board_id, _refs: None),
        )

        worker = OutboxWorker(db_factory, interval_seconds=5)
        processed = await worker.process_once()

        assert processed == 0
        async with db_factory() as db:
            row = (
                await db.execute(
                    worker_mod.select(GlobalUpdateOutbox)
                    .where(GlobalUpdateOutbox.event_id == event_id)
                )
            ).scalar_one()
            assert row.processed_at is None
            assert row.retry_count == 1
            assert "outbox.read_board_failed" in (row.last_error or "")
