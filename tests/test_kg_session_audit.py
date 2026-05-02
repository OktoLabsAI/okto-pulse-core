"""Tests for SessionStore + AuditRepository (Onda 2 — test card ba9bd9a6).

Scenarios:
  ts_d9de7cb6 — SessionStore duck typing + asyncio.Lock preserved
  ts_2d931650 — AuditRepository CRUD via DTOs Pydantic
  ts_94dad97f — primitives commit_consolidation uses audit_repo
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from okto_pulse.core.kg.interfaces.audit_dtos import (
    ConsolidationAuditData,
    NodeRefData,
    OutboxEventData,
)
from okto_pulse.core.kg.interfaces.audit_repository import AuditRepository
from okto_pulse.core.kg.interfaces.registry import (
    get_kg_registry,
    reset_registry_for_tests,
)
from okto_pulse.core.kg.interfaces.session_store import SessionStore
from okto_pulse.core.kg.providers.embedded.memory_session_store import InMemorySessionStore
from okto_pulse.core.kg.providers.testing.memory_audit_repo import InMemoryAuditRepository


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


# -----------------------------------------------------------------------
# ts_d9de7cb6 — SessionStore Protocol duck typing + asyncio.Lock
# -----------------------------------------------------------------------


class TestSessionStoreDuckTyping:
    """ts_d9de7cb6 — AC-0, AC-1, AC-2."""

    def test_protocol_accepts_in_memory_store(self):
        assert isinstance(InMemorySessionStore(), SessionStore)

    @pytest.mark.asyncio
    async def test_create_and_get(self):
        store = InMemorySessionStore(default_ttl_seconds=3600)
        session = await store.create(
            session_id="s1",
            board_id="b1",
            artifact_id="a1",
            artifact_type="spec",
            agent_id="agent-1",
            raw_content="test content",
        )
        assert session.session_id == "s1"
        assert session.board_id == "b1"
        assert len(session.node_candidates) == 0
        assert len(session.edge_candidates) == 0

        retrieved = await store.get("s1")
        assert retrieved is session

    @pytest.mark.asyncio
    async def test_remove(self):
        store = InMemorySessionStore()
        await store.create(
            session_id="s2", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1", raw_content="x",
        )
        await store.remove("s2")
        assert await store.get("s2") is None

    @pytest.mark.asyncio
    async def test_sweep_expired(self):
        store = InMemorySessionStore(default_ttl_seconds=0)
        await store.create(
            session_id="s3", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1", raw_content="x",
            ttl_seconds=0,
        )
        await asyncio.sleep(0.01)
        count = await store.sweep_expired()
        assert count == 1
        assert await store.active_count() == 0

    @pytest.mark.asyncio
    async def test_active_count(self):
        store = InMemorySessionStore()
        assert await store.active_count() == 0
        await store.create(
            session_id="s4", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1", raw_content="x",
        )
        assert await store.active_count() == 1

    @pytest.mark.asyncio
    async def test_asyncio_lock_per_session(self):
        store = InMemorySessionStore()
        session = await store.create(
            session_id="s5", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1", raw_content="x",
        )
        assert hasattr(session, "lock")
        assert isinstance(session.lock, asyncio.Lock)

        results = []

        async def writer(val):
            async with session.lock:
                results.append(val)
                await asyncio.sleep(0.01)

        await asyncio.gather(writer("a"), writer("b"))
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_duplicate_session_id_raises(self):
        store = InMemorySessionStore()
        await store.create(
            session_id="dup", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1", raw_content="x",
        )
        with pytest.raises(ValueError, match="already exists"):
            await store.create(
                session_id="dup", board_id="b1", artifact_id="a1",
                artifact_type="spec", agent_id="agent-1", raw_content="x",
            )

    def test_default_ttl(self):
        store = InMemorySessionStore(default_ttl_seconds=1800)
        assert store.default_ttl_seconds == 1800


# -----------------------------------------------------------------------
# ts_2d931650 — AuditRepository CRUD via DTOs (InMemory)
# -----------------------------------------------------------------------


class TestAuditRepositoryCRUD:
    """ts_2d931650 — AC-3, AC-4, AC-5."""

    def test_protocol_accepts_in_memory_repo(self):
        assert isinstance(InMemoryAuditRepository(), AuditRepository)

    @pytest.mark.asyncio
    async def test_commit_and_get_latest(self):
        repo = InMemoryAuditRepository()
        now = datetime.now(timezone.utc)

        audit = ConsolidationAuditData(
            session_id="ses1", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1",
            started_at=now, committed_at=now,
            nodes_added=3, content_hash="abc123",
        )
        refs = [
            NodeRefData(
                session_id="ses1", board_id="b1",
                kuzu_node_id="n1", kuzu_node_type="Decision", operation="add",
            )
        ]
        event = OutboxEventData(
            event_id="evt1", board_id="b1", session_id="ses1",
            event_type="consolidation_committed", payload={"nodes_added": 3},
        )
        await repo.commit_consolidation_records(audit, refs, event)

        latest = await repo.get_latest_for_artifact("b1", "a1")
        assert latest is not None
        assert latest.session_id == "ses1"
        assert latest.nodes_added == 3
        assert latest.content_hash == "abc123"

    @pytest.mark.asyncio
    async def test_get_audit_by_session(self):
        repo = InMemoryAuditRepository()
        now = datetime.now(timezone.utc)

        audit = ConsolidationAuditData(
            session_id="ses2", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1",
            started_at=now, committed_at=now,
        )
        await repo.commit_consolidation_records(audit, [], OutboxEventData(
            event_id="e2", board_id="b1", session_id="ses2",
            event_type="test", payload={},
        ))

        result = await repo.get_audit_by_session("ses2")
        assert result is not None
        assert result.session_id == "ses2"

    @pytest.mark.asyncio
    async def test_mark_undone(self):
        repo = InMemoryAuditRepository()
        now = datetime.now(timezone.utc)

        audit = ConsolidationAuditData(
            session_id="ses3", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1",
            started_at=now, committed_at=now,
        )
        await repo.commit_consolidation_records(audit, [], OutboxEventData(
            event_id="e3", board_id="b1", session_id="ses3",
            event_type="test", payload={},
        ))

        await repo.mark_audit_undone("ses3")
        result = await repo.get_audit_by_session("ses3")
        assert result.undo_status == "undone"

        latest = await repo.get_latest_for_artifact("b1", "a1")
        assert latest is None

    @pytest.mark.asyncio
    async def test_purge_by_board(self):
        repo = InMemoryAuditRepository()
        now = datetime.now(timezone.utc)

        for i in range(3):
            await repo.commit_consolidation_records(
                ConsolidationAuditData(
                    session_id=f"s{i}", board_id="b1", artifact_id="a1",
                    artifact_type="spec", agent_id="agent-1",
                    started_at=now, committed_at=now,
                ),
                [],
                OutboxEventData(
                    event_id=f"e{i}", board_id="b1", session_id=f"s{i}",
                    event_type="test", payload={},
                ),
            )

        count = await repo.purge_by_board("b1")
        assert count == 3
        assert len(repo.audits) == 0

    @pytest.mark.asyncio
    async def test_node_refs_and_outbox_stored(self):
        repo = InMemoryAuditRepository()
        now = datetime.now(timezone.utc)

        refs = [
            NodeRefData(session_id="s1", board_id="b1", kuzu_node_id="n1",
                        kuzu_node_type="Decision", operation="add"),
            NodeRefData(session_id="s1", board_id="b1", kuzu_node_id="n2",
                        kuzu_node_type="Criterion", operation="add"),
        ]
        await repo.commit_consolidation_records(
            ConsolidationAuditData(
                session_id="s1", board_id="b1", artifact_id="a1",
                artifact_type="spec", agent_id="agent-1",
                started_at=now, committed_at=now,
            ),
            refs,
            OutboxEventData(
                event_id="e1", board_id="b1", session_id="s1",
                event_type="test", payload={},
            ),
        )
        assert len(repo.node_refs) == 2
        assert len(repo.outbox_events) == 1

    def test_no_sqlalchemy_import(self):
        import ast
        repo = InMemoryAuditRepository()
        import sys
        module = sys.modules[repo.__class__.__module__]
        source = open(module.__file__).read()
        tree = ast.parse(source)
        imports = [
            node for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
        ]
        for imp in imports:
            if isinstance(imp, ast.ImportFrom) and imp.module:
                assert "sqlalchemy" not in imp.module.lower()
            elif isinstance(imp, ast.Import):
                for alias in imp.names:
                    assert "sqlalchemy" not in alias.name.lower()


# -----------------------------------------------------------------------
# ts_94dad97f — existing tests pass (AC-9)
# -----------------------------------------------------------------------


class TestBackwardCompat:
    """ts_94dad97f — AC-8, AC-9."""

    def test_session_manager_backward_compat(self):
        from okto_pulse.core.kg.session_manager import get_session_manager

        mgr = get_session_manager()
        assert mgr.default_ttl_seconds > 0

    @pytest.mark.asyncio
    async def test_session_manager_delegates_to_registry(self):
        from okto_pulse.core.kg.session_manager import get_session_manager, reset_session_manager_for_tests

        reset_session_manager_for_tests()
        mgr = get_session_manager()

        session = await mgr.create(
            session_id="bcompat1", board_id="b1", artifact_id="a1",
            artifact_type="spec", agent_id="agent-1", raw_content="x",
        )
        assert session.session_id == "bcompat1"

        retrieved = await mgr.get("bcompat1")
        assert retrieved is session

        store = get_kg_registry().session_store
        store_retrieved = await store.get("bcompat1")
        assert store_retrieved is session

        reset_session_manager_for_tests()
