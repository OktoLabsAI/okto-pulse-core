"""Tests for AuthContext + E2E InMemory providers (Onda 2 — test card 7d8434e7).

Scenarios:
  ts_05d93d48 — AuthContext resolves boards via factory
  ts_0db5378a — E2E primitives with InMemory providers (no Kuzu/SQLite)
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.interfaces.auth_context import AuthContext
from okto_pulse.core.kg.interfaces.registry import (
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)
from okto_pulse.core.kg.providers.embedded.memory_session_store import InMemorySessionStore
from okto_pulse.core.kg.providers.testing.memory_audit_repo import InMemoryAuditRepository


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


# -----------------------------------------------------------------------
# Mock AuthContext for testing
# -----------------------------------------------------------------------


class MockAuthContext:
    """Test AuthContext that returns fixed values."""

    def __init__(self, agent_id: str = "test-agent", boards: list[str] | None = None):
        self._agent_id = agent_id
        self._boards = boards or []

    async def get_agent_id(self) -> str | None:
        return self._agent_id

    async def get_accessible_boards(self) -> list[str]:
        return self._boards

    def has_admin_role(self) -> bool:
        return False


class UnauthenticatedContext:
    async def get_agent_id(self) -> str | None:
        return None

    async def get_accessible_boards(self) -> list[str]:
        return []

    def has_admin_role(self) -> bool:
        return False


# -----------------------------------------------------------------------
# ts_05d93d48 — AuthContext resolves boards via factory
# -----------------------------------------------------------------------


class TestAuthContextFactory:
    """ts_05d93d48 — AC-6, AC-7."""

    def test_protocol_accepts_mock(self):
        assert isinstance(MockAuthContext(), AuthContext)

    def test_protocol_rejects_incomplete(self):
        class Broken:
            pass

        assert not isinstance(Broken(), AuthContext)

    @pytest.mark.asyncio
    async def test_get_agent_id(self):
        auth = MockAuthContext(agent_id="agent-42")
        assert await auth.get_agent_id() == "agent-42"

    @pytest.mark.asyncio
    async def test_get_accessible_boards(self):
        auth = MockAuthContext(boards=["board-A", "board-B"])
        boards = await auth.get_accessible_boards()
        assert boards == ["board-A", "board-B"]

    @pytest.mark.asyncio
    async def test_unauthenticated_returns_none(self):
        auth = UnauthenticatedContext()
        assert await auth.get_agent_id() is None
        assert await auth.get_accessible_boards() == []

    @pytest.mark.asyncio
    async def test_factory_via_registry(self):
        mock = MockAuthContext(agent_id="factory-agent", boards=["b1"])
        configure_kg_registry(auth_context_factory=lambda: mock)
        reg = get_kg_registry()
        auth = reg.auth_context_factory()
        assert await auth.get_agent_id() == "factory-agent"
        assert await auth.get_accessible_boards() == ["b1"]

    def test_has_admin_role_default_false(self):
        auth = MockAuthContext()
        assert auth.has_admin_role() is False


# -----------------------------------------------------------------------
# ts_0db5378a — E2E primitives with InMemory providers (no Kuzu/SQLite)
# -----------------------------------------------------------------------


class TestE2EInMemoryProviders:
    """ts_0db5378a — AC-10: full primitive flow without Kuzu/SQLite."""

    @pytest.mark.asyncio
    async def test_begin_and_session_lifecycle(self):
        audit_repo = InMemoryAuditRepository()
        session_store = InMemorySessionStore(default_ttl_seconds=3600)
        configure_kg_registry(
            session_store=session_store,
            audit_repo=audit_repo,
        )

        from okto_pulse.core.kg.schemas import BeginConsolidationRequest
        from okto_pulse.core.kg.primitives import begin_consolidation

        req = BeginConsolidationRequest(
            board_id="test-board",
            artifact_type="spec",
            artifact_id="art-1",
            raw_content="test content for E2E",
            deterministic_candidates=[],
        )
        resp = await begin_consolidation(req, agent_id="test-agent")
        assert resp.session_id.startswith("kgses_")
        assert resp.nothing_changed is False

        session = await session_store.get(resp.session_id)
        assert session is not None
        assert session.board_id == "test-board"

    @pytest.mark.asyncio
    async def test_add_candidates_in_memory(self):
        session_store = InMemorySessionStore(default_ttl_seconds=3600)
        audit_repo = InMemoryAuditRepository()
        configure_kg_registry(
            session_store=session_store,
            audit_repo=audit_repo,
        )

        from okto_pulse.core.kg.schemas import (
            AddNodeCandidateRequest,
            BeginConsolidationRequest,
            NodeCandidate,
        )
        from okto_pulse.core.kg.primitives import (
            add_node_candidate,
            begin_consolidation,
        )

        begin_resp = await begin_consolidation(
            BeginConsolidationRequest(
                board_id="test-board",
                artifact_type="spec",
                artifact_id="art-2",
                raw_content="content",
                deterministic_candidates=[],
            ),
            agent_id="test-agent",
        )

        cand = NodeCandidate(
            candidate_id="cand-1",
            node_type="Decision",
            title="Test decision",
            content="Decision content",
            source_confidence=0.9,
        )
        resp = await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=begin_resp.session_id,
                candidate=cand,
            ),
            agent_id="test-agent",
        )
        assert resp.accepted is True
        assert resp.node_count_in_session == 1

    @pytest.mark.asyncio
    async def test_abort_cleans_session(self):
        session_store = InMemorySessionStore(default_ttl_seconds=3600)
        audit_repo = InMemoryAuditRepository()
        configure_kg_registry(
            session_store=session_store,
            audit_repo=audit_repo,
        )

        from okto_pulse.core.kg.schemas import (
            AbortConsolidationRequest,
            BeginConsolidationRequest,
        )
        from okto_pulse.core.kg.primitives import (
            abort_consolidation,
            begin_consolidation,
        )

        begin_resp = await begin_consolidation(
            BeginConsolidationRequest(
                board_id="test-board",
                artifact_type="spec",
                artifact_id="art-3",
                raw_content="content",
                deterministic_candidates=[],
            ),
            agent_id="test-agent",
        )

        abort_resp = await abort_consolidation(
            AbortConsolidationRequest(session_id=begin_resp.session_id),
            agent_id="test-agent",
        )
        assert abort_resp.compensating_delete_applied is False

        session = await session_store.get(begin_resp.session_id)
        assert session is None

    @pytest.mark.asyncio
    async def test_nothing_changed_detection(self):
        session_store = InMemorySessionStore(default_ttl_seconds=3600)
        audit_repo = InMemoryAuditRepository()
        configure_kg_registry(
            session_store=session_store,
            audit_repo=audit_repo,
        )

        from datetime import datetime, timezone
        from okto_pulse.core.kg.interfaces.audit_dtos import (
            ConsolidationAuditData,
            OutboxEventData,
        )
        from okto_pulse.core.kg.session_manager import compute_content_hash
        from okto_pulse.core.kg.schemas import BeginConsolidationRequest
        from okto_pulse.core.kg.primitives import begin_consolidation

        content = "same content"
        h = compute_content_hash(content, "art-4", "test-board")
        now = datetime.now(timezone.utc)
        await audit_repo.commit_consolidation_records(
            ConsolidationAuditData(
                session_id="prev-session", board_id="test-board",
                artifact_id="art-4", artifact_type="spec",
                agent_id="old-agent", started_at=now, committed_at=now,
                content_hash=h,
            ),
            [],
            OutboxEventData(
                event_id="e1", board_id="test-board",
                session_id="prev-session", event_type="test", payload={},
            ),
        )

        resp = await begin_consolidation(
            BeginConsolidationRequest(
                board_id="test-board",
                artifact_type="spec",
                artifact_id="art-4",
                raw_content=content,
                deterministic_candidates=[],
            ),
            agent_id="test-agent",
        )
        assert resp.nothing_changed is True
        assert resp.previous_session_id == "prev-session"

    @pytest.mark.asyncio
    async def test_sweep_expired_in_memory(self):
        session_store = InMemorySessionStore(default_ttl_seconds=0)
        configure_kg_registry(session_store=session_store)

        from okto_pulse.core.kg.schemas import BeginConsolidationRequest
        from okto_pulse.core.kg.primitives import begin_consolidation
        import asyncio

        await begin_consolidation(
            BeginConsolidationRequest(
                board_id="test-board",
                artifact_type="spec",
                artifact_id="art-5",
                raw_content="x",
                deterministic_candidates=[],
            ),
            agent_id="test-agent",
        )

        await asyncio.sleep(0.01)
        count = await session_store.sweep_expired()
        assert count == 1
        assert await session_store.active_count() == 0
