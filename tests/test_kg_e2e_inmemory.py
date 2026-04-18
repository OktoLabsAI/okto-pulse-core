"""Tests for TransactionOrchestrator + backward compat + E2E (Onda 3 — test card 043c9fa1).

Scenarios:
  ts_eaa9e33d — TransactionOrchestrator with GraphStore compensating delete
  ts_a3dada66 — cypher_templates backward compat + 139 tests pass
  ts_e861e40b — E2E commit with InMemory providers
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from okto_pulse.core.kg.interfaces.graph_store import QueryFilters, SemanticGraphStore
from okto_pulse.core.kg.interfaces.event_bus import EventBus, KGEvent
from okto_pulse.core.kg.interfaces.registry import (
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)
from okto_pulse.core.kg.providers.embedded.memory_session_store import InMemorySessionStore
from okto_pulse.core.kg.providers.testing.memory_audit_repo import InMemoryAuditRepository
from okto_pulse.core.kg.providers.testing.memory_event_bus import InMemoryEventBus
from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


# -----------------------------------------------------------------------
# ts_eaa9e33d — TransactionOrchestrator compensating delete
# -----------------------------------------------------------------------


class TestCompensatingDelete:
    """ts_eaa9e33d — AC-8."""

    def test_create_and_delete_by_session(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")

        for i in range(5):
            store.create_node("b1", "Decision", f"d{i}", {
                "title": f"Decision {i}",
                "source_session_id": "test-session-001",
            })
        store.create_edge("b1", "supersedes", "d0", "d1", {
            "created_by_session_id": "test-session-001",
        })

        assert len(store._board_nodes("b1")) == 5
        assert len(store._board_edges("b1")) == 1

        edge_count = store.delete_edges_by_session("b1", "test-session-001")
        node_count = store.delete_nodes_by_session("b1", "test-session-001")
        assert edge_count == 1
        assert node_count == 5
        assert len(store._board_nodes("b1")) == 0
        assert len(store._board_edges("b1")) == 0

    def test_partial_write_then_compensate(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")

        store.create_node("b1", "Decision", "d0", {
            "source_session_id": "ses-fail",
        })
        store.create_node("b1", "Decision", "d1", {
            "source_session_id": "ses-fail",
        })
        store.create_node("b1", "Decision", "d2", {
            "source_session_id": "ses-fail",
        })

        assert len(store._board_nodes("b1")) == 3

        count = store.delete_nodes_by_session("b1", "ses-fail")
        assert count == 3
        assert len(store._board_nodes("b1")) == 0

    def test_delete_only_target_session(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")

        store.create_node("b1", "Decision", "d0", {"source_session_id": "ses-A"})
        store.create_node("b1", "Decision", "d1", {"source_session_id": "ses-B"})
        store.create_node("b1", "Decision", "d2", {"source_session_id": "ses-A"})

        count = store.delete_nodes_by_session("b1", "ses-A")
        assert count == 2
        assert len(store._board_nodes("b1")) == 1
        remaining = list(store._board_nodes("b1").values())
        assert remaining[0]["id"] == "d1"

    def test_delete_empty_session(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")
        count = store.delete_nodes_by_session("b1", "nonexistent")
        assert count == 0


# -----------------------------------------------------------------------
# ts_a3dada66 — backward compat
# -----------------------------------------------------------------------


class TestBackwardCompat:
    """ts_a3dada66 — AC-9, AC-10."""

    def test_cypher_templates_importable(self):
        from okto_pulse.core.kg import cypher_templates as tpl

        assert hasattr(tpl, "GET_DECISION_HISTORY")
        assert hasattr(tpl, "FIND_CONTRADICTIONS_ALL")
        assert hasattr(tpl, "GET_SUPERSEDENCE_CHAIN")

    def test_open_board_connection_importable(self):
        from okto_pulse.core.kg.schema import open_board_connection

        assert callable(open_board_connection)

    def test_schema_constants_importable(self):
        from okto_pulse.core.kg.schema import (
            NODE_TYPES,
            REL_TYPES,
            SCHEMA_VERSION,
            VECTOR_INDEX_TYPES,
        )

        assert len(NODE_TYPES) == 11
        assert len(REL_TYPES) == 10
        assert SCHEMA_VERSION == "0.2.0"

    def test_all_interfaces_importable(self):
        from okto_pulse.core.kg.interfaces import (
            AuthContext,
            AuditRepository,
            CacheBackend,
            CypherExecutor,
            EmbeddingProvider,
            EventBus,
            KGConfig,
            KGEvent,
            QueryFilters,
            RateLimiter,
            SemanticGraphStore,
            SessionStore,
        )

        assert all([
            AuthContext, AuditRepository, CacheBackend, CypherExecutor,
            EmbeddingProvider, EventBus, KGConfig, KGEvent, QueryFilters,
            RateLimiter, SemanticGraphStore, SessionStore,
        ])


# -----------------------------------------------------------------------
# ts_e861e40b — E2E with InMemory providers
# -----------------------------------------------------------------------


class TestE2EInMemory:
    """ts_e861e40b — AC-11, AC-12."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_in_memory(self):
        store = InMemoryGraphStore()
        event_bus = InMemoryEventBus()
        session_store = InMemorySessionStore(default_ttl_seconds=3600)
        audit_repo = InMemoryAuditRepository()

        configure_kg_registry(
            graph_store=store,
            event_bus=event_bus,
            session_store=session_store,
            audit_repo=audit_repo,
        )

        store.bootstrap("test-board")

        from okto_pulse.core.kg.schemas import (
            AddNodeCandidateRequest,
            BeginConsolidationRequest,
            AbortConsolidationRequest,
            NodeCandidate,
        )
        from okto_pulse.core.kg.primitives import (
            add_node_candidate,
            abort_consolidation,
            begin_consolidation,
        )

        resp = await begin_consolidation(
            BeginConsolidationRequest(
                board_id="test-board",
                artifact_type="spec",
                artifact_id="art-e2e",
                raw_content="full e2e test content",
                deterministic_candidates=[],
            ),
            agent_id="e2e-agent",
        )
        assert resp.nothing_changed is False
        sid = resp.session_id

        for i in range(3):
            cand = NodeCandidate(
                candidate_id=f"cand-{i}",
                node_type="Decision",
                title=f"E2E Decision {i}",
                content=f"Content {i}",
                source_confidence=0.8 + i * 0.05,
            )
            r = await add_node_candidate(
                AddNodeCandidateRequest(session_id=sid, candidate=cand),
                agent_id="e2e-agent",
            )
            assert r.accepted is True

        session = await session_store.get(sid)
        assert len(session.node_candidates) == 3

        await abort_consolidation(
            AbortConsolidationRequest(session_id=sid),
            agent_id="e2e-agent",
        )

        assert await session_store.get(sid) is None

    @pytest.mark.asyncio
    async def test_vector_search_cosine_ordering(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")

        store.create_node("b1", "Decision", "exact", {
            "title": "Exact",
            "embedding": [1.0, 0.0, 0.0],
        })
        store.create_node("b1", "Decision", "similar", {
            "title": "Similar",
            "embedding": [0.9, 0.436, 0.0],
        })
        store.create_node("b1", "Decision", "orthogonal", {
            "title": "Orthogonal",
            "embedding": [0.0, 0.0, 1.0],
        })

        results = store.vector_search(
            "b1", "Decision", [1.0, 0.0, 0.0],
            top_k=10, min_similarity=0.0,
        )
        assert len(results) == 3
        assert results[0]["node_id"] == "exact"
        assert results[0]["similarity"] > 0.99
        assert results[1]["node_id"] == "similar"
        assert results[2]["node_id"] == "orthogonal"
        assert results[2]["similarity"] < 0.01

    @pytest.mark.asyncio
    async def test_event_bus_in_memory_publish(self):
        bus = InMemoryEventBus()
        configure_kg_registry(event_bus=bus)

        event = KGEvent(
            event_type="consolidation_committed",
            board_id="b1",
            session_id="s1",
            payload={"nodes": 5},
        )
        eid = await bus.publish(event)
        assert eid.startswith("evt_")
        assert len(bus.events) == 1

    @pytest.mark.asyncio
    async def test_nothing_changed_via_audit_repo(self):
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

        content = "unchanged content"
        h = compute_content_hash(content, "art-nc", "b1")
        now = datetime.now(timezone.utc)

        await audit_repo.commit_consolidation_records(
            ConsolidationAuditData(
                session_id="prev", board_id="b1", artifact_id="art-nc",
                artifact_type="spec", agent_id="old",
                started_at=now, committed_at=now, content_hash=h,
            ),
            [],
            OutboxEventData(
                event_id="e1", board_id="b1", session_id="prev",
                event_type="test", payload={},
            ),
        )

        resp = await begin_consolidation(
            BeginConsolidationRequest(
                board_id="b1", artifact_type="spec",
                artifact_id="art-nc", raw_content=content,
                deterministic_candidates=[],
            ),
            agent_id="test",
        )
        assert resp.nothing_changed is True
