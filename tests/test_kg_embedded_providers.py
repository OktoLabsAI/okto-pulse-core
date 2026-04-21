"""Tests for the new embedded providers (KuzuGraphStore, KuzuCypherExecutor,
SqliteOutboxEventBus) and the full registry wiring.

Validates:
- All 3 providers satisfy their respective Protocols
- Registry _build_defaults populates graph_store + cypher_executor
- configure_kg_registry with session_factory wires audit_repo + event_bus
- KuzuGraphStore delegates to Kuzu correctly (via InMemoryGraphStore parity)
- KuzuCypherExecutor applies safety rails
- SqliteOutboxEventBus lifecycle
- kg_service.py uses graph_store from registry (no direct open_board_connection)
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from okto_pulse.core.kg.interfaces.cypher_executor import CypherExecutor
from okto_pulse.core.kg.interfaces.event_bus import EventBus, KGEvent
from okto_pulse.core.kg.interfaces.graph_store import QueryFilters, SemanticGraphStore
from okto_pulse.core.kg.interfaces.registry import (
    KGProviderRegistry,
    configure_kg_registry,
    get_kg_registry,
    reset_registry_for_tests,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


# -----------------------------------------------------------------------
# Protocol compliance
# -----------------------------------------------------------------------


class TestProtocolCompliance:
    """Verify new providers satisfy their Protocol interfaces."""

    def test_kuzu_graph_store_satisfies_protocol(self):
        from okto_pulse.core.kg.providers.embedded.kuzu_graph_store import KuzuGraphStore

        assert isinstance(KuzuGraphStore(), SemanticGraphStore)

    def test_kuzu_cypher_executor_satisfies_protocol(self):
        from okto_pulse.core.kg.providers.embedded.kuzu_cypher_executor import KuzuCypherExecutor

        assert isinstance(KuzuCypherExecutor(), CypherExecutor)

    def test_sqlite_outbox_event_bus_satisfies_protocol(self):
        from okto_pulse.core.kg.providers.embedded.sqlite_outbox_event_bus import SqliteOutboxEventBus

        assert isinstance(SqliteOutboxEventBus(lambda: None), EventBus)


# -----------------------------------------------------------------------
# Registry wiring
# -----------------------------------------------------------------------


class TestRegistryWiring:
    """Verify _build_defaults populates Onda 3 providers."""

    def test_defaults_include_graph_store(self):
        reg = get_kg_registry()
        assert reg.graph_store is not None
        assert isinstance(reg.graph_store, SemanticGraphStore)

    def test_defaults_include_cypher_executor(self):
        reg = get_kg_registry()
        assert reg.cypher_executor is not None
        assert isinstance(reg.cypher_executor, CypherExecutor)

    def test_defaults_no_event_bus_without_session_factory(self):
        reg = get_kg_registry()
        # event_bus requires session_factory, so defaults have None
        assert reg.event_bus is None

    def test_defaults_no_audit_repo_without_session_factory(self):
        reg = get_kg_registry()
        assert reg.audit_repo is None

    def test_configure_with_session_factory_wires_audit_repo(self):
        mock_sf = lambda: None
        configure_kg_registry(session_factory=mock_sf)
        reg = get_kg_registry()
        assert reg.audit_repo is not None

    def test_configure_with_session_factory_wires_event_bus(self):
        mock_sf = lambda: None
        configure_kg_registry(session_factory=mock_sf)
        reg = get_kg_registry()
        assert reg.event_bus is not None
        assert isinstance(reg.event_bus, EventBus)

    def test_override_takes_precedence(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore

        custom_store = InMemoryGraphStore()
        configure_kg_registry(graph_store=custom_store)
        reg = get_kg_registry()
        assert reg.graph_store is custom_store

    def test_all_onda1_fields_populated(self):
        reg = get_kg_registry()
        assert reg.config is not None
        assert reg.cache_backend is not None
        assert reg.rate_limiter is not None
        assert reg.embedding_provider is not None

    def test_all_onda2_fields_populated(self):
        reg = get_kg_registry()
        assert reg.session_store is not None

    def test_full_registry_with_session_factory(self):
        """All 10 fields populated when session_factory is provided."""
        mock_sf = lambda: None
        configure_kg_registry(session_factory=mock_sf)
        reg = get_kg_registry()

        populated = 0
        for field_name in [
            "config", "cache_backend", "rate_limiter", "embedding_provider",
            "session_store", "audit_repo", "graph_store", "cypher_executor",
            "event_bus",
        ]:
            if getattr(reg, field_name) is not None:
                populated += 1

        # auth_context_factory is the only one not auto-wired
        assert populated >= 9, f"Only {populated}/9 providers populated"


# -----------------------------------------------------------------------
# KuzuCypherExecutor safety rails
# -----------------------------------------------------------------------


class TestKuzuCypherExecutorSafety:
    """Verify safety rails are applied via the executor."""

    def test_is_supported(self):
        from okto_pulse.core.kg.providers.embedded.kuzu_cypher_executor import KuzuCypherExecutor

        executor = KuzuCypherExecutor()
        assert executor.is_supported() is True

    def test_rejects_write_cypher(self):
        from okto_pulse.core.kg.providers.embedded.kuzu_cypher_executor import KuzuCypherExecutor
        from okto_pulse.core.kg.tier_power import TierPowerError

        executor = KuzuCypherExecutor()
        with pytest.raises(TierPowerError) as exc_info:
            executor.execute_read_only("board-1", "CREATE (n:Test {id: 'x'})")
        assert exc_info.value.code == "unsafe_cypher"

    def test_rejects_delete_cypher(self):
        from okto_pulse.core.kg.providers.embedded.kuzu_cypher_executor import KuzuCypherExecutor
        from okto_pulse.core.kg.tier_power import TierPowerError

        executor = KuzuCypherExecutor()
        with pytest.raises(TierPowerError):
            executor.execute_read_only("board-1", "MATCH (n) DELETE n")


# -----------------------------------------------------------------------
# SqliteOutboxEventBus lifecycle
# -----------------------------------------------------------------------


class TestSqliteOutboxEventBusLifecycle:

    @pytest.mark.asyncio
    async def test_start_stop(self):
        from okto_pulse.core.kg.providers.embedded.sqlite_outbox_event_bus import SqliteOutboxEventBus

        bus = SqliteOutboxEventBus(lambda: None)
        await bus.start()
        assert bus._running is True
        await bus.stop()
        assert bus._running is False

    @pytest.mark.asyncio
    async def test_subscribe_and_handle(self):
        from okto_pulse.core.kg.providers.embedded.sqlite_outbox_event_bus import SqliteOutboxEventBus

        received = []

        async def handler(event: KGEvent):
            received.append(event)

        bus = SqliteOutboxEventBus(lambda: None)
        await bus.subscribe("test_event", handler)

        # publish will fail on outbox write (no real DB), but handler should still fire
        event_id = await bus.publish(KGEvent(
            event_type="test_event",
            board_id="b1",
            session_id="s1",
            payload={"test": True},
        ))
        assert event_id.startswith("evt_")
        assert len(received) == 1
        assert received[0].board_id == "b1"


# -----------------------------------------------------------------------
# kg_service uses graph_store from registry
# -----------------------------------------------------------------------


class TestKGServiceUsesRegistry:
    """Verify kg_service.py delegates to registry.graph_store."""

    def test_service_uses_graph_store_for_schema_version(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService

        store = InMemoryGraphStore()
        configure_kg_registry(graph_store=store)

        svc = KGService()
        assert svc.get_schema_version("b1") is None

        store.bootstrap("b1")
        assert svc.get_schema_version("b1") == "0.3.0"

    def test_service_decision_history_via_graph_store(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Decision", "d1", {
            "title": "Use GraphQL for the API layer",
            "content": "Chose GraphQL over REST",
            "source_confidence": 0.9,
            "relevance_score": 0.8,
        })

        configure_kg_registry(graph_store=store)
        svc = KGService()

        results = svc.get_decision_history("b1", "GraphQL")
        assert len(results) == 1
        assert results[0]["id"] == "d1"
        assert "GraphQL" in results[0]["title"]

    def test_service_find_contradictions_via_graph_store(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Decision", "d1", {"title": "A"})
        store.create_node("b1", "Decision", "d2", {"title": "B"})
        store.create_edge("b1", "contradicts", "d1", "d2", {"confidence": 0.9})

        configure_kg_registry(graph_store=store)
        svc = KGService()

        results = svc.find_contradictions("b1")
        assert len(results) == 1
        assert results[0]["id_a"] == "d1"

    def test_service_explain_constraint_via_graph_store(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService, KGToolError

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Constraint", "c1", {
            "title": "Max 100 requests/sec",
            "content": "Rate limit",
            "justification": "Performance",
            "source_artifact_ref": "spec-123",
            "source_confidence": 0.95,
        })

        configure_kg_registry(graph_store=store)
        svc = KGService()

        result = svc.explain_constraint("b1", "c1")
        assert result["id"] == "c1"
        assert result["title"] == "Max 100 requests/sec"

    def test_service_explain_constraint_not_found(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService, KGToolError

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        configure_kg_registry(graph_store=store)
        svc = KGService()

        with pytest.raises(KGToolError) as exc_info:
            svc.explain_constraint("b1", "nonexistent")
        assert exc_info.value.code == "not_found"

    def test_service_list_alternatives_via_graph_store(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Decision", "d1", {"title": "Pick DB"})
        store.create_node("b1", "Alternative", "a1", {
            "title": "PostgreSQL",
            "content": "Full SQL",
            "justification": "Maturity",
            "source_confidence": 0.8,
            "source_artifact_ref": "spec-1",
        })
        store.create_edge("b1", "relates_to", "d1", "a1")

        configure_kg_registry(graph_store=store)
        svc = KGService()

        results = svc.list_alternatives("b1", "d1")
        assert len(results) == 1
        assert results[0]["title"] == "PostgreSQL"

    def test_service_find_similar_via_graph_store(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Decision", "d1", {
            "title": "Auth decision",
            "embedding": [1.0, 0.0, 0.0],
        })

        configure_kg_registry(graph_store=store)
        svc = KGService()

        # find_similar_decisions uses embedder.encode which returns a vector
        # With the stub embedder, it returns all-zeros, so similarity will be 0.
        # This test validates the flow works without errors.
        results = svc.find_similar_decisions("b1", "authentication")
        assert isinstance(results, list)
