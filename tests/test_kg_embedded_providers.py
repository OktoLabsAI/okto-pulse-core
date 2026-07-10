"""Tests for the core test KG providers and full registry wiring.

Validates:
- All 3 providers satisfy their respective Protocols
- Registry _build_defaults populates graph_store + cypher_executor
- configure_kg_registry fails closed without explicit audit_repo + event_bus
- InMemoryGraphStore enforces the schema contract for relationship endpoints
- InMemoryCypherExecutor satisfies the read-only execution port for tests
- test EventBus fake lifecycle
- kg_service.py uses graph_store from registry (no direct open_board_connection)
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.schema_contract import SCHEMA_VERSION
from okto_pulse.core.kg.interfaces.cypher_executor import CypherExecutor
from okto_pulse.core.kg.interfaces.event_bus import EventBus, KGEvent
from okto_pulse.core.kg.interfaces.graph_store import SemanticGraphStore
from okto_pulse.core.kg.interfaces.registry import (
    get_kg_registry,
    reset_registry_for_tests,
)
from kg_registry_testing import configure_test_kg_registry


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    configure_test_kg_registry()
    yield
    reset_registry_for_tests()


# -----------------------------------------------------------------------
# Protocol compliance
# -----------------------------------------------------------------------


class TestProtocolCompliance:
    """Verify new providers satisfy their Protocol interfaces."""

    def test_memory_graph_store_satisfies_protocol(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import (
            InMemoryGraphStore,
        )

        assert isinstance(InMemoryGraphStore(), SemanticGraphStore)

    def test_memory_cypher_executor_satisfies_protocol(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import (
            InMemoryCypherExecutor,
        )

        assert isinstance(InMemoryCypherExecutor(), CypherExecutor)

    def test_sqlite_outbox_event_bus_satisfies_protocol(self):
        from okto_pulse.core.kg.providers.testing.memory_event_bus import InMemoryEventBus

        assert isinstance(InMemoryEventBus(), EventBus)


class TestInMemoryGraphStoreRelationshipResolution:
    def test_create_edge_ambiguous_relationship_requires_endpoint_hints(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import (
            InMemoryGraphStore,
        )

        with pytest.raises(ValueError, match="ambiguous.*from_type/to_type"):
            InMemoryGraphStore().create_edge("b1", "implements", "api-login", "tr-audit")

    def test_create_edge_implements_constraint_honors_endpoint_hints(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import (
            InMemoryGraphStore,
        )

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_edge(
            "b1",
            "implements",
            "api-login",
            "tr-audit",
            from_type="APIContract",
            to_type="Constraint",
        )

        assert store._edges["b1"][0]["_from_type"] == "APIContract"
        assert store._edges["b1"][0]["_to_type"] == "Constraint"


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

    def test_test_composition_supplies_event_bus_fake(self):
        reg = get_kg_registry()
        assert reg.event_bus is not None
        assert isinstance(reg.event_bus, EventBus)

    def test_test_composition_supplies_audit_repo_fake(self):
        reg = get_kg_registry()
        assert reg.audit_repo is not None

    def test_configure_with_session_factory_does_not_auto_wire_data_ports(self):
        from okto_pulse.core.kg.interfaces.registry import (
            KGProviderRegistry,
            configure_kg_registry,
        )
        from okto_pulse.core.kg.providers.testing.settings_config import (
            SettingsKGConfig,
        )

        reset_registry_for_tests()
        try:
            with pytest.raises(RuntimeError, match="event_bus, audit_repo"):
                configure_kg_registry(
                    session_factory=lambda: None,
                    base_registry=KGProviderRegistry(config=SettingsKGConfig()),
                )
        finally:
            configure_test_kg_registry()

    def test_override_takes_precedence(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore

        custom_store = InMemoryGraphStore()
        configure_test_kg_registry(graph_store=custom_store)
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
        def mock_sf():
            return None

        configure_test_kg_registry(session_factory=mock_sf)
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
# InMemoryCypherExecutor test port
# -----------------------------------------------------------------------


class TestInMemoryCypherExecutor:
    """Verify the core test executor records read requests without runtime deps."""

    def test_is_supported(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import (
            InMemoryCypherExecutor,
        )

        executor = InMemoryCypherExecutor()
        assert executor.is_supported() is False

    def test_execute_read_only_records_query(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import (
            InMemoryCypherExecutor,
        )

        executor = InMemoryCypherExecutor()
        result = executor.execute_read_only(
            "board-1",
            "MATCH (n) RETURN n.id",
            {"limit": 1},
            max_rows=1,
        )
        assert executor.queries == [
            ("board-1", "MATCH (n) RETURN n.id", {"limit": 1})
        ]
        assert result["rows"] == []
        assert result["max_rows"] == 1


# -----------------------------------------------------------------------
# InMemoryEventBus lifecycle
# -----------------------------------------------------------------------


class TestInMemoryEventBusLifecycle:

    @pytest.mark.asyncio
    async def test_start_stop(self):
        from okto_pulse.core.kg.providers.testing.memory_event_bus import (
            InMemoryEventBus,
        )

        bus = InMemoryEventBus()
        await bus.start()
        assert bus.is_running is True
        await bus.stop()
        assert bus.is_running is False

    @pytest.mark.asyncio
    async def test_subscribe_and_handle(self):
        from okto_pulse.core.kg.providers.testing.memory_event_bus import (
            InMemoryEventBus,
        )

        received = []

        async def handler(event: KGEvent):
            received.append(event)

        bus = InMemoryEventBus()
        await bus.subscribe("test_event", handler)

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
        configure_test_kg_registry(graph_store=store)

        svc = KGService()
        assert svc.get_schema_version("b1") is None

        store.bootstrap("b1")
        assert svc.get_schema_version("b1") == SCHEMA_VERSION

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

        configure_test_kg_registry(graph_store=store)
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

        configure_test_kg_registry(graph_store=store)
        svc = KGService()

        results = svc.find_contradictions("b1")
        assert len(results) == 1
        assert results[0]["id_a"] == "d1"

    def test_service_explain_constraint_via_graph_store(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Constraint", "c1", {
            "title": "Max 100 requests/sec",
            "content": "Rate limit",
            "justification": "Performance",
            "source_artifact_ref": "spec-123",
            "source_confidence": 0.95,
        })

        configure_test_kg_registry(graph_store=store)
        svc = KGService()

        result = svc.explain_constraint("b1", "c1")
        assert result["id"] == "c1"
        assert result["title"] == "Max 100 requests/sec"

    def test_service_explain_constraint_not_found(self):
        from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore
        from okto_pulse.core.kg.kg_service import KGService, KGToolError

        store = InMemoryGraphStore()
        store.bootstrap("b1")
        configure_test_kg_registry(graph_store=store)
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

        configure_test_kg_registry(graph_store=store)
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

        configure_test_kg_registry(graph_store=store)
        svc = KGService()

        # find_similar_decisions uses embedder.encode which returns a vector
        # With the stub embedder, it returns all-zeros, so similarity will be 0.
        # This test validates the flow works without errors.
        results = svc.find_similar_decisions("b1", "authentication")
        assert isinstance(results, list)
