"""Tests for GraphStore + CypherExecutor + EventBus (Onda 3 — test card fb68e620).

Scenarios:
  ts_6e3558da — SemanticGraphStore accepts InMemoryGraphStore
  ts_57a11cec — CypherExecutor preserves safety rails
  ts_1cc0091a — EventBus publish fire-and-forget + lifecycle
"""

from __future__ import annotations

import pytest

from kg_schema_testing import SCHEMA_VERSION
from okto_pulse.core.kg.interfaces.cypher_executor import CypherExecutor
from okto_pulse.core.kg.interfaces.event_bus import EventBus, KGEvent
from okto_pulse.core.kg.interfaces.graph_store import QueryFilters, SemanticGraphStore
from okto_pulse.core.kg.interfaces.registry import (
    reset_registry_for_tests,
)
from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.providers.testing.memory_event_bus import InMemoryEventBus
from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    configure_test_kg_registry()
    yield
    reset_registry_for_tests()


def _unit_vec(dim: int, idx: int) -> list[float]:
    """Create a unit vector with 1.0 at position idx."""
    v = [0.0] * dim
    v[idx % dim] = 1.0
    return v


# -----------------------------------------------------------------------
# ts_6e3558da — SemanticGraphStore accepts InMemoryGraphStore
# -----------------------------------------------------------------------


class TestSemanticGraphStore:
    """ts_6e3558da — AC-0, AC-1, AC-2, AC-3."""

    def test_protocol_accepts_in_memory(self):
        assert isinstance(InMemoryGraphStore(), SemanticGraphStore)

    def test_bootstrap_and_schema_version(self):
        store = InMemoryGraphStore()
        assert store.get_schema_version("b1") is None
        store.bootstrap("b1")
        assert store.get_schema_version("b1") == SCHEMA_VERSION

    def test_create_and_find_by_topic(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node(
            "b1",
            "Decision",
            "d1",
            {
                "title": "Use FastAPI for REST",
                "content": "Chose FastAPI",
                "source_confidence": 0.9,
                "relevance_score": 0.8,
            },
        )
        store.create_node(
            "b1",
            "Decision",
            "d2",
            {
                "title": "Use Django ORM",
                "content": "Chose Django",
                "source_confidence": 0.8,
                "relevance_score": 0.7,
            },
        )

        results = store.find_by_topic("b1", "Decision", "fastapi", QueryFilters())
        assert len(results) == 1
        assert results[0][0] == "d1"

    def test_find_contradictions(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Decision", "d1", {"title": "A"})
        store.create_node("b1", "Decision", "d2", {"title": "B"})
        store.create_edge("b1", "contradicts", "d1", "d2", {"confidence": 0.95})

        results = store.find_contradictions("b1", None, 10)
        assert len(results) == 1
        assert results[0][0] == "d1"
        assert results[0][2] == "d2"

    def test_vector_search_cosine_similarity(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")

        dim = 8
        store.create_node(
            "b1",
            "Decision",
            "d1",
            {
                "title": "Exact match",
                "embedding": _unit_vec(dim, 0),
            },
        )
        store.create_node(
            "b1",
            "Decision",
            "d2",
            {
                "title": "Similar",
                "embedding": [0.9, 0.1, 0, 0, 0, 0, 0, 0],
            },
        )
        store.create_node(
            "b1",
            "Decision",
            "d3",
            {
                "title": "Orthogonal",
                "embedding": _unit_vec(dim, 3),
            },
        )

        query = _unit_vec(dim, 0)
        results = store.vector_search(
            "b1", "Decision", query, top_k=10, min_similarity=0.0
        )
        assert len(results) == 3
        assert results[0]["node_id"] == "d1"
        assert results[0]["similarity"] > 0.99
        assert results[1]["node_id"] == "d2"
        assert results[1]["similarity"] > 0.9

    def test_vector_search_min_similarity_filter(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")

        store.create_node(
            "b1",
            "Decision",
            "d1",
            {
                "title": "Close",
                "embedding": [1.0, 0.0, 0.0],
            },
        )
        store.create_node(
            "b1",
            "Decision",
            "d2",
            {
                "title": "Far",
                "embedding": [0.0, 1.0, 0.0],
            },
        )

        results = store.vector_search(
            "b1", "Decision", [1.0, 0.0, 0.0], top_k=10, min_similarity=0.5
        )
        assert len(results) == 1
        assert results[0]["node_id"] == "d1"

    def test_source_deleted_tombstone_is_not_active_or_vector_searchable(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")
        source_ref = "spec:deleted:decision:d1"
        store.create_node(
            "b1",
            "Decision",
            "active",
            {
                "title": "Active assertion",
                "source_artifact_ref": source_ref,
                "embedding": [1.0, 0.0],
                "graph_layer": "working",
                "maturity_status": "working_immature",
                "relevance_score": 0.8,
                "generation": 1,
            },
        )
        store.create_node(
            "b1",
            "Decision",
            "deleted",
            {
                "title": "Deleted assertion",
                "source_artifact_ref": source_ref,
                "embedding": [1.0, 0.0],
                "graph_layer": "working",
                "maturity_status": "working_stale",
                "revocation_reason": "source_deleted",
                "relevance_score": 0.0,
                "generation": 2,
            },
        )

        hits = store.vector_search(
            "b1",
            "Decision",
            [1.0, 0.0],
            top_k=10,
            min_similarity=0.0,
            include_superseded=True,
            graph_layer="all",
        )
        exact = store.find_active_by_source_ref("b1", "Decision", source_ref)

        assert [hit["node_id"] for hit in hits] == ["active"]
        assert exact is not None
        assert exact["node_id"] == "active"

    def test_delete_nodes_by_session(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Decision", "d1", {"source_session_id": "ses1"})
        store.create_node("b1", "Decision", "d2", {"source_session_id": "ses1"})
        store.create_node("b1", "Decision", "d3", {"source_session_id": "ses2"})

        count = store.delete_nodes_by_session("b1", "ses1")
        assert count == 2
        assert len(store._board_nodes("b1")) == 1

    def test_delete_edges_by_session(self):
        store = InMemoryGraphStore()
        store.bootstrap("b1")
        store.create_node("b1", "Decision", "d1", {})
        store.create_node("b1", "Decision", "d2", {})
        store.create_edge(
            "b1", "supersedes", "d1", "d2", {"created_by_session_id": "ses1"}
        )
        store.create_edge(
            "b1", "contradicts", "d1", "d2", {"created_by_session_id": "ses2"}
        )

        count = store.delete_edges_by_session("b1", "ses1")
        assert count == 1
        assert len(store._board_edges("b1")) == 1

    def test_get_schema_info(self):
        store = InMemoryGraphStore()
        info = store.get_schema_info("b1")
        assert info["schema_version"] == SCHEMA_VERSION
        assert len(info["stable_node_types"]) == 11
        # The current closed schema exposes 16 stable relationship names.
        assert len(info["stable_rel_types"]) == 16
        rel_names = {rel["name"] for rel in info["stable_rel_types"]}
        assert {"belongs_to", "originates_from", "covered_by"} <= rel_names
        assert len(info["vector_indexes"]) == 9

    def test_get_schema_info_with_internal(self):
        store = InMemoryGraphStore()
        info = store.get_schema_info("b1", include_internal=True)
        assert "internal_node_types" in info


# -----------------------------------------------------------------------
# ts_57a11cec — CypherExecutor safety rails
# -----------------------------------------------------------------------


class TestCypherExecutorSafetyRails:
    """ts_57a11cec — AC-4, AC-5."""

    def test_protocol_defined(self):
        class MockExecutor:
            def execute_read_only(
                self, board_id, cypher, params=None, *, max_rows=1000
            ):
                return {"rows": [], "row_count": 0}

            def is_supported(self):
                return True

        assert isinstance(MockExecutor(), CypherExecutor)

    def test_unsupported_executor(self):
        class UnsupportedExecutor:
            def execute_read_only(
                self, board_id, cypher, params=None, *, max_rows=1000
            ):
                raise NotImplementedError("Backend does not support Cypher")

            def is_supported(self):
                return False

        exec_ = UnsupportedExecutor()
        assert exec_.is_supported() is False

    def test_safety_rails_in_tier_power(self):
        from okto_pulse.core.kg.tier_power import (
            TierPowerError,
            validate_cypher_read_only,
        )

        with pytest.raises(TierPowerError) as exc_info:
            validate_cypher_read_only("CREATE (n:Test {id: 'x'})")
        assert exc_info.value.code == "unsafe_cypher"

    def test_whitelist_rejects_delete(self):
        from okto_pulse.core.kg.tier_power import (
            TierPowerError,
            validate_cypher_read_only,
        )

        with pytest.raises(TierPowerError):
            validate_cypher_read_only("MATCH (n) DELETE n")

    def test_read_only_query_passes(self):
        from okto_pulse.core.kg.tier_power import validate_cypher_read_only

        validate_cypher_read_only("MATCH (n:Decision) RETURN n.title LIMIT 10")


# -----------------------------------------------------------------------
# ts_1cc0091a — EventBus publish fire-and-forget + lifecycle
# -----------------------------------------------------------------------


class TestEventBusLifecycle:
    """ts_1cc0091a — AC-6, AC-7."""

    def test_protocol_accepts_in_memory(self):
        assert isinstance(InMemoryEventBus(), EventBus)

    @pytest.mark.asyncio
    async def test_publish_returns_event_id(self):
        bus = InMemoryEventBus()
        event = KGEvent(
            event_type="consolidation_committed",
            board_id="b1",
            session_id="s1",
            payload={"nodes_added": 5},
        )
        event_id = await bus.publish(event)
        assert event_id.startswith("evt_")
        assert len(bus.events) == 1

    @pytest.mark.asyncio
    async def test_subscribe_and_handler_called(self):
        bus = InMemoryEventBus()
        received = []

        async def handler(event: KGEvent):
            received.append(event)

        await bus.subscribe("test_event", handler)
        await bus.publish(
            KGEvent(
                event_type="test_event",
                board_id="b1",
                session_id="s1",
                payload={},
            )
        )
        assert len(received) == 1
        assert received[0].board_id == "b1"

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self):
        bus = InMemoryEventBus()
        assert bus.is_running is False
        await bus.start()
        assert bus.is_running is True
        await bus.stop()
        assert bus.is_running is False

    @pytest.mark.asyncio
    async def test_fire_and_forget_semantics(self):
        bus = InMemoryEventBus()
        import time

        t0 = time.monotonic()
        for _ in range(100):
            await bus.publish(
                KGEvent(
                    event_type="bulk",
                    board_id="b1",
                    session_id="s1",
                    payload={},
                )
            )
        dur = (time.monotonic() - t0) * 1000
        assert len(bus.events) == 100
        assert dur < 500  # 100 publishes should be < 500ms in memory
