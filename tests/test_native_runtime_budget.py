from __future__ import annotations

from types import MappingProxyType

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.interfaces import reset_registry_for_tests
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphRuntimeBudgetSnapshot,
    GraphRuntimeStore,
)
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphRuntimeStore,
)
from okto_pulse.core.services.kg_health_service import _build_native_runtime_budget


@pytest.fixture(autouse=True)
def _isolated_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


def test_budget_snapshot_copies_and_deep_freezes_nested_mappings() -> None:
    requested = {"board_buffer_pool_mb": 256}
    snapshot = GraphRuntimeBudgetSnapshot(
        source="runtime_capability",
        status="available",
        requested=requested,
    )

    requested["board_buffer_pool_mb"] = 512

    assert isinstance(snapshot.requested, MappingProxyType)
    assert snapshot.requested["board_buffer_pool_mb"] == 256
    with pytest.raises(TypeError):
        snapshot.requested["board_buffer_pool_mb"] = 128  # type: ignore[index]
    with pytest.raises(ValueError, match="not direct memory telemetry"):
        GraphRuntimeBudgetSnapshot(
            source="runtime_capability",
            status="available",
            is_direct_memory_telemetry=True,
        )


def test_in_memory_runtime_fake_exposes_the_port_capability() -> None:
    runtime = InMemoryGraphRuntimeStore()

    assert isinstance(runtime, GraphRuntimeStore)
    snapshot = runtime.budget_snapshot()
    assert isinstance(snapshot, GraphRuntimeBudgetSnapshot)
    assert snapshot.status == "available"
    assert snapshot.source == "runtime_capability"
    assert snapshot.unavailable_reason is None


def test_core_serializes_backend_neutral_budget_with_defensive_copies() -> None:
    class BudgetRuntime(InMemoryGraphRuntimeStore):
        def budget_snapshot(self) -> GraphRuntimeBudgetSnapshot:
            return GraphRuntimeBudgetSnapshot(
                source="runtime_capability",
                status="available",
                requested={"board_buffer_pool_mb": 512},
                normalized={"board_buffer_pool_cap_mb": 256},
                effective={"board_buffer_pool_mb": 256},
                sources={"board_buffer_pool_cap": "operational_default"},
                process_envelope={"max_derived_buffer_envelope_mb": 640},
            )

    configure_test_kg_registry(
        graph_provider="inmemory",
        graph_runtime_store=BudgetRuntime(),
    )

    payload = _build_native_runtime_budget()

    assert payload["requested"] == {"board_buffer_pool_mb": 512}
    assert payload["normalized"] == {"board_buffer_pool_cap_mb": 256}
    assert payload["effective"] == {"board_buffer_pool_mb": 256}
    assert payload["process_envelope"] == {"max_derived_buffer_envelope_mb": 640}
    assert payload["is_direct_memory_telemetry"] is False
    payload["effective"]["board_buffer_pool_mb"] = 128
    assert BudgetRuntime().budget_snapshot().effective["board_buffer_pool_mb"] == 256


def test_provider_failure_is_bounded_and_fail_closed() -> None:
    class FailingBudgetRuntime(InMemoryGraphRuntimeStore):
        def budget_snapshot(self) -> GraphRuntimeBudgetSnapshot:
            raise RuntimeError(r"C:\private\board\graph.lbug secret payload")

    configure_test_kg_registry(
        graph_provider="inmemory",
        graph_runtime_store=FailingBudgetRuntime(),
    )

    payload = _build_native_runtime_budget()

    assert payload["status"] == "unavailable"
    assert payload["unavailable_reason"] == "budget_snapshot_unavailable"
    assert payload["requested"] == {}
    assert payload["normalized"] == {}
    assert payload["effective"] == {}
    assert payload["sources"] == {}
    assert payload["process_envelope"] == {}
    assert "private" not in str(payload).lower()
    assert "graph.lbug" not in str(payload).lower()
