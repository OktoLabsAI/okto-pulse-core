from __future__ import annotations

import pytest

import kg_registry_testing
from kg_registry_testing import (
    configure_real_graph_test_kg_registry,
    configure_test_kg_registry,
)
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryBoardGraphRuntime,
)


def _missing_community() -> None:
    raise ModuleNotFoundError("No module named 'okto_pulse.community'")


def test_af04_contract_fake_mode_supplies_core_board_graph_runtime(monkeypatch):
    monkeypatch.setattr(
        kg_registry_testing,
        "_community_graph_providers",
        _missing_community,
    )
    monkeypatch.setattr(
        kg_registry_testing,
        "_community_source_reader",
        _missing_community,
    )
    monkeypatch.setattr(
        kg_registry_testing,
        "_community_rebuild_ingestion",
        _missing_community,
    )

    configure_test_kg_registry(graph_provider="real_if_available")

    registry = get_kg_registry()
    assert isinstance(registry.board_graph_runtime, InMemoryBoardGraphRuntime)

    from okto_pulse.core.kg import schema

    handle = schema.bootstrap_board_graph("board-contract")
    assert handle.board_id == "board-contract"
    assert schema.board_kuzu_path("board-contract").name == "graph.memory"
    with pytest.raises(RuntimeError, match="Configure the Community board_graph_runtime"):
        schema.open_board_connection("board-contract")


def test_af04_real_integration_mode_skips_when_community_runtime_is_unavailable(
    monkeypatch,
):
    monkeypatch.setattr(
        kg_registry_testing,
        "_community_graph_providers",
        _missing_community,
    )

    with pytest.raises(pytest.skip.Exception) as exc_info:
        configure_real_graph_test_kg_registry()

    assert "explicitly requires the real Community runtime" in str(exc_info.value)
