"""Decision history must preserve the source artifact provenance."""

from __future__ import annotations

from okto_pulse.core.kg import cypher_templates
from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
from okto_pulse.core.kg.kg_service import KGService
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphStore,
)


def test_decision_history_cypher_projects_source_artifact_ref() -> None:
    assert "d.source_artifact_ref" in cypher_templates.GET_DECISION_HISTORY


def test_memory_store_decision_history_row_includes_source_artifact_ref() -> None:
    store = InMemoryGraphStore()
    store.create_node(
        "board",
        "Decision",
        "decision-1",
        {
            "title": "Audit export scope boundary",
            "content": "Keep exports board-scoped.",
            "created_at": "2026-07-25T00:00:00+00:00",
            "source_confidence": 0.9,
            "relevance_score": 0.8,
            "source_artifact_ref": "ideation:ideation-1:decision:scope_boundary",
        },
    )

    rows = store.find_by_topic(
        "board",
        "Decision",
        "scope boundary",
        QueryFilters(),
    )

    assert rows[0][7] == "ideation:ideation-1:decision:scope_boundary"


def test_decision_history_service_projects_source_artifact_ref(monkeypatch) -> None:
    source_ref = "ideation:ideation-1:decision:scope_boundary"

    class FakeStore:
        def find_by_topic(self, *_args):
            return [
                [
                    "decision-1",
                    "Audit export scope boundary",
                    "Keep exports board-scoped.",
                    "2026-07-25T00:00:00+00:00",
                    0.9,
                    0.8,
                    None,
                    source_ref,
                ]
            ]

    import okto_pulse.core.kg.kg_service as kg_service_module

    monkeypatch.setattr(kg_service_module, "_get_graph_store", lambda: FakeStore())

    result = KGService().get_decision_history(
        "board",
        "scope boundary",
        use_semantic=False,
    )

    assert result[0]["source_artifact_ref"] == source_ref


def test_decision_history_service_keeps_legacy_seven_column_store_compatible(
    monkeypatch,
) -> None:
    class LegacyStore:
        def find_by_topic(self, *_args):
            return [
                [
                    "decision-legacy",
                    "Legacy",
                    "Legacy content",
                    "2026-07-25T00:00:00+00:00",
                    0.9,
                    0.8,
                    None,
                ]
            ]

    import okto_pulse.core.kg.kg_service as kg_service_module

    monkeypatch.setattr(kg_service_module, "_get_graph_store", lambda: LegacyStore())

    result = KGService().get_decision_history(
        "board",
        "legacy",
        use_semantic=False,
    )

    assert result[0]["source_artifact_ref"] is None
