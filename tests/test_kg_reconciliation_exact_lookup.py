"""Exact source-ref reconciliation is reserved for immutable Decision lineage."""

from __future__ import annotations

from types import SimpleNamespace

import okto_pulse.core.kg.primitives as primitives
import okto_pulse.core.kg.search as search
from okto_pulse.core.kg.reconciliation import reconcile_candidate
from okto_pulse.core.kg.schemas import (
    NodeCandidate,
    ReconciliationOperation,
)


class _Embedder:
    def encode(self, _text: str):
        return [0.0]


class _ExactLookupStore:
    def __init__(self):
        self.calls: list[tuple[str, str, str]] = []

    def find_active_by_source_ref(
        self,
        board_id: str,
        node_type: str,
        source_ref: str,
    ):
        self.calls.append((board_id, node_type, source_ref))
        return {
            "node_id": "decision-generation-0",
            "node_type": node_type,
            "source_artifact_ref": source_ref,
            "title": "Choose Kafka",
            "content": "Kafka is the approved broker.",
            "context": "Event delivery",
            "justification": "Existing operational baseline",
        }


def test_exact_source_lookup_only_loads_decision_lineage(monkeypatch):
    store = _ExactLookupStore()
    monkeypatch.setattr(
        primitives,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_store=store),
    )
    monkeypatch.setattr(
        search,
        "find_similar_for_candidate",
        lambda **_kwargs: [],
    )

    entity = NodeCandidate(
        candidate_id="entity-candidate",
        node_type="Entity",
        title="Updated entity",
        content="Changed structural content",
        source_artifact_ref="spec:entity-1",
    )
    decision = NodeCandidate(
        candidate_id="decision-candidate",
        node_type="Decision",
        title="Choose RabbitMQ",
        content="RabbitMQ replaces Kafka.",
        context="Event delivery",
        justification="New operational constraints",
        source_artifact_ref="spec:decision-1",
    )

    matches = primitives._find_existing_graph_matches(
        "board-1",
        {
            entity.candidate_id: entity,
            decision.candidate_id: decision,
        },
        _Embedder(),
    )

    assert store.calls == [
        ("board-1", "Decision", "spec:decision-1"),
    ]
    assert entity.candidate_id not in matches
    assert len(matches[decision.candidate_id]) == 1

    hint = reconcile_candidate(
        decision,
        nothing_changed=False,
        existing_matches=matches[decision.candidate_id],
    )
    assert hint.operation == ReconciliationOperation.SUPERSEDE
    assert hint.target_node_id == "decision-generation-0"
