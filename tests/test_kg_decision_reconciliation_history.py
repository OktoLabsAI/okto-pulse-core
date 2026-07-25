from __future__ import annotations

from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.primitives import (
    _preserve_decision_history_for_updates,
)
from okto_pulse.core.kg.schemas import (
    KGNodeType,
    NodeCandidate,
    ReconciliationHint,
    ReconciliationOperation,
)


class _SemanticGraphScope:
    def __init__(self, semantics):
        self.semantics = semantics

    def execute(self, statement, params):
        del statement, params
        if self.semantics is None:
            return GraphStatementResult()
        return GraphStatementResult.from_rows([self.semantics])


def _update_hint(candidate_id="decision-candidate"):
    return ReconciliationHint(
        candidate_id=candidate_id,
        operation=ReconciliationOperation.UPDATE,
        target_node_id="decision-generation-0",
        confidence=0.99,
        reason="agent requested an in-place update",
    )


def test_commit_boundary_converts_semantic_decision_update_to_supersede():
    candidate = NodeCandidate(
        candidate_id="decision-candidate",
        node_type=KGNodeType.DECISION,
        title="Use an event bus",
        content="Choose Kafka",
        context="High-volume stream",
        justification="Operational maturity",
        source_artifact_ref="spec:1:decision:dec_1",
    )
    scope = _SemanticGraphScope(
        (
            "Use an event bus",
            "Choose RabbitMQ",
            "High-volume stream",
            "Operational maturity",
        )
    )

    guarded = _preserve_decision_history_for_updates(
        graph_scope=scope,
        node_candidates={candidate.candidate_id: candidate},
        effective_hints={candidate.candidate_id: _update_hint()},
    )

    assert (
        guarded[candidate.candidate_id].operation
        == ReconciliationOperation.SUPERSEDE
    )
    assert guarded[candidate.candidate_id].target_node_id == (
        "decision-generation-0"
    )


def test_commit_boundary_keeps_identical_decision_reattestation_as_update():
    semantics = (
        "Use an event bus",
        "Choose Kafka",
        "High-volume stream",
        "Operational maturity",
    )
    candidate = NodeCandidate(
        candidate_id="decision-candidate",
        node_type=KGNodeType.DECISION,
        title=semantics[0],
        content=semantics[1],
        context=semantics[2],
        justification=semantics[3],
        source_artifact_ref="spec:1:decision:dec_1",
    )
    original = _update_hint()

    guarded = _preserve_decision_history_for_updates(
        graph_scope=_SemanticGraphScope(semantics),
        node_candidates={candidate.candidate_id: candidate},
        effective_hints={candidate.candidate_id: original},
    )

    assert guarded[candidate.candidate_id] is original
