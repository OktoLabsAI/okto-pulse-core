"""KG-10: exact internal Spec provenance never grants generic cognitive authority."""
from dataclasses import replace

import pytest

from test_kg_decision_declared_relationships import spec
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from okto_pulse.core.kg.connectivity_guard import KGNodeConnectivityGuard
from okto_pulse.core.kg.primitives import _validated_deterministic_spec_decision_grants


def batch():
    value = spec()
    value["decisions"] = [{"id": "dec_one", "title": "Choice", "linked_requirements": []}]
    return DeterministicWorker().process_spec(value)


def arguments(result):
    return dict(agent_id="system:historical_consolidation", session_artifact_type="spec",
        session_artifact_id="owner",
        node_candidates={n.candidate_id: n for n in result.nodes},
        edge_candidates={e.candidate_id: e for e in result.edges},
        relational_projection_active_set_intents=result.relational_projection_active_set_intents)


def validate(result, grants, writer="deterministic_worker"):
    return KGNodeConnectivityGuard().validate(board_id="board", writer_path=writer,
        kg_health_state="healthy", nodes=result.nodes, edges=result.edges,
        deterministic_spec_decision_candidate_ids=grants)


def test_exact_owner_allows_provenance_without_inventing_judgement():
    result = batch()
    before = tuple(result.edges)
    decision = next(n for n in result.nodes if n.node_type == "Decision")
    grants = _validated_deterministic_spec_decision_grants(**arguments(result))
    assert grants == frozenset({decision.candidate_id})
    assert validate(result, grants).passed
    denied = validate(result, frozenset())
    assert not denied.passed
    assert any(v.node_type == "Decision" for v in denied.violations)
    assert tuple(result.edges) == before
    assert not any(e.edge_type in {"derives_from", "mentions", "relates_to"} for e in result.edges)


@pytest.mark.parametrize("forgery", [
    "agent", "artifact_type", "owner", "node_owner", "root_owner", "root_type",
    "intent_owner", "missing_intent", "mutable_intents", "node_key",
    "missing_provenance", "edge_owner", "rule", "layer", "writer", "duplicate_provenance",
])
def test_invalid_identity_and_provenance_cannot_obtain_grant(forgery):
    result = batch()
    args = arguments(result)
    decision = next(n for n in result.nodes if n.node_type == "Decision")
    root = next(n for n in result.nodes if n.source_artifact_ref == "spec:owner")
    edge = next(e for e in result.edges if e.from_candidate_id == decision.candidate_id)
    if forgery == "agent": args["agent_id"] = "system:other_worker"
    elif forgery == "artifact_type": args["session_artifact_type"] = "card"
    elif forgery == "owner": args["session_artifact_id"] = "other"
    elif forgery == "node_owner": args["node_candidates"][decision.candidate_id] = replace(decision, source_artifact_ref="spec:other:decision:dec_one")
    elif forgery == "root_owner": args["node_candidates"][root.candidate_id] = replace(root, source_artifact_ref="spec:other")
    elif forgery == "root_type": args["node_candidates"][root.candidate_id] = replace(root, node_type="Bug")
    elif forgery == "intent_owner":
        args["relational_projection_active_set_intents"] = tuple(replace(i, owner_id="other") for i in result.relational_projection_active_set_intents)
    elif forgery == "missing_intent": args["relational_projection_active_set_intents"] = ()
    elif forgery == "mutable_intents": args["relational_projection_active_set_intents"] = list(result.relational_projection_active_set_intents)
    elif forgery == "node_key": args["node_candidates"]["forged"] = args["node_candidates"].pop(decision.candidate_id)
    elif forgery == "missing_provenance": del args["edge_candidates"][edge.candidate_id]
    elif forgery == "edge_owner": args["edge_candidates"][edge.candidate_id] = replace(edge, to_candidate_id="other")
    elif forgery == "rule": args["edge_candidates"][edge.candidate_id] = replace(edge, rule_id="belongs_to/arbitrary@v2.0")
    elif forgery == "layer": args["edge_candidates"][edge.candidate_id] = replace(edge, layer="cognitive")
    elif forgery == "writer": args["edge_candidates"][edge.candidate_id] = replace(edge, created_by="agent")
    elif forgery == "duplicate_provenance": args["edge_candidates"]["duplicate"] = replace(edge, candidate_id="duplicate")
    assert _validated_deterministic_spec_decision_grants(**args) == frozenset()


@pytest.mark.parametrize("writer", ["cognitive_agent", "unknown"])
def test_generic_writer_cannot_spend_internal_grant(writer):
    result = batch()
    grants = _validated_deterministic_spec_decision_grants(**arguments(result))
    denied = validate(result, grants, writer=writer)
    assert not denied.passed
    assert any(v.node_type == "Decision" for v in denied.violations)


def test_mutable_grant_does_not_relax_guard():
    result = batch()
    grants = _validated_deterministic_spec_decision_grants(**arguments(result))
    assert not validate(result, list(grants)).passed


def test_internal_grant_never_dispenses_with_provenance():
    result = batch()
    grants = _validated_deterministic_spec_decision_grants(**arguments(result))
    result.edges = [edge for edge in result.edges if edge.from_candidate_id not in grants]
    denied = validate(result, grants)
    assert not denied.passed
    assert any(v.node_type == "Decision" and v.required_edge.startswith("belongs_to:")
               for v in denied.violations)
