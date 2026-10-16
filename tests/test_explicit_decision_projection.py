"""G6 projects normative replacement independently of cognitive generations."""

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker


def project(decisions):
    return DeterministicWorker().process_spec({
        'id': 'spec', 'title': 'Spec', 'status': 'done', 'context': '',
        'functional_requirements': [{'id': 'fr', 'text': 'Behavior'}],
        'technical_requirements': [], 'decisions': decisions,
    })


def history():
    return [
        {'id': 'old', 'title': 'Earlier choice', 'status': 'superseded',
         'linked_requirements': ['fr']},
        {'id': 'new', 'title': 'Current choice', 'status': 'active',
         'supersedes_decision_id': 'old', 'linked_requirements': ['fr']},
    ]


def test_history_has_explicit_owned_link_and_retains_each_decisions_declared_provenance():
    result = project(history())
    refs = {node.candidate_id: node.source_artifact_ref for node in result.nodes}
    replacement = [edge for edge in result.edges if edge.edge_type == 'supersedes']
    assert len(replacement) == 1
    edge, = replacement
    assert (refs[edge.from_candidate_id], refs[edge.to_candidate_id]) == (
        'spec:spec:decision:new', 'spec:spec:decision:old')
    intent, = [item for item in result.relational_projection_active_set_intents
               if item.namespace == 'decision_supersedence']
    assert [item.candidate_id for item in intent.active_edges] == [edge.candidate_id]
    assert {refs[edge.from_candidate_id] for edge in result.edges
            if edge.rule_id == 'derives_from/explicit_link@v2.1'} == {
                'spec:spec:decision:new', 'spec:spec:decision:old'}


def test_link_removal_keeps_native_history_and_declares_empty_owned_set():
    decisions = history()
    decisions[1].pop('supersedes_decision_id')
    result = project(decisions)
    assert {node.source_artifact_ref for node in result.nodes if node.node_type == 'Decision'} == {
        'spec:spec:decision:old', 'spec:spec:decision:new'}
    intent, = [item for item in result.relational_projection_active_set_intents
               if item.namespace == 'decision_supersedence']
    assert intent.active_edges == ()


def test_missing_predecessor_is_reported_without_fabricating_identity():
    result = project(history()[1:])
    assert not [edge for edge in result.edges if edge.edge_type == 'supersedes']
    assert any(item.reason == 'decision_predecessor_missing'
               for item in result.missing_link_candidates)
    assert not any(node.source_artifact_ref == 'spec:spec:decision:old' for node in result.nodes)
