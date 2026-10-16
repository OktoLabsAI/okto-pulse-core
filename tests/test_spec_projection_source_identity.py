"""Spec projection identity is its source, not a similarity-ranked neighbour."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.primitives import KGPrimitiveError, _require_spec_source_identity_matches
from okto_pulse.core.kg.reconciliation import ExistingNodeSummary, reconcile_candidate
from okto_pulse.core.kg.schemas import NodeCandidate, ReconciliationHint, ReconciliationOperation

KINDS = [('Requirement', 'fr'), ('Constraint', 'tr'), ('Criterion', 'ac'),
    ('Constraint', 'business_rule'), ('TestScenario', 'test_scenario'),
    ('Requirement', 'integration_requirement'), ('Constraint', 'observability_requirement'),
    ('APIContract', 'api_contract'), ('Decision', 'decision')]


def candidate(kind, section):
    return NodeCandidate(candidate_id='c', node_type=kind, title='Shared contract',
        content='Same normative condition', source_artifact_ref=f'spec:second:{section}:item', source_confidence=1.0)


@pytest.mark.parametrize('kind,section', KINDS)
def test_identical_content_of_different_spec_requires_distinct_identity(kind, section):
    node = candidate(kind, section)
    match = ExistingNodeSummary('foreign', kind, f'spec:first:{section}:item',
        title=node.title, content=node.content, similarity=1.0)
    hint = reconcile_candidate(node, nothing_changed=False, existing_matches=[match])
    assert hint.operation == ReconciliationOperation.ADD and hint.target_node_id is None


@pytest.mark.parametrize('kind,section', KINDS)
def test_stale_or_forced_hint_cannot_cross_source_identity(kind, section):
    node = candidate(kind, section)
    scope = SimpleNamespace(snapshot_node_properties=lambda *_:
        SimpleNamespace(attrs={'source_artifact_ref': f'spec:first:{section}:item'}))
    hint = ReconciliationHint(candidate_id='c', operation=ReconciliationOperation.UPDATE,
        target_node_id='foreign', confidence=1.0, reason='Forced stale selection')
    with pytest.raises(KGPrimitiveError) as error:
        _require_spec_source_identity_matches(scope, node_candidates={'c': node},
            effective_hints={'c': hint}, session_id='session')
    assert error.value.code == 'spec_source_identity_mismatch'


def test_same_source_decision_preserves_semantic_history():
    node = candidate('Decision', 'decision')
    match = ExistingNodeSummary('same', 'Decision', node.source_artifact_ref,
        title=node.title, content='Previous assertion', similarity=1.0)
    hint = reconcile_candidate(node, nothing_changed=False, existing_matches=[match])
    assert hint.operation == ReconciliationOperation.SUPERSEDE and hint.target_node_id == 'same'


def test_generic_cognitive_similarity_is_unchanged():
    node = NodeCandidate(candidate_id='c', node_type='Decision', title='New assertion',
        content='New content', source_artifact_ref='cognitive:second', source_confidence=1.0)
    match = ExistingNodeSummary('old', 'Decision', 'cognitive:first', title='Old assertion',
        content='Old content', similarity=1.0)
    assert reconcile_candidate(node, nothing_changed=False, existing_matches=[match]).operation == ReconciliationOperation.SUPERSEDE
