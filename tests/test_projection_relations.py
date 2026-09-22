"""Source relation comparison is typed, complete and independent of counts."""

from dataclasses import replace
import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalNode, LogicalNodeType, LogicalPropertyDef, LogicalRelation, LogicalRelationLayout, LogicalSchema,
)
from okto_pulse.core.kg.primitives import _cross_session_entity_source_prefix, _resolve_endpoint
from okto_pulse.core.ports.projection_relations import compare_projection_relations

PROPS = (LogicalPropertyDef('id', 'string', False), LogicalPropertyDef('generation', 'int64')) + tuple(
    LogicalPropertyDef(name, 'string') for name in ('source_artifact_ref', 'superseded_by'))
EDGE_PROPS = (LogicalPropertyDef('confidence', 'float64'),) + tuple(LogicalPropertyDef(name, 'string')
    for name in ('layer', 'rule_id', 'created_by', 'created_by_session_id', 'fallback_reason'))
SCHEMA = LogicalSchema('board', tuple(LogicalNodeType(kind, 'id', PROPS) for kind in ('Entity', 'Requirement')),
    tuple(LogicalRelationLayout(kind, 'Requirement', 'Entity', EDGE_PROPS) for kind in ('belongs_to', 'derives_from')))


def node(kind, key, source, **changes):
    return LogicalNode(kind, key, {'id': key, 'source_artifact_ref': source, 'generation': 0,
        'superseded_by': LOGICAL_NULL, **changes})


ROOT = node('Entity', 'root', 'spec:s')
CHILD = node('Requirement', 'child', 'spec:s:fr:r')
RELATION = LogicalRelation('belongs_to', 'Requirement', 'Entity', 'child', 'root',
    {'confidence': 1.0, 'layer': 'deterministic', 'rule_id': 'belongs_to/requirement@v1',
        'created_by': 'system:worker', 'created_by_session_id': 'new', 'fallback_reason': ''})


def document(endpoint='root', *, copies=1):
    candidates = [{'candidate_id': key, 'node_type': kind, 'source_artifact_ref': ref, 'title': key}
        for key, kind, ref in [('root', 'Entity', 'spec:s'), ('child', 'Requirement', 'spec:s:fr:r')]]
    edge = {'candidate_id': 'edge', 'edge_type': 'belongs_to', 'from_candidate_id': 'child',
        'to_candidate_id': endpoint, 'confidence': 1.0, 'layer': 'deterministic',
        'rule_id': 'belongs_to/requirement@v1', 'created_by': 'system:worker'}
    plan = {'source': {'board_id': 'board', 'artifact_type': 'spec', 'artifact_id': 's'},
        'projection': {'nodes': candidates, 'edges': [edge] * copies}}
    value = {'format': 'deterministic-board-projection-plan/v3', 'board_id': 'board',
        'captured_at': '2026-09-22T00:00:00+00:00', 'source_rows': [], 'cognitive_rows': [],
        'census': {}, 'dependency_closure': [], 'plans': [plan]}
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def compare(*, nodes=(ROOT, CHILD), relations=(RELATION,), plan=None):
    return compare_projection_relations(document=document() if plan is None else plan, schema=SCHEMA,
        nodes=nodes, relations=relations, new_sessions=('new',))


def test_exact_typed_relation_matches_and_is_order_independent():
    result = compare()
    assert (result.expected_count, result.matched_count, result.missing_count,
        result.unresolved_count, result.unexpected_new_count) == (1, 1, 0, 0, 0)
    assert result.issues == () and not result.issues_truncated
    assert compare(nodes=(CHILD, ROOT)) == result
    # The writer's edge-existence rule makes duplicate identical proposals a set.
    assert compare(plan=document(copies=2)) == result


@pytest.mark.parametrize('field,value', [('rule_id', 'different-rule'), ('layer', 'cognitive'),
    ('created_by', 'other-worker'), ('confidence', 0.9), ('fallback_reason', 'unplanned')])
def test_same_count_and_endpoints_do_not_hide_wrong_provenance(field, value):
    relation = replace(RELATION, properties={**RELATION.properties, field: value})
    result = compare(relations=(relation,))
    assert result.missing_count == result.unexpected_new_count == 1 and result.matched_count == 0


def test_same_count_does_not_hide_wrong_relation_kind():
    result = compare(relations=(replace(RELATION, layout_name='derives_from'),))
    assert result.missing_count == result.unexpected_new_count == 1


def test_old_unknown_provenance_is_not_invented_or_assigned_to_a_new_session():
    relation = replace(RELATION, properties={name: LOGICAL_NULL for name in RELATION.properties})
    result = compare(relations=(relation,))
    assert result.missing_count == 1 and result.unexpected_new_count == 0
    assert result.unplanned_existing_count == 1


def test_duplicate_expected_occurrences_are_all_left_unclassified():
    old = replace(RELATION, properties={**RELATION.properties, 'created_by_session_id': 'old'})
    result = compare(relations=(old, old))
    assert result.matched_count == 1 and result.missing_count == 0
    assert result.duplicate_expected_count == 2 and result.unplanned_existing_count == 0


def test_current_local_identity_does_not_select_a_superseded_generation():
    old = node('Entity', 'old', 'spec:s', generation=99, superseded_by='root')
    result = compare(nodes=(old, ROOT, CHILD))
    assert result.matched_count == 1
    assert compare(nodes=(old, ROOT, CHILD), relations=(replace(RELATION, target_key='old'),)).missing_count == 1


@pytest.mark.parametrize('endpoint', ['kg:root', 'kgref:Entity:spec:s', 'spec_s_entity'], ids=['literal', 'typed', 'prefix'])
def test_existing_endpoint_grammars_resolve_the_same_unique_typed_identity(endpoint):
    assert compare(plan=document(endpoint)).matched_count == 1


@pytest.mark.parametrize('endpoint', ['kgref:Entity:spec:s', 'spec_s_entity'], ids=['typed', 'prefix'])
def test_ambiguous_historical_lookup_never_chooses_an_arbitrary_winner(endpoint):
    other = node('Entity', 'other', 'spec:s')
    assert compare(nodes=(ROOT, CHILD, other), plan=document(endpoint)).unresolved_count == 1


def test_raw_id_ambiguity_is_not_confused_with_a_typed_local_candidate():
    other = node('Requirement', 'root', 'spec:s:fr:other')
    assert compare(nodes=(ROOT, CHILD, other)).matched_count == 1
    assert compare(nodes=(ROOT, CHILD, other), plan=document('kg:root')).unresolved_count == 1


@pytest.mark.parametrize('kind', ['story', 'ideation', 'refinement', 'spec', 'sprint', 'card'])
def test_shared_prefix_parser_preserves_the_live_lookup_and_parameters(kind):
    endpoint = kind + '_12345678_entity'
    assert _cross_session_entity_source_prefix(endpoint) == kind + ':12345678'
    calls = []
    def execute(query, params):
        calls.append(params)
        return SimpleNamespace(rows=(('existing',),))
    assert _resolve_endpoint(endpoint, {}, graph_scope=SimpleNamespace(execute=execute)) == ('existing', 'Entity')
    assert calls == [{'ref': kind + ':12345678'}]


def test_dangling_and_duplicate_inventory_records_are_refused():
    with pytest.raises(ValueError, match='duplicate_node'):
        compare(nodes=(ROOT, ROOT, CHILD))
    with pytest.raises(ValueError, match='endpoint_missing'):
        compare(relations=(replace(RELATION, target_key='absent'),))


def test_unrelated_historical_metadata_does_not_acquire_current_identity_requirements():
    old = node('Entity', 'old', 'legacy:' + 'x' * 1200, generation=-1, superseded_by='')
    assert compare(nodes=(ROOT, CHILD, old)).matched_count == 1


def test_diagnostics_are_bounded_without_hiding_unresolved_counts():
    plan = json.loads(document())
    prototype = plan['plans'][0]['projection']['edges'][0]
    plan['plans'][0]['projection']['edges'] = [{**prototype, 'candidate_id': f'edge-{index:03}',
        'to_candidate_id': f'unknown-{index:03}'} for index in range(101)]
    data = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    result = compare(plan=data, relations=())
    assert result.unresolved_count == 101 and result.expected_count == 0
    assert len(result.issues) == 100 and result.issues_truncated
