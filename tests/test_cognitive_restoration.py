"""Literal recovery diagnosis cannot invent generations or semantic edges."""

from copy import deepcopy
from dataclasses import replace

import pytest

from okto_pulse.core.kg.logical_transfer import LogicalPropertyDef, LogicalNodeType
from okto_pulse.core.ports.cognitive_projection import observe_cognitive_restoration, cognitive_projection_source_node
from test_projection_connectivity import SCHEMA as BASE_SCHEMA

SCHEMA = replace(BASE_SCHEMA, node_types=tuple(LogicalNodeType(kind.name, kind.key,
    kind.properties + (LogicalPropertyDef('generation', 'int64'),)) for kind in BASE_SCHEMA.node_types))


def source(generation=0):
    return {'board_id': 'board', 'node_type': 'Decision', 'node_id': 'missing', 'generation': generation,
        'source_revision': 0, 'source_session_id': 'kgses_old', 'evidence_refs': ['spec:s'],
        'payload': {'generation': generation, 'created_by_agent': 'agent-a', 'source_artifact_ref': 'spec:s',
            'graph_layer': 'canonical', 'maturity_status': 'canonical_eligible'}}


def observe(records, nodes=(), relations=()):
    return observe_cognitive_restoration(schema=SCHEMA, board_id='board', records=records, nodes=nodes, relations=relations)


def test_missing_semantic_edges_are_reported_without_mutating_source_or_fabricating_relationships():
    record = source()
    before = deepcopy(record)
    report, = observe((record,))
    assert report.state == 'connectivity_rejected' and report.reasons
    assert len(report.literal_fingerprint) == 64 and record == before


def test_same_graph_identity_in_two_generations_is_not_resolved_by_input_order():
    records = (source(1), source(0))
    report, = observe(records)
    assert report.state == 'ambiguous_generation' and report.generations == (0, 1)
    assert observe(records[::-1]) == (report,)
    assert report.literal_fingerprint is None


def test_existing_node_is_not_replaced_even_when_source_has_different_content():
    record = source()
    node = cognitive_projection_source_node(schema=SCHEMA, board_id='board', record=record)
    node = replace(node, properties={**node.properties, 'created_by_agent': 'human'})
    assert observe((record,), (node,)) == ()


def test_missing_generation_is_not_filled_from_external_metadata():
    record = source()
    del record['payload']['generation']
    report, = observe((record,))
    assert report.state == 'projection_mismatch' and report.reasons == ('generation',)


def test_relationally_owned_decision_is_left_to_its_authoritative_projection():
    record = source()
    record['payload'].update(created_by_agent='system:projection', source_artifact_ref='refinement:r:rdl:d:decision')
    report, = observe((record,))
    assert report.state == 'relational_source'


def test_existing_identity_cannot_hide_invalid_durable_revision():
    record = source()
    node = cognitive_projection_source_node(schema=SCHEMA, board_id='board', record=record)
    record['generation'] = False
    with pytest.raises(ValueError, match='source_invalid'):
        observe((record,), (node,))


def test_existing_final_report_exception_is_observed_without_granting_admission():
    record = source()
    record['payload']['source_artifact_ref'] = 'final_report:historical'
    report, = observe((record,))
    assert report.state == 'literal_candidate' and not report.reasons
    assert report.literal_fingerprint and not hasattr(report, 'admitted')
