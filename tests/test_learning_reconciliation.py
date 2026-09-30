"""Historical work selection preserves uncertainty and never grants admission."""
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from okto_pulse.core.domain.learning_materialization_work import parse_learning_capture_work_ref
from okto_pulse.core.ports.cognitive_projection import cognitive_projection_source_node
from okto_pulse.core.kg.logical_transfer import LogicalPropertyDef
from okto_pulse.core.ports.kg_cognitive_source import canonical_cognitive_source_fingerprint, CognitiveSourceConflict
from okto_pulse.core.ports.learning_reconciliation import select_learning_reconciliation
from test_learning_capture_projection import capture_record
from test_cognitive_replay_qualification import durable, SCHEMA as BASE_SCHEMA

SCHEMA = replace(BASE_SCHEMA, node_types=tuple(replace(kind,
    properties=kind.properties + (LogicalPropertyDef('source_content_hash', 'string'),))
    for kind in BASE_SCHEMA.node_types))


def select(records=(), nodes=()):
    return select_learning_reconciliation(schema=SCHEMA, board_id='board', records=records, nodes=nodes)


def fingerprint(row):
    return canonical_cognitive_source_fingerprint(**{key: row[key] for key in (
        'board_id', 'node_type', 'node_id', 'generation', 'payload', 'evidence_refs')})


def test_complete_history_selects_latest_explicit_capture_per_origin_without_mutation():
    first = capture_record()
    other, newer = deepcopy(first), deepcopy(first)
    other['source_revision'] = 1
    other['payload']['source']['bug_id'] = 'second-origin'
    newer['source_revision'] = 2
    newer['payload']['applicability'] = 'Current explicit scope'
    records = (newer, first, other)
    before = deepcopy(records)
    result, = select(records)
    assert result.state == 'awaiting_revalidation'
    assert result.generations == (0,)
    assert result.reasons == ('learning_source_evidence_binding_and_lineage_revalidation_required',)
    work = tuple(parse_learning_capture_work_ref(ref) for ref in result.work_refs)
    assert [(item.bug_id, item.fingerprint) for item in work] == [
        ('bug-a', fingerprint(newer)), ('second-origin', fingerprint(other))]
    assert all(item.learning_id == 'old' and item.generation == 0 for item in work)
    assert records == before
    assert not hasattr(result, 'approved')
    encoded = tuple({**row, 'payload': json.dumps(row['payload']),
        'evidence_refs': json.dumps(row['evidence_refs'])} for row in records)
    assert select(encoded) == (result,)


def test_multiple_generations_do_not_choose_one_even_with_canonical_graph():
    first, second = capture_record(), capture_record()
    second['generation'] = 1
    result, = select((first, second))
    assert result.state == 'ambiguous_generation'
    assert result.generations == (0, 1)
    assert result.work_refs == ()


@pytest.mark.parametrize('with_source', [False, True])
def test_legacy_narrative_hash_or_graph_edge_cannot_invent_semantic_version(with_source):
    record = durable()
    record['node_type'] = 'Learning'
    record['payload']['source_content_hash'] = 'a' * 64
    node = cognitive_projection_source_node(schema=SCHEMA, board_id='board', record=record)
    result, = select((record,) if with_source else (), (node,))
    assert result.state == 'source_unavailable'
    assert result.reasons == ('learning_original_semantic_source_unavailable',)
    assert result.work_refs == ()


@pytest.mark.parametrize('change', ['fingerprint', 'board', 'payload', 'revision'])
def test_old_corruption_cannot_be_hidden_by_later_capture(change):
    old, new = capture_record(), capture_record()
    new['source_revision'] = 1
    if change == 'fingerprint': old['record_fingerprint'] = '0' * 64
    if change == 'board': old['board_id'] = 'other'
    if change == 'payload': old['payload']['trusted'] = True
    if change == 'revision': old['source_revision'] = False
    with pytest.raises((ValueError, CognitiveSourceConflict)):
        select((old, new))


def test_nonlearning_corruption_and_duplicate_graph_still_fail_closed():
    row = durable()
    row['record_fingerprint'] = '0' * 64
    with pytest.raises(CognitiveSourceConflict):
        select((row,))
    row.pop('record_fingerprint')
    node = cognitive_projection_source_node(schema=SCHEMA, board_id='board', record=row)
    with pytest.raises(ValueError, match='graph_identity_duplicate'):
        select((), (node, node))
    with pytest.raises(ValueError, match='graph_limit'):
        select((), [])


def test_empty_inventory_does_not_invent_debt():
    assert select() == ()
