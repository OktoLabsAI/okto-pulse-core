"""Durable replay qualification composes source proof, never just connectivity."""

from copy import deepcopy
from dataclasses import replace

import pytest

from okto_pulse.core.ports.cognitive_projection import qualify_cognitive_replay, cognitive_projection_source_node
from test_cognitive_restoration import SCHEMA, source


def durable():
    record = source()
    record['committed_at'] = '2026-01-01T00:00:00.000000'
    record['payload']['source_artifact_ref'] = 'final_report:historical'
    record['evidence_refs'] = ['final_report:historical']
    return record


def qualify(record, *, node=None, records=None):
    node = node or cognitive_projection_source_node(schema=SCHEMA, board_id='board', record=record)
    return qualify_cognitive_replay(schema=SCHEMA, board_id='board', records=records or (record,),
        nodes=(node,), relations=(), restored=((node.type_name, node.key),))[0]


def test_complete_committed_source_reconciles_without_mutation_or_new_approval():
    record = durable()
    before = deepcopy(record)
    observed = qualify(record)
    assert observed.state == 'durable_replay_reconciled' and not observed.reasons
    assert observed.source_fingerprint and record == before
    assert not hasattr(observed, 'approved')


@pytest.mark.parametrize('change,reason', [
    ('working', 'durable_canonical_partition_unproven'),
    ('unknown_maturity', 'durable_canonical_partition_unproven'),
    ('missing_author', 'durable_commit_identity_missing'),
    ('different_session', 'durable_commit_identity_missing'),
    ('missing_commit_time', 'durable_commit_time_missing'),
    ('wrong_evidence', 'durable_technical_source_binding_unproven'),
    ('sprint', 'durable_technical_source_binding_unproven'),
    ('empty_report_identity', 'durable_technical_source_binding_unproven'),
])
def test_canonical_flag_or_technical_allowlist_cannot_replace_source_proof(change, reason):
    record = durable()
    if change == 'working': record['payload']['graph_layer'] = 'working'
    if change == 'unknown_maturity': del record['payload']['maturity_status']
    if change == 'missing_author': del record['payload']['created_by_agent']
    if change == 'different_session': record['payload']['source_session_id'] = 'kgses_other'
    if change == 'missing_commit_time': del record['committed_at']
    if change == 'wrong_evidence': record['evidence_refs'] = ['spec:unrelated']
    if change == 'sprint': record['payload']['source_artifact_ref'] = 'sprint:historical'
    if change == 'empty_report_identity': record['payload']['source_artifact_ref'] = 'final_report:'
    observed = qualify(record)
    assert observed.state == 'pending' and reason in observed.reasons


def test_parity_and_generation_selection_are_not_inferred():
    record = durable()
    node = cognitive_projection_source_node(schema=SCHEMA, board_id='board', record=record)
    changed = replace(node, properties={**node.properties, 'created_by_agent': 'another-author'})
    assert 'durable_payload_mismatch' in qualify(record, node=changed).reasons
    other = deepcopy(record)
    other['generation'] = other['payload']['generation'] = 1
    assert 'durable_source_missing_or_ambiguous_generation' in qualify(record, records=(record, other)).reasons


def test_learning_technical_root_does_not_imply_applicability():
    record = durable()
    record['node_type'] = 'Learning'
    assert 'learning_applicability_reconciliation_required' in qualify(record).reasons


def test_unselected_corrupt_source_is_not_hidden_by_empty_selection():
    record = durable()
    record['generation'] = False
    with pytest.raises(ValueError):
        qualify_cognitive_replay(schema=SCHEMA, board_id='board', records=(record,), nodes=(), relations=(), restored=())
