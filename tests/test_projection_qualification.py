"""Only source-covered historical identities and relations are reconciled."""

from dataclasses import replace

import pytest

from okto_pulse.core.ports.projection_history import (
    ProjectionEdgeFingerprint, ProjectionHistoryDelta, ProjectionNodeFingerprint, ProjectionSourceIdentity,
)
from okto_pulse.core.ports.projection_qualification import ProjectionSourceObservation, qualify_projection_history
from okto_pulse.core.ports.projection_relations import ProjectionRelationComparison

NODE = ProjectionNodeFingerprint('Entity', 'current', 'a' * 64)
EDGE = ProjectionEdgeFingerprint('belongs_to', 'Entity', 'current', 'Entity', 'parent', 'b' * 64, 1)
HISTORY = ProjectionHistoryDelta((NODE,), (), (), (), (EDGE,), (), ())
FIELDS = (1, 2, 'draft', None, None, 'working', 'working_immature')
SOURCE = ProjectionSourceObservation(ProjectionSourceIdentity('Entity', 'current', 'spec:s', 0, None), FIELDS, FIELDS)
RELATIONS = ProjectionRelationComparison(1, 1, 0, 0, 0, 0, 0, 'c' * 64, (), False)


def qualify(history=HISTORY, sources=(SOURCE,), relations=RELATIONS):
    return qualify_projection_history(history=history, sources=sources, relations=relations)


def test_current_source_coverage_is_explicit_and_does_not_grant_admission():
    result = qualify()
    assert result.state == 'current_source_reconciled'
    assert result.current_source_node_count == result.current_relation_count == 1
    assert result.unclassified_node_count == result.unclassified_relation_count == 0
    assert result.reasons == () and not hasattr(result, 'runtime_ready')


def test_same_source_reference_does_not_reclassify_a_different_historical_identity():
    previous = replace(NODE, node_id='previous')
    result = qualify(history=replace(HISTORY, unchanged_nodes=(NODE, previous)))
    assert result.state == 'pending'
    assert result.current_source_node_count == result.unclassified_node_count == 1


@pytest.mark.parametrize('index,value', [(0, None), (1, 3), (2, 'done'), (3, 'critical'),
    (4, 5), (5, 'canonical'), (6, 'canonical_eligible')])
def test_maturity_and_each_source_metadata_field_are_required(index, value):
    fields = list(FIELDS)
    fields[index] = value
    result = qualify(sources=(replace(SOURCE, observed=tuple(fields)),))
    assert result.state == 'pending' and result.unclassified_node_count == 1


@pytest.mark.parametrize('changes,reason', [({'missing_count': 1, 'matched_count': 0}, 'current_relations_unreconciled'),
    ({'unresolved_count': 1}, 'current_relations_unreconciled'),
    ({'unexpected_new_count': 1}, 'current_relations_unreconciled'),
    ({'unplanned_existing_count': 1}, 'historical_relation_unplanned'),
    ({'duplicate_expected_count': 2}, 'historical_relation_duplicate')])
def test_relation_ambiguity_never_qualifies_retained_edges(changes, reason):
    result = qualify(relations=replace(RELATIONS, **changes))
    assert result.state == 'pending' and reason in result.reasons
    assert result.current_relation_count == 0 and result.unclassified_relation_count == 1


def test_physical_schema_record_is_not_a_domain_source_and_removals_stay_closed():
    meta = replace(NODE, node_type='BoardMeta')
    result = qualify(history=replace(HISTORY, unchanged_nodes=(meta,), retained_edges=()), sources=())
    assert result.state == 'not_applicable'
    with pytest.raises(ValueError, match='removal_unclassified'):
        qualify(history=replace(HISTORY, removed_nodes=(NODE,)))
    with pytest.raises(ValueError, match='duplicate_source'):
        qualify(sources=(SOURCE, SOURCE))


def test_observations_do_not_accept_truthy_timestamps_or_inconsistent_relation_counts():
    with pytest.raises(ValueError, match='source_fields_invalid'):
        replace(SOURCE, observed=(True, *FIELDS[1:]))
    with pytest.raises(ValueError, match='comparison_invalid'):
        replace(RELATIONS, matched_count=0)
    with pytest.raises(ValueError, match='comparison_invalid'):
        replace(RELATIONS, missing_count=True)
