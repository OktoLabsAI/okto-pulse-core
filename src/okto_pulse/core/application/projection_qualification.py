"""Historical current-source coverage, without inventing missing provenance."""

from dataclasses import asdict

from okto_pulse.core.application.projection_history import _edges, _nodes
from okto_pulse.core.kg.logical_transfer import canonical_bytes
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.ports.projection_history import ProjectionHistoryDelta, ProjectionNodeChange
from okto_pulse.core.ports.projection_qualification import ProjectionHistoryQualification, ProjectionSourceObservation
from okto_pulse.core.ports.projection_relations import ProjectionRelationComparison


def qualify(*, history, sources, relations):
    if (type(history) is not ProjectionHistoryDelta or type(relations) is not ProjectionRelationComparison
            or type(sources) is not tuple or len(sources) > 100_000):
        raise ValueError('projection_qualification_evidence_invalid')
    if history.removed_nodes or history.removed_edges:
        raise ValueError('projection_qualification_removal_unclassified')
    if (type(history.changed_nodes) is not tuple or len(history.changed_nodes) > 100_000
            or any(type(change) is not ProjectionNodeChange or
                (change.before.node_type, change.before.node_id) != (change.after.node_type, change.after.node_id)
                for change in history.changed_nodes)):
        raise ValueError('projection_qualification_change_invalid')
    old_records = _nodes(history.unchanged_nodes + tuple(change.after for change in history.changed_nodes))
    if any(kind not in {*NODE_TYPES, 'BoardMeta'} for kind, _ in old_records):
        raise ValueError('projection_qualification_node_type_unknown')
    old = {key for key in old_records if key[0] in NODE_TYPES}
    retained_records = _edges(history.retained_edges)
    retained = sum(edge.count for edge in retained_records.values())
    seen, covered, budget = set(), set(), len(canonical_bytes(asdict(relations)))

    def charge(record):
        nonlocal budget
        budget += len(canonical_bytes(asdict(record)))
        if budget > 64 * 1024 * 1024:
            raise ValueError('projection_qualification_evidence_limit')

    for record in old_records.values():
        charge(record)
    for record in retained_records.values():
        charge(record)
    for change in history.changed_nodes:
        charge(change.before)
    for source in sources:
        if type(source) is not ProjectionSourceObservation:
            raise TypeError('projection_qualification_source_observation_required')
        charge(source)
        key = source.identity.node_type, source.identity.node_id
        if key in seen:
            raise ValueError('projection_qualification_duplicate_source')
        seen.add(key)
        if source.expected == source.observed:
            covered.add(key)
    qualified = old & covered
    reasons = []
    if old - qualified:
        reasons.append('historical_source_unclassified')
    relation_mismatch = any((relations.missing_count, relations.unresolved_count, relations.unexpected_new_count))
    if relation_mismatch:
        reasons.append('current_relations_unreconciled')
    if relations.unplanned_existing_count:
        reasons.append('historical_relation_unplanned')
    if relations.duplicate_expected_count:
        reasons.append('historical_relation_duplicate')
    qualified_relations = retained if not (relation_mismatch or relations.unplanned_existing_count
        or relations.duplicate_expected_count) else 0
    state = ('not_applicable' if not old and not retained else
        'pending' if reasons else 'current_source_reconciled')
    return ProjectionHistoryQualification(state, len(qualified), len(old - qualified), qualified_relations,
        retained - qualified_relations, tuple(reasons))
