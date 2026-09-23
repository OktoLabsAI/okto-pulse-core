"""Read-only restoration diagnosis without inferred graph relationships."""

from collections import defaultdict

from okto_pulse.core.kg.logical_transfer import LOGICAL_NULL, LogicalFingerprintAccumulator
from okto_pulse.core.kg.relational_projection import is_relational_projection_node
from okto_pulse.core.ports.cognitive_projection import (
    CognitiveRestorationObservation, cognitive_projection_source_node,
    compare_cognitive_projection, validate_cognitive_projection_sources,
)
from okto_pulse.core.ports.projection_connectivity import observe_projection_connectivity


def observe(*, schema, board_id, records, nodes, relations):
    # Validate the full inventory even if no source is absent; never turn a
    # malformed/partial graph into a successful empty restoration observation.
    observe_projection_connectivity(schema=schema, board_id=board_id,
        nodes=nodes, relations=relations, selected=())
    sources = validate_cognitive_projection_sources(schema=schema, board_id=board_id, records=records)
    present = {(node.type_name, node.key) for node in nodes}
    grouped = defaultdict(list)
    for record in sources:
        key = record['node_type'], record['node_id']
        if key not in present:
            grouped[key].append(record)
    reports, candidates, fingerprints, generations = [], [], {}, {}

    def value(node, name):
        item = node.properties.get(name, LOGICAL_NULL)
        return None if item is LOGICAL_NULL else item

    for key, group in sorted(grouped.items()):
        epochs = tuple(sorted(record['generation'] for record in group))
        if len(group) != 1:
            reports.append(CognitiveRestorationObservation(*key, 'ambiguous_generation', epochs,
                ('multiple_durable_generations_for_one_graph_identity',)))
            continue
        record = group[0]
        node = cognitive_projection_source_node(schema=schema, board_id=board_id, record=record)
        parity = compare_cognitive_projection(schema=schema, board_id=board_id, record=record, node=node)
        if parity.state != 'matched':
            reports.append(CognitiveRestorationObservation(*key, 'projection_mismatch', epochs, parity.differing_fields))
            continue
        if is_relational_projection_node(node_type=node.type_name,
                source_artifact_ref=value(node, 'source_artifact_ref') or '',
                created_by_agent=value(node, 'created_by_agent') or ''):
            reports.append(CognitiveRestorationObservation(*key, 'relational_source', epochs,
                ('owned_by_relational_projection',)))
            continue
        measured = LogicalFingerprintAccumulator.for_schema(schema)
        measured.add_node(node)
        fingerprints[key], generations[key] = measured.digest(), epochs
        candidates.append(node)
    if candidates:
        # No relation is synthesized from evidence_refs or source_ref. Missing
        # semantic connections therefore remain visible to the ordinary guard.
        outcomes = observe_projection_connectivity(schema=schema, board_id=board_id,
            nodes=nodes + tuple(candidates), relations=relations,
            selected=tuple((node.type_name, node.key) for node in candidates))
        for outcome in outcomes:
            key = outcome.node_type, outcome.node_id
            reports.append(CognitiveRestorationObservation(*key,
                'connectivity_rejected' if outcome.outcome == 'rejected' else 'literal_candidate',
                generations[key], outcome.reasons, fingerprints[key]))
    return tuple(sorted(reports, key=lambda report: (report.node_type, report.node_id)))
