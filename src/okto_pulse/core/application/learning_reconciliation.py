"""Find replayable authorship without retrospectively inventing semantics."""
from collections import defaultdict
import json

from okto_pulse.core.domain.learning_materialization_work import LearningCaptureWorkRef
from okto_pulse.core.kg.logical_transfer import LogicalSchemaIndex
from okto_pulse.core.ports.cognitive_projection import validate_cognitive_projection_sources
from okto_pulse.core.ports.kg_cognitive_source import canonical_cognitive_source_fingerprint
from okto_pulse.core.ports.learning_reconciliation import LearningReconciliationSelection


def select(*, schema, board_id, records, nodes):
    # Validate the complete history even if there is no Learning to return.
    latest = validate_cognitive_projection_sources(schema=schema, board_id=board_id, records=records)
    if type(nodes) is not tuple or len(nodes) > 100_000:
        raise ValueError('learning_reconciliation_graph_limit')
    index, seen, identities = LogicalSchemaIndex.build(schema), set(), set()
    for node in nodes:
        index.validate_node(node)
        identity = node.type_name, node.key
        if identity in seen:
            raise ValueError('learning_reconciliation_graph_identity_duplicate')
        seen.add(identity)
        if node.type_name == 'Learning':
            identities.add(node.key)
    generations, captures = defaultdict(set), defaultdict(dict)
    for row in latest:
        if row['node_type'] == 'Learning':
            identities.add(row['node_id'])
            generations[row['node_id']].add(row['generation'])
    for row in records:
        if row['node_type'] != 'Learning':
            continue
        payload = json.loads(row['payload']) if type(row['payload']) is str else row['payload']
        if 'capture_format' not in payload:
            continue
        bug_id = payload['source']['bug_id']
        key = row['node_id'], row['generation']
        previous = captures[key].get(bug_id)
        if previous is None or row.get('source_revision', 0) > previous[0].get('source_revision', 0):
            captures[key][bug_id] = (row, payload)
    result = []
    for identity in sorted(identities):
        versions = tuple(sorted(generations[identity]))
        if len(versions) > 1:
            result.append(LearningReconciliationSelection(identity, versions, 'ambiguous_generation',
                ('learning_historical_generation_ambiguous',)))
            continue
        selected = captures.get((identity, versions[0]), {}) if versions else {}
        if not selected:
            result.append(LearningReconciliationSelection(identity, versions, 'source_unavailable',
                ('learning_original_semantic_source_unavailable',)))
            continue
        refs = []
        for bug_id, (row, payload) in sorted(selected.items()):
            evidence = json.loads(row['evidence_refs']) if type(row['evidence_refs']) is str else row['evidence_refs']
            fingerprint = canonical_cognitive_source_fingerprint(board_id=board_id,
                node_type='Learning', node_id=identity, generation=versions[0],
                payload=payload, evidence_refs=evidence)
            refs.append(LearningCaptureWorkRef(bug_id, identity, versions[0], fingerprint).encode())
        result.append(LearningReconciliationSelection(identity, versions, 'awaiting_revalidation',
            ('learning_source_evidence_binding_and_lineage_revalidation_required',), tuple(refs)))
    return tuple(result)
