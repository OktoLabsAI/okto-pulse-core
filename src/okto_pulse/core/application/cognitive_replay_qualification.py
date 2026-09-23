"""Preserve the committed durable-source meaning without promoting new knowledge."""

from collections import defaultdict
from datetime import datetime
import json

from okto_pulse.core.kg.cognitive_source_ref_resolver import resolve_cognitive_source_ref, CognitiveRefResolutionStatus
from okto_pulse.core.kg.logical_transfer import LOGICAL_NULL
from okto_pulse.core.ports.cognitive_projection import (
    CognitiveReplayQualification, compare_cognitive_projection, validate_cognitive_projection_sources,
)
from okto_pulse.core.ports.projection_connectivity import observe_projection_connectivity


def qualify(*, schema, board_id, records, nodes, relations, restored):
    # Full graph and full revision validation precede filtering. An empty
    # selection must not hide malformed edges or a corrupt older revision.
    connectivity = observe_projection_connectivity(schema=schema, board_id=board_id,
        nodes=nodes, relations=relations, selected=restored)
    latest = validate_cognitive_projection_sources(schema=schema, board_id=board_id, records=records)
    by_key = {(node.type_name, node.key): node for node in nodes}
    grouped = defaultdict(list)
    for record in latest:
        grouped[(record['node_type'], record['node_id'])].append(record)
    connected = {(edge.source_type, edge.source_key) for edge in relations} | {
        (edge.target_type, edge.target_key) for edge in relations}
    outcomes = {(row.node_type, row.node_id): row for row in connectivity}
    results = []

    def value(node, name):
        item = node.properties.get(name, LOGICAL_NULL)
        return None if item is LOGICAL_NULL else item

    for key in sorted(restored):
        reasons, fingerprint = [], None
        sources = grouped.get(key, ())
        if len(sources) != 1:
            reasons.append('durable_source_missing_or_ambiguous_generation')
        else:
            source, node = sources[0], by_key[key]
            parity = compare_cognitive_projection(schema=schema, board_id=board_id, record=source, node=node)
            fingerprint = parity.source_fingerprint
            if parity.state != 'matched':
                reasons.append('durable_payload_mismatch')
            # This is a replay of an already committed canonical source, never
            # a transition from working/unknown into canonical knowledge.
            if (value(node, 'graph_layer') != 'canonical'
                    or value(node, 'maturity_status') != 'canonical_eligible'
                    or value(node, 'superseded_by') is not None):
                reasons.append('durable_canonical_partition_unproven')
            author, session = value(node, 'created_by_agent'), value(node, 'source_session_id')
            if (type(author) is not str or not author.strip() or type(session) is not str
                    or not session.strip() or session != source.get('source_session_id')):
                reasons.append('durable_commit_identity_missing')
            committed = source.get('committed_at')
            try:
                if type(committed) is not str or not committed:
                    raise ValueError('missing timestamp')
                datetime.fromisoformat(committed.replace('Z', '+00:00'))
            except ValueError:
                reasons.append('durable_commit_time_missing')
            ref = value(node, 'source_artifact_ref')
            resolution = resolve_cognitive_source_ref(ref)
            # Connectivity alone cannot supply this durable evidence binding.
            # Other source kinds need current-source/association reconciliation;
            # do not reinterpret them as a technical report to pass the gate.
            refs = source.get('evidence_refs')
            if type(refs) is str:
                refs = json.loads(refs)
            if (resolution.resolution_status != CognitiveRefResolutionStatus.FINAL_REPORT_ALLOWLISTED.value
                    or type(ref) is not str or not ref.startswith('final_report:')
                    or not ref[len('final_report:'):].strip() or ref not in refs):
                reasons.append('durable_technical_source_binding_unproven')
            if key in connected:
                reasons.append('durable_association_reconciliation_required')
            if outcomes[key].outcome != 'allowlisted' or outcomes[key].reasons:
                reasons.append('existing_connectivity_policy_not_satisfied')
            # A Learning still has its separate applicability/completeness
            # predicate. A technical-root exception cannot establish that fact.
            if key[0] == 'Learning':
                reasons.append('learning_applicability_reconciliation_required')
        results.append(CognitiveReplayQualification(*key,
            'pending' if reasons else 'durable_replay_reconciled', tuple(reasons), fingerprint))
    return tuple(results)
