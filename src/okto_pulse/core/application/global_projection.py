"""Shared pure materialization of the established Global recovery seed."""

from dataclasses import asdict
import hashlib
import json
import math

from okto_pulse.core.kg.global_discovery.layer_parity import resolve_expected_digest_layer
from okto_pulse.core.kg.interfaces.global_discovery_recovery import GlobalDiscoveryBoardSeed, GlobalDiscoveryDigestSeed
from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id
from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES
from okto_pulse.core.ports.global_projection import GlobalProjectionSource


def build_seed(*, source_input, expected_sources, sources, summary_embedding):
    from okto_pulse.core.ports.global_discovery_recovery_control import GlobalDiscoveryRecoveryBoardSeedInput

    if (type(source_input) is not GlobalDiscoveryRecoveryBoardSeedInput
            or type(expected_sources) is not tuple or type(sources) is not tuple
            or max(len(expected_sources), len(sources)) > 100_000):
        raise ValueError('global_projection_inventory_invalid')
    expected = {}
    for identity in expected_sources:
        if (type(identity) is not tuple or len(identity) != 2 or type(identity[0]) is not str
                or not identity[0] or identity[1] not in VECTOR_INDEX_TYPES or identity[0] in expected):
            raise ValueError('global_projection_source_identity_invalid')
        expected[identity[0]] = identity[1]

    def vector(values):
        if type(values) is not tuple or not values or len(values) > 65_536 or any(
                type(value) is not float or not math.isfinite(value) for value in values):
            raise ValueError('global_projection_vector_invalid')
        return values

    summary_embedding = vector(summary_embedding)
    budget = len(json.dumps({'input': asdict(source_input), 'expected': expected_sources,
        'summary_embedding': summary_embedding}, ensure_ascii=False, allow_nan=False).encode('utf-8'))
    seen, digests = set(), []
    overlay = dict(source_input.overlay_exclusions)
    for source in sources:
        if type(source) is not GlobalProjectionSource:
            raise TypeError('global_projection_source_required')
        if (any(type(value) is not str for value in (source.node_id, source.node_type, source.title,
                source.source_artifact_ref, source.graph_layer))
                or source.node_id not in expected or expected[source.node_id] != source.node_type
                or source.node_id in seen or type(source.canonical_bug_count) is not int or source.canonical_bug_count < 0
                or type(source.relates_to_endpoints) is not tuple
                or any(type(endpoint) is not tuple or len(endpoint) != 2 or type(endpoint[0]) is not str
                    or (endpoint[1] is not None and type(endpoint[1]) is not str)
                    for endpoint in source.relates_to_endpoints)):
            raise ValueError('global_projection_source_identity_invalid')
        vector(source.embedding)
        seen.add(source.node_id)
        budget += len(json.dumps(asdict(source), ensure_ascii=False, allow_nan=False).encode('utf-8'))
        if budget > 64 * 1024 * 1024:
            raise ValueError('global_projection_inventory_limit')
        graph_layer, _ = resolve_expected_digest_layer(node_type=source.node_type,
            raw_graph_layer=source.graph_layer, source_artifact_ref=source.source_artifact_ref,
            canonical_bug_count=source.canonical_bug_count, relates_to_endpoints=source.relates_to_endpoints,
            overlay_exclusion_reason=overlay.get(normalize_cognitive_artifact_id(source.source_artifact_ref)))
        digests.append(GlobalDiscoveryDigestSeed(source.node_id, source.title, source.title[:280], source.node_type,
            graph_layer, source.source_artifact_ref, source.embedding))
    if seen != set(expected):
        raise ValueError('global_projection_source_inventory_incomplete')
    if budget > 64 * 1024 * 1024:
        raise ValueError('global_projection_inventory_limit')
    digests.sort(key=lambda row: row.original_node_id)
    encoded = json.dumps({'source_types': sorted(expected.items()), 'digests': [row.to_dict() for row in digests]},
        sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    return GlobalDiscoveryBoardSeed(source_input.board_id, source_input.board_name, source_input.board_summary,
        summary_embedding, tuple(digests), hashlib.sha256(encoded).hexdigest())
