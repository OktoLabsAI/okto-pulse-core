"""G5: infer association from explicit origin assignments, never root cause."""
from collections import Counter
import hashlib
import json

from okto_pulse.core.domain.card_scenario_references import reference_ids
from okto_pulse.core.ports.card_projection import BUG_ORIGIN_PROXY_FAMILIES
from okto_pulse.core.application.processors.deterministic_kg import (
    EmittedEdge, RelationalProjectionActiveEdgeRef, RelationalProjectionActiveSetIntent,
)


async def prepare_bug_origin_proxy(context, *, board_id, card, result, persistence):
    origin, parent = None, None
    origin_id = getattr(card, 'origin_task_id', None) if card.card_type == 'bug' else None
    if origin_id:
        origin = await persistence.load_artifact(context, artifact_type='card', artifact_id=origin_id)
        if (origin is None or origin.id != origin_id or origin.board_id != board_id
                or origin.id == card.id):
            raise ValueError('bug_origin_proxy_source_unavailable')
        if origin.spec_id:
            parent = await persistence.load_artifact(context, artifact_type='spec', artifact_id=origin.spec_id)
            if parent is None or parent.id != origin.spec_id or parent.board_id != board_id:
                raise ValueError('bug_origin_proxy_parent_unavailable')
    roots = [node for node in result.nodes if node.source_artifact_ref == f'card:{card.id}']
    if len(roots) != 1 or (origin_id and roots[0].node_type != 'Bug'):
        raise ValueError('bug_origin_proxy_owner_invalid')
    additions, intents = [], []
    for family in BUG_ORIGIN_PROXY_FAMILIES:
        if parent is not None and not hasattr(parent, family.field):
            raise ValueError('bug_origin_proxy_source_incomplete:' + family.field)
        values = getattr(parent, family.field) if parent is not None else []
        if values is None:
            values = []  # Observed SQL NULL is unset; a malformed value is not.
        if not isinstance(values, list) or any(not isinstance(item, (str, dict)) for item in values):
            raise ValueError('bug_origin_proxy_source_invalid:' + family.field)
        counts = Counter(item.get('id') for item in values if isinstance(item, dict)
            and isinstance(item.get('id'), str))
        edges = []
        for item in values:
            if not isinstance(item, dict):
                continue
            links = reference_ids(item.get('linked_task_ids'), family.field + '.linked_task_ids')
            if origin_id not in links or item.get('status') in {'cancelled', 'superseded', 'deprecated', 'revoked'}:
                continue
            identity = item.get('id')
            if (type(identity) is not str or not identity or identity.strip() != identity
                    or ':' in identity or counts[identity] != 1):
                raise ValueError('bug_origin_proxy_target_invalid:' + family.field)
            target = f'kgref:{family.target_type}:spec:{parent.id}:{family.section}:{identity}'
            rule = family.origin_rule(origin_id)
            edge_id = 'bug_origin_proxy_' + hashlib.sha256(f'{card.id}\0{rule}\0{target}'.encode()).hexdigest()
            additions.append(EmittedEdge(edge_id, 'violates', roots[0].candidate_id, target, 0.8, rule,
                fallback_reason='inferred_origin_proxy:card:' + origin_id))
            edges.append(RelationalProjectionActiveEdgeRef(edge_id, 'violates', roots[0].candidate_id, target, rule))
        intents.append(RelationalProjectionActiveSetIntent('card', card.id, family.namespace, (),
            tuple(sorted(edges, key=lambda edge: edge.candidate_id))))
    result.edges.extend(additions)
    result.relational_projection_active_set_intents += tuple(intents)
    # This optional proxy is now completely observed; absence is not a request
    # for an agent to invent a cause or fabricate links to the whole Spec.
    result.missing_link_candidates[:] = [candidate for candidate in result.missing_link_candidates
        if candidate.edge_type != 'violates' or candidate.reason != 'no_origin_task']
    result.content_hash = hashlib.sha256(json.dumps(['bug-origin-proxy/v1', result.content_hash,
        origin_id, [(edge.to_candidate_id, edge.rule_id) for edge in additions]],
        separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    return result
