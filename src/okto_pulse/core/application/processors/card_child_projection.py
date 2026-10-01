"""Project explicit child assignments; observed support never certifies delivery."""
from collections import Counter
import hashlib
import json

from okto_pulse.core.domain.card_scenario_references import reference_ids
from okto_pulse.core.ports.card_projection import CARD_CHILD_FAMILIES
from okto_pulse.core.application.processors.deterministic_kg import (
    EmittedEdge, RelationalProjectionActiveEdgeRef, RelationalProjectionActiveSetIntent,
)


def prepare_card_child_projection(*, card, parent, result):
    roots = [node for node in result.nodes if node.source_artifact_ref == f'card:{card.id}']
    if len(roots) != 1 or roots[0].node_type not in {'Entity', 'Bug'}:
        raise ValueError('card_child_owner_unresolved')
    additions, intents, fingerprint = [], [], []
    for family in CARD_CHILD_FAMILIES:
        if parent is not None and not hasattr(parent, family.field):
            raise ValueError('card_child_source_incomplete:' + family.field)
        values = getattr(parent, family.field) if parent is not None else []
        if values is None:
            values = []  # An explicitly observed SQL NULL is an unset collection.
        if not isinstance(values, list) or any(not isinstance(item, (dict, str)) for item in values):
            raise ValueError('card_child_source_invalid:' + family.field)
        counts = Counter(item.get('id') for item in values if isinstance(item, dict)
                         and isinstance(item.get('id'), str))
        edges = []
        for item in values:
            if not isinstance(item, dict):
                continue  # Legacy prose cannot declare a Card assignment.
            linked = reference_ids(item.get('linked_task_ids'), family.field + '.linked_task_ids')
            if card.id not in linked or item.get('status') in {'cancelled', 'superseded', 'deprecated', 'revoked'}:
                continue
            identity = item.get('id')
            if (type(identity) is not str or not identity or identity.strip() != identity
                    or ':' in identity or counts[identity] != 1):
                raise ValueError('card_child_identity_invalid:' + family.field)
            target = f'kgref:{family.target_type}:spec:{parent.id}:{family.section}:{identity}'
            edge_id = 'card_child_' + hashlib.sha256(f'{card.id}\0{target}'.encode()).hexdigest()
            additions.append(EmittedEdge(edge_id, 'supports', roots[0].candidate_id, target, 1.0, family.rule))
            edges.append(RelationalProjectionActiveEdgeRef(edge_id, 'supports', roots[0].candidate_id, target, family.rule))
        ordered = tuple(sorted(edges, key=lambda edge: edge.candidate_id))
        intents.append(RelationalProjectionActiveSetIntent('card', card.id, family.namespace, (), ordered))
        fingerprint.append((family.namespace, [edge.to_candidate_id for edge in ordered]))
    # Validate the entire source before attaching any new active set.
    result.edges.extend(additions)
    result.relational_projection_active_set_intents += tuple(intents)
    result.content_hash = hashlib.sha256(json.dumps(
        ['card-children/v1', result.content_hash, fingerprint], separators=(',', ':'),
        ensure_ascii=True).encode()).hexdigest()
    return result
