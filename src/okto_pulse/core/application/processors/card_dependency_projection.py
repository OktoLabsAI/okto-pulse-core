"""Project a complete dependency source without synthesizing prerequisite nodes."""
import hashlib
import json

from okto_pulse.core.ports.card_projection import CARD_DEPENDENCY_NAMESPACE, card_dependency_rule
from okto_pulse.core.application.processors.deterministic_kg import (
    EmittedEdge, RelationalProjectionActiveEdgeRef, RelationalProjectionActiveSetIntent,
)


def prepare_card_dependency_projection(*, board_id, card, inputs, result):
    roots = [node for node in result.nodes if node.source_artifact_ref == f'card:{card.id}']
    if len(roots) != 1 or roots[0].node_type not in {'Entity', 'Bug'}:
        raise ValueError('card_dependency_owner_unresolved')
    dependencies = getattr(inputs, 'card_dependencies', None)
    if not isinstance(dependencies, tuple):
        raise ValueError('card_dependency_source_incomplete')
    additions, refs, identities, targets = [], [], set(), set()
    for dependency in dependencies:
        if (dependency.board_id != board_id or dependency.dependent_card_id != card.id
                or dependency.prerequisite_card_id == card.id
                or dependency.prerequisite_card_type not in {'normal', 'test', 'bug'}
                or dependency.dependency_id in identities or dependency.prerequisite_card_id in targets
                or type(dependency.prerequisite_card_id) is not str or not dependency.prerequisite_card_id
                or dependency.prerequisite_card_id.strip() != dependency.prerequisite_card_id
                or ':' in dependency.prerequisite_card_id):
            raise ValueError('card_dependency_source_invalid')
        rule = card_dependency_rule(dependency.dependency_id)
        kind = 'Bug' if dependency.prerequisite_card_type == 'bug' else 'Entity'
        source = f'kgref:{kind}:card:{dependency.prerequisite_card_id}'
        identity = 'card_dependency_' + hashlib.sha256(rule.encode()).hexdigest()
        additions.append(EmittedEdge(identity, 'precedes', source, roots[0].candidate_id, 1.0, rule))
        refs.append(RelationalProjectionActiveEdgeRef(identity, 'precedes', source, roots[0].candidate_id, rule))
        identities.add(dependency.dependency_id)
        targets.add(dependency.prerequisite_card_id)
    ordered = tuple(sorted(refs, key=lambda edge: edge.candidate_id))
    result.edges.extend(additions)
    result.relational_projection_active_set_intents += (
        RelationalProjectionActiveSetIntent('card', card.id, CARD_DEPENDENCY_NAMESPACE, (), ordered),)
    result.content_hash = hashlib.sha256(json.dumps(
        ['card-dependencies/v1', result.content_hash,
         [(edge.from_candidate_id, edge.rule_id) for edge in ordered]],
        separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    return result
