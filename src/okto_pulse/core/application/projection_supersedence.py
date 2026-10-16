"""Recognize the existing NC8 Bug history write, without changing its semantics."""

import json

from okto_pulse.core.kg.cognitive_source_ref_resolver import typed_bug_source_reference_aliases
from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalFingerprintAccumulator, LogicalSchemaIndex, LogicalTimestamp,
    canonical_bytes, encode_value,
)
from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id, normalize_text
from okto_pulse.core.ports.projection_effects import (
    ProjectionSupersedenceEffect, reconcile_projection_node_effects,
)
from okto_pulse.core.ports.projection_history import ProjectionEdgeFingerprint, ProjectionNodeFingerprint

_MARK = {'superseded_by', 'superseded_at', 'revocation_reason'}
_USAGE = {'relevance_score', 'last_recomputed_at'}
_REASON = 'semantic change on NC-8 reuse (MKG-D trail)'


def observe_bug(*, schema, board_id, session_id, before, after, successor, relation, effects):
    if (schema.scope != 'board' or type(board_id) is not str or not board_id
            or type(session_id) is not str or not session_id):
        raise ValueError('projection_supersedence_scope_invalid')
    index = LogicalSchemaIndex.build(schema)
    for node in (before, after, successor):
        index.validate_node(node)
    index.validate_relation(relation)
    if (before.type_name != 'Bug' or after.type_name != 'Bug' or successor.type_name != 'Bug'
            or before.key != after.key or successor.key == before.key
            or (relation.layout_name, relation.source_type, relation.source_key,
                relation.target_type, relation.target_key) != ('supersedes', 'Bug', successor.key, 'Bug', before.key)):
        raise ValueError('projection_supersedence_identity_invalid')
    prior, current, following = before.properties, after.properties, successor.properties
    source_ref = prior.get('source_artifact_ref')
    if type(source_ref) is not str:
        raise ValueError('projection_supersedence_source_invalid')
    aliases = typed_bug_source_reference_aliases(source_ref)
    if (current.get('source_artifact_ref') != source_ref
            or following.get('source_artifact_ref') not in aliases
            or prior.get('human_curated') is True
            or prior.get('superseded_by') not in (LOGICAL_NULL, '')
            or current.get('superseded_by') != successor.key
            or current.get('revocation_reason') != _REASON
            or following.get('superseded_by') not in (LOGICAL_NULL, '')
            or following.get('source_session_id') != session_id):
        raise ValueError('projection_supersedence_source_or_state_changed')
    generation = prior.get('generation')
    generation = 0 if generation is LOGICAL_NULL else generation
    if (type(generation) is not int or generation < 0
            or type(following.get('generation')) is not int
            or following['generation'] != generation + 1
            or successor.key != mint_node_id(board_id, 'Bug', derive_natural_key(
                following['source_artifact_ref'], 'Bug', following.get('title')), generation + 1)
            or type(prior.get('title')) is not str or type(following.get('title')) is not str
            or normalize_text(prior['title']) == normalize_text(following['title'])):
        raise ValueError('projection_supersedence_generation_or_title_changed')
    changed = {name for name in set(prior) | set(current)
        if name not in prior or name not in current
        or canonical_bytes(encode_value(prior[name])) != canonical_bytes(encode_value(current[name]))}
    if changed - (_MARK | _USAGE) or not _MARK <= changed:
        raise ValueError('projection_supersedence_predecessor_content_changed')
    predecessor = reconcile_projection_node_effects(schema=schema, board_id=board_id,
        before=before, after=after, effects=effects)
    marked = 0
    for effect in effects:
        for node in effect.nodes:
            if (node.node_type, node.node_id) != ('Bug', before.key):
                continue
            fields = set(json.loads(node.after_json))
            if fields - (_MARK | _USAGE):
                raise ValueError('projection_supersedence_trace_unowned')
            if fields & _MARK:
                if not _MARK <= fields or effect.session_id != session_id:
                    raise ValueError('projection_supersedence_trace_unowned')
                marked += 1
    if marked != 1:
        raise ValueError('projection_supersedence_trace_ambiguous')
    attrs = relation.properties
    expected = {'confidence': 1.0, 'created_by': session_id,
        'created_by_session_id': session_id, 'fallback_reason': '', 'layer': 'cognitive', 'rule_id': ''}
    if (set(attrs) != set(expected) | {'created_at'}
            or any(attrs[name] != value for name, value in expected.items())
            or any(type(stamp) is not LogicalTimestamp for stamp in
                (following.get('created_at'), current.get('superseded_at'), attrs['created_at']))
            or not following['created_at'].micros <= current['superseded_at'].micros <= attrs['created_at'].micros):
        raise ValueError('projection_supersedence_edge_provenance_changed')
    node_hash = LogicalFingerprintAccumulator.for_schema(schema)
    node_hash.add_node(successor)
    edge_hash = LogicalFingerprintAccumulator.for_schema(schema)
    edge_hash.add_relation(relation)
    return ProjectionSupersedenceEffect(board_id, session_id, predecessor,
        ProjectionNodeFingerprint('Bug', successor.key, node_hash.digest()),
        ProjectionEdgeFingerprint('supersedes', 'Bug', successor.key, 'Bug', before.key, edge_hash.digest(), 1))
