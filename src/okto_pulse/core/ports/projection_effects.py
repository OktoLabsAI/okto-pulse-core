"""Closed, bounded observations of property effects on pre-existing graph nodes.

These records attest what a consolidation wrote. They do not authorize a write,
classify historical meaning, or turn a reused identity into a compensating delete.
"""

from dataclasses import dataclass
import hashlib
import json

from okto_pulse.core.kg.schema_contract import NODE_TYPES, STABLE_NODE_PROPERTIES

_LIMIT = 64 * 1024 * 1024
_PROPERTIES = frozenset(STABLE_NODE_PROPERTIES) | {'embedding'}


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(',', ':'), allow_nan=False)


def _identity(value):
    if type(value) is not str or not value or len(value) > 1024:
        raise ValueError('projection_effect_identity_invalid')


@dataclass(frozen=True, slots=True)
class ProjectionPropertyEffect:
    node_type: str
    node_id: str
    before_json: str
    after_json: str

    def __post_init__(self):
        _identity(self.node_id)
        if self.node_type not in NODE_TYPES:
            raise ValueError('projection_effect_node_type_invalid')
        states = []
        for encoded in (self.before_json, self.after_json):
            if type(encoded) is not str or len(encoded) > _LIMIT:
                raise ValueError('projection_effect_payload_limit')
            state = json.loads(encoded)
            if (type(state) is not dict or not state or not set(state) <= _PROPERTIES
                    or 'id' in state or _json(state) != encoded):
                raise ValueError('projection_effect_properties_invalid')
            states.append(state)
        if set(states[0]) != set(states[1]) or self.before_json == self.after_json:
            raise ValueError('projection_effect_delta_invalid')

    @classmethod
    def from_values(cls, node_type, node_id, before, after):
        return cls(node_type, node_id, _json(before), _json(after))

    def to_payload(self):
        return {'node_type': self.node_type, 'node_id': self.node_id,
            'before': json.loads(self.before_json), 'after': json.loads(self.after_json)}


@dataclass(frozen=True, slots=True)
class ProjectionPropertyEffects:
    board_id: str
    session_id: str
    nodes: tuple[ProjectionPropertyEffect, ...]

    def __post_init__(self):
        _identity(self.board_id)
        _identity(self.session_id)
        if (type(self.nodes) is not tuple or not 1 <= len(self.nodes) <= 100_000
                or any(type(node) is not ProjectionPropertyEffect for node in self.nodes)):
            raise ValueError('projection_effect_nodes_invalid')
        keys = tuple((node.node_type, node.node_id) for node in self.nodes)
        if keys != tuple(sorted(set(keys))):
            raise ValueError('projection_effect_node_identity_ambiguous')
        if len(_json(self._body())) > _LIMIT:
            raise ValueError('projection_effect_payload_limit')

    def _body(self):
        return {'format': 'projection-property-effects/v1', 'board_id': self.board_id,
            'session_id': self.session_id, 'nodes': [node.to_payload() for node in self.nodes]}

    def to_payload(self):
        body = self._body()
        return {**body, 'sha256': hashlib.sha256(_json(body).encode('ascii')).hexdigest()}

    @classmethod
    def from_payload(cls, value):
        if (type(value) is not dict or set(value) != {'format', 'board_id', 'session_id', 'nodes', 'sha256'}
                or value['format'] != 'projection-property-effects/v1' or type(value['nodes']) is not list
                or len(_json(value)) > _LIMIT):
            raise ValueError('projection_effect_payload_invalid')
        nodes = []
        for node in value['nodes']:
            if type(node) is not dict or set(node) != {'node_type', 'node_id', 'before', 'after'}:
                raise ValueError('projection_effect_payload_invalid')
            nodes.append(ProjectionPropertyEffect.from_values(node['node_type'], node['node_id'], node['before'], node['after']))
        result = cls(value['board_id'], value['session_id'], tuple(nodes))
        if _json(result.to_payload()) != _json(value):
            raise ValueError('projection_effect_payload_changed')
        return result


def validated_projection_effect_extension(payload, *, board_id, session_id):
    """Validate the optional extension of an existing audit/outbox envelope."""
    if type(payload) is not dict or 'projection_property_effects' not in payload:
        return {}
    effects = ProjectionPropertyEffects.from_payload(payload['projection_property_effects'])
    if (effects.board_id, effects.session_id) != (board_id, session_id):
        raise ValueError('projection_effect_scope_mismatch')
    return {'projection_property_effects': effects.to_payload()}


def reconcile_projection_node_effects(*, schema, board_id, before, after, effects):
    """Prove exact property composition after the caller authenticates order/ACKs.

    Returns the observed fingerprints only. It does not approve history, edges,
    admission, or the provenance of the supplied records and effect envelopes.
    """
    from okto_pulse.core.application.projection_effect_reconciliation import reconcile_node_effects
    return reconcile_node_effects(schema=schema, board_id=board_id, before=before, after=after, effects=effects)
