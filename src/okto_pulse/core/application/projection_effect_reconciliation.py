"""Compare an ordered property trace to exact portable node observations.

The caller authenticates snapshots, ACKs and execution order. This proves only
the declared property composition, never source authority or history eligibility.
"""

from datetime import UTC, datetime, timedelta
import json
import math

from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalFingerprintAccumulator, LogicalNode, LogicalSchemaIndex,
    LogicalTimestamp, LogicalVector, canonical_json, encode_value,
)
from okto_pulse.core.ports.projection_history import ProjectionNodeChange, ProjectionNodeFingerprint


def _timestamp_observation(value):
    try:
        return (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value.micros)).isoformat(
            timespec='microseconds').replace('+00:00', 'Z')
    except OverflowError:
        return {'type': 'timestamp', 'micros': str(value.micros)}


def _observed(value):
    if value is LOGICAL_NULL:
        return None
    if isinstance(value, LogicalTimestamp):
        return _timestamp_observation(value)
    if isinstance(value, LogicalVector):
        return [_observed(part) for part in value.components]
    if type(value) is float and not math.isfinite(value):
        # Normalized NaNs erase payload bits. They cannot prove a changed value.
        raise ValueError('projection_effect_lossy_observation')
    return value


def _logical(value, definition, schema):
    if value is None:
        return LOGICAL_NULL
    kind = definition.type
    scalar = {'string': str, 'bool': bool, 'int64': int, 'float64': float}
    if kind in scalar and type(value) is scalar[kind]:
        if kind == 'float64' and not math.isfinite(value):
            raise ValueError('projection_effect_lossy_observation')
        return value
    if kind == 'timestamp_us':
        if type(value) is dict and set(value) == {'type', 'micros'} and value['type'] == 'timestamp':
            timestamp = LogicalTimestamp(int(value['micros']))
        elif type(value) is str:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                raise ValueError('projection_effect_timestamp_timezone_missing')
            delta = parsed - datetime(1970, 1, 1, tzinfo=UTC)
            timestamp = LogicalTimestamp((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
        else:
            raise ValueError('projection_effect_value_type_invalid')
        if canonical_json(_timestamp_observation(timestamp)) != canonical_json(value):
            raise ValueError('projection_effect_timestamp_not_canonical')
        return timestamp
    if kind == 'vector' and type(value) is list and all(type(part) is float and math.isfinite(part) for part in value):
        space = schema.vector_space(definition.vector_space)
        return LogicalVector(space.name, space.storage_dtype, tuple(value))
    raise ValueError('projection_effect_value_type_invalid')


def reconcile_node_effects(*, schema, board_id, before, after, effects):
    from okto_pulse.core.ports.projection_effects import ProjectionPropertyEffects

    if (type(effects) is not tuple or not effects or len(effects) > 100_000
            or any(type(item) is not ProjectionPropertyEffects for item in effects)):
        raise ValueError('projection_effect_sequence_invalid')
    if schema.scope != 'board' or (before.type_name, before.key) != (after.type_name, after.key):
        raise ValueError('projection_effect_node_identity_changed')
    if (any(item.board_id != board_id for item in effects)
            or len({item.session_id for item in effects}) != len(effects)):
        raise ValueError('projection_effect_sequence_scope_or_replay')
    if sum(len(canonical_json(item.to_payload())) for item in effects) > 64 * 1024 * 1024:
        raise ValueError('projection_effect_sequence_limit')
    index = LogicalSchemaIndex.build(schema)
    index.validate_node(before)
    index.validate_node(after)
    properties = dict(before.properties)
    definition = schema.node_type(before.type_name)
    matched = 0
    for envelope in effects:
        for effect in envelope.nodes:
            if (effect.node_type, effect.node_id) != (before.type_name, before.key):
                continue
            matched += 1
            old, new = json.loads(effect.before_json), json.loads(effect.after_json)
            for name in old:
                # The legacy observation cannot distinguish absence from NULL.
                # Refuse that ambiguity rather than filling an invented NULL.
                if name not in properties:
                    raise ValueError('projection_effect_prior_property_absent')
                if canonical_json(_observed(properties[name])) != canonical_json(old[name]):
                    raise ValueError('projection_effect_before_mismatch')
                properties[name] = _logical(new[name], definition.property_def(name), schema)
            index.validate_node(LogicalNode(before.type_name, before.key, properties))
    if not matched:
        raise ValueError('projection_effect_node_trace_missing')
    encoded = lambda values: {name: encode_value(value) for name, value in values.items()}
    if encoded(properties) != encoded(after.properties):
        raise ValueError('projection_effect_final_node_mismatch')

    def fingerprint(node):
        accumulator = LogicalFingerprintAccumulator.for_schema(schema)
        accumulator.add_node(node)
        return ProjectionNodeFingerprint(node.type_name, node.key, accumulator.digest())

    return ProjectionNodeChange(fingerprint(before), fingerprint(after))
