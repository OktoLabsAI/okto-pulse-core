"""Observe durable cognitive payload parity using portable property types."""

from datetime import UTC, datetime
import json
import math
import re

from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalSchemaIndex, LogicalTimestamp, LogicalVector, encode_value,
)
from okto_pulse.core.ports.cognitive_projection import CognitiveProjectionParity
from okto_pulse.core.ports.kg_cognitive_source import (
    COGNITIVE_SOURCE_VOLATILE_USAGE_FIELDS, canonical_cognitive_source_fingerprint,
    latest_cognitive_source_records,
)


def _value(value, definition, schema):
    if value is None:
        return LOGICAL_NULL
    scalar = {'string': str, 'bool': bool, 'int64': int, 'float64': float}
    if definition.type in scalar and type(value) is scalar[definition.type]:
        if type(value) is float and not math.isfinite(value):
            raise ValueError('cognitive_projection_nonfinite')
        return value
    if definition.type == 'timestamp_us' and type(value) is str:
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?', value) is None:
            raise ValueError('cognitive_projection_timestamp_representation_unsupported')
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        # The existing cognitive writer stamps UTC with strftime, without an
        # offset. This recognizes that persisted contract, not local wall time.
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=UTC)
        delta = stamp - datetime(1970, 1, 1, tzinfo=UTC)
        return LogicalTimestamp((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
    if definition.type == 'vector' and type(value) is list:
        space = schema.vector_space(definition.vector_space)
        if space.storage_dtype != 'float64' or any(type(part) is not float or not math.isfinite(part) for part in value):
            raise ValueError('cognitive_projection_vector_representation_unsupported')
        return LogicalVector(space.name, space.storage_dtype, tuple(value))
    raise ValueError('cognitive_projection_property_type_invalid')


def compare(*, schema, board_id, record, node):
    if type(record) is not dict or len(json.dumps(record, allow_nan=False)) > 64 * 1024 * 1024:
        raise ValueError('cognitive_projection_source_invalid')
    # BoardSourceReader deliberately keeps SQL JSON cells as text in the
    # authenticated snapshot. The durable-source policy accepts both forms.
    # Detach/decode locally; never rewrite the source artifact or its digest.
    record = {**record, **{name: json.loads(record[name]) if type(record.get(name)) is str else record.get(name)
        for name in ('payload', 'evidence_refs')}}
    if (schema.scope != 'board' or record.get('board_id') != board_id
            or type(board_id) is not str or not board_id
            or record.get('node_type') not in {'Decision', 'Learning', 'Alternative', 'Assumption'}
            or type(record.get('node_id')) is not str or not record['node_id']
            or type(record.get('generation')) is not int or record['generation'] < 0
            or type(record.get('source_revision', 0)) is not int or record.get('source_revision', 0) < 0
            or type(record.get('payload')) is not dict
            or type(record.get('evidence_refs')) not in (list, tuple)
            or any(type(ref) is not str for ref in record['evidence_refs'])):
        raise ValueError('cognitive_projection_source_invalid')
    latest_cognitive_source_records((record,))  # Existing fingerprint verification.
    fingerprint = canonical_cognitive_source_fingerprint(board_id=board_id,
        node_type=record['node_type'], node_id=record['node_id'], generation=record['generation'],
        payload=record['payload'], evidence_refs=record['evidence_refs'])

    def result(state, differences=(), usage=()):
        return CognitiveProjectionParity(record['node_type'], record['node_id'], record['generation'],
            record.get('source_revision', 0), fingerprint, state, tuple(sorted(differences)), tuple(sorted(usage)))

    if node is None:
        return result('missing_node')
    if (node.type_name, node.key) != (record['node_type'], record['node_id']):
        raise ValueError('cognitive_projection_node_identity_invalid')
    LogicalSchemaIndex.build(schema).validate_node(node)
    definition = schema.node_type(node.type_name)
    expected = dict(record['payload'])
    if 'id' in expected and expected['id'] != node.key:
        raise ValueError('cognitive_projection_payload_identity_invalid')
    expected['id'] = node.key
    # The pre-existing durable replay uses this exact fallback for old payloads.
    expected.setdefault('source_session_id', record.get('source_session_id') or '')
    if set(expected) - set(definition.property_names()):
        raise ValueError('cognitive_projection_property_unsupported')
    differences, usage = set(), set()
    if node.properties.get('generation') != record['generation']:
        differences.add('generation')
    for name in set(expected) | set(node.properties):
        actual = node.properties.get(name, LOGICAL_NULL)
        wanted = _value(expected[name], definition.property_def(name), schema) if name in expected else LOGICAL_NULL
        if encode_value(actual) != encode_value(wanted):
            (usage if name in COGNITIVE_SOURCE_VOLATILE_USAGE_FIELDS else differences).add(name)
    return result('different' if differences else 'matched', differences, usage)
