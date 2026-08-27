"""Canonical encoding primitives for ``okto-pulse-logical-graph/1``.

The codec and the fingerprint both have to agree, byte for byte, on what a
value, a record and a schema *are*.  They agree by sharing this module rather
than by each implementing the same rules, because two independent
canonicalizations that drift produce an artifact whose own manifest disagrees
with its own fingerprint.

Values are tagged explicitly.  An untagged encoding would have to recover a
type from JSON's own type lattice, which cannot tell an int from a timestamp,
cannot represent a float exactly, and has no way at all to say ``absent``.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from typing import Any, Final

from .errors import ArtifactMalformedError, LogicalValueError
from .model import (
    INT64_MAX,
    INT64_MIN,
    LOGICAL_NULL,
    LogicalNode,
    LogicalNodeType,
    LogicalNull,
    LogicalPropertyDef,
    LogicalRelation,
    LogicalRelationLayout,
    LogicalSchema,
    LogicalTimestamp,
    LogicalValue,
    LogicalVector,
    LogicalVectorSpace,
)


TAG_NULL: Final[str] = "null"
TAG_BOOL: Final[str] = "bool"
TAG_INT64: Final[str] = "int64"
TAG_FLOAT64: Final[str] = "float64"
TAG_STRING: Final[str] = "string"
TAG_TIMESTAMP_US: Final[str] = "timestamp_us"
TAG_VECTOR: Final[str] = "vector"

VALUE_TAGS: Final[tuple[str, ...]] = (
    TAG_NULL,
    TAG_BOOL,
    TAG_INT64,
    TAG_FLOAT64,
    TAG_STRING,
    TAG_TIMESTAMP_US,
    TAG_VECTOR,
)


def canonical_json(payload: Any) -> str:
    """Render one canonical JSON document.

    Sorted keys and separator-free formatting make the same logical content
    produce the same bytes on every machine and every run, which is what lets a
    checksum and a fingerprint mean anything.
    """

    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def canonical_bytes(payload: Any) -> bytes:
    return canonical_json(payload).encode("utf-8")


class _DuplicateKey(ValueError):
    """Raised through the JSON parser when one object repeats a key."""


class _NonFinite(ValueError):
    """Raised through the JSON parser for NaN, Infinity and -Infinity."""


def _reject_constant(name: str) -> None:
    # Python's json accepts these three by default. They are not JSON, they
    # have no portable logical meaning, and a graph carrying one would compare
    # unequal to itself.
    raise _NonFinite(name)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise _DuplicateKey(key)
        seen[key] = value
    return seen


def loads_canonical(line: str) -> Mapping[str, Any]:
    """Parse one record line, refusing anything that is not exactly canonical.

    Two rules, and each closes a hole a plain ``json.loads`` leaves open.

    Duplicate keys are refused because the parser silently keeps the last one,
    so a record could carry two values for the same property and decode to
    whichever the writer happened to put second.

    The line must then re-serialize to itself byte for byte. Reordered keys,
    added whitespace, a non-canonical float spelling and a differently escaped
    string all parse to the same object, so without this a reader would accept
    bytes no encoder of this format could have produced -- and the stream
    checksum, which is a function of those bytes, would stop meaning anything.
    """

    try:
        payload = json.loads(
            line,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except _DuplicateKey as duplicate:
        raise ArtifactMalformedError(
            "record carries a duplicate key", detail=str(duplicate)
        ) from duplicate
    except _NonFinite as constant:
        raise ArtifactMalformedError(
            "record carries a non-finite JSON constant", detail=str(constant)
        ) from constant
    except ValueError as failure:
        raise ArtifactMalformedError(
            "record is not valid JSON", detail=line[:60]
        ) from failure
    if not isinstance(payload, Mapping):
        raise ArtifactMalformedError(
            "record must be a JSON object", detail=type(payload).__name__
        )
    try:
        rendered = canonical_json(payload)
    except ValueError as failure:
        # A literal like 1e999 parses to inf without going through
        # parse_constant, and the canonicalizer refuses to render it. That
        # refusal is a ValueError, and it must not leave this boundary untyped.
        raise ArtifactMalformedError(
            "record carries a non-finite number", detail=line[:60]
        ) from failure
    if rendered != line:
        raise ArtifactMalformedError(
            "record is not in canonical form", detail=line[:60]
        )
    return payload


def encode_value(value: LogicalValue) -> list[Any]:
    """Encode one logical value as its tagged, exactly-recoverable form."""

    if isinstance(value, LogicalNull):
        return [TAG_NULL]
    # bool before int: bool is a subclass of int, so the int branch would
    # otherwise swallow True/False and re-decode them as 1/0.
    if value is True or value is False:
        return [TAG_BOOL, value]
    if isinstance(value, LogicalTimestamp):
        return [TAG_TIMESTAMP_US, str(value.micros)]
    if isinstance(value, LogicalVector):
        return [
            TAG_VECTOR,
            {
                "components": [_encode_float(part) for part in value.components],
                "dtype": value.dtype,
                "space_name": value.space_name,
            },
        ]
    if type(value) is int:
        if not INT64_MIN <= value <= INT64_MAX:
            raise LogicalValueError(
                "integer exceeds the int64 range the format carries",
                detail=str(value),
            )
        return [TAG_INT64, str(value)]
    if type(value) is float:
        return [TAG_FLOAT64, _encode_float(value)]
    if type(value) is str:
        return [TAG_STRING, value]
    if value is None:
        raise LogicalValueError(
            "python None is ambiguous here; use LOGICAL_NULL for an explicit "
            "null, or omit the property entirely for absent",
            detail="None",
        )
    raise LogicalValueError(
        "unsupported logical value type", detail=type(value).__name__
    )


def _encode_float(value: float) -> str:
    if type(value) is not float:
        raise LogicalValueError(
            "float64 requires a float", detail=type(value).__name__
        )
    if math.isnan(value) or math.isinf(value):
        raise LogicalValueError(
            "NaN and infinity have no portable logical meaning",
            detail=repr(value),
        )
    # ``float.hex`` is exact and platform-independent, so a round trip cannot
    # quietly move the last bit of a stored embedding component.
    return value.hex()


def decode_value(payload: Any) -> LogicalValue:
    """Decode one tagged value, refusing anything the format cannot mean."""

    if not isinstance(payload, list) or not payload:
        raise ArtifactMalformedError(
            "a value must be a non-empty tagged array", detail=repr(payload)[:80]
        )
    tag = payload[0]
    if tag == TAG_NULL:
        _require_arity(payload, 1, tag)
        return LOGICAL_NULL
    _require_arity(payload, 2, tag)
    body = payload[1]
    if tag == TAG_BOOL:
        if body is not True and body is not False:
            raise ArtifactMalformedError("bool carries a non-bool", detail=repr(body))
        return body
    if tag == TAG_INT64:
        return _decode_int64(body, TAG_INT64)
    if tag == TAG_FLOAT64:
        return _decode_float(body)
    if tag == TAG_STRING:
        if type(body) is not str:
            raise ArtifactMalformedError(
                "string carries a non-string", detail=type(body).__name__
            )
        return body
    if tag == TAG_TIMESTAMP_US:
        return LogicalTimestamp(_decode_int64(body, TAG_TIMESTAMP_US))
    if tag == TAG_VECTOR:
        return _decode_vector(body)
    raise ArtifactMalformedError("unknown value tag", detail=repr(tag)[:40])


def _require_arity(payload: list[Any], arity: int, tag: Any) -> None:
    if len(payload) != arity:
        raise ArtifactMalformedError(
            "tagged value has the wrong arity",
            detail=f"{tag!r} expects {arity}, got {len(payload)}",
        )


def _decode_int64(body: Any, tag: str) -> int:
    if type(body) is not str:
        raise ArtifactMalformedError(
            f"{tag} must be carried as a decimal string",
            detail=type(body).__name__,
        )
    try:
        value = int(body, 10)
    except ValueError as failure:
        raise ArtifactMalformedError(
            f"{tag} is not a decimal integer", detail=body[:40]
        ) from failure
    if str(value) != body:
        raise ArtifactMalformedError(
            f"{tag} is not in canonical decimal form", detail=body[:40]
        )
    if not INT64_MIN <= value <= INT64_MAX:
        raise ArtifactMalformedError(
            f"{tag} exceeds the int64 range", detail=body[:40]
        )
    return value


def _decode_float(body: Any) -> float:
    if type(body) is not str:
        raise ArtifactMalformedError(
            "float64 must be carried as a hexadecimal string",
            detail=type(body).__name__,
        )
    try:
        value = float.fromhex(body)
    except ValueError as failure:
        raise ArtifactMalformedError(
            "float64 is not an exact hexadecimal float", detail=body[:40]
        ) from failure
    except OverflowError as failure:
        # An extreme exponent raises OverflowError, not ValueError. Letting it
        # escape would put an untyped builtin on a boundary that promises only
        # typed refusals.
        raise ArtifactMalformedError(
            "float64 exponent is outside the representable range",
            detail=body[:40],
        ) from failure
    if math.isnan(value) or math.isinf(value):
        raise ArtifactMalformedError(
            "float64 decoded to NaN or infinity", detail=body[:40]
        )
    if value.hex() != body:
        # Several spellings parse to the same float. Accepting them would let
        # two artifacts with identical content have different bytes, so the
        # checksum would stop being a function of the graph.
        raise ArtifactMalformedError(
            "float64 is not in canonical hexadecimal form", detail=body[:40]
        )
    return value


def _decode_vector(body: Any) -> LogicalVector:
    if not isinstance(body, Mapping):
        raise ArtifactMalformedError(
            "vector must be an object", detail=type(body).__name__
        )
    _require_exact_keys(body, ("components", "dtype", "space_name"), "vector")
    raw_components = body["components"]
    if not isinstance(raw_components, list):
        raise ArtifactMalformedError(
            "vector components must be an array",
            detail=type(raw_components).__name__,
        )
    return LogicalVector(
        space_name=_require_str(body["space_name"], "vector space_name"),
        dtype=_require_str(body["dtype"], "vector dtype"),
        components=tuple(_decode_float(part) for part in raw_components),
    )


def _require_exact_keys(
    payload: Mapping[str, Any], expected: tuple[str, ...], what: str
) -> None:
    present = set(payload)
    wanted = set(expected)
    missing = wanted - present
    if missing:
        raise ArtifactMalformedError(
            f"{what} is missing fields", detail=",".join(sorted(missing))
        )
    unknown = present - wanted
    if unknown:
        raise ArtifactMalformedError(
            f"{what} carries unknown fields", detail=",".join(sorted(unknown))
        )


def _require_str(value: Any, what: str) -> str:
    if type(value) is not str or not value:
        raise ArtifactMalformedError(
            f"{what} must be a non-empty string", detail=repr(value)[:40]
        )
    return value


def encode_properties(properties: Mapping[str, LogicalValue]) -> dict[str, Any]:
    """Encode a property mapping; an absent property simply has no key."""

    return {name: encode_value(value) for name, value in properties.items()}


def decode_properties(payload: Any) -> dict[str, LogicalValue]:
    if not isinstance(payload, Mapping):
        raise ArtifactMalformedError(
            "properties must be an object", detail=type(payload).__name__
        )
    decoded: dict[str, LogicalValue] = {}
    for name, value in payload.items():
        if type(name) is not str or not name:
            raise ArtifactMalformedError(
                "property name must be a non-empty string", detail=repr(name)[:40]
            )
        decoded[name] = decode_value(value)
    return decoded


def encode_node(node: LogicalNode) -> dict[str, Any]:
    return {
        "key": node.key,
        "properties": encode_properties(node.properties),
        "type": node.type_name,
    }


def decode_node(payload: Mapping[str, Any]) -> LogicalNode:
    _require_exact_keys(payload, ("key", "properties", "type"), "node")
    return LogicalNode(
        type_name=_require_str(payload["type"], "node type"),
        key=_require_str(payload["key"], "node key"),
        properties=decode_properties(payload["properties"]),
    )


def encode_relation(relation: LogicalRelation) -> dict[str, Any]:
    return {
        "layout": relation.layout_name,
        "properties": encode_properties(relation.properties),
        "source": relation.source_key,
        "source_type": relation.source_type,
        "target": relation.target_key,
        "target_type": relation.target_type,
    }


def decode_relation(payload: Mapping[str, Any]) -> LogicalRelation:
    _require_exact_keys(
        payload,
        ("layout", "properties", "source", "source_type", "target", "target_type"),
        "relation",
    )
    return LogicalRelation(
        layout_name=_require_str(payload["layout"], "relation layout"),
        source_type=_require_str(payload["source_type"], "relation source_type"),
        target_type=_require_str(payload["target_type"], "relation target_type"),
        source_key=_require_str(payload["source"], "relation source"),
        target_key=_require_str(payload["target"], "relation target"),
        properties=decode_properties(payload["properties"]),
    )


def encode_schema(schema: LogicalSchema) -> dict[str, Any]:
    return {
        "node_types": [
            {
                "key": node_type.key,
                "name": node_type.name,
                "properties": [_encode_property(p) for p in node_type.properties],
            }
            for node_type in schema.node_types
        ],
        "relation_layouts": [
            {
                "name": layout.name,
                "properties": [_encode_property(p) for p in layout.properties],
                "source_type": layout.source_type,
                "target_type": layout.target_type,
            }
            for layout in schema.relation_layouts
        ],
        "scope": schema.scope,
        "vector_spaces": [
            {
                "dimension": space.dimension,
                "metric": space.metric,
                "name": space.name,
                "normalized": space.normalized,
                "storage_dtype": space.storage_dtype,
            }
            for space in schema.vector_spaces
        ],
    }


def _encode_property(prop: LogicalPropertyDef) -> dict[str, Any]:
    return {
        "name": prop.name,
        "nullable": prop.nullable,
        "type": prop.type,
        "vector_space": prop.vector_space,
    }


def decode_schema(payload: Any) -> LogicalSchema:
    if not isinstance(payload, Mapping):
        raise ArtifactMalformedError(
            "schema must be an object", detail=type(payload).__name__
        )
    _require_exact_keys(
        payload,
        ("node_types", "relation_layouts", "scope", "vector_spaces"),
        "schema",
    )
    scope = payload["scope"]
    if type(scope) is not str:
        raise ArtifactMalformedError(
            "schema scope must be a string", detail=type(scope).__name__
        )
    return LogicalSchema(
        scope=scope,  # type: ignore[arg-type]
        node_types=tuple(
            _decode_node_type(entry)
            for entry in _require_list(payload["node_types"], "node_types")
        ),
        relation_layouts=tuple(
            _decode_relation_layout(entry)
            for entry in _require_list(payload["relation_layouts"], "relation_layouts")
        ),
        vector_spaces=tuple(
            _decode_vector_space(entry)
            for entry in _require_list(payload["vector_spaces"], "vector_spaces")
        ),
    )


def _require_list(value: Any, what: str) -> list[Any]:
    if not isinstance(value, list):
        raise ArtifactMalformedError(
            f"{what} must be an array", detail=type(value).__name__
        )
    return value


def _require_mapping(value: Any, what: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ArtifactMalformedError(
            f"{what} must be an object", detail=type(value).__name__
        )
    return value


def _decode_node_type(entry: Any) -> LogicalNodeType:
    payload = _require_mapping(entry, "node type")
    _require_exact_keys(payload, ("key", "name", "properties"), "node type")
    return LogicalNodeType(
        name=_require_str(payload["name"], "node type name"),
        key=_require_str(payload["key"], "node type key"),
        properties=tuple(
            _decode_property(item)
            for item in _require_list(payload["properties"], "node type properties")
        ),
    )


def _decode_relation_layout(entry: Any) -> LogicalRelationLayout:
    payload = _require_mapping(entry, "relation layout")
    _require_exact_keys(
        payload,
        ("name", "properties", "source_type", "target_type"),
        "relation layout",
    )
    return LogicalRelationLayout(
        name=_require_str(payload["name"], "relation layout name"),
        source_type=_require_str(payload["source_type"], "relation source_type"),
        target_type=_require_str(payload["target_type"], "relation target_type"),
        properties=tuple(
            _decode_property(item)
            for item in _require_list(
                payload["properties"], "relation layout properties"
            )
        ),
    )


def _decode_vector_space(entry: Any) -> LogicalVectorSpace:
    payload = _require_mapping(entry, "vector space")
    _require_exact_keys(
        payload,
        ("dimension", "metric", "name", "normalized", "storage_dtype"),
        "vector space",
    )
    dimension = payload["dimension"]
    if type(dimension) is not int:
        raise ArtifactMalformedError(
            "vector space dimension must be an int",
            detail=type(dimension).__name__,
        )
    normalized = payload["normalized"]
    if normalized is not True and normalized is not False:
        raise ArtifactMalformedError(
            "vector space normalized must be a bool",
            detail=repr(normalized)[:40],
        )
    return LogicalVectorSpace(
        name=_require_str(payload["name"], "vector space name"),
        storage_dtype=_require_str(
            payload["storage_dtype"], "vector space storage_dtype"
        ),
        dimension=dimension,
        metric=_require_str(payload["metric"], "vector space metric"),
        normalized=normalized,
    )


def _decode_property(entry: Any) -> LogicalPropertyDef:
    payload = _require_mapping(entry, "property")
    _require_exact_keys(
        payload, ("name", "nullable", "type", "vector_space"), "property"
    )
    vector_space = payload["vector_space"]
    if vector_space is not None and (
        type(vector_space) is not str or not vector_space
    ):
        raise ArtifactMalformedError(
            "property vector_space must be a non-empty string or null",
            detail=repr(vector_space)[:40],
        )
    nullable = payload["nullable"]
    if nullable is not True and nullable is not False:
        raise ArtifactMalformedError(
            "property nullable must be a bool", detail=repr(nullable)[:40]
        )
    property_type = payload["type"]
    if type(property_type) is not str:
        raise ArtifactMalformedError(
            "property type must be a string", detail=type(property_type).__name__
        )
    return LogicalPropertyDef(
        name=_require_str(payload["name"], "property name"),
        type=property_type,  # type: ignore[arg-type]
        nullable=nullable,
        vector_space=vector_space,
    )


__all__ = [
    "TAG_BOOL",
    "TAG_FLOAT64",
    "TAG_INT64",
    "TAG_NULL",
    "TAG_STRING",
    "TAG_TIMESTAMP_US",
    "TAG_VECTOR",
    "VALUE_TAGS",
    "canonical_bytes",
    "canonical_json",
    "decode_node",
    "decode_properties",
    "decode_relation",
    "decode_schema",
    "decode_value",
    "encode_node",
    "encode_properties",
    "encode_relation",
    "encode_schema",
    "encode_value",
    "loads_canonical",
]
