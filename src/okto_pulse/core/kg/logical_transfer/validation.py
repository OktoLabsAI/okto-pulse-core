"""Strict record-against-schema validation, one record at a time.

Without this, a schema is only a header decoration: an adapter bug could emit a
node of an undeclared type, a property nobody declared, a null in a
non-nullable column, or a vector belonging to the wrong space, and every
downstream check would still agree with itself.  The counts would match, the
checksum would match, and the fingerprint would match, because all three
describe the bytes that were produced rather than whether those bytes mean
anything under the declared schema.  The result is the worst kind of artifact:
internally consistent and semantically invalid.

The index is built once per schema and every check after that is a dict lookup,
so validating a stream costs O(properties of one record) and never grows with
the graph.

What is deliberately NOT checked here is referential existence -- whether a
relation's endpoints name nodes that actually arrived.  That needs the whole
graph, so it belongs to the sink's own verification rather than to a bounded
streaming pass.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from .errors import LogicalSchemaError
from .model import (
    INT64_MAX,
    INT64_MIN,
    LayoutIdentity,
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


@dataclass(frozen=True, slots=True)
class LogicalSchemaIndex:
    """A schema prepared for per-record validation in constant time."""

    schema: LogicalSchema
    node_types: Mapping[str, LogicalNodeType]
    layouts: Mapping[LayoutIdentity, LogicalRelationLayout]
    spaces: Mapping[str, LogicalVectorSpace]
    node_properties: Mapping[str, Mapping[str, LogicalPropertyDef]]
    layout_properties: Mapping[LayoutIdentity, Mapping[str, LogicalPropertyDef]]

    @classmethod
    def build(cls, schema: LogicalSchema) -> LogicalSchemaIndex:
        return cls(
            schema=schema,
            node_types=MappingProxyType(
                {node.name: node for node in schema.node_types}
            ),
            layouts=MappingProxyType(
                {layout.identity: layout for layout in schema.relation_layouts}
            ),
            spaces=MappingProxyType(
                {space.name: space for space in schema.vector_spaces}
            ),
            node_properties=MappingProxyType(
                {
                    node.name: MappingProxyType(
                        {prop.name: prop for prop in node.properties}
                    )
                    for node in schema.node_types
                }
            ),
            layout_properties=MappingProxyType(
                {
                    layout.identity: MappingProxyType(
                        {prop.name: prop for prop in layout.properties}
                    )
                    for layout in schema.relation_layouts
                }
            ),
        )

    def validate_node(self, node: LogicalNode) -> None:
        node_type = self.node_types.get(node.type_name)
        if node_type is None:
            raise LogicalSchemaError(
                "node names a type the schema does not declare",
                detail=node.type_name,
            )
        declared = self.node_properties[node.type_name]
        owner = f"{node.type_name}:{node.key}"
        self._validate_properties(node.properties, declared, owner)
        # The key property is what names this node; a record whose key column
        # disagrees with its own key would be two different identities.
        key_value = node.properties.get(node_type.key)
        if key_value is None:
            raise LogicalSchemaError(
                "node omits the key property its type declares",
                detail=f"{owner} missing {node_type.key!r}",
            )
        if type(key_value) is not str or key_value != node.key:
            raise LogicalSchemaError(
                "node key does not match its key property",
                detail=f"{owner} carries {key_value!r}",
            )

    def validate_relation(self, relation: LogicalRelation) -> None:
        identity = relation.layout_identity
        layout = self.layouts.get(identity)
        if layout is None:
            # Looked up by the whole triple: a name that exists between other
            # endpoint types is not this relation's layout.
            raise LogicalSchemaError(
                "relation names a layout the schema does not declare",
                detail=(
                    f"{identity[0]}({identity[1]}->{identity[2]})"
                ),
            )
        declared = self.layout_properties[identity]
        self._validate_properties(
            relation.properties,
            declared,
            f"relation {identity[0]}({identity[1]}->{identity[2]})",
        )

    def _validate_properties(
        self,
        properties: Mapping[str, LogicalValue],
        declared: Mapping[str, LogicalPropertyDef],
        owner: str,
    ) -> None:
        for name, value in properties.items():
            prop = declared.get(name)
            if prop is None:
                raise LogicalSchemaError(
                    "record carries a property the schema does not declare",
                    detail=f"{owner}.{name}",
                )
            self._validate_value(value, prop, owner)

    def _validate_value(
        self, value: LogicalValue, prop: LogicalPropertyDef, owner: str
    ) -> None:
        where = f"{owner}.{prop.name}"
        if isinstance(value, LogicalNull):
            if not prop.nullable:
                raise LogicalSchemaError(
                    "null in a property the schema declares non-nullable",
                    detail=where,
                )
            return
        expected = prop.type
        if expected == "bool":
            if value is not True and value is not False:
                raise _wrong_type(where, expected, value)
            return
        if expected == "int64":
            # bool first: it is a subclass of int, and an int64 column that
            # accepted True would round-trip it as 1.
            if value is True or value is False or type(value) is not int:
                raise _wrong_type(where, expected, value)
            if not INT64_MIN <= value <= INT64_MAX:
                raise LogicalSchemaError(
                    "int64 property is out of range", detail=where
                )
            return
        if expected == "float64":
            if type(value) is not float:
                raise _wrong_type(where, expected, value)
            return
        if expected == "string":
            if type(value) is not str:
                raise _wrong_type(where, expected, value)
            return
        if expected == "timestamp_us":
            if not isinstance(value, LogicalTimestamp):
                raise _wrong_type(where, expected, value)
            return
        if not isinstance(value, LogicalVector):
            raise _wrong_type(where, expected, value)
        self._validate_vector(value, prop, where)

    def _validate_vector(
        self, value: LogicalVector, prop: LogicalPropertyDef, where: str
    ) -> None:
        if value.space_name != prop.vector_space:
            raise LogicalSchemaError(
                "vector belongs to a different space than its property declares",
                detail=f"{where}: {value.space_name!r} != {prop.vector_space!r}",
            )
        space = self.spaces.get(value.space_name)
        if space is None:
            raise LogicalSchemaError(
                "vector names a space the schema does not declare",
                detail=f"{where}: {value.space_name}",
            )
        if value.dtype != space.storage_dtype:
            raise LogicalSchemaError(
                "vector dtype differs from its space",
                detail=f"{where}: {value.dtype!r} != {space.storage_dtype!r}",
            )
        if len(value.components) != space.dimension:
            raise LogicalSchemaError(
                "vector width differs from its space dimension",
                detail=(
                    f"{where}: {len(value.components)} != {space.dimension}"
                ),
            )


def _wrong_type(where: str, expected: str, value: object) -> LogicalSchemaError:
    return LogicalSchemaError(
        "property value does not match its declared type",
        detail=f"{where}: expected {expected}, got {type(value).__name__}",
    )


__all__ = ["LogicalSchemaIndex"]
