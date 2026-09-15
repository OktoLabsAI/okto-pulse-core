"""Frozen logical graph DTOs for the ``okto-pulse-logical-graph/1`` format.

Everything here is logical.  A node is named by its declared type and its key, a
relation by its layout and the keys of its endpoints, and a vector by the
logical name of the space it belongs to.  Physical identity -- record ids,
pages, filenames, WAL positions, space ids, HNSW topology -- is deliberately
absent, because a portable artifact carrying it would only be restorable into
the storage engine that produced it.

Two distinctions in this module are load-bearing and easy to lose:

``absent`` versus ``NULL``
    A property missing from ``properties`` was never set.  A property mapped to
    :data:`LOGICAL_NULL` was set to null.  Those are different facts about the
    source graph, so the mapping omits the first and records the second.

one entry per occurrence
    Relations are carried in a tuple, never a set.  Two parallel relations with
    identical endpoints and identical properties are two relations, and a
    transfer that preserved only one of them would be lossy.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final, Literal, Union

from .errors import LogicalSchemaError


LogicalScope = Literal["board", "global_discovery"]
LOGICAL_SCOPES: Final[tuple[LogicalScope, ...]] = ("board", "global_discovery")

LogicalPropertyType = Literal[
    "bool",
    "int64",
    "float64",
    "string",
    "timestamp_us",
    "vector",
]
LOGICAL_PROPERTY_TYPES: Final[tuple[LogicalPropertyType, ...]] = (
    "bool",
    "int64",
    "float64",
    "string",
    "timestamp_us",
    "vector",
)

INT64_MIN: Final[int] = -(2**63)
INT64_MAX: Final[int] = 2**63 - 1


class LogicalNull:
    """The explicit null value, distinct from a property being absent."""

    __slots__ = ()

    _instance: LogicalNull | None = None

    def __new__(cls) -> LogicalNull:
        existing = cls._instance
        if existing is None:
            existing = super().__new__(cls)
            cls._instance = existing
        return existing

    def __repr__(self) -> str:
        return "LOGICAL_NULL"

    def __reduce__(self) -> str:
        return "LOGICAL_NULL"


LOGICAL_NULL: Final[LogicalNull] = LogicalNull()


@dataclass(frozen=True, slots=True)
class LogicalTimestamp:
    """An instant carried as whole microseconds since the Unix epoch.

    Micros rather than formatted text: the format has to round-trip an instant
    exactly, and a text encoding invites the destination to reinterpret an
    offset or to drop sub-second precision.
    """

    micros: int

    def __post_init__(self) -> None:
        if type(self.micros) is not int:
            raise LogicalSchemaError(
                "timestamp requires whole microseconds as an int",
                detail=f"got {type(self.micros).__name__}",
            )
        if not INT64_MIN <= self.micros <= INT64_MAX:
            raise LogicalSchemaError(
                "timestamp microseconds exceed the int64 range",
                detail=str(self.micros),
            )


@dataclass(frozen=True, slots=True)
class LogicalVector:
    """An embedding named by the LOGICAL space it belongs to.

    ``space_name`` is the portable half of a vector: the destination maps that
    name onto whatever space id it happens to allocate.  Carrying the source's
    space id instead would bind the artifact to one database.
    """

    space_name: str
    dtype: str
    components: tuple[float, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "components", tuple(self.components))
        _require_name(self.space_name, "vector space_name")
        _require_name(self.dtype, "vector dtype")
        for position, component in enumerate(self.components):
            if type(component) is not float:
                raise LogicalSchemaError(
                    "vector components must be floats",
                    detail=f"{self.space_name}[{position}]",
                )


LogicalValue = Union[
    LogicalNull,
    bool,
    int,
    float,
    str,
    LogicalTimestamp,
    LogicalVector,
]


@dataclass(frozen=True, slots=True)
class LogicalPropertyDef:
    """One declared property of a node type or a relation layout.

    A vector property names its space through ``vector_space`` rather than by
    being called after it.  The real schema needs that: every Board node type
    carries a property named ``embedding``, and each one belongs to a different
    space.  Deriving the space from the property name would collapse eleven
    spaces into one.
    """

    name: str
    type: LogicalPropertyType
    nullable: bool = True
    vector_space: str | None = None

    def __post_init__(self) -> None:
        _require_name(self.name, "property name")
        if self.type not in LOGICAL_PROPERTY_TYPES:
            raise LogicalSchemaError(
                "unknown logical property type",
                detail=f"{self.name}: {self.type!r}",
            )
        if self.nullable is not True and self.nullable is not False:
            # The wire carries a JSON bool and the decoder refuses anything
            # else, so accepting a truthy 1 here would let the encoder produce
            # a schema this very build cannot read back.
            raise LogicalSchemaError(
                "property nullable must be a bool",
                detail=f"{self.name}: {self.nullable!r}",
            )
        if self.type == "vector":
            _require_name(self.vector_space, f"vector_space of {self.name!r}")
        elif self.vector_space is not None:
            raise LogicalSchemaError(
                "only a vector property may name a vector space",
                detail=f"{self.name}: {self.type}",
            )


@dataclass(frozen=True, slots=True)
class LogicalVectorSpace:
    """A logical embedding space, including what makes its distances mean something.

    ``metric`` and ``normalized`` travel because they decide what a neighbour
    IS.  A round trip that recreated a cosine space as L2, or flipped
    normalization, would still match on names, counts and dimensions -- and
    would answer every future search differently.  Carrying them means the
    schema digest and the fingerprint refuse that transfer instead of
    certifying it.
    """

    name: str
    storage_dtype: str
    dimension: int
    metric: str
    normalized: bool

    def __post_init__(self) -> None:
        _require_name(self.name, "vector space name")
        _require_name(self.storage_dtype, "vector space storage_dtype")
        _require_name(self.metric, "vector space metric")
        if type(self.dimension) is not int or self.dimension <= 0:
            raise LogicalSchemaError(
                "vector space dimension must be a positive int",
                detail=f"{self.name}: {self.dimension!r}",
            )
        if self.normalized is not True and self.normalized is not False:
            raise LogicalSchemaError(
                "vector space normalized must be a bool",
                detail=f"{self.name}: {self.normalized!r}",
            )


@dataclass(frozen=True, slots=True)
class LogicalNodeType:
    """A node type, its key property and every property it declares."""

    name: str
    key: str
    properties: tuple[LogicalPropertyDef, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "properties", tuple(self.properties))
        _require_name(self.name, "node type name")
        _require_name(self.key, "node type key")
        _require_unique_properties(self.properties, f"node type {self.name!r}")
        if self.key not in self.property_names():
            raise LogicalSchemaError(
                "node type key must be one of its declared properties",
                detail=f"{self.name}.{self.key}",
            )

    def property_names(self) -> frozenset[str]:
        return frozenset(prop.name for prop in self.properties)

    def property_def(self, name: str) -> LogicalPropertyDef:
        for prop in self.properties:
            if prop.name == name:
                return prop
        raise LogicalSchemaError(
            "undeclared property on node type", detail=f"{self.name}.{name}"
        )


LayoutIdentity = tuple[str, str, str]


@dataclass(frozen=True, slots=True)
class LogicalRelationLayout:
    """A directed relation layout, identified by its name AND its endpoint types.

    The name alone is not an identity.  The real Board schema has 69 concrete
    endpoint triples sharing only 16 logical names -- ``supersedes`` exists
    between Decision and Decision and again between Alternative and
    Alternative.  Keying layouts by name would drop 53 of them, and merely
    allowing duplicate names would leave an occurrence ambiguous whenever the
    same key value exists under two different node types.

    So identity is the triple, and every occurrence carries it.
    """

    name: str
    source_type: str
    target_type: str
    properties: tuple[LogicalPropertyDef, ...] = ()

    @property
    def identity(self) -> LayoutIdentity:
        return (self.name, self.source_type, self.target_type)

    def __post_init__(self) -> None:
        object.__setattr__(self, "properties", tuple(self.properties))
        _require_name(self.name, "relation layout name")
        _require_name(self.source_type, "relation source type")
        _require_name(self.target_type, "relation target type")
        _require_unique_properties(self.properties, f"relation layout {self.name!r}")

    def property_names(self) -> frozenset[str]:
        return frozenset(prop.name for prop in self.properties)

    def property_def(self, name: str) -> LogicalPropertyDef:
        for prop in self.properties:
            if prop.name == name:
                return prop
        raise LogicalSchemaError(
            "undeclared property on relation layout", detail=f"{self.name}.{name}"
        )


@dataclass(frozen=True, slots=True)
class LogicalSchema:
    """The complete logical shape of one transferable scope."""

    scope: LogicalScope
    node_types: tuple[LogicalNodeType, ...] = ()
    relation_layouts: tuple[LogicalRelationLayout, ...] = ()
    vector_spaces: tuple[LogicalVectorSpace, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_types", tuple(self.node_types))
        object.__setattr__(self, "relation_layouts", tuple(self.relation_layouts))
        object.__setattr__(self, "vector_spaces", tuple(self.vector_spaces))
        if self.scope not in LOGICAL_SCOPES:
            raise LogicalSchemaError("unknown logical scope", detail=repr(self.scope))
        _require_unique((node.name for node in self.node_types), "node type")
        _require_unique_identities(
            tuple(layout.identity for layout in self.relation_layouts)
        )
        _require_unique((space.name for space in self.vector_spaces), "vector space")
        known_types = {node.name for node in self.node_types}
        for layout in self.relation_layouts:
            for role, referenced in (
                ("source", layout.source_type),
                ("target", layout.target_type),
            ):
                if referenced not in known_types:
                    raise LogicalSchemaError(
                        f"relation layout names an undeclared {role} node type",
                        detail=f"{layout.name} -> {referenced}",
                    )
        known_spaces = {space.name for space in self.vector_spaces}
        for owner, properties in self._owned_properties():
            for prop in properties:
                if prop.type == "vector" and prop.vector_space not in known_spaces:
                    raise LogicalSchemaError(
                        "vector property names an undeclared vector space",
                        detail=f"{owner}.{prop.name} -> {prop.vector_space}",
                    )

    def _owned_properties(
        self,
    ) -> Iterator[tuple[str, tuple[LogicalPropertyDef, ...]]]:
        for node_type in self.node_types:
            yield node_type.name, node_type.properties
        for layout in self.relation_layouts:
            yield layout.name, layout.properties

    def node_type(self, name: str) -> LogicalNodeType:
        for node_type in self.node_types:
            if node_type.name == name:
                return node_type
        raise LogicalSchemaError("undeclared node type", detail=name)

    def relation_layout(
        self, name: str, source_type: str, target_type: str
    ) -> LogicalRelationLayout:
        wanted = (name, source_type, target_type)
        for layout in self.relation_layouts:
            if layout.identity == wanted:
                return layout
        raise LogicalSchemaError(
            "undeclared relation layout",
            detail=f"{name}({source_type}->{target_type})",
        )

    def vector_space(self, name: str) -> LogicalVectorSpace:
        for space in self.vector_spaces:
            if space.name == name:
                return space
        raise LogicalSchemaError("undeclared vector space", detail=name)


@dataclass(frozen=True, slots=True)
class LogicalNode:
    """One node occurrence, named by its type and its logical key."""

    type_name: str
    key: str
    properties: Mapping[str, LogicalValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_name(self.type_name, "node type_name")
        _require_name(self.key, "node key")
        object.__setattr__(self, "properties", _frozen_properties(self.properties))
        _require_property_names(
            self.properties, f"node {self.type_name}:{self.key}"
        )


@dataclass(frozen=True, slots=True)
class LogicalRelation:
    """One directed relation occurrence between two logical keys.

    A self-loop is ``source_key == target_key``.  Two of these that compare
    equal are two occurrences, not a duplicate to collapse.

    The endpoint TYPES travel alongside the keys because the layout name does
    not identify a layout on its own.  Without them, a ``supersedes`` between
    two Decisions and a ``supersedes`` between two Alternatives would be
    indistinguishable whenever the keys happened to coincide.
    """

    layout_name: str
    source_type: str
    target_type: str
    source_key: str
    target_key: str
    properties: Mapping[str, LogicalValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_name(self.layout_name, "relation layout_name")
        _require_name(self.source_type, "relation source_type")
        _require_name(self.target_type, "relation target_type")
        _require_name(self.source_key, "relation source_key")
        _require_name(self.target_key, "relation target_key")
        object.__setattr__(self, "properties", _frozen_properties(self.properties))
        _require_property_names(self.properties, f"relation {self.layout_name}")

    @property
    def layout_identity(self) -> LayoutIdentity:
        return (self.layout_name, self.source_type, self.target_type)


@dataclass(frozen=True, slots=True)
class LogicalCounts:
    """The census a transfer has to reproduce exactly."""

    nodes: int = 0
    relations: int = 0
    properties: int = 0
    vectors: int = 0

    def __post_init__(self) -> None:
        for name in COUNT_FIELDS:
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise LogicalSchemaError(
                    "counts must be non-negative ints",
                    detail=f"{name}={value!r}",
                )

    def as_mapping(self) -> dict[str, int]:
        return {name: getattr(self, name) for name in COUNT_FIELDS}

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> LogicalCounts:
        missing = [name for name in COUNT_FIELDS if name not in payload]
        if missing:
            raise LogicalSchemaError(
                "counts are incomplete", detail=",".join(sorted(missing))
            )
        unknown = [name for name in payload if name not in COUNT_FIELDS]
        if unknown:
            raise LogicalSchemaError(
                "counts carry unknown fields", detail=",".join(sorted(unknown))
            )
        values: dict[str, int] = {}
        for name in COUNT_FIELDS:
            raw = payload[name]
            if type(raw) is not int:
                raise LogicalSchemaError(
                    "counts must be ints", detail=f"{name}={raw!r}"
                )
            values[name] = raw
        return cls(**values)


COUNT_FIELDS: Final[tuple[str, ...]] = ("nodes", "relations", "properties", "vectors")


def _frozen_properties(
    properties: Mapping[str, LogicalValue],
) -> Mapping[str, LogicalValue]:
    return MappingProxyType(dict(properties))


def _require_name(value: object, what: str) -> None:
    if type(value) is not str or not value:
        raise LogicalSchemaError(
            f"{what} must be a non-empty string", detail=repr(value)
        )


def _require_unique(names: Iterable[str], what: str) -> None:
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise LogicalSchemaError(f"duplicate {what}", detail=name)
        seen.add(name)


def _require_unique_identities(identities: tuple[LayoutIdentity, ...]) -> None:
    seen: set[LayoutIdentity] = set()
    for identity in identities:
        if identity in seen:
            raise LogicalSchemaError(
                "duplicate relation layout",
                detail=f"{identity[0]}({identity[1]}->{identity[2]})",
            )
        seen.add(identity)


def _require_unique_properties(
    properties: tuple[LogicalPropertyDef, ...], owner: str
) -> None:
    seen: set[str] = set()
    for prop in properties:
        if prop.name in seen:
            raise LogicalSchemaError(f"duplicate property on {owner}", detail=prop.name)
        seen.add(prop.name)


def _require_property_names(properties: Mapping[str, object], owner: str) -> None:
    for name in properties:
        if type(name) is not str or not name:
            raise LogicalSchemaError(
                f"property name on {owner} must be a non-empty string",
                detail=repr(name),
            )


def record_census(properties: Mapping[str, LogicalValue]) -> tuple[int, int]:
    """Return ``(property_count, vector_count)`` for one record's properties.

    Counting lives here so a streaming transfer and a materialized graph cannot
    drift into counting the same graph two different ways.
    """

    vectors = sum(
        1 for value in properties.values() if isinstance(value, LogicalVector)
    )
    return len(properties), vectors


def count_graph(
    nodes: Iterable[LogicalNode], relations: Iterable[LogicalRelation]
) -> LogicalCounts:
    """Count a materialized graph exactly as a streaming transfer counts one."""

    node_total = 0
    relation_total = 0
    properties = 0
    vectors = 0
    for node in nodes:
        node_total += 1
        node_properties, node_vectors = record_census(node.properties)
        properties += node_properties
        vectors += node_vectors
    for relation in relations:
        relation_total += 1
        relation_properties, relation_vectors = record_census(relation.properties)
        properties += relation_properties
        vectors += relation_vectors
    return LogicalCounts(
        nodes=node_total,
        relations=relation_total,
        properties=properties,
        vectors=vectors,
    )


__all__ = [
    "COUNT_FIELDS",
    "INT64_MAX",
    "INT64_MIN",
    "LOGICAL_NULL",
    "LOGICAL_PROPERTY_TYPES",
    "LOGICAL_SCOPES",
    "LogicalCounts",
    "LogicalNode",
    "LogicalNodeType",
    "LogicalNull",
    "LogicalPropertyDef",
    "LogicalPropertyType",
    "LogicalRelation",
    "LogicalRelationLayout",
    "LogicalSchema",
    "LogicalScope",
    "LogicalTimestamp",
    "LogicalValue",
    "LogicalVector",
    "LogicalVectorSpace",
    "LayoutIdentity",
    "count_graph",
    "record_census",
]
