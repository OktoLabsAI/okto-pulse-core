"""The logical fingerprint of a graph: order-independent, multiplicity-exact.

A transfer has to prove that what arrived is what left, without depending on
the order either side happened to walk its storage in.  That rules out hashing
a concatenation.  It also rules out XOR-combining per-item digests, which is
order-independent but *cancels*: two identical parallel relations would XOR to
zero and vanish from the digest -- precisely the multiplicity this format
exists to preserve.

So each item is digested on its own and the digests are summed modulo 2**256.
Addition is commutative (order-independent) and does not cancel (adding the
same digest twice is visibly different from adding it once), and the
accumulator stays a single integer, so a stream of any size costs O(1) memory.

Each section is digested under its own domain tag, and the final digest commits
to the schema plus, per section, the domain, the count and the modular sum.
Committing to the count as well as the sum means a section cannot be confused
with another that happens to sum alike.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

from .canonical import canonical_bytes, encode_node, encode_relation, encode_schema
from .model import (
    LogicalCounts,
    LogicalNode,
    LogicalRelation,
    LogicalSchema,
    record_census,
)


FINGERPRINT_DOMAIN: Final[bytes] = b"okto-pulse-logical-graph/1"
SECTION_SCHEMA: Final[bytes] = b"schema"
SECTION_NODES: Final[bytes] = b"nodes"
SECTION_RELATIONS: Final[bytes] = b"relations"
SECTION_FINAL: Final[bytes] = b"fingerprint"

_MODULUS: Final[int] = 2**256
_SEPARATOR: Final[bytes] = b"\x00"


def _item_digest(section: bytes, payload: bytes) -> int:
    """Digest one item under its section's domain, as an integer."""

    digest = hashlib.sha256(
        FINGERPRINT_DOMAIN + _SEPARATOR + section + _SEPARATOR + payload
    ).digest()
    return int.from_bytes(digest, "big")


def schema_digest(schema: LogicalSchema) -> str:
    """Return the hex digest of a schema, precomputable before any record."""

    payload = canonical_bytes(encode_schema(schema))
    digest = hashlib.sha256(
        FINGERPRINT_DOMAIN + _SEPARATOR + SECTION_SCHEMA + _SEPARATOR + payload
    )
    return digest.hexdigest()


@dataclass(slots=True)
class LogicalFingerprintAccumulator:
    """Accumulates a logical fingerprint from a stream of records.

    The accumulator never holds the graph -- only two counters and two modular
    sums -- so the fingerprint of an artifact costs the same memory whether it
    carries ten records or ten million.
    """

    schema_hex: str
    node_count: int = 0
    relation_count: int = 0
    property_count: int = 0
    vector_count: int = 0
    _node_sum: int = 0
    _relation_sum: int = 0

    @classmethod
    def for_schema(cls, schema: LogicalSchema) -> LogicalFingerprintAccumulator:
        return cls(schema_hex=schema_digest(schema))

    def add_node(self, node: LogicalNode) -> None:
        payload = canonical_bytes(encode_node(node))
        self._node_sum = (self._node_sum + _item_digest(SECTION_NODES, payload)) % (
            _MODULUS
        )
        self.node_count += 1
        properties, vectors = record_census(node.properties)
        self.property_count += properties
        self.vector_count += vectors

    def add_relation(self, relation: LogicalRelation) -> None:
        payload = canonical_bytes(encode_relation(relation))
        self._relation_sum = (
            self._relation_sum + _item_digest(SECTION_RELATIONS, payload)
        ) % _MODULUS
        self.relation_count += 1
        properties, vectors = record_census(relation.properties)
        self.property_count += properties
        self.vector_count += vectors

    def counts(self) -> LogicalCounts:
        return LogicalCounts(
            nodes=self.node_count,
            relations=self.relation_count,
            properties=self.property_count,
            vectors=self.vector_count,
        )

    def digest(self) -> str:
        """Return the final hex fingerprint over schema and both multisets."""

        final = hashlib.sha256()
        final.update(FINGERPRINT_DOMAIN + _SEPARATOR + SECTION_FINAL + _SEPARATOR)
        final.update(bytes.fromhex(self.schema_hex))
        _commit_section(final, SECTION_NODES, self.node_count, self._node_sum)
        _commit_section(
            final, SECTION_RELATIONS, self.relation_count, self._relation_sum
        )
        # Property and vector totals are already implied by the record digests;
        # committing them explicitly makes a census disagreement visible in the
        # fingerprint itself rather than only in the manifest.
        _commit_count(final, b"properties", self.property_count)
        _commit_count(final, b"vectors", self.vector_count)
        return final.hexdigest()


def _commit_section(
    digest: "hashlib._Hash", section: bytes, count: int, total: int
) -> None:
    digest.update(_SEPARATOR + section + _SEPARATOR)
    digest.update(count.to_bytes(8, "big"))
    digest.update(total.to_bytes(32, "big"))


def _commit_count(digest: "hashlib._Hash", section: bytes, count: int) -> None:
    digest.update(_SEPARATOR + section + _SEPARATOR)
    digest.update(count.to_bytes(8, "big"))


def fingerprint_graph(
    schema: LogicalSchema,
    nodes: Iterable[LogicalNode],
    relations: Iterable[LogicalRelation],
) -> str:
    """Fingerprint a materialized graph, for callers that already hold one."""

    accumulator = LogicalFingerprintAccumulator.for_schema(schema)
    for node in nodes:
        accumulator.add_node(node)
    for relation in relations:
        accumulator.add_relation(relation)
    return accumulator.digest()


__all__ = [
    "FINGERPRINT_DOMAIN",
    "LogicalFingerprintAccumulator",
    "SECTION_FINAL",
    "SECTION_NODES",
    "SECTION_RELATIONS",
    "SECTION_SCHEMA",
    "fingerprint_graph",
    "schema_digest",
]
