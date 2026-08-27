"""The ``okto-pulse-logical-graph/1`` artifact codec.

The artifact is a sequence of canonical JSON records, one per line: a header,
then every node, then every relation, then a terminal manifest.  Nothing here
touches a filesystem, a path or a handle -- the encoder yields lines and the
decoder consumes them, so the same codec serves whatever the edition adapters
decide to write those lines to.

The shape is chosen so a reader can be strict on a single forward pass:

* the header arrives before any record, so a reader knows the schema, the
  declared census and the required features before it interprets anything;
* nodes precede relations, so a relation's endpoints are already known;
* the manifest is terminal and mandatory, which is the only thing that
  distinguishes a complete artifact from a truncated one;
* counts and checksums are verified against what the stream actually carried,
  so a silently damaged record cannot pass as a smaller graph.

The stream checksum lives only in the manifest.  Putting it in the header would
force the writer either to buffer the whole artifact or to seek back and patch
it, and this codec must be able to stream a graph it never holds in memory.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from .canonical import (
    canonical_json,
    decode_node,
    decode_relation,
    decode_schema,
    encode_node,
    encode_relation,
    encode_schema,
    loads_canonical,
)
from .errors import (
    ArtifactIntegrityError,
    ArtifactMalformedError,
    ArtifactSequenceError,
    ArtifactTrailingDataError,
    ArtifactTruncatedError,
    UnsupportedFeatureError,
    UnsupportedFormatVersionError,
)
from .fingerprint import LogicalFingerprintAccumulator, schema_digest
from .model import (
    LogicalCounts,
    LogicalNode,
    LogicalRelation,
    LogicalSchema,
    LogicalScope,
)
from .validation import LogicalSchemaIndex


LOGICAL_GRAPH_FORMAT: Final[str] = "okto-pulse-logical-graph/1"

FEATURE_VECTORS: Final[str] = "vectors"
SUPPORTED_FEATURES: Final[frozenset[str]] = frozenset({FEATURE_VECTORS})

RECORD_HEADER: Final[str] = "header"
RECORD_NODE: Final[str] = "node"
RECORD_RELATION: Final[str] = "relation"
RECORD_MANIFEST: Final[str] = "manifest"

HEADER_KEYS: Final[tuple[str, ...]] = (
    "counts",
    "features",
    "format",
    "record",
    "schema",
    "schema_digest",
    "scope",
)
FEATURE_KEYS: Final[tuple[str, ...]] = ("optional", "required")
MANIFEST_KEYS: Final[tuple[str, ...]] = (
    "counts",
    "fingerprint",
    "record",
    "stream_checksum",
)


@dataclass(frozen=True, slots=True)
class LogicalArtifactHeader:
    """The opening record: schema, features and the declared census."""

    scope: LogicalScope
    schema: LogicalSchema
    counts: LogicalCounts
    required_features: tuple[str, ...] = ()
    optional_features: tuple[str, ...] = ()
    schema_digest: str = ""


@dataclass(frozen=True, slots=True)
class LogicalArtifactManifest:
    """The terminal record: the census and checksums actually produced."""

    counts: LogicalCounts
    fingerprint: str
    stream_checksum: str


@dataclass(frozen=True, slots=True)
class LogicalArtifact:
    """A fully materialized artifact, for callers that can hold one."""

    header: LogicalArtifactHeader
    nodes: tuple[LogicalNode, ...] = ()
    relations: tuple[LogicalRelation, ...] = ()
    manifest: LogicalArtifactManifest | None = None


@dataclass(frozen=True, slots=True)
class ArtifactEvent:
    """One decoded record, in the order the stream carried it."""

    kind: str
    header: LogicalArtifactHeader | None = None
    node: LogicalNode | None = None
    relation: LogicalRelation | None = None
    manifest: LogicalArtifactManifest | None = None


def required_features_for(schema: LogicalSchema) -> tuple[str, ...]:
    """Derive the features a reader must implement to read this schema.

    Derived from the schema rather than from the records so the header can be
    written before the first record is seen.
    """

    if schema.vector_spaces:
        return (FEATURE_VECTORS,)
    return ()


def encode_artifact(
    schema: LogicalSchema,
    nodes: Iterable[LogicalNode],
    relations: Iterable[LogicalRelation],
    *,
    counts: LogicalCounts,
    optional_features: Iterable[str] = (),
) -> Iterator[str]:
    """Yield the artifact one line at a time.

    ``counts`` is the census the source already knows; it is written into the
    header so a reader can detect divergence at the first record rather than
    only at the manifest.  The encoder still verifies it against what it
    actually emitted, because a header that disagrees with its own body would
    otherwise become somebody else's corruption report.
    """

    index = LogicalSchemaIndex.build(schema)
    digest = schema_digest(schema)
    header = {
        "counts": counts.as_mapping(),
        "features": {
            "optional": sorted(set(optional_features)),
            "required": list(required_features_for(schema)),
        },
        "format": LOGICAL_GRAPH_FORMAT,
        "record": RECORD_HEADER,
        "schema": encode_schema(schema),
        "schema_digest": digest,
        "scope": schema.scope,
    }
    checksum = hashlib.sha256()
    accumulator = LogicalFingerprintAccumulator(schema_hex=digest)

    line = canonical_json(header)
    _absorb(checksum, line)
    yield line

    for node in nodes:
        # Validated before it is written: an artifact whose records contradict
        # its own header schema is internally consistent and semantically
        # invalid, and every later check would agree with it.
        index.validate_node(node)
        payload = {"record": RECORD_NODE, **encode_node(node)}
        line = canonical_json(payload)
        _absorb(checksum, line)
        accumulator.add_node(node)
        yield line

    for relation in relations:
        index.validate_relation(relation)
        payload = {"record": RECORD_RELATION, **encode_relation(relation)}
        line = canonical_json(payload)
        _absorb(checksum, line)
        accumulator.add_relation(relation)
        yield line

    observed = accumulator.counts()
    if observed != counts:
        raise ArtifactIntegrityError(
            "the header census does not match what the encoder emitted",
            detail=f"declared={counts.as_mapping()} observed={observed.as_mapping()}",
        )
    manifest = {
        "counts": observed.as_mapping(),
        "fingerprint": accumulator.digest(),
        "record": RECORD_MANIFEST,
        "stream_checksum": checksum.hexdigest(),
    }
    yield canonical_json(manifest)


def _absorb(checksum: "hashlib._Hash", line: str) -> None:
    checksum.update(line.encode("utf-8"))
    checksum.update(b"\n")


@dataclass(slots=True)
class _DecodeState:
    header: LogicalArtifactHeader | None = None
    checksum: Any = field(default_factory=hashlib.sha256)
    accumulator: LogicalFingerprintAccumulator | None = None
    index: LogicalSchemaIndex | None = None
    seen_relation: bool = False
    seen_manifest: bool = False


def decode_records(lines: Iterable[str]) -> Iterator[ArtifactEvent]:
    """Decode an artifact into events, holding no more than one record.

    Consume this to the end.  Truncation is only observable once the input is
    exhausted without a manifest, so a caller that stops early has verified
    nothing.
    """

    state = _DecodeState()
    for raw in lines:
        if type(raw) is not str:
            raise ArtifactMalformedError(
                "artifact lines must be strings", detail=type(raw).__name__
            )
        if state.seen_manifest:
            # ANY further line, blank included. Stripping first and skipping
            # blanks would let a writer append after the terminal record and
            # still be read as a complete artifact.
            raise ArtifactTrailingDataError(
                "the artifact continues past its terminal manifest",
                detail=raw[:60],
            )
        line = raw
        if not line:
            raise ArtifactMalformedError(
                "the artifact carries an empty line", detail="empty line"
            )
        # loads_canonical refuses duplicate keys and any line whose bytes are
        # not exactly what this format would have produced, so a reordered or
        # re-spaced record is corruption rather than an accepted variant.
        payload = loads_canonical(line)
        kind = payload.get("record")
        if kind == RECORD_HEADER:
            yield _consume_header(state, payload, line)
        elif kind == RECORD_NODE:
            yield _consume_node(state, payload, line)
        elif kind == RECORD_RELATION:
            yield _consume_relation(state, payload, line)
        elif kind == RECORD_MANIFEST:
            yield _consume_manifest(state, payload)
        else:
            raise ArtifactMalformedError(
                "unknown record kind", detail=repr(kind)[:40]
            )
    if not state.seen_manifest:
        raise ArtifactTruncatedError(
            "the artifact ended before its terminal manifest",
            detail="no manifest record",
        )


def _consume_header(
    state: _DecodeState, payload: Mapping[str, Any], line: str
) -> ArtifactEvent:
    if state.header is not None:
        raise ArtifactSequenceError(
            "the artifact carries more than one header", detail="second header"
        )
    _require_exact_keys(payload, HEADER_KEYS, "header")
    declared_format = payload.get("format")
    if declared_format != LOGICAL_GRAPH_FORMAT:
        raise UnsupportedFormatVersionError(
            "artifact does not declare a format this build reads",
            detail=repr(declared_format)[:60],
        )
    features = payload.get("features")
    if not isinstance(features, Mapping):
        raise ArtifactMalformedError(
            "header features must be an object",
            detail=type(features).__name__,
        )
    _require_exact_keys(features, FEATURE_KEYS, "header features")
    required = _feature_list(features.get("required"), "required")
    optional = _feature_list(features.get("optional"), "optional")
    unknown = sorted(set(required) - SUPPORTED_FEATURES)
    if unknown:
        # Unknown REQUIRED features are refused, never skipped: a reader that
        # ignored one would import a graph that means something other than what
        # the writer recorded. Unknown optional features are ignored by design.
        raise UnsupportedFeatureError(
            "artifact requires features this build does not implement",
            detail=",".join(unknown),
        )
    schema = decode_schema(payload.get("schema"))
    expected_features = set(required_features_for(schema))
    if not expected_features.issubset(required):
        # A schema with vector spaces that does not require the vectors feature
        # would let a reader without vector support accept it and silently
        # import a graph whose embeddings it cannot represent.
        raise ArtifactIntegrityError(
            "the header omits a feature its own schema requires",
            detail=",".join(sorted(expected_features - set(required))),
        )
    counts = LogicalCounts.from_mapping(_require_mapping(payload.get("counts")))
    digest = schema_digest(schema)
    declared_digest = payload.get("schema_digest")
    if declared_digest != digest:
        raise ArtifactIntegrityError(
            "the header schema digest does not match its own schema",
            detail=str(declared_digest)[:64],
        )
    scope = payload.get("scope")
    if scope != schema.scope:
        raise ArtifactIntegrityError(
            "the header scope disagrees with its schema scope",
            detail=f"{scope!r} != {schema.scope!r}",
        )
    header = LogicalArtifactHeader(
        scope=schema.scope,
        schema=schema,
        counts=counts,
        required_features=tuple(required),
        optional_features=tuple(optional),
        schema_digest=digest,
    )
    state.header = header
    state.accumulator = LogicalFingerprintAccumulator(schema_hex=digest)
    state.index = LogicalSchemaIndex.build(schema)
    _absorb(state.checksum, line)
    return ArtifactEvent(kind=RECORD_HEADER, header=header)


def _feature_list(value: Any, what: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ArtifactMalformedError(
            f"header {what} features must be an array",
            detail=type(value).__name__,
        )
    names: list[str] = []
    for entry in value:
        if type(entry) is not str or not entry:
            raise ArtifactMalformedError(
                f"header {what} feature must be a non-empty string",
                detail=repr(entry)[:40],
            )
        if entry in names:
            raise ArtifactMalformedError(
                f"header {what} features repeat a name", detail=entry
            )
        names.append(entry)
    return names


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
        # Unknown fields are refused rather than ignored: this format has a
        # frozen wire shape, so an unexpected key is either a different format
        # or a corrupted record, and both deserve a refusal.
        raise ArtifactMalformedError(
            f"{what} carries unknown fields", detail=",".join(sorted(unknown))
        )


def _require_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ArtifactMalformedError(
            "header counts must be an object", detail=type(value).__name__
        )
    return value


def _require_started(state: _DecodeState, kind: str) -> LogicalFingerprintAccumulator:
    if state.header is None or state.accumulator is None or state.index is None:
        raise ArtifactSequenceError(
            f"a {kind} record arrived before the header", detail=kind
        )
    return state.accumulator


def _consume_node(
    state: _DecodeState, payload: Mapping[str, Any], line: str
) -> ArtifactEvent:
    accumulator = _require_started(state, RECORD_NODE)
    if state.seen_relation:
        # Nodes before relations is structural, not stylistic: it is what lets a
        # reader resolve an endpoint without buffering the whole artifact.
        raise ArtifactSequenceError(
            "a node record arrived after the relation section began",
            detail=RECORD_NODE,
        )
    node = decode_node(_without_record(payload))
    assert state.index is not None  # narrowed by _require_started
    state.index.validate_node(node)
    accumulator.add_node(node)
    _absorb(state.checksum, line)
    return ArtifactEvent(kind=RECORD_NODE, node=node)


def _consume_relation(
    state: _DecodeState, payload: Mapping[str, Any], line: str
) -> ArtifactEvent:
    accumulator = _require_started(state, RECORD_RELATION)
    relation = decode_relation(_without_record(payload))
    assert state.index is not None  # narrowed by _require_started
    state.index.validate_relation(relation)
    accumulator.add_relation(relation)
    state.seen_relation = True
    _absorb(state.checksum, line)
    return ArtifactEvent(kind=RECORD_RELATION, relation=relation)


def _without_record(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "record"}


def _consume_manifest(
    state: _DecodeState, payload: Mapping[str, Any]
) -> ArtifactEvent:
    accumulator = _require_started(state, RECORD_MANIFEST)
    _require_exact_keys(payload, MANIFEST_KEYS, "manifest")
    header = state.header
    assert header is not None  # narrowed by _require_started
    declared = LogicalCounts.from_mapping(_manifest_counts(payload))
    observed = accumulator.counts()
    if declared != observed:
        raise ArtifactIntegrityError(
            "the manifest census does not match the records that arrived",
            detail=(
                f"manifest={declared.as_mapping()} observed={observed.as_mapping()}"
            ),
        )
    if header.counts != observed:
        raise ArtifactIntegrityError(
            "the header census does not match the records that arrived",
            detail=(
                f"header={header.counts.as_mapping()} "
                f"observed={observed.as_mapping()}"
            ),
        )
    fingerprint = accumulator.digest()
    declared_fingerprint = payload.get("fingerprint")
    if declared_fingerprint != fingerprint:
        raise ArtifactIntegrityError(
            "the manifest fingerprint does not match the records that arrived",
            detail=str(declared_fingerprint)[:64],
        )
    declared_checksum = payload.get("stream_checksum")
    observed_checksum = state.checksum.hexdigest()
    if declared_checksum != observed_checksum:
        raise ArtifactIntegrityError(
            "the manifest stream checksum does not match the bytes that arrived",
            detail=str(declared_checksum)[:64],
        )
    manifest = LogicalArtifactManifest(
        counts=observed,
        fingerprint=fingerprint,
        stream_checksum=observed_checksum,
    )
    state.seen_manifest = True
    return ArtifactEvent(kind=RECORD_MANIFEST, manifest=manifest)


def _manifest_counts(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    counts = payload.get("counts")
    if not isinstance(counts, Mapping):
        raise ArtifactMalformedError(
            "manifest counts must be an object", detail=type(counts).__name__
        )
    return counts


def decode_artifact(lines: Iterable[str]) -> LogicalArtifact:
    """Decode an artifact in full.

    Convenience for callers that can hold the graph.  A transfer uses
    :func:`decode_records` instead, so its memory does not grow with the graph.
    """

    header: LogicalArtifactHeader | None = None
    manifest: LogicalArtifactManifest | None = None
    nodes: list[LogicalNode] = []
    relations: list[LogicalRelation] = []
    for event in decode_records(lines):
        if event.kind == RECORD_HEADER:
            header = event.header
        elif event.kind == RECORD_NODE and event.node is not None:
            nodes.append(event.node)
        elif event.kind == RECORD_RELATION and event.relation is not None:
            relations.append(event.relation)
        elif event.kind == RECORD_MANIFEST:
            manifest = event.manifest
    if header is None:
        raise ArtifactSequenceError(
            "the artifact carries no header", detail="empty artifact"
        )
    return LogicalArtifact(
        header=header,
        nodes=tuple(nodes),
        relations=tuple(relations),
        manifest=manifest,
    )


__all__ = [
    "FEATURE_VECTORS",
    "LOGICAL_GRAPH_FORMAT",
    "RECORD_HEADER",
    "RECORD_MANIFEST",
    "RECORD_NODE",
    "RECORD_RELATION",
    "SUPPORTED_FEATURES",
    "ArtifactEvent",
    "LogicalArtifact",
    "LogicalArtifactHeader",
    "LogicalArtifactManifest",
    "decode_artifact",
    "decode_records",
    "encode_artifact",
    "required_features_for",
]
