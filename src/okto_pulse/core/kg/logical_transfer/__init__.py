"""Portable logical graph transfer: model, codec, fingerprint, ports, service.

This package is the Core half of M-PULSE-5.  It owns the
``okto-pulse-logical-graph/1`` wire format, the canonical value codec, the
logical fingerprint, the two neutral ports and the orchestration that moves one
snapshot into one candidate and certifies it.

It owns nothing physical.  There is no filesystem access, no path, no fsync, no
concrete backend type and no backend exception anywhere in it: the edition
adapters supply those behind :mod:`.ports`.  That boundary is what makes the
same format and the same certification apply in both transfer directions.
"""

from __future__ import annotations

from .canonical import (
    canonical_bytes,
    canonical_json,
    decode_value,
    encode_value,
)
from .codec import (
    FEATURE_VECTORS,
    LOGICAL_GRAPH_FORMAT,
    SUPPORTED_FEATURES,
    ArtifactEvent,
    LogicalArtifact,
    LogicalArtifactHeader,
    LogicalArtifactManifest,
    decode_artifact,
    decode_records,
    encode_artifact,
    required_features_for,
)
from .errors import (
    TRANSFER_PHASES,
    ArtifactIntegrityError,
    ArtifactMalformedError,
    ArtifactSequenceError,
    ArtifactTrailingDataError,
    ArtifactTruncatedError,
    CertificationRefusedError,
    LogicalArtifactError,
    LogicalFormatError,
    LogicalSchemaError,
    LogicalTransferError,
    LogicalValueError,
    TransferFailedError,
    TransferPhase,
    UnsupportedFeatureError,
    UnsupportedFormatVersionError,
)
from .fingerprint import (
    LogicalFingerprintAccumulator,
    fingerprint_graph,
    schema_digest,
)
from .model import (
    INT64_MAX,
    INT64_MIN,
    LOGICAL_NULL,
    LOGICAL_PROPERTY_TYPES,
    LOGICAL_SCOPES,
    LogicalCounts,
    LogicalNode,
    LogicalNodeType,
    LogicalNull,
    LogicalPropertyDef,
    LogicalPropertyType,
    LogicalRelation,
    LogicalRelationLayout,
    LogicalSchema,
    LogicalScope,
    LogicalTimestamp,
    LogicalValue,
    LogicalVector,
    LogicalVectorSpace,
    count_graph,
)
from .ports import (
    CandidateCertificate,
    LogicalCandidateSink,
    LogicalSnapshot,
    LogicalSnapshotSource,
)
from .service import DEFAULT_BATCH_SIZE, TransferReport, transfer_logical_graph


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "FEATURE_VECTORS",
    "INT64_MAX",
    "INT64_MIN",
    "LOGICAL_GRAPH_FORMAT",
    "LOGICAL_NULL",
    "LOGICAL_PROPERTY_TYPES",
    "LOGICAL_SCOPES",
    "SUPPORTED_FEATURES",
    "TRANSFER_PHASES",
    "ArtifactEvent",
    "ArtifactIntegrityError",
    "ArtifactMalformedError",
    "ArtifactSequenceError",
    "ArtifactTrailingDataError",
    "ArtifactTruncatedError",
    "CandidateCertificate",
    "CertificationRefusedError",
    "LogicalArtifact",
    "LogicalArtifactError",
    "LogicalArtifactHeader",
    "LogicalArtifactManifest",
    "LogicalCandidateSink",
    "LogicalCounts",
    "LogicalFingerprintAccumulator",
    "LogicalFormatError",
    "LogicalNode",
    "LogicalNodeType",
    "LogicalNull",
    "LogicalPropertyDef",
    "LogicalPropertyType",
    "LogicalRelation",
    "LogicalRelationLayout",
    "LogicalSchema",
    "LogicalSchemaError",
    "LogicalScope",
    "LogicalSnapshot",
    "LogicalSnapshotSource",
    "LogicalTimestamp",
    "LogicalTransferError",
    "LogicalValue",
    "LogicalValueError",
    "LogicalVector",
    "LogicalVectorSpace",
    "TransferFailedError",
    "TransferPhase",
    "TransferReport",
    "UnsupportedFeatureError",
    "UnsupportedFormatVersionError",
    "canonical_bytes",
    "canonical_json",
    "count_graph",
    "decode_artifact",
    "decode_records",
    "decode_value",
    "encode_artifact",
    "encode_value",
    "fingerprint_graph",
    "required_features_for",
    "schema_digest",
    "transfer_logical_graph",
]
