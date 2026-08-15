"""Source enumerator + immutable manifest for KG rebuild (KG-02.2).

This module implements three primitives that the rebuild flow chains
in order — preflight reads them, confirm binds them, run consumes the
manifest_ref + preflight_hash bound at confirm time:

    RebuildSourceEnumerator.enumerate(board_id) -> RebuildSourceSet
    KGRebuildSourceManifest.build(source_set, preflight_hash) -> Manifest
    KGRebuildSourceManifest.load(manifest_ref) -> Manifest (read-only)

Validator (val_efc6dd03) called out strict invariants:

* TR4: enumeration excludes ``cancelled`` specs deterministically.
* TR5: stable ordering by ``artifact_type, created_at, id, version``.
* IR ir_1959b2e1: the manifest is the ONLY owner of
  ``source_set_hash`` and ``preflight_hash`` — confirm/run consume
  these via ``manifest_ref`` rather than recomputing from raw source.
* No mutation: this module is pure orchestration + persistence of
  an immutable manifest record on disk.

The manifest is persisted as ``<base>/rebuild/manifests/<manifest_ref>.json``
so the confirm endpoint can hand the operator a reference and the run
endpoint can re-validate the same manifest before mutation.

Counter ``kg_rebuild_source_enumeration_total`` (OR ``or_2d295e26`` /
``or_01279a1c``) records ``success``, ``source_store_unavailable``,
``unsupported_schema_version`` and ``source_set_hash_mismatch``
outcomes with safe labels only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
from okto_pulse.core.runtime_context import runtime_lock, runtime_state
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterable, Mapping

from okto_pulse.core.kg.source_maturity import (
    CANONICAL_ARTIFACT_TYPES,
    DEFAULT_WORKING_TTL_DAYS,
    DISPOSITION_CANONICAL,
    DISPOSITION_LEGACY_UNKNOWN,
    DISPOSITION_SKIPPED_BY_MATURITY,
    DISPOSITION_SKIPPED_CANCELLED,
    DISPOSITION_SKIPPED_EXPIRED_WORKING,
    DISPOSITION_WORKING,
    GRAPH_LAYER_CANONICAL,
    MATURITY_CANONICAL_ELIGIBLE,
    REBUILD_ARTIFACT_TYPES,
    classify_source_for_kg,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    REBUILD_AUDIT_GLOBAL_BOARD_ID,
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import (
    resolve_rebuild_audit_artifact_store,
)

logger = logging.getLogger("okto_pulse.kg.rebuild_sources")


REBUILD_DIRNAME = "rebuild"
MANIFEST_DIRNAME = "manifests"
MANIFEST_REF_PREFIX = "rebuild_manifest_"

_LEGACY_PREDIGEST_V3_MANIFEST_KEYS = frozenset(
    {
        "board_id",
        "canonical_source_count",
        "created_at",
        "has_non_deterministic_inputs",
        "legacy_unknown",
        "legacy_unknown_count",
        "manifest_ref",
        "manifest_schema_version",
        "preflight_hash",
        "skipped_by_maturity",
        "skipped_by_maturity_count",
        "skipped_cancelled_count",
        "skipped_expired_working",
        "skipped_expired_working_count",
        "source_set_hash",
        "sources",
        "working_source_count",
        "working_sources",
    }
)
_LEGACY_PREDIGEST_V3_ROW_KEYS = frozenset(
    {
        "artifact_type",
        "content_hash",
        "created_at",
        "disposition",
        "expires_at",
        "graph_layer",
        "id",
        "maturity_status",
        "reason_code",
        "source_artifact_status",
        "source_ref",
        "source_version",
    }
)
_LEGACY_PREDIGEST_V3_PARTITIONS = (
    "sources",
    "working_sources",
    "skipped_by_maturity",
    "skipped_expired_working",
    "legacy_unknown",
)


class RebuildSourceManifestVerificationError(RuntimeError):
    """Base error for fail-closed recovery manifest loading."""


class RebuildSourceManifestNotFoundError(RebuildSourceManifestVerificationError):
    """The exact manifest reference has no durable artifact."""


class RebuildSourceManifestIntegrityError(RebuildSourceManifestVerificationError):
    """A durable manifest exists but its identity or payload is invalid."""


class RebaselineEvidenceError(RuntimeError):
    """Base error for governed, run-bound rebaseline evidence."""


class RebaselineEvidenceFenceLostError(RebaselineEvidenceError):
    """Administrative authority expired inside the durable transaction."""


class RebaselineEvidenceConflictError(RebaselineEvidenceError):
    """A deterministic evidence id is already bound to different content."""


# val_d0da4a75 rework: preflight_hash MUST be lowercase SHA256 hex
# 64 chars. Centralised validator is the single source of truth — the
# builder, the endpoint and any future caller use this.
_PREFLIGHT_HASH_PATTERN = "0123456789abcdef"


def validate_preflight_hash(value: str) -> str:
    """Validate + return ``value`` as a canonical SHA256 hex.

    Raises ``ValueError`` on anything that's not lowercase 64-char hex.
    Centralised so the API, the manifest builder and any future caller
    enforce the same shape.
    """
    if not isinstance(value, str):
        raise ValueError("preflight_hash must be a string")
    if len(value) != 64:
        raise ValueError(f"preflight_hash must be 64 chars (got {len(value)})")
    if any(c not in _PREFLIGHT_HASH_PATTERN for c in value):
        raise ValueError("preflight_hash must be lowercase hex")
    return value


def validate_manifest_ref(value: str) -> str:
    """Validate + return ``value`` as a canonical manifest_ref token.

    Rejects path traversal (``/``, ``\\``, ``..``), absolute paths, NUL
    bytes and anything outside the ``rebuild_manifest_<urlsafe>`` shape
    that ``KGRebuildSourceManifest.build`` emits. Used by ``load`` so a
    crafted ref like ``../manifests/<real>`` can't escape the dir.
    """
    if not isinstance(value, str):
        raise ValueError("manifest_ref must be a string")
    if not value.startswith(MANIFEST_REF_PREFIX):
        raise ValueError(f"manifest_ref must start with {MANIFEST_REF_PREFIX!r}")
    suffix = value[len(MANIFEST_REF_PREFIX) :]
    if not suffix:
        raise ValueError("manifest_ref suffix is empty")
    # token_urlsafe produces only [A-Za-z0-9_-]. Allow that alphabet.
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-")
    if any(c not in allowed for c in suffix):
        raise ValueError("manifest_ref contains forbidden characters")
    return value


class EnumerationOutcome(str, Enum):
    SUCCESS = "success"
    SOURCE_STORE_UNAVAILABLE = "source_store_unavailable"
    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"
    SOURCE_SET_HASH_MISMATCH = "source_set_hash_mismatch"


@dataclass(frozen=True, slots=True)
class RebuildSourceRow:
    """One row of the deterministic source set.

    Per IR ir_1959b2e1 contract: every row carries (artifact_type,
    source_ref, source_version, content_hash, created_at, id). No
    payload body — only the metadata needed to drive a deterministic
    rebuild.
    """

    artifact_type: str
    source_ref: str
    source_version: str
    content_hash: str
    created_at: str  # ISO8601 UTC
    id: str
    source_artifact_status: str = ""
    graph_layer: str = GRAPH_LAYER_CANONICAL
    maturity_status: str = MATURITY_CANONICAL_ELIGIBLE
    disposition: str = DISPOSITION_CANONICAL
    reason_code: str = ""
    expires_at: str | None = None
    # Manifest compatibility hashes are TRANSIENT live-read evidence. They are
    # intentionally absent from ``to_dict`` and persisted manifests. V1 differs
    # from V2 only for specs; V3 additionally binds current quality/RDL heads
    # into their owning roots, so ideation/refinement/spec rows carry V2.
    content_hash_v1: str = ""
    content_hash_v2: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "source_ref": self.source_ref,
            "source_version": self.source_version,
            "content_hash": self.content_hash,
            "created_at": self.created_at,
            "id": self.id,
            "source_artifact_status": self.source_artifact_status,
            "graph_layer": self.graph_layer,
            "maturity_status": self.maturity_status,
            "disposition": self.disposition,
            "reason_code": self.reason_code,
            "expires_at": self.expires_at,
        }

    def to_dict_v1(self) -> dict[str, Any]:
        """Manifest-v1-compatible projection: identical shape to ``to_dict``
        but carrying the v1 content hash for spec rows, so a freshly
        enumerated source set can reproduce a legacy board's stored
        ``source_set_hash`` byte-for-byte (card 5ec8c75c)."""
        d = self.to_dict()
        if self.content_hash_v1:
            d["content_hash"] = self.content_hash_v1
        return d

    def to_dict_v2(self) -> dict[str, Any]:
        """Manifest-v2-compatible projection without persisting compat data."""

        d = self.to_dict()
        if self.content_hash_v2:
            d["content_hash"] = self.content_hash_v2
        return d


@dataclass(frozen=True, slots=True)
class RebuildSourceSet:
    """Read-only set of sources for a board.

    ``has_non_deterministic_inputs`` flags rows with empty
    ``content_hash`` (legacy or fallback) so the preflight UI surfaces
    confirmation_required.
    """

    board_id: str
    sources: tuple[RebuildSourceRow, ...]
    skipped_cancelled_count: int
    has_non_deterministic_inputs: bool
    generated_at: str
    working_sources: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    skipped_by_maturity: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    skipped_expired_working: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    legacy_unknown: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    # Spec MKG-A-S1 (FR5/TR5): deterministic digest of the durable cognitive
    # source class ('cognitive_durable'). {} when the store is absent or has
    # no records for this board — in that case the source_set_hash payload is
    # byte-identical to the pre-feature composition (no rebaseline storm).
    # These records are replay-only: they NEVER enter sources/
    # materializable_sources (the consolidation enqueue path), the rebuild
    # restores them literally via replay_durable_cognitive.
    cognitive_durable_digest: dict[str, Any] = field(default_factory=dict)

    @property
    def eligible_count(self) -> int:
        return len(self.sources)

    @property
    def materializable_sources(self) -> tuple[RebuildSourceRow, ...]:
        """Sources that an explicit rebuild should materialize.

        ``sources`` are canonical-eligible rows. ``working_sources`` and
        ``skipped_by_maturity`` are still non-expired working graph rows, so
        a corruption recovery rebuild must restore them with their
        ``graph_layer=working`` metadata instead of dropping all immature
        context. Expired/legacy/cancelled rows remain excluded.
        """

        return self.sources + self.working_sources + self.skipped_by_maturity

    @property
    def canonical_source_count(self) -> int:
        return len(self.sources)

    @property
    def working_source_count(self) -> int:
        return len(self.working_sources)

    @property
    def skipped_by_maturity_count(self) -> int:
        return len(self.skipped_by_maturity)

    @property
    def skipped_expired_working_count(self) -> int:
        return len(self.skipped_expired_working)

    @property
    def legacy_unknown_count(self) -> int:
        return len(self.legacy_unknown)

    @property
    def layer_counts(self) -> dict[str, int]:
        return {
            "canonical": self.canonical_source_count,
            "working": self.working_source_count + self.skipped_by_maturity_count,
            "none": self.skipped_cancelled_count + self.legacy_unknown_count,
            "expired_working": self.skipped_expired_working_count,
        }

    @property
    def source_partition_counts(self) -> dict[str, int]:
        return {
            DISPOSITION_CANONICAL: self.canonical_source_count,
            DISPOSITION_WORKING: self.working_source_count,
            DISPOSITION_SKIPPED_BY_MATURITY: self.skipped_by_maturity_count,
            DISPOSITION_SKIPPED_EXPIRED_WORKING: self.skipped_expired_working_count,
            DISPOSITION_LEGACY_UNKNOWN: self.legacy_unknown_count,
            DISPOSITION_SKIPPED_CANCELLED: self.skipped_cancelled_count,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "sources": [s.to_dict() for s in self.sources],
            "working_sources": [s.to_dict() for s in self.working_sources],
            "skipped_by_maturity": [s.to_dict() for s in self.skipped_by_maturity],
            "skipped_expired_working": [
                s.to_dict() for s in self.skipped_expired_working
            ],
            "legacy_unknown": [s.to_dict() for s in self.legacy_unknown],
            "skipped_cancelled_count": self.skipped_cancelled_count,
            "has_non_deterministic_inputs": self.has_non_deterministic_inputs,
            "generated_at": self.generated_at,
            "eligible_count": self.eligible_count,
            "canonical_source_count": self.canonical_source_count,
            "working_source_count": self.working_source_count,
            "skipped_by_maturity_count": self.skipped_by_maturity_count,
            "skipped_expired_working_count": self.skipped_expired_working_count,
            "legacy_unknown_count": self.legacy_unknown_count,
            "layer_counts": self.layer_counts,
            "source_partition_counts": self.source_partition_counts,
            "cognitive_durable_digest": dict(self.cognitive_durable_digest),
            "cognitive_durable_count": int(
                self.cognitive_durable_digest.get("count", 0)
            ),
        }


@dataclass(frozen=True, slots=True)
class RebuildSourceManifest:
    """Immutable manifest record bound to a single preflight result."""

    manifest_ref: str
    board_id: str
    source_set_hash: str
    preflight_hash: str
    sources: tuple[RebuildSourceRow, ...]
    skipped_cancelled_count: int
    has_non_deterministic_inputs: bool
    created_at: str
    # Source manifest schema version. 1 = legacy spec hash; 2 = IR/OR-aware
    # spec hash; 3 = current quality/RDL head fingerprints bound into roots.
    # Old manifests load as 1 and are revalidated against that exact schema.
    manifest_schema_version: int = 1
    working_sources: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    skipped_by_maturity: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    skipped_expired_working: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    legacy_unknown: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    payload_digest: str = ""

    @property
    def materializable_sources(self) -> tuple[RebuildSourceRow, ...]:
        return self.sources + self.working_sources + self.skipped_by_maturity

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_ref": self.manifest_ref,
            "board_id": self.board_id,
            "source_set_hash": self.source_set_hash,
            "preflight_hash": self.preflight_hash,
            "sources": [s.to_dict() for s in self.sources],
            "working_sources": [s.to_dict() for s in self.working_sources],
            "skipped_by_maturity": [s.to_dict() for s in self.skipped_by_maturity],
            "skipped_expired_working": [
                s.to_dict() for s in self.skipped_expired_working
            ],
            "legacy_unknown": [s.to_dict() for s in self.legacy_unknown],
            "skipped_cancelled_count": self.skipped_cancelled_count,
            "has_non_deterministic_inputs": self.has_non_deterministic_inputs,
            "created_at": self.created_at,
            "manifest_schema_version": self.manifest_schema_version,
            "payload_digest": self.payload_digest,
            "canonical_source_count": len(self.sources),
            "working_source_count": len(self.working_sources),
            "skipped_by_maturity_count": len(self.skipped_by_maturity),
            "skipped_expired_working_count": len(self.skipped_expired_working),
            "legacy_unknown_count": len(self.legacy_unknown),
        }


def _manifest_payload_digest(payload: Mapping[str, Any]) -> str:
    """Bind every persisted manifest field, including the recovery cut."""

    canonical = dict(payload)
    canonical.pop("payload_digest", None)
    return hashlib.sha256(
        json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def _canonical_json_snapshot(payload: object, *, code: str) -> tuple[Any, bytes]:
    """Deep-copy one JSON value and return its canonical encoded form."""

    try:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        return json.loads(encoded), encoded
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RebuildSourceManifestIntegrityError(code) from exc


def _require_sha256(value: object, *, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _PREFLIGHT_HASH_PATTERN for character in value)
    ):
        raise RebuildSourceManifestIntegrityError(code)
    return value


def _legacy_predigest_v3_cognitive_digest(
    value: object,
) -> dict[str, Any]:
    """Validate the exact optional cognitive hash member of a v3 source set."""

    if type(value) is not dict:
        raise RebuildSourceManifestIntegrityError(
            "rebuild_source_manifest_legacy_predigest_cognitive_digest_invalid"
        )
    if not value:
        return {}
    if set(value) != {"count", "digest"}:
        raise RebuildSourceManifestIntegrityError(
            "rebuild_source_manifest_legacy_predigest_cognitive_digest_invalid"
        )
    count = value.get("count")
    if type(count) is not int or count <= 0:
        raise RebuildSourceManifestIntegrityError(
            "rebuild_source_manifest_legacy_predigest_cognitive_digest_invalid"
        )
    digest = _require_sha256(
        value.get("digest"),
        code="rebuild_source_manifest_legacy_predigest_cognitive_digest_invalid",
    )
    return {"count": count, "digest": digest}


def _legacy_predigest_v3_row(payload: object) -> RebuildSourceRow:
    if type(payload) is not dict or set(payload) != _LEGACY_PREDIGEST_V3_ROW_KEYS:
        raise RebuildSourceManifestIntegrityError(
            "rebuild_source_manifest_legacy_predigest_row_shape_invalid"
        )
    string_fields = _LEGACY_PREDIGEST_V3_ROW_KEYS - {"expires_at"}
    if any(type(payload.get(field)) is not str for field in string_fields):
        raise RebuildSourceManifestIntegrityError(
            "rebuild_source_manifest_legacy_predigest_row_type_invalid"
        )
    expires_at = payload.get("expires_at")
    if expires_at is not None and type(expires_at) is not str:
        raise RebuildSourceManifestIntegrityError(
            "rebuild_source_manifest_legacy_predigest_row_type_invalid"
        )
    required_nonempty = {
        "artifact_type",
        "content_hash",
        "created_at",
        "disposition",
        "graph_layer",
        "id",
        "maturity_status",
        "source_ref",
        "source_version",
    }
    if any(not payload[field] for field in required_nonempty):
        raise RebuildSourceManifestIntegrityError(
            "rebuild_source_manifest_legacy_predigest_row_value_invalid"
        )
    try:
        datetime.fromisoformat(payload["created_at"])
        if expires_at is not None:
            datetime.fromisoformat(expires_at)
    except ValueError as exc:
        raise RebuildSourceManifestIntegrityError(
            "rebuild_source_manifest_legacy_predigest_row_timestamp_invalid"
        ) from exc
    return RebuildSourceRow(**payload)


def _legacy_predigest_v3_payload(
    manifest: RebuildSourceManifest,
) -> dict[str, Any]:
    """Serialize exactly as the pre-envelope v3 writer did."""

    return {
        "manifest_ref": manifest.manifest_ref,
        "board_id": manifest.board_id,
        "source_set_hash": manifest.source_set_hash,
        "preflight_hash": manifest.preflight_hash,
        "sources": [source.to_dict() for source in manifest.sources],
        "working_sources": [source.to_dict() for source in manifest.working_sources],
        "skipped_by_maturity": [
            source.to_dict() for source in manifest.skipped_by_maturity
        ],
        "skipped_expired_working": [
            source.to_dict() for source in manifest.skipped_expired_working
        ],
        "legacy_unknown": [source.to_dict() for source in manifest.legacy_unknown],
        "skipped_cancelled_count": manifest.skipped_cancelled_count,
        "has_non_deterministic_inputs": manifest.has_non_deterministic_inputs,
        "created_at": manifest.created_at,
        "manifest_schema_version": manifest.manifest_schema_version,
        "canonical_source_count": len(manifest.sources),
        "working_source_count": len(manifest.working_sources),
        "skipped_by_maturity_count": len(manifest.skipped_by_maturity),
        "skipped_expired_working_count": len(manifest.skipped_expired_working),
        "legacy_unknown_count": len(manifest.legacy_unknown),
    }


# --- Counter (OR or_2d295e26 / or_01279a1c) -----------------------------------

_ENUM_LABELS = ("board_id", "outcome", "reason")
_enum_counter = runtime_state("kg.rebuild_sources.enum_counter", dict)
_enum_counter_lock = runtime_lock("kg.rebuild_sources.enum_counter")


def _bump_enum(*, board_id: str, outcome: str, reason: str) -> None:
    key = (board_id, outcome, reason or "n/a")
    with _enum_counter_lock:
        _enum_counter[key] = _enum_counter.get(key, 0) + 1


def get_enumeration_count(
    board_id: str, outcome: str, *, reason: str | None = None
) -> int:
    with _enum_counter_lock:
        total = 0
        for (b, out, rsn), value in _enum_counter.items():
            if b != board_id or out != outcome:
                continue
            if reason is not None and rsn != reason:
                continue
            total += value
        return total


def get_enumeration_samples() -> list[dict[str, Any]]:
    with _enum_counter_lock:
        return [
            {"board_id": b, "outcome": out, "reason": rsn, "count": value}
            for (b, out, rsn), value in _enum_counter.items()
        ]


def get_enumeration_counter_labels() -> tuple[str, ...]:
    return _ENUM_LABELS


def reset_enumeration_counter() -> None:
    with _enum_counter_lock:
        _enum_counter.clear()


# --- Source enumerator -------------------------------------------------------

# Adapter: source_store(board_id) -> list[dict] of raw rows. Production
# wires it to the SQL queries against specs/refinements/etc; tests
# inject in-memory lists.
SourceStore = Callable[[str], list[dict[str, Any]]]


def _row_from_raw(
    row: dict[str, Any],
    *,
    classification,
) -> RebuildSourceRow:
    source_version = str(row.get("source_version") or row.get("version") or "")
    return RebuildSourceRow(
        artifact_type=classification.artifact_type,
        source_ref=str(row.get("source_ref") or row.get("id") or ""),
        source_version=source_version,
        content_hash=str(row.get("content_hash") or ""),
        created_at=str(row.get("created_at") or ""),
        id=str(row.get("id") or ""),
        source_artifact_status=classification.artifact_status,
        graph_layer=classification.graph_layer,
        maturity_status=classification.maturity_status,
        disposition=classification.disposition,
        reason_code=classification.reason_code,
        expires_at=classification.expires_at,
        content_hash_v1=str(row.get("content_hash_v1") or ""),
        content_hash_v2=str(row.get("content_hash_v2") or ""),
    )


def _sort_source_rows(rows: list[RebuildSourceRow]) -> None:
    # Stable ordering by partition vocabulary -> created_at -> id -> version.
    artifact_rank = {t: i for i, t in enumerate(REBUILD_ARTIFACT_TYPES)}
    rows.sort(
        key=lambda r: (
            artifact_rank.get(r.artifact_type, len(REBUILD_ARTIFACT_TYPES)),
            r.created_at,
            r.id,
            r.source_version,
        )
    )


def _row_working_ttl_days(row: dict[str, Any], *, default: int) -> int:
    raw = (
        row.get("working_ttl_days")
        or row.get("kg_working_ttl_days")
        or row.get("kg_working_source_ttl_days")
        or default
    )
    try:
        ttl = int(raw)
    except (TypeError, ValueError):
        return default
    return ttl if ttl >= 0 else default


@dataclass(frozen=True, slots=True)
class RebuildSourceEnumerator:
    """Stateless enumerator.

    Per TR4: drops any row with ``status == 'cancelled'``. Per TR5:
    sorts by ``(artifact_type rank, created_at, id, source_version)``.
    The artifact_type rank uses ``CANONICAL_ARTIFACT_TYPES`` index so
    ordering is independent of dict iteration order.
    """

    source_store: SourceStore
    working_ttl_days: int = DEFAULT_WORKING_TTL_DAYS
    cognitive_digest_provider: Callable[[str], dict[str, Any]] | None = None
    now: datetime | None = None

    def enumerate(self, *, board_id: str) -> RebuildSourceSet:
        if not board_id:
            raise ValueError("board_id is required")
        try:
            raw = self.source_store(board_id)
        except Exception as exc:
            _bump_enum(
                board_id=board_id,
                outcome=EnumerationOutcome.SOURCE_STORE_UNAVAILABLE.value,
                reason="store_raised",
            )
            logger.warning(
                "kg.rebuild_sources.store_unavailable board=%s err=%s",
                board_id,
                exc,
            )
            raise

        skipped_cancelled = 0
        eligible_rows: list[RebuildSourceRow] = []
        working_rows: list[RebuildSourceRow] = []
        skipped_by_maturity_rows: list[RebuildSourceRow] = []
        skipped_expired_rows: list[RebuildSourceRow] = []
        legacy_unknown_rows: list[RebuildSourceRow] = []
        non_deterministic = False
        for row in raw:
            artifact_type = str(row.get("artifact_type") or "").strip().lower()
            status = (
                row.get("source_artifact_status")
                or row.get("artifact_status")
                or row.get("status")
                or ""
            )
            content_hash = str(row.get("content_hash") or "")
            classification = classify_source_for_kg(
                artifact_type=artifact_type,
                artifact_status=status,
                content_hash=content_hash,
                updated_at=row.get("updated_at") or row.get("created_at"),
                working_ttl_days=_row_working_ttl_days(
                    row,
                    default=self.working_ttl_days,
                ),
                now=self.now,
                has_minimal_evidence=bool(row.get("has_minimal_evidence", True)),
                # Path B amendment (spec 7ea1e4be): canonical only at done AND
                # complete lineage. Defaults True so non-amendment sources are
                # unaffected (they never carry lineage_complete).
                lineage_complete=bool(row.get("lineage_complete", True)),
            )
            if classification.disposition == DISPOSITION_SKIPPED_CANCELLED:
                skipped_cancelled += 1
                continue
            row_model = _row_from_raw(row, classification=classification)
            if classification.disposition == DISPOSITION_LEGACY_UNKNOWN:
                non_deterministic = True
                legacy_unknown_rows.append(row_model)
                continue
            if classification.disposition == DISPOSITION_CANONICAL:
                eligible_rows.append(row_model)
                continue
            if classification.disposition == DISPOSITION_WORKING:
                working_rows.append(row_model)
                continue
            if classification.disposition == DISPOSITION_SKIPPED_EXPIRED_WORKING:
                skipped_expired_rows.append(row_model)
                continue
            skipped_by_maturity_rows.append(row_model)

        for bucket in (
            eligible_rows,
            working_rows,
            skipped_by_maturity_rows,
            skipped_expired_rows,
            legacy_unknown_rows,
        ):
            _sort_source_rows(bucket)

        if legacy_unknown_rows:
            # One bounded warning per enumeration.  Large legacy boards used to
            # emit one line per row here (often hundreds of identical warnings),
            # obscuring the actual worker failure that triggered the health
            # enumeration.  Keep the full rows in ``RebuildSourceSet`` and retain
            # an exact, deterministic type+reason breakdown in structured log
            # metadata, while making log volume independent of row count.
            grouped: dict[tuple[str, str], int] = {}
            for legacy_row in legacy_unknown_rows:
                key = (
                    legacy_row.artifact_type or "unknown",
                    legacy_row.reason_code or "unknown",
                )
                grouped[key] = grouped.get(key, 0) + 1
            breakdown = [
                {
                    "artifact_type": artifact_type,
                    "reason_code": reason_code,
                    "count": count,
                }
                for (artifact_type, reason_code), count in sorted(grouped.items())
            ]
            logger.warning(
                "kg.rebuild_sources.legacy_unknown board=%s count=%d breakdown=%s",
                board_id,
                len(legacy_unknown_rows),
                json.dumps(breakdown, sort_keys=True, separators=(",", ":")),
                extra={
                    "event": "kg.rebuild_sources.legacy_unknown",
                    "board_id": board_id,
                    "legacy_unknown_count": len(legacy_unknown_rows),
                    "legacy_unknown_breakdown": breakdown,
                },
            )

        if skipped_by_maturity_rows or skipped_expired_rows:
            logger.info(
                "kg.rebuild_sources.maturity_skips board=%s canonical=%d "
                "working=%d skipped_by_maturity=%d expired_working=%d",
                board_id,
                len(eligible_rows),
                len(working_rows),
                len(skipped_by_maturity_rows),
                len(skipped_expired_rows),
            )

        _bump_enum(
            board_id=board_id,
            outcome=EnumerationOutcome.SUCCESS.value,
            reason=("non_deterministic" if non_deterministic else "deterministic"),
        )
        return RebuildSourceSet(
            board_id=board_id,
            sources=tuple(eligible_rows),
            skipped_cancelled_count=skipped_cancelled,
            has_non_deterministic_inputs=non_deterministic,
            generated_at=datetime.now(timezone.utc).isoformat(),
            working_sources=tuple(working_rows),
            skipped_by_maturity=tuple(skipped_by_maturity_rows),
            skipped_expired_working=tuple(skipped_expired_rows),
            legacy_unknown=tuple(legacy_unknown_rows),
            cognitive_durable_digest=(
                self.cognitive_digest_provider(board_id)
                if self.cognitive_digest_provider is not None
                else _cognitive_durable_digest(board_id)
            ),
        )


def cognitive_durable_digest_from_rows(
    records: Iterable[object],
) -> dict[str, Any]:
    """Hash preloaded durable cognitive rows using Core's canonical policy.

    Community recovery captures these rows in its one relational snapshot.
    Accepting mappings as well as the Core DTO keeps that snapshot payload
    edition-neutral without duplicating classification or hashing rules.
    """

    from okto_pulse.core.ports.kg_cognitive_source import (
        canonical_cognitive_source_fingerprint,
        latest_cognitive_source_records,
    )

    def value(record: object, field: str, default: object = None) -> object:
        if isinstance(record, Mapping):
            return record.get(field, default)
        return getattr(record, field, default)

    normalized: list[dict[str, object]] = []
    for record in latest_cognitive_source_records(records):
        raw_payload = value(record, "payload")
        if isinstance(raw_payload, str):
            raw_payload = json.loads(raw_payload)
        if not isinstance(raw_payload, Mapping):
            raise ValueError("cognitive source payload must be a mapping")
        raw_evidence_refs = value(record, "evidence_refs", ()) or ()
        if isinstance(raw_evidence_refs, str):
            parsed_refs = json.loads(raw_evidence_refs)
            raw_evidence_refs = (
                parsed_refs if isinstance(parsed_refs, list) else (parsed_refs,)
            )
        evidence_refs = tuple(str(ref) for ref in raw_evidence_refs)
        source_revision = int(value(record, "source_revision", 0) or 0)
        record_fingerprint = canonical_cognitive_source_fingerprint(
            board_id=str(value(record, "board_id", "") or ""),
            node_id=str(value(record, "node_id", "") or ""),
            node_type=str(value(record, "node_type", "") or ""),
            generation=int(value(record, "generation", 0) or 0),
            payload=raw_payload,
            evidence_refs=evidence_refs,
        )
        normalized.append(
            {
                "committed_at": str(value(record, "committed_at") or ""),
                "node_id": str(value(record, "node_id") or ""),
                "node_type": str(value(record, "node_type") or ""),
                "generation": int(value(record, "generation") or 0),
                "payload_hash": hashlib.sha256(
                    json.dumps(
                        dict(raw_payload),
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ).encode("utf-8")
                ).hexdigest(),
                "source_revision": source_revision,
                "record_fingerprint": record_fingerprint,
            }
        )
    if not normalized:
        return {}
    normalized.sort(
        key=lambda row: (
            str(row["committed_at"]),
            str(row["node_id"]),
            int(row["generation"]),
        )
    )
    canonical: list[dict[str, object]] = []
    for row in normalized:
        item: dict[str, object] = {
            "node_id": row["node_id"],
            "node_type": row["node_type"],
            "generation": row["generation"],
            "payload_hash": row["payload_hash"],
        }
        # Base-only databases must retain the exact pre-ledger digest. Once a
        # real append-only revision exists, bind both its ordinal and its
        # evidence-aware semantic fingerprint into the manifest.
        if int(row["source_revision"]) > 0:
            item["source_revision"] = row["source_revision"]
            item["record_fingerprint"] = row["record_fingerprint"]
        canonical.append(item)
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {"count": len(canonical), "digest": digest}


def _cognitive_durable_digest(board_id: str) -> dict[str, Any]:
    """Deterministic digest of the durable cognitive class (spec MKG-A-S1 TR5).

    Returns ``{}`` when no CognitiveSourceStore is registered (feature
    absent) or when the board has no durable records — the source_set_hash
    then stays byte-identical to the pre-feature composition. A registered
    store that FAILS to read raises (fail-closed, contract api_33539a3f):
    the rebuild reports a structured enumeration error, never a silent
    partial manifest.
    """

    from okto_pulse.core.ports.kg_cognitive_source import (
        resolve_cognitive_source_store,
    )

    store = resolve_cognitive_source_store()
    if store is None:
        return {}
    from okto_pulse.core.kg.async_bridge import run_async_blocking

    records = run_async_blocking(store.enumerate(board_id))
    return cognitive_durable_digest_from_rows(records)


# --- Manifest builder + store -----------------------------------------------


def _compose_source_set_hash(source_set: RebuildSourceSet) -> str:
    """SHA256 hex 64 chars over the canonical-ordered source partition.

    Excludes ``generated_at`` and ``board_id`` from the input
    (board_id is implicit; timestamp is non-deterministic). The hash
    intentionally includes working/debt partitions so a status transition
    from working->canonical changes the manifest binding even if the
    content_hash stayed stable.
    """
    return _compose_source_set_hash_with(source_set, lambda r: r.to_dict())


def _compose_source_set_hash_v1(source_set: RebuildSourceSet) -> str:
    """v1-compatible source_set_hash (card 5ec8c75c): identical composition to
    :func:`_compose_source_set_hash` but projecting each row through
    ``to_dict_v1`` so spec rows use the v1 content hash. Reproduces a legacy
    board's stored hash byte-for-byte, which is how a schema-rebaseline is
    PROVEN distinct from real content drift."""
    return _compose_source_set_hash_with(source_set, lambda r: r.to_dict_v1())


def _compose_source_set_hash_v2(source_set: RebuildSourceSet) -> str:
    """Reproduce the exact manifest-v2 source-set hash.

    Compatibility fields never enter persisted JSON; they are consumed only
    from the fresh, transactionally captured source set.
    """

    return _compose_source_set_hash_with(source_set, lambda r: r.to_dict_v2())


def _compose_source_set_hash_with(source_set: RebuildSourceSet, project) -> str:
    payload_dict = {
        "sources": [project(s) for s in source_set.sources],
        "working_sources": [project(s) for s in source_set.working_sources],
        "skipped_by_maturity": [project(s) for s in source_set.skipped_by_maturity],
        "skipped_expired_working": [
            project(s) for s in source_set.skipped_expired_working
        ],
        "legacy_unknown": [project(s) for s in source_set.legacy_unknown],
        "skipped_cancelled_count": source_set.skipped_cancelled_count,
        "source_partition_counts": source_set.source_partition_counts,
    }
    # Spec MKG-A-S1 (TR5): the durable cognitive class binds into the hash
    # ONLY when records exist — boards without durable records keep their
    # pre-feature hash byte-for-byte (v1 reproduction contract preserved).
    if source_set.cognitive_durable_digest.get("count"):
        payload_dict["cognitive_durable"] = dict(source_set.cognitive_durable_digest)
    payload = json.dumps(
        payload_dict,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class SourceSetRevalidation(str, Enum):
    """Typed outcome of revalidating a live source set against a stored
    manifest (card 5ec8c75c / dec_c8e418e7)."""

    EQUIVALENT = "equivalent"
    REBASELINE = "rebaseline"
    MANIFEST_DRIFT = "manifest_drift"


@dataclass(frozen=True, slots=True)
class RevalidationResult:
    outcome: SourceSetRevalidation
    rebaselined_source_refs: tuple[str, ...] = ()
    from_manifest_schema_version: int = 0
    to_manifest_schema_version: int = 0
    to_source_set_hash: str = ""
    hash_fields_v1: tuple[str, ...] = ()
    hash_fields_v2: tuple[str, ...] = ()
    hash_fields_v3: tuple[str, ...] = ()

    @property
    def is_drift(self) -> bool:
        return self.outcome is SourceSetRevalidation.MANIFEST_DRIFT

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome.value,
            "rebaselined_source_refs": list(self.rebaselined_source_refs),
            "from_manifest_schema_version": self.from_manifest_schema_version,
            "to_manifest_schema_version": self.to_manifest_schema_version,
            "to_source_set_hash": self.to_source_set_hash,
            "hash_fields_v1": list(self.hash_fields_v1),
            "hash_fields_v2": list(self.hash_fields_v2),
            "hash_fields_v3": list(self.hash_fields_v3),
        }


# Counter OR or_b9c33b77 — kg_spec_source_manifest_rebaseline_total. Bounded
# labels (board_id, outcome); one sample per spec-manifest rebaseline event.
_REBASELINE_LABELS = ("board_id", "outcome")
_rebaseline_counter = runtime_state("kg.rebuild_sources.rebaseline_counter", dict)
_rebaseline_lock = runtime_lock("kg.rebuild_sources.rebaseline_counter")


def _bump_rebaseline(*, board_id: str, outcome: str = "rebaseline") -> None:
    with _rebaseline_lock:
        key = (board_id, outcome)
        _rebaseline_counter[key] = _rebaseline_counter.get(key, 0) + 1


def get_spec_manifest_rebaseline_count(
    board_id: str, *, outcome: str = "rebaseline"
) -> int:
    with _rebaseline_lock:
        return _rebaseline_counter.get((board_id, outcome), 0)


def get_spec_manifest_rebaseline_labels() -> tuple[str, ...]:
    return _REBASELINE_LABELS


def reset_spec_manifest_rebaseline_counter() -> None:
    with _rebaseline_lock:
        _rebaseline_counter.clear()


# FR7 (card 5ec8c75c): a FORMAL, persisted, queryable rebaseline audit record
# (not just a textual log) — per board, append-only JSONL under the rebuild
# dir. Each record carries from/to manifest schema version, the spec hash
# fields considered, and the rebaselined source_refs.
REBASELINE_AUDIT_DIRNAME = "rebaseline_audit"
REBASELINE_AUDIT_ARTIFACT_ID = "records"


def _rebaseline_audit_key(board_id: str) -> RebuildAuditKey:
    return RebuildAuditKey(
        namespace="rebaseline_audit",
        board_id=board_id,
        artifact_id=REBASELINE_AUDIT_ARTIFACT_ID,
    )


def _append_spec_manifest_rebaseline_audit(
    base_dir: object | None,
    *,
    board_id: str,
    manifest_ref: str,
    result: "RevalidationResult",
    recorded_at: str,
    artifact_store: RebuildAuditArtifactStore | None = None,
    evidence_id: str | None = None,
    fence_valid: Callable[[], bool] | None = None,
) -> bool:
    """Append one rebaseline record and report whether it was newly durable.

    ``evidence_id`` enables the recovery service's exactly-once, run-bound
    evidence path.  Its fence predicate is deliberately evaluated *inside*
    the artifact store's serialized transformer: a writer that waited behind
    board erasure cannot recreate this board-scoped audit after its
    reservation or graph-writer lease expired.

    The optional arguments preserve the historical helper contract used by
    non-recovery callers: without an evidence id each invocation appends one
    independent record.
    """
    record = {
        "board_id": board_id,
        "manifest_ref": manifest_ref,
        "recorded_at": recorded_at,
        **result.to_dict(),
    }
    if evidence_id is not None:
        if not isinstance(evidence_id, str) or not evidence_id:
            raise ValueError("rebaseline evidence_id must be non-empty")
        record["evidence_id"] = evidence_id
    resolved_store = resolve_rebuild_audit_artifact_store(
        base_dir=base_dir,
        artifact_store=artifact_store,
    )
    key = _rebaseline_audit_key(board_id)
    appended = False

    def _append(current: dict[str, Any] | None) -> dict[str, Any]:
        nonlocal appended
        if fence_valid is not None and not fence_valid():
            raise RebaselineEvidenceFenceLostError("rebaseline_audit_fence_lost")
        records = []
        if current and isinstance(current.get("records"), list):
            records = list(current["records"])
        if evidence_id is not None:
            existing = [
                item
                for item in records
                if isinstance(item, Mapping) and item.get("evidence_id") == evidence_id
            ]
            if existing:
                expected = {
                    key: value for key, value in record.items() if key != "recorded_at"
                }
                observed = {
                    key: value
                    for key, value in dict(existing[0]).items()
                    if key != "recorded_at"
                }
                if len(existing) != 1 or observed != expected:
                    raise RebaselineEvidenceConflictError(
                        "rebaseline_audit_evidence_conflict"
                    )
                return current or {
                    "board_id": board_id,
                    "artifact_id": REBASELINE_AUDIT_ARTIFACT_ID,
                    "updated_at": str(existing[0].get("recorded_at") or recorded_at),
                    "records": records,
                }
        records.append(record)
        appended = True
        return {
            "board_id": board_id,
            "artifact_id": REBASELINE_AUDIT_ARTIFACT_ID,
            "updated_at": recorded_at,
            "records": records,
        }

    resolved_store.replace_json(key, _append)
    return appended


def read_spec_manifest_rebaseline_audit(
    base_dir: object | None,
    board_id: str,
    *,
    artifact_store: RebuildAuditArtifactStore | None = None,
) -> list[dict[str, Any]]:
    """Read back the persisted spec-manifest rebaseline records for a board
    (FR7 audit evidence — queryable from the rebuild artifacts)."""
    resolved_store = resolve_rebuild_audit_artifact_store(
        base_dir=base_dir,
        artifact_store=artifact_store,
    )
    payload = resolved_store.read_json(_rebaseline_audit_key(board_id))
    if not payload:
        return []
    records = payload.get("records")
    if not isinstance(records, list):
        return []
    return [dict(record) for record in records if isinstance(record, dict)]


@dataclass(frozen=True, slots=True)
class KGRebuildSourceManifest:
    """Sole owner of source_set_hash and preflight_hash binding.

    ``build(source_set, preflight_hash)`` writes the immutable JSON
    manifest to disk and returns the in-memory record. ``load(ref)``
    reads it back. Confirm + run consume via these only.

    ``base_dir`` is the same KG storage root used by quarantine — the
    rebuild artifacts live under ``<base>/rebuild/manifests/``.
    """

    base_dir: object | None = None
    artifact_store: RebuildAuditArtifactStore | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    @staticmethod
    def _manifest_key(manifest_ref: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="source_manifest",
            board_id=REBUILD_AUDIT_GLOBAL_BOARD_ID,
            artifact_id=manifest_ref,
        )

    def build(
        self,
        *,
        source_set: RebuildSourceSet,
        preflight_hash: str,
    ) -> RebuildSourceManifest:
        if not preflight_hash:
            raise ValueError("preflight_hash is required to bind a manifest")
        # val_d0da4a75 #3: enforce canonical sha256 hex 64 chars.
        validate_preflight_hash(preflight_hash)

        from okto_pulse.core.kg.board_source_store import (
            SPEC_SOURCE_MANIFEST_VERSION,
        )

        manifest_ref = f"{MANIFEST_REF_PREFIX}{secrets.token_urlsafe(16)}"
        source_set_hash = _compose_source_set_hash(source_set)
        manifest = RebuildSourceManifest(
            manifest_ref=manifest_ref,
            board_id=source_set.board_id,
            source_set_hash=source_set_hash,
            preflight_hash=preflight_hash,
            sources=source_set.sources,
            skipped_cancelled_count=source_set.skipped_cancelled_count,
            has_non_deterministic_inputs=source_set.has_non_deterministic_inputs,
            created_at=datetime.now(timezone.utc).isoformat(),
            manifest_schema_version=SPEC_SOURCE_MANIFEST_VERSION,
            working_sources=source_set.working_sources,
            skipped_by_maturity=source_set.skipped_by_maturity,
            skipped_expired_working=source_set.skipped_expired_working,
            legacy_unknown=source_set.legacy_unknown,
        )
        manifest = replace(
            manifest,
            payload_digest=_manifest_payload_digest(manifest.to_dict()),
        )

        self.artifact_store.write_json_atomic(
            self._manifest_key(manifest_ref), manifest.to_dict()
        )
        logger.info(
            "kg.rebuild_sources.manifest_built ref=%s board=%s "
            "source_set_hash=%s preflight_hash=%s eligible=%d",
            manifest_ref,
            source_set.board_id,
            source_set_hash[:12],
            preflight_hash[:12],
            len(source_set.sources),
        )
        return manifest

    def validate_integrity(
        self,
        *,
        manifest: RebuildSourceManifest,
        expected_manifest_ref: str,
        expected_board_id: str,
        expected_preflight_hash: str,
        cognitive_durable_digest: Mapping[str, Any] | None = None,
    ) -> bool:
        """Verify identity fields and recompute the hash from stored rows.

        A declared ``source_set_hash`` is not authority by itself. Rebuild must
        prove that the exact persisted partitions still compose that digest
        before the manifest can authorize a queue or confirmation receipt.
        """

        from okto_pulse.core.kg.board_source_store import (
            SPEC_SOURCE_MANIFEST_VERSION,
        )

        if manifest.manifest_ref != expected_manifest_ref:
            return False
        if manifest.board_id != expected_board_id:
            return False
        if manifest.preflight_hash != expected_preflight_hash:
            return False
        if manifest.manifest_schema_version not in (
            1,
            2,
            SPEC_SOURCE_MANIFEST_VERSION,
        ):
            return False
        # Current manifests require the canonical envelope digest. Legacy v1
        # and v2 artifacts predate it; when present it is still verified, and
        # when absent their exact persisted partitions remain bound by the
        # historical source_set_hash below. The live compatibility projection
        # is independently proved by ``classify_revalidation`` before use.
        if manifest.payload_digest and not secrets.compare_digest(
            manifest.payload_digest, _manifest_payload_digest(manifest.to_dict())
        ):
            return False
        if (
            manifest.manifest_schema_version == SPEC_SOURCE_MANIFEST_VERSION
            and not manifest.payload_digest
        ):
            return False
        try:
            created_at = datetime.fromisoformat(manifest.created_at)
        except (TypeError, ValueError):
            return False
        if created_at.tzinfo is None:
            return False
        if manifest.has_non_deterministic_inputs != bool(manifest.legacy_unknown):
            return False
        reconstructed = RebuildSourceSet(
            board_id=manifest.board_id,
            sources=manifest.sources,
            skipped_cancelled_count=manifest.skipped_cancelled_count,
            has_non_deterministic_inputs=manifest.has_non_deterministic_inputs,
            generated_at=manifest.created_at,
            working_sources=manifest.working_sources,
            skipped_by_maturity=manifest.skipped_by_maturity,
            skipped_expired_working=manifest.skipped_expired_working,
            legacy_unknown=manifest.legacy_unknown,
            cognitive_durable_digest=(
                dict(cognitive_durable_digest or {})
                if manifest.manifest_schema_version == SPEC_SOURCE_MANIFEST_VERSION
                else {}
            ),
        )
        return _compose_source_set_hash(reconstructed) == manifest.source_set_hash

    def load(self, manifest_ref: str) -> RebuildSourceManifest | None:
        # val_d0da4a75 #2: reject path-traversal / alias attempts.
        try:
            validate_manifest_ref(manifest_ref)
        except ValueError:
            return None
        data = self.artifact_store.read_json(self._manifest_key(manifest_ref))
        if data is None:
            return None
        try:

            def _rows(key: str) -> tuple[RebuildSourceRow, ...]:
                return tuple(
                    RebuildSourceRow(
                        artifact_type=str(s["artifact_type"]),
                        source_ref=str(s["source_ref"]),
                        source_version=str(s["source_version"]),
                        content_hash=str(s["content_hash"]),
                        created_at=str(s["created_at"]),
                        id=str(s["id"]),
                        source_artifact_status=str(s.get("source_artifact_status", "")),
                        graph_layer=str(s.get("graph_layer", GRAPH_LAYER_CANONICAL)),
                        maturity_status=str(
                            s.get("maturity_status", MATURITY_CANONICAL_ELIGIBLE)
                        ),
                        disposition=str(s.get("disposition", DISPOSITION_CANONICAL)),
                        reason_code=str(s.get("reason_code", "")),
                        expires_at=(
                            str(s["expires_at"])
                            if s.get("expires_at") is not None
                            else None
                        ),
                    )
                    for s in data.get(key, [])
                )

            sources = tuple(
                RebuildSourceRow(
                    artifact_type=str(s["artifact_type"]),
                    source_ref=str(s["source_ref"]),
                    source_version=str(s["source_version"]),
                    content_hash=str(s["content_hash"]),
                    created_at=str(s["created_at"]),
                    id=str(s["id"]),
                    source_artifact_status=str(s.get("source_artifact_status", "")),
                    graph_layer=str(s.get("graph_layer", GRAPH_LAYER_CANONICAL)),
                    maturity_status=str(
                        s.get("maturity_status", MATURITY_CANONICAL_ELIGIBLE)
                    ),
                    disposition=str(s.get("disposition", DISPOSITION_CANONICAL)),
                    reason_code=str(s.get("reason_code", "")),
                    expires_at=(
                        str(s["expires_at"])
                        if s.get("expires_at") is not None
                        else None
                    ),
                )
                for s in data["sources"]
            )
            return RebuildSourceManifest(
                manifest_ref=str(data["manifest_ref"]),
                board_id=str(data["board_id"]),
                source_set_hash=str(data["source_set_hash"]),
                preflight_hash=str(data["preflight_hash"]),
                sources=sources,
                skipped_cancelled_count=int(data.get("skipped_cancelled_count", 0)),
                has_non_deterministic_inputs=bool(
                    data.get("has_non_deterministic_inputs", False)
                ),
                created_at=str(data["created_at"]),
                manifest_schema_version=int(data.get("manifest_schema_version", 1)),
                working_sources=_rows("working_sources"),
                skipped_by_maturity=_rows("skipped_by_maturity"),
                skipped_expired_working=_rows("skipped_expired_working"),
                legacy_unknown=_rows("legacy_unknown"),
                payload_digest=str(data.get("payload_digest") or ""),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def load_verified(
        self,
        manifest_ref: str,
        *,
        expected_board_id: str,
        expected_preflight_hash: str,
        cognitive_digest: Mapping[str, Any] | None = None,
    ) -> RebuildSourceManifest:
        """Load one manifest and prove its complete recovery binding.

        Missing storage and corrupt/tampered storage are intentionally distinct
        typed failures so the offline executor can stop without guessing.  The
        verification includes the canonical payload digest (therefore the cut
        timestamp and nondeterministic-input flag), identity fields, schema,
        every persisted source partition, and the cognitive durable digest.
        """

        try:
            validate_manifest_ref(manifest_ref)
        except ValueError as exc:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_ref_invalid"
            ) from exc
        key = self._manifest_key(manifest_ref)
        if not self.artifact_store.exists(key):
            raise RebuildSourceManifestNotFoundError(
                "rebuild_source_manifest_not_found"
            )
        manifest = self.load(manifest_ref)
        if manifest is None:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_payload_invalid"
            )
        if not self.validate_integrity(
            manifest=manifest,
            expected_manifest_ref=manifest_ref,
            expected_board_id=expected_board_id,
            expected_preflight_hash=expected_preflight_hash,
            cognitive_durable_digest=cognitive_digest,
        ):
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_integrity_invalid"
            )
        return manifest

    def load_verified_legacy_predigest_v3(
        self,
        manifest_ref: str,
        *,
        expected_board_id: str,
        expected_preflight_hash: str,
        expected_canonical_payload_sha256: str,
        cognitive_digest: dict[str, Any],
    ) -> RebuildSourceManifest:
        """Verify the exact v3 serializer emitted before envelope digests.

        This is a recovery-only compatibility seam.  It deliberately does not
        relax :meth:`load_verified`: current v3 manifests must still carry
        ``payload_digest``.  The caller must bind the canonical JSON snapshot
        it inspected and supply the durable cognitive digest from the original
        manifest cut; both are required to reproduce ``source_set_hash``.
        """

        try:
            validate_manifest_ref(manifest_ref)
            validate_preflight_hash(expected_preflight_hash)
        except ValueError as exc:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_identity_invalid"
            ) from exc
        if type(expected_board_id) is not str or not expected_board_id:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_identity_invalid"
            )
        expected_canonical_payload_sha256 = _require_sha256(
            expected_canonical_payload_sha256,
            code=("rebuild_source_manifest_legacy_predigest_canonical_digest_invalid"),
        )
        normalized_cognitive_digest = _legacy_predigest_v3_cognitive_digest(
            cognitive_digest
        )
        key = self._manifest_key(manifest_ref)
        try:
            exists = self.artifact_store.exists(key)
        except Exception as exc:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_storage_unverifiable"
            ) from exc
        if not exists:
            raise RebuildSourceManifestNotFoundError(
                "rebuild_source_manifest_legacy_predigest_not_found"
            )
        try:
            raw_payload = self.artifact_store.read_json(key)
        except Exception as exc:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_storage_unverifiable"
            ) from exc
        payload, canonical_payload = _canonical_json_snapshot(
            raw_payload,
            code="rebuild_source_manifest_legacy_predigest_payload_invalid",
        )
        if type(payload) is not dict or set(payload) != (
            _LEGACY_PREDIGEST_V3_MANIFEST_KEYS
        ):
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_shape_invalid"
            )
        if not secrets.compare_digest(
            hashlib.sha256(canonical_payload).hexdigest(),
            expected_canonical_payload_sha256,
        ):
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_canonical_digest_mismatch"
            )
        if (
            type(payload.get("manifest_ref")) is not str
            or payload["manifest_ref"] != manifest_ref
            or type(payload.get("board_id")) is not str
            or payload["board_id"] != expected_board_id
            or type(payload.get("preflight_hash")) is not str
            or payload["preflight_hash"] != expected_preflight_hash
            or type(payload.get("manifest_schema_version")) is not int
            or payload["manifest_schema_version"] != 3
            or type(payload.get("created_at")) is not str
            or type(payload.get("has_non_deterministic_inputs")) is not bool
            or type(payload.get("skipped_cancelled_count")) is not int
            or payload["skipped_cancelled_count"] < 0
        ):
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_identity_invalid"
            )
        _require_sha256(
            payload.get("source_set_hash"),
            code="rebuild_source_manifest_legacy_predigest_source_hash_invalid",
        )
        try:
            created_at = datetime.fromisoformat(payload["created_at"])
        except ValueError as exc:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_timestamp_invalid"
            ) from exc
        if created_at.tzinfo is None:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_timestamp_invalid"
            )

        partitions: dict[str, tuple[RebuildSourceRow, ...]] = {}
        for partition in _LEGACY_PREDIGEST_V3_PARTITIONS:
            raw_rows = payload.get(partition)
            if type(raw_rows) is not list:
                raise RebuildSourceManifestIntegrityError(
                    "rebuild_source_manifest_legacy_predigest_partition_invalid"
                )
            partitions[partition] = tuple(
                _legacy_predigest_v3_row(row) for row in raw_rows
            )
        count_bindings = {
            "canonical_source_count": "sources",
            "working_source_count": "working_sources",
            "skipped_by_maturity_count": "skipped_by_maturity",
            "skipped_expired_working_count": "skipped_expired_working",
            "legacy_unknown_count": "legacy_unknown",
        }
        for count_field, partition in count_bindings.items():
            if type(payload.get(count_field)) is not int or payload[count_field] != len(
                partitions[partition]
            ):
                raise RebuildSourceManifestIntegrityError(
                    "rebuild_source_manifest_legacy_predigest_count_invalid"
                )
        if payload["has_non_deterministic_inputs"] != bool(
            partitions["legacy_unknown"]
        ):
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_nondeterministic_invalid"
            )

        manifest = RebuildSourceManifest(
            manifest_ref=payload["manifest_ref"],
            board_id=payload["board_id"],
            source_set_hash=payload["source_set_hash"],
            preflight_hash=payload["preflight_hash"],
            sources=partitions["sources"],
            skipped_cancelled_count=payload["skipped_cancelled_count"],
            has_non_deterministic_inputs=payload["has_non_deterministic_inputs"],
            created_at=payload["created_at"],
            manifest_schema_version=payload["manifest_schema_version"],
            working_sources=partitions["working_sources"],
            skipped_by_maturity=partitions["skipped_by_maturity"],
            skipped_expired_working=partitions["skipped_expired_working"],
            legacy_unknown=partitions["legacy_unknown"],
        )
        reserialized, canonical_reserialized = _canonical_json_snapshot(
            _legacy_predigest_v3_payload(manifest),
            code="rebuild_source_manifest_legacy_predigest_reserialization_invalid",
        )
        if reserialized != payload or canonical_reserialized != canonical_payload:
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_reserialization_mismatch"
            )
        reconstructed = RebuildSourceSet(
            board_id=manifest.board_id,
            sources=manifest.sources,
            skipped_cancelled_count=manifest.skipped_cancelled_count,
            has_non_deterministic_inputs=manifest.has_non_deterministic_inputs,
            generated_at=manifest.created_at,
            working_sources=manifest.working_sources,
            skipped_by_maturity=manifest.skipped_by_maturity,
            skipped_expired_working=manifest.skipped_expired_working,
            legacy_unknown=manifest.legacy_unknown,
            cognitive_durable_digest=normalized_cognitive_digest,
        )
        if not secrets.compare_digest(
            _compose_source_set_hash(reconstructed), manifest.source_set_hash
        ):
            raise RebuildSourceManifestIntegrityError(
                "rebuild_source_manifest_legacy_predigest_source_hash_mismatch"
            )
        return manifest

    def classify_revalidation(
        self,
        *,
        manifest: RebuildSourceManifest,
        current_source_set: RebuildSourceSet,
    ) -> RevalidationResult:
        """Purely classify a live source set against a stored manifest.

        This method never writes an artifact and never increments a counter.
        Recovery discovery and terminal-receipt reconciliation must use this
        seam because they run before (or intentionally avoid) governed board
        mutation authority.

        The classification has exact manifest-v1/v2/v3 compatibility:

        * EQUIVALENT — a v3 manifest matches the current v3 hash.
        * REBASELINE — a v1 or v2 manifest matches the corresponding transient
          compatibility projection byte-for-byte. The only difference is the
          governed schema upgrade to v3.
        * MANIFEST_DRIFT — the hash for the manifest's exact schema differs, or
          the schema version is unsupported. This always blocks.
        """
        from okto_pulse.core.kg.board_source_store import (
            SOURCE_PROJECTION_HASH_FIELDS_V3,
            SPEC_SOURCE_MANIFEST_VERSION,
        )

        schema_version = manifest.manifest_schema_version
        if schema_version == SPEC_SOURCE_MANIFEST_VERSION:
            if _compose_source_set_hash(current_source_set) == manifest.source_set_hash:
                return RevalidationResult(SourceSetRevalidation.EQUIVALENT)
        elif schema_version in (1, 2):
            compatibility_hash = (
                _compose_source_set_hash_v1(current_source_set)
                if schema_version == 1
                else _compose_source_set_hash_v2(current_source_set)
            )
            if compatibility_hash == manifest.source_set_hash:
                compatibility_field = (
                    "content_hash_v1" if schema_version == 1 else "content_hash_v2"
                )
                rebaselined = tuple(
                    row.source_ref
                    for partition in (
                        current_source_set.sources,
                        current_source_set.working_sources,
                        current_source_set.skipped_by_maturity,
                        current_source_set.skipped_expired_working,
                        current_source_set.legacy_unknown,
                    )
                    for row in partition
                    if (
                        getattr(row, compatibility_field)
                        and row.content_hash != getattr(row, compatibility_field)
                    )
                )
                from okto_pulse.core.kg.board_source_store import (
                    SPEC_CONTENT_COLUMNS_V1,
                    SPEC_CONTENT_COLUMNS_V2,
                )

                return RevalidationResult(
                    SourceSetRevalidation.REBASELINE,
                    rebaselined_source_refs=rebaselined,
                    from_manifest_schema_version=schema_version,
                    to_manifest_schema_version=SPEC_SOURCE_MANIFEST_VERSION,
                    to_source_set_hash=_compose_source_set_hash(current_source_set),
                    hash_fields_v1=SPEC_CONTENT_COLUMNS_V1,
                    hash_fields_v2=SPEC_CONTENT_COLUMNS_V2,
                    hash_fields_v3=SOURCE_PROJECTION_HASH_FIELDS_V3,
                )
        return RevalidationResult(SourceSetRevalidation.MANIFEST_DRIFT)

    def record_rebaseline(
        self,
        *,
        manifest: RebuildSourceManifest,
        result: RevalidationResult,
        evidence_id: str,
        fence_valid: Callable[[], bool],
        recorded_at: str | None = None,
    ) -> bool:
        """Persist exactly-once governed evidence for one recovery run.

        ``evidence_id`` is supplied by the service as a deterministic
        run+manifest binding.  An exact durable retry is a no-op; a conflicting
        record fails closed.  The counter increments only after the first
        append has become durable.
        """

        if result.outcome is not SourceSetRevalidation.REBASELINE:
            raise ValueError("only REBASELINE results may be recorded")
        if result.from_manifest_schema_version != manifest.manifest_schema_version:
            raise ValueError("rebaseline result manifest schema mismatch")
        if len(result.to_source_set_hash) != 64 or any(
            character not in "0123456789abcdef"
            for character in result.to_source_set_hash
        ):
            raise ValueError("rebaseline target source_set_hash invalid")
        appended = _append_spec_manifest_rebaseline_audit(
            self.base_dir,
            board_id=manifest.board_id,
            manifest_ref=manifest.manifest_ref,
            result=result,
            recorded_at=recorded_at or datetime.now(timezone.utc).isoformat(),
            artifact_store=self.artifact_store,
            evidence_id=evidence_id,
            fence_valid=fence_valid,
        )
        if appended:
            _bump_rebaseline(board_id=manifest.board_id)
            logger.info(
                "kg.rebuild_sources.spec_manifest_rebaseline board=%s "
                "from_version=%d to_version=%d rebaselined=%d evidence=%s",
                manifest.board_id,
                result.from_manifest_schema_version,
                result.to_manifest_schema_version,
                len(result.rebaselined_source_refs),
                evidence_id,
            )
        return appended

    def revalidate(
        self,
        *,
        manifest: RebuildSourceManifest,
        current_source_set: RebuildSourceSet,
    ) -> RevalidationResult:
        """Compatibility API that classifies and records legacy rebaseline.

        Recovery discovery and the governed rebuild service use
        :meth:`classify_revalidation` plus :meth:`record_rebaseline` instead.
        This method retains the pre-existing observable behavior for callers
        outside that lane.
        """

        result = self.classify_revalidation(
            manifest=manifest,
            current_source_set=current_source_set,
        )
        if result.outcome is SourceSetRevalidation.REBASELINE:
            appended = _append_spec_manifest_rebaseline_audit(
                self.base_dir,
                board_id=manifest.board_id,
                manifest_ref=manifest.manifest_ref,
                result=result,
                recorded_at=datetime.now(timezone.utc).isoformat(),
                artifact_store=self.artifact_store,
            )
            if appended:
                _bump_rebaseline(board_id=manifest.board_id)
                logger.info(
                    "kg.rebuild_sources.spec_manifest_rebaseline board=%s "
                    "from_version=%d to_version=%d rebaselined=%d",
                    manifest.board_id,
                    result.from_manifest_schema_version,
                    result.to_manifest_schema_version,
                    len(result.rebaselined_source_refs),
                )
        elif result.outcome is SourceSetRevalidation.MANIFEST_DRIFT:
            from okto_pulse.core.kg.board_source_store import (
                SPEC_SOURCE_MANIFEST_VERSION,
            )

            supported_versions = (1, 2, SPEC_SOURCE_MANIFEST_VERSION)
            schema_version = manifest.manifest_schema_version
            _bump_enum(
                board_id=manifest.board_id,
                outcome=(
                    EnumerationOutcome.SOURCE_SET_HASH_MISMATCH.value
                    if schema_version in supported_versions
                    else EnumerationOutcome.UNSUPPORTED_SCHEMA_VERSION.value
                ),
                reason=(
                    "manifest_drift"
                    if schema_version in supported_versions
                    else "unsupported_manifest_schema"
                ),
            )
        return result


__all__ = [
    "CANONICAL_ARTIFACT_TYPES",
    "EnumerationOutcome",
    "KGRebuildSourceManifest",
    "MANIFEST_DIRNAME",
    "MANIFEST_REF_PREFIX",
    "REBUILD_DIRNAME",
    "REBUILD_ARTIFACT_TYPES",
    "RebaselineEvidenceConflictError",
    "RebaselineEvidenceError",
    "RebaselineEvidenceFenceLostError",
    "RebuildSourceEnumerator",
    "RebuildSourceManifest",
    "RebuildSourceManifestIntegrityError",
    "RebuildSourceManifestNotFoundError",
    "RebuildSourceManifestVerificationError",
    "RebuildSourceRow",
    "RebuildSourceSet",
    "RevalidationResult",
    "SourceSetRevalidation",
    "SourceStore",
    "cognitive_durable_digest_from_rows",
    "get_enumeration_count",
    "get_enumeration_counter_labels",
    "get_enumeration_samples",
    "get_spec_manifest_rebaseline_count",
    "get_spec_manifest_rebaseline_labels",
    "read_spec_manifest_rebaseline_audit",
    "reset_enumeration_counter",
    "reset_spec_manifest_rebaseline_counter",
    "validate_manifest_ref",
    "validate_preflight_hash",
]
