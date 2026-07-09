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
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable

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
        raise ValueError(
            f"preflight_hash must be 64 chars (got {len(value)})"
        )
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
        raise ValueError(
            f"manifest_ref must start with {MANIFEST_REF_PREFIX!r}"
        )
    suffix = value[len(MANIFEST_REF_PREFIX):]
    if not suffix:
        raise ValueError("manifest_ref suffix is empty")
    # token_urlsafe produces only [A-Za-z0-9_-]. Allow that alphabet.
    allowed = set(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
    )
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
    # Spec manifest v2 (card 5ec8c75c / dec_c8e418e7): the v1-compatible content
    # hash (spec rows only; "" otherwise). TRANSIENT — computed from the live
    # source set, intentionally NOT in to_dict() so both the v2 source_set_hash
    # and the persisted manifest JSON shape stay stable. Used only to PROVE a
    # legacy schema-rebaseline at revalidate time.
    content_hash_v1: str = ""

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
    # Spec source manifest schema version (card 5ec8c75c). 1 = legacy (spec
    # hash without IR/OR); 2 = current (IR/OR included). Old manifests load
    # as 1 so the first post-upgrade rebuild can rebaseline them.
    manifest_schema_version: int = 1
    working_sources: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    skipped_by_maturity: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    skipped_expired_working: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)
    legacy_unknown: tuple[RebuildSourceRow, ...] = field(default_factory=tuple)

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
            "canonical_source_count": len(self.sources),
            "working_source_count": len(self.working_sources),
            "skipped_by_maturity_count": len(self.skipped_by_maturity),
            "skipped_expired_working_count": len(self.skipped_expired_working),
            "legacy_unknown_count": len(self.legacy_unknown),
        }


# --- Counter (OR or_2d295e26 / or_01279a1c) -----------------------------------

_ENUM_LABELS = ("board_id", "outcome", "reason")
_enum_counter: dict[tuple[str, str, str], int] = {}
_enum_counter_lock = threading.Lock()


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
    source_version = str(
        row.get("source_version") or row.get("version") or ""
    )
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
                board_id, exc,
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
                logger.warning(
                    "kg.rebuild_sources.legacy_unknown board=%s type=%s reason=%s",
                    board_id, artifact_type,
                    classification.reason_code,
                )
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
        )


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


def _compose_source_set_hash_with(source_set: RebuildSourceSet, project) -> str:
    payload_dict = {
        "sources": [project(s) for s in source_set.sources],
        "working_sources": [project(s) for s in source_set.working_sources],
        "skipped_by_maturity": [
            project(s) for s in source_set.skipped_by_maturity
        ],
        "skipped_expired_working": [
            project(s) for s in source_set.skipped_expired_working
        ],
        "legacy_unknown": [project(s) for s in source_set.legacy_unknown],
        "skipped_cancelled_count": source_set.skipped_cancelled_count,
        "source_partition_counts": source_set.source_partition_counts,
    }
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
    hash_fields_v1: tuple[str, ...] = ()
    hash_fields_v2: tuple[str, ...] = ()

    @property
    def is_drift(self) -> bool:
        return self.outcome is SourceSetRevalidation.MANIFEST_DRIFT

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome.value,
            "rebaselined_source_refs": list(self.rebaselined_source_refs),
            "from_manifest_schema_version": self.from_manifest_schema_version,
            "to_manifest_schema_version": self.to_manifest_schema_version,
            "hash_fields_v1": list(self.hash_fields_v1),
            "hash_fields_v2": list(self.hash_fields_v2),
        }


# Counter OR or_b9c33b77 — kg_spec_source_manifest_rebaseline_total. Bounded
# labels (board_id, outcome); one sample per spec-manifest rebaseline event.
_REBASELINE_LABELS = ("board_id", "outcome")
_rebaseline_counter: dict[tuple[str, str], int] = {}
_rebaseline_lock = threading.Lock()


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


def _rebaseline_audit_path(base_dir: Path, board_id: str) -> Path:
    safe = "".join(
        c if (c.isalnum() or c in "_.-") else "_" for c in board_id
    ) or "board"
    return base_dir / REBUILD_DIRNAME / REBASELINE_AUDIT_DIRNAME / f"{safe}.jsonl"


def _rebaseline_audit_key(board_id: str) -> RebuildAuditKey:
    return RebuildAuditKey(
        namespace="rebaseline_audit",
        board_id=board_id,
        artifact_id=REBASELINE_AUDIT_ARTIFACT_ID,
    )


def _append_spec_manifest_rebaseline_audit(
    base_dir: Path | None,
    *,
    board_id: str,
    manifest_ref: str,
    result: "RevalidationResult",
    recorded_at: str,
    artifact_store: RebuildAuditArtifactStore | None = None,
) -> None:
    record = {
        "board_id": board_id,
        "manifest_ref": manifest_ref,
        "recorded_at": recorded_at,
        **result.to_dict(),
    }
    resolved_store = resolve_rebuild_audit_artifact_store(
        base_dir=base_dir,
        artifact_store=artifact_store,
    )
    if resolved_store is not None:
        key = _rebaseline_audit_key(board_id)

        def _append(current: dict[str, Any] | None) -> dict[str, Any]:
            records = []
            if current and isinstance(current.get("records"), list):
                records = list(current["records"])
            records.append(record)
            return {
                "board_id": board_id,
                "artifact_id": REBASELINE_AUDIT_ARTIFACT_ID,
                "updated_at": recorded_at,
                "records": records,
            }

        resolved_store.replace_json(key, _append)
        return

    if base_dir is None:
        raise RuntimeError(
            "base_dir is required when RebuildAuditArtifactStore is not supplied"
        )
    path = _rebaseline_audit_path(base_dir, board_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")


def read_spec_manifest_rebaseline_audit(
    base_dir: Path | None,
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
    if resolved_store is not None:
        payload = resolved_store.read_json(_rebaseline_audit_key(board_id))
        if not payload:
            return []
        records = payload.get("records")
        if not isinstance(records, list):
            return []
        return [dict(record) for record in records if isinstance(record, dict)]

    if base_dir is None:
        raise RuntimeError(
            "base_dir is required when RebuildAuditArtifactStore is not supplied"
        )
    path = _rebaseline_audit_path(base_dir, board_id)
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return out


@dataclass(frozen=True, slots=True)
class KGRebuildSourceManifest:
    """Sole owner of source_set_hash and preflight_hash binding.

    ``build(source_set, preflight_hash)`` writes the immutable JSON
    manifest to disk and returns the in-memory record. ``load(ref)``
    reads it back. Confirm + run consume via these only.

    ``base_dir`` is the same KG storage root used by quarantine — the
    rebuild artifacts live under ``<base>/rebuild/manifests/``.
    """

    base_dir: Path | None = None
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

    def _base_dir(self) -> Path:
        if self.base_dir is None:
            raise RuntimeError(
                "base_dir is required when RebuildAuditArtifactStore is not supplied"
            )
        return self.base_dir

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

        if self.artifact_store is not None:
            self.artifact_store.write_json_atomic(
                self._manifest_key(manifest_ref), manifest.to_dict()
            )
        else:
            base_dir = self._base_dir()
            target_dir = base_dir / REBUILD_DIRNAME / MANIFEST_DIRNAME
            target_dir.mkdir(parents=True, exist_ok=True)
            path = target_dir / f"{manifest_ref}.json"
            with path.open("w", encoding="utf-8") as fh:
                json.dump(manifest.to_dict(), fh, indent=2)
        logger.info(
            "kg.rebuild_sources.manifest_built ref=%s board=%s "
            "source_set_hash=%s preflight_hash=%s eligible=%d",
            manifest_ref, source_set.board_id,
            source_set_hash[:12], preflight_hash[:12],
            len(source_set.sources),
        )
        return manifest

    def load(self, manifest_ref: str) -> RebuildSourceManifest | None:
        # val_d0da4a75 #2: reject path-traversal / alias attempts.
        try:
            validate_manifest_ref(manifest_ref)
        except ValueError:
            return None
        if self.artifact_store is not None:
            data = self.artifact_store.read_json(self._manifest_key(manifest_ref))
            if data is None:
                return None
        else:
            base_dir = self._base_dir()
            path = base_dir / REBUILD_DIRNAME / MANIFEST_DIRNAME / f"{manifest_ref}.json"
            # Defence in depth: even with a validated ref, ensure the
            # resolved path stays under the manifests dir. Path.resolve()
            # collapses any residual ``..`` symbolic-link tricks.
            try:
                resolved = path.resolve(strict=False)
                allowed_root = (base_dir / REBUILD_DIRNAME / MANIFEST_DIRNAME).resolve(strict=False)
                resolved.relative_to(allowed_root)
            except (OSError, ValueError):
                return None
            try:
                with path.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except (FileNotFoundError, OSError, ValueError):
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
                        source_artifact_status=str(
                            s.get("source_artifact_status", "")
                        ),
                        graph_layer=str(
                            s.get("graph_layer", GRAPH_LAYER_CANONICAL)
                        ),
                        maturity_status=str(
                            s.get("maturity_status", MATURITY_CANONICAL_ELIGIBLE)
                        ),
                        disposition=str(
                            s.get("disposition", DISPOSITION_CANONICAL)
                        ),
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
                    source_artifact_status=str(
                        s.get("source_artifact_status", "")
                    ),
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
            )
        except (KeyError, TypeError, ValueError):
            return None

    def revalidate(
        self,
        *,
        manifest: RebuildSourceManifest,
        current_source_set: RebuildSourceSet,
    ) -> RevalidationResult:
        """Classify the current source set against a stored manifest — KG-02.3
        calls this before mutation (IR ir_1959b2e1 run_validation contract),
        extended for spec manifest v2 (card 5ec8c75c / dec_c8e418e7):

        * EQUIVALENT — current v2 hash matches the stored hash.
        * REBASELINE — the stored manifest is legacy (<v2) AND the
          v1-compatible hash still matches it byte-for-byte, so the ONLY
          difference is the v2 schema (IR/OR added to the spec hash). PROVEN,
          not inferred from "hash changed". Permitted by rebuild_service with
          audit + counter; never DLQ/canonical-debt.
        * MANIFEST_DRIFT — real content change (or an already-v2 manifest).
          Blocks, same as before.
        """
        from okto_pulse.core.kg.board_source_store import (
            SPEC_SOURCE_MANIFEST_VERSION,
        )

        current_hash = _compose_source_set_hash(current_source_set)
        if current_hash == manifest.source_set_hash:
            return RevalidationResult(SourceSetRevalidation.EQUIVALENT)

        if manifest.manifest_schema_version < SPEC_SOURCE_MANIFEST_VERSION:
            current_v1 = _compose_source_set_hash_v1(current_source_set)
            if current_v1 == manifest.source_set_hash:
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
                    if row.content_hash_v1
                    and row.content_hash != row.content_hash_v1
                )
                from okto_pulse.core.kg.board_source_store import (
                    SPEC_CONTENT_COLUMNS_V1,
                    SPEC_CONTENT_COLUMNS_V2,
                )

                result = RevalidationResult(
                    SourceSetRevalidation.REBASELINE,
                    rebaselined_source_refs=rebaselined,
                    from_manifest_schema_version=manifest.manifest_schema_version,
                    to_manifest_schema_version=SPEC_SOURCE_MANIFEST_VERSION,
                    hash_fields_v1=SPEC_CONTENT_COLUMNS_V1,
                    hash_fields_v2=SPEC_CONTENT_COLUMNS_V2,
                )
                _bump_rebaseline(board_id=manifest.board_id)
                _append_spec_manifest_rebaseline_audit(
                    self.base_dir,
                    board_id=manifest.board_id,
                    manifest_ref=manifest.manifest_ref,
                    result=result,
                    recorded_at=datetime.now(timezone.utc).isoformat(),
                    artifact_store=self.artifact_store,
                )
                logger.info(
                    "kg.rebuild_sources.spec_manifest_rebaseline board=%s "
                    "from_version=%d to_version=%d rebaselined=%d",
                    manifest.board_id, manifest.manifest_schema_version,
                    SPEC_SOURCE_MANIFEST_VERSION, len(rebaselined),
                )
                return result

        _bump_enum(
            board_id=manifest.board_id,
            outcome=EnumerationOutcome.SOURCE_SET_HASH_MISMATCH.value,
            reason="manifest_drift",
        )
        return RevalidationResult(SourceSetRevalidation.MANIFEST_DRIFT)


__all__ = [
    "CANONICAL_ARTIFACT_TYPES",
    "EnumerationOutcome",
    "KGRebuildSourceManifest",
    "MANIFEST_DIRNAME",
    "MANIFEST_REF_PREFIX",
    "REBUILD_DIRNAME",
    "REBUILD_ARTIFACT_TYPES",
    "RebuildSourceEnumerator",
    "RebuildSourceManifest",
    "RebuildSourceRow",
    "RebuildSourceSet",
    "RevalidationResult",
    "SourceSetRevalidation",
    "SourceStore",
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
