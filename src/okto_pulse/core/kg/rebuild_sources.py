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


# Canonical artifact_type vocabulary used for stable ordering.
# Refinements are semantic-only cognitive sources. Specs and card-derived
# task/test/bug rows are both deterministic rebuild sources and semantic
# cognitive sources. Decisions are semantic first-class sources, but their
# structural KG nodes are still materialized through the owning spec.
# Ideations are deliberately excluded from this source set.
CANONICAL_ARTIFACT_TYPES: tuple[str, ...] = (
    "spec",
    "decision",
    "refinement",
    "task",
    "test",
    "bug",
)


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "source_ref": self.source_ref,
            "source_version": self.source_version,
            "content_hash": self.content_hash,
            "created_at": self.created_at,
            "id": self.id,
        }


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

    @property
    def eligible_count(self) -> int:
        return len(self.sources)

    def to_dict(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "sources": [s.to_dict() for s in self.sources],
            "skipped_cancelled_count": self.skipped_cancelled_count,
            "has_non_deterministic_inputs": self.has_non_deterministic_inputs,
            "generated_at": self.generated_at,
            "eligible_count": self.eligible_count,
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_ref": self.manifest_ref,
            "board_id": self.board_id,
            "source_set_hash": self.source_set_hash,
            "preflight_hash": self.preflight_hash,
            "sources": [s.to_dict() for s in self.sources],
            "skipped_cancelled_count": self.skipped_cancelled_count,
            "has_non_deterministic_inputs": self.has_non_deterministic_inputs,
            "created_at": self.created_at,
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


@dataclass(frozen=True, slots=True)
class RebuildSourceEnumerator:
    """Stateless enumerator.

    Per TR4: drops any row with ``status == 'cancelled'``. Per TR5:
    sorts by ``(artifact_type rank, created_at, id, source_version)``.
    The artifact_type rank uses ``CANONICAL_ARTIFACT_TYPES`` index so
    ordering is independent of dict iteration order.
    """

    source_store: SourceStore

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
        non_deterministic = False
        for row in raw:
            status = (row.get("status") or "").lower()
            if status == "cancelled":
                skipped_cancelled += 1
                continue
            artifact_type = str(row.get("artifact_type") or "")
            if artifact_type not in CANONICAL_ARTIFACT_TYPES:
                # Unknown artifact_type — skip with audit log (no raise);
                # surfacing as non-deterministic flips the preflight UI
                # to confirmation_required.
                non_deterministic = True
                logger.warning(
                    "kg.rebuild_sources.unknown_artifact_type board=%s type=%s",
                    board_id, artifact_type,
                )
                continue
            content_hash = str(row.get("content_hash") or "")
            if not content_hash:
                non_deterministic = True
            eligible_rows.append(RebuildSourceRow(
                artifact_type=artifact_type,
                source_ref=str(row.get("source_ref") or row.get("id") or ""),
                source_version=str(row.get("source_version") or row.get("version") or ""),
                content_hash=content_hash,
                created_at=str(row.get("created_at") or ""),
                id=str(row.get("id") or ""),
            ))

        # TR5: stable ordering by artifact_type rank → created_at → id → version.
        artifact_rank = {t: i for i, t in enumerate(CANONICAL_ARTIFACT_TYPES)}
        eligible_rows.sort(
            key=lambda r: (
                artifact_rank.get(r.artifact_type, len(CANONICAL_ARTIFACT_TYPES)),
                r.created_at,
                r.id,
                r.source_version,
            )
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
        )


# --- Manifest builder + store -----------------------------------------------


def _compose_source_set_hash(source_set: RebuildSourceSet) -> str:
    """SHA256 hex 64 chars over the canonical-ordered source list.

    Excludes ``generated_at`` and ``board_id`` from the input
    (board_id is implicit; timestamp is non-deterministic). The
    manifest re-includes board_id at the wrapper level so two boards
    with the same source set still get distinct manifest_refs.
    """
    payload = json.dumps(
        [s.to_dict() for s in source_set.sources],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class KGRebuildSourceManifest:
    """Sole owner of source_set_hash and preflight_hash binding.

    ``build(source_set, preflight_hash)`` writes the immutable JSON
    manifest to disk and returns the in-memory record. ``load(ref)``
    reads it back. Confirm + run consume via these only.

    ``base_dir`` is the same KG storage root used by quarantine — the
    rebuild artifacts live under ``<base>/rebuild/manifests/``.
    """

    base_dir: Path

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
        )

        target_dir = self.base_dir / REBUILD_DIRNAME / MANIFEST_DIRNAME
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
        path = self.base_dir / REBUILD_DIRNAME / MANIFEST_DIRNAME / f"{manifest_ref}.json"
        # Defence in depth: even with a validated ref, ensure the
        # resolved path stays under the manifests dir. Path.resolve()
        # collapses any residual ``..`` symbolic-link tricks.
        try:
            resolved = path.resolve(strict=False)
            allowed_root = (self.base_dir / REBUILD_DIRNAME / MANIFEST_DIRNAME).resolve(strict=False)
            resolved.relative_to(allowed_root)
        except (OSError, ValueError):
            return None
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (FileNotFoundError, OSError, ValueError):
            return None
        try:
            sources = tuple(
                RebuildSourceRow(
                    artifact_type=str(s["artifact_type"]),
                    source_ref=str(s["source_ref"]),
                    source_version=str(s["source_version"]),
                    content_hash=str(s["content_hash"]),
                    created_at=str(s["created_at"]),
                    id=str(s["id"]),
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
            )
        except (KeyError, TypeError, ValueError):
            return None

    def revalidate(
        self,
        *,
        manifest: RebuildSourceManifest,
        current_source_set: RebuildSourceSet,
    ) -> bool:
        """Compare the manifest's source_set_hash against a freshly
        enumerated source set — KG-02.3 calls this before mutation
        (IR ir_1959b2e1 run_validation contract)."""
        current_hash = _compose_source_set_hash(current_source_set)
        if current_hash != manifest.source_set_hash:
            _bump_enum(
                board_id=manifest.board_id,
                outcome=EnumerationOutcome.SOURCE_SET_HASH_MISMATCH.value,
                reason="manifest_drift",
            )
            return False
        return True


__all__ = [
    "CANONICAL_ARTIFACT_TYPES",
    "EnumerationOutcome",
    "KGRebuildSourceManifest",
    "MANIFEST_DIRNAME",
    "MANIFEST_REF_PREFIX",
    "REBUILD_DIRNAME",
    "RebuildSourceEnumerator",
    "RebuildSourceManifest",
    "RebuildSourceRow",
    "RebuildSourceSet",
    "SourceStore",
    "get_enumeration_count",
    "get_enumeration_counter_labels",
    "get_enumeration_samples",
    "reset_enumeration_counter",
    "validate_manifest_ref",
    "validate_preflight_hash",
]
