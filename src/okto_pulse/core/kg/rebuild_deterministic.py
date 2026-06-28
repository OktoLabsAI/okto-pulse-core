"""Deterministic structural rebuild + hash comparison (KG-02.5).

KG-02.5 turns the manifest produced by KG-02.2 into a deterministic
in-memory graph materialisation and computes the canonical
``structural_hash`` consumed by the KG-02.4 promotion guard.

TR15 / BR br_10a33261 invariants:

* The hash inputs INCLUDE:
    - schema_version
    - normalised source rows (artifact_type, source_ref, source_version,
      content_hash, id)
    - reconciliation decisions (normalised)
    - normalised node payloads
    - normalised edge payloads
* The hash inputs EXCLUDE:
    - kg_generation_id, run_id
    - timestamps (started_at, finished_at, created_at, etc.)
    - random ids (uuids generated per run)
    - lock owner tokens, confirmation ids — anything operational

Two rebuilds over equivalent source sets MUST produce equivalent
hashes; any change to source content, source version, schema version or
a reconciliation decision MUST flip the hash so the promotion guard
blocks promotion (unless an operator override is recorded).

Counters (OR or_c67ee067 + or_a938f80b):

* ``kg_rebuild_structural_hash_total`` — every hash computation
  (label: ``board_id``, ``status``, ``deterministic``).
* ``kg_rebuild_structural_hash_mismatch_total`` — every comparison that
  returned non-equivalent.

This module ships the ``DeterministicStructuralRebuilder`` class that
can be wired as the ``rebuild_step_adapter`` of ``KGRebuildService``.
The KG-02.5 rebuilder is materialisation-only — it does NOT mutate
graph.lbug; KG-02.5 ships the boundary while KG-02.7 hooks the audit
event around it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger("okto_pulse.kg.rebuild_deterministic")


# Deterministic rebuild inputs. Decision source rows are intentionally absent:
# they are emitted from the owning spec payload by the existing consolidation
# worker, so adding decision rows here would duplicate KG materialization.
DETERMINISTIC_REBUILD_ARTIFACT_TYPES: frozenset[str] = frozenset({
    "story",
    "ideation",
    "refinement",
    "spec",
    "sprint",
    "task",
    "test",
    "bug",
    # Backward-compatible queue/source rows from older rebuild manifests.
    "card",
})


# Schema version pinned in the hash so a schema migration trips the
# mismatch path and blocks promotion. The module imports the constant
# lazily so test fakes (and Ladybug schema migrations) can patch it.
def _current_schema_version() -> str:
    try:
        from okto_pulse.core.kg.schema_contract import SCHEMA_VERSION

        return SCHEMA_VERSION
    except Exception:  # pragma: no cover — defensive fallback
        return "unknown"


# Fields excluded from the structural hash even if a caller surfaces
# them. The exclusion lives here (not in the caller) so a forgetful
# adapter cannot accidentally leak run-scoped state into the hash.
EXCLUDED_HASH_FIELDS: frozenset[str] = frozenset({
    "kg_generation_id",
    "previous_kg_generation_id",
    "current_kg_generation_id",
    "run_id",
    "confirmation_id",
    "manifest_ref",
    "owner_token",
    "started_at",
    "finished_at",
    "created_at",
    "updated_at",
    "promoted_at",
    "issued_at",
    "expires_at",
    "report_ref",
    "report_id",
    "audit_ref",
    "history_ref",
    "actor_id",
    "triggered_by",
})


class StructuralHashStatus(str, Enum):
    """Bounded labels for OR or_c67ee067 ``status`` field."""

    COMPUTED = "computed"
    COMPARED_EQUIVALENT = "compared_equivalent"
    COMPARED_MISMATCH = "compared_mismatch"


class HashMismatchReason(str, Enum):
    """Bounded labels for OR or_a938f80b ``reason`` field."""

    SOURCE_SET_CHANGED = "source_set_changed"
    RECONCILIATION_CHANGED = "reconciliation_changed"
    SCHEMA_VERSION_CHANGED = "schema_version_changed"
    NODES_CHANGED = "nodes_changed"
    EDGES_CHANGED = "edges_changed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class StructuralHashInputs:
    """Normalised, hash-able snapshot of a rebuild result.

    Built from the manifest + the reconciliation pipeline output. Every
    field is either a primitive, a sorted tuple of primitives, or a
    sorted tuple of dicts whose keys are predictable. Construction is
    deterministic — no datetime.now, no UUID generation.
    """

    schema_version: str
    sources: tuple[dict[str, Any], ...]
    reconciliation_decisions: tuple[dict[str, Any], ...]
    nodes: tuple[dict[str, Any], ...]
    edges: tuple[dict[str, Any], ...]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "sources": list(self.sources),
            "reconciliation_decisions": list(self.reconciliation_decisions),
            "nodes": list(self.nodes),
            "edges": list(self.edges),
        }


@dataclass(frozen=True, slots=True)
class StructuralComparison:
    """Pure comparison result between two structural hashes (or full
    inputs when the caller wants per-field drill-down)."""

    equivalent: bool
    mismatch_reasons: tuple[str, ...] = field(default_factory=tuple)
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class StructuralRebuildResult:
    """Frozen output of ``DeterministicStructuralRebuilder.rebuild``."""

    structural_hash: str
    source_hash: str
    inputs: StructuralHashInputs
    counts: dict[str, int]


# --- Counters ----------------------------------------------------------------

_HASH_LABELS = ("board_id", "status", "deterministic")
_hash_counter: dict[tuple[str, str, str], int] = {}
_hash_counter_lock = threading.Lock()


def _bump_hash(*, board_id: str, status: str, deterministic: bool) -> None:
    key = (board_id, status, "true" if deterministic else "false")
    with _hash_counter_lock:
        _hash_counter[key] = _hash_counter.get(key, 0) + 1


def get_structural_hash_count(
    board_id: str,
    *,
    status: str | None = None,
    deterministic: bool | None = None,
) -> int:
    with _hash_counter_lock:
        total = 0
        for (b, st, det), value in _hash_counter.items():
            if b != board_id:
                continue
            if status is not None and st != status:
                continue
            if deterministic is not None:
                want = "true" if deterministic else "false"
                if det != want:
                    continue
            total += value
        return total


def get_structural_hash_counter_labels() -> tuple[str, ...]:
    return _HASH_LABELS


def get_structural_hash_samples() -> list[dict[str, Any]]:
    with _hash_counter_lock:
        return [
            {
                "board_id": b,
                "status": st,
                "deterministic": det,
                "count": value,
            }
            for (b, st, det), value in _hash_counter.items()
        ]


def reset_structural_hash_counter() -> None:
    with _hash_counter_lock:
        _hash_counter.clear()


_MISMATCH_LABELS = ("board_id", "reason")
_mismatch_counter: dict[tuple[str, str], int] = {}
_mismatch_counter_lock = threading.Lock()


def _bump_mismatch(*, board_id: str, reason: str) -> None:
    key = (board_id, reason)
    with _mismatch_counter_lock:
        _mismatch_counter[key] = _mismatch_counter.get(key, 0) + 1


def get_structural_hash_mismatch_count(
    board_id: str, *, reason: str | None = None
) -> int:
    with _mismatch_counter_lock:
        total = 0
        for (b, rsn), value in _mismatch_counter.items():
            if b != board_id:
                continue
            if reason is not None and rsn != reason:
                continue
            total += value
        return total


def get_structural_hash_mismatch_counter_labels() -> tuple[str, ...]:
    return _MISMATCH_LABELS


def get_structural_hash_mismatch_samples() -> list[dict[str, Any]]:
    with _mismatch_counter_lock:
        return [
            {"board_id": b, "reason": rsn, "count": value}
            for (b, rsn), value in _mismatch_counter.items()
        ]


def reset_structural_hash_mismatch_counter() -> None:
    with _mismatch_counter_lock:
        _mismatch_counter.clear()


# --- Normalisation helpers -------------------------------------------------


def _strip_excluded(payload: Any) -> Any:
    """Recursively drop keys in ``EXCLUDED_HASH_FIELDS`` from the
    payload. Preserves order otherwise so the canonical serializer can
    work with a stable shape."""

    if isinstance(payload, Mapping):
        cleaned: dict[str, Any] = {}
        for key in sorted(payload.keys()):
            if key in EXCLUDED_HASH_FIELDS:
                continue
            cleaned[key] = _strip_excluded(payload[key])
        return cleaned
    if isinstance(payload, (list, tuple)):
        return [_strip_excluded(item) for item in payload]
    return payload


def _normalise_source(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return only the 5 hash-load-bearing fields from a source row.

    ``created_at`` and any operational state are excluded per TR15.
    """

    return {
        "artifact_type": row.get("artifact_type", ""),
        "source_ref": row.get("source_ref", ""),
        "source_version": row.get("source_version", ""),
        "content_hash": row.get("content_hash", ""),
        "id": row.get("id", ""),
    }


def _normalise_decision(decision: Mapping[str, Any]) -> dict[str, Any]:
    """Strip excluded keys, keep everything else. Decisions are pure
    business outcomes — the hash MUST capture them."""

    return _strip_excluded(decision)


def _normalise_node(node: Mapping[str, Any]) -> dict[str, Any]:
    return _strip_excluded(node)


def _normalise_edge(edge: Mapping[str, Any]) -> dict[str, Any]:
    return _strip_excluded(edge)


def _sort_key_source(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("artifact_type", "")),
        str(row.get("id", "")),
        str(row.get("source_version", "")),
    )


def _sort_key_decision(decision: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(decision.get("kind", "")),
        str(decision.get("node", decision.get("subject", ""))),
    )


def _sort_key_node(node: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(node.get("type", "")),
        str(node.get("id", node.get("key", ""))),
    )


def _sort_key_edge(edge: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(edge.get("type", edge.get("relation", ""))),
        str(edge.get("from", "")),
        str(edge.get("to", "")),
    )


def _canonical_json(payload: Any) -> str:
    """JSON serializer with sorted keys + tight separators. Used as the
    pre-image of the SHA256 hash so any pair of inputs that serialise
    to the same bytes produce the same hash."""

    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def compute_structural_hash(inputs: StructuralHashInputs) -> str:
    """Return the SHA256 hex digest of the canonical pre-image."""

    canonical = _canonical_json(inputs.to_canonical_dict())
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_source_hash(sources: Sequence[Mapping[str, Any]]) -> str:
    """Hash JUST the normalised source rows. Surfaced separately so the
    promotion guard can detect a source-only change distinct from a
    decision-only change."""

    normalised = sorted(
        (_normalise_source(row) for row in sources),
        key=_sort_key_source,
    )
    return hashlib.sha256(
        _canonical_json(normalised).encode("utf-8")
    ).hexdigest()


def build_structural_inputs(
    *,
    sources: Iterable[Mapping[str, Any]],
    reconciliation_decisions: Iterable[Mapping[str, Any]] = (),
    nodes: Iterable[Mapping[str, Any]] = (),
    edges: Iterable[Mapping[str, Any]] = (),
    schema_version: str | None = None,
) -> StructuralHashInputs:
    """Public constructor for the hash inputs. Performs normalisation
    + canonical sort so two callers passing equivalent inputs in
    different orders still produce identical bytes."""

    sorted_sources = tuple(
        sorted(
            (_normalise_source(row) for row in sources),
            key=_sort_key_source,
        )
    )
    sorted_decisions = tuple(
        sorted(
            (_normalise_decision(d) for d in reconciliation_decisions),
            key=_sort_key_decision,
        )
    )
    sorted_nodes = tuple(
        sorted(
            (_normalise_node(n) for n in nodes),
            key=_sort_key_node,
        )
    )
    sorted_edges = tuple(
        sorted(
            (_normalise_edge(e) for e in edges),
            key=_sort_key_edge,
        )
    )
    return StructuralHashInputs(
        schema_version=schema_version or _current_schema_version(),
        sources=sorted_sources,
        reconciliation_decisions=sorted_decisions,
        nodes=sorted_nodes,
        edges=sorted_edges,
    )


def compare_structural_inputs(
    a: StructuralHashInputs,
    b: StructuralHashInputs,
    *,
    board_id: str | None = None,
) -> StructuralComparison:
    """Compare two normalised inputs field-by-field. Bumps the
    appropriate counter on the result."""

    reasons: list[str] = []
    if a.schema_version != b.schema_version:
        reasons.append(HashMismatchReason.SCHEMA_VERSION_CHANGED.value)
    if a.sources != b.sources:
        reasons.append(HashMismatchReason.SOURCE_SET_CHANGED.value)
    if a.reconciliation_decisions != b.reconciliation_decisions:
        reasons.append(HashMismatchReason.RECONCILIATION_CHANGED.value)
    if a.nodes != b.nodes:
        reasons.append(HashMismatchReason.NODES_CHANGED.value)
    if a.edges != b.edges:
        reasons.append(HashMismatchReason.EDGES_CHANGED.value)

    equivalent = not reasons
    if board_id is not None:
        _bump_hash(
            board_id=board_id,
            status=(
                StructuralHashStatus.COMPARED_EQUIVALENT.value
                if equivalent
                else StructuralHashStatus.COMPARED_MISMATCH.value
            ),
            deterministic=True,
        )
        if not equivalent:
            for reason in reasons:
                _bump_mismatch(board_id=board_id, reason=reason)
    return StructuralComparison(
        equivalent=equivalent,
        mismatch_reasons=tuple(reasons),
        detail=None if equivalent else ",".join(reasons),
    )


def _coerce_mismatch_reason(reason_hint: str | None) -> str:
    """val_ebffe9ce rework: ``reason_hint`` may come from operator input
    or from a caller that has not yet adopted the enum. Coerce it to a
    bounded ``HashMismatchReason`` value; anything unrecognised becomes
    ``UNKNOWN`` so the metric label vocabulary stays closed (OR
    or_a938f80b)."""

    if reason_hint is None:
        return HashMismatchReason.UNKNOWN.value
    try:
        return HashMismatchReason(reason_hint).value
    except ValueError:
        return HashMismatchReason.UNKNOWN.value


def compare_structural_hashes(
    *,
    board_id: str,
    expected_hash: str,
    actual_hash: str,
    reason_hint: str | None = None,
) -> StructuralComparison:
    """Compare two pre-computed hashes; used by the promotion path when
    re-running the deterministic rebuild and comparing only the final
    digests.

    ``reason_hint`` is coerced to ``HashMismatchReason`` so the counter
    label never receives raw operator input (val_ebffe9ce rework).
    """

    equivalent = expected_hash == actual_hash
    if equivalent:
        _bump_hash(
            board_id=board_id,
            status=StructuralHashStatus.COMPARED_EQUIVALENT.value,
            deterministic=True,
        )
        return StructuralComparison(equivalent=True)

    bounded_reason = _coerce_mismatch_reason(reason_hint)
    _bump_hash(
        board_id=board_id,
        status=StructuralHashStatus.COMPARED_MISMATCH.value,
        deterministic=True,
    )
    _bump_mismatch(board_id=board_id, reason=bounded_reason)
    return StructuralComparison(
        equivalent=False,
        mismatch_reasons=(bounded_reason,),
        detail=f"expected={expected_hash} actual={actual_hash}",
    )


# --- Materialiser + step adapter -------------------------------------------


# A materialiser callable that turns a manifest source row sequence into
# the in-memory node/edge tuple expected by the structural hash. KG-02.5
# ships a default identity materialiser (one node per source row); KG-02
# follow-ups can swap this for the real Ladybug-aware materialiser.
SourceMaterialiser = Callable[
    [Sequence[Mapping[str, Any]]],
    tuple[
        tuple[dict[str, Any], ...],
        tuple[dict[str, Any], ...],
        tuple[dict[str, Any], ...],
    ],  # nodes, edges, reconciliation_decisions
]


def default_source_materialiser(
    sources: Sequence[Mapping[str, Any]],
) -> tuple[
    tuple[dict[str, Any], ...],
    tuple[dict[str, Any], ...],
    tuple[dict[str, Any], ...],
]:
    """Identity materialiser: one node per source row, no edges, no
    reconciliation decisions. Deterministic + sufficient to exercise
    the hash pipeline in tests. KG-02.6+ replaces with the production
    materialiser once the deterministic spec→graph translation is wired.
    """

    nodes = tuple(
        {
            "type": str(row.get("artifact_type", "unknown")),
            "id": str(row.get("id", "")),
            "source_ref": str(row.get("source_ref", "")),
            "content_hash": str(row.get("content_hash", "")),
        }
        for row in sources
    )
    return nodes, (), ()


@dataclass(frozen=True, slots=True)
class DeterministicStructuralRebuilder:
    """Wiring shim that converts a manifest into a
    ``RebuildStepResult`` consumable by ``KGRebuildService``.

    ``materialiser`` is injected so tests + production can plug their
    own deterministic translator. The rebuilder itself owns the
    hashing pipeline + counter bumps so every wired adapter inherits
    the same invariants.
    """

    materialiser: SourceMaterialiser = default_source_materialiser
    schema_version_resolver: Callable[[], str] = _current_schema_version

    def rebuild(
        self,
        *,
        board_id: str,
        sources: Sequence[Mapping[str, Any]],
    ) -> StructuralRebuildResult:
        deterministic_sources = tuple(
            row
            for row in sources
            if str(row.get("artifact_type", ""))
            in DETERMINISTIC_REBUILD_ARTIFACT_TYPES
        )
        nodes, edges, decisions = self.materialiser(deterministic_sources)
        inputs = build_structural_inputs(
            sources=deterministic_sources,
            reconciliation_decisions=decisions,
            nodes=nodes,
            edges=edges,
            schema_version=self.schema_version_resolver(),
        )
        structural_hash = compute_structural_hash(inputs)
        source_hash = compute_source_hash(deterministic_sources)
        _bump_hash(
            board_id=board_id,
            status=StructuralHashStatus.COMPUTED.value,
            deterministic=True,
        )
        return StructuralRebuildResult(
            structural_hash=structural_hash,
            source_hash=source_hash,
            inputs=inputs,
            counts={
                "sources": len(inputs.sources),
                "nodes": len(inputs.nodes),
                "edges": len(inputs.edges),
                "reconciliation_decisions": len(
                    inputs.reconciliation_decisions
                ),
            },
        )

    def as_rebuild_step_adapter(
        self,
        source_resolver: Callable[[Any], Sequence[Mapping[str, Any]]] | None = None,
    ) -> Callable[[Any], Any]:
        """Return a callable conforming to ``RebuildStepAdapter`` that
        wraps this rebuilder.

        val_ebffe9ce rework: there is no silent fallback to an empty
        source set anymore. The adapter resolves sources in this order:

        1. ``source_resolver(req)`` — production wiring passes a closure
           that knows how to load the manifest sources from disk + the
           ``KGRebuildSourceManifest`` store.
        2. ``req.sources_payload`` — test convenience attribute.

        If neither produces a sequence, the adapter returns
        ``RebuildStepResult(ok=False, detail="source_payload_required")``
        so ``KGRebuildService`` lands on the FAILED path and the report
        store records why the rebuild aborted. Promoting a generation
        from an empty graph is never allowed by accident.
        """

        from okto_pulse.core.kg.rebuild_service import RebuildStepResult

        def _adapter(req):
            sources_payload: Sequence[Mapping[str, Any]] | None = None
            if source_resolver is not None:
                try:
                    sources_payload = source_resolver(req)
                except Exception as exc:
                    logger.error(
                        "kg.rebuild.deterministic.source_resolver_failed "
                        "board=%s err=%s",
                        getattr(req, "board_id", "unknown"), exc,
                    )
                    return RebuildStepResult(
                        ok=False,
                        detail=f"source_resolver_failed:{type(exc).__name__}",
                        previous_kg_generation_id=getattr(
                            req, "previous_kg_generation_id", None
                        ),
                        current_kg_generation_id=None,
                    )
            else:
                sources_payload = getattr(req, "sources_payload", None)

            if sources_payload is None:
                # Fail-closed: NEVER materialise a deterministic empty
                # graph by accident. A real rebuild with zero sources
                # would still need an explicit empty tuple from the
                # resolver, not a missing attribute.
                return RebuildStepResult(
                    ok=False,
                    detail="source_payload_required",
                    previous_kg_generation_id=getattr(
                        req, "previous_kg_generation_id", None
                    ),
                    current_kg_generation_id=None,
                )

            result = self.rebuild(
                board_id=req.board_id, sources=sources_payload
            )
            return RebuildStepResult(
                ok=True,
                current_kg_generation_id=req.candidate_kg_generation_id,
                previous_kg_generation_id=req.previous_kg_generation_id,
                structural_hash=result.structural_hash,
                source_hash=result.source_hash,
                counts=result.counts,
                reconciliation_decisions=tuple(
                    result.inputs.reconciliation_decisions
                ),
                drilldown={
                    "schema_version": result.inputs.schema_version,
                    "node_count": result.counts["nodes"],
                    "edge_count": result.counts["edges"],
                },
            )

        return _adapter


__all__ = [
    "DETERMINISTIC_REBUILD_ARTIFACT_TYPES",
    "DeterministicStructuralRebuilder",
    "EXCLUDED_HASH_FIELDS",
    "HashMismatchReason",
    "SourceMaterialiser",
    "StructuralComparison",
    "StructuralHashInputs",
    "StructuralHashStatus",
    "StructuralRebuildResult",
    "build_structural_inputs",
    "compare_structural_hashes",
    "compare_structural_inputs",
    "compute_source_hash",
    "compute_structural_hash",
    "default_source_materialiser",
    "get_structural_hash_count",
    "get_structural_hash_counter_labels",
    "get_structural_hash_mismatch_count",
    "get_structural_hash_mismatch_counter_labels",
    "get_structural_hash_mismatch_samples",
    "get_structural_hash_samples",
    "reset_structural_hash_counter",
    "reset_structural_hash_mismatch_counter",
]
