"""Bounded-label counters for the Global Discovery outbox worker.

Spec 849d6292 — observability requirements:

* ``or_a921cc64`` — ``kg_global_discovery_missing_embedding_skipped_total``:
  count board-graph nodes that are an eligible ``VECTOR_INDEX_TYPES`` type but
  were skipped by the global outbox because their ``embedding`` was NULL. The
  threshold is "0 on new fixtures; >0 only on legacy fixtures with a
  diagnostic", so a non-zero value is always paired with a structured log that
  carries the high-cardinality ``original_node_id``.

* ``or_38b60fe1`` — ``kg_global_discovery_digest_upsert_total``: count
  DecisionDigest rows created/updated per node_type so a regression that stops
  digesting Requirement/APIContract/TestScenario/Bug is detectable.

Labels are deliberately bounded — ``board_id`` and ``node_type`` (and
``outcome`` for upserts). ``original_node_id`` is high cardinality and lives in
the structured log, never in a metric label.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.observability.sample_buffer import runtime_counter_sample_buffer
from okto_pulse.core.runtime_context import runtime_lock

# ---------------------------------------------------------------------------
# or_a921cc64 — kg_global_discovery_missing_embedding_skipped_total
# ---------------------------------------------------------------------------

_MISSING_EMBEDDING_LABELS = ("board_id", "node_type")
_missing_embedding_samples = runtime_counter_sample_buffer(
    "kg.global_discovery.missing_embedding",
    _MISSING_EMBEDDING_LABELS,
)
_missing_embedding_lock = runtime_lock("kg.global_discovery.missing_embedding.samples")


def emit_missing_embedding_skipped(*, board_id: str, node_type: str) -> None:
    """Record one eligible board-graph node skipped for a NULL embedding."""
    with _missing_embedding_lock:
        _missing_embedding_samples.append(
            {"board_id": board_id, "node_type": node_type}
        )


def get_missing_embedding_skipped_count(
    *,
    board_id: str | None = None,
    node_type: str | None = None,
) -> int:
    with _missing_embedding_lock:
        return _missing_embedding_samples.count(board_id=board_id, node_type=node_type)


def get_missing_embedding_skipped_labels() -> tuple[str, ...]:
    return _MISSING_EMBEDDING_LABELS


def get_missing_embedding_skipped_samples() -> list[dict[str, Any]]:
    with _missing_embedding_lock:
        return _missing_embedding_samples.snapshot()


def reset_missing_embedding_skipped_counter() -> None:
    with _missing_embedding_lock:
        _missing_embedding_samples.clear()


# ---------------------------------------------------------------------------
# or_38b60fe1 — kg_global_discovery_digest_upsert_total
# ---------------------------------------------------------------------------

DIGEST_UPSERT_CREATED = "created"
DIGEST_UPSERT_UPDATED = "updated"

_DIGEST_UPSERT_LABELS = ("board_id", "node_type", "outcome")
_digest_upsert_samples = runtime_counter_sample_buffer(
    "kg.global_discovery.digest_upsert",
    _DIGEST_UPSERT_LABELS,
)
_digest_upsert_lock = runtime_lock("kg.global_discovery.digest_upsert.samples")


def emit_digest_upsert(*, board_id: str, node_type: str, outcome: str) -> None:
    """Record one DecisionDigest created or updated for ``node_type``."""
    with _digest_upsert_lock:
        _digest_upsert_samples.append(
            {"board_id": board_id, "node_type": node_type, "outcome": outcome}
        )


def get_digest_upsert_count(
    *,
    board_id: str | None = None,
    node_type: str | None = None,
    outcome: str | None = None,
) -> int:
    with _digest_upsert_lock:
        return _digest_upsert_samples.count(
            board_id=board_id,
            node_type=node_type,
            outcome=outcome,
        )


def get_digest_upsert_labels() -> tuple[str, ...]:
    return _DIGEST_UPSERT_LABELS


def get_digest_upsert_samples() -> list[dict[str, Any]]:
    with _digest_upsert_lock:
        return _digest_upsert_samples.snapshot()


def reset_digest_upsert_counter() -> None:
    with _digest_upsert_lock:
        _digest_upsert_samples.clear()


# ---------------------------------------------------------------------------
# or_e7c41a05 — kg_global_discovery_canonical_incomplete_excluded_total (R7-IMP5)
# ---------------------------------------------------------------------------
# Count canonical Learning digests whose PUBLICATION layer was downgraded to
# 'working' because the Learning is not complete-canonical (working-only Bug
# evidence, or an open canonical_debt / active cognitive_pending hold). This is
# a digest publication-layer correction — the board-graph source node is NOT
# demoted. Bounded labels: board_id (matches the existing GD metric convention
# above) + reason_code (closed R7 vocab). A clean board emits nothing.

_CANONICAL_INCOMPLETE_EXCLUDED_LABELS = ("board_id", "reason_code")
_canonical_incomplete_excluded_samples = runtime_counter_sample_buffer(
    "kg.global_discovery.canonical_incomplete_excluded",
    _CANONICAL_INCOMPLETE_EXCLUDED_LABELS,
)
_canonical_incomplete_excluded_lock = runtime_lock(
    "kg.global_discovery.canonical_incomplete_excluded.samples"
)


def emit_canonical_incomplete_excluded(*, board_id: str, reason_code: str) -> None:
    """Record one canonical Learning digest excluded from complete-canonical
    publication (downgraded to the 'working' diagnostic layer)."""
    with _canonical_incomplete_excluded_lock:
        _canonical_incomplete_excluded_samples.append(
            {"board_id": board_id, "reason_code": reason_code}
        )


def get_canonical_incomplete_excluded_count(
    *,
    board_id: str | None = None,
    reason_code: str | None = None,
) -> int:
    with _canonical_incomplete_excluded_lock:
        return _canonical_incomplete_excluded_samples.count(
            board_id=board_id,
            reason_code=reason_code,
        )


def get_canonical_incomplete_excluded_labels() -> tuple[str, ...]:
    return _CANONICAL_INCOMPLETE_EXCLUDED_LABELS


def get_canonical_incomplete_excluded_samples() -> list[dict[str, Any]]:
    with _canonical_incomplete_excluded_lock:
        return _canonical_incomplete_excluded_samples.snapshot()


def reset_canonical_incomplete_excluded_counter() -> None:
    with _canonical_incomplete_excluded_lock:
        _canonical_incomplete_excluded_samples.clear()


# ---------------------------------------------------------------------------
# or_d1f2a3b4 — kg_discovery_digest_layer_mismatch_total (R1-IMP2)
# ---------------------------------------------------------------------------
# Count DecisionDigest rows whose published graph_layer DIVERGES from the
# expected_digest_layer recomputed from the board graph (digest_vs_board_layer_
# mismatch). In steady state the R1-IMP1 reconciler drives this to ~0 on each
# drain; a non-zero value is a transient/parity-debt signal surfaced by KG Health
# + the drilldown. Bounded labels only: board_id + expected_layer + actual_layer
# (closed layer vocab canonical|working|legacy_unknown|none); no node ids.

_DIGEST_LAYER_MISMATCH_LABELS = ("board_id", "expected_layer", "actual_layer")
_digest_layer_mismatch_samples = runtime_counter_sample_buffer(
    "kg.global_discovery.digest_layer_mismatch",
    _DIGEST_LAYER_MISMATCH_LABELS,
)
_digest_layer_mismatch_lock = runtime_lock(
    "kg.global_discovery.digest_layer_mismatch.samples"
)


def emit_digest_layer_mismatch(
    *, board_id: str, expected_layer: str, actual_layer: str
) -> None:
    """Record one digest-vs-board layer mismatch observed by a drilldown/Health read."""
    with _digest_layer_mismatch_lock:
        _digest_layer_mismatch_samples.append({
            "board_id": board_id,
            "expected_layer": expected_layer,
            "actual_layer": actual_layer,
        })


def get_digest_layer_mismatch_count(
    *,
    board_id: str | None = None,
    expected_layer: str | None = None,
    actual_layer: str | None = None,
) -> int:
    with _digest_layer_mismatch_lock:
        return _digest_layer_mismatch_samples.count(
            board_id=board_id,
            expected_layer=expected_layer,
            actual_layer=actual_layer,
        )


def get_digest_layer_mismatch_labels() -> tuple[str, ...]:
    return _DIGEST_LAYER_MISMATCH_LABELS


def get_digest_layer_mismatch_samples() -> list[dict[str, Any]]:
    with _digest_layer_mismatch_lock:
        return _digest_layer_mismatch_samples.snapshot()


def reset_digest_layer_mismatch_counter() -> None:
    with _digest_layer_mismatch_lock:
        _digest_layer_mismatch_samples.clear()


def reset_global_discovery_metrics() -> None:
    """Reset every Global Discovery outbox counter (test helper)."""
    reset_missing_embedding_skipped_counter()
    reset_digest_upsert_counter()
    reset_canonical_incomplete_excluded_counter()
    reset_digest_layer_mismatch_counter()


__all__ = [
    "DIGEST_UPSERT_CREATED",
    "DIGEST_UPSERT_UPDATED",
    "emit_missing_embedding_skipped",
    "get_missing_embedding_skipped_count",
    "get_missing_embedding_skipped_labels",
    "get_missing_embedding_skipped_samples",
    "reset_missing_embedding_skipped_counter",
    "emit_digest_upsert",
    "get_digest_upsert_count",
    "get_digest_upsert_labels",
    "get_digest_upsert_samples",
    "reset_digest_upsert_counter",
    "emit_canonical_incomplete_excluded",
    "get_canonical_incomplete_excluded_count",
    "get_canonical_incomplete_excluded_labels",
    "get_canonical_incomplete_excluded_samples",
    "reset_canonical_incomplete_excluded_counter",
    "emit_digest_layer_mismatch",
    "get_digest_layer_mismatch_count",
    "get_digest_layer_mismatch_labels",
    "get_digest_layer_mismatch_samples",
    "reset_digest_layer_mismatch_counter",
    "reset_global_discovery_metrics",
]
