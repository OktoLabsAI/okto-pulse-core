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

import threading
from typing import Any

# ---------------------------------------------------------------------------
# or_a921cc64 — kg_global_discovery_missing_embedding_skipped_total
# ---------------------------------------------------------------------------

_MISSING_EMBEDDING_LABELS = ("board_id", "node_type")
_missing_embedding_samples: list[dict[str, Any]] = []
_missing_embedding_lock = threading.Lock()


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
        return sum(
            1
            for s in _missing_embedding_samples
            if (board_id is None or s["board_id"] == board_id)
            and (node_type is None or s["node_type"] == node_type)
        )


def get_missing_embedding_skipped_labels() -> tuple[str, ...]:
    return _MISSING_EMBEDDING_LABELS


def get_missing_embedding_skipped_samples() -> list[dict[str, Any]]:
    with _missing_embedding_lock:
        return [dict(s) for s in _missing_embedding_samples]


def reset_missing_embedding_skipped_counter() -> None:
    with _missing_embedding_lock:
        _missing_embedding_samples.clear()


# ---------------------------------------------------------------------------
# or_38b60fe1 — kg_global_discovery_digest_upsert_total
# ---------------------------------------------------------------------------

DIGEST_UPSERT_CREATED = "created"
DIGEST_UPSERT_UPDATED = "updated"

_DIGEST_UPSERT_LABELS = ("board_id", "node_type", "outcome")
_digest_upsert_samples: list[dict[str, Any]] = []
_digest_upsert_lock = threading.Lock()


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
        return sum(
            1
            for s in _digest_upsert_samples
            if (board_id is None or s["board_id"] == board_id)
            and (node_type is None or s["node_type"] == node_type)
            and (outcome is None or s["outcome"] == outcome)
        )


def get_digest_upsert_labels() -> tuple[str, ...]:
    return _DIGEST_UPSERT_LABELS


def get_digest_upsert_samples() -> list[dict[str, Any]]:
    with _digest_upsert_lock:
        return [dict(s) for s in _digest_upsert_samples]


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
_canonical_incomplete_excluded_samples: list[dict[str, Any]] = []
_canonical_incomplete_excluded_lock = threading.Lock()


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
        return sum(
            1
            for s in _canonical_incomplete_excluded_samples
            if (board_id is None or s["board_id"] == board_id)
            and (reason_code is None or s["reason_code"] == reason_code)
        )


def get_canonical_incomplete_excluded_labels() -> tuple[str, ...]:
    return _CANONICAL_INCOMPLETE_EXCLUDED_LABELS


def get_canonical_incomplete_excluded_samples() -> list[dict[str, Any]]:
    with _canonical_incomplete_excluded_lock:
        return [dict(s) for s in _canonical_incomplete_excluded_samples]


def reset_canonical_incomplete_excluded_counter() -> None:
    with _canonical_incomplete_excluded_lock:
        _canonical_incomplete_excluded_samples.clear()


def reset_global_discovery_metrics() -> None:
    """Reset every Global Discovery outbox counter (test helper)."""
    reset_missing_embedding_skipped_counter()
    reset_digest_upsert_counter()
    reset_canonical_incomplete_excluded_counter()


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
    "reset_global_discovery_metrics",
]
