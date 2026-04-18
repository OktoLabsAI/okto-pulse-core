"""Relevance scoring pipeline for KG nodes (spec R2, v0.3.0).

Continuous replacement for the binary validation_status removed in R1. The
score is a float in [0.0, 1.5] combining four signals:

    relevance = clamp(
        0.4 * source_confidence
      + 0.3 * log(1 + degree) / log(100)
      + 0.3 * decayed_hits
      - contradict_penalty
    )

where:
    degree             = in_degree + out_degree (any relation type)
    decayed_hits       = query_hits * exp(-ln(2) * days_since_last_query / 30)
    contradict_penalty = SUM(COALESCE(r.confidence, 0.5)) for every incoming
                         :contradicts edge. Edges with NULL confidence fall
                         back to 0.5.

Design notes:
    * ``_compute_relevance`` is pure (no I/O) so it's trivially testable.
    * ``_recompute_relevance`` reads the graph state for a single node,
      calculates the score, persists it via UPDATE, emits an SSE event
      and observes the histogram bucket. Safe to call inside a commit
      transaction.
    * ``_recompute_relevance_batch`` is the UNWIND-based variant used by
      commit_consolidation when a commit touches >50 endpoints, reducing
      Kùzu round-trips from N to 3 (lookup + UPDATE + distribution log).
    * The decay is applied on *read* — the score persisted in Kùzu is
      always the "raw" value. Downstream ORDER BY queries need to reapply
      the decay expression (R3 covers that path).

Observability:
    * Every score change emits a structured log event
      ``kg.scoring.recompute`` with ``{board_id, node_id, score_before,
      score_after, trigger}`` via the standard Python logger. The SSE
      handler translates these into ``kg.node.score_changed`` on the
      board event stream.
    * ``RELEVANCE_HISTOGRAM`` is an in-process dict keyed by (board_id,
      node_type) holding bucket counters. The ``/api/v1/kg/metrics/relevance``
      route serialises this to Prometheus text format. No external dep.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("okto_pulse.kg.scoring")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CLAMP_MIN = 0.0
CLAMP_MAX = 1.5

SOURCE_WEIGHT = 0.4
DEGREE_WEIGHT = 0.3
HITS_WEIGHT = 0.3

DEGREE_SATURATION = 100  # log base: log(1+degree)/log(DEGREE_SATURATION)
DECAY_HALF_LIFE_DAYS = 30  # decayed = hits * 0.5 ** (days/30)
DEFAULT_CONTRADICT_CONFIDENCE = 0.5  # NULL fallback

BATCH_UPDATE_THRESHOLD = 50  # endpoints above this use the UNWIND path

# Histogram buckets matching the spec (8 upper bounds).
HISTOGRAM_BUCKETS: tuple[float, ...] = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.5)

# In-process histogram state: {(board_id, node_type): [count_per_bucket...]}
# Cardinality is bounded: node_type enum has 11 values, board count is low
# single-digit in practice. Safe to keep in memory.
RELEVANCE_HISTOGRAM: dict[tuple[str, str], list[int]] = defaultdict(
    lambda: [0] * len(HISTOGRAM_BUCKETS)
)


# ---------------------------------------------------------------------------
# Pure function — AC1/AC2/AC3 coverage
# ---------------------------------------------------------------------------


def _compute_relevance(
    source_conf: float,
    degree: int,
    decayed_hits: float,
    contradict_penalty: float,
) -> float:
    """Combine the four signals into a relevance score in [0.0, 1.5].

    Pure function — no I/O, no global state. The clamp is the last step so
    an arbitrary penalty can never push the output below 0 nor can a huge
    degree+hits combination exceed 1.5.

    Emits WARN when the raw (pre-clamp) value falls outside the allowed
    range — helps detecting calibration drift (e.g. someone sets a huge
    contradict_penalty that keeps everyone at 0).
    """
    if degree < 0:
        degree = 0
    if decayed_hits < 0:
        decayed_hits = 0.0
    if contradict_penalty < 0:
        contradict_penalty = 0.0

    degree_term = 0.0
    if degree > 0:
        degree_term = DEGREE_WEIGHT * math.log(1 + degree) / math.log(DEGREE_SATURATION)

    raw = (
        SOURCE_WEIGHT * source_conf
        + degree_term
        + HITS_WEIGHT * decayed_hits
        - contradict_penalty
    )

    if raw < CLAMP_MIN or raw > CLAMP_MAX:
        logger.warning(
            "kg.scoring.clamp_applied raw=%.4f source=%.2f degree=%d "
            "decayed_hits=%.2f penalty=%.2f",
            raw, source_conf, degree, decayed_hits, contradict_penalty,
            extra={
                "event": "kg.scoring.clamp_applied",
                "raw_score": raw,
                "source_confidence": source_conf,
                "degree": degree,
                "decayed_hits": decayed_hits,
                "contradict_penalty": contradict_penalty,
            },
        )

    return max(CLAMP_MIN, min(CLAMP_MAX, raw))


# ---------------------------------------------------------------------------
# Decay helper — AC7 coverage
# ---------------------------------------------------------------------------


def _decay_hits(
    query_hits: int,
    last_queried_at: str | datetime | None,
    *,
    now: datetime | None = None,
) -> float:
    """Apply the 30-day half-life decay to ``query_hits``.

    ``last_queried_at`` can be an ISO string (as stored in Kùzu), a
    ``datetime``, or ``None``. ``None`` → returns 0.0 (node never queried,
    so any decayed count would be meaningless).

    The formula is ``hits * exp(-ln(2) * days_since / 30)``, which is
    equivalent to ``hits * 0.5 ** (days_since / 30)`` but slightly faster
    and avoids fractional-exponent edge cases on CPython.
    """
    if query_hits <= 0 or last_queried_at is None:
        return 0.0

    if isinstance(last_queried_at, str):
        try:
            last_queried_at = datetime.fromisoformat(last_queried_at.replace("Z", "+00:00"))
        except ValueError:
            return 0.0

    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    if last_queried_at.tzinfo is None:
        last_queried_at = last_queried_at.replace(tzinfo=timezone.utc)

    delta_days = max(0.0, (reference - last_queried_at).total_seconds() / 86400.0)
    decayed = query_hits * math.exp(-math.log(2) * delta_days / DECAY_HALF_LIFE_DAYS)
    # Round to 4 decimals so equality checks in tests don't diverge on
    # floating-point noise.
    return round(decayed, 4)


# ---------------------------------------------------------------------------
# Histogram observe — AC9 coverage
# ---------------------------------------------------------------------------


def _observe_histogram(board_id: str, node_type: str, score: float) -> None:
    """Increment the bucket counters for (board_id, node_type) holding score.

    Buckets are cumulative (Prometheus ``le`` semantics): a score of 0.45
    increments every bucket with ``le >= 0.45`` (i.e. 0.6, 0.8, 1.0, 1.2,
    1.5). Gauge for total count is the last bucket.
    """
    counts = RELEVANCE_HISTOGRAM[(board_id, node_type)]
    for idx, upper in enumerate(HISTOGRAM_BUCKETS):
        if score <= upper:
            counts[idx] += 1


def get_histogram_snapshot() -> dict[tuple[str, str], list[int]]:
    """Return a shallow copy of the histogram for the metrics endpoint."""
    return {k: list(v) for k, v in RELEVANCE_HISTOGRAM.items()}


def reset_histogram() -> None:
    """Drop histogram state — tests only."""
    RELEVANCE_HISTOGRAM.clear()


# ---------------------------------------------------------------------------
# _recompute_relevance — AC4/AC8 coverage
# ---------------------------------------------------------------------------


def _fetch_node_inputs(conn, node_type: str, node_id: str) -> dict[str, Any] | None:
    """Read the four signals + current score for ``node_id`` in one query.

    ``node_type`` is required because Kùzu stores each type in its own table
    and ``MATCH (n)`` without a label would be expensive on large boards.
    Returns ``None`` when the node doesn't exist in this table.
    """
    try:
        res = conn.execute(
            f"MATCH (n:{node_type} {{id: $nid}}) "
            f"OPTIONAL MATCH (n)-[r_out]->() "
            f"WITH n, COUNT(r_out) AS out_deg "
            f"OPTIONAL MATCH (n)<-[r_in]-() "
            f"WITH n, out_deg, COUNT(r_in) AS in_deg "
            f"OPTIONAL MATCH (n)<-[c:contradicts]-() "
            f"RETURN n.source_confidence, out_deg, in_deg, "
            f"n.query_hits, n.last_queried_at, n.relevance_score, "
            f"SUM(COALESCE(c.confidence, $default_conf))",
            {"nid": node_id, "default_conf": DEFAULT_CONTRADICT_CONFIDENCE},
        )
    except Exception as exc:
        logger.warning(
            "kg.scoring.fetch_failed node_type=%s node_id=%s err=%s",
            node_type, node_id, exc,
        )
        return None

    if not res.has_next():
        return None
    row = res.get_next()
    source_conf = float(row[0]) if row[0] is not None else 0.5
    out_deg = int(row[1] or 0)
    in_deg = int(row[2] or 0)
    query_hits = int(row[3] or 0)
    last_queried_at = row[4]
    score_before = float(row[5]) if row[5] is not None else 0.5
    penalty = float(row[6] or 0.0)

    return {
        "source_confidence": source_conf,
        "degree": out_deg + in_deg,
        "query_hits": query_hits,
        "last_queried_at": last_queried_at,
        "contradict_penalty": penalty,
        "score_before": score_before,
    }


def _persist_score(conn, node_type: str, node_id: str, score: float) -> None:
    """UPDATE the node's relevance_score in Kùzu. Best-effort — logs on error."""
    try:
        conn.execute(
            f"MATCH (n:{node_type} {{id: $nid}}) "
            f"SET n.relevance_score = $score",
            {"nid": node_id, "score": score},
        )
    except Exception as exc:
        logger.error(
            "kg.scoring.persist_failed node_type=%s node_id=%s err=%s",
            node_type, node_id, exc,
        )


def _recompute_relevance(
    conn,
    board_id: str,
    node_type: str,
    node_id: str,
    *,
    trigger: str = "degree_delta",
    now: datetime | None = None,
) -> float | None:
    """Recompute and persist a single node's relevance score.

    Returns the new score, or ``None`` if the node couldn't be found.
    Emits a structured log event consumed by the SSE translator. Updates
    the in-process histogram.
    """
    inputs = _fetch_node_inputs(conn, node_type, node_id)
    if inputs is None:
        return None

    decayed = _decay_hits(inputs["query_hits"], inputs["last_queried_at"], now=now)
    new_score = _compute_relevance(
        inputs["source_confidence"],
        inputs["degree"],
        decayed,
        inputs["contradict_penalty"],
    )

    if abs(new_score - inputs["score_before"]) > 1e-6:
        _persist_score(conn, node_type, node_id, new_score)
        logger.info(
            "kg.scoring.recompute board=%s node=%s type=%s "
            "before=%.4f after=%.4f trigger=%s",
            board_id, node_id, node_type,
            inputs["score_before"], new_score, trigger,
            extra={
                "event": "kg.scoring.recompute",
                "board_id": board_id,
                "node_id": node_id,
                "node_type": node_type,
                "score_before": inputs["score_before"],
                "score_after": new_score,
                "trigger": trigger,
                "timestamp": (now or datetime.now(timezone.utc)).isoformat(),
            },
        )
    _observe_histogram(board_id, node_type, new_score)
    return new_score


# ---------------------------------------------------------------------------
# Batch variant — AC10 coverage
# ---------------------------------------------------------------------------


def _recompute_relevance_batch(
    conn,
    board_id: str,
    endpoints: list[tuple[str, str]],
    *,
    trigger: str = "degree_delta",
    now: datetime | None = None,
) -> int:
    """Recompute scores for a list of ``(node_type, node_id)`` in a batch.

    Returns the number of nodes successfully recomputed. When
    ``len(endpoints) > BATCH_UPDATE_THRESHOLD``, uses the UNWIND variant:
    inputs are fetched individually (Kùzu 0.6 has no cross-table batch
    fetch), scores computed in Python, and persisted with a single
    ``UNWIND $rows AS r MATCH (n:Type {id: r.id}) SET n.relevance_score = r.s``
    per node type. Groups by node type to keep the MATCH table-bound.

    Below the threshold, falls back to the per-row path since the saving
    is negligible vs. the UNWIND machinery overhead.
    """
    started = datetime.now(timezone.utc)
    if len(endpoints) <= BATCH_UPDATE_THRESHOLD:
        recomputed = 0
        for node_type, node_id in endpoints:
            result = _recompute_relevance(
                conn, board_id, node_type, node_id,
                trigger=trigger, now=now,
            )
            if result is not None:
                recomputed += 1
        return recomputed

    # Batch path: compute every new score in memory, then do one UPDATE
    # per node_type carrying all the (id, score) rows via UNWIND.
    score_rows_by_type: dict[str, list[dict[str, Any]]] = {}
    observed_scores: list[tuple[str, float]] = []  # (node_type, score) for histogram

    for node_type, node_id in endpoints:
        inputs = _fetch_node_inputs(conn, node_type, node_id)
        if inputs is None:
            continue
        decayed = _decay_hits(
            inputs["query_hits"], inputs["last_queried_at"], now=now,
        )
        new_score = _compute_relevance(
            inputs["source_confidence"],
            inputs["degree"],
            decayed,
            inputs["contradict_penalty"],
        )
        score_rows_by_type.setdefault(node_type, []).append(
            {"id": node_id, "score": new_score}
        )
        observed_scores.append((node_type, new_score))

    recomputed = 0
    for node_type, rows in score_rows_by_type.items():
        try:
            conn.execute(
                f"UNWIND $rows AS r "
                f"MATCH (n:{node_type} {{id: r.id}}) "
                f"SET n.relevance_score = r.score",
                {"rows": rows},
            )
            recomputed += len(rows)
        except Exception as exc:
            logger.error(
                "kg.scoring.batch_persist_failed node_type=%s count=%d err=%s",
                node_type, len(rows), exc,
            )

    for node_type, score in observed_scores:
        _observe_histogram(board_id, node_type, score)

    duration_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000
    logger.info(
        "kg.recompute.batch board=%s endpoints=%d recomputed=%d duration_ms=%.1f",
        board_id, len(endpoints), recomputed, duration_ms,
        extra={
            "event": "kg.recompute.batch",
            "board_id": board_id,
            "endpoints": len(endpoints),
            "recomputed": recomputed,
            "duration_ms": round(duration_ms, 1),
        },
    )
    return recomputed
