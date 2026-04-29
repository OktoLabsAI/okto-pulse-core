"""Relevance scoring pipeline for KG nodes (spec R2, v0.3.0+).

Continuous replacement for the binary validation_status removed in R1. The
score is a float in [0.0, 1.5] combining four signals:

    relevance = clamp(
        0.4 * source_confidence
      + 0.3 * log(1 + degree) / log(100)
      + 0.3 * decayed_hits
      - contradict_penalty
      + priority_boost
    )

where:
    degree             = in_degree + out_degree (any relation type)
    decayed_hits       = query_hits * exp(-ln(2) * days_since_last_query / 30)
    contradict_penalty = SUM(COALESCE(r.confidence, 0.5)) for every incoming
                         :contradicts edge, capped at CONTRADICT_PENALTY_CAP.
    priority_boost     = MAX(_resolve_priority_boost, _resolve_severity_boost)
                         for Bug nodes; priority-only for other types.

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
      the decay expression via ``_apply_decay_reorder`` (R3 covers that
      path; spec 20f67c2a — Ideação #5 — wired it into ``find_by_topic``).

v0.3.3 (spec 28583299 — Ideação #4): the persisted score is kept fresh
through three event-driven entry points so cypher-direct ``ORDER BY
relevance_score DESC`` queries see a recent value without paying the
on-read decay cost:

    1. Recompute on hit-flush via the ``kg.hit_flushed`` event
       (KGHitRecomputeHandler) — fired by ``_flush_hits`` after the lazy
       counter reaches HIT_FLUSH_THRESHOLD.
    2. Recompute on priority/severity change via ``card.priority_changed``
       and ``card.severity_changed`` (CardPriorityChangedHandler /
       CardSeverityChangedHandler) — fired by services.update_card after
       a real transition. Audit Decision nodes are emitted in the KG when
       ``|delta_boost| > 0.05`` (dec_cb956457).
    3. Daily decay tick via ``kg.tick.daily`` at 03:00 UTC
       (KGDailyTickHandler) — APScheduler in-process emits the event;
       handler iterates active boards and recomputes nodes whose
       ``last_recomputed_at`` is older than KG_DECAY_TICK_STALENESS_DAYS.

The decay-on-read path (``_apply_decay_reorder``) remains the canonical
ranking truth — events and tick keep the raw score "reasonably fresh" so
cypher-direct queries that don't use over-fetch still see meaningful
ordering. ``last_recomputed_at`` is written by these recompute paths;
``last_queried_at`` is owned by ``record_query_hit`` exclusively (BR4 of
Ideação #5 preserved — separation of responsibilities).

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

# v0.3.2 (spec 20f67c2a — Ideação #5): upper cap on the per-node contradict
# penalty. Without this cap, 5+ incoming :contradicts edges (raw_sum 2.5+)
# silently zero-out the relevance score via the [0, 1.5] clamp, even for
# nodes with high source_confidence. Cap = 0.5 keeps the score above zero
# for the typical source_conf=1.0 + degree>0 case while preserving the
# proportional penalty signal up to the threshold.
CONTRADICT_PENALTY_CAP = 0.5

# In-process counter: how many times the cap was applied per board. Read by
# /api/v1/kg/health (and the gemelar MCP tool) to surface "spec mal-definido"
# heuristics. Resettable via reset_contradict_warn_counters() for tests.
CONTRADICT_WARN_COUNTERS: dict[str, int] = defaultdict(int)


def get_contradict_warn_count(board_id: str) -> int:
    """Return the per-board count of contradict_penalty cap events."""
    return CONTRADICT_WARN_COUNTERS.get(board_id, 0)


def reset_contradict_warn_counters() -> None:
    """Drop the per-board counter state — tests only."""
    CONTRADICT_WARN_COUNTERS.clear()


# v0.3.2 (spec 20f67c2a — Ideação #5, BR3): how many extra candidates beyond
# top_k the pre-filter should return so post-processing reorder can change
# the composition of the final top-K. 3x is a heuristic from the refinement
# (top_k=10 → pool=30) — tunable if production telemetry shows it's too
# permissive or too tight.
DECAY_REORDER_POOL_MULTIPLIER = 3


def _apply_decay_reorder(
    rows: list[dict[str, Any]],
    top_k: int,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Reorder a candidate pool by decayed_relevance and return the top-K.

    Spec 20f67c2a (Ideação #5, BR3 + Decision dec_b072d257) — the persisted
    ``relevance_score`` is the raw value; downstream queries that
    ``ORDER BY relevance_score DESC`` rank by stale state. This helper
    receives a pool already pre-filtered (typically ``top_k * DECAY_REORDER_POOL_MULTIPLIER``
    rows from the Cypher templates) and recomputes a decay-aware ordering
    in pure Python, returning the top_k.

    ``rows`` items must carry these keys: ``node_id``, ``relevance_score``,
    ``query_hits``, ``last_queried_at``. Other keys are preserved untouched.
    Each returned row gains ``decayed_relevance`` and ``original_relevance``
    fields so callers can introspect the reorder decision.

    Pure function — no I/O, O(N) where N == len(rows). Empty input returns
    an empty list. ``top_k <= 0`` returns an empty list as well.
    """
    if top_k <= 0 or not rows:
        return []

    enriched: list[dict[str, Any]] = []
    for row in rows:
        original = float(row.get("relevance_score", 0.0) or 0.0)
        raw_hits = int(row.get("query_hits", 0) or 0)
        last_queried = row.get("last_queried_at")
        decayed_hits = _decay_hits(raw_hits, last_queried, now=now)
        # The raw score embedded HITS_WEIGHT * raw_hits as part of the sum.
        # We compensate by subtracting the stale hit term and re-adding the
        # decayed one — that yields the score the agent would compute today
        # without persisting it back to Kùzu.
        stale_hit_term = HITS_WEIGHT * float(raw_hits)
        decayed_hit_term = HITS_WEIGHT * decayed_hits
        decayed_relevance = original - stale_hit_term + decayed_hit_term
        enriched.append(
            {
                **row,
                "decayed_relevance": decayed_relevance,
                "original_relevance": original,
            }
        )

    enriched.sort(key=lambda r: r["decayed_relevance"], reverse=True)
    return enriched[:top_k]


BATCH_UPDATE_THRESHOLD = 50  # endpoints above this use the UNWIND path

# v0.3.1 (spec 0eb51d3e): priority_boost mapping table. The boost is applied
# additively ONCE at node insert time and then persisted as an immutable
# column; _fetch_node_inputs reads it back on every recompute. Caps at +0.2
# (CRITICAL) so the combined score stays bounded under the +1.5 clamp.
PRIORITY_BOOST_BY_LEVEL: dict[str, float] = {
    "none": 0.0,
    "low": 0.0,
    "medium": 0.05,
    "high": 0.10,
    "very_high": 0.15,
    "critical": 0.20,
}


def _resolve_priority_boost(priority: Any) -> float:
    """Map a card priority to its additive boost.

    Accepts str, CardPriority enum, or None. Unknown values (typos, future
    enum additions, historical data) return 0.0 silently — the worker must
    never raise on consolidation because a priority field drifted.
    """
    if priority is None:
        return 0.0
    # CardPriority and similar str-valued enums expose `.value`; fall back to
    # str() so plain strings and unexpected shapes both normalise cleanly.
    raw = getattr(priority, "value", priority)
    if not isinstance(raw, str):
        return 0.0
    return PRIORITY_BOOST_BY_LEVEL.get(raw.strip().lower(), 0.0)


# v0.3.3 (spec 28583299 — Ideação #4, dec_27de54df): bug severity is the
# second additive input to a Bug node's priority_boost. The worker takes
# MAX(priority_boost, severity_boost) so neither signal overrides the other:
# severity captures TECHNICAL impact (data loss, prod down) while priority
# captures BUSINESS urgency (SLA, customer tier). Only Bug nodes consult the
# severity field — feature/task/chore cards ignore it even when populated.
SEVERITY_BOOST_BY_LEVEL: dict[str, float] = {
    "critical": 0.20,
    "major": 0.15,
    "minor": 0.10,
}


def _resolve_severity_boost(severity: Any) -> float:
    """Map a bug severity level to its additive boost.

    Mirrors :func:`_resolve_priority_boost`'s defensive contract: accepts a
    BugSeverity enum, str, or None. Unknown values (typos, future enum
    additions, historical data) return 0.0 silently so the worker never
    raises on consolidation due to severity drift. Only consulted for
    ``card_type == 'bug'`` — see ``deterministic_worker.process_card``.
    """
    if severity is None:
        return 0.0
    raw = getattr(severity, "value", severity)
    if not isinstance(raw, str):
        return 0.0
    return SEVERITY_BOOST_BY_LEVEL.get(raw.strip().lower(), 0.0)

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
    priority_boost: float = 0.0,
) -> float:
    """Combine the four signals (plus frozen priority_boost) into [0.0, 1.5].

    Pure function — no I/O, no global state. The clamp is the last step so
    an arbitrary penalty can never push the output below 0 nor can a huge
    degree+hits+boost combination exceed 1.5.

    ``priority_boost`` is the additive term resolved from the source card's
    priority (spec 0eb51d3e, R2.1) and, for Bug nodes, MAX with the severity
    boost (spec 28583299 — Ideação #4, IMPL-A). It represents the criticality
    of the source card at the time of the most recent priority/severity
    change. The CardPriorityChangedHandler / CardSeverityChangedHandler
    (Ideação #4, IMPL-C) recomputes and persists it on each transition;
    pure callers (this function) treat the persisted column as canonical
    input and never recalculate it from scratch on the read path.

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
    if priority_boost < 0:
        priority_boost = 0.0

    degree_term = 0.0
    if degree > 0:
        degree_term = DEGREE_WEIGHT * math.log(1 + degree) / math.log(DEGREE_SATURATION)

    raw = (
        SOURCE_WEIGHT * source_conf
        + degree_term
        + HITS_WEIGHT * decayed_hits
        - contradict_penalty
        + priority_boost
    )

    if raw < CLAMP_MIN or raw > CLAMP_MAX:
        logger.warning(
            "kg.scoring.clamp_applied raw=%.4f source=%.2f degree=%d "
            "decayed_hits=%.2f penalty=%.2f boost=%.2f",
            raw, source_conf, degree, decayed_hits, contradict_penalty,
            priority_boost,
            extra={
                "event": "kg.scoring.clamp_applied",
                "raw_score": raw,
                "source_confidence": source_conf,
                "degree": degree,
                "decayed_hits": decayed_hits,
                "contradict_penalty": contradict_penalty,
                "priority_boost": priority_boost,
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


def _fetch_node_inputs(
    conn,
    node_type: str,
    node_id: str,
    *,
    board_id: str | None = None,
) -> dict[str, Any] | None:
    """Read the four signals + current score for ``node_id`` in one query.

    ``node_type`` is required because Kùzu stores each type in its own table
    and ``MATCH (n)`` without a label would be expensive on large boards.
    Returns ``None`` when the node doesn't exist in this table.

    ``board_id`` is optional for retrocompat — when provided, the contradict
    penalty cap (spec 20f67c2a — Ideação #5) increments the per-board
    CONTRADICT_WARN_COUNTERS so /api/v1/kg/health can surface the count.
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
            f"SUM(COALESCE(c.confidence, $default_conf)), "
            f"n.priority_boost",
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
    raw_penalty = float(row[6] or 0.0)
    # priority_boost persisted column — NULL on legacy rows pre-migration,
    # which maps cleanly to 0.0 (no boost, no-op additive term).
    priority_boost = float(row[7]) if row[7] is not None else 0.0

    # Spec 20f67c2a (Ideação #5, BR2): cap contradict_penalty at
    # CONTRADICT_PENALTY_CAP so an unbounded sum of incoming :contradicts
    # edges cannot drag the relevance score to zero artificially.
    penalty = min(raw_penalty, CONTRADICT_PENALTY_CAP)
    if raw_penalty > CONTRADICT_PENALTY_CAP:
        edge_count = int(round(raw_penalty / DEFAULT_CONTRADICT_CONFIDENCE))
        if board_id is not None:
            CONTRADICT_WARN_COUNTERS[board_id] += 1
        logger.warning(
            "kg.scoring.contradict_penalty_capped node_type=%s node_id=%s "
            "raw_sum=%.4f applied_cap=%.2f edge_count_estimate=%d",
            node_type, node_id, raw_penalty, CONTRADICT_PENALTY_CAP,
            edge_count,
            extra={
                "event": "kg.scoring.contradict_penalty_capped",
                "node_id": node_id,
                "node_type": node_type,
                "board_id": board_id,
                "raw_sum": raw_penalty,
                "applied_cap": CONTRADICT_PENALTY_CAP,
                "edge_count": edge_count,
            },
        )

    return {
        "source_confidence": source_conf,
        "degree": out_deg + in_deg,
        "query_hits": query_hits,
        "last_queried_at": last_queried_at,
        "contradict_penalty": penalty,
        "raw_contradict_penalty": raw_penalty,
        "score_before": score_before,
        "priority_boost": priority_boost,
    }


def _persist_score(
    conn,
    node_type: str,
    node_id: str,
    score: float,
    *,
    now_iso: str | None = None,
) -> None:
    """UPDATE the node's relevance_score and last_recomputed_at in Kùzu.

    ``now_iso`` is an optional ISO-8601 string for ``last_recomputed_at``;
    when omitted, ``datetime.now(timezone.utc).isoformat()`` is used. Pass
    a fixed value when batching so all nodes in the same recompute share
    the same recomputed-at marker (BR8 / spec 28583299 — Ideação #4).
    Best-effort: logs on error and does not raise.
    """
    if now_iso is None:
        now_iso = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(
            f"MATCH (n:{node_type} {{id: $nid}}) "
            f"SET n.relevance_score = $score, n.last_recomputed_at = $now",
            {"nid": node_id, "score": score, "now": now_iso},
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
    inputs = _fetch_node_inputs(conn, node_type, node_id, board_id=board_id)
    if inputs is None:
        return None

    decayed = _decay_hits(inputs["query_hits"], inputs["last_queried_at"], now=now)
    new_score = _compute_relevance(
        inputs["source_confidence"],
        inputs["degree"],
        decayed,
        inputs["contradict_penalty"],
        priority_boost=inputs["priority_boost"],
    )

    if abs(new_score - inputs["score_before"]) > 1e-6:
        _persist_score(
            conn, node_type, node_id, new_score,
            now_iso=(now or datetime.now(timezone.utc)).isoformat(),
        )
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
    # per node_type carrying all the (id, score, now) rows via UNWIND.
    # All rows in a single batch share the same now_iso so kg_health can
    # report a coherent "last decay tick" timestamp (BR8 / spec 28583299).
    now_iso = (now or datetime.now(timezone.utc)).isoformat()
    score_rows_by_type: dict[str, list[dict[str, Any]]] = {}
    observed_scores: list[tuple[str, float]] = []  # (node_type, score) for histogram

    for node_type, node_id in endpoints:
        inputs = _fetch_node_inputs(conn, node_type, node_id, board_id=board_id)
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
            priority_boost=inputs["priority_boost"],
        )
        score_rows_by_type.setdefault(node_type, []).append(
            {"id": node_id, "score": new_score, "now": now_iso}
        )
        observed_scores.append((node_type, new_score))

    recomputed = 0
    for node_type, rows in score_rows_by_type.items():
        try:
            conn.execute(
                f"UNWIND $rows AS r "
                f"MATCH (n:{node_type} {{id: r.id}}) "
                f"SET n.relevance_score = r.score, n.last_recomputed_at = r.now",
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
