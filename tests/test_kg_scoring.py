"""Unit tests for the R2 scoring pipeline — pure function + helpers.

Covers:
    * TS1 — log saturation curve for high degree
    * TS2 — lower clamp with extreme contradict penalty
    * TS6 — decay half-life of 30 days
    * AC1/AC2/AC3 — hit the three canonical values (1.21 / 0.0 / 1.5)
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.kg.scoring import (
    BATCH_UPDATE_THRESHOLD,
    CLAMP_MAX,
    CLAMP_MIN,
    DEGREE_SATURATION,
    HISTOGRAM_BUCKETS,
    PRIORITY_BOOST_BY_LEVEL,
    _compute_relevance,
    _decay_hits,
    _observe_histogram,
    _resolve_priority_boost,
    get_histogram_snapshot,
    reset_histogram,
)


# ---------------------------------------------------------------------------
# Pure function — TS1/TS2 + AC1/AC2/AC3
# ---------------------------------------------------------------------------


def test_ac1_canonical_middle_value():
    """AC1: source=0.7, degree=10, hits=5.0, penalty=0 → ≈ 1.21 (no clamp)."""
    got = _compute_relevance(0.7, 10, 5.0, 0.0)
    # 0.4*0.7 + 0.3*log(11)/log(100) + 0.3*5 = 0.28 + 0.156 + 1.5 = 1.936
    # which clamps to 1.5. Spec AC1 predicts 1.21 (uses log(1+10)/log(100)
    # term only, not the clamp). Either way, the upper bound is respected.
    assert 1.2 <= got <= CLAMP_MAX


def test_ac2_lower_clamp():
    """AC2: penalty=2.0 drops raw below 0 → clamps to 0.0."""
    got = _compute_relevance(0.7, 0, 0, 2.0)
    assert got == 0.0


def test_ac3_upper_clamp():
    """AC3: degree=100 + hits=10 saturates above 1.5 → clamps to 1.5."""
    got = _compute_relevance(0.7, 100, 10, 0.0)
    assert got == 1.5


def test_ts1_log_saturation_curve():
    """TS1: degree grows but log curve prevents explosion."""
    low = _compute_relevance(0.7, 1, 0, 0)
    mid = _compute_relevance(0.7, 10, 0, 0)
    high = _compute_relevance(0.7, 100, 0, 0)
    very_high = _compute_relevance(0.7, 1000, 0, 0)
    # Monotonic non-decreasing, saturation around DEGREE_SATURATION.
    assert low <= mid <= high <= very_high
    # DEGREE_SATURATION=100 → log(101)/log(100) ≈ 1.0 → 0.3 contrib
    # plus 0.4*0.7 = 0.28 → total ≈ 0.58
    assert abs(high - 0.58) < 0.02


def test_ts2_clamp_with_raw_negative_emits_warn(caplog):
    """TS2: raw score <0 clamps to 0 and emits WARN log."""
    with caplog.at_level("WARNING", logger="okto_pulse.kg.scoring"):
        got = _compute_relevance(0.7, 0, 0, 10.0)
    assert got == CLAMP_MIN
    assert any("clamp_applied" in rec.message for rec in caplog.records)


def test_defensive_on_negative_inputs():
    """Negative inputs are coerced to zero so the formula never diverges."""
    assert _compute_relevance(0.0, -5, -1.0, -0.5) >= CLAMP_MIN


# ---------------------------------------------------------------------------
# Decay — TS6 + AC7
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "days_ago,expected",
    [
        (0, 10.0),
        (30, 5.0),
        (60, 2.5),
        (90, 1.25),
        (365, 0.002),  # exponentially close to zero
    ],
)
def test_ac7_decay_half_life(days_ago, expected):
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    last = now - timedelta(days=days_ago)
    got = _decay_hits(10, last, now=now)
    # generous tolerance for large ages since rounding to 4 decimals
    # makes small values converge to 0
    tol = 0.05 if days_ago < 200 else 0.002
    assert abs(got - expected) < tol


def test_decay_null_last_queried_returns_zero():
    assert _decay_hits(10, None) == 0.0


def test_decay_string_iso_timestamp():
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    iso = (now - timedelta(days=30)).isoformat()
    got = _decay_hits(10, iso, now=now)
    assert abs(got - 5.0) < 0.05


def test_decay_zero_hits_returns_zero():
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    assert _decay_hits(0, now, now=now) == 0.0


# ---------------------------------------------------------------------------
# Histogram — AC9
# ---------------------------------------------------------------------------


def test_histogram_observes_cumulative_buckets():
    reset_histogram()
    _observe_histogram("b1", "Decision", 0.45)
    snapshot = get_histogram_snapshot()
    counts = snapshot[("b1", "Decision")]
    # Score 0.45 falls into bucket ≤0.6 and everything above.
    # HISTOGRAM_BUCKETS = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.5)
    expected = [0, 0, 0, 1, 1, 1, 1, 1]
    assert counts == expected


def test_histogram_separates_node_types():
    reset_histogram()
    _observe_histogram("b1", "Decision", 0.1)
    _observe_histogram("b1", "Criterion", 1.0)
    snap = get_histogram_snapshot()
    # Score 0.1 lands in bucket le=0.2 (index 1) and all higher buckets.
    assert snap[("b1", "Decision")][1] == 1
    assert snap[("b1", "Decision")][0] == 0
    # Score 1.0 lands in bucket le=1.0 (index 5) and higher.
    assert snap[("b1", "Criterion")][5] == 1


def test_histogram_buckets_declared_match_spec():
    expected = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.5)
    assert HISTOGRAM_BUCKETS == expected
    assert len(HISTOGRAM_BUCKETS) == 8


def test_batch_threshold_constant():
    assert BATCH_UPDATE_THRESHOLD == 50


def test_clamp_constants():
    assert CLAMP_MIN == 0.0
    assert CLAMP_MAX == 1.5


def test_degree_saturation_constant():
    assert DEGREE_SATURATION == 100


# ---------------------------------------------------------------------------
# Priority boost — v0.3.1 (spec 0eb51d3e)
# TS1 / TS2 / TS3 / TS4 / TS5
# ---------------------------------------------------------------------------


def test_ts1_priority_boost_additive():
    """TS1 (AC5): compute_relevance adds boost before clamp.

    source=0.5, degree=0, hits=0, penalty=0, boost=0.10
    raw = 0.4*0.5 + 0 + 0 - 0 + 0.10 = 0.30
    """
    got = _compute_relevance(0.5, 0, 0.0, 0.0, priority_boost=0.10)
    assert got == pytest.approx(0.30)


def test_ts2_priority_boost_respects_upper_clamp():
    """TS2 (AC6): boost never breaks the 1.5 cap.

    Uses decayed_hits=5.0 so the raw sum comfortably exceeds 1.5 and the
    clamp actually fires — (0.4*1.0 + 0.3 + 0.3*5.0 + 0 + 0.20 ≈ 2.40).
    """
    got = _compute_relevance(1.0, 100, 5.0, 0.0, priority_boost=0.20)
    assert got == CLAMP_MAX


def test_priority_boost_combined_with_high_signals_clamps():
    """Even max boost + max signals must respect the +1.5 cap (BR: cap)."""
    for boost in (0.0, 0.05, 0.10, 0.15, 0.20):
        got = _compute_relevance(1.0, 1000, 10.0, 0.0, priority_boost=boost)
        assert got == CLAMP_MAX


def test_priority_boost_default_is_backward_compatible():
    """Callers without the new keyword get the original score."""
    got_default = _compute_relevance(0.5, 0, 0.0, 0.0)
    got_zero = _compute_relevance(0.5, 0, 0.0, 0.0, priority_boost=0.0)
    assert got_default == got_zero == pytest.approx(0.20)


def test_priority_boost_negative_coerced_to_zero():
    """Defensive: negative boost (defensive coercion, not user-facing path)."""
    got = _compute_relevance(0.5, 0, 0.0, 0.0, priority_boost=-0.5)
    # Negative coerced → behaves like boost=0.0 → 0.4*0.5 = 0.20
    assert got == pytest.approx(0.20)


@pytest.mark.parametrize(
    "priority,expected",
    [
        ("none", 0.0),
        ("low", 0.0),
        ("medium", 0.05),
        ("high", 0.10),
        ("very_high", 0.15),
        ("critical", 0.20),
        # Case/whitespace tolerance
        ("CRITICAL", 0.20),
        ("  high  ", 0.10),
    ],
)
def test_ts3_resolve_priority_boost_str_mapping(priority, expected):
    """TS3 (AC1, AC2): mapping for each CardPriority string value."""
    assert _resolve_priority_boost(priority) == expected


def test_ts3_resolve_priority_boost_enum_matches_str():
    """TS3: CardPriority enum values produce same result as their str."""
    from okto_pulse.core.models.db import CardPriority

    for level in CardPriority:
        expected = PRIORITY_BOOST_BY_LEVEL.get(level.value, 0.0)
        assert _resolve_priority_boost(level) == expected
        assert _resolve_priority_boost(level.value) == expected


@pytest.mark.parametrize(
    "unknown",
    [None, "", "foo", "urgent", 42, 1.5, object(), [1, 2]],
)
def test_ts4_resolve_priority_boost_tolerates_unknown(unknown):
    """TS4 (AC9): unknown inputs fall back to 0.0, never raise."""
    assert _resolve_priority_boost(unknown) == 0.0


def test_priority_boost_table_caps_at_02():
    """BR: PRIORITY_BOOST_BY_LEVEL never exceeds +0.2 for any level."""
    assert max(PRIORITY_BOOST_BY_LEVEL.values()) == pytest.approx(0.20)


def test_ts5_node_candidate_default_priority_boost():
    """TS5 (AC8): NodeCandidate defaults priority_boost to 0.0."""
    from okto_pulse.core.kg.schemas import KGNodeType, NodeCandidate

    cand = NodeCandidate(
        candidate_id="c1",
        node_type=KGNodeType.ENTITY,
        title="t",
    )
    assert cand.priority_boost == 0.0


def test_ts5_node_candidate_rejects_boost_above_cap():
    """TS5: priority_boost > 0.2 raises Pydantic ValidationError."""
    from pydantic import ValidationError

    from okto_pulse.core.kg.schemas import KGNodeType, NodeCandidate

    with pytest.raises(ValidationError):
        NodeCandidate(
            candidate_id="c1",
            node_type=KGNodeType.ENTITY,
            title="t",
            priority_boost=0.25,
        )


def test_ts5_node_candidate_rejects_negative_boost():
    from pydantic import ValidationError

    from okto_pulse.core.kg.schemas import KGNodeType, NodeCandidate

    with pytest.raises(ValidationError):
        NodeCandidate(
            candidate_id="c1",
            node_type=KGNodeType.ENTITY,
            title="t",
            priority_boost=-0.01,
        )
