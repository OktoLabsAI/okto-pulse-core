"""Tests for daily velocity + spec/sprint event overlays (spec 630ce8fd)."""

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.routing import APIRoute

from okto_pulse.core.api.analytics import (
    _bucket_key,
    _build_velocity_buckets,
    router,
)


class TestRouteContract:
    def test_velocity_endpoint_accepts_granularity(self):
        route = next(
            r for r in router.routes
            if isinstance(r, APIRoute)
            and r.path == "/boards/{board_id}/analytics/velocity"
        )
        param_names = {p.name for p in route.dependant.query_params}
        assert "granularity" in param_names
        assert "days" in param_names
        assert "weeks" in param_names


class TestBucketKey:
    def test_day_granularity_uses_iso_date(self):
        dt = datetime(2026, 4, 19, 14, 30, tzinfo=timezone.utc)
        assert _bucket_key(dt, "day") == "2026-04-19"

    def test_week_granularity_aligns_to_monday(self):
        # 2026-04-19 is a Sunday → previous Monday is 2026-04-13
        dt = datetime(2026, 4, 19, 14, 30, tzinfo=timezone.utc)
        assert _bucket_key(dt, "week") == "2026-04-13"

    def test_week_granularity_for_monday(self):
        # Monday itself stays the same
        dt = datetime(2026, 4, 13, 9, 0, tzinfo=timezone.utc)
        assert _bucket_key(dt, "week") == "2026-04-13"


class TestBuildBuckets:
    def _make_done_card(self, updated_at, card_type="normal"):
        class Card:
            pass
        c = Card()
        c.updated_at = updated_at
        c.validations = None

        class _CT:
            def __init__(self, v):
                self.value = v

            def __str__(self):
                return self.v if hasattr(self, "v") else str.__str__(self)

        # Avoid relying on real CardType — the analytics _is_test_card / _is_bug_card
        # helpers call str(card_type) and str.endswith("test"/"bug").
        c.card_type = card_type
        return c

    def test_empty_inputs_return_zeroed_buckets(self):
        result = _build_velocity_buckets(
            done_cards=[], all_cards=[],
            periods=7, granularity="day",
            spec_moves=[], sprint_moves=[],
        )
        assert len(result) == 7
        for bucket in result:
            assert bucket["impl"] == 0
            assert bucket["spec_done"] == 0
            assert bucket["sprint_done"] == 0

    def test_spec_done_only_counts_done_status(self):
        now = datetime.now(timezone.utc)
        today = now.replace(hour=12, minute=0, second=0, microsecond=0)
        spec_moves = [
            (today, "done"),
            (today, "in_progress"),  # should NOT count
            (today, "done"),
        ]
        result = _build_velocity_buckets(
            done_cards=[], all_cards=[],
            periods=7, granularity="day",
            spec_moves=spec_moves, sprint_moves=[],
        )
        today_bucket = [b for b in result if b["day"] == today.strftime("%Y-%m-%d")][0]
        assert today_bucket["spec_done"] == 2

    def test_sprint_done_only_counts_closed_status(self):
        now = datetime.now(timezone.utc)
        today = now.replace(hour=12, minute=0, second=0, microsecond=0)
        sprint_moves = [
            (today, "closed"),
            (today, "active"),  # should NOT count
        ]
        result = _build_velocity_buckets(
            done_cards=[], all_cards=[],
            periods=7, granularity="day",
            spec_moves=[], sprint_moves=sprint_moves,
        )
        today_bucket = [b for b in result if b["day"] == today.strftime("%Y-%m-%d")][0]
        assert today_bucket["sprint_done"] == 1

    def test_old_events_outside_period_dropped(self):
        # Event 100 days ago — outside a 7-day window
        old = datetime.now(timezone.utc) - timedelta(days=100)
        result = _build_velocity_buckets(
            done_cards=[], all_cards=[],
            periods=7, granularity="day",
            spec_moves=[(old, "done")], sprint_moves=[],
        )
        assert sum(b["spec_done"] for b in result) == 0

    def test_day_vs_week_bucket_shape(self):
        day_result = _build_velocity_buckets(
            done_cards=[], all_cards=[],
            periods=14, granularity="day",
            spec_moves=[], sprint_moves=[],
        )
        week_result = _build_velocity_buckets(
            done_cards=[], all_cards=[],
            periods=14, granularity="week",
            spec_moves=[], sprint_moves=[],
        )
        assert "day" in day_result[0]
        assert "week" in week_result[0]
        assert "day" not in week_result[0]
