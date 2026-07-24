from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
    _parse_dt,
)
from okto_pulse.core.domain.enums import CardType
from okto_pulse.core.services.analytics_contract import (
    ACTIVITY_CURSOR_VERSION,
    classify_analytics_card,
    decode_activity_cursor,
    encode_activity_cursor,
    parse_analytics_datetime,
    partition_analytics_cards,
)
from okto_pulse.core.services.analytics_service import _artifact_filters


def _card(card_type, scenarios=None):
    return SimpleNamespace(card_type=card_type, test_scenario_ids=scenarios or [])


def test_date_only_range_is_utc_half_open_and_timestamp_preserves_instant():
    assert parse_analytics_datetime("2026-07-14") == datetime(
        2026, 7, 14, tzinfo=timezone.utc
    )
    assert parse_analytics_datetime("2026-07-14", end_exclusive=True) == datetime(
        2026, 7, 15, tzinfo=timezone.utc
    )
    assert parse_analytics_datetime(
        "2026-07-14T18:30:15-03:00", end_exclusive=True
    ) == datetime(2026, 7, 14, 21, 30, 15, tzinfo=timezone.utc)
    assert parse_analytics_datetime("not-a-date") is None


def test_mcp_and_analytics_filters_use_the_same_exclusive_upper_bound():
    upper = _parse_dt("2026-07-14", end_exclusive=True)
    filters = _artifact_filters(
        "board",
        include_archived=False,
        dt_from=_parse_dt("2026-07-14"),
        dt_to=upper,
    )

    temporal = [(item.field, item.operator, item.value) for item in filters]
    assert ("created_at", "gte", datetime(2026, 7, 14, tzinfo=timezone.utc)) in temporal
    assert ("created_at", "lt", datetime(2026, 7, 15, tzinfo=timezone.utc)) in temporal


def test_card_categories_are_disjoint_with_bug_test_implementation_precedence():
    cards = [
        _card(CardType.BUG, ["scenario-even-on-bug"]),
        _card(CardType.TEST, ["scenario"]),
        _card(CardType.NORMAL),
        _card(CardType.NORMAL, ["legacy-test-signal"]),
        _card(None),
    ]

    assert classify_analytics_card(cards[0]) == "bug"
    assert classify_analytics_card(cards[1]) == "test"
    assert classify_analytics_card(cards[2]) == "implementation"
    assert classify_analytics_card(cards[3]) == "test"
    assert classify_analytics_card(cards[4]) == "implementation"
    partitions = partition_analytics_cards(cards)
    assert {name: len(rows) for name, rows in partitions.items()} == {
        "implementation": 2,
        "test": 2,
        "bug": 1,
    }
    assert sum(len(rows) for rows in partitions.values()) == len(cards)


def test_activity_cursor_v2_round_trip_is_exclusive_scope_bound_and_utc():
    timestamp = datetime(2026, 7, 14, 10, 11, 12, 123456)
    cursor = encode_activity_cursor(
        timestamp,
        "row-2",
        board_id="board-a",
        action="card_moved",
        card_id="card-7",
    )
    decoded = decode_activity_cursor(
        cursor,
        board_id="board-a",
        action="card_moved",
        card_id="card-7",
    )

    assert decoded.valid is True
    assert decoded.version == ACTIVITY_CURSOR_VERSION == 2
    assert decoded.position == (
        timestamp.replace(tzinfo=timezone.utc),
        "row-2",
    )


def test_activity_cursor_rejects_filter_mismatch_and_byte_tampering():
    cursor = encode_activity_cursor(
        "2026-07-14T00:00:00Z",
        "row-2",
        action="card_moved",
    )
    mismatch = decode_activity_cursor(cursor, action="spec_moved")
    assert mismatch.error_code == "cursor_scope_mismatch"

    envelope = json.loads(base64.urlsafe_b64decode(cursor).decode("utf-8"))
    envelope["payload"]["id"] = "row-attacker"
    tampered = base64.urlsafe_b64encode(
        json.dumps(envelope, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    rejected = decode_activity_cursor(tampered, action="card_moved")
    assert rejected.error_code == "cursor_integrity_failed"


def test_activity_cursor_rejects_cross_board_replay():
    cursor = encode_activity_cursor(
        "2026-07-14T00:00:00Z",
        "row-board-a",
        board_id="board-a",
    )

    assert (
        decode_activity_cursor(cursor, board_id="board-b").error_code
        == "cursor_scope_mismatch"
    )


def test_activity_cursor_cannot_be_resigned_with_public_context_constant():
    cursor = encode_activity_cursor(datetime(2026, 7, 14, tzinfo=timezone.utc), "row-1")
    envelope = json.loads(base64.urlsafe_b64decode(cursor).decode("utf-8"))
    envelope["payload"]["id"] = "forged-row"
    canonical = json.dumps(
        envelope["payload"], sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    envelope["integrity"] = hmac.new(
        b"okto-pulse.activity-cursor.v2", canonical, hashlib.sha256
    ).hexdigest()
    forged = base64.urlsafe_b64encode(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")

    assert decode_activity_cursor(forged).error_code == "cursor_integrity_failed"


def test_activity_cursor_reads_legacy_only_without_filter_scope():
    legacy = base64.urlsafe_b64encode(
        json.dumps({"ts": "2026-07-14T00:00:00", "id": "legacy-row"}).encode()
    ).decode()

    assert decode_activity_cursor(legacy).position == (
        datetime(2026, 7, 14, tzinfo=timezone.utc),
        "legacy-row",
    )
    assert (
        decode_activity_cursor(legacy, action="card_moved").error_code
        == "cursor_scope_mismatch"
    )
    assert (
        decode_activity_cursor(legacy, board_id="board-a").error_code
        == "cursor_scope_mismatch"
    )


def test_resources_publish_temporal_category_and_cursor_contracts():
    root = (
        Path(__file__).parents[1]
        / "src/okto_pulse/core/mcp/resources/reference/tool-docs"
    )
    analytics = (root / "analytics.md").read_text(encoding="utf-8")
    activity = (root / "activity.md").read_text(encoding="utf-8")

    assert "[from, to)" in analytics
    assert "bug → test → implementation" in analytics
    assert "23:59:59.999999 sentinel is used" in analytics
    assert "cursor_scope_mismatch" in activity
    assert "cursor_integrity_failed" in activity
