"""Cursor-based pagination tests for okto_pulse_get_activity_log.

Spec `353f656f` (follow-up Story 3): keyset pagination via opaque base64 cursor,
envelope=true opt-in, edition-supplied legacy-offset escape hatch.

Test cards in Okto Pulse (board 1dad8706):
  - TC1 6e7362ec — helpers unit (ts_a0849bc9, ts_e118d46c)
  - TC2 65897ff2 — paginação básica (ts_0733628b, ts_4e97d70a, ts_ae581cbb)
  - TC3 2d6bde8a — keyset correctness (ts_c51b1002, ts_d117a139, ts_08ce92e5)
  - TC4 17bc0075 — envelope/legacy/filtros (ts_bc9a61f4, ts_f08fd5bd, ts_6714cf3f)
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.server import (
    _decode_activity_cursor,
    _encode_activity_cursor,
)
from sqlalchemy_test_models import ActivityLog, Board


# ---------------------------------------------------------------------------
# Helpers — auth stub mirroring tests/test_architecture_mcp.py pattern
# ---------------------------------------------------------------------------


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": "test-agent",
            "agent_name": "cursor-pagination-test",
            "board_id": board_id,
            "permissions": ["board:read", "board.activity_read"],
        },
    )()


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed_board(db_factory, board_id: str) -> None:
    """Create the Board row once (boards.id is UNIQUE)."""
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id, name=f"Cursor test {board_id[:8]}", owner_id="test-agent"
            )
        )
        await db.commit()


async def _seed_logs(
    db_factory,
    *,
    board_id: str,
    count: int,
    action: str = "card_moved",
    card_id: str | None = None,
    base_time: datetime | None = None,
    timestamp_step_seconds: float = 1.0,
) -> None:
    """Seed N ActivityLog rows with deterministic ascending timestamps.

    Assumes Board already exists (call _seed_board first).
    """
    if base_time is None:
        base_time = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

    async with db_factory() as db:
        for i in range(count):
            db.add(
                ActivityLog(
                    id=str(uuid.uuid4()),
                    board_id=board_id,
                    card_id=card_id or _id("card"),
                    action=action,
                    actor_type="user",
                    actor_id="test-agent",
                    actor_name="cursor-test",
                    details={"from_status": "a", "to_status": "b"},
                    created_at=base_time
                    + timedelta(seconds=i * timestamp_step_seconds),
                )
            )
        await db.commit()


async def _call_tool(db_factory, **kwargs) -> str:
    """Invoke okto_pulse_get_activity_log.fn with stubbed auth + registered factory."""
    board_id = kwargs["board_id"]
    register_mcp_test_runtime(db_factory)
    with (
        patch.object(
            mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        return await mcp_server.okto_pulse_get_activity_log.fn(**kwargs)


# ---------------------------------------------------------------------------
# Unit tests — encode/decode helpers (TC1, scenarios ts_a0849bc9, ts_e118d46c)
# ---------------------------------------------------------------------------


def test_encode_decode_round_trip():
    """ts_a0849bc9 — encoded cursor decodes back to identical (ts, id)."""
    ts = datetime(2026, 5, 17, 12, 30, 45, 123456, tzinfo=timezone.utc)
    cursor = _encode_activity_cursor(ts, "row-abc-123")
    assert isinstance(cursor, str)
    decoded = _decode_activity_cursor(cursor)
    assert decoded == (ts, "row-abc-123")


def test_encode_produces_opaque_base64():
    """Sanity: cursor is base64-url-safe (no plain-text artifacts)."""
    cursor = _encode_activity_cursor(
        datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc), "x"
    )
    assert "ts" not in cursor  # not plain JSON
    assert "id" not in cursor  # not plain JSON
    # base64 url-safe alphabet only
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=")
    assert all(c in allowed for c in cursor)


def test_decode_invalid_returns_none():
    """ts_e118d46c — malformed inputs return None (never raise)."""
    assert _decode_activity_cursor("not-base64-at-all!!!") is None
    assert _decode_activity_cursor("aGVsbG8=") is None  # valid b64 but not JSON
    assert _decode_activity_cursor("eyJmb28iOiAxfQ==") is None  # JSON but no ts/id


def test_decode_empty_returns_none():
    """Empty cursor is "no cursor" — None signals first-page semantics."""
    assert _decode_activity_cursor("") is None


def test_decode_missing_fields_returns_none():
    """JSON with only `ts` or only `id` returns None (both required)."""
    import base64

    payload_only_ts = base64.urlsafe_b64encode(
        json.dumps({"ts": "2026-05-17T12:00:00"}).encode()
    ).decode()
    payload_only_id = base64.urlsafe_b64encode(
        json.dumps({"id": "row-x"}).encode()
    ).decode()
    assert _decode_activity_cursor(payload_only_ts) is None
    assert _decode_activity_cursor(payload_only_id) is None


def test_encode_preserves_microseconds():
    """Microsecond precision survives the encode/decode cycle."""
    ts = datetime(2026, 5, 17, 12, 30, 45, 999999, tzinfo=timezone.utc)
    decoded = _decode_activity_cursor(_encode_activity_cursor(ts, "row"))
    assert decoded is not None
    assert decoded[0].microsecond == 999999


# ---------------------------------------------------------------------------
# Integration — first page + cursor continuation (TC2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_page_no_cursor(db_factory):
    """ts_0733628b — empty cursor returns the N newest rows."""
    board_id = _id("board-first-page")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=20)

    raw = await _call_tool(db_factory, board_id=board_id, limit=5)
    rows = json.loads(raw)

    assert isinstance(rows, list)
    assert len(rows) == 5


@pytest.mark.asyncio
async def test_second_page_uses_cursor_from_first(db_factory):
    """ts_4e97d70a — cursor from batch 1 produces batch 2 without overlap."""
    board_id = _id("board-second-page")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=20)

    first_raw = await _call_tool(db_factory, board_id=board_id, limit=5, envelope=True)
    first = json.loads(first_raw)
    assert len(first["items"]) == 5
    assert first["next_cursor"] is not None

    second_raw = await _call_tool(
        db_factory,
        board_id=board_id,
        limit=5,
        cursor=first["next_cursor"],
        envelope=True,
    )
    second = json.loads(second_raw)
    assert len(second["items"]) == 5

    first_ids = {r["id"] for r in first["items"]}
    second_ids = {r["id"] for r in second["items"]}
    assert first_ids.isdisjoint(second_ids), "no row may appear in both batches"


@pytest.mark.asyncio
async def test_end_of_stream_returns_null_cursor(db_factory):
    """ts_ae581cbb — last page has next_cursor=null."""
    board_id = _id("board-eos")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=15)

    first_raw = await _call_tool(db_factory, board_id=board_id, limit=10, envelope=True)
    first = json.loads(first_raw)
    assert len(first["items"]) == 10
    assert first["next_cursor"] is not None

    second_raw = await _call_tool(
        db_factory,
        board_id=board_id,
        limit=10,
        cursor=first["next_cursor"],
        envelope=True,
    )
    second = json.loads(second_raw)
    assert len(second["items"]) == 5
    assert second["next_cursor"] is None, "end-of-stream must be null cursor"


# ---------------------------------------------------------------------------
# Integration — keyset correctness (TC3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_keyset_filter_correctness(db_factory):
    """ts_c51b1002 — every row delivered has (created_at, id) < cursor pair."""
    board_id = _id("board-keyset")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=20)

    first_raw = await _call_tool(db_factory, board_id=board_id, limit=5, envelope=True)
    first = json.loads(first_raw)
    cursor_pair = _decode_activity_cursor(first["next_cursor"], board_id=board_id)
    assert cursor_pair is not None
    cursor_ts, cursor_id = cursor_pair

    second_raw = await _call_tool(
        db_factory,
        board_id=board_id,
        limit=20,
        cursor=first["next_cursor"],
        envelope=True,
    )
    second = json.loads(second_raw)

    for row in second["items"]:
        row_ts = datetime.fromisoformat(row["created_at"])
        assert (row_ts, row["id"]) < (cursor_ts, cursor_id), (
            f"row {row['id']!s:.8} broke keyset invariant"
        )


@pytest.mark.asyncio
async def test_stable_order_with_identical_timestamps(db_factory):
    """ts_d117a139 — rows with identical created_at use id DESC as tiebreaker."""
    board_id = _id("board-stable")
    same_ts = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)
    await _seed_board(db_factory, board_id)
    # Seed 5 rows with the SAME timestamp (step=0)
    await _seed_logs(
        db_factory,
        board_id=board_id,
        count=5,
        base_time=same_ts,
        timestamp_step_seconds=0,
    )

    # Paginate page-by-page; collect all rows; assert no duplicates and no gaps
    all_seen: list[str] = []
    cursor = ""
    while True:
        raw = await _call_tool(
            db_factory, board_id=board_id, limit=2, cursor=cursor, envelope=True
        )
        page = json.loads(raw)
        page_ids = [r["id"] for r in page["items"]]
        all_seen.extend(page_ids)
        if page["next_cursor"] is None:
            break
        cursor = page["next_cursor"]

    assert len(all_seen) == 5
    assert len(set(all_seen)) == 5, "no row should repeat across pages"


@pytest.mark.asyncio
async def test_limit_clamp_preserved_with_cursor(db_factory):
    """ts_08ce92e5 — clamp limit=min(limit, 200) regression with cursor present."""
    board_id = _id("board-clamp")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=300)

    # No cursor: clamp still applies
    raw1 = await _call_tool(db_factory, board_id=board_id, limit=500)
    rows1 = json.loads(raw1)
    assert len(rows1) == 200, "limit=500 must clamp to 200"

    # With cursor: still clamped
    first_raw = await _call_tool(db_factory, board_id=board_id, limit=10, envelope=True)
    first = json.loads(first_raw)
    raw2 = await _call_tool(
        db_factory, board_id=board_id, limit=500, cursor=first["next_cursor"]
    )
    rows2 = json.loads(raw2)
    assert len(rows2) == 200, "limit=500 with cursor must also clamp to 200"


# ---------------------------------------------------------------------------
# Integration — envelope + legacy_offset + filters (TC4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_envelope_flag_opt_in(db_factory):
    """ts_bc9a61f4 — envelope=false returns list (Story 3 compat); =true returns dict."""
    board_id = _id("board-envelope")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=10)

    raw_list = await _call_tool(db_factory, board_id=board_id, limit=5, envelope=False)
    parsed_list = json.loads(raw_list)
    assert isinstance(parsed_list, list)

    raw_dict = await _call_tool(db_factory, board_id=board_id, limit=5, envelope=True)
    parsed_dict = json.loads(raw_dict)
    assert isinstance(parsed_dict, dict)
    assert set(parsed_dict.keys()) == {"items", "next_cursor"}
    assert len(parsed_dict["items"]) == 5


@pytest.mark.asyncio
async def test_offset_honored_on_first_page_and_ignored_with_cursor(db_factory):
    """ts_f08fd5bd (E2E remediation item 2) — the public ``offset`` is honored on
    the FIRST page (no cursor) and MUST NOT be a decorative parameter; once paging
    by ``cursor`` the cursor's keyset position is authoritative and offset is
    ignored (pairing them would double-skip)."""
    board_id = _id("board-offset")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=20)

    all_ids = [
        r["id"]
        for r in json.loads(await _call_tool(db_factory, board_id=board_id, limit=20))
    ]
    first_ids = [
        r["id"]
        for r in json.loads(await _call_tool(db_factory, board_id=board_id, limit=5))
    ]
    assert first_ids == all_ids[:5]

    # offset skips rows on the first page — it is NOT ignored (the pre-fix bug).
    off3_ids = [
        r["id"]
        for r in json.loads(
            await _call_tool(db_factory, board_id=board_id, limit=5, offset=3)
        )
    ]
    assert off3_ids != first_ids, "offset=3 must not repeat the first page"
    assert off3_ids == all_ids[3:8], "offset=3 must skip exactly the first 3 rows"

    # With a cursor, offset is ignored (cursor authoritative, no double-skip).
    env = json.loads(
        await _call_tool(db_factory, board_id=board_id, limit=5, envelope=True)
    )
    next_cursor = env["next_cursor"]
    assert next_cursor
    via_cursor = [
        r["id"]
        for r in json.loads(
            await _call_tool(db_factory, board_id=board_id, limit=5, cursor=next_cursor)
        )
    ]
    via_cursor_with_offset = [
        r["id"]
        for r in json.loads(
            await _call_tool(
                db_factory, board_id=board_id, limit=5, cursor=next_cursor, offset=7
            )
        )
    ]
    assert via_cursor == via_cursor_with_offset, (
        "offset must be ignored when a cursor is supplied"
    )
    # The cursor's second page continues after the first (no overlap / no re-skip).
    assert not (set(via_cursor) & set(first_ids))


@pytest.mark.asyncio
async def test_filters_action_and_card_compose_with_cursor(db_factory):
    """ts_6714cf3f — action + card_id filters AND with cursor keyset."""
    board_id = _id("board-filters")
    target_card = _id("card-target")
    other_card = _id("card-other")
    await _seed_board(db_factory, board_id)
    # 10 rows for target card, 10 for others, all action=card_moved
    await _seed_logs(db_factory, board_id=board_id, count=10, card_id=target_card)
    await _seed_logs(
        db_factory,
        board_id=board_id,
        count=10,
        card_id=other_card,
        base_time=datetime(2026, 5, 1, 14, 0, 0, tzinfo=timezone.utc),
    )

    first_raw = await _call_tool(
        db_factory,
        board_id=board_id,
        limit=3,
        action="card_moved",
        card_id=target_card,
        envelope=True,
    )
    first = json.loads(first_raw)
    assert len(first["items"]) == 3
    for row in first["items"]:
        assert row["card_id"] == target_card

    second_raw = await _call_tool(
        db_factory,
        board_id=board_id,
        limit=3,
        action="card_moved",
        card_id=target_card,
        cursor=first["next_cursor"],
        envelope=True,
    )
    second = json.loads(second_raw)
    for row in second["items"]:
        assert row["card_id"] == target_card


# ---------------------------------------------------------------------------
# Integration — invalid cursor returns structured error (FR5, BR br_acd0b762)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_cursor_returns_error_envelope(db_factory):
    """Cursor non-empty but malformed returns {error_code: invalid_cursor}."""
    board_id = _id("board-invalid")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=5)

    raw = await _call_tool(
        db_factory, board_id=board_id, limit=5, cursor="not-a-valid-cursor-at-all!!!"
    )
    parsed = json.loads(raw)
    assert isinstance(parsed, dict)
    assert parsed.get("error_code") == "invalid_cursor"
    assert "error" in parsed


@pytest.mark.asyncio
async def test_cursor_is_bound_to_issuing_board(db_factory):
    """A valid cursor from board A is rejected when replayed against board B."""
    board_a = _id("board-cursor-scope-a")
    board_b = _id("board-cursor-scope-b")
    await _seed_board(db_factory, board_a)
    await _seed_board(db_factory, board_b)
    await _seed_logs(db_factory, board_id=board_a, count=4)
    await _seed_logs(db_factory, board_id=board_b, count=4)

    first = json.loads(
        await _call_tool(
            db_factory,
            board_id=board_a,
            limit=2,
            envelope=True,
        )
    )
    assert first["next_cursor"]

    replay = json.loads(
        await _call_tool(
            db_factory,
            board_id=board_b,
            limit=2,
            cursor=first["next_cursor"],
            envelope=True,
        )
    )
    assert replay["error_code"] == "cursor_scope_mismatch"


# ---------------------------------------------------------------------------
# Regression — cursor pointing to a row MUST NOT redeliver that row
# (smoke E2E pós-restart detectou: sqlalchemy tuple_(col1, col2) < (val1, val2)
# em SQLite não honra row comparison e degenera para col1 < val1, deixando o
# tiebreaker passar. Fix: or_(col1 < val1, and_(col1 == val1, col2 < val2)).)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cursor_strict_less_than_excludes_anchor_row(db_factory):
    """Bug regression: when cursor anchors on row R, batch 2 must NOT include R.

    Builds a cursor MANUALLY for an existing row in the DB and asserts the
    very next call with that cursor returns rows ONLY older than R (by ts)
    or with same ts and id strictly less than R.id.
    """
    board_id = _id("board-strict-less")
    await _seed_board(db_factory, board_id)
    await _seed_logs(db_factory, board_id=board_id, count=10)

    first_raw = await _call_tool(db_factory, board_id=board_id, limit=5, envelope=True)
    first = json.loads(first_raw)
    anchor = first["items"][-1]  # last row of first batch
    anchor_ts = datetime.fromisoformat(anchor["created_at"])

    # Forge cursor pointing exactly at the anchor (same as next_cursor would)
    forged = _encode_activity_cursor(anchor_ts, anchor["id"], board_id=board_id)

    second_raw = await _call_tool(
        db_factory, board_id=board_id, limit=10, cursor=forged, envelope=True
    )
    second = json.loads(second_raw)

    second_ids = [r["id"] for r in second["items"]]
    assert anchor["id"] not in second_ids, (
        f"anchor row {anchor['id']!r} must NOT appear in batch 2 — "
        "keyset filter must be strict-less-than"
    )
    for row in second["items"]:
        row_ts = datetime.fromisoformat(row["created_at"])
        assert (row_ts, row["id"]) < (anchor_ts, anchor["id"]), (
            f"row {row['id']!r} broke strict-less-than invariant"
        )
