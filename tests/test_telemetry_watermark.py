"""Behavioral tests for the local watermark schema (spec R3A, card R3A-A).

Covers FR ``fr_55e194c2`` (stable + orderable per-event cursor, not timestamp
alone), the R3A-A part of ``fr_2dc7b6da`` (advance only forward / idempotent),
TR ``tr_f5b5d90a`` (do not break the existing ``state.json``) and scenario
``ts_0d21a342`` (a legacy state without watermark fields must NOT discard pending
events and must not expose a token/secret in the diagnostic state).

The skew-robust delta *selection* (scenario ``ts_07d9a8b2``) and the wiring of
:func:`advance` into ``send_once`` are R3A-B/C — out of scope here. These tests
exercise the schema + pure primitives this card delivers.
"""

from __future__ import annotations

from pathlib import Path

from okto_pulse.core.telemetry.settings import load_state, save_state
from okto_pulse.core.telemetry.watermark import (
    DEFAULT_NEXT_BATCH_SEQ,
    DEFAULT_RETENTION_DAYS,
    NEXT_BATCH_SEQ_KEY,
    WATERMARK_FIELDS,
    Watermark,
    advance,
    compare_to_cursor,
    event_cursor_tuple,
    load_watermark,
    persist_watermark,
    public_watermark_projection,
    read_watermark,
    write_watermark,
)

# A legacy state.json exactly as scenario ts_0d21a342 describes: it carries the
# install identity/token and next_batch_seq, but NO watermark fields.
_LEGACY_STATE = {
    "install_id": "11111111-2222-3333-4444-555555555555",
    "install_token": "super-secret-token-value",
    "token_hash": "deadbeefdeadbeef",
    "install_token_expires_at": "2026-07-01T00:00:00Z",
    "last_send_at": "2026-06-01T10:00:00Z",
    "next_batch_seq": 7,
    "failure_state": {"status": "ok"},
    "mode": "anonymous_beacon",
}

_SECRET_VALUES = {"super-secret-token-value", "deadbeefdeadbeef"}


def _write_legacy_state(tmp_path: Path, **extra) -> Path:
    metrics_dir = tmp_path / "metrics"
    save_state(metrics_dir, {**_LEGACY_STATE, **extra})
    return metrics_dir


def _event(event_id: str, occurred_at: str) -> dict:
    return {"event_id": event_id, "occurred_at": occurred_at, "event_type": "cli"}


# --- defaults / fresh state ------------------------------------------------


def test_fresh_state_has_empty_cursor_and_documented_defaults() -> None:
    wm = read_watermark({})
    assert wm.is_empty
    assert wm.watermark is None and wm.watermark_event_id is None
    assert wm.pending_event_count == 0
    assert wm.next_batch_seq == DEFAULT_NEXT_BATCH_SEQ == 1
    assert wm.retention_days == DEFAULT_RETENTION_DAYS == 30
    assert wm.cursor_tuple() is None


# --- conservative legacy migration (ts_0d21a342) ---------------------------


def test_legacy_state_migration_is_conservative(tmp_path: Path) -> None:
    """A legacy state migrates with an EMPTY cursor — nothing marked confirmed."""
    metrics_dir = _write_legacy_state(tmp_path)
    wm = read_watermark(load_state(metrics_dir))

    # Cursor stays empty: no pending event is treated as already sent.
    assert wm.is_empty
    assert wm.watermark is None and wm.watermark_event_id is None
    # The legacy send-time sequence is carried over (single source of truth).
    assert wm.next_batch_seq == 7
    assert wm.retention_days == DEFAULT_RETENTION_DAYS
    # We must NOT seed the cursor from last_send_at (that would confirm-and-drop).
    assert wm.watermark != _LEGACY_STATE["last_send_at"]


def test_legacy_empty_cursor_keeps_every_local_event_pending() -> None:
    """With an empty cursor every existing event is "after" it → still pending."""
    wm = read_watermark({"next_batch_seq": 3})
    for ev in (
        _event("e-old", "2026-05-01T00:00:00Z"),
        _event("e-mid", "2026-06-01T00:00:00Z"),
        _event("e-new", "2026-06-15T00:00:00Z"),
    ):
        assert compare_to_cursor(wm, ev) == 1  # 1 == after cursor == pending


def test_diagnostic_projection_omits_token_even_when_injected(tmp_path: Path) -> None:
    """ts_0d21a342: the diagnostic watermark state never exposes a token/secret."""
    metrics_dir = _write_legacy_state(
        tmp_path,
        watermark="2026-06-10T00:00:00Z",
        watermark_event_id="evt-123",
    )
    projection = public_watermark_projection(load_state(metrics_dir))

    assert set(projection) == set(WATERMARK_FIELDS)
    blob = repr(projection)
    for secret in _SECRET_VALUES:
        assert secret not in blob
    assert "install_token" not in projection and "token_hash" not in projection


# --- backward compatibility (tr_f5b5d90a) ----------------------------------


def test_persist_roundtrip_preserves_other_state_keys(tmp_path: Path) -> None:
    metrics_dir = _write_legacy_state(tmp_path)
    wm = Watermark(
        watermark="2026-06-10T00:00:00Z",
        watermark_event_id="evt-abc",
        watermark_updated_at="2026-06-10T00:05:00Z",
        pending_event_count=4,
        next_batch_seq=8,
        retention_days=30,
    )
    persist_watermark(metrics_dir, wm)

    reloaded = load_state(metrics_dir)
    # Every pre-existing key survives untouched.
    assert reloaded["install_token"] == "super-secret-token-value"
    assert reloaded["mode"] == "anonymous_beacon"
    assert reloaded["failure_state"] == {"status": "ok"}
    # And the watermark round-trips.
    assert load_watermark(metrics_dir) == wm


def test_next_batch_seq_is_a_single_flat_source_of_truth(tmp_path: Path) -> None:
    """The watermark reads/writes the SAME flat key the R1 sender uses."""
    metrics_dir = tmp_path / "metrics"
    # Sender-style state advancing the send sequence.
    save_state(metrics_dir, {NEXT_BATCH_SEQ_KEY: 9})
    assert read_watermark(load_state(metrics_dir)).next_batch_seq == 9

    written = write_watermark({"other": 1}, Watermark(next_batch_seq=12))
    # Stored flat (top-level), not nested under a separate block.
    assert written[NEXT_BATCH_SEQ_KEY] == 12
    assert written["other"] == 1
    assert "watermark" not in {k for k in written if isinstance(written.get(k), dict)}


# --- stable + orderable cursor, not timestamp alone (fr_55e194c2) ----------


def test_event_cursor_tuple_breaks_timestamp_ties_by_event_id() -> None:
    same_ts = "2026-06-10T12:00:00Z"
    lo = event_cursor_tuple(_event("aaa", same_ts))
    hi = event_cursor_tuple(_event("bbb", same_ts))
    assert lo < hi  # identical timestamp → ordered deterministically by event_id


def test_compare_distinguishes_events_sharing_a_timestamp() -> None:
    same_ts = "2026-06-10T12:00:00Z"
    wm = Watermark(watermark=same_ts, watermark_event_id="aaa")
    assert compare_to_cursor(wm, _event("aaa", same_ts)) == 0  # the cursor itself
    assert compare_to_cursor(wm, _event("bbb", same_ts)) == 1  # later id → pending
    assert compare_to_cursor(wm, _event("000", same_ts)) == -1  # earlier id → before


def test_cursor_anchors_on_event_id_despite_skewed_timestamp() -> None:
    """The stable event_id anchor wins over the timestamp (no skew corruption)."""
    wm = Watermark(watermark="2026-06-10T12:00:00Z", watermark_event_id="anchor")
    # Same event_id but a clock-skewed EARLIER timestamp is still the cursor (0),
    # not "before" — confirmation is by stable id, not by timestamp alone.
    skewed = _event("anchor", "2026-06-10T11:00:00Z")
    assert compare_to_cursor(wm, skewed) == 0


# --- advance: forward-only / idempotent (fr_2dc7b6da, R3A-A part) -----------


def test_advance_sets_cursor_from_confirmed_event() -> None:
    moved = advance(
        Watermark(pending_event_count=5),
        event_id="evt-1",
        occurred_at="2026-06-10T12:00:00Z",
        updated_at="2026-06-10T12:00:01Z",
        pending_event_count=2,
        next_batch_seq=3,
    )
    assert moved.watermark_event_id == "evt-1"
    assert moved.watermark == "2026-06-10T12:00:00Z"
    assert moved.watermark_updated_at == "2026-06-10T12:00:01Z"
    assert moved.pending_event_count == 2
    assert moved.next_batch_seq == 3


def test_advance_is_monotonic_and_does_not_rewind_on_replay() -> None:
    base = advance(
        Watermark(),
        event_id="evt-2",
        occurred_at="2026-06-10T12:00:00Z",
        updated_at="2026-06-10T12:00:01Z",
    )
    # A replay / out-of-order ack for an OLDER event must not rewind the cursor,
    # but bookkeeping (pending/seq) may still update — idempotent reconciliation.
    replayed = advance(
        base,
        event_id="evt-1",
        occurred_at="2026-06-10T11:00:00Z",
        updated_at="2026-06-10T13:00:00Z",
        pending_event_count=0,
        next_batch_seq=5,
    )
    assert replayed.watermark_event_id == "evt-2"  # cursor unchanged
    assert replayed.watermark == "2026-06-10T12:00:00Z"
    assert replayed.watermark_updated_at == "2026-06-10T12:00:01Z"
    assert replayed.pending_event_count == 0  # counters still reconciled
    assert replayed.next_batch_seq == 5

    # A genuinely newer confirmed event advances the cursor.
    forward = advance(
        base,
        event_id="evt-3",
        occurred_at="2026-06-10T13:00:00Z",
        updated_at="2026-06-10T13:00:01Z",
    )
    assert forward.watermark_event_id == "evt-3"


def test_advance_floors_counters() -> None:
    moved = advance(
        Watermark(),
        event_id="evt-1",
        occurred_at="2026-06-10T12:00:00Z",
        updated_at="2026-06-10T12:00:01Z",
        pending_event_count=-3,
        next_batch_seq=0,
    )
    assert moved.pending_event_count == 0  # never negative
    assert moved.next_batch_seq == DEFAULT_NEXT_BATCH_SEQ  # never below 1


# --- coercion / robustness -------------------------------------------------


def test_read_watermark_coerces_bad_types_to_safe_defaults() -> None:
    wm = read_watermark(
        {
            "watermark": 12345,  # not a str → dropped
            "watermark_event_id": "",  # empty → None
            "pending_event_count": True,  # bool → default 0 (not 1)
            "next_batch_seq": "not-an-int",
            "retention_days": -5,  # negative → default
        }
    )
    assert wm.watermark is None
    assert wm.watermark_event_id is None
    assert wm.pending_event_count == 0
    assert wm.next_batch_seq == DEFAULT_NEXT_BATCH_SEQ
    assert wm.retention_days == DEFAULT_RETENTION_DAYS
