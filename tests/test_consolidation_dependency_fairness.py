from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors.consolidation import _select_board_aware_entries


NOW = datetime(2026, 9, 7, 13, tzinfo=timezone.utc)
WAIT = "relational_projection_endpoint_pending:prerequisite not materialized"


def _row(ident, *, board="board", error=None, retry=None, source="deterministic_projection_repair", kind="consolidate"):
    return SimpleNamespace(
        id=ident, board_id=board, artifact_type="spec", artifact_id=ident,
        last_error=error, next_retry_at=retry, source=source, work_kind=kind,
        triggered_at=NOW - timedelta(minutes=10),
    )


def _select(rows, now=NOW, claimed=frozenset(), limit=10):
    return _select_board_aware_entries(rows, claimed_board_ids=claimed, limit=limit, now=now)


@pytest.mark.parametrize("delta", [-3600, -1, 0, 1, 3600])
def test_typed_dependency_wait_yields_to_prerequisite_even_after_retry_due(delta):
    dependent = _row("dependent-first-uuid", error=WAIT, retry=NOW + timedelta(seconds=delta))
    prerequisite = _row("prerequisite-last-uuid")
    other_board = _row("other-board", board="other")
    assert _select([dependent, prerequisite, other_board]) == [prerequisite, other_board]


def test_three_source_chain_converges_without_timing_assumption():
    leaf, middle, root = _row("leaf"), _row("middle"), _row("root")
    pending = [leaf, middle, root]
    completed = set()
    attempts = []
    now = NOW
    needs = {"leaf": "middle", "middle": "root"}
    for _ in range(5):
        row, = _select(pending, now=now)
        attempts.append(row.id)
        if row.id in needs and needs[row.id] not in completed:
            row.last_error = WAIT
            row.next_retry_at = now + timedelta(seconds=1)
        else:
            completed.add(row.id)
            pending.remove(row)
        # Every next attempt happens long AFTER the retry is already due.
        now += timedelta(minutes=2)
    assert attempts == ["leaf", "middle", "root", "leaf", "middle"]
    row, = _select(pending, now=now)
    assert row is leaf
    assert needs[row.id] in completed


def test_all_typed_waits_rotate_oldest_deferral_without_spinning():
    a = _row("lowest-uuid", error=WAIT, retry=NOW - timedelta(seconds=2))
    b = _row("highest-uuid", error=WAIT, retry=(NOW - timedelta(seconds=3)).replace(tzinfo=None))
    assert _select([a, b]) == [b]
    b.next_retry_at = NOW + timedelta(seconds=1)
    assert _select([a, b], now=NOW + timedelta(seconds=5)) == [a]
    a.next_retry_at = NOW + timedelta(seconds=10)
    b.next_retry_at = NOW + timedelta(seconds=11)
    assert _select([a, b]) == []


@pytest.mark.parametrize("error,source,kind", [
    ("lease_timeout:busy", "deterministic_projection_repair", "consolidate"),
    ("unexpected relational_projection_endpoint_pending:detail", "deterministic_projection_repair", "consolidate"),
    (WAIT, "rebuild:exact-membership", "consolidate"),
    (WAIT, "deterministic_projection_repair", "stale_reconcile"),
], ids=["ordinary-error", "untyped-substring", "exact-rebuild", "different-work-kind"])
def test_ordinary_errors_and_rebuild_keep_original_backoff_barrier(error, source, kind):
    head = _row("head", error=error, retry=NOW + timedelta(seconds=30), source=source, kind=kind)
    sibling = _row("sibling")
    other = _row("other", board="other")
    assert _select([head, sibling, other]) == [other]
    assert _select([head, sibling, other], now=NOW + timedelta(minutes=1)) == [head, other]


def test_yielding_prefix_never_bypasses_an_ordinary_backoff_or_rebuild_boundary():
    deferred = _row("dependent", error=WAIT, retry=NOW - timedelta(seconds=30))
    barrier = _row("ordinary-failure", error="lease_timeout:busy", retry=NOW + timedelta(seconds=30))
    assert _select([deferred, barrier, _row("ready-after-barrier")]) == []
    rebuild = _row("exact", source="rebuild:manifest")
    assert _select([deferred, rebuild]) == [deferred]
    assert _select([rebuild, deferred]) == [rebuild]


def test_claimed_board_and_batch_limit_still_hold_after_dependency_promotion():
    deferred = _row("dependent", error=WAIT, retry=NOW)
    prerequisite = _row("prerequisite")
    other = _row("other", board="other")
    assert _select([deferred, prerequisite, other], claimed=frozenset({"board"})) == [other]
    assert _select([deferred, prerequisite, other], limit=1) == [prerequisite]
