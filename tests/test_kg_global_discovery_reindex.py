"""Tests for KG-02.6 global discovery reindex visibility.

Covers:
* BR br_1dd21e39 — global discovery never silently stale; reindex OR
  mark pending with explicit reason in health + report.
* IR ir_f98042ee — trigger_reasons, visible_statuses, report_required.
* OR or_34a86124 + or_80f18a3a — bounded counter labels.
* API api_396302bd / api_2325f7e1 — frozen response shapes.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from okto_pulse.core.kg.global_discovery_reindex import (
    GlobalDiscoveryReindexStatusStore,
    GlobalDiscoveryReindexer,
    ReindexAttempt,
    ReindexErrorCode,
    ReindexReason,
    ReindexStatus,
    VISIBLE_STATUSES,
    get_reindex_count,
    get_reindex_counter_labels,
    get_reindex_samples,
    reset_reindex_counter,
)
from okto_pulse.core.kg.global_discovery_writer import GlobalDiscoveryWriterLease
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id


BOARD = "board-001"


class _AlwaysOwnedWriterLock:
    def is_owner(self, _board_id: str, _owner_token: str) -> bool:
        return True

    def release(self, *, board_id: str, owner_token: str) -> bool:
        del board_id, owner_token
        return True


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    target = tmp_path / "kg-02-6-reindex"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


@pytest.fixture(autouse=True)
def _reset_counter() -> None:
    reset_reindex_counter()
    lease = GlobalDiscoveryWriterLease(
        lock=_AlwaysOwnedWriterLock(),  # type: ignore[arg-type]
        owner_token="reindex-test-writer",
        operation="test_global_discovery_reindex",
    )
    try:
        with lease.guard():
            yield
    finally:
        lease.release()
        reset_reindex_counter()


# -------- Status store ----------------------------------------------------


def test_record_persists_and_returns_visible_true(base_dir: Path) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    gen = generate_kg_generation_id()
    result = store.record(
        board_id=BOARD,
        kg_generation_id=gen,
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
        status=ReindexStatus.REINDEXED.value,
        indexed_generation=gen,
    )
    assert result.visible_in_health is True
    assert result.visible_in_report is True
    assert result.recorded_at is not None
    assert result.record_ref is not None
    assert Path(result.record_ref).exists()

    loaded = store.get_status(BOARD, gen)
    assert loaded is not None
    assert loaded["status"] == ReindexStatus.REINDEXED.value
    assert (
        loaded["reason"] == ReindexReason.DISCOVERY_LBUG_AFFECTED.value
    )
    assert loaded["indexed_generation"] == gen


def test_record_with_invalid_generation_returns_invalid_generation_error(
    base_dir: Path,
) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    result = store.record(
        board_id=BOARD,
        kg_generation_id="not-a-uuid",
        reason=ReindexReason.OPERATOR_REQUESTED.value,
        status=ReindexStatus.REINDEXED.value,
    )
    assert result.visible_in_health is False
    assert result.visible_in_report is False
    assert result.error_code == ReindexErrorCode.INVALID_GENERATION.value


def test_record_unknown_reason_coerced_to_operator_requested(
    base_dir: Path,
) -> None:
    """Bounded counter label — unknown reasons fall back to
    operator_requested instead of leaking arbitrary strings."""

    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    gen = generate_kg_generation_id()
    store.record(
        board_id=BOARD,
        kg_generation_id=gen,
        reason="arbitrary_reason_from_caller",
        status=ReindexStatus.REINDEXED.value,
    )
    samples = get_reindex_samples()
    assert all(
        s["reason"] != "arbitrary_reason_from_caller" for s in samples
    )
    assert (
        get_reindex_count(
            BOARD,
            reason=ReindexReason.OPERATOR_REQUESTED.value,
            status=ReindexStatus.REINDEXED.value,
        )
        == 1
    )


def test_record_unknown_status_falls_back_to_failed(
    base_dir: Path,
) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    gen = generate_kg_generation_id()
    store.record(
        board_id=BOARD,
        kg_generation_id=gen,
        reason=ReindexReason.OPERATOR_REQUESTED.value,
        status="bogus_status",
    )
    assert (
        get_reindex_count(
            BOARD, status=ReindexStatus.FAILED.value
        )
        == 1
    )


def test_latest_for_board_returns_most_recent(base_dir: Path) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    older = store.record(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
        status=ReindexStatus.REINDEX_PENDING.value,
        job_ref="job-1",
    )
    # Ensure recorded_at timestamps order correctly even at sub-millisecond
    # granularity by waiting just enough; ISO8601 includes microseconds.
    import time

    time.sleep(0.005)
    newer = store.record(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        reason=ReindexReason.OPERATOR_REQUESTED.value,
        status=ReindexStatus.REINDEXED.value,
    )
    latest = store.latest_for_board(BOARD)
    assert latest is not None
    assert newer.recorded_at > older.recorded_at
    assert latest["recorded_at"] == newer.recorded_at


# -------- Reindexer happy path -------------------------------------------


def test_reindexer_reindexed_path_persists_and_returns_outcome(
    base_dir: Path,
) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    gen = generate_kg_generation_id()

    def _adapter(board, kg_gen, refs):
        return ReindexAttempt(
            success=True,
            indexed_generation=kg_gen,
            detail="ok",
        )

    reindexer = GlobalDiscoveryReindexer(
        status_store=store, reindex_adapter=_adapter
    )
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=gen,
        reason=ReindexReason.STRUCTURAL_REFERENCE_CHANGED.value,
        affected_refs=("spec-1", "spec-2"),
    )
    assert outcome.status == ReindexStatus.REINDEXED.value
    assert outcome.reason == ReindexReason.STRUCTURAL_REFERENCE_CHANGED.value
    assert outcome.report_ref
    assert Path(outcome.report_ref).exists()
    assert outcome.kg_generation_id == gen
    assert outcome.indexed_generation == gen
    assert outcome.error_code is None
    # Counter bumped reindexed.
    assert (
        get_reindex_count(
            BOARD,
            reason=ReindexReason.STRUCTURAL_REFERENCE_CHANGED.value,
            status=ReindexStatus.REINDEXED.value,
        )
        == 1
    )


# -------- Reindexer pending path -----------------------------------------


def test_reindexer_pending_when_adapter_returns_failure(
    base_dir: Path,
) -> None:
    """Adapter returning ``success=False`` lands on the pending path —
    NEVER silently stale."""

    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    gen = generate_kg_generation_id()

    def _adapter(_b, _g, _r):
        return ReindexAttempt(
            success=False,
            job_ref="scheduled-job-123",
            detail="discovery worker offline",
            error_code=ReindexErrorCode.GLOBAL_DISCOVERY_UNAVAILABLE.value,
        )

    reindexer = GlobalDiscoveryReindexer(
        status_store=store, reindex_adapter=_adapter
    )
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=gen,
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
    )
    assert outcome.status == ReindexStatus.REINDEX_PENDING.value
    assert outcome.job_ref == "scheduled-job-123"
    assert outcome.indexed_generation is None
    assert (
        outcome.error_code
        == ReindexErrorCode.GLOBAL_DISCOVERY_UNAVAILABLE.value
    )
    # Status visible via the store.
    loaded = store.get_status(BOARD, gen)
    assert loaded is not None
    assert loaded["status"] == ReindexStatus.REINDEX_PENDING.value
    assert loaded["job_ref"] == "scheduled-job-123"


def test_reindexer_default_adapter_marks_pending(base_dir: Path) -> None:
    """The default adapter must NOT silently succeed — it marks
    pending with manual_reindex_required so the operator action is
    surfaced."""

    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    reindexer = GlobalDiscoveryReindexer(status_store=store)
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        reason=ReindexReason.OPERATOR_REQUESTED.value,
    )
    assert outcome.status == ReindexStatus.REINDEX_PENDING.value
    assert outcome.job_ref == "manual_reindex_required"


def test_reindexer_adapter_exception_lands_on_pending(
    base_dir: Path,
) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)

    def _adapter(_b, _g, _r):
        raise RuntimeError("network blew up")

    reindexer = GlobalDiscoveryReindexer(
        status_store=store, reindex_adapter=_adapter
    )
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
    )
    assert outcome.status == ReindexStatus.REINDEX_PENDING.value
    assert outcome.job_ref == "adapter_exception"
    assert (
        outcome.error_code
        == ReindexErrorCode.GLOBAL_DISCOVERY_UNAVAILABLE.value
    )


# -------- Invalid generation path ----------------------------------------


def test_reindexer_rejects_invalid_generation_returns_failed(
    base_dir: Path,
) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    reindexer = GlobalDiscoveryReindexer(status_store=store)
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id="not-a-uuid",
        reason=ReindexReason.OPERATOR_REQUESTED.value,
    )
    assert outcome.status == ReindexStatus.FAILED.value
    assert outcome.error_code == ReindexErrorCode.INVALID_GENERATION.value


# -------- Counter label invariants --------------------------------------


def test_counter_labels_are_bounded() -> None:
    assert get_reindex_counter_labels() == ("board_id", "reason", "status")


@pytest.mark.parametrize(
    "reason",
    sorted(r.value for r in ReindexReason),
)
def test_each_canonical_reason_yields_visible_status(
    base_dir: Path, reason: str
) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    reindexer = GlobalDiscoveryReindexer(
        status_store=store,
        reindex_adapter=lambda b, g, r: ReindexAttempt(
            success=True, indexed_generation=g
        ),
    )
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        reason=reason,
    )
    assert outcome.reason == reason
    assert outcome.status in VISIBLE_STATUSES


@pytest.mark.parametrize(
    "status",
    sorted(s.value for s in ReindexStatus),
)
def test_each_canonical_status_recorded_is_visible(
    base_dir: Path, status: str
) -> None:
    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    result = store.record(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        reason=ReindexReason.OPERATOR_REQUESTED.value,
        status=status,
    )
    assert result.visible_in_health is True
    assert result.visible_in_report is True


# -------- Visibility invariants per BR br_1dd21e39 -----------------------


# -------- val_bf8c49c2 regression: store failure invalidates outcome ----


class _BrokenStore:
    """Fake status store that always returns store-unavailable. Lets us
    verify the reindexer NEVER claims a status that cannot be made
    visible (BR br_1dd21e39 — no silent stale)."""

    def __init__(self):
        self.calls: list[dict] = []

    def record(self, **kwargs):
        from okto_pulse.core.kg.global_discovery_reindex import (
            ReindexErrorCode,
            ReindexRecordResult,
        )

        self.calls.append(kwargs)
        return ReindexRecordResult(
            visible_in_health=False,
            visible_in_report=False,
            recorded_at=None,
            record_ref=None,
            error_code=ReindexErrorCode.REINDEX_STATUS_STORE_UNAVAILABLE.value,
            detail="forced",
        )


def test_adapter_success_with_store_failure_yields_failed_outcome() -> None:
    """val_bf8c49c2 repro: adapter succeeds but the status store cannot
    persist the visible state. The runtime MUST downgrade to FAILED
    with an empty report_ref — never claim ``reindexed`` without a
    durable record."""

    broken = _BrokenStore()
    reindexer = GlobalDiscoveryReindexer(
        status_store=broken,
        reindex_adapter=lambda b, g, r: ReindexAttempt(
            success=True, indexed_generation=g, detail="ok"
        ),
    )
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
    )
    assert outcome.status == ReindexStatus.FAILED.value
    assert outcome.report_ref == ""
    assert (
        outcome.error_code
        == ReindexErrorCode.REINDEX_STATUS_STORE_UNAVAILABLE.value
    )
    assert outcome.indexed_generation is None
    # Store was attempted exactly once with the intended status.
    assert len(broken.calls) == 1
    assert broken.calls[0]["status"] == ReindexStatus.REINDEXED.value


def test_adapter_pending_with_store_failure_yields_failed_outcome() -> None:
    """Same invariant on the pending path: pending without a durable
    record_ref is silently stale, which is forbidden."""

    broken = _BrokenStore()
    reindexer = GlobalDiscoveryReindexer(
        status_store=broken,
        reindex_adapter=lambda b, g, r: ReindexAttempt(
            success=False,
            job_ref="scheduled-x",
            detail="worker offline",
            error_code=ReindexErrorCode.GLOBAL_DISCOVERY_UNAVAILABLE.value,
        ),
    )
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        reason=ReindexReason.OPERATOR_REQUESTED.value,
    )
    assert outcome.status == ReindexStatus.FAILED.value
    assert outcome.report_ref == ""
    # The store's error_code takes precedence — it's the gating failure.
    assert (
        outcome.error_code
        == ReindexErrorCode.REINDEX_STATUS_STORE_UNAVAILABLE.value
    )
    # Detail surfaces the store failure detail.
    assert outcome.detail == "forced"
    assert broken.calls[0]["status"] == ReindexStatus.REINDEX_PENDING.value


def test_invalid_generation_bumps_failed_counter(base_dir: Path) -> None:
    """val_bf8c49c2 item 3: invalid generation path is observable in
    the metric counter even though no record can be persisted."""

    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    reindexer = GlobalDiscoveryReindexer(status_store=store)
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id="not-a-uuid",
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
    )
    assert outcome.status == ReindexStatus.FAILED.value
    assert outcome.error_code == ReindexErrorCode.INVALID_GENERATION.value
    assert outcome.report_ref == ""
    # The FAILED bucket received the bump for visibility.
    assert (
        get_reindex_count(
            BOARD,
            reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
            status=ReindexStatus.FAILED.value,
        )
        >= 1
    )


def test_no_outcome_is_silently_stale(base_dir: Path) -> None:
    """Validate the BR invariant: every recorded outcome is visible in
    health + report; no path produces a successful return without a
    durable record_ref."""

    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    reindexer = GlobalDiscoveryReindexer(status_store=store)
    for reason in ReindexReason:
        outcome = reindexer.reindex_or_mark_pending(
            board_id=BOARD,
            kg_generation_id=generate_kg_generation_id(),
            reason=reason.value,
        )
        assert outcome.report_ref, f"silent stale on reason={reason.value}"
        assert outcome.status in VISIBLE_STATUSES
