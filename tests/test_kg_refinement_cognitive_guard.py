"""Tests for KG-03.7 — RefinementCognitiveCompletionGuard (api_0cd358dd)
+ deterministic terminal-write block (tr_9521d8fd + ir_dbf036d8).

Coverage:
    * AC16 + br_f6fc7cbc — active cognitive item blocks refinement done.
    * AC17 + br_929c2164 — terminal-only items allow refinement done.
    * AC18 + tr_9521d8fd — deterministic write path can only set pending.
    * invalid_target_status — guard only applies to target_status=done.
    * Fail-closed: store unavailable → allowed=False, reason=cognitive_status_unavailable.
    * Counter or_fb5bbbe7 bounded labels (board_id_hash, outcome, reason).
    * Counter or_303fbcb3 fires on any deterministic terminal-write attempt.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingMarker,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.kg.refinement_cognitive_guard import (
    RefinementCognitiveCompletionGuard,
    RefinementGuardError,
    RefinementGuardOutcome,
    RefinementGuardReason,
    TerminalWriteBlockOutcome,
    assert_deterministic_only_pending,
    get_guard_counter_labels,
    get_guard_event_count,
    get_guard_samples,
    get_terminal_block_counter_labels,
    get_terminal_block_event_count,
    get_terminal_block_samples,
    reset_guard_counter,
    reset_terminal_block_counter,
)


BOARD = "board-kg03-7"


@pytest.fixture
def isolated_base_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "kg-03-7"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_guard_counter()
    reset_terminal_block_counter()
    yield
    reset_guard_counter()
    reset_terminal_block_counter()


def _row(artifact_type: str, id_: str) -> dict:
    return {
        "artifact_type": artifact_type,
        "id": id_,
        "source_ref": f"{artifact_type}:{id_}",
    }


def _seed(base_dir: Path, sources: list[dict]) -> tuple[str, list]:
    gen = generate_kg_generation_id()
    marker = CognitivePendingMarker(base_dir=base_dir)
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=sources,
        event_ref="evt_kg03_7",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    return gen, store.list_items(BOARD, gen)


# -------- AC16 — active cognitive item blocks refinement done -----------


def test_ac16_active_pending_item_blocks_refinement_done(
    isolated_base_dir: Path,
) -> None:
    """br_f6fc7cbc + FR14: any active cognitive item (pending) blocks
    refinement done."""

    gen, items = _seed(isolated_base_dir, [_row("refinement", "r1")])
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    guard = RefinementCognitiveCompletionGuard(store=store)

    result = guard.evaluate(
        board_id=BOARD,
        refinement_id="r1",
        target_status="done",
    )
    assert result.allowed is False
    assert result.reason == RefinementGuardReason.COGNITIVE_CONSOLIDATION_PENDING.value
    assert len(result.blocking_items) == 1
    assert result.blocking_items[0].status == "pending"
    assert result.blocking_items[0].item_id == items[0].item_id


@pytest.mark.parametrize(
    "active_status",
    [
        CognitiveItemStatus.IN_PROGRESS.value,
        CognitiveItemStatus.FAILED.value,
    ],
)
def test_ac16_in_progress_or_failed_item_blocks_done(
    isolated_base_dir: Path,
    active_status: str,
) -> None:
    """All 3 active statuses (pending/in_progress/failed) block done."""

    gen, items = _seed(isolated_base_dir, [_row("refinement", "r1")])
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        new_status=active_status,
        updated_by_agent_id="agent-1",
        reason="exercise" if active_status == "failed" else None,
    )
    guard = RefinementCognitiveCompletionGuard(store=store)
    result = guard.evaluate(
        board_id=BOARD,
        refinement_id="r1",
        target_status="done",
    )
    assert result.allowed is False
    assert (
        result.reason
        == RefinementGuardReason.COGNITIVE_CONSOLIDATION_PENDING.value
    )
    assert result.blocking_items[0].status == active_status


# -------- AC17 — terminal-only items allow refinement done --------------


@pytest.mark.parametrize(
    "terminal_status",
    [
        CognitiveItemStatus.CONSOLIDATED.value,
        CognitiveItemStatus.SKIPPED.value,
    ],
)
def test_ac17_terminal_items_allow_refinement_done(
    isolated_base_dir: Path,
    terminal_status: str,
) -> None:
    """br_929c2164: terminal MCP-owned statuses unblock refinement done."""

    gen, items = _seed(isolated_base_dir, [_row("refinement", "r1")])
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        new_status=terminal_status,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-1" if terminal_status == "consolidated" else None,
        reason="not applicable" if terminal_status == "skipped" else None,
    )
    guard = RefinementCognitiveCompletionGuard(store=store)
    result = guard.evaluate(
        board_id=BOARD,
        refinement_id="r1",
        target_status="done",
    )
    assert result.allowed is True
    assert (
        result.reason
        == RefinementGuardReason.NO_ACTIVE_COGNITIVE_ITEMS.value
    )
    assert result.blocking_items == ()


def test_no_items_at_all_allows_done(
    isolated_base_dir: Path,
) -> None:
    """When the refinement has no cognitive items, done is allowed."""

    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    guard = RefinementCognitiveCompletionGuard(store=store)
    result = guard.evaluate(
        board_id=BOARD,
        refinement_id="never_existed",
        target_status="done",
    )
    assert result.allowed is True
    assert (
        result.reason
        == RefinementGuardReason.NO_ACTIVE_COGNITIVE_ITEMS.value
    )


# -------- invalid_target_status ----------------------------------------


@pytest.mark.parametrize(
    "bad_target",
    ["approved", "review", "draft", "started", "in_progress", ""],
)
def test_invalid_target_status_raises_typed_error(
    isolated_base_dir: Path,
    bad_target: str,
) -> None:
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    guard = RefinementCognitiveCompletionGuard(store=store)
    with pytest.raises(RefinementGuardError) as excinfo:
        guard.evaluate(
            board_id=BOARD,
            refinement_id="r1",
            target_status=bad_target,
        )
    assert excinfo.value.code == "invalid_target_status"


# -------- Fail-closed on store unavailable -----------------------------


def test_store_unavailable_fails_closed(
    isolated_base_dir: Path,
) -> None:
    """If reading the store raises, the guard MUST block the transition
    (fail-closed, tr_e2b4e9de)."""

    class _RaisingStore:
        """Stand-in store that raises on every read path the guard uses."""

        def latest_generation(self, _board_id: str) -> str | None:
            raise OSError("disk gone")

        def list_items(self, *_args, **_kwargs):
            raise OSError("disk gone")

    guard = RefinementCognitiveCompletionGuard(store=_RaisingStore())  # type: ignore[arg-type]
    result = guard.evaluate(
        board_id=BOARD,
        refinement_id="r1",
        target_status="done",
    )
    assert result.allowed is False
    assert (
        result.reason
        == RefinementGuardReason.COGNITIVE_STATUS_UNAVAILABLE.value
    )
    assert (
        get_guard_event_count(
            outcome=RefinementGuardOutcome.UNAVAILABLE.value,
            reason=RefinementGuardReason.COGNITIVE_STATUS_UNAVAILABLE.value,
        )
        == 1
    )


# -------- Custom source_ref -------------------------------------------


def test_custom_source_ref_is_used_when_provided(
    isolated_base_dir: Path,
) -> None:
    """Caller may override the default ``refinement:<id>`` source_ref —
    useful when the refinement ledger keys on a non-canonical ref."""

    gen, items = _seed(
        isolated_base_dir, [_row("refinement", "weird-key")]
    )
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    guard = RefinementCognitiveCompletionGuard(store=store)
    result = guard.evaluate(
        board_id=BOARD,
        refinement_id="r1",  # mismatch on purpose
        source_ref="refinement:weird-key",
        target_status="done",
    )
    assert result.allowed is False
    assert result.blocking_items[0].item_id == items[0].item_id


# -------- or_fb5bbbe7 — bounded labels ---------------------------------


def test_guard_counter_label_set_is_bounded(
    isolated_base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    guard = RefinementCognitiveCompletionGuard(store=store)
    guard.evaluate(
        board_id=BOARD,
        refinement_id="r1",
        target_status="done",
    )
    assert get_guard_counter_labels() == (
        "board_id_hash",
        "outcome",
        "reason",
    )
    samples = get_guard_samples()
    assert samples
    forbidden = {
        "refinement_id",
        "source_ref",
        "item_id",
        "board_id",
        "agent",
    }
    for sample in samples:
        assert set(sample.keys()) == {"board_id_hash", "outcome", "reason"}
        assert not forbidden.intersection(sample.keys())


def test_guard_counter_emits_exactly_one_sample_per_call(
    isolated_base_dir: Path,
) -> None:
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    guard = RefinementCognitiveCompletionGuard(store=store)
    guard.evaluate(board_id=BOARD, refinement_id="r1", target_status="done")
    guard.evaluate(board_id=BOARD, refinement_id="r2", target_status="done")
    guard.evaluate(board_id=BOARD, refinement_id="r3", target_status="done")
    assert (
        get_guard_event_count(
            outcome=RefinementGuardOutcome.ALLOWED.value,
            reason=RefinementGuardReason.NO_ACTIVE_COGNITIVE_ITEMS.value,
        )
        == 3
    )


# -------- AC18 + tr_9521d8fd — deterministic only pending --------------


def test_ac18_marker_only_writes_pending_status_never_terminal(
    isolated_base_dir: Path,
) -> None:
    """Repeated marker invocations never flip an item to a terminal
    status. The defensive guard short-circuits the write path before
    a non-pending status can land on disk."""

    marker = CognitivePendingMarker(base_dir=isolated_base_dir)
    gen = generate_kg_generation_id()
    for _ in range(3):
        marker.mark_for_generation(
            board_id=BOARD,
            kg_generation_id=gen,
            source_set=[_row("refinement", "r1")],
            event_ref="evt_loop",
        )
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    items = store.list_items(BOARD, gen)
    assert all(item.status == "pending" for item in items)
    # No terminal-write block samples on the happy path.
    assert get_terminal_block_event_count() == 0


def test_assert_deterministic_only_pending_blocks_terminal_status(
    isolated_base_dir: Path,
) -> None:
    """If a future bug ever produces a non-pending item in the
    deterministic path, the assert raises + the alert metric fires."""

    bad_item = CognitiveConsolidationItem(
        item_id="cogn_bad",
        board_id=BOARD,
        kg_generation_id="00000000-0000-4000-8000-000000000000",
        source_ref="refinement:r1",
        artifact_type="refinement",
        status=CognitiveItemStatus.CONSOLIDATED.value,
        recorded_at="2026-05-26T00:00:00+00:00",
        event_ref="evt_test",
    )
    with pytest.raises(RefinementGuardError) as excinfo:
        assert_deterministic_only_pending(board_id=BOARD, items=[bad_item])
    assert excinfo.value.code == "terminal_write_blocked"
    # Counter saw the blocked attempt.
    assert (
        get_terminal_block_event_count(
            attempted_status=CognitiveItemStatus.CONSOLIDATED.value,
            outcome=TerminalWriteBlockOutcome.BLOCKED.value,
        )
        == 1
    )


@pytest.mark.parametrize(
    "terminal_status",
    [
        CognitiveItemStatus.CONSOLIDATED.value,
        CognitiveItemStatus.SKIPPED.value,
        CognitiveItemStatus.FAILED.value,
        CognitiveItemStatus.IN_PROGRESS.value,
    ],
)
def test_assert_blocks_all_non_pending_statuses(
    terminal_status: str,
) -> None:
    """Any status other than pending is rejected by the deterministic
    write guard (tr_9521d8fd)."""

    bad_item = CognitiveConsolidationItem(
        item_id="cogn_bad",
        board_id=BOARD,
        kg_generation_id="00000000-0000-4000-8000-000000000000",
        source_ref="refinement:r1",
        artifact_type="refinement",
        status=terminal_status,
        recorded_at="2026-05-26T00:00:00+00:00",
        event_ref="evt_test",
    )
    with pytest.raises(RefinementGuardError):
        assert_deterministic_only_pending(board_id=BOARD, items=[bad_item])


def test_terminal_block_counter_labels_bounded() -> None:
    bad_item = CognitiveConsolidationItem(
        item_id="cogn_bad",
        board_id=BOARD,
        kg_generation_id="00000000-0000-4000-8000-000000000000",
        source_ref="refinement:r1",
        artifact_type="refinement",
        status=CognitiveItemStatus.CONSOLIDATED.value,
        recorded_at="2026-05-26T00:00:00+00:00",
        event_ref="evt_test",
    )
    try:
        assert_deterministic_only_pending(board_id=BOARD, items=[bad_item])
    except RefinementGuardError:
        pass
    assert get_terminal_block_counter_labels() == (
        "board_id_hash",
        "attempted_status",
        "outcome",
    )
    samples = get_terminal_block_samples()
    assert samples
    forbidden = {
        "source_ref",
        "item_id",
        "board_id",
        "refinement_id",
    }
    for sample in samples:
        assert set(sample.keys()) == {
            "board_id_hash",
            "attempted_status",
            "outcome",
        }
        assert not forbidden.intersection(sample.keys())


# -------- Replay safety preserves terminal items WITHOUT firing alert ---


def test_replay_preserves_terminal_items_without_firing_terminal_block_alert(
    isolated_base_dir: Path,
) -> None:
    """Deterministic replay of the marker on a generation where MCP has
    already moved items to terminal MUST preserve the terminal state
    (Codex KG-03.1 regression) AND MUST NOT fire the terminal-write-
    blocked alert (the preserved item is NOT a new write)."""

    marker = CognitivePendingMarker(base_dir=isolated_base_dir)
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    gen = generate_kg_generation_id()
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("refinement", "r1")],
        event_ref="evt_initial",
    )
    items = store.list_items(BOARD, gen)
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-1",
    )
    reset_terminal_block_counter()  # clear any prior bookkeeping

    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[_row("refinement", "r1")],
        event_ref="evt_replay",
    )
    # Terminal state survived.
    replayed = store.list_items(BOARD, gen)
    assert replayed[0].status == "consolidated"
    # No terminal-write-blocked alert was fired (replay preserves
    # MCP-owned state by construction).
    assert get_terminal_block_event_count() == 0
