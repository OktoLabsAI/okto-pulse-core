from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier

import pytest

from okto_pulse.core.kg.global_discovery_recovery_control import (
    RecoveryAttempt,
    RecoveryCheckpoint,
    RecoveryCheckpointConflict,
    RecoveryProgressCounts,
    RecoveryRunPhase,
)
from okto_pulse.core.kg.providers.testing.memory_recovery_run_store import (
    MemoryRecoveryRunStore,
)


def test_checkpoint_cas_allows_one_writer_and_fences_the_stale_writer() -> None:
    """ts_331d96ee: one epoch/progress CAS wins and one loses unchanged."""

    heartbeat = datetime(2026, 7, 16, 14, 0, tzinfo=timezone.utc)
    initial = RecoveryAttempt(
        run_id="run-cas",
        epoch=4,
        progress_seq=9,
        phase=RecoveryRunPhase.CUTOVER,
        heartbeat_at=heartbeat,
        active_elapsed_ms=1_000,
        counts=RecoveryProgressCounts(
            sources_total=3,
            sources_processed=1,
            nodes_written=8,
            edges_written=5,
            outbox_events_drained=2,
            errors=0,
        ),
    )
    store = MemoryRecoveryRunStore()
    store.create_attempt(initial)

    ready = Barrier(3)

    def checkpoint(phase: str) -> RecoveryAttempt | RecoveryCheckpointConflict:
        ready.wait(timeout=2)
        try:
            return store.compare_and_set_checkpoint(
                RecoveryCheckpoint(
                    run_id="run-cas",
                    epoch=4,
                    expected_progress_seq=9,
                    phase=phase,
                    heartbeat_at=heartbeat + timedelta(seconds=1),
                    active_elapsed_ms=2_000,
                    counts=RecoveryProgressCounts(
                        sources_total=3,
                        sources_processed=2,
                        nodes_written=13,
                        edges_written=9,
                        outbox_events_drained=2,
                        errors=0,
                    ),
                )
            )
        except RecoveryCheckpointConflict as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(checkpoint, RecoveryRunPhase.CUTOVER),
            pool.submit(checkpoint, RecoveryRunPhase.CUTOVER),
        ]
        ready.wait(timeout=2)
        results = [future.result(timeout=2) for future in futures]

    winners = [result for result in results if isinstance(result, RecoveryAttempt)]
    conflicts = [
        result for result in results if isinstance(result, RecoveryCheckpointConflict)
    ]
    assert len(winners) == 1
    assert len(conflicts) == 1

    winner = winners[0]
    conflict = conflicts[0]
    assert winner.progress_seq == 10
    assert conflict.code == "recovery_progress_conflict"
    assert conflict.run_id == "run-cas"
    assert conflict.expected_epoch == conflict.actual_epoch == 4
    assert conflict.expected_progress_seq == 9
    assert conflict.actual_progress_seq == 10

    persisted = store.get_attempt(run_id="run-cas", epoch=4)
    assert persisted == winner
    assert persisted.progress_seq == 10
    assert store.successful_checkpoint_writes == 1


def test_checkpoint_run_id_conflict_is_typed_and_leaves_attempt_unchanged() -> None:
    """N1: a checkpoint can never cross one immutable logical run binding."""

    heartbeat = datetime(2026, 7, 16, 15, 50, tzinfo=timezone.utc)
    attempt = RecoveryAttempt(
        run_id="run-authoritative",
        epoch=2,
        progress_seq=7,
        phase=RecoveryRunPhase.CUTOVER,
        heartbeat_at=heartbeat,
        active_elapsed_ms=3_000,
        counts=RecoveryProgressCounts(sources_total=1),
    )
    mismatched = RecoveryCheckpoint(
        run_id="run-other",
        epoch=2,
        expected_progress_seq=7,
        phase="cutover",
        heartbeat_at=heartbeat + timedelta(seconds=1),
        active_elapsed_ms=4_000,
        counts=RecoveryProgressCounts(sources_total=1, sources_processed=1),
    )

    with pytest.raises(RecoveryCheckpointConflict) as conflict:
        attempt.advance_checkpoint(mismatched)

    assert conflict.value.code == "recovery_run_conflict"
    assert conflict.value.run_id == "run-authoritative"
    assert conflict.value.expected_epoch == conflict.value.actual_epoch == 2
    assert (
        conflict.value.expected_progress_seq == conflict.value.actual_progress_seq == 7
    )
    assert attempt.progress_seq == 7
    assert attempt.phase is RecoveryRunPhase.CUTOVER
