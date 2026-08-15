"""Opaque offline KG recovery execution capability regressions."""

from __future__ import annotations

import threading

from okto_pulse.core.kg.recovery_execution import (
    issue_recovery_execution_capability,
    validate_recovery_execution_capability,
)


def test_scope_revocation_wins_race_with_slow_successful_lifetime_probe():
    probe_entered = threading.Event()
    release_probe = threading.Event()
    results: list[bool] = []

    def _slow_probe() -> bool:
        probe_entered.set()
        assert release_probe.wait(timeout=5)
        return True

    with issue_recovery_execution_capability(
        board_id="board-1",
        lifetime_probe=_slow_probe,
    ) as capability:
        worker = threading.Thread(
            target=lambda: results.append(
                validate_recovery_execution_capability(
                    capability,
                    board_id="board-1",
                    run_id="run-1",
                )
            )
        )
        worker.start()
        assert probe_entered.wait(timeout=5)

    # Scope exit revoked the capability while the probe was blocked. A stale
    # probe result must not resurrect authority in the background thread.
    release_probe.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert results == [False]


def test_capability_is_bound_to_first_run_id_for_sequential_reuse():
    with issue_recovery_execution_capability(
        board_id="board-1",
        lifetime_probe=lambda: True,
    ) as capability:
        assert validate_recovery_execution_capability(
            capability,
            board_id="board-1",
            run_id="run-1",
        )
        assert validate_recovery_execution_capability(
            capability,
            board_id="board-1",
            run_id="run-1",
        )
        assert not validate_recovery_execution_capability(
            capability,
            board_id="board-1",
            run_id="run-2",
        )


def test_concurrent_run_ids_cannot_share_one_capability():
    start = threading.Barrier(3)
    results: dict[str, bool] = {}

    with issue_recovery_execution_capability(
        board_id="board-1",
        lifetime_probe=lambda: True,
    ) as capability:

        def _validate(run_id: str) -> None:
            start.wait(timeout=5)
            results[run_id] = validate_recovery_execution_capability(
                capability,
                board_id="board-1",
                run_id=run_id,
            )

        workers = [
            threading.Thread(target=_validate, args=(run_id,))
            for run_id in ("run-1", "run-2")
        ]
        for worker in workers:
            worker.start()
        start.wait(timeout=5)
        for worker in workers:
            worker.join(timeout=5)
            assert not worker.is_alive()

    assert sorted(results.values()) == [False, True]
