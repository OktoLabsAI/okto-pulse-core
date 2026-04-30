"""Tests for the per-board commit serialisation + retry coordinator.

Pairs with spec 194583e5 (KG commit serialization — lock + retry dentro de
0.1.4). Covers test scenarios ts_f7203b87, ts_e5f93bf8, ts_061a76d6,
ts_5ffd0471, ts_efe0cc8f (plus integration ts_26a5d1b4 via asyncio.gather).
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import pytest

from okto_pulse.core.kg.commit_coordinator import (
    JITTER_MAX_MS,
    RETRY_BACKOFFS_MS,
    _is_kuzu_lock_error,
    acquire_commit_lock,
    reset_commit_locks_for_tests,
    run_with_commit_lock_and_retry,
)


@pytest.fixture(autouse=True)
def _fresh_lock_registry():
    """Isolate tests by resetting the module-level lock registry."""
    reset_commit_locks_for_tests()
    yield
    reset_commit_locks_for_tests()


# ----------------------------------------------------------------------
# AC1, AC2 — lock singleton per board (TS ts_f7203b87)
# ----------------------------------------------------------------------

def test_acquire_commit_lock_returns_same_instance_for_same_board():
    """AC1: same board_id → same Lock object (identity)."""
    a1 = acquire_commit_lock("board-A")
    a2 = acquire_commit_lock("board-A")
    assert a1 is a2


def test_acquire_commit_lock_returns_distinct_instances_for_distinct_boards():
    """AC2: distinct board_id → distinct Lock objects."""
    a = acquire_commit_lock("board-A")
    b = acquire_commit_lock("board-B")
    assert a is not b


# ----------------------------------------------------------------------
# AC5, AC6 — retry absorbs transient failures / exhausts after 4 attempts
# (TS ts_e5f93bf8)
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_absorbs_two_transient_lock_errors(caplog):
    """AC5: 2 lock errors → 3rd attempt succeeds; 2 retry log lines emitted."""
    attempts = {"count": 0}

    async def flaky_commit():
        attempts["count"] += 1
        if attempts["count"] <= 2:
            raise RuntimeError(
                "IO exception: Could not set lock on file : C:/tmp/x.kuzu"
            )
        return {"status": "committed"}

    caplog.set_level(logging.WARNING, logger="okto_pulse.kg.commit_coordinator")
    result = await run_with_commit_lock_and_retry("board-A", flaky_commit)

    assert result == {"status": "committed"}
    assert attempts["count"] == 3
    retry_logs = [r for r in caplog.records if r.getMessage().startswith("kg.commit.lock_retry")]
    assert len(retry_logs) == 2


@pytest.mark.asyncio
async def test_retry_exhausts_after_four_attempts(caplog):
    """AC6: 4 consecutive lock errors → propagates + exhausted log emitted."""
    attempts = {"count": 0}

    async def always_locked():
        attempts["count"] += 1
        raise RuntimeError("IO exception: Could not set lock on file : X")

    caplog.set_level(logging.WARNING, logger="okto_pulse.kg.commit_coordinator")

    with pytest.raises(RuntimeError, match="Could not set lock on file"):
        await run_with_commit_lock_and_retry("board-A", always_locked)

    # 4 total attempts — len(RETRY_BACKOFFS_MS) + 1.
    assert attempts["count"] == len(RETRY_BACKOFFS_MS) + 1
    retry_logs = [r for r in caplog.records if r.getMessage().startswith("kg.commit.lock_retry")]
    exhausted_logs = [r for r in caplog.records if r.getMessage().startswith("kg.commit.lock_exhausted")]
    assert len(retry_logs) == len(RETRY_BACKOFFS_MS)
    assert len(exhausted_logs) == 1
    assert exhausted_logs[0].levelno == logging.ERROR


# ----------------------------------------------------------------------
# AC7 — non-lock exception propagates immediately (TS ts_061a76d6)
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_non_lock_exception_is_not_retried():
    """AC7: unrelated exceptions bypass the retry loop completely."""
    attempts = {"count": 0}

    async def bug():
        attempts["count"] += 1
        raise ValueError("some unrelated bug")

    with pytest.raises(ValueError, match="some unrelated bug"):
        await run_with_commit_lock_and_retry("board-A", bug)

    assert attempts["count"] == 1


def test_is_kuzu_lock_error_rejects_other_runtime_errors():
    """Only lock-substring RuntimeError triggers retry — other RuntimeErrors propagate."""
    assert _is_kuzu_lock_error(
        RuntimeError("IO exception: Could not set lock on file : X")
    )
    assert not _is_kuzu_lock_error(RuntimeError("something else failed"))
    assert not _is_kuzu_lock_error(ValueError("not a runtime error"))


# ----------------------------------------------------------------------
# AC8 — backoff timing observable (TS ts_5ffd0471)
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backoff_timings_follow_schedule():
    """AC8: deltas between attempts fall within base ± jitter ceiling."""
    timestamps: list[float] = []

    async def always_locked():
        timestamps.append(time.perf_counter())
        raise RuntimeError("IO exception: Could not set lock on file : X")

    with pytest.raises(RuntimeError):
        await run_with_commit_lock_and_retry("board-A", always_locked)

    # We recorded timestamps at the *start* of each attempt. Deltas between
    # consecutive attempts should equal the backoff base + some jitter in
    # [0, JITTER_MAX_MS].
    tolerance_ms = 20.0  # allow scheduler jitter on top of configured jitter
    for i, base_ms in enumerate(RETRY_BACKOFFS_MS):
        delta_ms = (timestamps[i + 1] - timestamps[i]) * 1000.0
        lower = base_ms
        upper = base_ms + JITTER_MAX_MS + tolerance_ms
        assert lower <= delta_ms <= upper, (
            f"attempt {i + 1}→{i + 2}: expected delta in "
            f"[{lower:.1f}, {upper:.1f}] ms, got {delta_ms:.1f}"
        )


# ----------------------------------------------------------------------
# AC3, AC4 — parallel commits serialise within a board, parallelise across
# boards (TS ts_26a5d1b4, unit-grade via ordered counters)
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parallel_commits_on_same_board_are_serialised():
    """AC3: 10 parallel commits on one board complete without racing.

    Instead of spinning a real Kùzu we instrument the coroutine to assert
    exclusive access: every call increments and decrements an in_flight
    counter; the lock guarantees it never exceeds 1.
    """
    in_flight = {"n": 0, "max_observed": 0}
    done_count = {"n": 0}

    async def fake_commit():
        in_flight["n"] += 1
        in_flight["max_observed"] = max(in_flight["max_observed"], in_flight["n"])
        # Yield so other tasks can try to enter.
        await asyncio.sleep(0.005)
        in_flight["n"] -= 1
        done_count["n"] += 1
        return "ok"

    await asyncio.gather(*(
        run_with_commit_lock_and_retry("board-A", fake_commit)
        for _ in range(10)
    ))

    assert done_count["n"] == 10
    assert in_flight["max_observed"] == 1  # strictly serialised


@pytest.mark.asyncio
async def test_parallel_commits_on_distinct_boards_run_concurrently():
    """AC4: locks are per-board — distinct boards don't block each other."""
    in_flight_per_board: dict[str, int] = {"A": 0, "B": 0}
    max_total = {"n": 0}

    async def fake_commit_for(board: str):
        async def _inner():
            in_flight_per_board[board] += 1
            total = sum(in_flight_per_board.values())
            max_total["n"] = max(max_total["n"], total)
            await asyncio.sleep(0.01)
            in_flight_per_board[board] -= 1
            return board
        return _inner

    tasks = []
    for _ in range(5):
        factory_a = await fake_commit_for("A")
        factory_b = await fake_commit_for("B")
        tasks.append(run_with_commit_lock_and_retry("board-A", factory_a))
        tasks.append(run_with_commit_lock_and_retry("board-B", factory_b))

    await asyncio.gather(*tasks)

    # With per-board locks, up to 2 concurrent in-flight calls are expected
    # (one per distinct board). If a shared global lock were used, the
    # max would collapse to 1.
    assert max_total["n"] == 2


# ----------------------------------------------------------------------
# AC9, AC10 — instructions updated + zero version bump (TS ts_efe0cc8f)
# ----------------------------------------------------------------------

def _read_instructions() -> str:
    path = (
        Path(__file__).resolve().parent.parent
        / "src/okto_pulse/core/mcp/agent_instructions.md"
    )
    return path.read_text(encoding="utf-8")


def test_instructions_drop_manual_serialisation_line():
    """AC9: old 'Commits are serialised across sessions' bullet removed."""
    content = _read_instructions()
    assert "Commits are serialised across sessions" not in content
    assert "Server serialises commits per board automatically" in content \
        or "Server serializes commits per board automatically" in content


def test_version_pinned_in_pyproject():
    """AC10 (refactored): core pyproject declares a stable, pinned version
    string. Original AC pinned to 0.1.4 (no bump for spec 194583e5); after
    the v0.1.6 release, this asserts only that pyproject still carries a
    well-formed semver, decoupling the test from future bumps."""
    import re

    core_pyproject = (
        Path(__file__).resolve().parent.parent / "pyproject.toml"
    ).read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"(\d+\.\d+\.\d+)"', core_pyproject, re.MULTILINE)
    assert match, "pyproject.toml must declare a top-level semver version"
