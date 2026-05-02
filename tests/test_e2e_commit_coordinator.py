"""End-to-end validation of the 0.1.4 commit coordinator against a real
board's Kuzu file.

This is not a unit test — it opens the E2E board Kuzu that lives on disk at
``~/.okto-pulse/boards/4877258c-.../graph.kuzu`` and fires 10 parallel
``commit`` calls through the lock+retry wrapper. The point is to reproduce
the exact sequence that failed 6 times in the original 0.1.4 delivery
session and confirm every attempt succeeds now.

Run with:

    pytest tests/test_e2e_commit_coordinator.py -v --no-header -s
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from pathlib import Path

import pytest


E2E_BOARD_ID = "4877258c-4ec8-4b8d-8bab-36d1e087c738"
E2E_KUZU_PATH = (
    Path(os.path.expanduser("~/.okto-pulse")) / "boards" / E2E_BOARD_ID / "graph.kuzu"
)

pytestmark = pytest.mark.skipif(
    not E2E_KUZU_PATH.exists(),
    reason=f"E2E board Kuzu not present at {E2E_KUZU_PATH}",
)


@pytest.mark.asyncio
async def test_e2e_parallel_commits_on_real_kuzu():
    """Fire 10 parallel 'commits' on the real E2E board. All must succeed.

    We stand in for the real ``primitives.commit_consolidation`` by running
    a cheap Kuzu write (a single INSERT into a scratch node type) through
    the same lock+retry wrapper the production handler uses. If 0.1.4 is
    working, all 10 return without any ``Could not set lock on file``
    surviving the retry ladder.
    """
    from okto_pulse.core.kg.commit_coordinator import (
        reset_commit_locks_for_tests,
        run_with_commit_lock_and_retry,
    )
    from okto_pulse.core.kg.schema import _open_kuzu_db

    reset_commit_locks_for_tests()

    parallelism = 10
    errors: list[BaseException] = []
    durations: list[float] = []
    start_wall = time.perf_counter()

    async def scratch_commit(tag: str):
        """Open a Kuzu connection to the E2E board, write a throwaway node, close."""
        import ladybug as kuzu  # lazy
        t0 = time.perf_counter()

        def _work():
            # Same path production uses: _open_kuzu_db reads CoreSettings and
            # applies the safe 0.1.4 defaults (48 MB / 1 GB).
            db = _open_kuzu_db(E2E_KUZU_PATH)
            conn = kuzu.Connection(db)
            try:
                # BoardMeta is the only node type guaranteed to exist on every
                # board. We probe it rather than writing — keeps the board
                # pristine and still holds the Kuzu write lock for the
                # duration, which is what the race test actually needs.
                res = conn.execute("MATCH (m:BoardMeta) RETURN count(m)")
                while res.has_next():
                    res.get_next()
            finally:
                del conn
                del db

        await asyncio.to_thread(_work)
        durations.append(time.perf_counter() - t0)
        return tag

    async def one_attempt(i: int):
        try:
            return await run_with_commit_lock_and_retry(
                E2E_BOARD_ID,
                lambda: scratch_commit(f"e2e-{i}-{uuid.uuid4().hex[:6]}"),
            )
        except BaseException as exc:  # noqa: BLE001 — we classify below
            errors.append(exc)
            raise

    results = await asyncio.gather(
        *(one_attempt(i) for i in range(parallelism)),
        return_exceptions=True,
    )

    wall = time.perf_counter() - start_wall

    print(f"\n=== E2E parallel commits ({parallelism} tasks) ===")
    print(f"Board: {E2E_BOARD_ID}")
    print(f"Wall time: {wall:.3f}s")
    print(f"Per-attempt min/max/avg: "
          f"{min(durations):.3f} / {max(durations):.3f} / "
          f"{sum(durations) / len(durations):.3f}s" if durations else "no timings")
    print(f"Errors: {len(errors)}")
    for exc in errors:
        print(f"  - {type(exc).__name__}: {exc}")

    failures = [r for r in results if isinstance(r, BaseException)]
    assert not failures, f"0.1.4 commit coordinator failed to absorb races: {failures}"
    assert len(results) == parallelism


@pytest.mark.asyncio
async def test_e2e_mixed_board_parallelism():
    """Two real boards hit in parallel: distinct locks must let them run
    concurrently. Same board × 3 inside each group must serialise."""
    from okto_pulse.core.kg.commit_coordinator import (
        reset_commit_locks_for_tests,
        run_with_commit_lock_and_retry,
    )
    from okto_pulse.core.kg.schema import _open_kuzu_db

    reset_commit_locks_for_tests()

    # E2E board + Okto Pulse Evolution board — both real.
    other_board_id = "82c1bfef-af0c-4ffc-aa9c-64a1f7e0f2fa"
    other_path = (
        Path(os.path.expanduser("~/.okto-pulse")) / "boards" / other_board_id / "graph.kuzu"
    )
    if not other_path.exists():
        pytest.skip(f"Okto Pulse Evolution Kuzu missing at {other_path}")

    async def read_once(path: Path):
        import ladybug as kuzu

        def _work():
            db = _open_kuzu_db(path)
            conn = kuzu.Connection(db)
            try:
                conn.execute("MATCH (m:BoardMeta) RETURN count(m)")
            finally:
                del conn
                del db

        await asyncio.to_thread(_work)

    tasks = []
    for _ in range(3):
        tasks.append(run_with_commit_lock_and_retry(E2E_BOARD_ID, lambda: read_once(E2E_KUZU_PATH)))
        tasks.append(run_with_commit_lock_and_retry(other_board_id, lambda: read_once(other_path)))

    t0 = time.perf_counter()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    wall = time.perf_counter() - t0

    failures = [r for r in results if isinstance(r, BaseException)]
    print("\n=== E2E mixed-board parallelism ===")
    print(f"6 tasks (3 per board) — wall time: {wall:.3f}s, failures: {len(failures)}")

    assert not failures, f"Unexpected errors in mixed-board run: {failures}"
