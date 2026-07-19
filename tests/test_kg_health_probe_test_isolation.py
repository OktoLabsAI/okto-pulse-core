"""Regression proof for the suite-level KG health-probe lifecycle guard."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap

import pytest


def test_probe_drain_guard_is_bounded_and_fails_closed(monkeypatch) -> None:
    """A non-zero lifecycle drain is observable instead of silently reset."""

    import conftest
    from okto_pulse.core.services import application_kg

    observed_timeouts: list[float] = []

    def leaked_probe_count(*, timeout_s: float) -> int:
        observed_timeouts.append(timeout_s)
        return 1

    monkeypatch.setattr(
        application_kg,
        "drain_kg_health_probes",
        leaked_probe_count,
    )

    with pytest.raises(
        pytest.fail.Exception,
        match=(
            "kg_health_probe_test_isolation_timeout:"
            "phase=proof:nodeid=proof-node:remaining=1:timeout_s=30.0"
        ),
    ):
        conftest._assert_kg_health_probes_drained(
            nodeid="proof-node",
            phase="proof",
        )

    assert observed_timeouts == [30.0]


def test_probe_then_sync_and_async_graph_tests_are_isolated() -> None:
    """Real pytest lifecycle drains contaminating probes between graph tests.

    A nested run is deliberate: it lets this test observe the *post-test*
    finalizer from the real project conftest. Both a synchronous and an
    asynchronous graph consumer follow a probe that returns control while its
    worker still owns an open native graph connection.
    """

    tests_dir = Path(__file__).parent
    project_dir = tests_dir.parent
    child_source = textwrap.dedent(
        r'''
        from __future__ import annotations

        import asyncio
        from contextvars import copy_context
        import os
        from pathlib import Path
        import threading
        import time

        import pytest

        from kg_schema_testing import (
            bootstrap_board_graph,
            open_materialized_board_connection,
        )
        from okto_pulse.core.runtime_context import current_runtime_values
        from okto_pulse.core.services import kg_health_service as health


        _RECORD = Path(os.environ["OKTO_PROBE_ISOLATION_RECORD"])
        _POOL = health._DaemonHealthProbePool(
            max_workers=1,
            max_queue_size=4,
            idle_timeout_s=0.02,
        )
        _RUNTIME = current_runtime_values(create=True)
        assert _RUNTIME is not None
        _RUNTIME.register(health._HEALTH_PROBE_POOL.key, _POOL)


        def _graph_count(board_id: str) -> int:
            bootstrap_board_graph(board_id)
            with open_materialized_board_connection(board_id) as (_db, conn):
                rows = conn.execute("MATCH (n) RETURN count(n) AS total")
            return int(rows.rows[0][0])


        def _schedule_connection_holding_probe(label: str) -> None:
            board_id = f"health-probe-isolation-{label}"
            bootstrap_board_graph(board_id)
            started = threading.Event()

            def probe() -> str:
                with open_materialized_board_connection(board_id) as (_db, conn):
                    conn.execute("MATCH (n) RETURN count(n) AS total")
                    started.set()
                    time.sleep(0.15)
                with _RECORD.open("a", encoding="utf-8") as handle:
                    handle.write(f"{label}:{threading.current_thread().name}\n")
                return label

            future = _POOL.submit(context=copy_context(), build=probe)
            assert future is not None
            assert started.wait(2), "health probe never acquired its graph handle"
            assert not future.done(), "probe must still be live when the test returns"


        def _assert_pool_has_no_orphan_worker() -> None:
            assert _POOL.wait_until_idle(timeout_s=0) == 0
            deadline = time.monotonic() + 1
            while _POOL.active_worker_count() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _POOL.active_worker_count() == 0


        def test_01_health_probe_precedes_sync_graph_test() -> None:
            _schedule_connection_holding_probe("sync")


        def test_02_sync_graph_starts_after_probe_drain() -> None:
            assert _RECORD.read_text(encoding="utf-8").splitlines()[0].startswith(
                "sync:okto-pulse-health-probe-"
            )
            _assert_pool_has_no_orphan_worker()
            assert _graph_count("sync-graph-after-health") >= 0


        @pytest.mark.asyncio
        async def test_03_health_probe_precedes_async_graph_test() -> None:
            _schedule_connection_holding_probe("async")


        @pytest.mark.asyncio
        async def test_04_async_graph_starts_after_probe_drain() -> None:
            assert _RECORD.read_text(encoding="utf-8").splitlines()[-1].startswith(
                "async:okto-pulse-health-probe-"
            )
            _assert_pool_has_no_orphan_worker()
            assert await asyncio.wait_for(
                asyncio.to_thread(_graph_count, "async-graph-after-health"),
                timeout=5,
            ) >= 0
            current = asyncio.current_task()
            probe_tasks = [
                task
                for task in asyncio.all_tasks()
                if task is not current
                and not task.done()
                and any(
                    token in task.get_name().lower()
                    for token in ("health", "probe")
                )
            ]
            assert probe_tasks == []
        '''
    )

    with tempfile.TemporaryDirectory(
        prefix="_probe_isolation_",
        dir=tests_dir,
    ) as temp_dir:
        temp_path = Path(temp_dir)
        child_test = temp_path / "test_probe_order.py"
        child_test.write_text(child_source, encoding="utf-8")
        record = temp_path / "probe-order.log"
        env = os.environ.copy()
        env["OKTO_PROBE_ISOLATION_RECORD"] = str(record)
        env.pop("PYTEST_CURRENT_TEST", None)
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                str(child_test),
                "--tb=short",
            ],
            cwd=project_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

    assert completed.returncode == 0, (
        "nested probe-isolation regression failed\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    assert "4 passed" in completed.stdout
