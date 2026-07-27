from __future__ import annotations

import asyncio
import threading
import time

import pytest

from okto_pulse.core.services import kg_health_service as health


@pytest.mark.asyncio
async def test_generation_change_forces_fresh_probe_past_old_inflight() -> None:
    board_id = "board-materialization-generation"
    probe_name = "materialization-generation-test"
    old_started = threading.Event()
    release_old = threading.Event()
    calls: list[str] = []

    def old_build() -> str:
        calls.append("old")
        old_started.set()
        assert release_old.wait(timeout=2.0)
        return "stale-generation-value"

    def new_build() -> str:
        calls.append("new")
        return "fresh-generation-value"

    health._reset_health_probe_cache_for_tests(board_id)
    try:
        first = await health.run_bounded_health_probe(
            name=probe_name,
            board_id=board_id,
            generation_id="generation-1",
            build=old_build,
            fallback="fallback-1",
            deadline_at=time.monotonic() + 0.05,
        )
        assert old_started.wait(timeout=1.0)
        assert first.status == "unavailable"
        assert first.value == "fallback-1"

        second = await health.run_bounded_health_probe(
            name=probe_name,
            board_id=board_id,
            generation_id="generation-2",
            build=new_build,
            fallback="fallback-2",
            deadline_at=time.monotonic() + 1.0,
        )
        assert second.status == "available"
        assert second.value == "fresh-generation-value"
        assert calls == ["old", "new"]

        release_old.set()
        await asyncio.sleep(0)
        assert health.drain_health_probe_runtime(timeout_s=1.0) == 0

        # The replaced old-generation worker cannot overwrite the new cache.
        warm = await health.run_bounded_health_probe(
            name=probe_name,
            board_id=board_id,
            generation_id="generation-2",
            build=lambda: "unexpected-refresh",
            fallback="fallback-2",
            deadline_at=time.monotonic() + 1.0,
        )
        assert warm.value == "fresh-generation-value"
        assert calls == ["old", "new"]
    finally:
        release_old.set()
        health.drain_health_probe_runtime(timeout_s=1.0)
        health._reset_health_probe_cache_for_tests(board_id)
