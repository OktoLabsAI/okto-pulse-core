"""Phase observations cannot change consolidation, leak payloads or replay writes."""

import asyncio
import logging

import pytest

from okto_pulse.core.kg import consolidation_timing as timing

pytestmark = pytest.mark.asyncio


async def test_result_identity_elapsed_and_private_payload(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger=timing.__name__)
    ticks = iter((10.0, 10.25))
    monkeypatch.setattr(timing, "perf_counter", lambda: next(ticks))
    result = {"secret": "private-candidate-content"}
    calls = 0

    async def operation():
        nonlocal calls
        calls += 1
        return result

    assert (
        await timing.observe_consolidation_phase(
            "graph_dispatch", "session-timing", operation()
        )
        is result
    )
    assert calls == 1
    (record,) = caplog.records
    assert record.phase == "graph_dispatch"
    assert record.outcome == "returned"
    assert record.elapsed_s == 0.25
    assert record.session_id == "session-timing"
    assert "private-candidate-content" not in caplog.text
    assert record.exc_info is None


@pytest.mark.parametrize(
    "failure", [ValueError("private-error"), asyncio.CancelledError()]
)
async def test_failure_and_cancellation_identity_preserved(failure, caplog):
    caplog.set_level(logging.INFO, logger=timing.__name__)

    async def operation():
        raise failure

    with pytest.raises(type(failure)) as caught:
        await timing.observe_consolidation_phase("health_admission", "s", operation())
    assert caught.value is failure
    (record,) = caplog.records
    assert record.outcome == (
        "cancelled" if isinstance(failure, asyncio.CancelledError) else "raised"
    )
    assert "private-error" not in caplog.text
    assert record.exc_info is None


@pytest.mark.parametrize("broken_part", ["clock", "end_clock", "enabled", "emit"])
@pytest.mark.parametrize("fails", [False, True])
async def test_broken_diagnostics_do_not_change_the_operation(
    monkeypatch, broken_part, fails
):
    marker = object()
    failure = ValueError("business failure")
    calls = 0
    monkeypatch.setattr(timing.logger, "isEnabledFor", lambda _level: True)

    def broken(*_args, **_kwargs):
        raise RuntimeError("telemetry failed")

    if broken_part == "clock":
        monkeypatch.setattr(timing, "perf_counter", broken)
    elif broken_part == "end_clock":
        reads = iter((10.0,))
        monkeypatch.setattr(timing, "perf_counter", lambda: next(reads))
    else:
        monkeypatch.setattr(
            timing.logger,
            "isEnabledFor" if broken_part == "enabled" else "info",
            broken,
        )

    async def operation():
        nonlocal calls
        calls += 1
        if fails:
            raise failure
        return marker

    if fails:
        with pytest.raises(ValueError) as caught:
            await timing.observe_consolidation_phase(
                "audit_outbox_stage", "s", operation()
            )
        assert caught.value is failure
    else:
        assert (
            await timing.observe_consolidation_phase(
                "audit_outbox_stage", "s", operation()
            )
            is marker
        )
    assert calls == 1


async def test_disabled_logging_does_not_read_clock(monkeypatch):
    monkeypatch.setattr(timing.logger, "isEnabledFor", lambda _level: False)

    def forbidden():
        pytest.fail("disabled diagnostics read the clock")

    monkeypatch.setattr(timing, "perf_counter", forbidden)

    async def operation():
        return 42

    assert (
        await timing.observe_consolidation_phase("session_finalize", "s", operation())
        == 42
    )


async def test_waiting_phase_remains_cancellable(caplog):
    caplog.set_level(logging.INFO, logger=timing.__name__)
    entered = asyncio.Event()
    settled = asyncio.Event()

    async def operation():
        try:
            entered.set()
            await asyncio.Event().wait()
        finally:
            settled.set()

    task = asyncio.create_task(
        timing.observe_consolidation_phase("health_admission", "s", operation())
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert settled.is_set()
    assert caplog.records[-1].outcome == "cancelled"


async def test_actual_deferred_retry_times_only_the_phases_it_executes(
    monkeypatch, caplog
):
    from test_kg_deferred_commit_finalization import (
        test_deferred_retry_restages_relational_without_replaying_graph,
    )

    caplog.set_level(logging.INFO, logger=timing.__name__)
    await test_deferred_retry_restages_relational_without_replaying_graph(monkeypatch)
    records = [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "kg.consolidation.phase"
    ]
    phases = [r.phase for r in records]
    assert phases.count("graph_dispatch") == 1
    # This existing saga fixture performs two relational restages after its
    # original write; neither restage may create a new graph phase.
    assert phases.count("cognitive_source_append") == 3
    assert phases.count("audit_outbox_stage") == 3
    assert "session_finalize" not in phases, "deferred caller still owns finalization"
    assert all(r.session_id == "session-deferred-retry" for r in records)
    assert all(r.outcome == "returned" for r in records)
