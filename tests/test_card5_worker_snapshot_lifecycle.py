"""Card 5 worker durability behavior for incomplete source snapshots."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.ports.consolidation import ConsolidationQueueRecord


def _entry() -> ConsolidationQueueRecord:
    delete_event_id = "delete-event-card5"
    artifact_id = "deleted-spec-card5"
    return ConsolidationQueueRecord(
        id="reconcile-card5",
        board_id="board-card5",
        artifact_type="spec",
        artifact_id=artifact_id,
        status="claimed",
        attempts=0,
        last_error=None,
        next_retry_at=None,
        claimed_at=datetime.now(timezone.utc),
        claim_timeout_at=None,
        worker_id="card5-worker",
        claimed_by_session_id="card5-worker",
        triggered_at=datetime.now(timezone.utc),
        priority="high",
        work_kind="stale_reconcile",
        generation=1,
        payload={
            "schema_version": 1,
            "delete_event_id": delete_event_id,
            "source_refs": [f"spec:{artifact_id}"],
        },
        delete_event_id=delete_event_id,
        claim_token="claim-card5",
    )


def _patch_worker_shell(monkeypatch, *, lifecycle_calls: list[str]) -> None:
    async def _claim_is_current(*_args: Any, **_kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(
        consolidation,
        "_queue_claim_is_current_and_unfenced",
        _claim_is_current,
    )
    monkeypatch.setattr(
        consolidation,
        "guarded_board_write",
        lambda *_args, **_kwargs: nullcontext(
            SimpleNamespace(
                durability_applied=True,
                ensure_owned=lambda **_kwargs: None,
            )
        ),
    )

    def _lifecycle(**_kwargs: Any) -> SimpleNamespace:
        lifecycle_calls.append("lifecycle")
        return SimpleNamespace()

    monkeypatch.setattr(
        consolidation,
        "_apply_board_graph_lifecycle_after_commit",
        _lifecycle,
    )


@pytest.mark.asyncio
async def test_source_snapshot_incomplete_skips_graph_lifecycle(monkeypatch):
    from okto_pulse.core.kg import canonical_stale_reconciler

    lifecycle_calls: list[str] = []

    async def _incomplete_snapshot(_db: object, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(
            incomplete=True,
            incomplete_cause="db_missing",
            failed_types=(),
            target_identity_count=1,
            target_found_count=0,
            target_demoted_count=0,
            target_already_converged_count=0,
            target_skipped_cognitive_count=0,
            target_preserved_canonical_count=0,
        )

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _incomplete_snapshot,
    )
    _patch_worker_shell(monkeypatch, lifecycle_calls=lifecycle_calls)

    processed = await consolidation._process_stale_reconcile_entry(
        object(),
        _entry(),
    )

    assert processed is False
    assert lifecycle_calls == []


@pytest.mark.asyncio
async def test_partial_type_failure_keeps_graph_lifecycle(monkeypatch):
    from okto_pulse.core.kg import canonical_stale_reconciler

    lifecycle_calls: list[str] = []

    async def _partial_failure(_db: object, **_kwargs: Any) -> SimpleNamespace:
        _kwargs["before_graph_write"]()
        return SimpleNamespace(
            incomplete=True,
            incomplete_cause=None,
            failed_types=("Decision",),
            target_identity_count=1,
            target_found_count=0,
            target_demoted_count=0,
            target_already_converged_count=0,
            target_skipped_cognitive_count=0,
            target_preserved_canonical_count=0,
        )

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _partial_failure,
    )
    _patch_worker_shell(monkeypatch, lifecycle_calls=lifecycle_calls)

    processed = await consolidation._process_stale_reconcile_entry(
        object(),
        _entry(),
    )

    assert processed is False
    assert lifecycle_calls == ["lifecycle"]


@pytest.mark.asyncio
async def test_exception_after_graph_callback_runs_lifecycle_before_reraise(
    monkeypatch,
):
    from okto_pulse.core.kg import canonical_stale_reconciler

    lifecycle_calls: list[str] = []

    async def _write_then_fail(_db: object, **kwargs: Any) -> SimpleNamespace:
        kwargs["before_graph_write"]()
        raise RuntimeError("graph scan failed after possible auto-commit")

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _write_then_fail,
    )
    _patch_worker_shell(monkeypatch, lifecycle_calls=lifecycle_calls)

    with pytest.raises(
        RuntimeError,
        match="graph scan failed after possible auto-commit",
    ):
        await consolidation._process_stale_reconcile_entry(
            object(),
            _entry(),
        )

    assert lifecycle_calls == ["lifecycle"]


@pytest.mark.asyncio
async def test_cancelled_graph_scan_drains_before_lifecycle_and_reraise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import canonical_stale_reconciler

    event_loop_thread = threading.get_ident()
    events: list[str] = []
    threads: dict[str, int] = {}
    graph_started = threading.Event()
    release_graph = threading.Event()
    lease = SimpleNamespace(
        durability_applied=True,
        ensure_owned=lambda **_kwargs: None,
    )

    def _snapshot(_board_id: str):
        threads["snapshot"] = threading.get_ident()
        events.append("snapshot")
        return {}, True, None

    async def _scan(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        threads["graph"] = threading.get_ident()
        events.append("graph_started")
        graph_started.set()
        release_graph.wait(timeout=5.0)
        events.append("graph_finished")
        return []

    async def _claim_is_current(*_args: Any, **_kwargs: Any) -> bool:
        return True

    def _lifecycle(**kwargs: Any) -> SimpleNamespace:
        assert kwargs["write_lease"] is lease
        assert events[-1] == "graph_finished"
        threads["lifecycle"] = threading.get_ident()
        events.append("lifecycle")
        return SimpleNamespace()

    @contextmanager
    def _guarded_write(*_args: Any, **_kwargs: Any):
        threads["callback"] = threading.get_ident()
        events.append("callback")
        try:
            yield lease
        finally:
            threads["guard_exit"] = threading.get_ident()
            events.append("guard_exit")

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "_build_source_classification_map",
        _snapshot,
    )
    monkeypatch.setattr(
        canonical_stale_reconciler,
        "_scan_and_demote",
        _scan,
    )
    monkeypatch.setattr(
        consolidation,
        "_queue_claim_is_current_and_unfenced",
        _claim_is_current,
    )
    monkeypatch.setattr(
        consolidation,
        "_apply_board_graph_lifecycle_after_commit",
        _lifecycle,
    )
    monkeypatch.setattr(
        consolidation,
        "guarded_board_write",
        _guarded_write,
    )

    task = asyncio.create_task(
        consolidation._process_stale_reconcile_entry(
            object(),
            _entry(),
        )
    )
    try:
        assert await asyncio.to_thread(graph_started.wait, 1.0) is True
        task.cancel()
        await asyncio.sleep(0)
        assert task.done() is False
    finally:
        release_graph.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert events == [
        "callback",
        "snapshot",
        "graph_started",
        "graph_finished",
        "lifecycle",
        "guard_exit",
    ]
    assert threads["snapshot"] != event_loop_thread
    assert threads["graph"] != event_loop_thread
    assert threads["callback"] == event_loop_thread
    assert threads["lifecycle"] == event_loop_thread
    assert threads["guard_exit"] == event_loop_thread
