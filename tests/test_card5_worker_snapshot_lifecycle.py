"""Card 5 worker durability behavior for incomplete source snapshots."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
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
        "under_safe_write",
        lambda *_args, **_kwargs: nullcontext(),
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
