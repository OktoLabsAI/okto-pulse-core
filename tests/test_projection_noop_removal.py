"""An empty removal scope has no native commit receipt to acknowledge."""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.kg import primitives, transaction
from okto_pulse.core.kg.projection_removals import (
    DeferredProjectionProgress,
    GraphRemovalProgress,
)


def test_empty_removal_rolls_back_without_preparing_a_graph_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class _Scope:
        start_commit_token = "graph-baseline"

        async def rollback(self) -> None:
            events.append("rollback")

        async def commit(self) -> None:
            raise AssertionError("an empty removal must not commit")

    class _GraphTransaction:
        async def begin(self, _board_id, **_kwargs):
            return _Scope()

    class _Orchestrator:
        def __init__(self, **_kwargs):
            self.records = []
            self.counters = SimpleNamespace()

        def reconcile_projection_active_set(self, _intent) -> None:
            events.append("reconcile_no_change")

    monkeypatch.setattr(
        primitives, "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=_GraphTransaction()),
    )
    monkeypatch.setattr(transaction, "TransactionOrchestrator", _Orchestrator)
    monkeypatch.setattr(primitives, "card_removal_intents", lambda **_kwargs: [object()])
    monkeypatch.setattr(primitives, "dependency_removal_intents", lambda **_kwargs: [])
    monkeypatch.setattr(primitives, "card_dependency_removal_intents", lambda **_kwargs: [])

    result = primitives._do_graph_commit(
        "board-noop", "session-noop", {}, {}, {},
        "system:historical_consolidation", object(), "healthy", "hash",
        "card-noop", frozenset(), "card",
        primitives.SpecLineageParentIntent.PRESERVE,
        frozenset(), (), allow_known_removals=True,
        projection_operation_id="operation-noop",
        projection_precommit=lambda *_args, **_kwargs: events.append("prepare"),
    )

    assert isinstance(result, GraphRemovalProgress)
    assert result.graph_unchanged is True
    assert result.records == ()
    assert events == ["reconcile_no_change", "rollback"]


def test_removal_with_graph_changes_keeps_precommit_and_native_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class _Scope:
        start_commit_token = "graph-baseline"

        async def rollback(self) -> None:
            raise AssertionError("a successful removal must not roll back")

        async def commit(self) -> None:
            events.append("commit")

    class _GraphTransaction:
        async def begin(self, _board_id, **_kwargs):
            return _Scope()

    class _Orchestrator:
        def __init__(self, **_kwargs):
            self.records = []
            self.counters = SimpleNamespace()

        def reconcile_projection_active_set(self, _intent) -> None:
            self.records.append(object())
            events.append("reconcile_change")

    monkeypatch.setattr(
        primitives, "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=_GraphTransaction()),
    )
    monkeypatch.setattr(transaction, "TransactionOrchestrator", _Orchestrator)
    monkeypatch.setattr(primitives, "card_removal_intents", lambda **_kwargs: [object()])
    monkeypatch.setattr(primitives, "dependency_removal_intents", lambda **_kwargs: [])
    monkeypatch.setattr(primitives, "card_dependency_removal_intents", lambda **_kwargs: [])

    result = primitives._do_graph_commit(
        "board-change", "session-change", {}, {}, {},
        "system:historical_consolidation", object(), "healthy", "hash",
        "card-change", frozenset(), "card",
        primitives.SpecLineageParentIntent.PRESERVE,
        frozenset(), (), allow_known_removals=True,
        projection_operation_id="operation-change",
        projection_precommit=lambda *_args, **_kwargs: events.append("prepare"),
    )

    assert isinstance(result, GraphRemovalProgress)
    assert result.graph_unchanged is False
    assert len(result.records) == 1
    assert events == ["reconcile_change", "prepare", "commit"]


@pytest.mark.asyncio
async def test_empty_removal_progress_needs_no_native_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _commit(*_args, **_kwargs):
        return DeferredProjectionProgress("session-noop", graph_unchanged=True)

    async def _unexpected_durability(**_kwargs):
        raise AssertionError("no graph write needs a durability barrier")

    monkeypatch.setattr(consolidation, "commit_consolidation", _commit)
    monkeypatch.setattr(
        consolidation, "_ensure_entry_admitted_by_reservation_under_writer",
        lambda _entry: None,
    )
    monkeypatch.setattr(
        consolidation, "_ensure_board_graph_durable", _unexpected_durability,
    )
    prepared: list[str] = []
    receipts: list[tuple[str, str]] = []
    entry = SimpleNamespace(
        id="entry-noop", board_id="board-noop", artifact_type="card",
        artifact_id="card-noop", source="state_transition", claim_token="claim-noop",
    )

    result = await consolidation._commit_consolidation_with_board_graph_lifecycle(
        entry=entry,
        session_id="session-noop",
        summary_text="No changes to remove",
        db=object(),
        defer_session_finalization=True,
        enter_graph_write=lambda _mutation_ref: nullcontext(object()),
        allow_known_removals=True,
        journaled_prepared=prepared,
        journaled_receipts=receipts,
    )

    assert isinstance(result, DeferredProjectionProgress)
    assert result.graph_unchanged is True
    assert prepared == []
    assert receipts == []


@pytest.mark.asyncio
async def test_empty_removal_cannot_bypass_an_existing_prepared_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _commit(*_args, **_kwargs):
        return DeferredProjectionProgress("session-noop", graph_unchanged=True)

    monkeypatch.setattr(consolidation, "commit_consolidation", _commit)
    monkeypatch.setattr(
        consolidation, "_ensure_entry_admitted_by_reservation_under_writer",
        lambda _entry: None,
    )
    entry = SimpleNamespace(
        id="entry-noop", board_id="board-noop", artifact_type="card",
        artifact_id="card-noop", source="state_transition", claim_token="claim-noop",
    )

    with pytest.raises(RuntimeError, match="projection_noop_operation_prepared"):
        await consolidation._commit_consolidation_with_board_graph_lifecycle(
            entry=entry,
            session_id="session-noop",
            summary_text="No changes to remove",
            db=object(),
            defer_session_finalization=True,
            enter_graph_write=lambda _mutation_ref: nullcontext(object()),
            allow_known_removals=True,
            journaled_prepared=["already-prepared"],
            journaled_receipts=[],
        )
