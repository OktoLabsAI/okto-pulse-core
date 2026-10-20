"""Pre-journal graph failures must compensate auto-committed writes."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import primitives, transaction


@pytest.mark.parametrize("failure", ["primitive", "backend"])
def test_graph_failure_before_journal_preparation_compensates(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    records = [object()]
    events: list[str] = []

    class _Scope:
        async def rollback(self):
            events.append("rollback")

    class _GraphTransaction:
        async def begin(self, _board_id, **_kwargs):
            return _Scope()

    class _Orchestrator:
        def __init__(self, **_kwargs):
            self.records = []

        def reconcile_projection_active_set(self, _intent):
            self.records.extend(records)
            if failure == "primitive":
                raise primitives.KGPrimitiveError(
                    "injected_precommit_failure", "graph mutation failed"
                )
            raise RuntimeError("graph backend failed")

    def _compensate(_board_id, _session_id, applied):
        assert applied == records
        events.append("compensate")

    monkeypatch.setattr(
        primitives, "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=_GraphTransaction()),
    )
    monkeypatch.setattr(transaction, "TransactionOrchestrator", _Orchestrator)
    monkeypatch.setattr(primitives, "card_removal_intents", lambda **_kwargs: [object()])
    monkeypatch.setattr(primitives, "dependency_removal_intents", lambda **_kwargs: [])
    monkeypatch.setattr(
        primitives, "card_dependency_removal_intents", lambda **_kwargs: []
    )
    monkeypatch.setattr(primitives, "_compensate_graph_writes", _compensate)

    with pytest.raises(primitives.KGPrimitiveError):
        primitives._do_graph_commit(
            "board-precommit", "session-precommit", {}, {}, {},
            "system:historical_consolidation", object(), "healthy", "hash",
            "spec-precommit", frozenset(), "spec",
            primitives.SpecLineageParentIntent.PRESERVE,
            frozenset(), (), allow_known_removals=True,
            projection_operation_id="operation-precommit",
            projection_precommit=lambda *_args, **_kwargs: events.append("prepare"),
        )

    assert events == ["rollback", "compensate"]
