"""Card 8 — durable, bounded stale-sweep coordination contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.events.handlers import kg_decay_tick
from okto_pulse.core.kg import canonical_stale_reconciler as reconciler
from okto_pulse.core.ports.consolidation import ConsolidationQueueRecord
from okto_pulse.core.ports.stale_sweep import (
    StaleSweepBatchRequest,
    StaleSweepCandidate,
    StaleSweepRunAction,
    StaleSweepRunReceipt,
    StaleSweepScheduleReceipt,
    register_stale_sweep_port,
)


NOW = datetime(2026, 7, 21, 12, tzinfo=timezone.utc)


def _sweep_entry(**overrides) -> ConsolidationQueueRecord:
    values = {
        "id": "sweep-1",
        "board_id": "board-1",
        "artifact_type": "board",
        "artifact_id": "board-1",
        "status": "claimed",
        "attempts": 0,
        "last_error": None,
        "next_retry_at": None,
        "claimed_at": NOW,
        "claim_timeout_at": None,
        "worker_id": "worker-1",
        "claimed_by_session_id": "worker-1",
        "triggered_at": NOW,
        "priority": "low",
        "work_kind": "stale_sweep",
        "generation": 0,
        "payload": {"cursor": "", "budget": 2, "attempt": 0},
        "delete_event_id": None,
        "claim_token": "claim-1",
    }
    values.update(overrides)
    return ConsolidationQueueRecord(**values)


@pytest.mark.parametrize(
    "entry",
    [
        _sweep_entry(artifact_type="spec"),
        _sweep_entry(artifact_id="other-board"),
        _sweep_entry(generation=1),
        _sweep_entry(delete_event_id="delete-1"),
        _sweep_entry(payload={"cursor": None, "budget": 2, "attempt": 0}),
        _sweep_entry(payload={"cursor": "", "budget": True, "attempt": 0}),
        _sweep_entry(payload={"cursor": "", "budget": 0, "attempt": 0}),
        _sweep_entry(payload={"cursor": "", "budget": 2, "attempt": True}),
        _sweep_entry(payload={"cursor": "bad", "budget": 2, "attempt": 0}),
        _sweep_entry(payload={"cursor": "", "budget": 2, "attempt": 0, "extra": 1}),
    ],
)
def test_stale_sweep_payload_is_strict_and_never_broadens_scope(entry) -> None:
    assert consolidation._validated_stale_sweep_payload(entry) is None


def test_stale_sweep_payload_accepts_canonical_cursor() -> None:
    cursor = reconciler.encode_stale_sweep_cursor(StaleSweepCandidate("card", "card-1"))
    entry = _sweep_entry(payload={"cursor": cursor, "budget": 3, "attempt": 0})
    assert consolidation._validated_stale_sweep_payload(entry) == (cursor, 3, 0)


def test_sprint_sources_are_governed_stale_sweep_candidates() -> None:
    candidate = StaleSweepCandidate("sprint", "sprint-1")
    cursor = reconciler.encode_stale_sweep_cursor(candidate)
    assert reconciler.decode_stale_sweep_cursor(cursor) == (
        "sprint",
        "sprint-1",
    )


def test_stale_sweep_batch_cursor_contract_rejects_skips_and_regressions() -> None:
    cursor = reconciler.encode_stale_sweep_cursor(StaleSweepCandidate("card", "b"))
    with pytest.raises(ValueError, match="cursor_contract"):
        StaleSweepBatchRequest(
            entry_id="sweep-1",
            claim_token="claim-1",
            board_id="board-1",
            cursor=cursor,
            budget=2,
            attempt=0,
            candidates=(StaleSweepCandidate("card", "d"),),
            next_cursor=reconciler.encode_stale_sweep_cursor(
                StaleSweepCandidate("card", "c")
            ),
            has_more=False,
            now=NOW,
        )
    with pytest.raises(ValueError, match="cursor_contract"):
        StaleSweepBatchRequest(
            entry_id="sweep-1",
            claim_token="claim-1",
            board_id="board-1",
            cursor=cursor,
            budget=2,
            attempt=0,
            candidates=(),
            next_cursor=cursor,
            has_more=True,
            now=NOW,
        )
    with pytest.raises(ValueError, match="cursor_contract"):
        StaleSweepBatchRequest(
            entry_id="sweep-1",
            claim_token="claim-1",
            board_id="board-1",
            cursor=cursor,
            budget=2,
            attempt=0,
            candidates=(),
            next_cursor="",
            has_more=False,
            now=NOW,
        )

    live_only = StaleSweepBatchRequest(
        entry_id="sweep-1",
        claim_token="claim-1",
        board_id="board-1",
        cursor=cursor,
        budget=2,
        attempt=0,
        candidates=(),
        next_cursor=reconciler.encode_stale_sweep_cursor(
            StaleSweepCandidate("card", "c")
        ),
        has_more=True,
        now=NOW,
    )
    assert live_only.has_more is True


class _Rows:
    def __init__(self, rows):
        self.rows = rows


class _GraphScope:
    def __init__(self, refs_by_type: dict[str, list[str]], *, fail: bool = False):
        self.refs_by_type = refs_by_type
        self.fail = fail
        self.queries: list[tuple[str, dict]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def execute(self, query: str, params: dict):
        if self.fail:
            raise RuntimeError("injected graph read failure")
        assert "MATCH (n)" in query
        assert "string_split(n.source_artifact_ref, ':')" in query
        assert "RETURN DISTINCT artifact_type, artifact_id" in query
        assert "ORDER BY artifact_type ASC, artifact_id ASC" in query
        assert "LIMIT $scan_limit" in query
        limit = params["scan_limit"]
        assert limit >= 2
        identities = {
            identity
            for refs in self.refs_by_type.values()
            for ref in refs
            if (identity := reconciler._source_identity_from_ref(ref)) is not None
            and identity[0] in params["governed_types"]
        }
        after = (params["after_type"], params["after_id"])
        if params["after_type"]:
            identities = {identity for identity in identities if identity > after}
        ordered = sorted(identities)
        self.queries.append((query, dict(params)))
        return _Rows(ordered[:limit])


class _GraphTransaction:
    def __init__(self, scope: _GraphScope):
        self.scope = scope

    async def begin(self, _board_id: str):
        return self.scope


class _GraphRuntime:
    def exists(self, _board_id: str) -> bool:
        return True


@pytest.mark.asyncio
async def test_stale_sweep_page_is_globally_ordered_deduped_and_bounded(
    monkeypatch,
) -> None:
    scope = _GraphScope(
        {
            "Decision": [
                "spec:z:decision:1",
                "card:b",
                "test:a",
                "unknown:x",
                "board:board-1",
            ],
            "Requirement": [
                "task:a",
                "ideation:c",
                "refinement:d",
                "spec:live",
            ],
        }
    )
    registry = SimpleNamespace(
        graph_runtime_store=_GraphRuntime(),
        graph_transaction=_GraphTransaction(scope),
    )
    monkeypatch.setattr(reconciler, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        reconciler,
        "_build_source_classification_map",
        lambda _board_id: ({("spec", "live"): object()}, True, None),
    )

    first = await reconciler.enumerate_stale_sweep_page("board-1", cursor="", budget=2)
    assert [(c.artifact_type, c.artifact_id) for c in first.candidates] == [
        ("card", "a"),
        ("card", "b"),
    ]
    assert first.has_more is True
    assert first.graph_rows_scanned == 3
    assert len(scope.queries) == 1
    assert all(params["scan_limit"] == 3 for _, params in scope.queries)

    second = await reconciler.enumerate_stale_sweep_page(
        "board-1", cursor=first.next_cursor, budget=2
    )
    assert [(c.artifact_type, c.artifact_id) for c in second.candidates] == [
        ("ideation", "c"),
        ("refinement", "d"),
    ]
    assert second.has_more is True
    assert second.graph_rows_scanned == 3
    assert len(scope.queries) == 2


@pytest.mark.asyncio
async def test_stale_sweep_live_only_page_advances_bounded_inventory_cursor(
    monkeypatch,
) -> None:
    scope = _GraphScope({"Decision": ["card:a", "card:b", "spec:c"]})
    registry = SimpleNamespace(
        graph_runtime_store=_GraphRuntime(),
        graph_transaction=_GraphTransaction(scope),
    )
    monkeypatch.setattr(reconciler, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        reconciler,
        "_build_source_classification_map",
        lambda _board_id: (
            {("card", "a"): object(), ("card", "b"): object()},
            True,
            None,
        ),
    )

    first = await reconciler.enumerate_stale_sweep_page("board-1", cursor="", budget=2)
    assert first.candidates == ()
    assert first.has_more is True
    assert reconciler.decode_stale_sweep_cursor(first.next_cursor) == ("card", "b")
    assert first.graph_rows_scanned == 3

    second = await reconciler.enumerate_stale_sweep_page(
        "board-1", cursor=first.next_cursor, budget=2
    )
    assert [(c.artifact_type, c.artifact_id) for c in second.candidates] == [
        ("spec", "c")
    ]
    assert second.has_more is False


@pytest.mark.asyncio
async def test_stale_sweep_seven_identities_resume_in_four_bounded_pages(
    monkeypatch,
) -> None:
    scope = _GraphScope({"Decision": [f"card:card-{index}" for index in range(7)]})
    registry = SimpleNamespace(
        graph_runtime_store=_GraphRuntime(),
        graph_transaction=_GraphTransaction(scope),
    )
    monkeypatch.setattr(reconciler, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        reconciler,
        "_build_source_classification_map",
        lambda _board_id: ({}, True, None),
    )

    cursor = ""
    collected: list[tuple[str, str]] = []
    while True:
        page = await reconciler.enumerate_stale_sweep_page(
            "board-1",
            cursor=cursor,
            budget=2,
        )
        assert page.complete is True
        assert len(page.candidates) <= 2
        assert page.graph_rows_scanned <= 3
        collected.extend(
            (candidate.artifact_type, candidate.artifact_id)
            for candidate in page.candidates
        )
        if not page.has_more:
            break
        assert page.next_cursor != cursor
        cursor = page.next_cursor

    assert collected == [("card", f"card-{index}") for index in range(7)]
    assert len(scope.queries) == 4


@pytest.mark.asyncio
async def test_stale_sweep_page_fails_closed_on_any_graph_type_error(
    monkeypatch,
) -> None:
    scope = _GraphScope({"Decision": ["card:a"]}, fail=True)
    registry = SimpleNamespace(
        graph_runtime_store=_GraphRuntime(),
        graph_transaction=_GraphTransaction(scope),
    )
    monkeypatch.setattr(reconciler, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        reconciler,
        "_build_source_classification_map",
        lambda _board_id: ({}, True, None),
    )

    page = await reconciler.enumerate_stale_sweep_page("board-1", cursor="", budget=2)
    assert page.complete is False
    assert page.candidates == ()
    assert page.next_cursor == ""
    assert page.incomplete_cause == "graph_scan_incomplete"
    assert page.failed_types == reconciler.ALL_NODE_TYPES


@pytest.mark.asyncio
async def test_stale_sweep_page_fails_closed_when_graph_open_fails(
    monkeypatch,
) -> None:
    class _FailingTransaction:
        async def begin(self, _board_id: str):
            raise RuntimeError("graph locked")

    registry = SimpleNamespace(
        graph_runtime_store=_GraphRuntime(),
        graph_transaction=_FailingTransaction(),
    )
    monkeypatch.setattr(reconciler, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        reconciler,
        "_build_source_classification_map",
        lambda _board_id: ({}, True, None),
    )

    page = await reconciler.enumerate_stale_sweep_page("board-1", cursor="", budget=2)
    assert page.complete is False
    assert page.next_cursor == ""
    assert page.incomplete_cause == "graph_scan_incomplete"


@pytest.mark.asyncio
async def test_stale_sweep_page_fails_closed_when_source_snapshot_raises(
    monkeypatch,
) -> None:
    def _raise(_board_id: str):
        raise RuntimeError("source reader unavailable")

    monkeypatch.setattr(reconciler, "_build_source_classification_map", _raise)
    page = await reconciler.enumerate_stale_sweep_page("board-1", cursor="", budget=2)
    assert page.complete is False
    assert page.next_cursor == ""
    assert page.incomplete_cause == "source_snapshot_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runtime_mode", "expected_reason"),
    [
        ("missing", "graph_unavailable"),
        ("probe_error", "graph_runtime_probe_failed"),
    ],
)
async def test_degraded_graph_preserves_cursor_and_reschedules(
    monkeypatch,
    runtime_mode: str,
    expected_reason: str,
) -> None:
    entry = _sweep_entry()

    class _Store:
        async def board_exists(self, _db, *, board_id: str) -> bool:
            return board_id == entry.board_id

    class _Runtime:
        def exists(self, _board_id: str) -> bool:
            if runtime_mode == "probe_error":
                raise RuntimeError("runtime probe unavailable")
            return False

    class _Port:
        request = None

        async def reschedule_stale_sweep(self, _db, request):
            self.request = request
            return StaleSweepRunReceipt(
                entry_id=request.entry_id,
                board_id=request.board_id,
                action=StaleSweepRunAction.RESCHEDULED,
                cursor=request.cursor,
                budget=request.budget,
                attempt=request.attempt,
                enqueued=0,
                has_more=True,
                reason=request.reason,
            )

    port = _Port()
    monkeypatch.setattr(
        consolidation, "get_consolidation_persistence_port", lambda: _Store()
    )
    monkeypatch.setattr(consolidation, "get_stale_sweep_port", lambda: port)
    monkeypatch.setattr(
        consolidation,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_runtime_store=_Runtime()),
    )

    receipt = await consolidation._process_stale_sweep_entry(object(), entry)
    assert isinstance(receipt, StaleSweepRunReceipt)
    assert receipt.action is StaleSweepRunAction.RESCHEDULED
    assert port.request.cursor == ""
    assert port.request.attempt == 0
    assert port.request.reason == expected_reason
    assert port.request.retry_at > NOW


@pytest.mark.asyncio
async def test_tick_schedules_sweep_only_after_decay_graph_scope_closed(
    monkeypatch,
) -> None:
    state = {"graph_active": False, "scheduled": 0}

    def _process_board(_board_id: str, _cutoff: str, *, batch_size: int):
        assert batch_size > 0
        state["graph_active"] = True
        state["graph_active"] = False
        return (0, 0)

    class _Port:
        async def schedule_stale_sweep(self, _session, request):
            assert state["graph_active"] is False
            state["scheduled"] += 1
            return StaleSweepScheduleReceipt(
                board_id=request.board_id,
                sweep_id="sweep-tick",
                scheduled=True,
                board_present=True,
                cursor="",
                budget=request.budget,
                attempt=0,
            )

    async def _no_persist(*_args, **_kwargs):
        return None

    monkeypatch.setattr(kg_decay_tick, "_process_board_sync", _process_board)
    monkeypatch.setattr(kg_decay_tick, "_persist_tick_run", _no_persist)
    monkeypatch.setattr(kg_decay_tick, "_optional_delivery_ledger_port", lambda: None)
    register_stale_sweep_port(_Port())

    await kg_decay_tick._run_daily_tick(
        tick_id="tick-card8",
        session=object(),
        board_id="board-1",
        stale_sweep_budget=7,
    )
    assert state["scheduled"] == 1
