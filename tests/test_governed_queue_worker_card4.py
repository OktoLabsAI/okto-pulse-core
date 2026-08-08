"""Card 4 -- governed queue branching, fencing and acknowledgement semantics."""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.application.processors.consolidation import (
    ConsolidationProcessor,
    _process_queue_entry,
)
from okto_pulse.core.ports.consolidation import (
    ConsolidationProjectionInputs,
    ConsolidationQueueRecord,
    get_consolidation_persistence_port,
    register_consolidation_persistence_port,
)


def _entry(
    *,
    entry_id: str = "card4-entry",
    work_kind: str = "consolidate",
    generation: int = 0,
    delete_event_id: str | None = None,
    payload: dict[str, Any] | None = None,
    status: str = "pending",
    claim_token: str | None = None,
    board_id: str = "card4-board",
    artifact_id: str = "card4-spec",
) -> ConsolidationQueueRecord:
    return ConsolidationQueueRecord(
        id=entry_id,
        board_id=board_id,
        artifact_type="spec",
        artifact_id=artifact_id,
        status=status,
        attempts=0,
        last_error=None,
        next_retry_at=None,
        claimed_at=(datetime.now(timezone.utc) if status == "claimed" else None),
        claim_timeout_at=None,
        worker_id=("old-worker" if status == "claimed" else None),
        claimed_by_session_id=("old-worker" if status == "claimed" else None),
        triggered_at=datetime.now(timezone.utc),
        priority="high",
        work_kind=work_kind,
        generation=generation,
        payload=payload,
        delete_event_id=delete_event_id,
        claim_token=claim_token,
    )


def _valid_reconcile_entry(**overrides: Any) -> ConsolidationQueueRecord:
    artifact_id = str(overrides.get("artifact_id", "deleted-spec"))
    delete_event_id = str(overrides.get("delete_event_id", "delete-event-g1"))
    values: dict[str, Any] = {
        "entry_id": "reconcile-g1",
        "work_kind": "stale_reconcile",
        "generation": 1,
        "delete_event_id": delete_event_id,
        "payload": {
            "schema_version": 1,
            "delete_event_id": delete_event_id,
            "source_refs": [f"spec:{artifact_id}"],
        },
        "artifact_id": artifact_id,
    }
    values.update(overrides)
    return _entry(**values)


class _MemoryConsolidationStore:
    def __init__(
        self,
        entries: tuple[ConsolidationQueueRecord, ...] = (),
        *,
        fence_result: bool = True,
        ack_result: bool = True,
    ) -> None:
        self.entries = {entry.id: entry for entry in entries}
        self.fence_result = fence_result
        self.ack_result = ack_result
        self.fence_calls: list[dict[str, Any]] = []
        self.ack_calls: list[dict[str, Any]] = []
        self.load_calls: list[tuple[str, str]] = []
        self.commit_count = 0
        self.rollback_count = 0

    async def load_artifact(self, _context, *, artifact_type, artifact_id):
        self.load_calls.append((artifact_type, artifact_id))
        return SimpleNamespace(title="Legacy spec")

    async def load_projection_inputs(self, _context, **_identity):
        return ConsolidationProjectionInputs()

    async def count_pending(self, _context) -> int:
        return sum(entry.status == "pending" for entry in self.entries.values())

    async def list_claimed_board_ids(self, _context) -> frozenset[str]:
        return frozenset(
            entry.board_id
            for entry in self.entries.values()
            if entry.status == "claimed"
        )

    async def list_ready_pending(self, _context, *, now):
        del now
        return tuple(
            entry for entry in self.entries.values() if entry.status == "pending"
        )

    async def list_stale_claims(self, _context, *, now, legacy_cutoff):
        del legacy_cutoff
        return tuple(
            entry
            for entry in self.entries.values()
            if entry.status == "claimed"
            and entry.claim_timeout_at is not None
            and entry.claim_timeout_at < now
        )

    async def save_queue_entries(self, _context, entries) -> None:
        for entry in entries:
            self.entries[entry.id] = entry

    async def get_queue_entry(self, _context, *, entry_id):
        return self.entries.get(entry_id)

    async def queue_claim_is_current_and_unfenced(self, _context, **identity):
        self.fence_calls.append(identity)
        return self.fence_result

    async def ack_claimed_queue_entry(self, _context, **identity):
        self.ack_calls.append(identity)
        if self.ack_result:
            self.entries.pop(str(identity["entry_id"]), None)
        return self.ack_result

    async def delete_queue_entry(self, _context, *, entry_id):
        raise AssertionError(f"legacy non-CAS ACK used for {entry_id}")

    async def commit(self, _context) -> None:
        self.commit_count += 1

    async def rollback(self, _context) -> None:
        self.rollback_count += 1


@asynccontextmanager
async def _scope():
    yield object()


@contextmanager
def _registered(store: _MemoryConsolidationStore):
    previous = get_consolidation_persistence_port()
    register_consolidation_persistence_port(store)
    try:
        yield
    finally:
        register_consolidation_persistence_port(previous)


def _patch_graph_write_shell(monkeypatch, *, lifecycle_calls: list[str]) -> None:
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

    def _lifecycle(**_kwargs):
        lifecycle_calls.append("lifecycle")
        return SimpleNamespace()

    monkeypatch.setattr(
        consolidation,
        "_apply_board_graph_lifecycle_after_commit",
        _lifecycle,
    )


@pytest.mark.asyncio
async def test_stale_reconcile_branches_before_artifact_load(monkeypatch):
    """The governed-delete lane reconciles by source ref, never by deleted row."""

    from okto_pulse.core.kg import canonical_stale_reconciler

    entry = _valid_reconcile_entry(status="claimed", claim_token="claim-g1")
    store = _MemoryConsolidationStore((entry,))
    reconciliations: list[dict[str, Any]] = []
    lifecycle_calls: list[str] = []

    class _BlockingExecution:
        async def run(self, operation):
            return operation()

        async def join(self, _timeout: float) -> int:
            return 0

    blocking_execution = _BlockingExecution()

    async def _reconcile(_db, **kwargs):
        kwargs["before_graph_write"]()
        reconciliations.append(kwargs)
        kwargs.pop("before_graph_write")
        return SimpleNamespace(
            incomplete=False,
            failed_types=(),
            **_complete_target_contract(),
        )

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _reconcile,
    )
    _patch_graph_write_shell(monkeypatch, lifecycle_calls=lifecycle_calls)

    with _registered(store):
        assert await _process_queue_entry(
            object(),
            entry,
            blocking_execution=blocking_execution,
        )

    assert store.load_calls == []
    assert reconciliations == [
        {
            "board_id": entry.board_id,
            "source_refs": [f"spec:{entry.artifact_id}"],
            "correlation_id": entry.delete_event_id,
            "blocking_execution": blocking_execution,
        }
    ]
    assert lifecycle_calls == ["lifecycle"]


@pytest.mark.asyncio
async def test_invalid_stale_payload_has_no_graph_write_or_ack(monkeypatch):
    entry = _valid_reconcile_entry(
        payload={
            "schema_version": 1,
            "delete_event_id": "delete-event-g1",
            "source_refs": ["spec:some-other-artifact"],
        }
    )
    store = _MemoryConsolidationStore((entry,))
    lifecycle_calls: list[str] = []
    failure_calls: list[str] = []
    _patch_graph_write_shell(monkeypatch, lifecycle_calls=lifecycle_calls)

    async def _mark_failed(_db, failed_entry, **_kwargs):
        failure_calls.append(failed_entry.id)
        failed_entry.status = "pending"

    processor = ConsolidationProcessor(_scope, batch_size=1)
    monkeypatch.setattr(processor, "_mark_failed", _mark_failed)

    with _registered(store):
        assert await processor.process_batch() == 0

    assert store.load_calls == []
    # The only fence read is the batch's ownership check before recording the
    # operational failure. The malformed intent never reaches graph code.
    assert len(store.fence_calls) == 1
    assert lifecycle_calls == []
    assert store.ack_calls == []
    assert failure_calls == [entry.id]


def _complete_target_contract() -> dict[str, int]:
    return {
        "target_identity_count": 1,
        "target_found_count": 0,
        "target_demoted_count": 0,
        "target_already_converged_count": 0,
        "target_skipped_cognitive_count": 0,
        "target_preserved_canonical_count": 0,
    }


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (SimpleNamespace(), False),
        ({}, False),
        (
            SimpleNamespace(
                incomplete=False,
                failed_types=(),
                **_complete_target_contract(),
            ),
            True,
        ),
        (
            {
                "incomplete": False,
                "failed_types": [],
                **_complete_target_contract(),
            },
            True,
        ),
        (SimpleNamespace(incomplete=True, failed_types=()), False),
        ({"incomplete": False, "failed_types": ["Decision"]}, False),
    ],
)
def test_stale_reconcile_completeness_is_explicit_and_fail_closed(result, expected):
    assert consolidation._stale_reconcile_is_complete(result) is expected


def test_stale_reconcile_empty_retry_cannot_ack_prior_partial_graph_failure():
    result = SimpleNamespace(
        incomplete=False,
        failed_types=(),
        **_complete_target_contract(),
    )

    assert (
        consolidation._stale_reconcile_is_complete(
            result,
            previous_error="stale_reconcile_graph_partial:Decision",
        )
        is False
    )
    assert (
        consolidation._stale_reconcile_failure_error(
            existing_error=None,
            reconcile_details={"failed_types": ["Requirement", "Decision"]},
        )
        == "stale_reconcile_graph_partial:Decision,Requirement"
    )


@pytest.mark.asyncio
async def test_incomplete_stale_reconcile_is_not_acknowledged(monkeypatch):
    from okto_pulse.core.kg import canonical_stale_reconciler

    entry = _valid_reconcile_entry()
    store = _MemoryConsolidationStore((entry,))
    lifecycle_calls: list[str] = []
    failure_calls: list[str] = []

    async def _incomplete(_db, **_kwargs):
        _kwargs["before_graph_write"]()
        return SimpleNamespace(incomplete=True, failed_types=("Decision",))

    async def _mark_failed(_db, failed_entry, **_kwargs):
        failure_calls.append(failed_entry.id)
        failed_entry.status = "pending"

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _incomplete,
    )
    _patch_graph_write_shell(monkeypatch, lifecycle_calls=lifecycle_calls)
    processor = ConsolidationProcessor(_scope, batch_size=1)
    monkeypatch.setattr(processor, "_mark_failed", _mark_failed)

    with _registered(store):
        assert await processor.process_batch() == 0

    assert lifecycle_calls == ["lifecycle"]
    assert failure_calls == [entry.id]
    assert store.ack_calls == []
    assert entry.id in store.entries


@pytest.mark.asyncio
async def test_lost_claim_is_neutral_before_stale_reconcile_write(monkeypatch):
    from okto_pulse.core.kg import canonical_stale_reconciler

    entry = _valid_reconcile_entry()
    store = _MemoryConsolidationStore((entry,), fence_result=False)
    failure_calls: list[str] = []

    async def _must_not_reconcile(*_args, **_kwargs):
        raise AssertionError("stale reconciliation ran after the claim was lost")

    async def _mark_failed(_db, failed_entry, **_kwargs):
        failure_calls.append(failed_entry.id)

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _must_not_reconcile,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)
    monkeypatch.setattr(processor, "_mark_failed", _mark_failed)

    with _registered(store):
        assert await processor.process_batch() == 0

    assert len(store.fence_calls) == 1
    assert store.ack_calls == []
    assert failure_calls == []
    assert entry.status == "claimed"
    assert entry.attempts == 0


@pytest.mark.asyncio
async def test_delete_between_extraction_and_publish_blocks_legacy_commit(monkeypatch):
    """AC6/TS3: a tombstone winning at the final re-check publishes nothing."""

    entry = _entry(status="claimed", claim_token="claim-before-delete")
    store = _MemoryConsolidationStore((entry,), fence_result=False)
    observed: list[str] = []

    worker_result = SimpleNamespace(
        nodes=[object()],
        edges=[],
        missing_link_candidates=[],
        raw_content="legacy spec body",
        relational_projection_candidate_ids=(),
        relational_projection_active_set_intent=None,
    )

    def _extract(*_args, **_kwargs):
        observed.append("extract")
        return worker_result

    async def _passthrough(_db, _entry, _artifact, result):
        return result

    async def _resolve(_db, _board_id, result):
        return result

    async def _begin(*_args, **_kwargs):
        return SimpleNamespace(session_id="uncommitted-session")

    async def _propose(*_args, **_kwargs):
        return SimpleNamespace()

    async def _abort(**_kwargs):
        observed.append("abort")

    async def _publish(**_kwargs):
        observed.append("publish")
        return SimpleNamespace(nodes_added=1, edges_added=0)

    monkeypatch.setattr(consolidation, "_run_deterministic_worker", _extract)
    monkeypatch.setattr(
        consolidation,
        "_materialize_lineage_endpoint_nodes",
        _passthrough,
    )
    monkeypatch.setattr(consolidation, "_resolve_missing_link_candidates", _resolve)
    monkeypatch.setattr(
        consolidation,
        "_worker_node_to_candidate",
        lambda _node: {
            "candidate_id": "candidate-before-delete",
            "node_type": "Requirement",
            "title": "Candidate before governed delete",
        },
    )
    monkeypatch.setattr(consolidation, "begin_consolidation", _begin)
    monkeypatch.setattr(consolidation, "propose_reconciliation", _propose)
    monkeypatch.setattr(
        consolidation,
        "_abort_open_consolidation_after_fence",
        _abort,
    )
    monkeypatch.setattr(
        consolidation,
        "_commit_consolidation_with_board_graph_lifecycle",
        _publish,
    )

    with _registered(store):
        with pytest.raises(consolidation._QueueClaimLostOrFenced):
            await _process_queue_entry(object(), entry)

    assert store.load_calls == [("spec", entry.artifact_id)]
    assert observed == ["extract", "abort"]


@pytest.mark.asyncio
@pytest.mark.parametrize("ack_result", [False, True])
async def test_legacy_processed_count_requires_ack_cas_rowcount_one(
    monkeypatch,
    ack_result,
):
    entry = _entry(
        entry_id=f"ack-{ack_result}",
        work_kind="consolidate",
        generation=0,
        delete_event_id=None,
        payload=None,
    )
    store = _MemoryConsolidationStore((entry,), ack_result=ack_result)

    async def _success(*_args, **_kwargs):
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _success)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        processed = await processor.process_batch()

    assert processed == int(ack_result)
    assert len(store.ack_calls) == 1
    ack = store.ack_calls[0]
    assert ack == {
        "entry_id": entry.id,
        "claim_token": entry.claim_token,
        "generation": 0,
        "delete_event_id": None,
    }
    assert (entry.id not in store.entries) is ack_result


@pytest.mark.asyncio
async def test_recovery_clears_token_and_reclaim_uses_fresh_token(monkeypatch):
    old_token = "token-from-crashed-worker"
    entry = _entry(
        status="claimed",
        claim_token=old_token,
    )
    entry.claim_timeout_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    store = _MemoryConsolidationStore((entry,))
    observed_tokens: list[str | None] = []

    async def _lose_after_reclaim(_db, claimed_entry, **_kwargs):
        observed_tokens.append(claimed_entry.claim_token)
        raise consolidation._QueueClaimLostOrFenced("simulated takeover")

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _lose_after_reclaim,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        assert await processor.recover_stale_claims() == 1
        assert entry.claim_token is None
        assert await processor.process_batch() == 0

    assert len(observed_tokens) == 1
    assert observed_tokens[0]
    assert observed_tokens[0] != old_token
    assert store.ack_calls == []
    assert entry.attempts == 0


@pytest.mark.asyncio
async def test_stale_sweep_is_claimable_with_legacy_consolidate(monkeypatch):
    legacy = _entry(entry_id="legacy-consolidate", board_id="legacy-board")
    sweep = _entry(
        entry_id="card8-sweep",
        work_kind="stale_sweep",
        board_id="sweep-board",
        artifact_id="board-sweep",
    )
    store = _MemoryConsolidationStore((legacy, sweep))
    processed_ids: list[str] = []

    async def _success(_db, claimed_entry, **_kwargs):
        processed_ids.append(claimed_entry.id)
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _success)
    processor = ConsolidationProcessor(_scope, batch_size=5)

    with _registered(store):
        assert await processor.process_batch() == 2

    assert processed_ids == [legacy.id, sweep.id]
    assert legacy.id not in store.entries
    assert sweep.id not in store.entries
