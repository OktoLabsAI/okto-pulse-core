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
    ConsolidationClaimScope,
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
    source: str = "state_transition",
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
        source=source,
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


def test_queue_record_preserves_original_positional_work_kind_abi() -> None:
    now = datetime.now(timezone.utc)
    record = ConsolidationQueueRecord(
        "entry-positional",
        "board-positional",
        "spec",
        "spec-positional",
        "pending",
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        now,
        "high",
        "stale_sweep",
    )

    assert record.work_kind == "stale_sweep"
    assert record.source == "state_transition"


class _MemoryConsolidationStore:
    def __init__(
        self,
        entries: tuple[ConsolidationQueueRecord, ...] = (),
        *,
        fence_result: bool = True,
        ack_result: bool = True,
        reservation_sources: tuple[str | None, ...] = (),
    ) -> None:
        self.entries = {entry.id: entry for entry in entries}
        self.fence_result = fence_result
        self.ack_result = ack_result
        self.fence_calls: list[dict[str, Any]] = []
        self.ack_calls: list[dict[str, Any]] = []
        self.load_calls: list[tuple[str, str]] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.reservation_sources = list(reservation_sources)
        self.current_reservation_source: str | None = None
        self.repend_calls: list[dict[str, Any]] = []

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

    async def list_ready_pending_exact(
        self,
        _context,
        *,
        now,
        board_id,
        source,
        work_kind,
    ):
        del now
        return tuple(
            entry
            for entry in self.entries.values()
            if entry.status == "pending"
            and entry.board_id == board_id
            and entry.source == source
            and entry.work_kind == work_kind
        )

    async def list_claimed_exact(
        self,
        _context,
        *,
        board_id,
        source,
        work_kind,
    ):
        return tuple(
            entry
            for entry in self.entries.values()
            if entry.status == "claimed"
            and entry.board_id == board_id
            and entry.source == source
            and entry.work_kind == work_kind
        )

    async def claim_ready_pending_exact(
        self,
        _context,
        *,
        entry_id,
        board_id,
        source,
        work_kind,
        generation,
        now,
        claim_timeout_at,
        worker_id,
        claim_token,
    ):
        entry = self.entries.get(entry_id)
        if not (
            entry is not None
            and entry.status == "pending"
            and entry.board_id == board_id
            and entry.source == source
            and entry.work_kind == work_kind
            and entry.generation == generation
        ):
            return None
        entry.status = "claimed"
        entry.claimed_at = now
        entry.claim_timeout_at = claim_timeout_at
        entry.worker_id = worker_id
        entry.claimed_by_session_id = worker_id
        entry.claim_token = claim_token
        return entry

    async def board_administrative_rebuild_source(self, _context, *, board_id):
        del board_id
        if self.reservation_sources:
            self.current_reservation_source = self.reservation_sources.pop(0)
        return self.current_reservation_source

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

    async def repend_claimed_queue_entry(self, _context, **identity):
        self.repend_calls.append(identity)
        entry = self.entries.get(str(identity["entry_id"]))
        if not (
            entry is not None
            and entry.status == "claimed"
            and entry.claim_token == identity["claim_token"]
            and entry.board_id == identity["board_id"]
            and entry.source == identity["source"]
            and entry.work_kind == identity["work_kind"]
            and entry.generation == identity["generation"]
            and entry.delete_event_id == identity["delete_event_id"]
        ):
            return False
        entry.status = "pending"
        entry.claimed_at = None
        entry.claim_timeout_at = None
        entry.worker_id = None
        entry.claimed_by_session_id = None
        entry.claim_token = None
        return True

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
    assert entry.status == "pending"
    assert entry.claim_token is None
    assert entry.attempts == 0


@pytest.mark.asyncio
async def test_stale_reconcile_claim_cas_runs_after_graph_writer(monkeypatch):
    from okto_pulse.core.kg import canonical_stale_reconciler

    entry = _valid_reconcile_entry(status="claimed", claim_token="claim-order")
    store = _MemoryConsolidationStore((entry,))
    events: list[str] = []

    async def _claim_cas(*_args, **_kwargs):
        events.append("claim-cas")
        return True

    async def _reconcile(_db, **kwargs):
        events.append("reconcile")
        kwargs["before_graph_write"]()
        return SimpleNamespace(
            incomplete=False,
            failed_types=(),
            **_complete_target_contract(),
        )

    async def _durable(**_kwargs):
        events.append("durable")

    lease = SimpleNamespace(
        ensure_owned=lambda **_kwargs: events.append("lease-check"),
        durability_applied=True,
    )

    def _enter(_mutation_ref):
        events.append("graph-writer")
        return lease

    monkeypatch.setattr(store, "queue_claim_is_current_and_unfenced", _claim_cas)
    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _reconcile,
    )
    monkeypatch.setattr(
        consolidation,
        "_ensure_board_graph_durable",
        _durable,
    )

    with _registered(store):
        assert await consolidation._process_stale_reconcile_entry(
            object(),
            entry,
            enter_graph_write=_enter,
        )

    assert events == [
        "graph-writer",
        "claim-cas",
        "reconcile",
        "lease-check",
        "durable",
    ]


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

    @contextmanager
    def _writer(*_args, **_kwargs):
        observed.append("graph-writer")
        yield SimpleNamespace()

    async def _claim_cas(*_args, **_kwargs):
        observed.append("claim-cas")
        return False

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
    monkeypatch.setattr(consolidation, "guarded_board_write", _writer)
    monkeypatch.setattr(
        consolidation,
        "_queue_claim_is_current_and_unfenced",
        _claim_cas,
    )

    with _registered(store):
        with pytest.raises(consolidation._QueueClaimLostOrFenced):
            await _process_queue_entry(object(), entry)

    assert store.load_calls == [("spec", entry.artifact_id)]
    assert observed == ["extract", "graph-writer", "claim-cas", "abort"]


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
        "board_id": entry.board_id,
        "source": entry.source,
        "work_kind": entry.work_kind,
        "generation": 0,
        "delete_event_id": None,
    }
    assert (entry.id not in store.entries) is ack_result


@pytest.mark.asyncio
async def test_live_claim_is_neutrally_repended_when_rebuild_reserves_before_step2(
    monkeypatch,
) -> None:
    rebuild_source = "rebuild:manifest-card4"
    live = _entry(entry_id="live-preclaimed-race")
    rebuild = _entry(
        entry_id="exact-rebuild-row",
        artifact_id="rebuild-spec",
        source=rebuild_source,
    )
    store = _MemoryConsolidationStore(
        (live, rebuild),
        reservation_sources=(
            None,  # Step 1 lists/claims the ordinary row.
            rebuild_source,  # Reservation appears before Step 2.
            rebuild_source,
            rebuild_source,
            rebuild_source,
        ),
    )

    async def _success(*_args, **_kwargs):
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _success)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        assert await processor.process_batch() == 0
        assert live.status == "pending"
        assert live.claim_token is None
        assert live.source == "state_transition"
        assert rebuild.status == "pending"

        assert await processor.process_batch() == 1

    assert rebuild.id not in store.entries
    assert live.id in store.entries
    assert live.status == "pending"
    assert len(store.repend_calls) == 1


def test_under_writer_reservation_probe_admits_only_exact_rebuild_source(
    monkeypatch,
) -> None:
    expires = datetime.now(timezone.utc).timestamp() + 60

    class _Reservation:
        def bind_write_lock_port(self):
            return object()

        def inspect(self, *, board_id):
            assert board_id == "card4-board"
            return SimpleNamespace(
                operation="kg02_rebuild_reservation:manifest-card4",
                expires_at_epoch=expires,
            )

    monkeypatch.setattr(
        consolidation,
        "KGAdministrativeOperationReservation",
        _Reservation,
    )

    exact = _entry(source="rebuild:manifest-card4")
    consolidation._ensure_entry_admitted_by_reservation_under_writer(exact)

    with pytest.raises(
        consolidation._QueueClaimLostOrFenced,
        match="rebuild_reservation_scope_mismatch_under_writer",
    ):
        consolidation._ensure_entry_admitted_by_reservation_under_writer(_entry())


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
async def test_exact_recovery_repends_killed_claim_and_replays_logical_commit_once(
    monkeypatch,
) -> None:
    source = "rebuild:manifest-crash-recovery"
    entry = _entry(
        entry_id="claimed-before-process-kill",
        status="claimed",
        claim_token="dead-process-token",
        source=source,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    logical_graph_commits = {f"spec:{entry.artifact_id}"}

    async def _idempotent_replay(_db, claimed_entry, **_kwargs):
        logical_graph_commits.add(f"spec:{claimed_entry.artifact_id}")
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _idempotent_replay,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        recovered = await processor.recover_exact_claims(
            claim_scope=scope,
            recovery_authority_probe=lambda: True,
        )
        assert recovered == 1
        assert entry.status == "pending"
        assert entry.claim_token is None
        assert entry.attempts == 0

        assert await processor.process_batch(claim_scope=scope) == 1

    assert entry.id not in store.entries
    assert logical_graph_commits == {f"spec:{entry.artifact_id}"}
    assert len(store.repend_calls) == 1
    assert store.repend_calls[0]["claim_token"] == "dead-process-token"


@pytest.mark.asyncio
@pytest.mark.parametrize("authority,reservation", [(False, "exact"), (True, None)])
async def test_exact_recovery_requires_offline_authority_and_matching_reservation(
    authority: bool,
    reservation: str | None,
) -> None:
    source = "rebuild:manifest-crash-recovery"
    entry = _entry(
        entry_id="claimed-fenced",
        status="claimed",
        claim_token="still-owned",
        source=source,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source if reservation == "exact" else reservation
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store), pytest.raises(RuntimeError):
        await processor.recover_exact_claims(
            claim_scope=ConsolidationClaimScope(
                board_id=entry.board_id,
                source=source,
            ),
            recovery_authority_probe=lambda: authority,
        )

    assert entry.status == "claimed"
    assert entry.claim_token == "still-owned"
    assert store.repend_calls == []


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
