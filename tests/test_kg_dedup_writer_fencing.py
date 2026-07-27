"""Behavioral fencing proofs for dedup, proposal approval, and unmerge."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import replace
from threading import Event
from time import monotonic

import pytest

from okto_pulse.core.kg.guarded_write import (
    GuardedWriteError,
    guarded_board_write as _real_guarded_board_write,
    revalidate_active_board_write_lease,
)
from okto_pulse.core.kg.interfaces.graph_lifecycle import (
    GraphLifecycleStepResult,
)
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.safe_write_lifecycle import (
    KGSafeWriteLifecycle,
    LockOwnerProbe,
)
from okto_pulse.core.kg.single_writer_lock import LockAcquisition
from okto_pulse.core.kg.write_barrier import (
    BarrierMode,
    get_barrier_mode,
    require_write_token,
    set_barrier_mode,
)
from okto_pulse.core.ports.kg_curation_proposals import CurationProposal
from okto_pulse.core.ports.kg_equivalence_ledger import EquivalenceRecord


BOARD_ID = "board-dedup-fence"


class _WriterLock:
    def __init__(self, trace: list[str], *, contended: bool = False) -> None:
        self.trace = trace
        self.contended = contended
        self.active = False
        self.token = "dedup-owner-token"

    def acquire(self, **_kwargs) -> LockAcquisition:
        self.trace.append("lock_acquire")
        if self.contended:
            return LockAcquisition(
                acquired=False,
                owner_token=None,
                expires_at=None,
                current_owner="other-writer",
            )
        self.active = True
        return LockAcquisition(
            acquired=True,
            owner_token=self.token,
            expires_at=None,
            current_owner=None,
        )

    def is_owner(self, _board_id: str, owner_token: str) -> bool:
        return self.active and owner_token == self.token

    def renew(self, **_kwargs) -> bool:
        return self.active

    def release(self, **_kwargs) -> bool:
        self.trace.append("lock_release")
        self.active = False
        return True


class _RenewalLossLock(_WriterLock):
    def __init__(self, trace: list[str]) -> None:
        super().__init__(trace)
        self.renew_failed = Event()

    def renew(self, **_kwargs) -> bool:
        # Model a lease that expired before heartbeat renewal. Signal only
        # after ownership is no longer valid, so the next statement gate must
        # fail even if it races the heartbeat's poison assignment.
        self.active = False
        self.renew_failed.set()
        return False


class _BlockingRenewLock(_WriterLock):
    def __init__(self, trace: list[str]) -> None:
        super().__init__(trace)
        self.renew_entered = Event()
        self.allow_renew_return = Event()
        self.renew_returned = Event()
        self.release_calls = 0

    def renew(self, **_kwargs) -> bool:
        self.renew_entered.set()
        self.allow_renew_return.wait()
        self.renew_returned.set()
        return True

    def release(self, **kwargs) -> bool:
        self.release_calls += 1
        return super().release(**kwargs)


def _lifecycle(
    writer_lock: _WriterLock,
    trace: list[str],
    *,
    fail_flush: bool = False,
) -> KGSafeWriteLifecycle:
    def _step(_board_id: str, _graph_type: str, step: str):
        trace.append(step)
        return GraphLifecycleStepResult(
            ok=not (fail_flush and step == "flush"),
            detail="injected lifecycle failure" if step == "flush" else None,
        )

    return KGSafeWriteLifecycle(
        step_adapter=_step,
        owner_probe=LockOwnerProbe(is_active_owner=writer_lock.is_owner),
    )


def _install_guard(
    monkeypatch,
    module,
    writer_lock: _WriterLock,
    lifecycle: KGSafeWriteLifecycle,
) -> None:
    def _guard(board_id: str, **kwargs):
        return _real_guarded_board_write(
            board_id,
            **kwargs,
            writer_lock=writer_lock,
            lifecycle=lifecycle,
        )

    monkeypatch.setattr(module, "guarded_board_write", _guard)


@contextmanager
def _strict_barrier():
    previous = get_barrier_mode()
    set_barrier_mode(BarrierMode.STRICT)
    try:
        yield
    finally:
        set_barrier_mode(previous)


class _Scope:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def execute(self, _statement, _params=None):
        require_write_token(BOARD_ID)
        self.trace.append("graph_mutation")
        return GraphStatementResult()


class _ScopeContext:
    def __init__(self, trace: list[str]) -> None:
        self.scope = _Scope(trace)
        self.trace = trace

    async def __aenter__(self):
        return self.scope

    async def __aexit__(self, *_args):
        self.trace.append("transaction_exit")


class _GraphTransaction:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace
        self.begin_calls = 0

    async def begin(self, _board_id: str):
        self.begin_calls += 1
        return _ScopeContext(self.trace)


class _Ledger:
    def __init__(
        self,
        trace: list[str],
        *,
        record: EquivalenceRecord | None = None,
    ) -> None:
        self.trace = trace
        self.records: dict[str, EquivalenceRecord] = {}
        if record is not None:
            self.records[record.record_id] = record
        self.append_calls = 0
        self.revoke_calls = 0

    async def append(self, record: EquivalenceRecord) -> str:
        require_write_token(record.board_id)
        self.trace.append("ledger_append")
        self.append_calls += 1
        self.records.setdefault(record.record_id, record)
        return record.record_id

    async def get(self, record_id: str) -> EquivalenceRecord | None:
        return self.records.get(record_id)

    async def revoke(self, record_id: str, reason: str) -> EquivalenceRecord:
        require_write_token(BOARD_ID)
        self.trace.append("ledger_revoke")
        self.revoke_calls += 1
        record = self.records[record_id]
        revoked = replace(
            record,
            revoked_at="2026-07-25T00:00:00+00:00",
            revoke_reason=reason,
        )
        self.records[record_id] = revoked
        return revoked

    async def active_for_board(self, board_id: str):
        return tuple(
            record
            for record in self.records.values()
            if record.board_id == board_id and record.is_active
        )


class _ProposalStore:
    def __init__(self, proposal: CurationProposal, trace: list[str]) -> None:
        self.proposal = proposal
        self.trace = trace
        self.resolve_calls = 0

    async def get(self, _proposal_id: str):
        return self.proposal

    async def resolve(self, _proposal_id: str, status: str):
        require_write_token(BOARD_ID)
        self.trace.append("proposal_resolve")
        self.resolve_calls += 1
        self.proposal = replace(self.proposal, status=status)
        return self.proposal


def _dedup_group() -> list[dict]:
    return [
        {
            "source_artifact_ref": "spec:dedup",
            "count": 2,
            "members": [
                {
                    "id": "survivor",
                    "created_at": "2026-02-01T00:00:00",
                    "title": "new",
                    "human_curated": False,
                },
                {
                    "id": "duplicate",
                    "created_at": "2026-01-01T00:00:00",
                    "title": "old",
                    "human_curated": False,
                },
            ],
        }
    ]


def _prepare_dedup(
    monkeypatch,
    *,
    trace: list[str],
    contended: bool = False,
    fail_flush: bool = False,
):
    from okto_pulse.core.kg import dedup_migration as dm
    from okto_pulse.core.kg import equivalence_fold

    graph_transaction = _GraphTransaction(trace)
    ledger = _Ledger(trace)
    writer_lock = _WriterLock(trace, contended=contended)
    lifecycle = _lifecycle(writer_lock, trace, fail_flush=fail_flush)

    monkeypatch.setattr(
        dm,
        "get_kg_registry",
        lambda: type(
            "_Registry",
            (),
            {"graph_transaction": graph_transaction},
        )(),
    )
    monkeypatch.setattr(dm, "require_equivalence_ledger", lambda: ledger)
    monkeypatch.setattr(dm, "NODE_TYPES", ("Entity",))
    monkeypatch.setattr(
        dm,
        "_fetch_groups",
        lambda _scope, _node_type: _dedup_group(),
    )
    monkeypatch.setattr(
        dm,
        "_snapshot_group",
        lambda *_args, **_kwargs: {"nodes": [], "edges": []},
    )
    monkeypatch.setattr(
        dm,
        "_tombstone_members",
        lambda *_args, **_kwargs: (
            require_write_token(BOARD_ID),
            trace.append("graph_mutation"),
            1,
        )[-1],
    )
    monkeypatch.setattr(
        equivalence_fold,
        "invalidate_equivalence_fold_cache",
        lambda _board_id: trace.append("cache_invalidate"),
    )
    _install_guard(monkeypatch, dm, writer_lock, lifecycle)
    return dm, graph_transaction, ledger, writer_lock


def test_dedup_strict_barrier_keeps_one_lease_through_finalization(
    monkeypatch,
) -> None:
    trace: list[str] = []
    dm, _graph, ledger, _lock = _prepare_dedup(
        monkeypatch,
        trace=trace,
    )

    with _strict_barrier():
        report = dm.migrate_dedup_entities(BOARD_ID, confirmed=True)

    assert report["nodes_tombstoned"] == 1
    assert ledger.append_calls == 1
    assert trace == [
        "lock_acquire",
        "ledger_append",
        "graph_mutation",
        "transaction_exit",
        "checkpoint",
        "flush",
        "fsync",
        "cache_invalidate",
        "lock_release",
    ]


def test_dedup_lock_contention_causes_zero_mutations(monkeypatch) -> None:
    trace: list[str] = []
    dm, graph, ledger, _lock = _prepare_dedup(
        monkeypatch,
        trace=trace,
        contended=True,
    )

    with pytest.raises(GuardedWriteError) as caught:
        dm.migrate_dedup_entities(BOARD_ID, confirmed=True)

    assert caught.value.code == "lock_contention"
    assert graph.begin_calls == 0
    assert ledger.append_calls == 0
    assert trace == ["lock_acquire"]


@pytest.mark.parametrize(
    "operation_error",
    [RuntimeError("auto-commit failed"), asyncio.CancelledError()],
)
def test_dedup_exception_after_possible_auto_commit_runs_lifecycle(
    monkeypatch,
    operation_error: BaseException,
) -> None:
    trace: list[str] = []
    dm, _graph, ledger, _lock = _prepare_dedup(
        monkeypatch,
        trace=trace,
    )

    def _write_then_fail(*_args, **_kwargs):
        require_write_token(BOARD_ID)
        trace.append("graph_mutation")
        raise operation_error

    monkeypatch.setattr(dm, "_tombstone_members", _write_then_fail)

    with _strict_barrier(), pytest.raises(type(operation_error)):
        dm.migrate_dedup_entities(BOARD_ID, confirmed=True)

    assert ledger.append_calls == 1
    assert trace == [
        "lock_acquire",
        "ledger_append",
        "graph_mutation",
        "transaction_exit",
        "checkpoint",
        "flush",
        "fsync",
        "lock_release",
    ]


def test_dedup_retry_reuses_ledger_append_from_interrupted_attempt(
    monkeypatch,
) -> None:
    trace: list[str] = []
    dm, _graph, ledger, _lock = _prepare_dedup(
        monkeypatch,
        trace=trace,
    )

    monkeypatch.setattr(
        dm,
        "_tombstone_members",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("interrupted before tombstone")
        ),
    )
    with pytest.raises(RuntimeError, match="interrupted before tombstone"):
        dm.migrate_dedup_entities(BOARD_ID, confirmed=True)

    assert ledger.append_calls == 1
    record_id = next(iter(ledger.records))

    monkeypatch.setattr(
        dm,
        "_tombstone_members",
        lambda *_args, **_kwargs: trace.append("graph_mutation") or 1,
    )
    retry = dm.migrate_dedup_entities(BOARD_ID, confirmed=True)

    assert ledger.append_calls == 1
    assert retry["ledger_records_created"] == 0
    assert retry["details"][0]["record_id"] == record_id


def test_proposal_ack_waits_for_lifecycle_and_stays_inside_same_lease(
    monkeypatch,
) -> None:
    trace: list[str] = []
    dm, _graph, _ledger, _lock = _prepare_dedup(
        monkeypatch,
        trace=trace,
    )
    plan = {"operation": "dedup_entities", "groups": []}
    proposal = CurationProposal(
        proposal_id="prop-fenced",
        board_id=BOARD_ID,
        operation="dedup_entities",
        plan=plan,
        proposal_hash=dm.compute_proposal_hash(plan),
    )
    store = _ProposalStore(proposal, trace)
    monkeypatch.setattr(dm, "require_curation_proposal_store", lambda: store)
    monkeypatch.setattr(dm, "_build_canonical_plan", lambda _board_id: plan)

    with _strict_barrier():
        result = dm.approve_dedup_proposal(BOARD_ID, proposal.proposal_id)

    assert result["proposal_status"] == "resolved"
    assert store.resolve_calls == 1
    assert trace.count("lock_acquire") == 1
    assert trace.index("fsync") < trace.index("proposal_resolve")
    assert trace.index("proposal_resolve") < trace.index("lock_release")


def test_proposal_lifecycle_failure_never_acknowledges_success(
    monkeypatch,
) -> None:
    trace: list[str] = []
    dm, _graph, _ledger, _lock = _prepare_dedup(
        monkeypatch,
        trace=trace,
        fail_flush=True,
    )
    plan = {"operation": "dedup_entities", "groups": []}
    proposal = CurationProposal(
        proposal_id="prop-lifecycle-failure",
        board_id=BOARD_ID,
        operation="dedup_entities",
        plan=plan,
        proposal_hash=dm.compute_proposal_hash(plan),
    )
    store = _ProposalStore(proposal, trace)
    monkeypatch.setattr(dm, "require_curation_proposal_store", lambda: store)
    monkeypatch.setattr(dm, "_build_canonical_plan", lambda _board_id: plan)

    with pytest.raises(GuardedWriteError) as caught:
        dm.approve_dedup_proposal(BOARD_ID, proposal.proposal_id)

    assert caught.value.code == "safe_lifecycle_failed"
    assert store.resolve_calls == 0
    assert "proposal_resolve" not in trace
    assert trace[-1] == "lock_release"


def test_proposal_second_scan_failure_keeps_ack_pending(monkeypatch) -> None:
    trace: list[str] = []
    dm, _graph, ledger, _lock = _prepare_dedup(
        monkeypatch,
        trace=trace,
    )
    group = _dedup_group()[0]
    plan = {
        "operation": "dedup_entities",
        "groups": [
            {
                "node_type": "Entity",
                "source_artifact_ref": group["source_artifact_ref"],
                "survivor_id": "survivor",
                "merged_ids": ["duplicate"],
                "edge_counts": {"duplicate": 0},
            }
        ],
    }
    proposal = CurationProposal(
        proposal_id="prop-second-scan-failure",
        board_id=BOARD_ID,
        operation="dedup_entities",
        plan=plan,
        proposal_hash=dm.compute_proposal_hash(plan),
    )
    store = _ProposalStore(proposal, trace)
    monkeypatch.setattr(dm, "require_curation_proposal_store", lambda: store)
    scan_calls = 0

    def _fetch(_scope, _node_type):
        nonlocal scan_calls
        scan_calls += 1
        if scan_calls == 1:
            return [group]
        raise RuntimeError("transient second scan failure")

    monkeypatch.setattr(dm, "_fetch_groups", _fetch)

    with pytest.raises(RuntimeError, match="transient second scan failure"):
        dm.approve_dedup_proposal(BOARD_ID, proposal.proposal_id)

    assert scan_calls == 2
    assert ledger.append_calls == 0
    assert store.resolve_calls == 0
    assert store.proposal.status == "pending"


def test_proposal_retry_finishes_lifecycle_and_ack_after_prior_apply(
    monkeypatch,
) -> None:
    trace: list[str] = []
    dm, _graph, ledger, writer_lock = _prepare_dedup(
        monkeypatch,
        trace=trace,
        fail_flush=True,
    )
    group = _dedup_group()[0]
    plan = {
        "operation": "dedup_entities",
        "groups": [
            {
                "node_type": "Entity",
                "source_artifact_ref": group["source_artifact_ref"],
                "survivor_id": "survivor",
                "merged_ids": ["duplicate"],
                "edge_counts": {"duplicate": 0},
            }
        ],
    }
    proposal = CurationProposal(
        proposal_id="prop-retry-after-apply",
        board_id=BOARD_ID,
        operation="dedup_entities",
        plan=plan,
        proposal_hash=dm.compute_proposal_hash(plan),
    )
    store = _ProposalStore(proposal, trace)
    applied = False

    class _RecoveryScope:
        def execute(self, statement, params=None):
            require_write_token(BOARD_ID)
            node_id = str((params or {}).get("id") or "")
            if "RETURN n.superseded_by, n.revocation_reason" in statement:
                record_id = next(iter(ledger.records))
                return GraphStatementResult.from_rows(
                    [("survivor", f"dedup:{record_id}")]
                    if node_id == "duplicate"
                    else []
                )
            if "RETURN n.superseded_by" in statement:
                return GraphStatementResult.from_rows(
                    [(None,)] if node_id == "survivor" else []
                )
            return GraphStatementResult()

    class _RecoveryContext:
        async def __aenter__(self):
            return _RecoveryScope()

        async def __aexit__(self, *_args):
            return None

    class _RecoveryTransaction:
        async def begin(self, _board_id):
            return _RecoveryContext()

    monkeypatch.setattr(
        dm,
        "get_kg_registry",
        lambda: type(
            "_Registry",
            (),
            {"graph_transaction": _RecoveryTransaction()},
        )(),
    )
    monkeypatch.setattr(dm, "require_curation_proposal_store", lambda: store)
    monkeypatch.setattr(
        dm,
        "_build_canonical_plan",
        lambda _board_id: (
            {"operation": "dedup_entities", "groups": []}
            if applied
            else plan
        ),
    )
    monkeypatch.setattr(
        dm,
        "_fetch_groups",
        lambda _scope, _node_type: [] if applied else [group],
    )

    def _apply(*_args, **_kwargs):
        nonlocal applied
        require_write_token(BOARD_ID)
        trace.append("graph_mutation")
        applied = True
        return 1

    monkeypatch.setattr(dm, "_tombstone_members", _apply)

    with pytest.raises(GuardedWriteError) as first_failure:
        dm.approve_dedup_proposal(BOARD_ID, proposal.proposal_id)
    assert first_failure.value.code == "safe_lifecycle_failed"
    assert applied is True
    assert ledger.append_calls == 1
    assert store.resolve_calls == 0

    _install_guard(
        monkeypatch,
        dm,
        writer_lock,
        _lifecycle(writer_lock, trace),
    )
    retry = dm.approve_dedup_proposal(BOARD_ID, proposal.proposal_id)

    assert retry["already_applied"] is True
    assert retry["proposal_status"] == "resolved"
    assert ledger.append_calls == 1
    assert store.resolve_calls == 1


def test_proposal_edge_count_failure_is_fail_closed(monkeypatch) -> None:
    from okto_pulse.core.kg import dedup_migration as dm

    trace: list[str] = []
    graph_transaction = _GraphTransaction(trace)
    monkeypatch.setattr(
        dm,
        "get_kg_registry",
        lambda: type(
            "_Registry",
            (),
            {"graph_transaction": graph_transaction},
        )(),
    )
    monkeypatch.setattr(dm, "NODE_TYPES", ("Entity",))
    monkeypatch.setattr(
        dm,
        "_fetch_groups",
        lambda _scope, _node_type: _dedup_group(),
    )
    monkeypatch.setattr(
        dm,
        "_count_incident_edges",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("edge count unavailable")
        ),
    )

    class _AppendStore:
        append_calls = 0

        async def append(self, _proposal):
            self.append_calls += 1
            return "unexpected"

    store = _AppendStore()
    monkeypatch.setattr(dm, "require_curation_proposal_store", lambda: store)

    with pytest.raises(RuntimeError, match="edge count unavailable"):
        dm.propose_dedup_entities(BOARD_ID)

    assert store.append_calls == 0


def _active_record() -> EquivalenceRecord:
    return EquivalenceRecord(
        record_id="eqv-fenced",
        board_id=BOARD_ID,
        node_type="Entity",
        survivor_id="survivor",
        merged_ids=("duplicate-a", "duplicate-b"),
        operation="dedup_entities",
    )


def _prepare_unmerge(
    monkeypatch,
    *,
    trace: list[str],
    fail_flush: bool = False,
):
    from okto_pulse.core.kg import dedup_migration as dm
    from okto_pulse.core.kg import equivalence_fold

    graph_transaction = _GraphTransaction(trace)
    ledger = _Ledger(trace, record=_active_record())
    writer_lock = _WriterLock(trace)
    lifecycle = _lifecycle(writer_lock, trace, fail_flush=fail_flush)
    monkeypatch.setattr(
        dm,
        "get_kg_registry",
        lambda: type(
            "_Registry",
            (),
            {"graph_transaction": graph_transaction},
        )(),
    )
    monkeypatch.setattr(dm, "require_equivalence_ledger", lambda: ledger)
    monkeypatch.setattr(
        equivalence_fold,
        "invalidate_equivalence_fold_cache",
        lambda _board_id: trace.append("cache_invalidate"),
    )
    _install_guard(monkeypatch, dm, writer_lock, lifecycle)
    return dm, graph_transaction, ledger


def test_unmerge_is_durable_before_revoke_and_retry_is_idempotent(
    monkeypatch,
) -> None:
    trace: list[str] = []
    dm, graph, ledger = _prepare_unmerge(monkeypatch, trace=trace)

    with _strict_barrier():
        result = dm.unmerge_equivalence(BOARD_ID, "eqv-fenced")
        trace_after_first = list(trace)
        again = dm.unmerge_equivalence(BOARD_ID, "eqv-fenced")

    assert result["revoked"] is True
    assert again["already_revoked"] is True
    assert ledger.revoke_calls == 1
    assert graph.begin_calls == 1
    assert trace_after_first.index("fsync") < trace_after_first.index(
        "ledger_revoke"
    )
    assert trace_after_first.index("ledger_revoke") < trace_after_first.index(
        "lock_release"
    )
    assert trace == trace_after_first


def test_unmerge_lifecycle_failure_does_not_revoke_ledger(monkeypatch) -> None:
    trace: list[str] = []
    dm, _graph, ledger = _prepare_unmerge(
        monkeypatch,
        trace=trace,
        fail_flush=True,
    )

    with pytest.raises(GuardedWriteError) as caught:
        dm.unmerge_equivalence(BOARD_ID, "eqv-fenced")

    assert caught.value.code == "safe_lifecycle_failed"
    assert ledger.revoke_calls == 0
    assert ledger.records["eqv-fenced"].is_active
    assert "ledger_revoke" not in trace
    assert trace[-1] == "lock_release"


def test_statement_hook_blocks_every_write_after_heartbeat_loss() -> None:
    trace: list[str] = []
    writes: list[str] = []
    writer_lock = _RenewalLossLock(trace)

    with pytest.raises(GuardedWriteError) as caught:
        with _real_guarded_board_write(
            BOARD_ID,
            operation="kg.test_statement_fence",
            owner_id="test-agent",
            mutation_ref="renewal-loss",
            ttl_seconds=1,
            renew_interval_seconds=0.01,
            writer_lock=writer_lock,
            lifecycle=_lifecycle(writer_lock, trace),
        ):
            revalidate_active_board_write_lease(BOARD_ID)
            writes.append("before_loss")
            assert writer_lock.renew_failed.wait(timeout=1.0)
            revalidate_active_board_write_lease(BOARD_ID)
            writes.append("after_loss")

    assert caught.value.code == "writer_lease_lost"
    assert writes == ["before_loss"]
    assert trace == ["lock_acquire", "lock_release"]


def test_blocked_heartbeat_shutdown_is_bounded_and_retains_lock() -> None:
    trace: list[str] = []
    writer_lock = _BlockingRenewLock(trace)
    started_at = monotonic()

    try:
        with pytest.raises(GuardedWriteError) as caught:
            with _real_guarded_board_write(
                BOARD_ID,
                operation="kg.test_blocked_heartbeat",
                owner_id="test-agent",
                mutation_ref="blocked-heartbeat",
                ttl_seconds=1,
                renew_interval_seconds=0.01,
                writer_lock=writer_lock,
                lifecycle=_lifecycle(writer_lock, trace),
            ) as lease:
                lease.ensure_durable()
                assert writer_lock.renew_entered.wait(timeout=1.0)

        assert monotonic() - started_at < 0.75
        assert caught.value.code == "writer_heartbeat_shutdown_timeout"
        assert caught.value.details["lock_release_skipped"] is True
        assert writer_lock.release_calls == 0
        assert writer_lock.active is True
        assert trace == ["lock_acquire", "checkpoint", "flush", "fsync"]
        assert revalidate_active_board_write_lease(BOARD_ID) is None
    finally:
        writer_lock.allow_renew_return.set()
        assert writer_lock.renew_returned.wait(timeout=1.0)
        writer_lock.active = False


def test_statement_hook_restores_nested_context_and_fails_board_mismatch() -> None:
    outer_trace: list[str] = []
    inner_trace: list[str] = []
    outer_lock = _WriterLock(outer_trace)
    inner_lock = _WriterLock(inner_trace)

    assert revalidate_active_board_write_lease("board-outer") is None
    with _real_guarded_board_write(
        "board-outer",
        operation="kg.outer",
        owner_id="outer-agent",
        mutation_ref="outer",
        writer_lock=outer_lock,
        lifecycle=_lifecycle(outer_lock, outer_trace),
    ) as outer:
        assert revalidate_active_board_write_lease("board-outer") is outer
        with pytest.raises(GuardedWriteError) as mismatch:
            revalidate_active_board_write_lease("board-other")
        assert mismatch.value.code == "writer_lease_board_mismatch"

        with _real_guarded_board_write(
            "board-inner",
            operation="kg.inner",
            owner_id="inner-agent",
            mutation_ref="inner",
            writer_lock=inner_lock,
            lifecycle=_lifecycle(inner_lock, inner_trace),
        ) as inner:
            assert revalidate_active_board_write_lease("board-inner") is inner
            assert revalidate_active_board_write_lease("board-outer") is outer
            inner.ensure_durable()

        with pytest.raises(GuardedWriteError) as restored_mismatch:
            revalidate_active_board_write_lease("board-inner")
        assert restored_mismatch.value.code == "writer_lease_board_mismatch"
        assert revalidate_active_board_write_lease("board-outer") is outer
        outer.ensure_durable()

    assert revalidate_active_board_write_lease("board-outer") is None
