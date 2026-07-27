"""Deferred consolidation finalization across graph + relational stores."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import primitives
from okto_pulse.core.kg.interfaces.graph_transaction import (
    SpecLineageParentIntent,
)
from okto_pulse.core.kg.schemas import (
    AbortConsolidationRequest,
    AddNodeCandidateRequest,
    CommitConsolidationRequest,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
    SessionStatus,
)


class _SessionStore:
    default_ttl_seconds = 60

    def __init__(self, session) -> None:
        self.session = session
        self.removed = 0

    async def get(self, session_id: str):
        if self.session is not None and self.session.session_id == session_id:
            return self.session
        return None

    async def remove(self, session_id: str) -> None:
        assert self.session is not None
        assert self.session.session_id == session_id
        self.removed += 1
        self.session = None


class _Cache:
    def __init__(self) -> None:
        self.invalidated: list[str] = []

    def invalidate_board(self, board_id: str) -> None:
        self.invalidated.append(board_id)


@pytest.mark.asyncio
async def test_deferred_retry_restages_relational_without_replaying_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="session-deferred-retry",
        board_id="board-deferred-retry",
        artifact_id="artifact-deferred-retry",
        artifact_type="spec",
        agent_id="agent-deferred-retry",
        started_at=datetime.now(timezone.utc),
        status=SessionStatus.OPEN,
        pending_commit=None,
        node_candidates={},
        edge_candidates={},
        reconciliation_hints={},
        content_hash="hash-deferred-retry",
        spec_lineage_parent_intent=SpecLineageParentIntent.PRESERVE,
        committed_graph_node_refs=[],
        lock=asyncio.Lock(),
    )
    session.check_ownership = lambda agent_id: agent_id == session.agent_id
    session.touch = lambda _ttl: None
    store = _SessionStore(session)
    cache = _Cache()
    registry = SimpleNamespace(
        require_session_store=lambda: store,
        require_embedding_provider=lambda: object(),
        require_cache_backend=lambda: cache,
    )
    graph_calls = 0
    compensations = 0
    cognitive_stages = 0
    audit_stages = 0

    async def _graph_io(func, *_args, **_kwargs):
        nonlocal compensations, graph_calls
        if func is primitives._compensate_graph_writes:
            compensations += 1
            return None
        assert func is primitives._do_graph_commit
        graph_calls += 1
        counters = SimpleNamespace(
            nodes_added=1,
            nodes_updated=0,
            nodes_superseded=0,
            edges_added=0,
            nodes_merged=0,
            nodes_noop=0,
            merge_audit_items=[],
        )
        record = SimpleNamespace(
            entity_id="node-deferred-retry",
            entity_type="Decision",
            kind="node",
        )
        return (
            {},
            counters,
            [record],
            datetime.now(timezone.utc),
            {"passed": True},
            [],
        )

    async def _append(*_args, **_kwargs) -> None:
        nonlocal cognitive_stages
        cognitive_stages += 1

    async def _audit(*_args, **_kwargs) -> None:
        nonlocal audit_stages
        audit_stages += 1

    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        primitives,
        "_validate_subtype_declarations",
        lambda _candidates: _async_none(),
    )
    monkeypatch.setattr(
        primitives,
        "_resolve_commit_kg_health_state",
        lambda *_args, **_kwargs: _async_value("healthy"),
    )
    monkeypatch.setattr(primitives, "_run_graph_io", _graph_io)
    monkeypatch.setattr(primitives, "_append_cognitive_source_records", _append)
    monkeypatch.setattr(primitives, "_commit_audit_records", _audit)
    monkeypatch.setattr(
        "okto_pulse.core.kg.write_barrier.require_write_token",
        lambda _board_id: None,
    )

    request = CommitConsolidationRequest(
        session_id=session.session_id,
        summary_text="stable retry payload",
    )
    first = await primitives.commit_consolidation(
        request,
        agent_id=session.agent_id,
        db=object(),
        defer_session_finalization=True,
    )

    assert first.status == SessionStatus.COMMITTED
    assert store.session is session
    assert session.status == SessionStatus.OPEN
    assert session.pending_commit.in_flight is True

    # Models the adapter's failed caller-owned commit: release preserves the
    # snapshot and makes the same session immediately retryable.
    await primitives.release_deferred_consolidation(
        session.session_id,
        agent_id=session.agent_id,
    )
    assert session.pending_commit.in_flight is False

    with pytest.raises(primitives.KGPrimitiveError) as mutation_error:
        await primitives.add_node_candidate(
            AddNodeCandidateRequest(
                session_id=session.session_id,
                candidate=NodeCandidate(
                    candidate_id="late-candidate",
                    node_type=KGNodeType.DECISION,
                    title="Must not mutate a graph-applied retry",
                ),
            ),
            agent_id=session.agent_id,
        )
    assert mutation_error.value.code == "session_commit_pending"
    assert session.node_candidates == {}

    # Model two adapters that passed their optimistic pre-check together: the
    # second caller reaches the primitive with a stale OPEN snapshot, but the
    # lock-protected re-check still rejects a concurrent relational restage.
    real_require_open_session = primitives._require_open_session

    async def _stale_precheck(*_args, **_kwargs):
        return session

    monkeypatch.setattr(primitives, "_require_open_session", _stale_precheck)
    session.pending_commit.in_flight = True
    with pytest.raises(primitives.KGPrimitiveError) as concurrent_error:
        await primitives.commit_consolidation(
            request,
            agent_id=session.agent_id,
            db=object(),
            defer_session_finalization=True,
        )
    assert concurrent_error.value.code == "session_commit_in_progress"
    assert graph_calls == 1
    assert cognitive_stages == 1
    assert audit_stages == 1
    session.pending_commit.in_flight = False
    monkeypatch.setattr(
        primitives,
        "_require_open_session",
        real_require_open_session,
    )

    retried = await primitives.commit_consolidation(
        request,
        agent_id=session.agent_id,
        db=object(),
        defer_session_finalization=True,
    )

    assert retried == first
    assert graph_calls == 1
    assert cognitive_stages == 2
    assert audit_stages == 2

    # A cancelled caller may never reach its adapter cleanup. After the atomic
    # relational restage drains, the primitive therefore compensates the
    # already-applied graph and removes the volatile retry snapshot itself.
    await primitives.release_deferred_consolidation(
        session.session_id,
        agent_id=session.agent_id,
    )
    audit_entered = asyncio.Event()
    allow_audit = asyncio.Event()

    async def _delayed_audit(*_args, **_kwargs) -> None:
        nonlocal audit_stages
        audit_stages += 1
        audit_entered.set()
        await allow_audit.wait()

    monkeypatch.setattr(primitives, "_commit_audit_records", _delayed_audit)
    cancelled_retry = asyncio.create_task(
        primitives.commit_consolidation(
            request,
            agent_id=session.agent_id,
            db=object(),
            defer_session_finalization=True,
        )
    )
    await audit_entered.wait()
    cancelled_retry.cancel()
    allow_audit.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_retry

    assert store.session is None
    assert store.removed == 1
    assert session.status == SessionStatus.ABORTED
    assert compensations == 1
    assert cognitive_stages == 3
    assert audit_stages == 3
    assert cache.invalidated == [session.board_id]


@pytest.mark.asyncio
async def test_finalize_deferred_is_terminal_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = SimpleNamespace(
        entity_id="node-finalize-idempotent",
        entity_type="Decision",
        kind="node",
    )
    session = SimpleNamespace(
        session_id="session-finalize-idempotent",
        board_id="board-finalize-idempotent",
        agent_id="agent-finalize-idempotent",
        status=SessionStatus.OPEN,
        pending_commit=primitives._PendingConsolidationCommit(
            request_payload={},
            records=(record,),
            counters=SimpleNamespace(),
            cognitive_source_records=(),
            response=SimpleNamespace(),
            in_flight=True,
        ),
        committed_graph_node_refs=[],
        lock=asyncio.Lock(),
    )
    session.check_ownership = lambda agent_id: agent_id == session.agent_id
    store = _SessionStore(session)
    cache = _Cache()
    registry = SimpleNamespace(
        require_session_store=lambda: store,
        require_cache_backend=lambda: cache,
    )
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)

    await primitives.finalize_deferred_consolidation(
        session.session_id,
        agent_id=session.agent_id,
    )
    await primitives.finalize_deferred_consolidation(
        session.session_id,
        agent_id=session.agent_id,
    )

    assert store.removed == 1
    assert session.status == SessionStatus.COMMITTED
    assert cache.invalidated == [session.board_id]


@pytest.mark.asyncio
async def test_mutators_recheck_pending_after_waiting_for_session_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="session-mutator-race",
        board_id="board-mutator-race",
        artifact_id="artifact-mutator-race",
        artifact_type="spec",
        agent_id="system:mutator-race",
        status=SessionStatus.OPEN,
        pending_commit=None,
        node_candidates={},
        edge_candidates={},
        reconciliation_hints={},
        count_only_attested=False,
        lock=asyncio.Lock(),
    )
    session.check_ownership = lambda agent_id: agent_id == session.agent_id
    session.touch = lambda _ttl: None
    store = _SessionStore(session)
    registry = SimpleNamespace(require_session_store=lambda: store)
    prechecked = asyncio.Event()

    async def _optimistic_precheck(*_args, **_kwargs):
        prechecked.set()
        return session

    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(primitives, "_require_open_session", _optimistic_precheck)

    await session.lock.acquire()
    add_task = asyncio.create_task(
        primitives.add_node_candidate(
            AddNodeCandidateRequest(
                session_id=session.session_id,
                candidate=NodeCandidate(
                    candidate_id="late-node",
                    node_type=KGNodeType.DECISION,
                    title="Must be rejected after pending commit",
                ),
            ),
            agent_id=session.agent_id,
        )
    )
    await prechecked.wait()
    session.pending_commit = SimpleNamespace(in_flight=True)
    session.lock.release()
    with pytest.raises(primitives.KGPrimitiveError) as add_error:
        await add_task
    assert add_error.value.code == "session_commit_in_progress"
    assert session.node_candidates == {}

    prechecked.clear()
    session.pending_commit = None
    await session.lock.acquire()
    abort_task = asyncio.create_task(
        primitives.abort_consolidation(
            AbortConsolidationRequest(session_id=session.session_id),
            agent_id=session.agent_id,
        )
    )
    await prechecked.wait()
    session.pending_commit = SimpleNamespace(in_flight=True)
    session.lock.release()
    with pytest.raises(primitives.KGPrimitiveError) as abort_error:
        await abort_task
    assert abort_error.value.code == "session_commit_in_progress"
    assert session.status == SessionStatus.OPEN
    assert store.removed == 0


@pytest.mark.asyncio
async def test_proposal_rechecks_pending_after_graph_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = NodeCandidate(
        candidate_id="proposal-race-node",
        node_type=KGNodeType.DECISION,
        title="Proposal race",
    )
    session = SimpleNamespace(
        session_id="session-proposal-race",
        board_id="board-proposal-race",
        artifact_id="artifact-proposal-race",
        artifact_type="spec",
        agent_id="agent-proposal-race",
        content_hash="hash-proposal-race",
        status=SessionStatus.OPEN,
        pending_commit=None,
        node_candidates={candidate.candidate_id: candidate},
        edge_candidates={},
        reconciliation_hints={},
        count_only_attested=False,
        lock=asyncio.Lock(),
    )
    session.check_ownership = lambda agent_id: agent_id == session.agent_id
    session.touch = lambda _ttl: None
    store = _SessionStore(session)
    registry = SimpleNamespace(
        require_session_store=lambda: store,
        require_embedding_provider=lambda: object(),
    )
    search_started = asyncio.Event()
    allow_search = asyncio.Event()

    async def _delayed_search(*_args, **_kwargs):
        search_started.set()
        await allow_search.wait()
        return {}

    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(primitives, "_run_graph_io", _delayed_search)

    proposal_task = asyncio.create_task(
        primitives.propose_reconciliation(
            ProposeReconciliationRequest(session_id=session.session_id),
            agent_id=session.agent_id,
            force_reprocess=True,
        )
    )
    await search_started.wait()
    async with session.lock:
        session.pending_commit = SimpleNamespace(in_flight=True)
    allow_search.set()

    with pytest.raises(primitives.KGPrimitiveError) as proposal_error:
        await proposal_task
    assert proposal_error.value.code == "session_commit_in_progress"
    assert session.reconciliation_hints == {}


async def _async_none() -> None:
    return None


async def _async_value(value):
    return value
