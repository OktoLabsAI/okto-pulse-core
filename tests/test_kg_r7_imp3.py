"""R7 IMP3 — rebuild uses the SAME layer-aware policy as normal commit.

The rebuild/recovery drain (BoardRebuildIngestionAdapter -> ConsolidationQueue
source=rebuild:* -> _process_queue_entry -> _commit_consolidation_with_board_graph_lifecycle
-> commit_consolidation -> _validate_kuzu_connectivity_before_commit + the IMP2
post-commit maintenance hook) inherits the IMP1 guard and IMP2 partition ledger
by construction — there is NO rebuild-only classifier and the deterministic
worker never emits Learning cognition.

These are teeth: they FAIL if rebuild ever diverges from normal commit (bypasses
the guard, gains a Learning-cognition path, closes canonical debt with working
evidence, or skips the maintenance hook).
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.canonical_learning_partition import (
    detect_historical_canonical_learning_debt,
    reconcile_canonical_learning_partition_debt,
    run_canonical_learning_partition_maintenance,
)
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_MIXED_DEFERRED_REASON,
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
)
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    _apply_kuzu_node_create_with_timestamp,
    add_edge_candidate,
    add_node_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    MATURITY_WORKING_IMMATURE,
)
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    AddNodeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    EdgeCandidate,
    KGEdgeType,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)
from okto_pulse.core.application.processors.consolidation import (
    AGENT_ID,
    _commit_consolidation_with_board_graph_lifecycle,
    _process_queue_entry,
    _run_deterministic_worker,
)
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    ConsolidationQueue,
)
from okto_pulse.core.services.canonical_debt_service import (
    OPEN_STATES,
    list_canonical_debt,
    schedule_canonical_debt_retry,
    upsert_canonical_debt,
)

USER_ID = "user-r7-imp3"


# ---------------------------------------------------------------------------
# Setup + seed helpers
# ---------------------------------------------------------------------------


async def _setup_board(db_factory) -> str:
    from kg_schema_testing import bootstrap_board_graph

    board_id = f"r7imp3-{uuid.uuid4().hex[:12]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r7 imp3", owner_id=USER_ID))
            await db.commit()
    return board_id


def _seed_bug(board_id: str, *, graph_layer: str) -> str:
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    bug_id = f"r7i3b_{uuid.uuid4().hex[:12]}"
    maturity = (
        MATURITY_CANONICAL_ELIGIBLE
        if graph_layer == GRAPH_LAYER_CANONICAL
        else MATURITY_WORKING_IMMATURE
    )
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn, sqlite_session=None,
            session_id=f"r7seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, "Bug", bug_id,
            _node_attrs(f"bug:{bug_id}", graph_layer, maturity),
        )
    return bug_id


def _seed_materialized_learning_with_working_bug(board_id: str, source_ref: str) -> str:
    """Pre-existing canonical Learning validating only a WORKING Bug (historical
    partition-integrity violation already on the graph)."""
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    learning_id = f"r7i3l_{uuid.uuid4().hex[:12]}"
    bug_id = f"r7i3b_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn, sqlite_session=None,
            session_id=f"r7seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, "Learning", learning_id,
            _node_attrs(source_ref, GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE),
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, "Bug", bug_id,
            _node_attrs(f"bug:{bug_id}", GRAPH_LAYER_WORKING, MATURITY_WORKING_IMMATURE),
        )
        orch.create_edge(
            edge_type="validates", from_id=learning_id, to_id=bug_id,
            attrs={"confidence": 0.9}, from_type="Learning", to_type="Bug",
        )
    return learning_id


def _node_attrs(source_ref: str, graph_layer: str, maturity_status: str) -> dict:
    return {
        "title": "R7 imp3 seed",
        "content": "",
        "context": "",
        "justification": "",
        "source_artifact_ref": source_ref,
        "created_at": "2026-06-08T00:00:00+00:00",
        "created_by_agent": "test",
        "source_confidence": 1.0,
        "relevance_score": 0.5,
        "query_hits": 0,
        "last_queried_at": None,
        "priority_boost": 0.0,
        "human_curated": False,
        "embedding": [0.0] * 384,
        "graph_layer": graph_layer,
        "maturity_status": maturity_status,
    }


async def _begin_learning(
    board_id, agent_id, db_factory, *, source_ref, candidate_id, validates_bug_ids
):
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id, artifact_type="bug",
                artifact_id=source_ref.split(":")[-1],
                raw_content=f"r7 imp3 {source_ref}",
            ),
            agent_id=agent_id, db=db,
        )
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id=candidate_id, node_type=KGNodeType.LEARNING,
                title="R7 imp3 learning", source_artifact_ref=source_ref,
                source_confidence=0.95,
            ),
        ),
        agent_id=agent_id,
    )
    for idx, bug_id in enumerate(validates_bug_ids):
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=EdgeCandidate(
                    candidate_id=f"{candidate_id}__v{idx}",
                    edge_type=KGEdgeType.VALIDATES,
                    from_candidate_id=candidate_id,
                    to_candidate_id=f"kg:{bug_id}",
                    confidence=0.92, layer="cognitive",
                    rule_id="R7-IMP3.test",
                ),
            ),
            agent_id=agent_id,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id, db=None, force_reprocess=True,
    )
    return begin


def _rebuild_entry(board_id: str) -> SimpleNamespace:
    """Minimal rebuild-marked queue entry shape consumed by the commit wrapper."""
    return SimpleNamespace(
        id=f"rebuild-entry-{uuid.uuid4().hex[:8]}",
        board_id=board_id,
        artifact_type="bug",
        artifact_id=uuid.uuid4().hex[:12],
        source=f"rebuild:{uuid.uuid4().hex[:8]}",
    )


# ---------------------------------------------------------------------------
# Parity — rebuild commit wrapper applies the SAME IMP1 guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rebuild_wrapper_holds_working_only_like_normal_commit(
    board_id, agent_id, db_factory
):
    board_id = await _setup_board(db_factory)
    working_bug = _seed_bug(board_id, graph_layer=GRAPH_LAYER_WORKING)

    # Normal commit path: working-only -> hold.
    src_normal = f"card:bug:{working_bug}:learning:{uuid.uuid4()}"
    begin_n = await _begin_learning(
        board_id, agent_id, db_factory,
        source_ref=src_normal, candidate_id="r7i3_normal", validates_bug_ids=[working_bug],
    )
    with pytest.raises(KGPrimitiveError) as exc_n:
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=begin_n.session_id),
                agent_id=agent_id, db=db,
            )

    # Rebuild commit wrapper commits as the worker identity (AGENT_ID); the
    # session must be owned by it. SAME input -> SAME hold verdict.
    src_rebuild = f"card:bug:{working_bug}:learning:{uuid.uuid4()}"
    begin_r = await _begin_learning(
        board_id, AGENT_ID, db_factory,
        source_ref=src_rebuild, candidate_id="r7i3_rebuild", validates_bug_ids=[working_bug],
    )
    with pytest.raises(KGPrimitiveError) as exc_r:
        async with db_factory() as db:
            await _commit_consolidation_with_board_graph_lifecycle(
                entry=_rebuild_entry(board_id),
                session_id=begin_r.session_id,
                summary_text="rebuild parity",
                db=db,
            )

    n_reason = exc_n.value.details["r7_cognitive_hold_candidate"]["reason_code"]
    r_reason = exc_r.value.details["r7_cognitive_hold_candidate"]["reason_code"]
    assert n_reason == r_reason == CANONICAL_LEARNING_WORKING_ONLY_REASON


@pytest.mark.asyncio
async def test_rebuild_wrapper_accepts_mixed_like_normal_commit(
    board_id, agent_id, db_factory
):
    board_id = await _setup_board(db_factory)
    canon_bug = _seed_bug(board_id, graph_layer=GRAPH_LAYER_CANONICAL)
    work_bug = _seed_bug(board_id, graph_layer=GRAPH_LAYER_WORKING)

    src = f"card:bug:{canon_bug}:learning:{uuid.uuid4()}"
    begin_r = await _begin_learning(
        board_id, AGENT_ID, db_factory,
        source_ref=src, candidate_id="r7i3_mixed",
        validates_bug_ids=[canon_bug, work_bug],
    )
    async with db_factory() as db:
        commit = await _commit_consolidation_with_board_graph_lifecycle(
            entry=_rebuild_entry(board_id),
            session_id=begin_r.session_id,
            summary_text="rebuild mixed",
            db=db,
        )
    # Same verdict as normal commit (IMP1 TS2): accepted, working edge deferred.
    assert commit.connectivity["passed"] is True
    advisories = commit.connectivity.get("advisories", [])
    assert any(
        a.get("reason") == CANONICAL_LEARNING_MIXED_DEFERRED_REASON for a in advisories
    )


# ---------------------------------------------------------------------------
# Deterministic worker has no Learning cognition
# ---------------------------------------------------------------------------


def test_deterministic_worker_emits_no_learning_for_bug_card():
    bug = Card(
        id=f"r7i3card_{uuid.uuid4().hex[:8]}",
        board_id="r7imp3-detworker",
        title="A done bug",
        description="bug description",
        status=CardStatus.DONE,
        card_type=CardType.BUG,
        created_by=USER_ID,
        expected_behavior="expected",
        observed_behavior="observed",
        steps_to_reproduce="step 1",
    )
    entry = SimpleNamespace(
        id="det-entry", board_id="r7imp3-detworker",
        artifact_type="card", artifact_id=bug.id, source="rebuild:det",
    )
    worker_result = _run_deterministic_worker(entry, bug)

    node_types = {str(getattr(n, "node_type", "")) for n in worker_result.nodes}
    # No Learning cognition from the deterministic/rebuild structural path.
    assert not any("Learning" in nt for nt in node_types), node_types
    edge_types = {str(getattr(e, "edge_type", "")) for e in worker_result.edges}
    assert not any("validates" in et for et in edge_types), edge_types


# ---------------------------------------------------------------------------
# IMP2 maintenance hook runs for source=rebuild:* entries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rebuild_entry_triggers_canonical_partition_maintenance(
    board_id, agent_id, db_factory, monkeypatch
):
    board_id = await _setup_board(db_factory)
    # Pre-existing historical violation on the board graph.
    violation_src = f"card:bug:{uuid.uuid4()}:learning:{uuid.uuid4()}"
    _seed_materialized_learning_with_working_bug(board_id, violation_src)

    # A rebuild-marked queue entry for a done bug card (deterministic, commits).
    bug = Card(
        id=f"r7i3rb_{uuid.uuid4().hex[:8]}",
        board_id=board_id,
        title="Rebuild bug",
        description="bug",
        status=CardStatus.DONE,
        card_type=CardType.BUG,
        created_by=USER_ID,
        expected_behavior="expected",
        observed_behavior="observed",
        steps_to_reproduce="step 1",
    )
    entry = ConsolidationQueue(
        board_id=board_id, artifact_type="card", artifact_id=bug.id,
        status="claimed", worker_id="worker-test", source="rebuild:run-imp3",
    )

    seen: dict[str, object] = {}
    real_maint = run_canonical_learning_partition_maintenance

    async def _spy(db, *, board_id, actor_id):
        seen["called_board"] = board_id
        return await real_maint(db, board_id=board_id, actor_id=actor_id)

    # patch the symbol imported lazily inside the hook (module attribute).
    monkeypatch.setattr(
        "okto_pulse.core.kg.canonical_learning_partition."
        "run_canonical_learning_partition_maintenance",
        _spy,
    )

    async with db_factory() as db:
        db.add_all([bug, entry])
        await db.flush()
        ok = await _process_queue_entry(db, entry)
        await db.commit()
    assert ok is True
    assert seen.get("called_board") == board_id

    # The maintenance ran for the rebuild entry and opened the historical debt.
    async with db_factory() as db:
        listed = await list_canonical_debt(db, board_id=board_id)
    assert listed.total >= 1
    assert any(d["source_ref"] == violation_src for d in listed.items)


# ---------------------------------------------------------------------------
# Rebuild never closes CanonicalDebt with working evidence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rebuild_maintenance_does_not_close_debt_with_working_evidence(
    board_id, agent_id, db_factory
):
    board_id = await _setup_board(db_factory)
    violation_src = f"card:bug:{uuid.uuid4()}:learning:{uuid.uuid4()}"
    _seed_materialized_learning_with_working_bug(board_id, violation_src)

    async with db_factory() as db:
        await detect_historical_canonical_learning_debt(
            db, board_id=board_id, actor_id="claude-coder"
        )
        await db.commit()

    # Reconcile while the Learning still only has a working Bug -> no close.
    async with db_factory() as db:
        result = await reconcile_canonical_learning_partition_debt(
            db, board_id=board_id, actor_id="claude-coder"
        )
        await db.commit()
    assert result["committed_count"] == 0

    async with db_factory() as db:
        listed = await list_canonical_debt(db, board_id=board_id)
    assert listed.total == 1
    assert listed.items[0]["canonical_state"] in OPEN_STATES


# ---------------------------------------------------------------------------
# Retry REST stays scheduler-only (preserved from IMP2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_is_scheduler_only(board_id, agent_id, db_factory):
    board_id = await _setup_board(db_factory)
    async with db_factory() as db:
        debt = await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="bug",
            artifact_id="bug-retry", source_ref="bug:bug-retry",
            content_hash="h-retry", target_status="canonical_learning_partition_integrity",
            canonical_state="pending", failure_reason="x",
        )
        await db.commit()
        result = await schedule_canonical_debt_retry(
            db, board_id=board_id, debt_id=debt.id,
            actor_id="claude-coder", kg_health_state="healthy",
        )
    # Schedules only; does NOT consume an attempt or reconcile directly.
    assert result["ok"] is True
    assert result["attempt_consumed"] is False
    assert result["debt"]["canonical_state"] == "retry_scheduled"
