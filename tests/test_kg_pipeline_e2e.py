"""End-to-end contract test for the Kanban-KG pipeline (Spec 4, Card 4.1).

Exercises the full chain on a tempdir-scoped board:

    Board → Spec → Cards → commit_consolidation → ConsolidationQueue →
    Kùzu per-board → KuzuNodeRef mirror → GlobalUpdateOutbox drain →
    Global discovery DecisionDigest.

After the commit + one outbox-worker tick the test asserts every layer reports
``healthy=True`` via :mod:`okto_pulse.core.kg.health`. Any regression in one
of the previously-fixed layers (Spec 1 Kùzu close, Spec 2 queue drain, Spec 3
embedder readiness, Spec 4.2/4.3 CLI + seed) fails this one test — which is
exactly the point.

Why marked ``e2e``
------------------
Runs out-of-process primitives, opens Kùzu file handles twice (per-board +
global), and waits for the background outbox worker to tick. It is the slowest
test in the suite — skipped by the fast unit path (``pytest -m "not e2e"``).
"""

from __future__ import annotations

import asyncio
import gc
import os
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest
import pytest_asyncio


pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def e2e_tempdir(monkeypatch):
    """Redirect KG base dir + SQLite DB into a throwaway directory.

    The env vars must be set before any ``okto_pulse.core`` module is imported
    by the test body, which happens inside the fixture (lazy import). The
    teardown calls :func:`close_all_connections` so Windows can rm the
    ``.kuzu`` directories — without it, the global discovery singleton holds
    the lock past the fixture exit.
    """
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_e2e_"))
    db_path = base / "pulse.db"
    kg_path = base / "kg"
    kg_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("KG_BASE_DIR", str(kg_path))
    monkeypatch.setenv("KG_CLEANUP_ENABLED", "false")
    # Force stub embeddings so the test never reaches out to huggingface.co.
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")

    yield base

    # Teardown — the Kùzu per-board + global singletons hold OS-level locks
    # on Windows. Close them before rmtree or the directory cleanup flakes
    # with ``WinError 32``.
    try:
        from okto_pulse.core.kg.schema import close_all_connections

        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


# ---------------------------------------------------------------------------
# The contract test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_pipeline_commits_and_all_layers_report_healthy(e2e_tempdir, monkeypatch):
    """Create a board+spec+cards, commit, drain outbox, assert 5 layers healthy.

    Bound to these acceptance criteria of spec 4aebbbcf:
      - AC-1: pytest -m e2e passes on a clean clone.
      - FR-1: queue / kuzu / node_refs / outbox / global all assert per layer.
      - FR-9: all checks come from core/kg/health.py (single source of truth).
      - TR-3: close_all_connections() is called by the fixture teardown.
    """
    # Lazy imports so the env vars from the fixture take effect before any
    # module reads from `get_settings()`.
    from sqlalchemy import select

    from okto_pulse.core.infra.database import (
        create_database,
        get_session_factory,
        init_db,
    )
    from okto_pulse.core.kg.global_discovery.outbox_worker import OutboxWorker
    from okto_pulse.core.kg.health import (
        check_global,
        check_kuzu,
        check_kuzu_node_refs,
        check_outbox,
        check_queue,
    )
    from okto_pulse.core.kg.interfaces.registry import (
        configure_kg_registry,
        reset_registry_for_tests,
    )
    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
    from okto_pulse.core.kg.schema import bootstrap_board_graph
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        EdgeCandidate,
        KGEdgeType,
        KGNodeType,
        NodeCandidate,
        ProposeReconciliationRequest,
    )
    from okto_pulse.core.models.db import Board, Card, ConsolidationQueue, Spec

    # --- bootstrap DB + registry ---------------------------------------
    db_url = os.environ["DATABASE_URL"]
    create_database(db_url, echo=False)
    await init_db()
    reset_registry_for_tests()
    session_factory = get_session_factory()
    configure_kg_registry(session_factory=session_factory)

    board_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    agent_id = "agent-e2e-pipeline"

    # Pre-bootstrap the per-board Kùzu graph. Creating it here up-front means
    # the first Kùzu open in the pipeline (via `find_similar_for_candidate`
    # during propose_reconciliation or via commit_consolidation) hits an
    # existing path and skips the implicit bootstrap inside
    # `BoardConnection.__init__` — which otherwise races the file lock on
    # Windows if the test runs right after a fresh tempdir creation.
    bootstrap_board_graph(board_id)
    gc.collect()

    # Stub out the similarity search. On a brand-new graph the HNSW index is
    # empty and the primary Kùzu vector query returns no rows, which sends
    # `find_similar_nodes_by_type` into `_fallback_manual_similarity_search`.
    # That path opens a second Kùzu `BoardConnection` on the same ``.kuzu``
    # directory in quick succession and races the Windows file lock — a known
    # issue in the deterministic search fallback, orthogonal to what the
    # health module is trying to assert. Returning ``[]`` forces the
    # reconciliation engine to ADD every candidate, which is exactly what we
    # want for a fresh board.
    import okto_pulse.core.kg.primitives as _primitives_mod
    import okto_pulse.core.kg.search as _search_mod

    monkeypatch.setattr(
        _search_mod, "find_similar_for_candidate", lambda **_: [], raising=True
    )
    monkeypatch.setattr(
        _primitives_mod, "find_similar_for_candidate", lambda **_: [], raising=False
    )

    async with session_factory() as db:
        board = Board(
            id=board_id,
            name="E2E Pipeline Board",
            owner_id="owner-e2e",
        )
        db.add(board)
        spec = Spec(
            id=spec_id,
            board_id=board_id,
            title="E2E Contract Spec",
            description="Describes the full pipeline contract.",
            context="Fixture-only spec for card 4.1.",
            functional_requirements=[
                {"title": "FR-1", "text": "The pipeline must be end-to-end verifiable."}
            ],
            technical_requirements=[
                {"title": "TR-1", "text": "All 5 layers must report healthy after commit."}
            ],
            acceptance_criteria=[
                {"title": "AC-1", "text": "check_queue/kuzu/outbox/global all return healthy=True."}
            ],
            business_rules=[
                {"title": "BR-1", "rule": "No orphaned outbox events."}
            ],
            status="done",
            created_by="owner-e2e",
        )
        db.add(spec)
        # 3 cards of distinct kinds (normal/bug/test) — the current Card model
        # only has a priority/status field, so we differentiate via title; the
        # commit_consolidation pipeline ingests them as Spec-derived nodes.
        for idx, title in enumerate(
            ("E2E Normal Card", "E2E Bug Card", "E2E Test Card")
        ):
            db.add(
                Card(
                    id=str(uuid.uuid4()),
                    board_id=board_id,
                    spec_id=spec_id,
                    title=title,
                    description=f"Card {idx} for pipeline E2E.",
                    position=idx,
                    created_by="owner-e2e",
                )
            )
        # Enqueue the consolidation trigger the worker would normally create
        # on a spec state transition — the test drives commit directly but
        # still exercises the queue row so check_queue has something to see.
        db.add(
            ConsolidationQueue(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec_id,
                status="done",  # already-consumed; the commit below is the "real" write.
                source="e2e_test",
            )
        )
        await db.commit()

    # --- drive the primitives pipeline ---------------------------------
    nodes = [
        NodeCandidate(
            candidate_id=f"e2e_spec_entity_{spec_id[:8]}",
            node_type=KGNodeType.ENTITY,
            title="E2E Spec Entity",
            content="Root spec node for the E2E pipeline test.",
            source_artifact_ref=f"spec:{spec_id}",
            source_confidence=0.95,
        ),
        NodeCandidate(
            candidate_id=f"e2e_decision_{spec_id[:8]}",
            node_type=KGNodeType.DECISION,
            title="E2E Decision",
            content="Chosen to validate digest propagation.",
            source_artifact_ref=f"spec:{spec_id}",
            source_confidence=0.9,
        ),
        NodeCandidate(
            candidate_id=f"e2e_criterion_{spec_id[:8]}",
            node_type=KGNodeType.CRITERION,
            title="E2E Criterion",
            content="All 5 layers report healthy.",
            source_artifact_ref=f"spec:{spec_id}",
            source_confidence=0.85,
        ),
    ]
    edges = [
        EdgeCandidate(
            candidate_id=f"e2e_edge_validates_{spec_id[:8]}",
            edge_type=KGEdgeType.VALIDATES,
            from_candidate_id=nodes[2].candidate_id,
            to_candidate_id=nodes[0].candidate_id,
            confidence=0.85,
        ),
    ]
    begin_req = BeginConsolidationRequest(
        board_id=board_id,
        artifact_type="spec",
        artifact_id=spec_id,
        raw_content="E2E Spec — deterministic fixture payload.",
        deterministic_candidates=nodes,
    )
    begin = await begin_consolidation(begin_req, agent_id=agent_id, db=None)
    for edge in edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(session_id=begin.session_id, candidate=edge),
            agent_id=agent_id,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id,
        db=None,
    )
    async with session_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text="E2E pipeline commit.",
            ),
            agent_id=agent_id,
            db=db,
        )
        assert commit.nodes_added >= 3, commit
        assert commit.edges_added >= 1, commit

    # --- drain the outbox once to mirror into global discovery ---------
    worker = OutboxWorker(session_factory=session_factory, interval_seconds=5)
    # process_once is the test-friendly hook — no asyncio.Task lifecycle needed.
    processed = await worker.process_once()
    assert processed >= 1, f"outbox worker did not process any events (got {processed})"

    # --- assert every layer reports healthy ----------------------------
    async with session_factory() as db:
        queue_h = await check_queue(db, board_id)
        kuzu_h = check_kuzu(board_id)
        refs_h = await check_kuzu_node_refs(db, board_id, kuzu_total=kuzu_h.counts.get("total"))
        outbox_h = await check_outbox(db, board_id)
        global_h = check_global(board_id)

    assert queue_h.healthy, f"queue unhealthy: {queue_h}"
    assert kuzu_h.healthy, f"kuzu unhealthy: {kuzu_h}"
    assert kuzu_h.counts["total"] >= 3, kuzu_h
    assert refs_h.healthy, f"kuzu_node_refs unhealthy: {refs_h}"
    assert outbox_h.healthy, f"outbox unhealthy: {outbox_h}"
    assert outbox_h.counts["pending"] == 0, outbox_h
    assert global_h.healthy, f"global unhealthy: {global_h}"
    assert global_h.counts["digests"] >= 1, global_h
