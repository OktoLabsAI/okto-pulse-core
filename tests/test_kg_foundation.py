"""Comprehensive test suite for the KG Foundation Layer (Sprint MVP Fase 0).

Covers all 6 test cards:
- 4a2d6fd7: Bootstrap schema + SQLite migration + Abandon
- 725c6d12: Happy path + SHA256 dedup + Reconciliation ADD
- bc3a99c4: Reconciliation UPDATE/SUPERSEDE/NOOP
- f029108d: TTL expiry + Kuzu failure + invalid candidate
- 3d393277: Ownership + HNSW + Idempotency
- 79eb2e55: Audit row schema completo
"""

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.kg.embedding import StubEmbeddingProvider, get_embedding_provider
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    abort_consolidation,
    add_edge_candidate,
    add_node_candidate,
    begin_consolidation,
    commit_consolidation,
    get_similar_nodes,
    propose_reconciliation,
)
from okto_pulse.core.kg.reconciliation import (
    ExistingNodeSummary,
    reconcile_candidate,
    reconcile_session,
)
from okto_pulse.core.kg.schema import (
    NODE_TYPES,
    REL_TYPES,
    SCHEMA_VERSION,
    VECTOR_INDEX_TYPES,
    bootstrap_board_graph,
    board_kuzu_path,
    open_board_connection,
)
from okto_pulse.core.kg.schemas import (
    AbortConsolidationRequest,
    AddEdgeCandidateRequest,
    AddNodeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    EdgeCandidate,
    GetSimilarNodesRequest,
    KGEdgeType,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
    ReconciliationOperation,
)
from okto_pulse.core.kg.session_manager import get_session_manager
from okto_pulse.core.kg.workers import get_cleanup_worker
from sqlalchemy import text


# ============================================================================
# Card 4a2d6fd7: Bootstrap schema + SQLite migration + Abandon
# ============================================================================


class TestBootstrapSchema:
    def test_node_types_count(self):
        assert len(NODE_TYPES) == 11

    def test_rel_types_count(self):
        assert len(REL_TYPES) == 10

    def test_vector_index_types(self):
        assert set(VECTOR_INDEX_TYPES) == {
            "Decision", "Criterion", "Constraint", "Entity", "Learning",
        }

    def test_schema_version(self):
        assert SCHEMA_VERSION == "0.3.3"

    def test_bootstrap_creates_kuzu_dir(self, board_id):
        handle = bootstrap_board_graph(board_id)
        assert handle.path.exists()
        assert handle.board_id == board_id
        assert handle.schema_version == SCHEMA_VERSION

    def test_bootstrap_idempotent(self, board_id):
        h1 = bootstrap_board_graph(board_id)
        h2 = bootstrap_board_graph(board_id)
        assert h1.path == h2.path

    def test_kuzu_has_all_node_tables(self, board_id):
        db, conn = open_board_connection(board_id)
        try:
            r = conn.execute("CALL SHOW_TABLES() RETURN *")
            tables = {}
            while r.has_next():
                row = r.get_next()
                tables[row[1]] = row[2]
            for nt in NODE_TYPES:
                assert nt in tables, f"Missing node table: {nt}"
                assert tables[nt] == "NODE"
            assert "BoardMeta" in tables
        finally:
            del conn, db

    def test_kuzu_has_all_rel_tables(self, board_id):
        db, conn = open_board_connection(board_id)
        try:
            r = conn.execute("CALL SHOW_TABLES() RETURN *")
            tables = {}
            while r.has_next():
                row = r.get_next()
                tables[row[1]] = row[2]
            for rel_name, _, _ in REL_TYPES:
                assert rel_name in tables, f"Missing rel table: {rel_name}"
                assert tables[rel_name] == "REL"
        finally:
            del conn, db

    def test_board_meta_recorded(self, board_id):
        db, conn = open_board_connection(board_id)
        try:
            r = conn.execute(
                "MATCH (m:BoardMeta {board_id: $b}) RETURN m.schema_version",
                {"b": board_id},
            )
            assert r.has_next()
            assert r.get_next()[0] == SCHEMA_VERSION
        finally:
            del conn, db

    @pytest.mark.asyncio
    async def test_sqlite_tables_exist(self, db_factory):
        async with db_factory() as session:
            conn = await session.connection()
            for table in [
                "consolidation_queue",
                "consolidation_audit",
                "kuzu_node_refs",
                "global_update_outbox",
            ]:
                r = await conn.execute(
                    text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
                )
                assert r.scalar() == table, f"Missing table: {table}"

    @pytest.mark.asyncio
    async def test_abort_removes_session(self, board_id, agent_id, db_factory):
        async with db_factory() as db:
            resp = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-abandon",
                    raw_content="abandon me",
                ),
                agent_id=agent_id,
                db=db,
            )
        resp_abort = await abort_consolidation(
            AbortConsolidationRequest(session_id=resp.session_id, reason="test"),
            agent_id=agent_id,
        )
        assert resp_abort.status == "aborted"
        assert await get_session_manager().get(resp.session_id) is None


# ============================================================================
# Card 725c6d12: Happy path + SHA256 dedup + Reconciliation ADD
# ============================================================================


class TestHappyPathDedup:
    @pytest.mark.asyncio
    async def test_full_commit_happy_path(self, board_id, agent_id, db_factory, board_handle):
        async with db_factory() as db:
            begin = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-happy",
                    raw_content="happy path content",
                ),
                agent_id=agent_id,
                db=db,
            )
        assert begin.nothing_changed is False
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=begin.session_id,
                candidate=NodeCandidate(
                    candidate_id="c1",
                    node_type=KGNodeType.DECISION,
                    title="Happy decision",
                    source_confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )
        async with db_factory() as db:
            commit = await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id=agent_id,
                db=db,
            )
        assert commit.status == "committed"
        assert commit.nodes_added == 1

    @pytest.mark.asyncio
    async def test_sha256_dedup_nothing_changed(self, board_id, agent_id, db_factory, board_handle):
        content = "dedup target content"
        # First commit
        async with db_factory() as db:
            b1 = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-dedup",
                    raw_content=content,
                ),
                agent_id=agent_id,
                db=db,
            )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=b1.session_id,
                candidate=NodeCandidate(
                    candidate_id="d1",
                    node_type=KGNodeType.DECISION,
                    title="Dedup decision",
                    source_confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=b1.session_id),
                agent_id=agent_id,
                db=db,
            )
        # Second begin with same content
        async with db_factory() as db:
            b2 = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-dedup",
                    raw_content=content,
                ),
                agent_id=agent_id,
                db=db,
            )
        assert b2.nothing_changed is True
        assert b2.previous_session_id == b1.session_id

    @pytest.mark.asyncio
    async def test_propose_returns_add_for_new(self, board_id, agent_id, db_factory, board_handle):
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-add-test",
                    raw_content="new content for add",
                ),
                agent_id=agent_id,
                db=db,
            )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=b.session_id,
                candidate=NodeCandidate(
                    candidate_id="new_c",
                    node_type=KGNodeType.LEARNING,
                    title="Totally unique learning",
                    content="Never seen before",
                    source_confidence=0.8,
                ),
            ),
            agent_id=agent_id,
        )
        async with db_factory() as db:
            prop = await propose_reconciliation(
                ProposeReconciliationRequest(session_id=b.session_id),
                agent_id=agent_id,
                db=db,
            )
        assert len(prop.hints) == 1
        op = prop.hints[0].operation
        op_val = op.value if hasattr(op, "value") else op
        assert op_val == "ADD"


# ============================================================================
# Card bc3a99c4: Reconciliation UPDATE/SUPERSEDE/NOOP
# ============================================================================


class TestReconciliationRules:
    def test_noop_when_nothing_changed(self):
        cand = NodeCandidate(
            candidate_id="c1",
            node_type=KGNodeType.DECISION,
            title="X",
            source_confidence=0.9,
        )
        h = reconcile_candidate(cand, nothing_changed=True, existing_matches=[])
        assert h.operation == ReconciliationOperation.NOOP

    def test_add_when_no_matches(self):
        cand = NodeCandidate(
            candidate_id="c1",
            node_type=KGNodeType.DECISION,
            title="X",
            source_confidence=0.9,
        )
        h = reconcile_candidate(cand, nothing_changed=False, existing_matches=[])
        assert h.operation == ReconciliationOperation.ADD

    def test_update_by_high_similarity(self):
        cand = NodeCandidate(
            candidate_id="c1",
            node_type=KGNodeType.DECISION,
            title="X",
            source_confidence=0.9,
        )
        match = ExistingNodeSummary(
            kuzu_node_id="kg:d1",
            node_type="Decision",
            stable_id=None,
            title="X",
            similarity=0.97,
        )
        h = reconcile_candidate(cand, nothing_changed=False, existing_matches=[match])
        assert h.operation == ReconciliationOperation.UPDATE
        assert h.target_node_id == "kg:d1"

    def test_supersede_by_mid_similarity(self):
        cand = NodeCandidate(
            candidate_id="c1",
            node_type=KGNodeType.DECISION,
            title="X",
            source_confidence=0.9,
        )
        match = ExistingNodeSummary(
            kuzu_node_id="kg:d2",
            node_type="Decision",
            stable_id=None,
            title="Y",
            similarity=0.88,
        )
        h = reconcile_candidate(cand, nothing_changed=False, existing_matches=[match])
        assert h.operation == ReconciliationOperation.SUPERSEDE

    def test_update_by_stable_id(self):
        cand = NodeCandidate(
            candidate_id="c1",
            node_type=KGNodeType.DECISION,
            title="X",
            source_artifact_ref="orn:spec:x",
            source_confidence=0.9,
        )
        match = ExistingNodeSummary(
            kuzu_node_id="kg:d3",
            node_type="Decision",
            stable_id="orn:spec:x",
            title="Old",
            similarity=0.1,
        )
        h = reconcile_candidate(cand, nothing_changed=False, existing_matches=[match])
        assert h.operation == ReconciliationOperation.UPDATE
        assert h.target_node_id == "kg:d3"

    def test_reconcile_session_batch(self):
        cands = {
            "a": NodeCandidate(
                candidate_id="a",
                node_type=KGNodeType.DECISION,
                title="A",
                source_confidence=0.9,
            ),
            "b": NodeCandidate(
                candidate_id="b",
                node_type=KGNodeType.CONSTRAINT,
                title="B",
                source_confidence=0.8,
            ),
        }
        hints = reconcile_session(
            cands,
            nothing_changed=True,
            existing_matches_by_candidate={},
        )
        assert all(
            h.operation == ReconciliationOperation.NOOP for h in hints.values()
        )


# ============================================================================
# Card f029108d: TTL expiry + Kuzu failure + invalid candidate
# ============================================================================


class TestErrorCases:
    @pytest.mark.asyncio
    async def test_expired_session_returns_not_found(self, board_id, agent_id, db_factory):
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-expire",
                    raw_content="will expire",
                ),
                agent_id=agent_id,
                db=db,
            )
        # Force the session to expire
        mgr = get_session_manager()
        session = await mgr.get(b.session_id)
        session.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        # Next access should fail
        with pytest.raises(KGPrimitiveError) as exc_info:
            await add_node_candidate(
                AddNodeCandidateRequest(
                    session_id=b.session_id,
                    candidate=NodeCandidate(
                        candidate_id="c_exp",
                        node_type=KGNodeType.DECISION,
                        title="Too late",
                    ),
                ),
                agent_id=agent_id,
            )
        assert exc_info.value.code == "session_not_found"

    @pytest.mark.asyncio
    async def test_duplicate_candidate_id_rejected(self, board_id, agent_id, db_factory):
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-dup",
                    raw_content="dup test",
                ),
                agent_id=agent_id,
                db=db,
            )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=b.session_id,
                candidate=NodeCandidate(
                    candidate_id="c_dup",
                    node_type=KGNodeType.DECISION,
                    title="First",
                ),
            ),
            agent_id=agent_id,
        )
        with pytest.raises(KGPrimitiveError) as exc_info:
            await add_node_candidate(
                AddNodeCandidateRequest(
                    session_id=b.session_id,
                    candidate=NodeCandidate(
                        candidate_id="c_dup",
                        node_type=KGNodeType.DECISION,
                        title="Duplicate",
                    ),
                ),
                agent_id=agent_id,
            )
        assert exc_info.value.code == "duplicate_candidate_id"

    @pytest.mark.asyncio
    async def test_edge_references_unknown_candidate(self, board_id, agent_id, db_factory):
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-edge-bad",
                    raw_content="edge ref test",
                ),
                agent_id=agent_id,
                db=db,
            )
        with pytest.raises(KGPrimitiveError) as exc_info:
            await add_edge_candidate(
                AddEdgeCandidateRequest(
                    session_id=b.session_id,
                    candidate=EdgeCandidate(
                        candidate_id="e_bad",
                        edge_type=KGEdgeType.DEPENDS_ON,
                        from_candidate_id="nonexistent_a",
                        to_candidate_id="nonexistent_b",
                        confidence=0.8,
                    ),
                ),
                agent_id=agent_id,
            )
        assert exc_info.value.code == "invalid_candidate"

    @pytest.mark.asyncio
    async def test_get_similar_unknown_candidate(self, board_id, agent_id, db_factory):
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-sim-bad",
                    raw_content="sim ref test",
                ),
                agent_id=agent_id,
                db=db,
            )
        with pytest.raises(KGPrimitiveError) as exc_info:
            await get_similar_nodes(
                GetSimilarNodesRequest(
                    session_id=b.session_id,
                    candidate_id="ghost",
                ),
                agent_id=agent_id,
            )
        assert exc_info.value.code == "candidate_not_found"


# ============================================================================
# Card 3d393277: Ownership + HNSW + Idempotency
# ============================================================================


class TestOwnershipHNSWIdempotency:
    @pytest.mark.asyncio
    async def test_wrong_agent_cannot_add_candidate(self, board_id, agent_id, db_factory):
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-own",
                    raw_content="ownership test",
                ),
                agent_id=agent_id,
                db=db,
            )
        with pytest.raises(KGPrimitiveError) as exc_info:
            await add_node_candidate(
                AddNodeCandidateRequest(
                    session_id=b.session_id,
                    candidate=NodeCandidate(
                        candidate_id="c_intruder",
                        node_type=KGNodeType.DECISION,
                        title="Intruder",
                    ),
                ),
                agent_id="wrong-agent",
            )
        assert exc_info.value.code == "session_ownership_mismatch"

    @pytest.mark.asyncio
    async def test_hnsw_returns_similar(self, board_id, agent_id, db_factory, board_handle):
        # Seed a decision
        async with db_factory() as db:
            b1 = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-hnsw-seed",
                    raw_content="hnsw seed",
                ),
                agent_id=agent_id,
                db=db,
            )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=b1.session_id,
                candidate=NodeCandidate(
                    candidate_id="seed",
                    node_type=KGNodeType.DECISION,
                    title="Use Kuzu for vector search",
                    content="Native HNSW in Kuzu",
                    source_confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=b1.session_id),
                agent_id=agent_id,
                db=db,
            )

        # New session with identical candidate
        async with db_factory() as db:
            b2 = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-hnsw-query",
                    raw_content="hnsw query different content",
                ),
                agent_id=agent_id,
                db=db,
            )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=b2.session_id,
                candidate=NodeCandidate(
                    candidate_id="query_c",
                    node_type=KGNodeType.DECISION,
                    title="Use Kuzu for vector search",
                    content="Native HNSW in Kuzu",
                    source_confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )
        sim = await get_similar_nodes(
            GetSimilarNodesRequest(
                session_id=b2.session_id,
                candidate_id="query_c",
                top_k=3,
                min_similarity=0.5,
            ),
            agent_id=agent_id,
        )
        assert len(sim.similar) >= 1
        assert sim.similar[0].similarity > 0.95

    def test_embedding_stub_deterministic(self):
        prov = get_embedding_provider()
        assert isinstance(prov, StubEmbeddingProvider)
        v1 = prov.encode("hello world")
        v2 = prov.encode("hello world")
        assert v1 == v2
        assert len(v1) == 384

    def test_board_path_rejects_traversal(self):
        with pytest.raises(ValueError):
            board_kuzu_path("../../etc/passwd")
        with pytest.raises(ValueError):
            board_kuzu_path("")
        with pytest.raises(ValueError):
            board_kuzu_path("a/b")


# ============================================================================
# Card 79eb2e55: Audit row schema completo
# ============================================================================


class TestAuditRowSchema:
    @pytest.mark.asyncio
    async def test_audit_row_has_all_fields(self, board_id, agent_id, db_factory, board_handle):
        # Commit a session and inspect the resulting audit row
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-audit",
                    raw_content="audit test content",
                ),
                agent_id=agent_id,
                db=db,
            )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=b.session_id,
                candidate=NodeCandidate(
                    candidate_id="audit_c",
                    node_type=KGNodeType.ENTITY,
                    title="Audit entity",
                    source_confidence=0.75,
                ),
            ),
            agent_id=agent_id,
        )
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(
                    session_id=b.session_id,
                    summary_text="Audit test summary",
                ),
                agent_id=agent_id,
                db=db,
            )

        from okto_pulse.core.infra.database import get_engine

        async with get_engine().begin() as conn:
            r = await conn.execute(
                text(
                    "SELECT session_id, board_id, artifact_id, artifact_type, "
                    "agent_id, started_at, committed_at, nodes_added, "
                    "nodes_updated, nodes_superseded, edges_added, "
                    "summary_text, content_hash, undo_status "
                    "FROM consolidation_audit WHERE session_id = :s"
                ),
                {"s": b.session_id},
            )
            row = r.fetchone()
            assert row is not None
            assert row[0] == b.session_id       # session_id
            assert row[1] == board_id            # board_id
            assert row[2] == "spec-audit"        # artifact_id
            assert row[3] == "spec"              # artifact_type
            assert row[4] == agent_id            # agent_id
            assert row[5] is not None            # started_at
            assert row[6] is not None            # committed_at
            assert row[7] == 1                   # nodes_added
            assert row[8] == 0                   # nodes_updated
            assert row[9] == 0                   # nodes_superseded
            assert row[10] == 0                  # edges_added
            assert row[11] == "Audit test summary"  # summary_text
            assert len(row[12]) == 64            # content_hash (sha256 hex)
            assert row[13] == "none"             # undo_status

    @pytest.mark.asyncio
    async def test_kuzu_node_refs_linked_to_audit(self, board_id, agent_id, db_factory, board_handle):
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-refs",
                    raw_content="refs test",
                ),
                agent_id=agent_id,
                db=db,
            )
        for i in range(3):
            await add_node_candidate(
                AddNodeCandidateRequest(
                    session_id=b.session_id,
                    candidate=NodeCandidate(
                        candidate_id=f"ref_c{i}",
                        node_type=KGNodeType.LEARNING,
                        title=f"Learning {i}",
                        source_confidence=0.8,
                    ),
                ),
                agent_id=agent_id,
            )
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=b.session_id),
                agent_id=agent_id,
                db=db,
            )

        from okto_pulse.core.infra.database import get_engine

        async with get_engine().begin() as conn:
            r = await conn.execute(
                text("SELECT COUNT(*) FROM kuzu_node_refs WHERE session_id = :s"),
                {"s": b.session_id},
            )
            assert r.scalar() == 3

    @pytest.mark.asyncio
    async def test_outbox_event_created(self, board_id, agent_id, db_factory, board_handle):
        async with db_factory() as db:
            b = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="sprint",
                    artifact_id="sprint-outbox",
                    raw_content="outbox test",
                ),
                agent_id=agent_id,
                db=db,
            )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=b.session_id,
                candidate=NodeCandidate(
                    candidate_id="outbox_c",
                    node_type=KGNodeType.BUG,
                    title="Outbox bug",
                    source_confidence=0.7,
                ),
            ),
            agent_id=agent_id,
        )
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=b.session_id),
                agent_id=agent_id,
                db=db,
            )

        from okto_pulse.core.infra.database import get_engine

        async with get_engine().begin() as conn:
            r = await conn.execute(
                text(
                    "SELECT event_type, processed_at FROM global_update_outbox "
                    "WHERE session_id = :s"
                ),
                {"s": b.session_id},
            )
            row = r.fetchone()
            assert row is not None
            assert row[0] == "consolidation_committed"
            assert row[1] is None  # not yet processed


# ============================================================================
# Bonus: Cleanup worker
# ============================================================================


class TestCleanupWorker:
    @pytest.mark.asyncio
    async def test_sweep_evicts_expired(self):
        mgr = get_session_manager()
        for i in range(3):
            await mgr.create(
                session_id=f"sweep_{i}",
                board_id="bx",
                artifact_id=f"a{i}",
                artifact_type="spec",
                agent_id="ax",
                raw_content=f"c{i}",
                ttl_seconds=60,
            )
        # Expire 2
        (await mgr.get("sweep_0")).expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        (await mgr.get("sweep_1")).expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        worker = get_cleanup_worker()
        expired = await worker.sweep_once()
        assert expired == 2
        assert await mgr.active_count() == 1
