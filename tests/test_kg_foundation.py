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

from kg_registry_testing import configure_real_graph_and_data_test_kg_registry
from okto_pulse.core.kg.embedding import get_embedding_provider
from okto_pulse.core.kg.providers.testing.embedding import TestingStubEmbeddingProvider
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    abort_consolidation,
    add_edge_candidate,
    add_node_candidate,
    begin_consolidation,
    commit_consolidation,
    finalize_deferred_consolidation,
    get_similar_nodes,
    propose_reconciliation,
)
from okto_pulse.core.kg.reconciliation import (
    ExistingNodeSummary,
    reconcile_candidate,
    reconcile_session,
)
from kg_schema_testing import (
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
from okto_pulse.core.application.processors import SessionCleanupProcessor
from sqlalchemy import text
from sqlalchemy_test_models import Board


SYSTEM_KG_WRITER = "system:layer1_worker"


async def _commit_connected_learning_session(
    *,
    board_id: str,
    db_factory,
    artifact_type: str,
    artifact_id: str,
    raw_content: str,
    learning_count: int = 1,
    summary_text: str | None = None,
    learning_title: str = "Connected learning",
    learning_content: str = "",
):
    configure_real_graph_and_data_test_kg_registry(db_factory)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(
                Board(
                    id=board_id,
                    name=f"KG Foundation {board_id}",
                    owner_id=SYSTEM_KG_WRITER,
                )
            )
            await db.commit()
    root = NodeCandidate(
        candidate_id="tech_root",
        node_type=KGNodeType.ENTITY,
        title="Technical root",
        source_artifact_ref="tech_entities.yml",
        source_confidence=1.0,
    )
    learnings = [
        NodeCandidate(
            candidate_id=f"learning_{i}",
            node_type=KGNodeType.LEARNING,
            title=learning_title if learning_count == 1 else f"{learning_title} {i}",
            content=learning_content,
            source_artifact_ref=f"{artifact_type}:{artifact_id}:learning:{i}",
            source_confidence=0.9,
        )
        for i in range(learning_count)
    ]
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                raw_content=raw_content,
                deterministic_candidates=[root, *learnings],
            ),
            agent_id=SYSTEM_KG_WRITER,
            db=db,
        )
    for i in range(learning_count):
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=EdgeCandidate(
                    candidate_id=f"learning_{i}_belongs_root",
                    edge_type=KGEdgeType.BELONGS_TO,
                    from_candidate_id=f"learning_{i}",
                    to_candidate_id="tech_root",
                    confidence=1.0,
                ),
            ),
            agent_id=SYSTEM_KG_WRITER,
        )
    async with db_factory() as db:
        await propose_reconciliation(
            ProposeReconciliationRequest(session_id=begin.session_id),
            agent_id=SYSTEM_KG_WRITER,
            db=db,
        )
    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text=summary_text,
            ),
            agent_id=SYSTEM_KG_WRITER,
            db=db,
            defer_session_finalization=True,
        )
        await db.commit()
        await finalize_deferred_consolidation(
            begin.session_id,
            agent_id=SYSTEM_KG_WRITER,
        )
    return begin, commit


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
            "Decision", "Criterion", "Constraint", "Requirement", "Entity",
            "APIContract", "TestScenario", "Bug", "Learning",
        }

    def test_schema_version(self):
        # Monotonic additive bumps preserve the floor (0.3.7 = implements
        # APIContract->Constraint endpoint pair) — assert known-version membership.
        assert SCHEMA_VERSION in {"0.3.2", "0.3.3", "0.3.4", "0.3.5", "0.3.6", "0.3.7", "0.3.8", "0.3.9", "0.3.10"}

    def test_implements_accepts_requirement_and_constraint_pairs(self):
        from kg_schema_testing import MULTI_REL_TYPES

        rel_pairs = [
            (rel_name, from_type, to_type)
            for rel_name, from_type, to_type in REL_TYPES
        ]
        for rel_name, pairs in MULTI_REL_TYPES:
            rel_pairs.extend(
                (rel_name, from_type, to_type)
                for from_type, to_type in pairs
            )
        assert ("implements", "APIContract", "Requirement") in rel_pairs
        assert ("implements", "APIContract", "Constraint") in rel_pairs

    def test_belongs_to_accepts_assumption_to_entity(self):
        from kg_schema_testing import MULTI_REL_TYPES

        belongs_to = dict(MULTI_REL_TYPES)["belongs_to"]
        assert ("Assumption", "Entity") in belongs_to

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
        with open_board_connection(board_id) as (_db, conn):
            r = conn.execute("CALL SHOW_TABLES() RETURN *")
            tables = {}
            while r.has_next():
                row = r.get_next()
                tables[row[1]] = row[2]
            for nt in NODE_TYPES:
                assert nt in tables, f"Missing node table: {nt}"
                assert tables[nt] == "NODE"
            assert "BoardMeta" in tables

    def test_kuzu_has_all_rel_tables(self, board_id):
        with open_board_connection(board_id) as (_db, conn):
            r = conn.execute("CALL SHOW_TABLES() RETURN *")
            tables = {}
            while r.has_next():
                row = r.get_next()
                tables[row[1]] = row[2]
            for rel_name, _, _ in REL_TYPES:
                assert rel_name in tables, f"Missing rel table: {rel_name}"
                assert tables[rel_name] == "REL"

    def test_board_meta_recorded(self, board_id):
        with open_board_connection(board_id) as (_db, conn):
            r = conn.execute(
                "MATCH (m:BoardMeta {board_id: $b}) RETURN m.schema_version",
                {"b": board_id},
            )
            assert r.has_next()
            assert r.get_next()[0] == SCHEMA_VERSION

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
# Cognitive edge validation
# ============================================================================


class TestCognitiveEdgeValidation:
    @pytest.mark.asyncio
    async def test_add_edge_candidate_rejects_invalid_local_endpoint_pair(
        self, board_id, agent_id, db_factory
    ):
        async with db_factory() as db:
            begin = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-invalid-edge-pair",
                    raw_content="invalid edge pair",
                ),
                agent_id=agent_id,
                db=db,
            )

        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=begin.session_id,
                candidate=NodeCandidate(
                    candidate_id="entity-api",
                    node_type=KGNodeType.ENTITY,
                    title="API",
                    source_confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=begin.session_id,
                candidate=NodeCandidate(
                    candidate_id="req-auth",
                    node_type=KGNodeType.REQUIREMENT,
                    title="Authentication requirement",
                    source_confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )

        with pytest.raises(KGPrimitiveError) as excinfo:
            await add_edge_candidate(
                AddEdgeCandidateRequest(
                    session_id=begin.session_id,
                    candidate=EdgeCandidate(
                        candidate_id="invalid-relates-to",
                        edge_type=KGEdgeType.RELATES_TO,
                        from_candidate_id="entity-api",
                        to_candidate_id="req-auth",
                        confidence=0.9,
                    ),
                ),
                agent_id=agent_id,
            )

        assert excinfo.value.code == "invalid_edge_endpoint_types"
        assert "Decision" in excinfo.value.message
        assert "Alternative" in excinfo.value.message

    @pytest.mark.asyncio
    async def test_add_edge_candidate_accepts_valid_local_endpoint_pair(
        self, board_id, agent_id, db_factory
    ):
        async with db_factory() as db:
            begin = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id="spec-valid-edge-pair",
                    raw_content="valid edge pair",
                ),
                agent_id=agent_id,
                db=db,
            )

        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=begin.session_id,
                candidate=NodeCandidate(
                    candidate_id="decision",
                    node_type=KGNodeType.DECISION,
                    title="Use event-driven integration",
                    source_confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=begin.session_id,
                candidate=NodeCandidate(
                    candidate_id="alternative",
                    node_type=KGNodeType.ALTERNATIVE,
                    title="Use synchronous polling",
                    source_confidence=0.8,
                ),
            ),
            agent_id=agent_id,
        )

        response = await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=EdgeCandidate(
                    candidate_id="decision-to-alternative",
                    edge_type=KGEdgeType.RELATES_TO,
                    from_candidate_id="decision",
                    to_candidate_id="alternative",
                    confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )

        assert response.accepted is True
        assert response.edge_count_in_session == 1


# ============================================================================
# Card 725c6d12: Happy path + SHA256 dedup + Reconciliation ADD
# ============================================================================


class TestHappyPathDedup:
    @pytest.mark.asyncio
    async def test_full_commit_happy_path(self, board_id, agent_id, db_factory, board_handle):
        begin, commit = await _commit_connected_learning_session(
            board_id=board_id,
            db_factory=db_factory,
            artifact_type="spec",
            artifact_id="spec-happy",
            raw_content="happy path content",
            learning_title="Happy learning",
        )
        assert begin.nothing_changed is False
        assert commit.status == "committed"
        assert commit.nodes_added >= 1
        assert commit.edges_added == 1
        assert commit.connectivity["passed"] is True

    @pytest.mark.asyncio
    async def test_sha256_dedup_nothing_changed(self, board_id, agent_id, db_factory, board_handle):
        content = "dedup target content"
        b1, _commit = await _commit_connected_learning_session(
            board_id=board_id,
            db_factory=db_factory,
            artifact_type="spec",
            artifact_id="spec-dedup",
            raw_content=content,
            learning_title="Dedup learning",
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
                agent_id=SYSTEM_KG_WRITER,
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
            graph_node_id="kg:d1",
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
            graph_node_id="kg:d2",
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
            graph_node_id="kg:d3",
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
        await _commit_connected_learning_session(
            board_id=board_id,
            db_factory=db_factory,
            artifact_type="spec",
            artifact_id="spec-hnsw-seed",
            raw_content="hnsw seed",
            learning_title="Use Kuzu for vector search",
            learning_content="Native HNSW in Kuzu",
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
                    node_type=KGNodeType.LEARNING,
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
        assert isinstance(prov, TestingStubEmbeddingProvider)
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
        b, commit = await _commit_connected_learning_session(
            board_id=board_id,
            db_factory=db_factory,
            artifact_type="spec",
            artifact_id="spec-audit",
            raw_content="audit test content",
            summary_text="Audit test summary",
            learning_title="Audit learning",
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
            assert row[4] == SYSTEM_KG_WRITER    # agent_id
            assert row[5] is not None            # started_at
            assert row[6] is not None            # committed_at
            assert row[7] >= 1                   # nodes_added
            assert row[8] >= 0                   # nodes_updated
            assert row[9] == 0                   # nodes_superseded
            # The commit may also attach a deterministic belongs_to
            # provenance backbone when a source root already exists.
            assert row[10] >= 1                  # edges_added
            assert row[10] == commit.edges_added
            assert row[11] == "Audit test summary"  # summary_text
            assert len(row[12]) == 64            # content_hash (sha256 hex)
            assert row[13] == "none"             # undo_status

    @pytest.mark.asyncio
    async def test_kuzu_node_refs_linked_to_audit(self, board_id, agent_id, db_factory, board_handle):
        b, _commit = await _commit_connected_learning_session(
            board_id=board_id,
            db_factory=db_factory,
            artifact_type="spec",
            artifact_id="spec-refs",
            raw_content="refs test",
            learning_count=3,
            learning_title="Audit learning",
        )

        from okto_pulse.core.infra.database import get_engine

        async with get_engine().begin() as conn:
            r = await conn.execute(
                text("SELECT COUNT(*) FROM kuzu_node_refs WHERE session_id = :s"),
                {"s": b.session_id},
            )
            assert r.scalar() >= 3

    @pytest.mark.asyncio
    async def test_outbox_event_created(self, board_id, agent_id, db_factory, board_handle):
        b, _commit = await _commit_connected_learning_session(
            board_id=board_id,
            db_factory=db_factory,
            artifact_type="sprint",
            artifact_id="sprint-outbox",
            raw_content="outbox test",
            learning_title="Outbox learning",
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
        worker = SessionCleanupProcessor()
        expired = await worker.sweep_once()
        assert expired == 2
        assert await mgr.active_count() == 1
