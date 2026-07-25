"""NC-8 — KG Entity dedup on re-consolidation (spec 7f23535f).

Covers the bug where re-consolidating the same artefact (spec/sprint/card)
produced multiple Kuzu Entity nodes for the same `source_artifact_ref`
because the ADD branch of `_do_graph_commit` never consulted the existing
`_lookup_existing_node` helper.

Tests in this module:
- TS1: re-consolidação não duplica Entity node
- TS2: re-consolidação atualiza atributos (com preservação dos históricos)
- TS3: human_curated=true preserva node
- TS8: structured log `kg.consolidation.dedup_reused` emitido
- TS7: tech-entity dedup cross-spec via tech_entities.yml mention path
"""

from __future__ import annotations

import gc
import os
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _restore_conftest_engine(preserve_relational_runtime):
    """FU-2 F4: tests here call create_database() against a throwaway DB,
    which swaps the composed runtime. The shared fixture closes that runtime
    and restores the conftest runtime after each test."""
    yield


@pytest.fixture
def dedup_tempdir(monkeypatch):
    """Throwaway KG base dir + SQLite DB for dedup tests.

    Mirrors the pipeline e2e fixture but stays focused — no global discovery
    drain, no health probes, just per-board Kuzu primitives.
    """
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_nc8_"))
    db_path = base / "pulse.db"
    kg_path = base / "kg"
    kg_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("KG_BASE_DIR", str(kg_path))
    monkeypatch.setenv("KG_CLEANUP_ENABLED", "false")
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()

    async def _healthy(_board_id, _db, scheduler_control=None):
        return {
            "overall_state": "healthy",
            "graph_state": "healthy",
            "discovery_state": "healthy",
            "total_nodes": 1,
        }

    import okto_pulse.core.services.kg_health_service as health_service

    monkeypatch.setattr(health_service, "get_kg_health", _healthy)

    yield base

    try:
        from kg_schema_testing import close_all_connections

        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


async def _drive_one_session(
    session_factory,
    board_id: str,
    artifact_ref: str,
    title: str,
    content: str = "",
    agent_id: str = "system:layer1_worker",
    *,
    force_add: bool = False,
    node_type: str = "Entity",
    source_confidence: float = 0.95,
):
    """Run one begin -> propose -> commit cycle for one graph candidate.

    Returns the CommitConsolidationResponse so callers can assert on
    `nodes_added` and the kuzu node id mappings.

    The zero-orphan guard requires structured Entity nodes to be attached to a
    source/root entity. NC-8 is a deterministic dedup test, so the fixture uses
    the allowlisted technical root and adds an explicit belongs_to edge instead
    of creating a standalone Entity.
    """
    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        EdgeCandidate,
        KGEdgeType,
        KGNodeType,
        NodeCandidate,
        ProposeReconciliationRequest,
        ReconciliationHint,
        ReconciliationOperation,
    )

    root_cand = NodeCandidate(
        candidate_id="nc8_technical_root",
        node_type=KGNodeType.ENTITY,
        title="NC-8 technical root",
        content="Allowlisted deterministic source root for NC-8 tests.",
        source_artifact_ref="tech_entities.yml",
        source_confidence=1.0,
    )
    cand = NodeCandidate(
        candidate_id=f"nc8_entity_{uuid.uuid4().hex[:8]}",
        node_type=node_type,
        title=title,
        content=content,
        source_artifact_ref=artifact_ref,
        source_confidence=source_confidence,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=artifact_ref.split(":", 1)[1],
            raw_content=f"NC-8 dedup test — {title} :: {content}",
            deterministic_candidates=[root_cand, cand],
        ),
        agent_id=agent_id,
        db=None,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id=f"edge_{cand.candidate_id}_belongs_to_root",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id=cand.candidate_id,
                to_candidate_id=root_cand.candidate_id,
                confidence=1.0,
            ),
        ),
        agent_id=agent_id,
    )
    if str(node_type) == "Decision":
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=EdgeCandidate(
                    candidate_id=f"edge_{cand.candidate_id}_mentions_root",
                    edge_type=KGEdgeType.MENTIONS,
                    from_candidate_id=cand.candidate_id,
                    to_candidate_id=root_cand.candidate_id,
                    confidence=1.0,
                ),
            ),
            agent_id=agent_id,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id,
        db=None,
    )
    overrides = (
        {
            cand.candidate_id: ReconciliationHint(
                candidate_id=cand.candidate_id,
                operation=ReconciliationOperation.ADD,
                confidence=1.0,
                reason="test forces the NC-8 ADD/replay path",
            )
        }
        if force_add
        else {}
    )
    async with session_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text=f"NC-8 commit — {title}",
                agent_overrides=overrides,
            ),
            agent_id=agent_id,
            db=db,
        )
    return commit


def _kgdet_spec_payload(spec_id: str) -> dict:
    return {
        "id": spec_id,
        "title": "KGDET deterministic import fixture",
        "description": "Multi-item fixture for deterministic KG ingestion.",
        "context": "No cognitive closeout is executed for this fixture.",
        "functional_requirements": [
            {"id": "fr_decisions", "text": "Import active formal decisions."},
            {"id": "fr_siblings", "text": "Preserve repeatable spec siblings."},
        ],
        "technical_requirements": [
            {"id": "tr_adapter", "text": "Adapter passes structured fields."},
            {"id": "tr_worker", "text": "Worker emits granular provenance."},
        ],
        "acceptance_criteria": [
            {"id": "ac_decisions", "text": "Decision count equals source count."},
            {"id": "ac_idempotent", "text": "Reprocessing keeps stable counts."},
        ],
        "business_rules": [
            {
                "id": "br_no_cognitive_repair",
                "title": "No cognitive repair",
                "rule": "Deterministic structured data must be imported first.",
            },
            {
                "id": "br_granular_refs",
                "title": "Granular refs",
                "rule": "Repeatable children cannot share the root spec source ref.",
            },
        ],
        "test_scenarios": [
            {
                "id": "ts_decisions",
                "title": "Decision import",
                "given": "A spec has active decisions",
                "when": "The deterministic worker runs",
                "then": "Decision nodes are produced",
                "linked_criteria": ["Decision count equals source count."],
            },
            {
                "id": "ts_reprocess",
                "title": "Reprocess import",
                "given": "The same spec was already committed",
                "when": "It is processed again",
                "then": "Counts remain stable",
                "linked_criteria": ["Reprocessing keeps stable counts."],
            },
        ],
        "api_contracts": [
            {
                "id": "api_adapter",
                "method": "COMPONENT",
                "path": "_spec_to_dict",
                "description": "Serialize structured decisions.",
                "linked_requirements": ["Import active formal decisions."],
            },
            {
                "id": "api_worker",
                "method": "COMPONENT",
                "path": "DeterministicWorker.process_spec",
                "description": "Emit granular source refs.",
                "linked_requirements": ["Preserve repeatable spec siblings."],
            },
        ],
        "decisions": [
            {
                "id": "dec_granular_ref",
                "title": "Use granular source refs",
                "rationale": "Commit dedup uses source_artifact_ref as natural identity.",
                "status": "active",
                "linked_requirements": ["1"],
            },
            {
                "id": "dec_no_cognitive_closeout",
                "title": "Validate without cognitive closeout",
                "rationale": "Layer 1 must import structured data independently.",
                "status": "active",
                "linked_requirements": ["0"],
            },
        ],
    }


async def _drive_spec_worker_session(
    session_factory,
    board_id: str,
    spec_payload: dict,
    agent_id: str = "system:historical_consolidation",
):
    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
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
    from okto_pulse.core.application.processors.consolidation import (
        _worker_edge_to_candidate,
        _worker_node_to_candidate,
    )
    from okto_pulse.core.application.processors.deterministic_kg import (
        DeterministicWorker,
    )

    worker_result = DeterministicWorker().process_spec(spec_payload)
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=spec_payload["id"],
            raw_content=worker_result.raw_content,
            deterministic_candidates=[
                NodeCandidate(
                    candidate_id="kgdet_technical_root",
                    node_type=KGNodeType.ENTITY,
                    title="KGDET technical root",
                    content="Allowlisted deterministic source root for KGDET tests.",
                    source_artifact_ref="tech_entities.yml",
                    source_confidence=1.0,
                ),
                *[_worker_node_to_candidate(node) for node in worker_result.nodes],
            ],
        ),
        agent_id=agent_id,
        db=None,
    )
    for edge in worker_result.edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=_worker_edge_to_candidate(edge),
            ),
            agent_id=agent_id,
        )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="kgdet_spec_belongs_to_technical_root",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id=f"spec_{spec_payload['id'][:8]}_entity",
                to_candidate_id="kgdet_technical_root",
                confidence=1.0,
                layer="deterministic",
                rule_id="belongs_to/kgdet_fixture@v1",
                created_by="worker_layer1",
            ),
        ),
        agent_id=agent_id,
    )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id,
        db=None,
    )
    async with session_factory() as db:
        return await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text="KGDET deterministic reprocess fixture",
            ),
            agent_id=agent_id,
            db=db,
        )


async def _bootstrap_test_board(monkeypatch):
    """Common setup: create DB schema + board, stub similarity search, return ids."""
    from okto_pulse.core.infra.database import (
        create_database,
        get_session_factory,
        init_db,
    )
    from okto_pulse.core.kg.interfaces.registry import (
        reset_registry_for_tests,
    )
    from kg_registry_testing import configure_real_graph_test_kg_registry
    from kg_schema_testing import bootstrap_board_graph

    db_url = os.environ["DATABASE_URL"]
    create_database(db_url, echo=False)
    await init_db()
    reset_registry_for_tests()
    session_factory = get_session_factory()
    configure_real_graph_test_kg_registry(session_factory=session_factory)

    board_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    from sqlalchemy_test_models import Board

    async with session_factory() as db:
        db.add(
            Board(
                id=board_id,
                name=f"KG test board {board_id}",
                owner_id="kg-test",
            )
        )
        await db.commit()
    await run_blocking_graph_io(
        lambda: bootstrap_board_graph(board_id),
        task_name="tests.dedup_nc8.bootstrap_board_graph",
    )
    gc.collect()

    # Same trick as the e2e test — fresh boards have no HNSW data, the
    # fallback similarity search races Windows file locks. Force ADD.
    import okto_pulse.core.kg.primitives as _prim
    import okto_pulse.core.kg.search as _search

    monkeypatch.setattr(
        _search, "find_similar_for_candidate", lambda **_: [], raising=True
    )
    monkeypatch.setattr(
        _prim, "find_similar_for_candidate", lambda **_: [], raising=False
    )

    return session_factory, board_id, spec_id


def _count_entities(board_id: str, source_artifact_ref: str) -> int:
    """Direct Kuzu count of Entity nodes by source_artifact_ref."""
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.source_artifact_ref = $r RETURN count(n)",
            {"r": source_artifact_ref},
        )
        try:
            return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass


def _count_nodes_by_source_prefix(
    board_id: str,
    node_type: str,
    source_prefix: str,
) -> int:
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            f"MATCH (n:{node_type}) WHERE n.source_artifact_ref STARTS WITH $r "
            "RETURN count(n)",
            {"r": source_prefix},
        )
        try:
            return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass


def _count_nodes(board_id: str, node_type: str) -> int:
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(f"MATCH (n:{node_type}) RETURN count(n)")
        try:
            return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass


def _query_one(board_id: str, source_artifact_ref: str):
    """Return the first Entity and its provenance anchor for a given ref."""
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.source_artifact_ref = $r "
            "RETURN n.id, n.title, n.content, n.created_at, "
            "n.source_content_hash, n.attestation_count LIMIT 1",
            {"r": source_artifact_ref},
        )
        try:
            row = res.get_next()
            return {
                "id": row[0],
                "title": row[1],
                "content": row[2],
                "created_at": row[3],
                "source_content_hash": row[4],
                "attestation_count": row[5],
            }
        finally:
            try:
                res.close()
            except Exception:
                pass


async def _count_entities_async(board_id: str, source_artifact_ref: str) -> int:
    return await run_blocking_graph_io(
        lambda: _count_entities(board_id, source_artifact_ref),
        task_name="tests.dedup_nc8.count_entities",
    )


async def _count_nodes_by_source_prefix_async(
    board_id: str,
    node_type: str,
    source_prefix: str,
) -> int:
    return await run_blocking_graph_io(
        lambda: _count_nodes_by_source_prefix(board_id, node_type, source_prefix),
        task_name="tests.dedup_nc8.count_nodes_by_source_prefix",
    )


async def _count_nodes_async(board_id: str, node_type: str) -> int:
    return await run_blocking_graph_io(
        lambda: _count_nodes(board_id, node_type),
        task_name="tests.dedup_nc8.count_nodes",
    )


async def _query_one_async(board_id: str, source_artifact_ref: str):
    return await run_blocking_graph_io(
        lambda: _query_one(board_id, source_artifact_ref),
        task_name="tests.dedup_nc8.query_one",
    )


def _query_decision_lineage(board_id: str, source_artifact_ref: str) -> dict:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_kdb, kconn):
        nodes_result = kconn.execute(
            "MATCH (n:Decision) WHERE n.source_artifact_ref = $ref "
            "RETURN n.id, n.title, n.content, coalesce(n.generation, 0), "
            "n.superseded_by ORDER BY coalesce(n.generation, 0)",
            {"ref": source_artifact_ref},
        )
        nodes = []
        try:
            while nodes_result.has_next():
                nodes.append(nodes_result.get_next())
        finally:
            nodes_result.close()

        edge_result = kconn.execute(
            "MATCH (new:Decision)-[r:supersedes]->(old:Decision) "
            "WHERE new.source_artifact_ref = $ref "
            "AND old.source_artifact_ref = $ref RETURN count(r)",
            {"ref": source_artifact_ref},
        )
        try:
            supersedes_edges = int(edge_result.get_next()[0])
        finally:
            edge_result.close()

    return {"nodes": nodes, "supersedes_edges": supersedes_edges}


async def _query_decision_lineage_async(
    board_id: str,
    source_artifact_ref: str,
) -> dict:
    return await run_blocking_graph_io(
        lambda: _query_decision_lineage(board_id, source_artifact_ref),
        task_name="tests.dedup_nc8.query_decision_lineage",
    )


def _set_human_curated_sync(
    board_id: str,
    node_id: str,
    node_type: str = "Entity",
) -> None:
    from kg_schema_testing import open_board_connection

    if node_type not in {"Entity", "Decision"}:
        raise ValueError(f"unsupported test node type: {node_type}")
    with open_board_connection(board_id) as (_kdb, kconn):
        kconn.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id SET n.human_curated = true",
            {"id": node_id},
        )


def _count_named_entities_sync(board_id: str, title: str) -> int:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.title = $t RETURN count(n)",
            {"t": title},
        )
        try:
            return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# TC-3 (spec eca49df9, AC6 + AC7) — merge contado/auditado + soma de counters
# ---------------------------------------------------------------------------


async def test_tc3_reconsolidation_counts_and_audits_merge(dedup_tempdir, monkeypatch):
    """AC6: NC-8 dedup-reuse on re-consolidation increments nodes_merged and
    emits a merge audit item in the response (no silent drop, no KuzuWriteRecord).
    AC7: the counters sum closes as processed_candidates. Structural Entity
    dedup stays intact (AC8 invariant)."""
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    commit1 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[TC-3] titulo original", content="v1"
    )
    assert commit1.nodes_added >= 1
    assert commit1.nodes_merged == 0  # first commit is a create, not a merge

    # Re-consolidate the SAME ref with changed CONTENT (same title) ->
    # NC-8 dedup-reuse. A changed TITLE would now supersede-with-trail
    # instead (spec MKG-D-S1 FR8), covered by test_kg_nc8_supersede_trail.
    commit2 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[TC-3] titulo original", content="v2"
    )

    # AC6: merge counted + audited, not a silent nodes_updated:0/superseded:0.
    assert commit2.nodes_merged >= 1
    assert commit2.nodes_added == 0
    artifact_merge_items = [
        item
        for item in commit2.merge_audit_items
        if item["source_artifact_ref"] == artifact_ref
    ]
    assert len(artifact_merge_items) == 1
    item = artifact_merge_items[0]
    assert item["operation"] == "MERGE"
    assert item["source_artifact_ref"] == artifact_ref
    assert item["node_type"] == "Entity"
    assert item["reused_node_id"]  # the prior node's id was reused, not re-created

    # AC7: the sum closes.
    assert commit2.processed_candidates == (
        commit2.nodes_added
        + commit2.nodes_updated
        + commit2.nodes_merged
        + commit2.nodes_superseded
        + commit2.nodes_noop
    )
    assert commit2.processed_candidates >= 1

    # AC8 invariant: structural Entity dedup intact — still exactly 1 node.
    assert await _count_entities_async(board_id, artifact_ref) == 1


async def test_decision_same_title_semantic_change_creates_supersedence_trail(
    dedup_tempdir,
    monkeypatch,
):
    """A source ref is lineage identity, never permission to rewrite history."""
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    decision_ref = f"spec:{spec_id}:decision:dec_1"

    first = await _drive_one_session(
        session_factory,
        board_id,
        decision_ref,
        "Choose the event broker",
        content="Kafka is the approved broker.",
        node_type="Decision",
    )
    second = await _drive_one_session(
        session_factory,
        board_id,
        decision_ref,
        "Choose the event broker",
        content="RabbitMQ replaces Kafka as the approved broker.",
        node_type="Decision",
    )

    assert first.nodes_added >= 1
    assert second.nodes_superseded == 1

    lineage = await _query_decision_lineage_async(board_id, decision_ref)
    assert len(lineage["nodes"]) == 2
    assert [row[3] for row in lineage["nodes"]] == [0, 1]
    assert lineage["nodes"][0][4] == lineage["nodes"][1][0]
    assert lineage["nodes"][0][2] == "Kafka is the approved broker."
    assert lineage["nodes"][1][2] == ("RabbitMQ replaces Kafka as the approved broker.")
    assert lineage["supersedes_edges"] == 1


# ---------------------------------------------------------------------------
# TC-3 (spec eca49df9, FR6 + TR6) — op==UPDATE usa update_node (nodes_updated)
# ---------------------------------------------------------------------------


async def test_tc3_update_op_increments_nodes_updated_not_merged(
    dedup_tempdir, monkeypatch
):
    """FR6/TR6 regression guard: a non-curated op==UPDATE candidate with a
    target_node_id routes through orch.update_node, so nodes_updated == 1 and
    nodes_merged == 0 (UPDATE is not miscounted as MERGE), and the counters sum
    closes. Fails before IMPL-3 rework (no update_node call site)."""
    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        EdgeCandidate,
        KGEdgeType,
        KGNodeType,
        NodeCandidate,
        ProposeReconciliationRequest,
        ReconciliationHint,
        ReconciliationOperation,
    )

    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    # Session 1: create the target node.
    commit1 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[TC-3 UPDATE] original", content="v1"
    )
    assert commit1.nodes_added >= 1
    original = await _query_one_async(board_id, artifact_ref)
    existing_id = original["id"]

    # Session 2: force op==UPDATE on the candidate via agent_overrides.
    cand_id = "nc8_update_cand"
    root_cand = NodeCandidate(
        candidate_id="nc8_update_technical_root",
        node_type=KGNodeType.ENTITY,
        title="NC-8 UPDATE technical root",
        content="Allowlisted deterministic source root for forced UPDATE test.",
        source_artifact_ref="tech_entities.yml",
        source_confidence=1.0,
    )
    cand = NodeCandidate(
        candidate_id=cand_id,
        node_type=KGNodeType.ENTITY,
        title="[TC-3 UPDATE] revised",
        content="v2",
        source_artifact_ref=artifact_ref,
        source_confidence=0.95,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=spec_id,
            raw_content="forced UPDATE content v2",
            deterministic_candidates=[root_cand, cand],
        ),
        agent_id="system:layer1_worker",
        db=None,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="edge_nc8_update_belongs_to_root",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id=cand_id,
                to_candidate_id=root_cand.candidate_id,
                confidence=1.0,
            ),
        ),
        agent_id="system:layer1_worker",
    )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id="system:layer1_worker",
        db=None,
    )
    override = ReconciliationHint(
        candidate_id=cand_id,
        operation=ReconciliationOperation.UPDATE,
        target_node_id=existing_id,
        confidence=0.9,  # < 1.0 -> not an authorship override; node is not curated
        reason="test forces in-place UPDATE",
    )
    async with session_factory() as db:
        commit2 = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text="forced update",
                agent_overrides={cand_id: override},
            ),
            agent_id="system:layer1_worker",
            db=db,
        )

    assert commit2.nodes_updated == 1  # in-place UPDATE via update_node
    artifact_merge_items = [
        item
        for item in commit2.merge_audit_items
        if item["source_artifact_ref"] == artifact_ref
    ]
    assert artifact_merge_items == []  # target UPDATE was NOT miscounted as MERGE
    assert commit2.nodes_added == 0  # NOT a CREATE
    assert commit2.processed_candidates == (
        commit2.nodes_added
        + commit2.nodes_updated
        + commit2.nodes_merged
        + commit2.nodes_superseded
        + commit2.nodes_noop
    )
    # In-place: still exactly one Entity, with the updated title.
    assert await _count_entities_async(board_id, artifact_ref) == 1
    updated = await _query_one_async(board_id, artifact_ref)
    assert updated["title"] == "[TC-3 UPDATE] revised"
    assert updated["source_content_hash"] == begin.content_hash
    assert updated["source_content_hash"] != original["source_content_hash"]
    assert updated["attestation_count"] == original["attestation_count"] + 1


async def test_cross_artifact_update_preserves_source_provenance(
    dedup_tempdir, monkeypatch
):
    """A child session may reconcile a parent reference, but its content hash
    must not replace the parent artifact's provenance anchor."""
    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        EdgeCandidate,
        KGEdgeType,
        KGNodeType,
        NodeCandidate,
        ProposeReconciliationRequest,
        ReconciliationHint,
        ReconciliationOperation,
    )

    session_factory, board_id, parent_spec_id = await _bootstrap_test_board(monkeypatch)
    parent_ref = f"spec:{parent_spec_id}"
    await _drive_one_session(
        session_factory,
        board_id,
        parent_ref,
        "[cross-artifact] parent",
        content="parent v1",
    )
    original = await _query_one_async(board_id, parent_ref)

    child_card_id = str(uuid.uuid4())
    candidate_id = "cross_artifact_parent_ref"
    root_candidate = NodeCandidate(
        candidate_id="cross_artifact_technical_root",
        node_type=KGNodeType.ENTITY,
        title="Cross-artifact technical root",
        content="Allowlisted deterministic source root.",
        source_artifact_ref="tech_entities.yml",
        source_confidence=1.0,
    )
    parent_candidate = NodeCandidate(
        candidate_id=candidate_id,
        node_type=KGNodeType.ENTITY,
        title="[cross-artifact] parent reference",
        content="Parent context repeated by the child card.",
        source_artifact_ref=parent_ref,
        source_confidence=0.95,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="card",
            artifact_id=child_card_id,
            raw_content="Child card content must not become the parent hash.",
            deterministic_candidates=[root_candidate, parent_candidate],
        ),
        agent_id="system:layer1_worker",
        db=None,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="edge_cross_artifact_parent_to_root",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id=candidate_id,
                to_candidate_id=root_candidate.candidate_id,
                confidence=1.0,
            ),
        ),
        agent_id="system:layer1_worker",
    )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id="system:layer1_worker",
        db=None,
    )
    override = ReconciliationHint(
        candidate_id=candidate_id,
        operation=ReconciliationOperation.UPDATE,
        target_node_id=original["id"],
        confidence=0.9,
        reason="child session reconciles an existing parent reference",
    )
    async with session_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text="cross-artifact provenance regression",
                agent_overrides={candidate_id: override},
            ),
            agent_id="system:layer1_worker",
            db=db,
        )

    assert commit.nodes_updated == 1
    updated = await _query_one_async(board_id, parent_ref)
    assert updated["title"] == "[cross-artifact] parent reference"
    assert updated["source_content_hash"] == original["source_content_hash"]
    assert updated["source_content_hash"] != begin.content_hash
    assert updated["attestation_count"] == original["attestation_count"] + 1


# ---------------------------------------------------------------------------
# TS1 — re-consolidação não duplica
# ---------------------------------------------------------------------------


async def test_ts1_reconsolidation_does_not_duplicate_entity(
    dedup_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    commit1 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[NC-8 TS1] Spec X"
    )
    assert commit1.nodes_added >= 1

    commit2 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[NC-8 TS1] Spec X"
    )
    # Commit on the existing node still records the candidate mapping but
    # doesn't add a new row — nodes_added must be 0.
    assert commit2.nodes_added == 0, (
        f"expected 0 new nodes on re-consolidation, got {commit2.nodes_added}"
    )

    count = await _count_entities_async(board_id, artifact_ref)
    assert count == 1, f"expected exactly 1 Entity for {artifact_ref}, got {count}"


async def test_kgdet_multi_item_refs_preserve_siblings_on_reprocess(
    dedup_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    spec_payload = _kgdet_spec_payload(spec_id)

    commit1 = await _drive_spec_worker_session(session_factory, board_id, spec_payload)
    assert commit1.nodes_added >= 15

    expected_counts = {
        "Requirement": 2,
        "Constraint": 4,
        "Criterion": 2,
        "TestScenario": 2,
        "APIContract": 2,
        "Decision": 2,
    }
    source_prefix = f"spec:{spec_id}:"
    for node_type, expected in expected_counts.items():
        assert (
            await _count_nodes_by_source_prefix_async(
                board_id, node_type, source_prefix
            )
            == expected
        )
    assert await _count_entities_async(board_id, f"spec:{spec_id}") == 1
    assert await _count_nodes_async(board_id, "Learning") == 0
    assert await _count_nodes_async(board_id, "Alternative") == 0

    commit2 = await _drive_spec_worker_session(session_factory, board_id, spec_payload)
    assert commit2.nodes_added == 0
    for node_type, expected in expected_counts.items():
        assert (
            await _count_nodes_by_source_prefix_async(
                board_id, node_type, source_prefix
            )
            == expected
        )
    assert await _count_entities_async(board_id, f"spec:{spec_id}") == 1
    assert await _count_nodes_async(board_id, "Learning") == 0
    assert await _count_nodes_async(board_id, "Alternative") == 0


# ---------------------------------------------------------------------------
# TS2 — re-consolidação atualiza atributos preservando históricos
# ---------------------------------------------------------------------------


async def test_ts2_reconsolidation_updates_attrs_preserves_history(
    dedup_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(session_factory, board_id, artifact_ref, "Original", "A")
    snapshot_before = await _query_one_async(board_id, artifact_ref)
    assert snapshot_before["title"] == "Original"
    assert snapshot_before["content"] == "A"

    # Same title (identity) with refreshed content — a changed title now
    # supersedes-with-trail (spec MKG-D-S1 FR8; see test_kg_nc8_supersede_trail).
    await _drive_one_session(session_factory, board_id, artifact_ref, "Original", "B")
    snapshot_after = await _query_one_async(board_id, artifact_ref)
    # Attrs updated:
    assert snapshot_after["title"] == "Original"
    assert snapshot_after["content"] == "B"
    # Same underlying node (same kuzu id, same created_at):
    assert snapshot_after["id"] == snapshot_before["id"], (
        "expected same kuzu node id after re-consolidation, dedup failed"
    )
    assert snapshot_after["created_at"] == snapshot_before["created_at"], (
        "created_at must be preserved across re-consolidation"
    )


# ---------------------------------------------------------------------------
# TS3 — human_curated=true preserva node
# ---------------------------------------------------------------------------


async def test_ts3_human_curated_preserves_node(dedup_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "Custom", "human-edited"
    )
    snapshot = await _query_one_async(board_id, artifact_ref)
    node_id = snapshot["id"]

    # Mark as human_curated=true via direct Cypher (simulating a back-office
    # action). The fix must honour this flag in the dedup branch.
    await run_blocking_graph_io(
        lambda: _set_human_curated_sync(board_id, node_id),
        task_name="tests.dedup_nc8.set_human_curated",
    )

    # Re-consolidate with a different title — must NOT alter the curated node.
    await _drive_one_session(
        session_factory, board_id, artifact_ref, "AutoGen", "auto-content"
    )
    snapshot_after = await _query_one_async(board_id, artifact_ref)
    assert snapshot_after["title"] == "Custom", (
        "human_curated=true must block UPDATE in the dedup branch"
    )
    assert snapshot_after["content"] == "human-edited"


async def test_automatic_confidence_one_is_not_a_human_curated_override(
    dedup_tempdir,
    monkeypatch,
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory,
        board_id,
        artifact_ref,
        "Curated title",
        "human content",
        source_confidence=1.0,
    )
    snapshot = await _query_one_async(board_id, artifact_ref)
    await run_blocking_graph_io(
        lambda: _set_human_curated_sync(board_id, snapshot["id"]),
        task_name="tests.dedup_nc8.set_confidence_one_human_curated",
    )

    await _drive_one_session(
        session_factory,
        board_id,
        artifact_ref,
        "Automatic replacement",
        "agent content",
        source_confidence=1.0,
    )

    snapshot_after = await _query_one_async(board_id, artifact_ref)
    assert snapshot_after["id"] == snapshot["id"]
    assert snapshot_after["title"] == "Curated title"
    assert snapshot_after["content"] == "human content"


async def test_automatic_decision_supersede_preserves_human_curated_target(
    dedup_tempdir,
    monkeypatch,
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}:decision:curated"

    await _drive_one_session(
        session_factory,
        board_id,
        artifact_ref,
        "Human decision",
        "Keep the curated assertion",
        node_type="Decision",
    )
    lineage = await _query_decision_lineage_async(board_id, artifact_ref)
    assert len(lineage["nodes"]) == 1
    original = lineage["nodes"][0]
    await run_blocking_graph_io(
        lambda: _set_human_curated_sync(board_id, original[0], "Decision"),
        task_name="tests.dedup_nc8.set_decision_human_curated",
    )

    await _drive_one_session(
        session_factory,
        board_id,
        artifact_ref,
        "Automatic replacement",
        "Different assertion",
        node_type="Decision",
    )

    lineage_after = await _query_decision_lineage_async(board_id, artifact_ref)
    assert lineage_after["nodes"] == [
        [original[0], "Human decision", "Keep the curated assertion", 0, None]
    ]
    assert lineage_after["supersedes_edges"] == 0


# ---------------------------------------------------------------------------
# TS8 — structured log `kg.consolidation.dedup_reused` emitido
# ---------------------------------------------------------------------------


async def test_ts8_dedup_reused_log_emitted(dedup_tempdir, monkeypatch):
    """Capture cross-thread log via a dedicated handler — pytest's caplog
    reads records from the calling thread only, but `_run_kuzu` runs in a
    `loop.run_in_executor` worker pool so the dedup log is emitted from a
    different thread. A custom handler attached to the primitives logger
    captures records regardless of thread origin.
    """
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "Spec for log test"
    )

    import logging

    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):  # noqa: D401
            if getattr(record, "event", None) == "kg.consolidation.dedup_reused":
                captured.append(record)

    logger = logging.getLogger("okto_pulse.kg.primitives")
    logger.setLevel(logging.INFO)
    handler = _Capture(level=logging.INFO)
    logger.addHandler(handler)
    try:
        # Same title (a changed title would supersede-with-trail instead —
        # spec MKG-D-S1 FR8) with different content to hit the reuse path.
        await _drive_one_session(
            session_factory,
            board_id,
            artifact_ref,
            "Spec for log test",
            content="conteudo novo v2",
        )
    finally:
        logger.removeHandler(handler)

    assert captured, (
        "expected kg.consolidation.dedup_reused log on re-consolidation; "
        "no matching records captured"
    )
    artifact_records = [
        rec for rec in captured if rec.source_artifact_ref == artifact_ref
    ]
    assert artifact_records, (
        "expected a dedup_reused log for the artifact Entity; captured only "
        f"{[rec.source_artifact_ref for rec in captured]}"
    )
    rec = artifact_records[0]
    assert rec.node_type == "Entity"
    assert rec.source_artifact_ref == artifact_ref
    assert rec.was_curated_preserved is False
    assert rec.cand_id
    assert rec.existing_id
    assert rec.session_id


# ---------------------------------------------------------------------------
# TS7 — tech-entity dedup cross-spec
# ---------------------------------------------------------------------------


async def test_ts7_tech_entity_dedup_cross_spec(dedup_tempdir, monkeypatch):
    """Three distinct specs that all mention the same tech canonical
    (via tech_entities.yml) must collapse to a single Entity node.

    The worker emits an `ent_<canonical_slug>` candidate with
    `source_artifact_ref="tech_entities.yml"` for each spec mentioning the
    tech. Without the dedup branch, three Entity nodes appeared in Kùzu;
    with the fix, the second + third specs reuse the existing Entity.
    """
    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    tech_ref = "tech_entities.yml"

    from okto_pulse.core.kg.primitives import (
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
    from okto_pulse.core.kg.schemas import (
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        KGNodeType,
        NodeCandidate,
        ProposeReconciliationRequest,
    )

    async def _emit_python_mention(spec_id: str) -> None:
        cand = NodeCandidate(
            candidate_id=f"ent_python_{spec_id[:6]}",
            node_type=KGNodeType.ENTITY,
            title="Python",
            content="Python",
            source_artifact_ref=tech_ref,
            source_confidence=1.0,
        )
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec_id,
                raw_content=f"NC-8 TS7 — spec {spec_id} mentions Python",
                deterministic_candidates=[cand],
            ),
            agent_id="system:layer1_worker",
            db=None,
        )
        await propose_reconciliation(
            ProposeReconciliationRequest(session_id=begin.session_id),
            agent_id="system:layer1_worker",
            db=None,
        )
        async with session_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(
                    session_id=begin.session_id,
                    summary_text="NC-8 TS7 Python mention",
                ),
                agent_id="system:layer1_worker",
                db=db,
            )

    spec_ids = [str(uuid.uuid4()) for _ in range(3)]
    for sid in spec_ids:
        await _emit_python_mention(sid)

    count = await run_blocking_graph_io(
        lambda: _count_named_entities_sync(board_id, "Python"),
        task_name="tests.dedup_nc8.count_named_entities",
    )
    assert count == 1, (
        f"expected 1 Python Entity node across 3 specs (cross-spec dedup), got {count}"
    )
