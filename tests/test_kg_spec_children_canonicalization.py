"""Behavioral tests for canonicalization of spec-done children (spec eaf185c9, card 302044a7).

    * ts_838e4ef9 (card 43bc4311) AC1 — a done spec materializes ALL structured children
      (FR/TR/AC/BR/API/TestScenario/IR/OR/Decision) as graph_layer=canonical + canonical_eligible.
    * ts_e16a76cd (card adcb1d25) AC2 — a pre-done spec keeps the same children working-only.
    * ts_bd28324f (card 42034e1c) AC5 — promotion working->canonical by source_artifact_ref
      without duplicating; including the human_curated subcase: maturity metadata promotes,
      but protected content (title/content/context/justification) is NOT overwritten.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.primitives import (
    _apply_graph_node_create,
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from kg_schema_testing import open_board_connection
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)
from okto_pulse.core.application.processors.consolidation import (
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker


def _full_spec(status: str) -> dict:
    """A spec carrying ALL nine structured child families."""
    return {
        "id": f"spec-{uuid.uuid4().hex[:8]}",
        "title": "Full spec",
        "description": "spec with every child type",
        "status": status,
        "board_id": "board-canon",
        "context": "## Decisions\n- Use PostgreSQL\n",
        "functional_requirements": ["FR alpha", "FR beta"],
        "technical_requirements": [{"text": "TR alpha"}],
        "acceptance_criteria": ["AC alpha"],
        "business_rules": ["BR alpha"],
        "test_scenarios": [
            {
                "id": "ts_x",
                "title": "Scenario",
                "given": "g",
                "when": "w",
                "then": "t",
                "linked_criteria": ["AC alpha"],
            }
        ],
        "api_contracts": [{"name": "GET /x", "description": "an api"}],
        "integration_requirements": [
            {"id": "ir_x", "title": "IR alpha", "description": "integ"}
        ],
        "observability_requirements": [
            {"id": "or_x", "title": "OR alpha", "metric_name": "m", "description": "obs"}
        ],
        "decisions": [
            {"id": "dec_x", "title": "Decision alpha", "status": "active", "rationale": "r"}
        ],
    }


def _children(result) -> list:
    return [
        n
        for n in result.nodes
        if not n.source_artifact_ref.startswith("board:")
        and n.source_artifact_ref != "tech_entities.yml"
    ]


async def _ensure_relational_board(db_factory, board_id: str, owner_id: str) -> None:
    from sqlalchemy_test_models import Board

    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name=board_id, owner_id=owner_id))
            await db.commit()


def _count_api_implements_constraint(board_id: str, *, api_title: str, tr_title: str) -> int:
    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (a:APIContract)-[r:implements]->(c:Constraint) "
            "WHERE a.title = $api_title AND c.title = $tr_title "
            "RETURN count(r)",
            {"api_title": api_title, "tr_title": tr_title},
        )
        try:
            if res.has_next():
                return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass
    return 0


# ---------------------------------------------------------------------------
# ts_838e4ef9 (card 43bc4311) AC1 — done spec -> all children canonical
# ---------------------------------------------------------------------------


def test_spec_done_materializes_all_children_canonical():
    full = _full_spec("done")
    children = _children(DeterministicWorker().process_spec(full))

    assert children
    assert all(n.graph_layer == "canonical" for n in children)
    assert all(n.maturity_status == "canonical_eligible" for n in children)

    # IR + OR + decisions materialize as their own canonical nodes — removing
    # them strictly reduces the emitted node set (so every family contributes).
    without = {
        **full,
        "integration_requirements": [],
        "observability_requirements": [],
        "decisions": [],
    }
    without_children = _children(DeterministicWorker().process_spec(without))
    assert len(children) > len(without_children)


# ---------------------------------------------------------------------------
# ts_e16a76cd (card adcb1d25) AC2 — pre-done spec -> working-only
# ---------------------------------------------------------------------------


def test_spec_predone_children_are_working_only():
    children = _children(DeterministicWorker().process_spec(_full_spec("draft")))

    assert children
    # working-only: a canonical-only KG read filters on graph_layer=canonical,
    # so these nodes are excluded by construction.
    assert all(n.graph_layer == "working" for n in children)
    assert all(n.maturity_status == "working_immature" for n in children)


@pytest.mark.asyncio
async def test_commit_materializes_api_contract_implements_tr_constraint(
    board_id, db_factory, board_handle,
):
    await _ensure_relational_board(db_factory, board_id, "system:layer1_worker")
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    spec = {
        "id": spec_id,
        "title": "TR linked API contract",
        "status": "done",
        "board_id": board_id,
        "functional_requirements": [
            {"id": "fr-login", "text": "User can log in"},
        ],
        "technical_requirements": [
            {"id": "tr-audit-events", "text": "Login API emits audit events"},
        ],
        "api_contracts": [
            {
                "id": "api-login",
                "method": "POST",
                "path": "/login",
                "linked_requirements": ["tr-audit-events"],
            },
        ],
    }
    worker_result = DeterministicWorker().process_spec(spec)

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec_id,
                raw_content=worker_result.raw_content or "tr linked api contract",
                deterministic_candidates=[
                    _worker_node_to_candidate(node) for node in worker_result.nodes
                ],
            ),
            agent_id="system:layer1_worker",
            db=db,
        )

    for edge in worker_result.edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=_worker_edge_to_candidate(edge),
            ),
            agent_id="system:layer1_worker",
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id="system:layer1_worker",
        db=None,
        force_reprocess=True,
    )
    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id="system:layer1_worker",
            db=db,
        )

    assert commit.connectivity["passed"] is True
    assert await _count_api_implements_constraint_async(
        board_id,
        api_title="POST /login",
        tr_title="Login API emits audit events",
    ) == 1


# ---------------------------------------------------------------------------
# ts_bd28324f (card 42034e1c) AC5 — promotion without duplicating + curated guard
# ---------------------------------------------------------------------------


def _seed_decision(board_id, *, title, human_curated):
    """Seed a root Entity + a WORKING (optionally human_curated) Decision that
    ``belongs_to`` it, so the source_artifact_ref merge has a CONNECTED node to
    reuse (the connectivity guard otherwise rejects an isolated node). Returns
    the Decision's source_artifact_ref."""
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    spec_ref = f"spec:{uuid.uuid4().hex[:10]}"
    decision_ref = f"{spec_ref}:decision:{uuid.uuid4().hex[:8]}"
    root_id = f"entity_seed_{uuid.uuid4().hex[:12]}"
    node_id = f"decision_seed_{uuid.uuid4().hex[:12]}"

    def _attrs(title_, content, ref, curated, layer):
        return {
            "title": title_,
            "content": content,
            "context": "ORIGINAL CONTEXT",
            "justification": "ORIGINAL JUSTIFICATION",
            "source_artifact_ref": ref,
            "created_at": "2026-06-08T00:00:00+00:00",
            "created_by_agent": "human",
            "source_confidence": 1.0,
            "relevance_score": 0.5,
            "query_hits": 0,
            "last_queried_at": None,
            "priority_boost": 0.0,
            "human_curated": curated,
            "graph_layer": layer,
            "maturity_status": (
                "working_immature" if layer == "working" else "canonical_eligible"
            ),
            "embedding": [0.0] * 384,
        }

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,

            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _apply_graph_node_create(
            orch, "Entity", root_id,
            _attrs("Spec root", "", spec_ref, False, "canonical"),
        )
        _apply_graph_node_create(
            orch, "Learning", node_id,
            _attrs(title, "ORIGINAL CONTENT", decision_ref, human_curated, "working"),
        )
        orch.create_edge(
            edge_type="belongs_to",
            from_id=node_id,
            to_id=root_id,
            attrs={"confidence": 1.0},
            from_type="Learning",
            to_type="Entity",
        )
    return decision_ref


def _read_decision(board_id, source_ref):
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (n:Learning) WHERE n.source_artifact_ref = $ref "
            "RETURN n.id, coalesce(n.graph_layer, 'legacy_unknown'), "
            "n.maturity_status, n.title, n.content, n.superseded_by",
            {"ref": source_ref},
        )
        rows = []
        try:
            while res.has_next():
                row = res.get_next()
                rows.append(row)
        finally:
            try:
                res.close()
            except Exception:
                pass
        active = next((row for row in rows if not row[5]), None)
        if active is not None:
            return {
                "count": len(rows),
                "superseded_count": sum(bool(row[5]) for row in rows),
                "graph_layer": active[1],
                "maturity_status": active[2],
                "title": active[3],
                "content": active[4],
            }
    return None


async def _count_api_implements_constraint_async(
    board_id: str,
    *,
    api_title: str,
    tr_title: str,
) -> int:
    return await run_blocking_graph_io(
        lambda: _count_api_implements_constraint(
            board_id, api_title=api_title, tr_title=tr_title
        ),
        task_name="tests.spec_children.count_api_constraint",
    )


async def _seed_decision_async(board_id, *, title, human_curated):
    return await run_blocking_graph_io(
        lambda: _seed_decision(
            board_id, title=title, human_curated=human_curated
        ),
        task_name="tests.spec_children.seed_decision",
    )


async def _read_decision_async(board_id, source_ref):
    return await run_blocking_graph_io(
        lambda: _read_decision(board_id, source_ref),
        task_name="tests.spec_children.read_decision",
    )


async def _promote(board_id, agent_id, db_factory, *, source_ref, cand_id):
    await _ensure_relational_board(db_factory, board_id, agent_id)
    # The real promotion is the Layer 1 deterministic worker re-emitting a
    # done spec's child as canonical; model that with a DETERMINISTIC candidate
    # carrying graph_layer=canonical (a cognitive add would be re-classified to
    # the consolidation's maturity instead).
    cand = NodeCandidate(
        candidate_id=cand_id,
        node_type=KGNodeType.LEARNING,
        title="UPDATED TITLE",
        content="UPDATED CONTENT",
        source_artifact_ref=source_ref,
        source_confidence=0.9,
        graph_layer="canonical",
        maturity_status="canonical_eligible",
    )
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=f"spec-{uuid.uuid4().hex[:8]}",
                raw_content=f"promote {source_ref}",
                deterministic_candidates=[cand],
            ),
            agent_id=agent_id,
            db=db,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id,
        db=None,
        force_reprocess=True,
    )
    async with db_factory() as db:
        return await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )


@pytest.mark.asyncio
async def test_promotion_curated_promotes_maturity_preserves_content(
    board_id, agent_id, db_factory, board_handle,
):
    # A human-curated WORKING decision: promotion must lift graph_layer to
    # canonical (maturity metadata) WITHOUT overwriting the curated content.
    source_ref = await _seed_decision_async(
        board_id, title="CURATED TITLE", human_curated=True
    )

    commit = await _promote(
        board_id, agent_id, db_factory, source_ref=source_ref, cand_id="cand_curated"
    )
    assert commit.nodes_merged == 1  # reused, not duplicated
    assert commit.nodes_added == 0

    node = await _read_decision_async(board_id, source_ref)
    assert node is not None
    assert node["count"] == 1  # no duplicate
    assert node["graph_layer"] == "canonical"  # maturity METADATA promoted
    assert node["maturity_status"] == "canonical_eligible"
    assert node["title"] == "CURATED TITLE"  # protected content preserved
    assert node["content"] == "ORIGINAL CONTENT"


@pytest.mark.asyncio
async def test_promotion_non_curated_promotes_and_updates_content(
    board_id, agent_id, db_factory, board_handle,
):
    # A title change is identity-bearing under MKG-D: promotion creates a
    # canonical successor and preserves the previous working state as history.
    source_ref = await _seed_decision_async(
        board_id, title="OLD TITLE", human_curated=False
    )

    commit = await _promote(
        board_id, agent_id, db_factory, source_ref=source_ref, cand_id="cand_agent"
    )
    assert commit.nodes_superseded == 1
    assert commit.nodes_merged == 0
    assert commit.nodes_added == 0

    node = await _read_decision_async(board_id, source_ref)
    assert node is not None
    assert node["count"] == 2
    assert node["superseded_count"] == 1
    assert node["graph_layer"] == "canonical"
    assert node["title"] == "UPDATED TITLE"  # agent-managed content updated
    assert node["content"] == "UPDATED CONTENT"
