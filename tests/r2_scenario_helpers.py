"""Shared real-pipeline builders for the R2 scenario test cards (R2-TEST1..8).

NOT a test module (no ``test_`` prefix -> pytest does not collect it). Every
canonical-current starting state these helpers build goes through the REAL
pipeline (``DeterministicWorker`` + ``begin/propose/commit_consolidation`` for
deterministic nodes, the consolidation orchestrator for cognitive nodes, and the
Global Discovery outbox worker for DecisionDigests) — never a raw Kuzu /
DecisionDigest seed of a demotion/sync proof target. This is the anti-test-theater
contract the R2 brief requires for AC1/AC2/AC6/AC7/AC11/TR12.
"""

from __future__ import annotations

import uuid

from okto_pulse.core.kg.primitives import (
    _apply_kuzu_node_create_with_timestamp,
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    MATURITY_CANONICAL_ELIGIBLE,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from okto_pulse.core.application.processors.consolidation import (
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from sqlalchemy_test_models import Board, Card, Spec

USER_ID = "user-r2-scenarios"
WORKER_AGENT = "system:layer1_worker"


# ---------------------------------------------------------------------------
# SQL source rows (the authoritative maturity source of truth) + boards
# ---------------------------------------------------------------------------


async def new_board(db_factory, prefix: str = "r2t") -> str:
    board_id = f"{prefix}-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r2 scenario", owner_id=USER_ID))
            await db.commit()
    return board_id


def spec_dict(spec_id, board_id, status, *, frs=None, acs=None) -> dict:
    return {
        "id": spec_id,
        "title": "scenario spec",
        "description": "spec producing deterministic canonical children",
        "status": status,
        "board_id": board_id,
        "functional_requirements": frs or ["FR alpha scenario", "FR beta scenario"],
        "acceptance_criteria": acs or ["AC alpha scenario"],
    }


async def insert_spec(db_factory, board_id, spec_id, *, status, frs=None, acs=None):
    async with db_factory() as db:
        db.add(Spec(
            id=spec_id, board_id=board_id, title="scenario spec", status=status,
            created_by=USER_ID,
            functional_requirements=frs or ["FR alpha scenario", "FR beta scenario"],
            acceptance_criteria=acs or ["AC alpha scenario"],
        ))
        await db.commit()


async def set_spec_status(db_factory, spec_id, status):
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.status = status
        await db.commit()


async def insert_bug_card(db_factory, board_id, card_id, *, status):
    async with db_factory() as db:
        db.add(Card(
            id=card_id, board_id=board_id, title="bug card",
            status=status, card_type="bug", created_by=USER_ID,
        ))
        await db.commit()


# ---------------------------------------------------------------------------
# Real deterministic worker + consolidation commit
# ---------------------------------------------------------------------------


async def commit_worker_result(db_factory, board_id, result, *, agent_id=WORKER_AGENT):
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id, artifact_type="spec",
                artifact_id=f"spec-{uuid.uuid4().hex[:8]}",
                raw_content=result.raw_content or "deterministic spec",
                deterministic_candidates=[
                    _worker_node_to_candidate(n) for n in result.nodes
                ],
            ),
            agent_id=agent_id, db=db,
        )
    session_id = begin.session_id
    for edge in result.edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=session_id, candidate=_worker_edge_to_candidate(edge),
            ),
            agent_id=agent_id,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=session_id),
        agent_id=agent_id, db=None, force_reprocess=True,
    )
    async with db_factory() as db:
        return await commit_consolidation(
            CommitConsolidationRequest(session_id=session_id),
            agent_id=agent_id, db=db,
        )


async def seed_done_spec_canonical(db_factory, board_id, *, frs=None, acs=None):
    """Real pipeline: SQL Spec(done) + DeterministicWorker + commit -> canonical
    deterministic nodes. Returns ``(spec_id, spec_ref)``."""
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await insert_spec(db_factory, board_id, spec_id, status="done", frs=frs, acs=acs)
    result = DeterministicWorker().process_spec(
        spec_dict(spec_id, board_id, "done", frs=frs, acs=acs)
    )
    await commit_worker_result(db_factory, board_id, result)
    return spec_id, f"spec:{spec_id}"


async def materialize_spec(db_factory, board_id, *, status, frs=None, acs=None):
    """Real pipeline at an explicit source status (done -> canonical children,
    draft/immature -> working children). Returns ``(spec_id, worker_result)``."""
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await insert_spec(db_factory, board_id, spec_id, status=status, frs=frs, acs=acs)
    result = DeterministicWorker().process_spec(
        spec_dict(spec_id, board_id, status, frs=frs, acs=acs)
    )
    await commit_worker_result(db_factory, board_id, result)
    return spec_id, result


# ---------------------------------------------------------------------------
# Cognitive canonical seed (consolidation orchestrator write primitive)
# ---------------------------------------------------------------------------


def _cognitive_attrs(source_ref, *, title="cognitive node"):
    return {
        "title": title, "content": "", "context": "", "justification": "",
        "source_artifact_ref": source_ref, "created_at": "2026-06-08T00:00:00+00:00",
        "created_by_agent": "agent:cognitive", "source_confidence": 1.0,
        "relevance_score": 0.5, "query_hits": 0, "last_queried_at": None,
        "priority_boost": 0.0, "human_curated": False, "embedding": [0.0] * 384,
        "graph_layer": GRAPH_LAYER_CANONICAL,
        "maturity_status": MATURITY_CANONICAL_ELIGIBLE,
    }


def seed_canonical_cognitive(board_id, node_type, *, source_ref, title="cognitive node"):
    """Materialize a canonical cognitive node via the orchestrator write primitive
    (the R7-validated path, not a raw digest seed). Returns the node id."""
    node_id = f"r2cog_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, conn):
        orch = TransactionOrchestrator(
            kuzu_conn=conn, sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, node_type, node_id, _cognitive_attrs(source_ref, title=title)
        )
    return node_id


def seed_learning_with_canonical_bug(board_id, *, learning_ref):
    """Canonical bug-derived Learning + canonical Bug + validates edge via the
    orchestrator. Returns ``(learning_id, bug_id)``."""
    learning_id = f"r2lng_{uuid.uuid4().hex[:12]}"
    bug_id = f"r2bug_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, conn):
        orch = TransactionOrchestrator(
            kuzu_conn=conn, sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, "Learning", learning_id,
            _cognitive_attrs(learning_ref, title="learning node"),
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, "Bug", bug_id, _cognitive_attrs(f"bug:{bug_id}", title="bug node"),
        )
        orch.create_edge(
            edge_type="validates", from_id=learning_id, to_id=bug_id,
            attrs={"confidence": 1.0}, from_type="Learning", to_type="Bug",
        )
    return learning_id, bug_id


# ---------------------------------------------------------------------------
# Board graph inspection (read-only)
# ---------------------------------------------------------------------------


def count_canonical(board_id, node_type) -> int:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type}) WHERE n.graph_layer = $c RETURN count(n)",
            {"c": GRAPH_LAYER_CANONICAL},
        )
        return int(res.get_next()[0]) if res.has_next() else 0


def count_layer(board_id, node_type, layer) -> int:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type}) WHERE n.graph_layer = $l RETURN count(n)",
            {"l": layer},
        )
        return int(res.get_next()[0]) if res.has_next() else 0


def node_layer(board_id, node_type, node_id) -> str | None:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) RETURN n.graph_layer",
            {"id": node_id},
        )
        return str(res.get_next()[0]) if res.has_next() else None


def first_canonical_node(board_id, node_type) -> tuple[str, str] | None:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type}) WHERE n.graph_layer = $c "
            f"RETURN n.id, n.source_artifact_ref LIMIT 1",
            {"c": GRAPH_LAYER_CANONICAL},
        )
        if res.has_next():
            row = res.get_next()
            return str(row[0]), str(row[1] or "")
    return None


def requirement_ids_from_result(result) -> list[str]:
    return [n.id for n in result.nodes if n.node_type == "Requirement"]
