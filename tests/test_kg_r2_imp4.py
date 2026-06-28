"""R2-IMP4 — read-only stale-canonical parity diagnostics.

Spec 9aedfe78 / card f79ef144 (AC4/AC5, TR4/TR13, FR5).

Anti-test-theater: the canonical starting state is materialized by the REAL
DeterministicWorker + commit_consolidation; the source regression is the real SQL
maturity signal. The drilldown/Health path is READ-ONLY (proven: the stale node
stays canonical after the diagnostic — no demotion/reconcile/sync).
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2i4_"))

from okto_pulse.core.kg.primitives import (
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.schema import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_CANONICAL
from okto_pulse.core.kg.stale_canonical_parity import (
    GD_EVALUATED,
    GD_NOT_EVALUATED,
    list_stale_canonical_parity,
)
from okto_pulse.core.kg.workers.consolidation import (
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.kg.workers.deterministic_worker import DeterministicWorker
from okto_pulse.core.models.db import Board, Spec
from okto_pulse.core.services.kg_health_service import get_kg_health
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphPathResolverForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)

USER_ID = "user-r2-imp4"
SCP_CODE = "stale_canonical_parity"


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
        graph_path_resolver=RealBoardGraphPathResolverForTests(),
    )


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _new_board(db_factory) -> str:
    board_id = f"r2i4-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r2 imp4", owner_id=USER_ID))
            await db.commit()
    return board_id


def _spec_dict(spec_id, board_id, status):
    return {
        "id": spec_id, "title": "stale parity spec",
        "description": "spec producing deterministic canonical children",
        "status": status, "board_id": board_id,
        "functional_requirements": ["FR alpha parity", "FR beta parity"],
        "acceptance_criteria": ["AC alpha parity"],
    }


async def _insert_spec(db_factory, board_id, spec_id, *, status):
    async with db_factory() as db:
        db.add(Spec(
            id=spec_id, board_id=board_id, title="stale parity spec",
            status=status, created_by=USER_ID,
            functional_requirements=["FR alpha parity", "FR beta parity"],
            acceptance_criteria=["AC alpha parity"],
        ))
        await db.commit()


async def _set_spec_status(db_factory, spec_id, status):
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.status = status
        await db.commit()


async def _commit_worker_result(db_factory, board_id, agent_id, result):
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


def _count_canonical(board_id, node_type) -> int:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type}) WHERE n.graph_layer = $c RETURN count(n)",
            {"c": GRAPH_LAYER_CANONICAL},
        )
        return int(res.get_next()[0]) if res.has_next() else 0


async def _make_stale_spec(db_factory, board_id):
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await _insert_spec(db_factory, board_id, spec_id, status="done")
    result = DeterministicWorker().process_spec(_spec_dict(spec_id, board_id, "done"))
    await _commit_worker_result(db_factory, board_id, "system:layer1_worker", result)
    assert _count_canonical(board_id, "Requirement") >= 1
    await _set_spec_status(db_factory, spec_id, "draft")  # regress (real maturity signal)
    return spec_id


def _issues(health, code):
    return [i for i in health.get("health_issues", []) if i.get("code") == code]


# ===========================================================================
# board_graph_stale exposed READ-ONLY (no demotion) — AC4/AC5 + no-mutating
# ===========================================================================


@pytest.mark.asyncio
async def test_board_graph_stale_exposed_read_only(db_factory):
    board_id = await _new_board(db_factory)
    await _make_stale_spec(db_factory, board_id)
    canonical_before = _count_canonical(board_id, "Requirement")
    assert canonical_before >= 1

    async with db_factory() as db:
        result = await list_stale_canonical_parity(db, board_id=board_id)

    assert result["count"] >= 1, result
    item = result["items"][0]
    assert item["board_graph_stale"] is True
    assert item["expected_graph_layer"] == "working"
    assert item["current_source_status"] == "draft"
    assert "expected_maturity_status" in item and item["recommended_action"]
    assert "global_discovery_stale_digest" in item
    assert result["global_discovery_evaluation"] in (GD_EVALUATED, GD_NOT_EVALUATED)

    # NO-MUTATING PATH: the diagnostic did NOT demote — the node is still canonical.
    assert _count_canonical(board_id, "Requirement") == canonical_before


@pytest.mark.asyncio
async def test_health_surfaces_stale_canonical_parity_with_fields(db_factory):
    board_id = await _new_board(db_factory)
    await _make_stale_spec(db_factory, board_id)

    async with db_factory() as db:
        health = await get_kg_health(board_id, db)
    issues = _issues(health, SCP_CODE)
    assert len(issues) == 1, issues
    issue = issues[0]
    assert issue["drill_down_tool"] == "okto_pulse_kg_stale_canonical_parity_list"
    sample = issue["sample"]
    for field in (
        "board_graph_stale", "global_discovery_stale_digest", "expected_graph_layer",
        "expected_maturity_status", "current_source_status", "recommended_action",
    ):
        assert field in sample, field


# ===========================================================================
# Distinct category + deterministic precedence that does not mask R7/debt (TR4)
# ===========================================================================


@pytest.mark.asyncio
async def test_stale_parity_distinct_and_does_not_mask_canonical_debt(db_factory):
    from okto_pulse.core.kg.canonical_learning_partition import PARTITION_TARGET_STATUS
    from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

    board_id = await _new_board(db_factory)
    await _make_stale_spec(db_factory, board_id)  # a real stale_canonical_parity
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="bug", artifact_id="bug-scp",
            source_ref=f"card:bug:{uuid.uuid4()}:learning:s",
            content_hash="r2i4_debt", target_status=PARTITION_TARGET_STATUS,
            canonical_state="pending", failure_reason="some_reason",
        )
        await db.commit()
        health = await get_kg_health(board_id, db)

    # Both are present as DISTINCT categories ...
    assert len(_issues(health, SCP_CODE)) == 1
    assert len(_issues(health, "canonical_debt_open")) == 1
    # ... and stale_canonical_parity NEVER claims primary over the stronger cause.
    assert health["primary_health_cause"] != SCP_CODE


@pytest.mark.asyncio
async def test_gd_evaluation_is_safe_when_unavailable(db_factory, monkeypatch):
    """If the R1 digest detector is unavailable, global_discovery_evaluation is
    not_evaluated (transparent) and items report None (unknown) — never a false
    healthy / false-clean digest."""
    board_id = await _new_board(db_factory)
    await _make_stale_spec(db_factory, board_id)

    import okto_pulse.core.kg.global_discovery.layer_parity as lp

    async def _boom(db, *, board_id):
        raise RuntimeError("simulated R1 digest layer unavailable")

    monkeypatch.setattr(lp, "detect_digest_layer_mismatches", _boom)

    async with db_factory() as db:
        result = await list_stale_canonical_parity(db, board_id=board_id)
    assert result["global_discovery_evaluation"] == GD_NOT_EVALUATED
    assert result["count"] >= 1
    assert result["items"][0]["global_discovery_stale_digest"] is None  # unknown, not False
