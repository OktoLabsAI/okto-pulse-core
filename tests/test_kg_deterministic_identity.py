"""MKG-A C2 — deterministic node identity in the real commit path.

Covers spec MKG-A-S1 scenarios S2 (supersede mints generation+1, stable on
re-computation) and the integration side of S1/S8 (fresh CREATE mints the
deterministic recipe id; NC-8 reuse keeps the existing id; zero data
migration on the way in).

Reuses the NC-8 real-graph harness (throwaway SQLite + per-board Ladybug
graph, stub embeddings).
"""

from __future__ import annotations

import gc
import shutil
import tempfile
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry

from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id

from test_kg_dedup_nc8 import (  # noqa: F401  (fixture reuse)
    _bootstrap_test_board,
    _drive_one_session,
    _query_one,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _restore_conftest_engine(preserve_relational_runtime):
    yield


@pytest.fixture
def identity_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_mkga_"))
    db_path = base / "pulse.db"
    kg_path = base / "kg"
    kg_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("KG_BASE_DIR", str(kg_path))
    monkeypatch.setenv("KG_CLEANUP_ENABLED", "false")
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()

    yield base

    try:
        from kg_schema_testing import close_all_connections

        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _node_attrs(board_id: str, node_id: str) -> dict:
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.id = $id "
            "RETURN n.id, n.generation, n.superseded_by LIMIT 1",
            {"id": node_id},
        )
        try:
            row = res.get_next()
            return {"id": row[0], "generation": row[1], "superseded_by": row[2]}
        finally:
            try:
                res.close()
            except Exception:
                pass


async def test_fresh_create_mints_deterministic_recipe_id(
    identity_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    commit = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-A] Spec Y"
    )
    assert commit.nodes_added >= 1

    node = _query_one(board_id, artifact_ref)
    expected = mint_node_id(
        board_id,
        "Entity",
        derive_natural_key(artifact_ref, "Entity", "[MKG-A] Spec Y"),
        0,
    )
    assert node["id"] == expected

    attrs = _node_attrs(board_id, node["id"])
    assert int(attrs["generation"] or 0) == 0


async def test_nc8_reuse_keeps_existing_id(identity_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-A] Spec Y"
    )
    first_id = _query_one(board_id, artifact_ref)["id"]

    commit2 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-A] Spec Y revised"
    )
    assert commit2.nodes_added == 0
    assert _query_one(board_id, artifact_ref)["id"] == first_id


async def test_supersede_mints_generation_plus_one_deterministically(
    identity_tempdir, monkeypatch
):
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
        ReconciliationHint,
        ReconciliationOperation,
    )

    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-A] Spec Z"
    )
    old_id = _query_one(board_id, artifact_ref)["id"]

    cand_id = "mkga_supersede_cand"
    cand = NodeCandidate(
        candidate_id=cand_id,
        node_type=KGNodeType.ENTITY,
        title="[MKG-A] Spec Z v2",
        content="superseding content",
        source_artifact_ref=artifact_ref,
        source_confidence=0.95,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=spec_id,
            raw_content="MKG-A supersede cycle",
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
    override = ReconciliationHint(
        candidate_id=cand_id,
        operation=ReconciliationOperation.SUPERSEDE,
        target_node_id=old_id,
        confidence=0.9,
        reason="test forces SUPERSEDE",
    )
    async with session_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text="forced supersede",
                agent_overrides={cand_id: override},
            ),
            agent_id="system:layer1_worker",
            db=db,
        )
    assert commit.nodes_superseded == 1

    # Successor id follows the deterministic recipe at generation 1 (S2/AC2):
    # the old node was generation 0 (fresh create), so the successor mints 1.
    expected_successor = mint_node_id(
        board_id,
        "Entity",
        derive_natural_key(artifact_ref, "Entity", "[MKG-A] Spec Z v2"),
        1,
    )
    successor = _node_attrs(board_id, expected_successor)
    assert successor["id"] == expected_successor
    assert int(successor["generation"] or 0) == 1
    assert successor["id"] != old_id

    old = _node_attrs(board_id, old_id)
    assert old["superseded_by"] == expected_successor
