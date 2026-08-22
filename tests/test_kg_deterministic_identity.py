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

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id

from test_kg_dedup_nc8 import (  # noqa: F401  (fixture reuse)
    _bootstrap_test_board,
    _drive_one_session,
    _query_one_async,
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


async def _node_attrs_async(board_id: str, node_id: str) -> dict:
    return await run_blocking_graph_io(
        lambda: _node_attrs(board_id, node_id),
        task_name="tests.deterministic_identity.node_attrs",
    )


def _materialize_entity_sync(board_id: str, node_id: str, title: str) -> None:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            "CREATE (n:Entity {id: $id, title: $title, content: '', "
            "context: '', justification: '', source_artifact_ref: '', "
            "source_confidence: 0.95, relevance_score: 0.5, "
            "priority_boost: 0.0, human_curated: false, generation: 0})",
            {"id": node_id, "title": title},
        )


def _count_entity_id_sync(board_id: str, node_id: str) -> int:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, conn):
        result = conn.execute(
            "MATCH (n:Entity) WHERE n.id = $id RETURN count(n)",
            {"id": node_id},
        )
        try:
            return int(result.get_next()[0])
        finally:
            result.close()


def _count_outgoing_belongs_to_sync(board_id: str, node_id: str) -> int:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, conn):
        result = conn.execute(
            "MATCH (n:Entity)-[:belongs_to]->() WHERE n.id = $id RETURN count(*)",
            {"id": node_id},
        )
        try:
            return int(result.get_next()[0])
        finally:
            result.close()


async def test_fresh_create_mints_deterministic_recipe_id(
    identity_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    commit = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-A] Spec Y"
    )
    assert commit.nodes_added >= 1

    node = await _query_one_async(board_id, artifact_ref)
    expected = mint_node_id(
        board_id,
        "Entity",
        derive_natural_key(artifact_ref, "Entity", "[MKG-A] Spec Y"),
        0,
    )
    assert node["id"] == expected

    attrs = await _node_attrs_async(board_id, node["id"])
    assert int(attrs["generation"] or 0) == 0


async def test_nc8_reuse_keeps_existing_id(identity_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(session_factory, board_id, artifact_ref, "[MKG-A] Spec Y")
    first_id = (await _query_one_async(board_id, artifact_ref))["id"]

    commit2 = await _drive_one_session(
        session_factory, board_id, artifact_ref, "[MKG-A] Spec Y revised"
    )
    assert commit2.nodes_added == 0
    assert (await _query_one_async(board_id, artifact_ref))["id"] == first_id


async def test_replay_reuses_already_materialized_deterministic_id(
    identity_tempdir, monkeypatch
):
    """An already-written id is an ACKed replay, not a duplicate-PK DLQ.

    This models the field failure precisely: the deterministic Entity exists
    in Ladybug, but the historical row has no source_artifact_ref, so the
    legacy NC-8 lookup cannot find it.  Reprocessing must bind the missing ref,
    keep one node, and report a merge instead of issuing CREATE again.
    """

    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"
    title = "[MKG-A] Already materialized"
    node_id = mint_node_id(
        board_id,
        "Entity",
        derive_natural_key(artifact_ref, "Entity", title),
        0,
    )

    await run_blocking_graph_io(
        lambda: _materialize_entity_sync(board_id, node_id, title),
        task_name="tests.deterministic_identity.materialize_entity",
    )

    commit = await _drive_one_session(
        session_factory,
        board_id,
        artifact_ref,
        title,
    )

    assert commit.nodes_merged >= 1
    assert any(
        item["operation"] == "MERGE_BY_DETERMINISTIC_ID"
        and item["reused_node_id"] == node_id
        for item in commit.merge_audit_items
    )
    assert (await _query_one_async(board_id, artifact_ref))["id"] == node_id

    assert (
        await run_blocking_graph_io(
            lambda: _count_entity_id_sync(board_id, node_id),
            task_name="tests.deterministic_identity.count_entity_id",
        )
        == 1
    )


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

    await _drive_one_session(session_factory, board_id, artifact_ref, "[MKG-A] Spec Z")
    old_id = (await _query_one_async(board_id, artifact_ref))["id"]

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
    successor = await _node_attrs_async(board_id, expected_successor)
    assert successor["id"] == expected_successor
    assert int(successor["generation"] or 0) == 1
    assert successor["id"] != old_id

    old = await _node_attrs_async(board_id, old_id)
    assert old["superseded_by"] == expected_successor
    assert (
        await run_blocking_graph_io(
            lambda: _count_outgoing_belongs_to_sync(board_id, expected_successor),
            task_name="tests.deterministic_identity.count_successor_provenance",
        )
        == 1
    )

    # At-least-once replay of the same explicit SUPERSEDE must acknowledge the
    # already materialized deterministic successor.  Before the replay guard,
    # this second commit issued CREATE for ``expected_successor`` again and
    # Ladybug raised a duplicate-primary-key error.
    replay_begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=spec_id,
            raw_content="MKG-A supersede cycle replay",
            deterministic_candidates=[cand],
        ),
        agent_id="system:layer1_worker",
        db=None,
    )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=replay_begin.session_id),
        agent_id="system:layer1_worker",
        db=None,
    )
    replay_override = ReconciliationHint(
        candidate_id=cand_id,
        operation=ReconciliationOperation.SUPERSEDE,
        target_node_id=old_id,
        confidence=0.9,
        reason="test replays the same explicit SUPERSEDE",
    )
    async with session_factory() as db:
        replay = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=replay_begin.session_id,
                summary_text="forced supersede replay",
                agent_overrides={cand_id: replay_override},
            ),
            agent_id="system:layer1_worker",
            db=db,
        )
    assert replay.nodes_superseded == 0
    assert replay.nodes_merged == 1
    assert (await _node_attrs_async(board_id, old_id))[
        "superseded_by"
    ] == expected_successor
    assert (
        await run_blocking_graph_io(
            lambda: _count_outgoing_belongs_to_sync(board_id, expected_successor),
            task_name="tests.deterministic_identity.count_replayed_provenance",
        )
        == 1
    )


async def test_supersede_missing_source_stays_blocked(identity_tempdir, monkeypatch):
    """A target id alone cannot launder an unresolved source into provenance."""
    from okto_pulse.core.kg.primitives import (
        KGPrimitiveError,
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
        session_factory,
        board_id,
        artifact_ref,
        "[MKG-A] Existing source",
    )
    old_id = (await _query_one_async(board_id, artifact_ref))["id"]

    missing_ref = f"spec:missing-{spec_id}"
    candidate_id = "mkga_missing_source_supersede"
    candidate = NodeCandidate(
        candidate_id=candidate_id,
        node_type=KGNodeType.ENTITY,
        title="[MKG-A] Unresolved source",
        source_artifact_ref=missing_ref,
        source_confidence=0.95,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=spec_id,
            raw_content="MKG-A unresolved-source supersede",
            deterministic_candidates=[candidate],
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
        candidate_id=candidate_id,
        operation=ReconciliationOperation.SUPERSEDE,
        target_node_id=old_id,
        confidence=0.9,
        reason="attempt to supersede from an unresolved source",
    )

    with pytest.raises(KGPrimitiveError) as excinfo:
        async with session_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(
                    session_id=begin.session_id,
                    agent_overrides={candidate_id: override},
                ),
                agent_id="system:layer1_worker",
                db=db,
            )

    assert excinfo.value.code == "entity_source_identity_mismatch"
    assert excinfo.value.details == {
        "candidate_id": candidate_id,
        "candidate_source_artifact_ref": missing_ref,
        "target_node_id": old_id,
        "target_source_artifact_ref": artifact_ref,
    }
    assert (await _node_attrs_async(board_id, old_id))["superseded_by"] is None
