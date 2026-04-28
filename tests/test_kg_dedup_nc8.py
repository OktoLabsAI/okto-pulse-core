"""NC-8 — KG Entity dedup on re-consolidation (spec 7f23535f).

Covers the bug where re-consolidating the same artefact (spec/sprint/card)
produced multiple Kuzu Entity nodes for the same `source_artifact_ref`
because the ADD branch of `_do_kuzu_commit` never consulted the existing
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


pytestmark = pytest.mark.asyncio


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

    yield base

    try:
        from okto_pulse.core.kg.schema import close_all_connections
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
    agent_id: str = "agent-nc8-test",
):
    """Run one begin → propose → commit cycle for a single Entity candidate.

    Returns the CommitConsolidationResponse so callers can assert on
    `nodes_added` and the kuzu node id mappings.
    """
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

    cand = NodeCandidate(
        candidate_id=f"nc8_entity_{uuid.uuid4().hex[:8]}",
        node_type=KGNodeType.ENTITY,
        title=title,
        content=content,
        source_artifact_ref=artifact_ref,
        source_confidence=0.95,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=artifact_ref.split(":", 1)[1],
            raw_content=f"NC-8 dedup test — {title}",
            deterministic_candidates=[cand],
        ),
        agent_id=agent_id,
        db=None,
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
                summary_text=f"NC-8 commit — {title}",
            ),
            agent_id=agent_id,
            db=db,
        )
    return commit


async def _bootstrap_test_board(monkeypatch):
    """Common setup: create DB schema, stub similarity search, return ids."""
    from okto_pulse.core.infra.database import (
        create_database,
        get_session_factory,
        init_db,
    )
    from okto_pulse.core.kg.interfaces.registry import (
        configure_kg_registry,
        reset_registry_for_tests,
    )
    from okto_pulse.core.kg.schema import bootstrap_board_graph

    db_url = os.environ["DATABASE_URL"]
    create_database(db_url, echo=False)
    await init_db()
    reset_registry_for_tests()
    session_factory = get_session_factory()
    configure_kg_registry(session_factory=session_factory)

    board_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
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
    from okto_pulse.core.kg.schema import open_board_connection
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.source_artifact_ref = $r "
            "RETURN count(n)",
            {"r": source_artifact_ref},
        )
        try:
            return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass


def _query_one(board_id: str, source_artifact_ref: str):
    """Return (id, title, content) of the first Entity for a given ref."""
    from okto_pulse.core.kg.schema import open_board_connection
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.source_artifact_ref = $r "
            "RETURN n.id, n.title, n.content, n.created_at LIMIT 1",
            {"r": source_artifact_ref},
        )
        try:
            row = res.get_next()
            return {
                "id": row[0], "title": row[1], "content": row[2],
                "created_at": row[3],
            }
        finally:
            try:
                res.close()
            except Exception:
                pass


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

    count = _count_entities(board_id, artifact_ref)
    assert count == 1, f"expected exactly 1 Entity for {artifact_ref}, got {count}"


# ---------------------------------------------------------------------------
# TS2 — re-consolidação atualiza atributos preservando históricos
# ---------------------------------------------------------------------------


async def test_ts2_reconsolidation_updates_attrs_preserves_history(
    dedup_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "Original", "A"
    )
    snapshot_before = _query_one(board_id, artifact_ref)
    assert snapshot_before["title"] == "Original"
    assert snapshot_before["content"] == "A"

    await _drive_one_session(
        session_factory, board_id, artifact_ref, "Atualizado", "B"
    )
    snapshot_after = _query_one(board_id, artifact_ref)
    # Attrs updated:
    assert snapshot_after["title"] == "Atualizado"
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
    snapshot = _query_one(board_id, artifact_ref)
    node_id = snapshot["id"]

    # Mark as human_curated=true via direct Cypher (simulating a back-office
    # action). The fix must honour this flag in the dedup branch.
    from okto_pulse.core.kg.schema import open_board_connection
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        kconn.execute(
            "MATCH (n:Entity) WHERE n.id = $id SET n.human_curated = true",
            {"id": node_id},
        )

    # Re-consolidate with a different title — must NOT alter the curated node.
    await _drive_one_session(
        session_factory, board_id, artifact_ref, "AutoGen", "auto-content"
    )
    snapshot_after = _query_one(board_id, artifact_ref)
    assert snapshot_after["title"] == "Custom", (
        "human_curated=true must block UPDATE in the dedup branch"
    )
    assert snapshot_after["content"] == "human-edited"


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
        await _drive_one_session(
            session_factory, board_id, artifact_ref, "Spec for log test (v2)"
        )
    finally:
        logger.removeHandler(handler)

    assert captured, (
        "expected kg.consolidation.dedup_reused log on re-consolidation; "
        "no matching records captured"
    )
    rec = captured[0]
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
            agent_id="agent-nc8-ts7",
            db=None,
        )
        await propose_reconciliation(
            ProposeReconciliationRequest(session_id=begin.session_id),
            agent_id="agent-nc8-ts7",
            db=None,
        )
        async with session_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(
                    session_id=begin.session_id,
                    summary_text="NC-8 TS7 Python mention",
                ),
                agent_id="agent-nc8-ts7",
                db=db,
            )

    spec_ids = [str(uuid.uuid4()) for _ in range(3)]
    for sid in spec_ids:
        await _emit_python_mention(sid)

    from okto_pulse.core.kg.schema import open_board_connection
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.title = $t RETURN count(n)",
            {"t": "Python"},
        )
        try:
            count = int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass
    assert count == 1, (
        f"expected 1 Python Entity node across 3 specs (cross-spec dedup), "
        f"got {count}"
    )
