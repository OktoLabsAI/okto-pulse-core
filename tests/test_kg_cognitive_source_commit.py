"""MKG-A C4 — durable cognitive-source append in the real commit path.

Covers spec MKG-A-S1 scenario S3 (AC3): a cognitive commit appends the
durable record BEFORE reporting success; with the store unavailable the
commit aborts fail-closed with the stable code
``kg_cognitive_source_unavailable`` and NO cognitive node lands in the
graph; retry after recovery succeeds.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry
from sqlalchemy import select

from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id
from okto_pulse.core.kg.primitives import KGPrimitiveError
from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceError,
    CognitiveSourceRecord,
    register_cognitive_source_store,
    reset_cognitive_source_store_for_tests,
)

from test_kg_dedup_nc8 import _bootstrap_test_board  # noqa: F401

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _restore_conftest_engine(preserve_relational_runtime):
    yield
    reset_cognitive_source_store_for_tests()


@pytest.fixture
def cogsrc_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_mkga_c4_"))
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


async def _ensure_community_tables():
    """The throwaway harness DB may lack community-only tables — create
    them from the community Base metadata (checkfirst, idempotent)."""

    from okto_pulse.community.adapters.sqlalchemy_base import Base as CommunityBase
    from okto_pulse.core.infra.database import get_engine

    async with get_engine().begin() as conn:
        await conn.run_sync(CommunityBase.metadata.create_all)


class _BrokenStore:
    async def append(self, record: CognitiveSourceRecord) -> str:
        raise CognitiveSourceError(
            "cognitive_source_append_failed",
            board_id=record.board_id,
            node_id=record.node_id,
            remediation="test-injected outage",
        )

    async def enumerate(self, board_id: str):
        return ()


async def _drive_learning_session(session_factory, board_id: str, title: str):
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

    root_cand = NodeCandidate(
        candidate_id="mkga_c4_root",
        node_type=KGNodeType.ENTITY,
        title="MKG-A C4 technical root",
        content="Allowlisted deterministic source root.",
        source_artifact_ref="tech_entities.yml",
        source_confidence=1.0,
    )
    learning = NodeCandidate(
        candidate_id=f"mkga_learning_{uuid.uuid4().hex[:8]}",
        node_type=KGNodeType.LEARNING,
        title=title,
        content="lesson body",
        justification="observed in test",
        source_confidence=0.9,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=str(uuid.uuid4()),
            raw_content=f"MKG-A C4 — {title}",
            deterministic_candidates=[root_cand, learning],
        ),
        agent_id="system:layer1_worker",
        db=None,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id=f"edge_{learning.candidate_id}_belongs",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id=learning.candidate_id,
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
    async with session_factory() as db:
        return await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text=f"MKG-A C4 commit — {title}",
            ),
            agent_id="system:layer1_worker",
            db=db,
        )


def _count_learnings(board_id: str) -> int:
    from kg_schema_testing import open_board_connection

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute("MATCH (n:Learning) RETURN count(n)")
        try:
            return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass


async def test_commit_appends_durable_record_before_success(
    cogsrc_tempdir, monkeypatch
):
    from okto_pulse.community.adapters.sqlalchemy_kg_cognitive_source import (
        CommunitySqlAlchemyCognitiveSourceStore,
    )
    from okto_pulse.community.adapters.sqlalchemy_models import KGCognitiveSource

    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    await _ensure_community_tables()
    register_cognitive_source_store(
        CommunitySqlAlchemyCognitiveSourceStore(session_factory)
    )

    title = "[MKG-A C4] retry-safe learning"
    commit = await _drive_learning_session(session_factory, board_id, title)
    assert commit.nodes_added >= 1

    expected_id = mint_node_id(
        board_id, "Learning", derive_natural_key("", "Learning", title), 0
    )
    async with session_factory() as db:
        row = (
            await db.execute(
                select(KGCognitiveSource).where(
                    KGCognitiveSource.node_id == expected_id
                )
            )
        ).scalar_one()
    assert row.board_id == board_id
    assert row.node_type == "Learning"
    assert row.generation == 0
    assert row.payload["title"] == title
    assert row.source_session_id


async def test_store_outage_aborts_commit_fail_closed_then_retry_works(
    cogsrc_tempdir, monkeypatch
):
    from okto_pulse.community.adapters.sqlalchemy_kg_cognitive_source import (
        CommunitySqlAlchemyCognitiveSourceStore,
    )

    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    await _ensure_community_tables()
    register_cognitive_source_store(_BrokenStore())

    title = "[MKG-A C4] outage learning"
    with pytest.raises(KGPrimitiveError) as excinfo:
        await _drive_learning_session(session_factory, board_id, title)
    assert excinfo.value.code == "kg_cognitive_source_unavailable"

    # Fail-closed: NO Learning node landed in the graph (S3/AC3 —
    # the graph is never ahead of the durable source).
    assert _count_learnings(board_id) == 0

    # Retry after recovery: register the healthy store and rerun.
    register_cognitive_source_store(
        CommunitySqlAlchemyCognitiveSourceStore(session_factory)
    )
    commit = await _drive_learning_session(session_factory, board_id, title)
    assert commit.nodes_added >= 1
    assert _count_learnings(board_id) == 1
