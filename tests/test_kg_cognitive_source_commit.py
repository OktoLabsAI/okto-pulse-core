"""MKG-A C4 — durable cognitive-source append in the real commit path.

Covers spec MKG-A-S1 scenario S3 (AC3): a cognitive commit closes the graph
scope, appends one atomic durable batch on the owning event loop, and only then
reports success.  With the store unavailable the commit aborts fail-closed,
requests graph compensation and returns ``kg_cognitive_source_unavailable``;
retry after recovery succeeds.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import threading
import uuid
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry
from sqlalchemy import select

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
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


def _require_ambient_community_store(session_factory):
    """Resolve the matching cross-edition adapter or skip this integration oracle.

    Core and Community are independently installable distributions.  A Core
    source checkout can therefore legitimately run beside an older installed
    Community wheel.  These SQL integration cases require the new ambient-UOW
    capability; the Core-owned contract cases below still run when that
    matching adapter is not installed.
    """

    from okto_pulse.community.adapters.sqlalchemy_kg_cognitive_source import (
        CommunitySqlAlchemyCognitiveSourceStore,
    )

    if not callable(
        getattr(
            CommunitySqlAlchemyCognitiveSourceStore,
            "append_many_in_context",
            None,
        )
    ):
        pytest.skip(
            "matching Community cognitive-source adapter is not installed "
            "(append_many_in_context is required)"
        )
    return CommunitySqlAlchemyCognitiveSourceStore(session_factory)


class _BrokenStore:
    async def append(self, record: CognitiveSourceRecord) -> str:
        raise CognitiveSourceError(
            "cognitive_source_append_failed",
            board_id=record.board_id,
            node_id=record.node_id,
            remediation="test-injected outage",
        )

    async def append_many(
        self, records: tuple[CognitiveSourceRecord, ...]
    ) -> tuple[str, ...]:
        record = records[0]
        raise CognitiveSourceError(
            "cognitive_source_append_failed",
            board_id=record.board_id,
            node_id=record.node_id,
            remediation="test-injected outage",
        )

    async def append_many_in_context(
        self,
        context: object,
        records: tuple[CognitiveSourceRecord, ...],
    ) -> tuple[str, ...]:
        del context
        return await self.append_many(records)

    async def enumerate(self, board_id: str):
        return ()


class _CapturingBatchStore:
    def __init__(self) -> None:
        self.append_many_calls = 0
        self.thread_id: int | None = None
        self.records: tuple[CognitiveSourceRecord, ...] = ()
        self.contexts: list[object] = []

    async def append(self, record: CognitiveSourceRecord) -> str:
        raise AssertionError("commit path must use append_many")

    async def append_many(
        self, records: tuple[CognitiveSourceRecord, ...]
    ) -> tuple[str, ...]:
        self.append_many_calls += 1
        self.thread_id = threading.get_ident()
        self.records = records
        return tuple(record.node_id for record in records)

    async def append_many_in_context(
        self,
        context: object,
        records: tuple[CognitiveSourceRecord, ...],
    ) -> tuple[str, ...]:
        self.contexts.append(context)
        return await self.append_many(records)

    async def enumerate(self, board_id: str):
        return tuple(record for record in self.records if record.board_id == board_id)


class _UnexpectedBrokenStore(_CapturingBatchStore):
    async def append_many(
        self, records: tuple[CognitiveSourceRecord, ...]
    ) -> tuple[str, ...]:
        raise RuntimeError("test-injected unexpected adapter failure")


class _LegacyStoreWithoutContextAppend:
    async def append(self, record: CognitiveSourceRecord) -> str:
        return record.node_id

    async def append_many(
        self, records: tuple[CognitiveSourceRecord, ...]
    ) -> tuple[str, ...]:
        return tuple(record.node_id for record in records)

    async def enumerate(self, board_id: str):
        del board_id
        return ()


async def _drive_learning_session(
    session_factory,
    board_id: str,
    title: str,
    *,
    source_artifact_ref: str = "",
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
        source_artifact_ref=source_artifact_ref,
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
        result = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text=f"MKG-A C4 commit — {title}",
            ),
            agent_id="system:layer1_worker",
            db=db,
        )
        await db.commit()
        return result


def _count_learnings_sync(board_id: str) -> int:
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


async def _count_learnings(board_id: str) -> int:
    return await run_blocking_graph_io(
        lambda: _count_learnings_sync(board_id),
        task_name="tests.cognitive_source_commit.count_learnings",
    )


def _create_graph_ahead_learning_sync(
    board_id: str,
    *,
    node_id: str,
    title: str,
    source_ref: str = "",
) -> None:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, connection):
        result = connection.execute(
            "CREATE (n:Learning {id: $id, title: $title, content: $content, "
            "context: '', justification: $justification, "
            "source_artifact_ref: $source_ref, source_confidence: 0.9, "
            "relevance_score: 0.5, priority_boost: 0.0, "
            "human_curated: false, generation: 0})",
            {
                "id": node_id,
                "title": title,
                "content": "lesson body",
                "justification": "observed in test",
                "source_ref": source_ref,
            },
        )
        result.close()


async def _create_graph_ahead_learning(
    board_id: str,
    *,
    node_id: str,
    title: str,
    source_ref: str = "",
) -> None:
    await run_blocking_graph_io(
        lambda: _create_graph_ahead_learning_sync(
            board_id,
            node_id=node_id,
            title=title,
            source_ref=source_ref,
        ),
        task_name="tests.cognitive_source_commit.create_graph_ahead_learning",
    )


async def test_commit_appends_durable_record_before_success(
    cogsrc_tempdir, monkeypatch
):
    from okto_pulse.community.adapters.sqlalchemy_models import KGCognitiveSource

    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    store = _require_ambient_community_store(session_factory)
    await _ensure_community_tables()
    register_cognitive_source_store(store)

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


async def test_commit_uses_ambient_uow_and_batches_on_event_loop(
    cogsrc_tempdir, monkeypatch
):
    from okto_pulse.core.kg.primitives import reset_commit_health_cache_for_tests

    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    capture = _CapturingBatchStore()
    register_cognitive_source_store(capture)
    reset_commit_health_cache_for_tests(board_id)
    event_loop_thread = threading.get_ident()

    commit = await _drive_learning_session(
        session_factory,
        board_id,
        "[MKG-A P1] no relational write under graph writer",
    )

    assert commit.nodes_added >= 1
    assert capture.append_many_calls == 1
    assert len(capture.contexts) == 1
    assert capture.thread_id == event_loop_thread
    assert len(capture.records) == 1
    assert capture.records[0].source_revision == 0
    assert len(capture.records[0].record_fingerprint) == 64


async def test_nc8_reuse_derives_revision_from_post_update_attestation(
    cogsrc_tempdir, monkeypatch
):
    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    capture = _CapturingBatchStore()
    register_cognitive_source_store(capture)
    source_ref = "spec:cognitive-revision-source"

    await _drive_learning_session(
        session_factory,
        board_id,
        "[MKG-A revision] initial",
        source_artifact_ref=source_ref,
    )
    assert capture.records[0].source_revision == 0

    await _drive_learning_session(
        session_factory,
        board_id,
        "[MKG-A revision] initial",
        source_artifact_ref=source_ref,
    )

    assert capture.append_many_calls == 2
    assert len(capture.records) == 1
    assert capture.records[0].generation == 0
    assert capture.records[0].source_revision == 1
    assert capture.records[0].payload["attestation_count"] == 2


async def test_missing_store_rejects_cognitive_commit_before_graph_write(
    cogsrc_tempdir, monkeypatch
):
    from okto_pulse.core.kg import primitives

    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    reset_cognitive_source_store_for_tests()
    real_run_graph_io = primitives._run_graph_io
    graph_commit_calls = 0

    async def _spy_graph_io(operation, *args, **kwargs):
        nonlocal graph_commit_calls
        if operation is primitives._do_graph_commit:
            graph_commit_calls += 1
        return await real_run_graph_io(operation, *args, **kwargs)

    monkeypatch.setattr(primitives, "_run_graph_io", _spy_graph_io)

    with pytest.raises(KGPrimitiveError) as excinfo:
        await _drive_learning_session(
            session_factory,
            board_id,
            "[MKG-A revision] missing store preflight",
        )

    assert excinfo.value.code == "kg_cognitive_source_unavailable"
    assert excinfo.value.details["failure_reason"] == (
        "cognitive_source_store_absent"
    )
    assert graph_commit_calls == 0
    assert await _count_learnings(board_id) == 0


async def test_missing_context_capability_rejects_before_graph_write(
    cogsrc_tempdir, monkeypatch
):
    from okto_pulse.core.kg import primitives

    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    register_cognitive_source_store(_LegacyStoreWithoutContextAppend())
    real_run_graph_io = primitives._run_graph_io
    graph_commit_calls = 0

    async def _spy_graph_io(operation, *args, **kwargs):
        nonlocal graph_commit_calls
        if operation is primitives._do_graph_commit:
            graph_commit_calls += 1
        return await real_run_graph_io(operation, *args, **kwargs)

    monkeypatch.setattr(primitives, "_run_graph_io", _spy_graph_io)

    with pytest.raises(KGPrimitiveError) as excinfo:
        await _drive_learning_session(
            session_factory,
            board_id,
            "[MKG-A P1] legacy store capability preflight",
        )

    assert excinfo.value.code == "kg_cognitive_source_unavailable"
    assert excinfo.value.details["failure_reason"] == (
        "cognitive_source_context_append_unsupported"
    )
    assert graph_commit_calls == 0
    assert await _count_learnings(board_id) == 0


async def test_store_outage_aborts_commit_fail_closed_then_retry_works(
    cogsrc_tempdir, monkeypatch
):
    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    await _ensure_community_tables()
    register_cognitive_source_store(_BrokenStore())

    title = "[MKG-A C4] outage learning"
    with pytest.raises(KGPrimitiveError) as excinfo:
        await _drive_learning_session(session_factory, board_id, title)
    assert excinfo.value.code == "kg_cognitive_source_unavailable"

    # Fail-closed: NO Learning node landed in the graph (S3/AC3 —
    # the graph is never ahead of the durable source).
    assert await _count_learnings(board_id) == 0

    # Retry after recovery: register the healthy store and rerun.
    register_cognitive_source_store(_require_ambient_community_store(session_factory))
    commit = await _drive_learning_session(session_factory, board_id, title)
    assert commit.nodes_added >= 1
    assert await _count_learnings(board_id) == 1


async def test_unexpected_store_failure_keeps_stable_error_and_compensates(
    cogsrc_tempdir, monkeypatch
):
    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    register_cognitive_source_store(_UnexpectedBrokenStore())

    with pytest.raises(KGPrimitiveError) as excinfo:
        await _drive_learning_session(
            session_factory,
            board_id,
            "[MKG-A P1] unexpected source adapter failure",
        )

    assert excinfo.value.code == "kg_cognitive_source_unavailable"
    assert excinfo.value.details["failure_reason"] == (
        "cognitive_source_append_unexpected"
    )
    assert await _count_learnings(board_id) == 0


async def test_explicit_update_repairs_missing_durable_record(
    cogsrc_tempdir, monkeypatch
):
    """The in-place UPDATE branch must enqueue its post-update snapshot."""

    from sqlalchemy import delete

    from okto_pulse.community.adapters.sqlalchemy_models import KGCognitiveSource
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

    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    delegate = _require_ambient_community_store(session_factory)
    await _ensure_community_tables()

    class _CapturingDelegate:
        def __init__(self):
            self.records: tuple[CognitiveSourceRecord, ...] = ()

        async def append(self, record: CognitiveSourceRecord) -> str:
            return (await self.append_many((record,)))[0]

        async def append_many(
            self, records: tuple[CognitiveSourceRecord, ...]
        ) -> tuple[str, ...]:
            self.records = records
            return await delegate.append_many(records)

        async def append_many_in_context(
            self,
            context: object,
            records: tuple[CognitiveSourceRecord, ...],
        ) -> tuple[str, ...]:
            self.records = records
            return await delegate.append_many_in_context(context, records)

        async def enumerate(self, requested_board_id: str):
            return await delegate.enumerate(requested_board_id)

    capture = _CapturingDelegate()
    register_cognitive_source_store(capture)

    original_title = "[MKG-A P1] explicit update predecessor"
    target_id = mint_node_id(
        board_id,
        "Learning",
        derive_natural_key("", "Learning", original_title),
        0,
    )
    await _drive_learning_session(session_factory, board_id, original_title)

    # Model a recoverable graph-ahead image: the graph node exists, but its
    # immutable generation-zero source row is absent.
    async with session_factory() as db:
        await db.execute(
            delete(KGCognitiveSource).where(
                KGCognitiveSource.node_id == target_id,
                KGCognitiveSource.generation == 0,
            )
        )
        await db.commit()

    root = NodeCandidate(
        candidate_id="mkga_p1_update_root",
        node_type=KGNodeType.ENTITY,
        title="MKG-A C4 technical root",
        content="Allowlisted deterministic source root.",
        source_artifact_ref="tech_entities.yml",
        source_confidence=1.0,
    )
    update = NodeCandidate(
        candidate_id="mkga_p1_explicit_update",
        node_type=KGNodeType.LEARNING,
        title="[MKG-A P1] explicit update successor content",
        content="updated lesson body",
        justification="explicit UPDATE source coverage",
        source_confidence=0.9,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=str(uuid.uuid4()),
            raw_content="MKG-A P1 explicit UPDATE durable source",
            deterministic_candidates=[root, update],
        ),
        agent_id="system:layer1_worker",
        db=None,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="mkga_p1_update_belongs",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id=update.candidate_id,
                to_candidate_id=root.candidate_id,
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
        candidate_id=update.candidate_id,
        operation=ReconciliationOperation.UPDATE,
        target_node_id=target_id,
        confidence=0.9,
        reason="test forces explicit cognitive UPDATE",
    )
    async with session_factory() as db:
        result = await commit_consolidation(
            CommitConsolidationRequest(
                session_id=begin.session_id,
                summary_text="explicit UPDATE source snapshot",
                agent_overrides={update.candidate_id: override},
            ),
            agent_id="system:layer1_worker",
            db=db,
        )
        await db.commit()

    assert result.nodes_updated == 1
    async with session_factory() as db:
        repaired = (
            await db.execute(
                select(KGCognitiveSource).where(
                    KGCognitiveSource.node_id == target_id,
                    KGCognitiveSource.generation == 0,
                )
            )
        ).scalar_one()
    assert repaired.payload["title"] == update.title
    assert repaired.payload["content"] == "updated lesson body"
    assert repaired.payload["attestation_count"] == 2
    assert len(capture.records) == 1
    assert capture.records[0].generation == 0
    assert capture.records[0].source_revision == 1


async def test_explicit_supersede_replay_repairs_missing_durable_record(
    cogsrc_tempdir, monkeypatch
):
    """A graph-ahead replay must restore the MKG-A source ledger.

    This models interruption after Ladybug materialized the deterministic
    successor but before its durable append/ACK survived.  The explicit
    SUPERSEDE replay takes the ``existing_successor`` branch; success is valid
    only when that branch re-appends the generation-1 source record.
    """

    from sqlalchemy import delete

    from okto_pulse.community.adapters.sqlalchemy_models import KGCognitiveSource
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

    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    store = _require_ambient_community_store(session_factory)
    await _ensure_community_tables()
    register_cognitive_source_store(store)

    original_title = "[MKG-A C4] durable predecessor"
    successor_title = "[MKG-A C4] durable successor"
    await _drive_learning_session(session_factory, board_id, original_title)
    old_id = mint_node_id(
        board_id,
        "Learning",
        derive_natural_key("", "Learning", original_title),
        0,
    )
    successor_id = mint_node_id(
        board_id,
        "Learning",
        derive_natural_key("", "Learning", successor_title),
        1,
    )
    candidate_id = "mkga_c4_supersede_replay"
    candidate = NodeCandidate(
        candidate_id=candidate_id,
        node_type=KGNodeType.LEARNING,
        title=successor_title,
        content="successor lesson body",
        justification="observed in replay-repair test",
        source_confidence=0.9,
    )
    root_candidate = NodeCandidate(
        candidate_id="mkga_c4_supersede_root",
        node_type=KGNodeType.ENTITY,
        title="MKG-A C4 technical root",
        content="Allowlisted deterministic source root.",
        source_artifact_ref="tech_entities.yml",
        source_confidence=1.0,
    )

    async def _force_supersede(summary: str):
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=str(uuid.uuid4()),
                raw_content=summary,
                deterministic_candidates=[root_candidate, candidate],
            ),
            agent_id="system:layer1_worker",
            db=None,
        )
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=EdgeCandidate(
                    candidate_id="mkga_c4_supersede_belongs_to_root",
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
            operation=ReconciliationOperation.SUPERSEDE,
            target_node_id=old_id,
            confidence=0.9,
            reason="test forces replayable cognitive SUPERSEDE",
        )
        async with session_factory() as db:
            result = await commit_consolidation(
                CommitConsolidationRequest(
                    session_id=begin.session_id,
                    summary_text=summary,
                    agent_overrides={candidate_id: override},
                ),
                agent_id="system:layer1_worker",
                db=db,
            )
            await db.commit()
            return result

    first = await _force_supersede("first cognitive supersede")
    assert first.nodes_superseded == 1
    assert await _count_learnings(board_id) == 2

    # Test-only crash image: graph successor remains, durable generation-1
    # record did not survive.
    async with session_factory() as db:
        await db.execute(
            delete(KGCognitiveSource).where(
                KGCognitiveSource.node_id == successor_id,
                KGCognitiveSource.generation == 1,
            )
        )
        await db.commit()
    async with session_factory() as db:
        missing = (
            await db.execute(
                select(KGCognitiveSource).where(
                    KGCognitiveSource.node_id == successor_id,
                    KGCognitiveSource.generation == 1,
                )
            )
        ).scalar_one_or_none()
    assert missing is None

    replay = await _force_supersede("replayed cognitive supersede")
    assert any(
        item["operation"] == "MERGE_SUPERSEDE_BY_DETERMINISTIC_ID"
        and item["reused_node_id"] == successor_id
        for item in replay.merge_audit_items
    )
    assert await _count_learnings(board_id) == 2

    async with session_factory() as db:
        repaired = (
            await db.execute(
                select(KGCognitiveSource).where(
                    KGCognitiveSource.node_id == successor_id,
                    KGCognitiveSource.generation == 1,
                )
            )
        ).scalar_one()
    assert repaired.board_id == board_id
    assert repaired.node_type == "Learning"
    assert repaired.payload["title"] == successor_title
    assert repaired.payload["attestation_count"] == 2


async def test_fresh_identity_replay_repairs_missing_generation_zero_record(
    cogsrc_tempdir, monkeypatch
):
    """The deterministic CREATE guard must heal a graph-ahead gen0 node."""

    from okto_pulse.community.adapters.sqlalchemy_models import KGCognitiveSource
    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    store = _require_ambient_community_store(session_factory)
    await _ensure_community_tables()
    register_cognitive_source_store(store)

    title = "[MKG-A C4] graph-ahead generation zero"
    node_id = mint_node_id(
        board_id,
        "Learning",
        derive_natural_key("", "Learning", title),
        0,
    )
    await _create_graph_ahead_learning(
        board_id,
        node_id=node_id,
        title=title,
    )

    async with session_factory() as db:
        missing = (
            await db.execute(
                select(KGCognitiveSource).where(
                    KGCognitiveSource.node_id == node_id,
                    KGCognitiveSource.generation == 0,
                )
            )
        ).scalar_one_or_none()
    assert missing is None

    replay = await _drive_learning_session(session_factory, board_id, title)
    assert any(
        item["operation"] == "MERGE_BY_DETERMINISTIC_ID"
        and item["reused_node_id"] == node_id
        for item in replay.merge_audit_items
    )
    assert await _count_learnings(board_id) == 1

    async with session_factory() as db:
        repaired = (
            await db.execute(
                select(KGCognitiveSource).where(
                    KGCognitiveSource.node_id == node_id,
                    KGCognitiveSource.generation == 0,
                )
            )
        ).scalar_one()
    assert repaired.board_id == board_id
    assert repaired.node_type == "Learning"
    assert repaired.payload["title"] == title
    assert repaired.payload["attestation_count"] == 2


async def test_nc8_reuse_repairs_missing_generation_zero_record(
    cogsrc_tempdir, monkeypatch
):
    """NC-8 source-ref reuse must heal the same graph-ahead condition."""

    from okto_pulse.community.adapters.sqlalchemy_models import KGCognitiveSource
    session_factory, board_id, _spec_id = await _bootstrap_test_board(monkeypatch)
    store = _require_ambient_community_store(session_factory)
    await _ensure_community_tables()
    register_cognitive_source_store(store)

    title = "[MKG-A C4] graph-ahead NC-8 reuse"
    source_ref = "spec:mkga-c4-graph-ahead-nc8"
    node_id = mint_node_id(
        board_id,
        "Learning",
        derive_natural_key(source_ref, "Learning", title),
        0,
    )
    await _create_graph_ahead_learning(
        board_id,
        node_id=node_id,
        title=title,
        source_ref=source_ref,
    )

    replay = await _drive_learning_session(
        session_factory,
        board_id,
        title,
        source_artifact_ref=source_ref,
    )
    assert any(
        item["operation"] == "MERGE"
        and item["reused_node_id"] == node_id
        for item in replay.merge_audit_items
    )
    assert await _count_learnings(board_id) == 1

    async with session_factory() as db:
        repaired = (
            await db.execute(
                select(KGCognitiveSource).where(
                    KGCognitiveSource.node_id == node_id,
                    KGCognitiveSource.generation == 0,
                )
            )
        ).scalar_one()
    assert repaired.board_id == board_id
    assert repaired.node_type == "Learning"
    assert repaired.generation == 0
    assert repaired.payload["title"] == title
    assert repaired.payload["source_artifact_ref"] == source_ref
    assert repaired.payload["generation"] == 0
    assert "embedding" in repaired.payload
