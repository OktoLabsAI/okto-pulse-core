"""The explicit MCP cognitive owner must not hold SQLite across graph durability."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from threading import Event
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy import insert, select, text

from kg_registry_testing import configure_real_graph_test_kg_registry
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
from test_kg_cognitive_canonical_invariant import _seed_spec_root_and_decision, _get_decision_layer_maturity
from test_r01a_mcp_consolidation_uow import _MCPRegistryDouble, _begin_add_propose
from kg_schema_testing import bootstrap_board_graph
from okto_pulse.community.adapters.sqlalchemy_models import Board, ConsolidationAudit, KGCognitiveSource
from okto_pulse.community.adapters.sqlalchemy_audit_repo import CommunityAuditRepository
from okto_pulse.community.adapters.sqlalchemy_kg_cognitive_source import CommunitySqlAlchemyCognitiveSourceStore
from okto_pulse.core.mcp import kg_tools
from okto_pulse.core.kg import primitives
from okto_pulse.core.ports.kg_cognitive_source import register_cognitive_source_store


@pytest.fixture
def fu1_board():
    board = "board-cognitive-writer-" + uuid.uuid4().hex[:8]
    bootstrap_board_graph(board)
    return board


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_stage", [False, True])
async def test_native_mcp_commit_releases_sql_writer_before_durability(
    fu1_board, agent_id, db_factory, monkeypatch, fail_stage,
):
    configure_real_graph_test_kg_registry(audit_repo=CommunityAuditRepository(db_factory))
    register_cognitive_source_store(CommunitySqlAlchemyCognitiveSourceStore(db_factory))
    async with db_factory() as seed:
        await seed.execute(insert(Board).values(id=fu1_board, name="Cognitive transaction fixture",
                                               owner_id="owner", realm_id="local"))
        await seed.commit()

    async def healthy(*args, **kwargs):
        return "healthy"

    monkeypatch.setattr("okto_pulse.core.kg.primitives._resolve_commit_kg_health_state", healthy)
    spec_id = "spec-" + uuid.uuid4().hex[:8]
    ref = "spec:" + spec_id
    _, decision_id = _seed_spec_root_and_decision(fu1_board, ref)
    begin = await _begin_add_propose(fu1_board, agent_id, db_factory, spec_id,
                                     ref + ":decision:concurrent", decision_id)
    if fail_stage:
        original_stage = CommunityAuditRepository.stage_consolidation_records

        async def fail_after_staging(self, *args, **kwargs):
            await original_stage(self, *args, **kwargs)
            raise RuntimeError("injected_cognitive_audit_stage_failure")

        monkeypatch.setattr(CommunityAuditRepository, "stage_consolidation_records", fail_after_staging)
    entered, release = Event(), Event()

    class Lease:
        def ensure_durable(self, **kwargs):
            entered.set()
            assert release.wait(20), "test did not release the durability barrier"

        def ensure_owned(self, **kwargs):
            pass

    @contextmanager
    def guarded(*args, **kwargs):
        yield Lease()

    async def get_agent():
        return SimpleNamespace(id=agent_id)

    async def get_board_agent(board_id):
        assert board_id == fu1_board
        return SimpleNamespace(agent_id=agent_id, permissions=None)

    monkeypatch.setattr(kg_tools, "guarded_board_write", guarded)
    registry = _MCPRegistryDouble()
    kg_tools.register_kg_tools(registry, get_agent=get_agent,
        get_uow=lambda: SQLAlchemyUnitOfWorkFactory(db_factory), get_board_agent=get_board_agent)
    operation = asyncio.create_task(registry.tools["okto_pulse_kg_commit_consolidation"](
        session_id=begin.session_id, summary_text="Concurrent SQLite writer witness"))
    writer_error = None
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 30), timeout=35)
        async with db_factory() as other:
            await other.execute(text("PRAGMA busy_timeout=250"))
            try:
                await other.execute(insert(Board).values(id="concurrent-" + uuid.uuid4().hex,
                    name="Independent SQL writer", owner_id="owner", realm_id="local"))
                await other.commit()
            except Exception as exc:
                writer_error = exc
                await other.rollback()
    finally:
        release.set()
        if fail_stage:
            with pytest.raises(RuntimeError, match="injected_cognitive_audit_stage_failure"):
                await operation
        else:
            response = await operation
    assert writer_error is None, f"SQLite remained write-locked during graph durability: {writer_error}"
    if not fail_stage:
        assert '"error"' not in response, response
    async with db_factory() as verify:
        audits = (await verify.execute(select(ConsolidationAudit).where(
            ConsolidationAudit.session_id == begin.session_id))).scalars().all()
        sources = (await verify.execute(select(KGCognitiveSource).where(
            KGCognitiveSource.board_id == fu1_board))).scalars().all()
        assert bool(audits) is not fail_stage
        assert bool(sources) is not fail_stage
    if fail_stage:
        assert _get_decision_layer_maturity(fu1_board, ref + ":decision:concurrent") == (None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("case,code", [
    ("missing", "session_not_found"),
    ("other_owner", "session_ownership_mismatch"),
    ("released", "session_commit_not_pending"),
    ("duplicate", "session_commit_already_staged"),
    ("premature_finalize", "session_commit_not_staged"),
])
async def test_cognitive_settlement_rejects_unowned_or_unstaged_snapshots(monkeypatch, case, code):
    pending = primitives._PendingConsolidationCommit(
        request_payload={}, records=(), counters=None, cognitive_source_records=(),
        response=SimpleNamespace(), in_flight=case != "released",
        relational_staging_deferred=True, relational_staged=case == "duplicate",
    )
    session = SimpleNamespace(
        lock=asyncio.Lock(), pending_commit=pending,
        check_ownership=lambda actor: case != "other_owner",
    )

    async def get_session(session_id):
        return None if case == "missing" else session

    # There is deliberately no persistence or graph port: rejected settlement
    # must not perform any SQL, graph write, or terminal cleanup.
    registry = SimpleNamespace(require_session_store=lambda: SimpleNamespace(get=get_session))
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    with pytest.raises(primitives.KGPrimitiveError) as caught:
        if case == "premature_finalize":
            await primitives.finalize_deferred_consolidation("session", agent_id="owner")
        else:
            await primitives.stage_deferred_consolidation("session", agent_id="owner", db=object())
    assert caught.value.code == code
    assert session.pending_commit is pending
