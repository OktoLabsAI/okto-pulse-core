"""RKG-03 — REAL candidate→consumer→graph.lbug→queryable integration.

The unit tests use a fake persister. This test drives the REAL
``ConsolidationPipelinePersister`` through begin→add_node→propose→commit against
a live board graph and proves the cognitive node is actually persisted +
queryable (FR2/BR2) — and idempotent on replay (AC1). This is the
"candidate reaches graph.lbug" proof.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg import cognitive_closeout_production as ccp
from okto_pulse.core.kg.interfaces.cognitive_pending_work import (
    CognitivePendingRecordRef,
)
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.primitives import _apply_graph_node_create
from memory_rebuild_audit_storage import (
    InMemoryCognitivePendingWorkProvider,
)
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
from okto_pulse.core.kg.workers.cognitive_closeout import CognitiveCloseoutWorker
from sqlalchemy_test_models import Board, Spec

ANALYSIS = (
    "## Analysis\n"
    "We considered using Redis instead of Postgres for the cache layer.\n"
)


@pytest.fixture(autouse=True)
def _require_real_community_graph(_kg_registry_test_fakes):
    from kg_registry_testing import configure_real_graph_test_kg_registry

    configure_real_graph_test_kg_registry()


@pytest.fixture(autouse=True)
async def _seed_relational_board(db_factory, board_id):
    """The real persister health guard requires both graph and SQL authority."""

    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="rkg03", owner_id="rkg03-owner"))
            await db.commit()


async def _seed_node(
    board_id: str,
    node_type: str,
    source_ref: str,
    *,
    node_id: str | None = None,
    graph_layer: str = "canonical",
) -> str:
    """Seed a canonical node (Entity provenance root / Decision / Bug) directly."""
    nid = node_id or f"{node_type.lower()}_seed_{uuid.uuid4().hex[:12]}"

    def _seed() -> None:
        from kg_schema_testing import open_board_connection
        from okto_pulse.core.kg.transaction import TransactionOrchestrator

        with open_board_connection(board_id) as (_db, kconn):
            orch = TransactionOrchestrator(
                graph_scope=kconn,
                session_id=f"seed_{uuid.uuid4().hex[:8]}",
                board_id=board_id,
            )
            _apply_graph_node_create(
                orch,
                node_type,
                nid,
                {
                    "title": f"Seed {node_type}",
                    "content": "",
                    "context": "",
                    "justification": "",
                    "source_artifact_ref": source_ref,
                    "created_at": "2026-06-08T00:00:00+00:00",
                    "created_by_agent": "test",
                    "source_confidence": 1.0,
                    "relevance_score": 0.5,
                    "query_hits": 0,
                    "last_queried_at": None,
                    "priority_boost": 0.0,
                    "human_curated": False,
                    "embedding": [0.0] * 384,
                    "graph_layer": graph_layer,
                    "maturity_status": "canonical_eligible",
                },
            )

    await run_blocking_graph_io(
        _seed,
        task_name=f"test-rkg03-seed-{board_id}-{node_type}-{nid}",
    )
    return nid


async def _seed_entity_root(board_id: str, source_ref: str) -> str:
    return await _seed_node(board_id, "Entity", source_ref)


async def _count_nodes_by_source_ref(
    board_id: str,
    node_type: str,
    source_ref: str,
) -> int:
    return await run_blocking_graph_io(
        lambda: ccp._count_nodes_by_source_ref(board_id, node_type, source_ref),
        task_name=f"test-rkg03-count-{board_id}-{node_type}",
    )


async def _count_relates_to_alternative(board_id: str, alt_ref: str) -> int:
    def _read() -> int:
        from kg_schema_testing import open_board_connection

        with open_board_connection(board_id) as (_db, kconn):
            res = kconn.execute(
                "MATCH (d:Decision)-[:relates_to]->(a:Alternative) "
                "WHERE a.source_artifact_ref = $ref RETURN count(*)",
                {"ref": alt_ref},
            )
            try:
                if res.has_next():
                    return int(res.get_next()[0] or 0)
            finally:
                try:
                    res.close()
                except Exception:
                    pass
        return 0

    return await run_blocking_graph_io(
        _read,
        task_name=f"test-rkg03-count-relates-to-{board_id}",
    )


@pytest.mark.asyncio
async def test_real_pipeline_persists_alternative_and_is_queryable(
    board_id, agent_id, db_factory, board_handle
):
    spec_id = f"spec-{uuid.uuid4()}"
    spec_ref = f"spec:{spec_id}"
    await _seed_entity_root(board_id, spec_ref)

    persister = ccp.ConsolidationPipelinePersister(db_factory, agent_id=agent_id)
    res = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="spec", artifact_ref=spec_ref,
        spec_context=ANALYSIS, persister=persister,
    )

    assert res.outcome == "persisted", res.detail
    assert res.persisted_refs
    alt_ref = res.persisted_refs[0]
    # THE PROOF: the candidate actually reached graph.lbug and is queryable.
    assert await _count_nodes_by_source_ref(
        board_id, "Alternative", alt_ref
    ) == 1

    # AC1 idempotent replay: a second closeout persists nothing new (no duplicate).
    res2 = await ccp.run_cognitive_closeout(
        board_id=board_id, artifact_type="spec", artifact_ref=spec_ref,
        spec_context=ANALYSIS, persister=persister,
    )
    assert res2.outcome == "persisted"
    assert res2.skipped_existing_refs  # recognised as already persisted
    assert await _count_nodes_by_source_ref(
        board_id, "Alternative", alt_ref
    ) == 1


# ---------------------------------------------------------------------------
# Ledger-backed worker (#1 codex) — handler opens pending, worker drains it.
# ---------------------------------------------------------------------------


def test_handler_opens_pending_without_graph_write(board_id, board_handle, tmp_path):
    """#1: the transition opens DURABLE pending work in the ledger and writes
    NOTHING to the graph (safe inside the event drain)."""
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    gen = "gen-open"
    spec_ref = f"spec:{uuid.uuid4()}"
    content_hash = "a" * 64
    n = ccp.open_cognitive_closeout_pending(
        store=store, board_id=board_id, kg_generation_id=gen,
        source_ref=spec_ref, artifact_type="spec", content_hash=content_hash)
    assert n == 1
    items = store.list_items(board_id, gen)
    item = next(i for i in items if i.source_ref == spec_ref)
    assert item.status == "pending"
    assert item.content_hash == content_hash
    # no graph mutation happened.
    assert ccp._count_nodes_by_source_ref(board_id, "Alternative", spec_ref) == 0


@pytest.mark.asyncio
async def test_worker_drains_spec_pending_to_alternative_relates_to_and_ledger(
    board_id, agent_id, db_factory, board_handle, tmp_path
):
    spec_id = f"spec-{uuid.uuid4()}"
    spec_ref = f"spec:{spec_id}"
    await _seed_entity_root(board_id, spec_ref)
    decision_id = await _seed_node(
        board_id,
        "Decision",
        f"{spec_ref}:decision:x",
    )
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    gen = "gen-rkg03w"
    ccp.open_cognitive_closeout_pending(
        store=store, board_id=board_id, kg_generation_id=gen,
        source_ref=spec_ref, artifact_type="spec")

    async def _loader(_bid, _item):
        return {"spec_context": ANALYSIS, "decision_ref": decision_id}

    persister = ccp.ConsolidationPipelinePersister(db_factory, agent_id=agent_id)
    results = await ccp.drain_cognitive_closeout_pending(
        db_factory, board_id, input_loader=_loader, store=store,
        persister=persister, agent_id=agent_id, kg_generation_id=gen)

    assert results and results[0].outcome == "persisted", results
    alt_ref = results[0].persisted_refs[0]
    # candidate -> graph.lbug -> queryable WITH the relates_to edge (AC1).
    assert await _count_nodes_by_source_ref(
        board_id, "Alternative", alt_ref
    ) == 1
    assert await _count_relates_to_alternative(board_id, alt_ref) == 1
    # the SAME ledger advanced to consolidated.
    item = next(i for i in store.list_items(board_id, gen) if i.source_ref == spec_ref)
    assert item.status == "consolidated"


@pytest.mark.asyncio
async def test_worker_persist_failure_marks_ledger_failed(
    board_id, db_factory, board_handle, tmp_path
):
    spec_ref = f"spec:{uuid.uuid4()}"
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    gen = "gen-fail"
    ccp.open_cognitive_closeout_pending(
        store=store, board_id=board_id, kg_generation_id=gen,
        source_ref=spec_ref, artifact_type="spec")

    class _FailPersister:
        def already_persisted(self, *_a):
            return False

        async def persist(self, *_a):
            return False  # persist never confirms queryable

    async def _loader(_bid, _item):
        return {"spec_context": ANALYSIS}

    results = await ccp.drain_cognitive_closeout_pending(
        db_factory, board_id, input_loader=_loader, store=store,
        persister=_FailPersister(), kg_generation_id=gen)

    assert results[0].outcome == "extractor_triggered_but_not_persisted"
    item = next(i for i in store.list_items(board_id, gen) if i.source_ref == spec_ref)
    assert item.status == "failed"
    # no partial graph: nothing was written.
    assert await _count_nodes_by_source_ref(
        board_id,
        "Alternative",
        f"{spec_ref}:alternative:",
    ) == 0


async def _count_validates_bug(board_id: str, learning_ref: str) -> int:
    def _read() -> int:
        from kg_schema_testing import open_board_connection

        with open_board_connection(board_id) as (_db, kconn):
            res = kconn.execute(
                "MATCH (l:Learning)-[:validates]->(b:Bug) "
                "WHERE l.source_artifact_ref = $ref RETURN count(*)",
                {"ref": learning_ref},
            )
            try:
                if res.has_next():
                    return int(res.get_next()[0] or 0)
            finally:
                try:
                    res.close()
                except Exception:
                    pass
        return 0

    return await run_blocking_graph_io(
        _read,
        task_name=f"test-rkg03-count-validates-{board_id}",
    )


@pytest.mark.asyncio
async def test_worker_drains_bug_pending_to_learning_validates_canonical_bug(
    board_id, agent_id, db_factory, board_handle, tmp_path
):
    bug_uuid = str(uuid.uuid4())
    bug_ref = f"bug:{bug_uuid}"
    await _seed_node(
        board_id,
        "Bug",
        bug_ref,
        graph_layer="canonical",
    )  # canonical Bug
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    gen = "gen-bug"
    ccp.open_cognitive_closeout_pending(
        store=store, board_id=board_id, kg_generation_id=gen,
        source_ref=bug_ref, artifact_type="bug")

    class _Summ:
        def summarise(self, *, bug_title, action_plan, context=None):
            return "Guard encoding before regex", "Normalise NFC first."

    async def _loader(_bid, _item):
        return {
            "bug_card_id": bug_uuid, "bug_title": "Regex misfires",
            "bug_action_plan": "Repro; root cause missing NFC; fixed + added a regression test.",
            "llm_config": {"provider": "openai"}, "summariser": _Summ(),
            "bug_probe": (lambda u: u == bug_uuid),
        }

    persister = ccp.ConsolidationPipelinePersister(db_factory, agent_id=agent_id)
    results = await ccp.drain_cognitive_closeout_pending(
        db_factory, board_id, input_loader=_loader, store=store,
        persister=persister, agent_id=agent_id, kg_generation_id=gen)

    assert results and results[0].outcome == "persisted", results
    learning_ref = f"bug:{bug_uuid}"
    assert await _count_nodes_by_source_ref(
        board_id, "Learning", learning_ref
    ) == 1
    # validates -> canonical Bug queryable (AC2).
    assert await _count_validates_bug(board_id, learning_ref) == 1
    bug_item = next(i for i in store.list_items(board_id, gen) if i.artifact_type == "bug")
    assert bug_item.status == "consolidated"


# ---------------------------------------------------------------------------
# #6 failure matrix — a failure in ANY pipeline step (real persister) fails
# closed to "not persisted" with no partial graph.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "step",
    ["begin_consolidation", "add_node_candidate", "add_edge_candidate",
     "propose_reconciliation", "commit_consolidation"],
)
@pytest.mark.asyncio
async def test_persister_failure_at_each_step_no_partial_graph(
    step, board_id, agent_id, db_factory, board_handle, monkeypatch
):
    import okto_pulse.core.kg.primitives as prim

    async def _boom(*_a, **_k):
        raise RuntimeError(f"{step} boom")

    monkeypatch.setattr(prim, step, _boom)
    persister = ccp.ConsolidationPipelinePersister(db_factory, agent_id=agent_id)
    alt_ref = f"spec:{uuid.uuid4()}:alternative:{uuid.uuid4().hex[:8]}"
    candidate = ccp.CloseoutCandidate(
        node_type="Alternative", title="t", content="c", source_artifact_ref=alt_ref,
        edges=(ccp.CloseoutEdge(edge_type="relates_to", to_ref="decision_x", incoming=True),),
    )
    ok = await persister.persist(board_id, "spec", candidate)
    assert ok is False  # any step failure -> not persisted, never raised
    # no partial graph was written.
    assert await _count_nodes_by_source_ref(
        board_id, "Alternative", alt_ref
    ) == 0


# ---------------------------------------------------------------------------
# The dedicated worker end-to-end: REAL input loader (loads Spec from SQL) +
# LEDGER scan as the work source (codex final wiring), not SQL board scan.
# ---------------------------------------------------------------------------


async def _count_alternatives_relates_to_for_spec(
    board_id: str,
    spec_id: str,
) -> int:
    def _read() -> int:
        from kg_schema_testing import open_board_connection

        with open_board_connection(board_id) as (_db, kconn):
            res = kconn.execute(
                "MATCH (d:Decision)-[:relates_to]->(a:Alternative) "
                "WHERE a.source_artifact_ref STARTS WITH $prefix RETURN count(*)",
                {"prefix": f"spec:{spec_id}:alternative:"},
            )
            try:
                if res.has_next():
                    return int(res.get_next()[0] or 0)
            finally:
                try:
                    res.close()
                except Exception:
                    pass
        return 0

    return await run_blocking_graph_io(
        _read,
        task_name=f"test-rkg03-count-spec-alternatives-{board_id}",
    )


@pytest.mark.asyncio
async def test_cognitive_closeout_worker_real_loader_and_ledger_scan(
    board_id, agent_id, db_factory, board_handle, tmp_path
):
    spec_id = str(uuid.uuid4())
    spec_ref = f"spec:{spec_id}"
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="w", owner_id="o"))
        db.add(Spec(id=spec_id, board_id=board_id, title="s", created_by="u", context=ANALYSIS))
        await db.commit()
    await _seed_entity_root(board_id, spec_ref)
    await _seed_node(board_id, "Decision", f"{spec_ref}:decision:x")

    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    # Handler-side open (default generation resolution); worker scans the ledger.
    ccp.open_cognitive_closeout_pending(
        board_id=board_id, source_ref=spec_ref, artifact_type="spec", store=store)

    gen = store.latest_generation(board_id)
    assert gen is not None
    pending_provider = InMemoryCognitivePendingWorkProvider(
        [CognitivePendingRecordRef(board_id=board_id, kg_generation_id=gen)]
    )
    worker = CognitiveCloseoutWorker(
        db_factory,
        store=store,
        pending_work_provider=pending_provider,
    )
    # drain_once consumes ledger refs from the provider (not SQL or directory
    # scanning) and uses the REAL loader, which loads Spec.context from SQL +
    # resolves the related Decision.
    processed = await worker.drain_once()

    assert processed >= 1
    # Alternative persisted from the SQL-loaded spec context, with relates_to edge.
    assert await _count_alternatives_relates_to_for_spec(board_id, spec_id) == 1


# ---------------------------------------------------------------------------
# FR1/AC1 canonical trigger — SpecMoved(done) opens spec pending in the REAL
# handler path, independent of any card; ledger-only (no graph write in drain).
# ---------------------------------------------------------------------------


def _registry_ledger_store() -> CognitiveConsolidationItemStore:
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    return CognitiveConsolidationItemStore(
        artifact_store=get_kg_registry().require_rebuild_audit_artifact_store()
    )


def _pending_spec_refs(board_id: str) -> list[str]:
    store = _registry_ledger_store()
    gen = store.latest_generation(board_id)
    if not gen:
        return []
    return [i.source_ref for i in store.list_items(board_id, gen) if i.status == "pending"]


@pytest.mark.asyncio
async def test_spec_moved_done_opens_pending_without_card(board_id, board_handle):
    from okto_pulse.core.events.handlers.cognitive_extraction import (
        CognitiveExtractionHandler,
    )
    from okto_pulse.core.events.types import SpecMoved

    spec_id = str(uuid.uuid4())
    handler = CognitiveExtractionHandler()
    # No card involved — the spec itself reaches done.
    event = SpecMoved(board_id=board_id, spec_id=spec_id, from_status="review", to_status="done")
    await handler.handle(event, None)

    assert f"spec:{spec_id}" in _pending_spec_refs(board_id)
    # Ledger-only: nothing written to the graph inside the drain.
    assert await _count_nodes_by_source_ref(
        board_id,
        "Alternative",
        f"spec:{spec_id}",
    ) == 0


@pytest.mark.asyncio
async def test_spec_moved_non_done_opens_nothing(board_id, board_handle):
    from okto_pulse.core.events.handlers.cognitive_extraction import (
        CognitiveExtractionHandler,
    )
    from okto_pulse.core.events.types import SpecMoved

    spec_id = str(uuid.uuid4())
    handler = CognitiveExtractionHandler()
    event = SpecMoved(board_id=board_id, spec_id=spec_id, from_status="draft", to_status="review")
    await handler.handle(event, None)

    assert f"spec:{spec_id}" not in _pending_spec_refs(board_id)
