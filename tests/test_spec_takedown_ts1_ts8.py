"""Executable acceptance proofs for governed-takedown scenarios TS1--TS8.

The scenarios in this module intentionally compose the current Core sources
with the concrete Community SQL adapters.  Queue-driven tests use an adapter
that exposes only explicitly selected entry ids to the worker; this keeps the
proof deterministic even when another test left unrelated pending work in the
shared test database.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, func, select

from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)
from global_graph_testing import (
    bootstrap_global_discovery,
    reset_global_discovery_runtime_for_tests,
)
from kg_schema_testing import open_board_connection
from okto_pulse.community.adapters.sqlalchemy_consolidation import (
    CommunitySqlAlchemyConsolidationPersistence,
)
from okto_pulse.community.adapters.sqlalchemy_delivery_ledger import (
    CommunitySqlAlchemyDeliveryLedger,
)
from okto_pulse.community.adapters.sqlalchemy_models import (
    ConsolidationQueue as CommunityConsolidationQueue,
)
from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.application.processors.consolidation import (
    ConsolidationProcessor,
)
from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
)
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.canonical_stale_reconciler import (
    ALL_NODE_TYPES,
    COGNITIVE_NODE_TYPES,
    _source_identity_from_ref,
)
from okto_pulse.core.kg.primitives import _apply_graph_node_create
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    MATURITY_CANONICAL_ELIGIBLE,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from okto_pulse.core.ports.consolidation import (
    get_consolidation_persistence_port,
    register_consolidation_persistence_port,
)
from okto_pulse.core.ports.delivery_ledger import (
    DeliveryState,
    get_delivery_ledger_port,
    register_delivery_ledger_port,
    reset_delivery_ledger_port_for_tests,
)
from okto_pulse.core.ports.reconcile_intent import (
    get_reconcile_intent_port,
    register_reconcile_intent_port,
)
from okto_pulse.core.ports.stale_sweep import (
    StaleSweepScheduleRequest,
    register_stale_sweep_port,
    reset_stale_sweep_port_for_tests,
)
from okto_pulse.core.ports.tombstone import (
    get_tombstone_port,
    register_tombstone_port,
)
from okto_pulse.core.services import main as services_main
from okto_pulse.core.services.canonical_debt_service import list_canonical_debt
from okto_pulse.core.services.main import (
    CardService,
    IdeationService,
    RefinementService,
    SpecService,
)
from r2_scenario_helpers import (
    USER_ID,
    commit_worker_result,
    first_canonical_node,
    insert_spec,
    new_board,
    node_layer,
    seed_done_spec_canonical,
    seed_canonical_cognitive,
    spec_dict,
)
from sqlalchemy_test_models import (
    ActivityLog,
    ArtifactDeletionTombstone,
    CanonicalDebt,
    Card,
    CardType,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    GlobalDiscoveryDeliveryLedger,
    GlobalUpdateOutbox,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
    Sprint,
)


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
    )


@pytest.fixture(scope="module", autouse=True)
def _global_discovery_graph():
    reset_global_discovery_runtime_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_discovery_runtime_for_tests()


class _InjectedUoWFailure(RuntimeError):
    pass


class _CrashAfterGraphCommit(BaseException):
    pass


class _TargetedConsolidationPersistence(CommunitySqlAlchemyConsolidationPersistence):
    """Production adapter whose worker inventory is scoped to chosen ids."""

    def __init__(self) -> None:
        self.target_entry_ids: set[str] = set()

    async def count_pending(self, context) -> int:
        if not self.target_entry_ids:
            return 0
        value = await context.scalar(
            select(func.count())
            .select_from(CommunityConsolidationQueue)
            .where(
                CommunityConsolidationQueue.id.in_(self.target_entry_ids),
                CommunityConsolidationQueue.status == "pending",
            )
        )
        return int(value or 0)

    async def list_claimed_board_ids(self, context) -> frozenset[str]:
        if not self.target_entry_ids:
            return frozenset()
        rows = (
            (
                await context.execute(
                    select(CommunityConsolidationQueue.board_id).where(
                        CommunityConsolidationQueue.id.in_(self.target_entry_ids),
                        CommunityConsolidationQueue.status == "claimed",
                    )
                )
            )
            .scalars()
            .all()
        )
        return frozenset(str(value) for value in rows)

    async def list_ready_pending(self, context, *, now):
        rows = await super().list_ready_pending(context, now=now)
        return tuple(row for row in rows if row.id in self.target_entry_ids)

    async def list_stale_claims(self, context, *, now, legacy_cutoff):
        rows = await super().list_stale_claims(
            context,
            now=now,
            legacy_cutoff=legacy_cutoff,
        )
        return tuple(row for row in rows if row.id in self.target_entry_ids)


@contextmanager
def _registered_targeted_adapters():
    previous_queue = get_consolidation_persistence_port()
    previous_tombstone = get_tombstone_port()
    previous_intent = get_reconcile_intent_port()
    try:
        previous_delivery = get_delivery_ledger_port()
    except RuntimeError:
        previous_delivery = None

    queue = _TargetedConsolidationPersistence()
    delivery = CommunitySqlAlchemyDeliveryLedger()
    register_consolidation_persistence_port(queue)
    register_tombstone_port(queue)
    register_reconcile_intent_port(queue)
    register_stale_sweep_port(queue)
    register_delivery_ledger_port(delivery)
    try:
        yield queue, delivery
    finally:
        register_consolidation_persistence_port(previous_queue)
        register_tombstone_port(previous_tombstone)
        register_reconcile_intent_port(previous_intent)
        reset_stale_sweep_port_for_tests()
        if previous_delivery is None:
            reset_delivery_ledger_port_for_tests()
        else:
            register_delivery_ledger_port(previous_delivery)


async def _seed_linked_card_and_legacy_work(
    db_factory,
    *,
    board_id: str,
    spec_id: str,
) -> str:
    card_id = f"card-ts1-{uuid.uuid4().hex[:12]}"
    async with db_factory() as session:
        session.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="TS1 linked card",
                status="done",
                priority="none",
                position=0,
                created_by=USER_ID,
                card_type=CardType.NORMAL,
                labels=[],
                test_scenario_ids=[],
                linked_test_task_ids=[],
            )
        )
        session.add_all(
            [
                ConsolidationQueue(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    work_kind="consolidate",
                    generation=0,
                    status="pending",
                ),
                ConsolidationDeadLetter(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    attempts=4,
                    errors=[],
                ),
                CanonicalDebt(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    source_ref=f"spec:{spec_id}",
                    content_hash="ts1-before-delete",
                    target_status="done",
                ),
            ]
        )
        await session.commit()
    return card_id


async def _assert_ts1_original_state(
    db_factory,
    *,
    board_id: str,
    spec_id: str,
    card_id: str,
    requirement_id: str,
) -> None:
    async with db_factory() as session:
        assert await session.get(Spec, spec_id) is not None
        card = await session.get(Card, card_id)
        assert card is not None and card.spec_id == spec_id
        legacy_count = await session.scalar(
            select(func.count())
            .select_from(ConsolidationQueue)
            .where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.artifact_type == "spec",
                ConsolidationQueue.artifact_id == spec_id,
                ConsolidationQueue.work_kind == "consolidate",
            )
        )
        intent_count = await session.scalar(
            select(func.count())
            .select_from(ConsolidationQueue)
            .where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.artifact_type == "spec",
                ConsolidationQueue.artifact_id == spec_id,
                ConsolidationQueue.work_kind == "stale_reconcile",
            )
        )
        tombstone_count = await session.scalar(
            select(func.count())
            .select_from(ArtifactDeletionTombstone)
            .where(
                ArtifactDeletionTombstone.board_id == board_id,
                ArtifactDeletionTombstone.artifact_type == "spec",
                ArtifactDeletionTombstone.artifact_id == spec_id,
            )
        )
        dlq_count = await session.scalar(
            select(func.count())
            .select_from(ConsolidationDeadLetter)
            .where(
                ConsolidationDeadLetter.board_id == board_id,
                ConsolidationDeadLetter.artifact_type == "spec",
                ConsolidationDeadLetter.artifact_id == spec_id,
            )
        )
        debt_count = await session.scalar(
            select(func.count())
            .select_from(CanonicalDebt)
            .where(
                CanonicalDebt.board_id == board_id,
                CanonicalDebt.artifact_type == "spec",
                CanonicalDebt.artifact_id == spec_id,
            )
        )
        activity_count = await session.scalar(
            select(func.count())
            .select_from(ActivityLog)
            .where(
                ActivityLog.board_id == board_id,
                ActivityLog.action == "spec_deleted",
            )
        )
    assert (legacy_count, intent_count, tombstone_count) == (1, 0, 0)
    assert (dlq_count, debt_count, activity_count) == (1, 1, 0)
    assert (
        await node_layer(
            board_id,
            "Requirement",
            requirement_id,
        )
        == "canonical"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_phase",
    (
        "after_unlink",
        "after_discard",
        "after_tombstone",
        "after_intent",
        "before_commit",
    ),
)
async def test_ts1_spec_done_rolls_back_at_each_uow_phase(
    db_factory,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    """TS1: every boundary restores SQL, queue, fence and graph exactly."""

    board_id = await new_board(db_factory, "ts1-spec")
    spec_id, _source_ref = await seed_done_spec_canonical(db_factory, board_id)
    requirement = await first_canonical_node(board_id, "Requirement")
    assert requirement is not None
    requirement_id, _ = requirement
    card_id = await _seed_linked_card_and_legacy_work(
        db_factory,
        board_id=board_id,
        spec_id=spec_id,
    )

    if failure_phase == "after_unlink":
        real_flush = services_main._application_flush

        async def _fail_after_unlink(context):
            await real_flush(context)
            raise _InjectedUoWFailure(failure_phase)

        monkeypatch.setattr(services_main, "_application_flush", _fail_after_unlink)
    elif failure_phase == "after_discard":
        persistence = get_consolidation_persistence_port()
        real_discard = persistence.discard_artifact_work

        async def _fail_after_discard(context, **kwargs):
            await real_discard(context, **kwargs)
            raise _InjectedUoWFailure(failure_phase)

        monkeypatch.setattr(
            persistence,
            "discard_artifact_work",
            _fail_after_discard,
        )
    elif failure_phase == "after_tombstone":
        tombstone_port = get_tombstone_port()
        real_advance = tombstone_port.advance_deletion_tombstone

        async def _fail_after_tombstone(context, request):
            await real_advance(context, request)
            raise _InjectedUoWFailure(failure_phase)

        monkeypatch.setattr(
            tombstone_port,
            "advance_deletion_tombstone",
            _fail_after_tombstone,
        )
    elif failure_phase == "after_intent":
        intent_port = get_reconcile_intent_port()
        real_persist = intent_port.persist_reconcile_intent

        async def _fail_after_intent(context, request):
            await real_persist(context, request)
            raise _InjectedUoWFailure(failure_phase)

        monkeypatch.setattr(
            intent_port,
            "persist_reconcile_intent",
            _fail_after_intent,
        )

    async with db_factory() as session:
        with pytest.raises(_InjectedUoWFailure, match=failure_phase):
            assert await SpecService(session).delete_spec(spec_id, USER_ID)
            if failure_phase == "before_commit":
                raise _InjectedUoWFailure(failure_phase)
        await session.rollback()

    await _assert_ts1_original_state(
        db_factory,
        board_id=board_id,
        spec_id=spec_id,
        card_id=card_id,
        requirement_id=requirement_id,
    )


@pytest.mark.asyncio
async def test_ts45_targeted_intent_crash_retry_is_order_independent(
    db_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TS4/TS5: target identity, graph crash, empty retry, one attempt:0."""

    board_id = await new_board(db_factory, "ts45-targeted")
    spec_id, _source_ref = await seed_done_spec_canonical(db_factory, board_id)
    requirement = await first_canonical_node(board_id, "Requirement")
    assert requirement is not None
    requirement_id, _ = requirement
    observed = []

    from okto_pulse.core.kg import canonical_stale_reconciler

    real_reconcile = canonical_stale_reconciler.reconcile_stale_canonical
    real_transfer = consolidation._transfer_stale_reconcile_ownership

    async def _observe(*args, **kwargs):
        result = await real_reconcile(*args, **kwargs)
        observed.append(result)
        return result

    async def _crash(*_args, **_kwargs):
        raise _CrashAfterGraphCommit("ts45-after-graph-commit")

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _observe,
    )

    with _registered_targeted_adapters() as (queue, _delivery):
        async with db_factory() as session:
            await session.execute(
                delete(GlobalUpdateOutbox).where(
                    GlobalUpdateOutbox.board_id == board_id
                )
            )
            assert await SpecService(session).delete_spec(spec_id, USER_ID)
            await session.commit()

        async with db_factory() as session:
            intent = (
                await session.execute(
                    select(ConsolidationQueue).where(
                        ConsolidationQueue.board_id == board_id,
                        ConsolidationQueue.artifact_type == "spec",
                        ConsolidationQueue.artifact_id == spec_id,
                        ConsolidationQueue.work_kind == "stale_reconcile",
                    )
                )
            ).scalar_one()
            intent_id = str(intent.id)
        queue.target_entry_ids.add(intent_id)

        processor = ConsolidationProcessor(db_factory, batch_size=1)
        monkeypatch.setattr(
            consolidation,
            "_transfer_stale_reconcile_ownership",
            _crash,
        )
        with pytest.raises(_CrashAfterGraphCommit, match="after-graph-commit"):
            await processor.process_batch()

        assert len(observed) == 1 and observed[0].demoted
        assert (
            await node_layer(
                board_id,
                "Requirement",
                requirement_id,
            )
            == "working"
        )

        async with db_factory() as session:
            claimed = await session.get(ConsolidationQueue, intent_id)
            assert claimed is not None and claimed.status == "claimed"
            claimed.claim_timeout_at = datetime(2000, 1, 1, tzinfo=timezone.utc)
            await session.commit()

        assert await processor.recover_stale_claims() == 1
        monkeypatch.setattr(
            consolidation,
            "_transfer_stale_reconcile_ownership",
            real_transfer,
        )
        assert await processor.process_batch() == 1

        assert len(observed) == 2
        assert observed[1].demoted == []
        async with db_factory() as session:
            assert await session.get(ConsolidationQueue, intent_id) is None
            ledgers = (
                (
                    await session.execute(
                        select(GlobalDiscoveryDeliveryLedger).where(
                            GlobalDiscoveryDeliveryLedger.board_id == board_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            outbox = (
                (
                    await session.execute(
                        select(GlobalUpdateOutbox).where(
                            GlobalUpdateOutbox.board_id == board_id
                        )
                    )
                )
                .scalars()
                .all()
            )

    assert len(ledgers) == len(outbox) == 1
    assert ledgers[0].state == DeliveryState.OUTBOX_PERSISTED.value
    assert ledgers[0].attempt == 0
    assert ledgers[0].attempt_event_key == outbox[0].event_id
    assert outbox[0].event_id.endswith(":attempt:0")


def _canonical_node_attrs(
    source_ref: str,
    *,
    title: str,
    writer: str,
) -> dict[str, object]:
    return {
        "title": title,
        "content": "",
        "context": "",
        "justification": "",
        "source_artifact_ref": source_ref,
        "created_at": "2026-07-21T00:00:00+00:00",
        "created_by_agent": writer,
        "source_confidence": 1.0,
        "relevance_score": 0.5,
        "query_hits": 0,
        "last_queried_at": None,
        "priority_boost": 0.0,
        "human_curated": False,
        "embedding": [0.0] * 384,
        "graph_layer": GRAPH_LAYER_CANONICAL,
        "maturity_status": MATURITY_CANONICAL_ELIGIBLE,
    }


async def _seed_deleted_bug_learning_graph(
    board_id: str,
    *,
    bug_id: str,
) -> tuple[str, str]:
    learning_id = f"ts6-learning-{uuid.uuid4().hex[:10]}"
    graph_bug_id = f"ts6-bug-{uuid.uuid4().hex[:10]}"

    def _write() -> None:
        with open_board_connection(board_id) as (_database, connection):
            orchestrator = TransactionOrchestrator(
                graph_scope=connection,
                session_id=f"ts6-seed-{uuid.uuid4().hex[:8]}",
                board_id=board_id,
            )
            _apply_graph_node_create(
                orchestrator,
                "Learning",
                learning_id,
                _canonical_node_attrs(
                    f"bug:{bug_id}",
                    title="TS6 retained learning",
                    writer="cognitive:analyst",
                ),
            )
            _apply_graph_node_create(
                orchestrator,
                "Bug",
                graph_bug_id,
                _canonical_node_attrs(
                    f"card:{bug_id}",
                    title="TS6 deterministic bug",
                    writer="system:deterministic",
                ),
            )
            orchestrator.create_edge(
                edge_type="validates",
                from_id=learning_id,
                to_id=graph_bug_id,
                attrs={"confidence": 1.0},
                from_type="Learning",
                to_type="Bug",
            )

    await run_blocking_graph_io(_write, task_name="tests.ts6.seed_bug_learning")
    return learning_id, graph_bug_id


def _node_state_sync(
    board_id: str,
    node_type: str,
    node_id: str,
) -> tuple[str, str] | None:
    with open_board_connection(board_id) as (_database, connection):
        result = connection.execute(
            f"MATCH (n:{node_type} {{id: $node_id}}) "
            "RETURN n.graph_layer, n.maturity_status",
            {"node_id": node_id},
        )
        if not result.has_next():
            return None
        row = result.get_next()
        return str(row[0]), str(row[1])


async def _node_state(
    board_id: str,
    node_type: str,
    node_id: str,
) -> tuple[str, str] | None:
    return await run_blocking_graph_io(
        lambda: _node_state_sync(board_id, node_type, node_id),
        task_name="tests.takedown.node_state",
    )


async def _intent_id(
    db_factory,
    *,
    board_id: str,
    artifact_type: str,
    artifact_id: str,
) -> str:
    async with db_factory() as session:
        row = (
            await session.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == artifact_type,
                    ConsolidationQueue.artifact_id == artifact_id,
                    ConsolidationQueue.work_kind == "stale_reconcile",
                )
            )
        ).scalar_one()
        return str(row.id)


@pytest.mark.asyncio
async def test_ts6_deleted_bug_preserves_learning_and_persists_visible_debt(
    db_factory,
) -> None:
    """TS6: real delete + worker keeps Learning and exposes source_absent debt."""

    board_id = await new_board(db_factory, "ts6-bug")
    bug_id = f"bug-ts6-{uuid.uuid4().hex[:12]}"
    async with db_factory() as session:
        session.add(
            Card(
                id=bug_id,
                board_id=board_id,
                title="TS6 deleted bug",
                status="done",
                priority="none",
                position=0,
                created_by=USER_ID,
                card_type=CardType.BUG,
                labels=[],
                test_scenario_ids=[],
                linked_test_task_ids=[],
            )
        )
        await session.commit()
    learning_id, graph_bug_id = await _seed_deleted_bug_learning_graph(
        board_id,
        bug_id=bug_id,
    )
    before_learning = await _node_state(board_id, "Learning", learning_id)
    assert before_learning == ("canonical", "canonical_eligible")
    assert await _node_state(board_id, "Bug", graph_bug_id) == (
        "canonical",
        "canonical_eligible",
    )

    with _registered_targeted_adapters() as (queue, _delivery):
        async with db_factory() as session:
            assert await CardService(session).delete_card(bug_id, USER_ID)
            await session.commit()
        intent_id = await _intent_id(
            db_factory,
            board_id=board_id,
            artifact_type="card",
            artifact_id=bug_id,
        )
        queue.target_entry_ids.add(intent_id)
        assert (
            await ConsolidationProcessor(db_factory, batch_size=1).process_batch() == 1
        )

    assert await _node_state(board_id, "Learning", learning_id) == before_learning
    assert await _node_state(board_id, "Bug", graph_bug_id) == (
        "working",
        "working_stale",
    )
    async with db_factory() as session:
        listed = await list_canonical_debt(session, board_id=board_id)
        tombstone = (
            await session.execute(
                select(ArtifactDeletionTombstone).where(
                    ArtifactDeletionTombstone.board_id == board_id,
                    ArtifactDeletionTombstone.artifact_type == "card",
                    ArtifactDeletionTombstone.artifact_id == bug_id,
                )
            )
        ).scalar_one()

    source_absent = [
        item
        for item in listed.items
        if item["source_ref"] == f"bug:{bug_id}"
        and item["failure_reason"] == "source_absent"
    ]
    assert len(source_absent) == 1
    assert source_absent[0]["canonical_state"] in {
        "pending",
        "failed",
        "deferred",
    }
    assert source_absent[0]["correlation_id"] == tombstone.delete_event_id


def _owner_graph_snapshot_sync(
    board_id: str,
    artifact_type: str,
    artifact_id: str,
) -> tuple[tuple[str, str, str, str, str], ...]:
    rows: list[tuple[str, str, str, str, str]] = []
    with open_board_connection(board_id) as (_database, connection):
        for node_type in ALL_NODE_TYPES:
            result = connection.execute(
                f"MATCH (n:{node_type}) RETURN n.id, "
                "n.source_artifact_ref, n.graph_layer, n.maturity_status"
            )
            while result.has_next():
                row = result.get_next()
                source_ref = str(row[1] or "")
                if _source_identity_from_ref(source_ref) != (
                    artifact_type,
                    artifact_id,
                ):
                    continue
                rows.append(
                    (
                        node_type,
                        str(row[0]),
                        source_ref,
                        str(row[2]),
                        str(row[3]),
                    )
                )
    return tuple(sorted(rows))


async def _owner_graph_snapshot(
    board_id: str,
    artifact_type: str,
    artifact_id: str,
) -> tuple[tuple[str, str, str, str, str], ...]:
    return await run_blocking_graph_io(
        lambda: _owner_graph_snapshot_sync(
            board_id,
            artifact_type,
            artifact_id,
        ),
        task_name="tests.takedown.owner_graph_snapshot",
    )


async def _force_owner_canonical_fixture(
    board_id: str,
    artifact_type: str,
    artifact_id: str,
) -> None:
    """Model a legacy canonical source even if current policy is working-only."""

    def _write() -> None:
        rows = _owner_graph_snapshot_sync(
            board_id,
            artifact_type,
            artifact_id,
        )
        with open_board_connection(board_id) as (_database, connection):
            for node_type, node_id, _source_ref, _layer, _maturity in rows:
                connection.execute(
                    f"MATCH (n:{node_type} {{id: $node_id}}) "
                    "SET n.graph_layer = $layer, n.maturity_status = $maturity",
                    {
                        "node_id": node_id,
                        "layer": GRAPH_LAYER_CANONICAL,
                        "maturity": MATURITY_CANONICAL_ELIGIBLE,
                    },
                )

    await run_blocking_graph_io(
        _write,
        task_name="tests.ts7.force_legacy_canonical_fixture",
    )


async def _seed_done_tree(db_factory, board_id: str):
    ideation_id = f"idea-ts7-{uuid.uuid4().hex[:10]}"
    refinement_id = f"ref-ts7-{uuid.uuid4().hex[:10]}"
    spec_id = f"spec-ts7-{uuid.uuid4().hex[:10]}"
    async with db_factory() as session:
        session.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="TS7 ideation",
                status=IdeationStatus.DONE,
                created_by=USER_ID,
            )
        )
        await session.flush()
        session.add(
            Refinement(
                id=refinement_id,
                board_id=board_id,
                ideation_id=ideation_id,
                title="TS7 refinement",
                status=RefinementStatus.DONE,
                created_by=USER_ID,
            )
        )
        await session.flush()
        session.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                ideation_id=ideation_id,
                refinement_id=refinement_id,
                title="TS7 spec",
                status=SpecStatus.DONE,
                created_by=USER_ID,
                functional_requirements=["FR TS7"],
                acceptance_criteria=["AC TS7"],
            )
        )
        await session.commit()

    worker = DeterministicWorker()
    results = (
        worker.process_ideation(
            {
                "id": ideation_id,
                "board_id": board_id,
                "title": "TS7 ideation",
                "status": "done",
                "story_ids": [],
            }
        ),
        worker.process_refinement(
            {
                "id": refinement_id,
                "board_id": board_id,
                "ideation_id": ideation_id,
                "title": "TS7 refinement",
                "status": "done",
            }
        ),
        worker.process_spec(
            {
                **spec_dict(
                    spec_id,
                    board_id,
                    "done",
                    frs=["FR TS7"],
                    acs=["AC TS7"],
                ),
                "ideation_id": ideation_id,
                "refinement_id": refinement_id,
            }
        ),
    )
    for result in results:
        await commit_worker_result(db_factory, board_id, result)
    # The refinement worker also emits its ideation endpoint. Replaying the
    # authoritative ideation projection last ensures the pre-delete fixture is
    # genuinely canonical rather than accepting a working endpoint stub.
    await commit_worker_result(db_factory, board_id, results[0])
    # Ideations are working-only under today's maturity policy, while TS7
    # explicitly starts from a pre-wiring tree whose three sources already
    # have canonical nodes. Normalize that legacy fixture state so the test
    # proves deletion convergence instead of silently starting converged.
    await _force_owner_canonical_fixture(board_id, "ideation", ideation_id)
    return ideation_id, refinement_id, spec_id


@pytest.mark.asyncio
async def test_spec_delete_mints_takedown_for_cascaded_sprint(
    db_factory,
) -> None:
    board_id = await new_board(db_factory, "spec-sprint-cascade-takedown")
    spec_id, _source_ref = await seed_done_spec_canonical(db_factory, board_id)
    async with db_factory() as session:
        sprint = Sprint(
            board_id=board_id,
            spec_id=spec_id,
            title="Sprint removed with spec",
            created_by=USER_ID,
        )
        session.add(sprint)
        await session.commit()
        await session.refresh(sprint)
        sprint_id = sprint.id

    with _registered_targeted_adapters():
        async with db_factory() as session:
            assert await SpecService(session).delete_spec(spec_id, USER_ID)
            await session.commit()

    async with db_factory() as session:
        intents = (
            (
                await session.execute(
                    select(ConsolidationQueue).where(
                        ConsolidationQueue.board_id == board_id,
                        ConsolidationQueue.work_kind == "stale_reconcile",
                        ConsolidationQueue.artifact_type.in_(("spec", "sprint")),
                    )
                )
            )
            .scalars()
            .all()
        )

    assert {(intent.artifact_type, intent.artifact_id) for intent in intents} == {
        ("spec", spec_id),
        ("sprint", sprint_id),
    }
    sprint_intent = next(
        intent for intent in intents if intent.artifact_type == "sprint"
    )
    assert sprint_intent.payload["source_refs"] == [f"sprint:{sprint_id}"]


@pytest.mark.asyncio
async def test_ts7_three_delete_uows_converge_independent_intents(
    db_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TS7: spec, refinement and ideation each commit and converge separately."""

    board_id = await new_board(db_factory, "ts7-cascade")
    ideation_id, refinement_id, spec_id = await _seed_done_tree(
        db_factory,
        board_id,
    )
    cognitive_id = await seed_canonical_cognitive(
        board_id,
        "Alternative",
        source_ref=f"spec:{spec_id}:alternative:1",
        title="TS7 preserved cognition",
    )
    identities = (
        ("spec", spec_id),
        ("refinement", refinement_id),
        ("ideation", ideation_id),
    )
    before = {
        identity: await _owner_graph_snapshot(board_id, *identity)
        for identity in identities
    }
    assert all(before.values())
    for identity, rows in before.items():
        deterministic = [row for row in rows if row[0] not in COGNITIVE_NODE_TYPES]
        assert deterministic, identity
        assert all(
            row[3:] == ("canonical", "canonical_eligible") for row in deterministic
        ), identity
    assert await _node_state(board_id, "Alternative", cognitive_id) == (
        "canonical",
        "canonical_eligible",
    )

    with _registered_targeted_adapters() as (queue, _delivery):
        async with db_factory() as session:
            assert await SpecService(session).delete_spec(spec_id, USER_ID)
            await session.commit()
        async with db_factory() as session:
            assert await RefinementService(session).delete_refinement(
                refinement_id,
                USER_ID,
            )
            await session.commit()
        async with db_factory() as session:
            assert await IdeationService(session).delete_ideation(
                ideation_id,
                USER_ID,
            )
            await session.commit()

        intent_ids = {
            await _intent_id(
                db_factory,
                board_id=board_id,
                artifact_type=artifact_type,
                artifact_id=artifact_id,
            )
            for artifact_type, artifact_id in identities
        }
        assert len(intent_ids) == 3
        queue.target_entry_ids.update(intent_ids)

        from okto_pulse.core.kg import canonical_stale_reconciler

        real_reconcile = canonical_stale_reconciler.reconcile_stale_canonical
        observed = []

        async def _observe(*args, **kwargs):
            result = await real_reconcile(*args, **kwargs)
            observed.append(result)
            return result

        monkeypatch.setattr(
            canonical_stale_reconciler,
            "reconcile_stale_canonical",
            _observe,
        )
        processor = ConsolidationProcessor(db_factory, batch_size=3)
        assert [await processor.process_batch() for _ in range(3)] == [1, 1, 1]

    assert len(observed) == 3
    assert {
        (record["owning_source_type"], record["owning_source_id"])
        for result in observed
        for record in result.demoted
    } == set(identities)
    for identity in identities:
        after = await _owner_graph_snapshot(board_id, *identity)
        deterministic = [row for row in after if row[0] not in COGNITIVE_NODE_TYPES]
        assert deterministic
        assert all(row[3:] == ("working", "working_stale") for row in deterministic)
    assert await _node_state(board_id, "Alternative", cognitive_id) == (
        "canonical",
        "canonical_eligible",
    )


async def _seed_named_done_spec(
    db_factory,
    *,
    board_id: str,
    spec_id: str,
) -> None:
    await insert_spec(
        db_factory,
        board_id,
        spec_id,
        status="done",
        frs=["FR identical"],
        acs=["AC identical"],
    )
    result = DeterministicWorker().process_spec(
        spec_dict(
            spec_id,
            board_id,
            "done",
            frs=["FR identical"],
            acs=["AC identical"],
        )
    )
    await commit_worker_result(db_factory, board_id, result)


async def _drain_global_outbox(db_factory) -> int:
    from okto_pulse.core.application.processors.global_outbox import (
        GlobalOutboxProcessor,
    )

    processed = 0
    worker = GlobalOutboxProcessor(db_factory, interval_seconds=5)
    for _ in range(10):
        count = await worker.process_once()
        processed += count
        if count == 0:
            break
    return processed


@pytest.mark.asyncio
async def test_ts8_fast_path_and_stale_sweep_converge_same_graph_and_digest(
    db_factory,
) -> None:
    """TS8: two boards converge through distinct durable ownership paths."""

    from test_kg_r2_test8 import (
        _digest_layer,
        _digest_node_via_gd_worker,
        _query_ids,
    )

    artifact_id = f"spec-ts8-{uuid.uuid4().hex[:12]}"
    board_fast = await new_board(db_factory, "ts8-fast")
    board_sweep = await new_board(db_factory, "ts8-sweep")

    with _registered_targeted_adapters() as (queue, _delivery):
        await _seed_named_done_spec(
            db_factory,
            board_id=board_fast,
            spec_id=artifact_id,
        )
        fast_requirement = await first_canonical_node(board_fast, "Requirement")
        assert fast_requirement is not None
        requirement_id, _ = fast_requirement
        assert (
            await _digest_node_via_gd_worker(
                db_factory,
                board_fast,
                requirement_id,
                "Requirement",
            )
            == 1
        )

        async with db_factory() as session:
            assert await SpecService(session).delete_spec(artifact_id, USER_ID)
            await session.commit()
        fast_intent = await _intent_id(
            db_factory,
            board_id=board_fast,
            artifact_type="spec",
            artifact_id=artifact_id,
        )
        queue.target_entry_ids.add(fast_intent)
        assert (
            await ConsolidationProcessor(db_factory, batch_size=1).process_batch() == 1
        )
        assert await _drain_global_outbox(db_factory) >= 1

        # The first delete freed the globally unique relational id, allowing
        # the second board to carry the exact same artifact identity/content.
        await _seed_named_done_spec(
            db_factory,
            board_id=board_sweep,
            spec_id=artifact_id,
        )
        sweep_requirement = await first_canonical_node(board_sweep, "Requirement")
        assert sweep_requirement is not None
        sweep_requirement_id, _ = sweep_requirement
        assert (
            await _digest_node_via_gd_worker(
                db_factory,
                board_sweep,
                sweep_requirement_id,
                "Requirement",
            )
            == 1
        )

        # Suppress the fast-path deliberately: the source disappears without
        # a tombstone or intent and must be discovered by the real stale sweep.
        async with db_factory() as session:
            source = await session.get(Spec, artifact_id)
            assert source is not None and source.board_id == board_sweep
            await session.delete(source)
            await session.commit()
        async with db_factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ArtifactDeletionTombstone)
                    .where(ArtifactDeletionTombstone.board_id == board_sweep)
                )
                == 0
            )
            schedule = await queue.schedule_stale_sweep(
                session,
                StaleSweepScheduleRequest(
                    board_id=board_sweep,
                    budget=50,
                    now=datetime.now(timezone.utc),
                ),
            )
            await session.commit()
        assert schedule.scheduled is True and schedule.sweep_id is not None
        queue.target_entry_ids.add(schedule.sweep_id)
        assert (
            await ConsolidationProcessor(db_factory, batch_size=1).process_batch() == 1
        )

        sweep_intent = await _intent_id(
            db_factory,
            board_id=board_sweep,
            artifact_type="spec",
            artifact_id=artifact_id,
        )
        assert sweep_intent != fast_intent
        queue.target_entry_ids.add(sweep_intent)
        assert (
            await ConsolidationProcessor(db_factory, batch_size=1).process_batch() == 1
        )
        assert await _drain_global_outbox(db_factory) >= 1

    fast_snapshot = await _owner_graph_snapshot(
        board_fast,
        "spec",
        artifact_id,
    )
    sweep_snapshot = await _owner_graph_snapshot(
        board_sweep,
        "spec",
        artifact_id,
    )

    # Node ids include the board namespace. Compare the complete publication
    # shape after removing only that expected physical identity difference.
    def _normalized(rows):
        return tuple(
            sorted(
                (node_type, source_ref, layer, maturity)
                for (
                    node_type,
                    _node_id,
                    source_ref,
                    layer,
                    maturity,
                ) in rows
            )
        )

    assert _normalized(fast_snapshot) == _normalized(sweep_snapshot)
    assert fast_snapshot
    assert all(row[3:] == ("working", "working_stale") for row in fast_snapshot)
    assert _digest_layer(board_fast, requirement_id) == "working"
    assert _digest_layer(board_sweep, sweep_requirement_id) == "working"
    assert requirement_id not in _query_ids(board_fast, "canonical")
    assert sweep_requirement_id not in _query_ids(board_sweep, "canonical")
    assert requirement_id in _query_ids(board_fast, "all")
    assert sweep_requirement_id in _query_ids(board_sweep, "all")
