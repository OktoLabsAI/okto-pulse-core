"""TS1/TS7 foundation: governed delete is atomic and generation-aware."""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from okto_pulse.core.ports.consolidation import (
    get_consolidation_persistence_port,
)
from okto_pulse.core.ports.reconcile_intent import (
    ReconcileIntentCreate,
    get_reconcile_intent_port,
)
from okto_pulse.core.ports.tombstone import (
    DeletionTombstoneAdvance,
    get_tombstone_port,
)
from okto_pulse.core.services.main import CardService, IdeationService
from sqlalchemy_test_models import (
    ActivityLog,
    ArtifactDeletionTombstone,
    Board,
    CanonicalDebt,
    Card,
    CardType,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
)


USER_ID = "user-governed-delete"


@pytest_asyncio.fixture
async def session(db_factory):
    async with db_factory() as db:
        yield db
        await db.rollback()


async def _seed_board(session) -> Board:
    board = Board(
        id=str(uuid.uuid4()),
        name="Governed delete",
        owner_id=USER_ID,
    )
    session.add(board)
    await session.flush()
    return board


async def _seed_card_with_legacy_work(session, board_id: str) -> Card:
    card = Card(
        id=str(uuid.uuid4()),
        board_id=board_id,
        title="Delete atomically",
        status="not_started",
        priority="none",
        position=0,
        created_by=USER_ID,
        card_type=CardType.NORMAL,
        labels=[],
        test_scenario_ids=[],
        linked_test_task_ids=[],
    )
    session.add(card)
    await session.flush()
    session.add_all(
        [
            ConsolidationQueue(
                board_id=board_id,
                artifact_type="card",
                artifact_id=card.id,
                work_kind="consolidate",
                generation=0,
                status="pending",
            ),
            ConsolidationDeadLetter(
                board_id=board_id,
                artifact_type="card",
                artifact_id=card.id,
                attempts=5,
                errors=[],
            ),
            CanonicalDebt(
                board_id=board_id,
                artifact_type="card",
                artifact_id=card.id,
                source_ref=f"card:{card.id}",
                content_hash="legacy-debt",
                target_status="done",
            ),
        ]
    )
    await session.commit()
    return card


async def _assert_original_state_restored(session, *, board_id: str, card_id: str):
    assert await session.get(Card, card_id) is not None
    legacy_count = await session.scalar(
        select(func.count())
        .select_from(ConsolidationQueue)
        .where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.artifact_type == "card",
            ConsolidationQueue.artifact_id == card_id,
            ConsolidationQueue.work_kind == "consolidate",
        )
    )
    intent_count = await session.scalar(
        select(func.count())
        .select_from(ConsolidationQueue)
        .where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.artifact_type == "card",
            ConsolidationQueue.artifact_id == card_id,
            ConsolidationQueue.work_kind == "stale_reconcile",
        )
    )
    tombstone_count = await session.scalar(
        select(func.count())
        .select_from(ArtifactDeletionTombstone)
        .where(
            ArtifactDeletionTombstone.board_id == board_id,
            ArtifactDeletionTombstone.artifact_type == "card",
            ArtifactDeletionTombstone.artifact_id == card_id,
        )
    )
    dlq_count = await session.scalar(
        select(func.count())
        .select_from(ConsolidationDeadLetter)
        .where(
            ConsolidationDeadLetter.board_id == board_id,
            ConsolidationDeadLetter.artifact_type == "card",
            ConsolidationDeadLetter.artifact_id == card_id,
        )
    )
    debt_count = await session.scalar(
        select(func.count())
        .select_from(CanonicalDebt)
        .where(
            CanonicalDebt.board_id == board_id,
            CanonicalDebt.artifact_type == "card",
            CanonicalDebt.artifact_id == card_id,
        )
    )
    activity_count = await session.scalar(
        select(func.count())
        .select_from(ActivityLog)
        .where(
            ActivityLog.board_id == board_id,
            ActivityLog.action == "card_deleted",
        )
    )
    assert (legacy_count, intent_count, tombstone_count) == (1, 0, 0)
    assert (dlq_count, debt_count, activity_count) == (1, 1, 0)


@pytest.mark.asyncio
async def test_ts_28c3edda_rolls_back_when_intent_insert_fails(session):
    board = await _seed_board(session)
    card = await _seed_card_with_legacy_work(session, board.id)
    board_id, card_id = board.id, card.id
    trigger_name = f"fail_intent_{uuid.uuid4().hex}"
    await session.execute(
        text(
            f'CREATE TRIGGER "{trigger_name}" '
            "BEFORE INSERT ON consolidation_queue "
            "WHEN NEW.work_kind = 'stale_reconcile' "
            "BEGIN SELECT RAISE(ABORT, 'injected_intent_failure'); END"
        )
    )
    await session.commit()

    try:
        with pytest.raises(IntegrityError, match="injected_intent_failure"):
            await CardService(session).delete_card(card_id, USER_ID)
        await session.rollback()

        await _assert_original_state_restored(
            session,
            board_id=board_id,
            card_id=card_id,
        )
    finally:
        await session.rollback()
        await session.execute(text(f'DROP TRIGGER IF EXISTS "{trigger_name}"'))
        await session.commit()


@pytest.mark.asyncio
async def test_ts_28c3edda_rolls_back_when_tombstone_insert_fails(session):
    board = await _seed_board(session)
    card = await _seed_card_with_legacy_work(session, board.id)
    board_id, card_id = board.id, card.id
    trigger_name = f"fail_tombstone_{uuid.uuid4().hex}"
    await session.execute(
        text(
            f'CREATE TRIGGER "{trigger_name}" '
            "BEFORE INSERT ON artifact_deletion_tombstones "
            "BEGIN SELECT RAISE(ABORT, 'injected_tombstone_failure'); END"
        )
    )
    await session.commit()

    try:
        with pytest.raises(IntegrityError, match="injected_tombstone_failure"):
            await CardService(session).delete_card(card_id, USER_ID)
        await session.rollback()

        await _assert_original_state_restored(
            session,
            board_id=board_id,
            card_id=card_id,
        )
    finally:
        await session.rollback()
        await session.execute(text(f'DROP TRIGGER IF EXISTS "{trigger_name}"'))
        await session.commit()


@pytest.mark.asyncio
async def test_ts_28c3edda_rolls_back_when_artifact_delete_fails(session):
    board = await _seed_board(session)
    card = await _seed_card_with_legacy_work(session, board.id)
    board_id, card_id = board.id, card.id
    trigger_name = f"fail_delete_{uuid.uuid4().hex}"
    await session.execute(
        text(
            f'CREATE TRIGGER "{trigger_name}" BEFORE DELETE ON cards '
            f"WHEN OLD.id = '{card_id}' "
            "BEGIN SELECT RAISE(ABORT, 'injected_artifact_delete_failure'); END"
        )
    )
    await session.commit()

    try:
        with pytest.raises(
            IntegrityError,
            match="injected_artifact_delete_failure",
        ):
            await CardService(session).delete_card(card_id, USER_ID)
        await session.rollback()

        await _assert_original_state_restored(
            session,
            board_id=board_id,
            card_id=card_id,
        )
    finally:
        await session.rollback()
        await session.execute(text(f'DROP TRIGGER IF EXISTS "{trigger_name}"'))
        await session.commit()


@pytest.mark.asyncio
async def test_ts_28c3edda_replay_is_idempotent_and_new_event_advances(session):
    board = await _seed_board(session)
    artifact_id = str(uuid.uuid4())
    first_event = str(uuid.uuid4())
    second_event = str(uuid.uuid4())

    first_tombstone = await get_tombstone_port().advance_deletion_tombstone(
        session,
        DeletionTombstoneAdvance(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            delete_event_id=first_event,
        ),
    )
    first_intent = await get_reconcile_intent_port().persist_reconcile_intent(
        session,
        ReconcileIntentCreate(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            generation=first_tombstone.generation,
            delete_event_id=first_event,
            source_refs=(f"card:{artifact_id}",),
        ),
    )
    replay_tombstone = await get_tombstone_port().advance_deletion_tombstone(
        session,
        DeletionTombstoneAdvance(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            delete_event_id=first_event,
        ),
    )
    replay_intent = await get_reconcile_intent_port().persist_reconcile_intent(
        session,
        ReconcileIntentCreate(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            generation=replay_tombstone.generation,
            delete_event_id=first_event,
            source_refs=(f"card:{artifact_id}",),
        ),
    )
    second_tombstone = await get_tombstone_port().advance_deletion_tombstone(
        session,
        DeletionTombstoneAdvance(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            delete_event_id=second_event,
        ),
    )
    second_intent = await get_reconcile_intent_port().persist_reconcile_intent(
        session,
        ReconcileIntentCreate(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            generation=second_tombstone.generation,
            delete_event_id=second_event,
            source_refs=(f"card:{artifact_id}",),
        ),
    )

    assert first_tombstone.generation == replay_tombstone.generation == 1
    assert first_intent.intent_id == replay_intent.intent_id
    assert second_tombstone.generation == 2
    assert second_intent.intent_id != first_intent.intent_id
    rows = (
        await session.execute(
            select(ConsolidationQueue)
            .where(
                ConsolidationQueue.board_id == board.id,
                ConsolidationQueue.artifact_id == artifact_id,
                ConsolidationQueue.work_kind == "stale_reconcile",
            )
            .order_by(ConsolidationQueue.generation)
        )
    ).scalars().all()
    assert [(row.generation, row.delete_event_id) for row in rows] == [
        (1, first_event),
        (2, second_event),
    ]


@pytest.mark.asyncio
async def test_ts_28c3edda_discard_preserves_governed_work_kinds(session):
    board = await _seed_board(session)
    artifact_id = str(uuid.uuid4())
    rows = [
        ConsolidationQueue(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            work_kind="consolidate",
            generation=0,
            status="pending",
        ),
        ConsolidationQueue(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            work_kind="stale_reconcile",
            generation=7,
            status="pending",
        ),
        ConsolidationQueue(
            board_id=board.id,
            artifact_type="card",
            artifact_id=artifact_id,
            work_kind="stale_sweep",
            generation=0,
            status="pending",
        ),
    ]
    session.add_all(rows)
    await session.flush()

    await get_consolidation_persistence_port().discard_artifact_work(
        session,
        board_id=board.id,
        artifact_type="card",
        artifact_id=artifact_id,
    )

    remaining = (
        await session.execute(
            select(ConsolidationQueue.work_kind).where(
                ConsolidationQueue.board_id == board.id,
                ConsolidationQueue.artifact_id == artifact_id,
            )
        )
    ).scalars().all()
    assert set(remaining) == {"stale_reconcile", "stale_sweep"}


@pytest.mark.asyncio
async def test_ts_7eaf5452_cascade_persists_one_intent_per_deleted_entity(session):
    board = await _seed_board(session)
    ideation = Ideation(
        id=str(uuid.uuid4()),
        board_id=board.id,
        title="Parent ideation",
        status=IdeationStatus.DONE,
        created_by=USER_ID,
    )
    session.add(ideation)
    await session.flush()
    refinement = Refinement(
        id=str(uuid.uuid4()),
        board_id=board.id,
        ideation_id=ideation.id,
        title="Child refinement",
        status=RefinementStatus.DONE,
        created_by=USER_ID,
    )
    session.add(refinement)
    await session.flush()
    spec = Spec(
        id=str(uuid.uuid4()),
        board_id=board.id,
        ideation_id=ideation.id,
        refinement_id=refinement.id,
        title="Child spec",
        status=SpecStatus.DONE,
        created_by=USER_ID,
    )
    session.add(spec)
    await session.commit()

    assert await IdeationService(session).delete_ideation(ideation.id, USER_ID)
    await session.commit()

    expected = {
        ("ideation", ideation.id),
        ("refinement", refinement.id),
        ("spec", spec.id),
    }
    tombstones = (
        await session.execute(
            select(ArtifactDeletionTombstone).where(
                ArtifactDeletionTombstone.board_id == board.id
            )
        )
    ).scalars().all()
    intents = (
        await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == board.id,
                ConsolidationQueue.work_kind == "stale_reconcile",
            )
        )
    ).scalars().all()
    assert {(row.artifact_type, row.artifact_id) for row in tombstones} == expected
    assert {(row.artifact_type, row.artifact_id) for row in intents} == expected
    assert all(row.generation == 1 for row in tombstones + intents)
    assert {
        tuple(row.payload["source_refs"])
        for row in intents
    } == {(f"{kind}:{artifact_id}",) for kind, artifact_id in expected}


@pytest.mark.asyncio
async def test_ts_7eaf5452_parent_failure_rolls_back_entire_cascade(session):
    board = await _seed_board(session)
    ideation = Ideation(
        id=str(uuid.uuid4()),
        board_id=board.id,
        title="Rollback parent ideation",
        status=IdeationStatus.DONE,
        created_by=USER_ID,
    )
    session.add(ideation)
    await session.flush()
    refinement = Refinement(
        id=str(uuid.uuid4()),
        board_id=board.id,
        ideation_id=ideation.id,
        title="Rollback child refinement",
        status=RefinementStatus.DONE,
        created_by=USER_ID,
    )
    session.add(refinement)
    await session.flush()
    spec = Spec(
        id=str(uuid.uuid4()),
        board_id=board.id,
        ideation_id=ideation.id,
        refinement_id=refinement.id,
        title="Rollback child spec",
        status=SpecStatus.DONE,
        created_by=USER_ID,
    )
    session.add(spec)
    await session.commit()
    board_id = board.id
    entity_ids = (ideation.id, refinement.id, spec.id)
    trigger_name = f"fail_parent_intent_{uuid.uuid4().hex}"
    await session.execute(
        text(
            f'CREATE TRIGGER "{trigger_name}" '
            "BEFORE INSERT ON consolidation_queue "
            "WHEN NEW.work_kind = 'stale_reconcile' "
            "AND NEW.artifact_type = 'ideation' "
            "BEGIN SELECT RAISE(ABORT, 'injected_parent_intent_failure'); END"
        )
    )
    await session.commit()

    try:
        with pytest.raises(
            IntegrityError,
            match="injected_parent_intent_failure",
        ):
            await IdeationService(session).delete_ideation(
                entity_ids[0],
                USER_ID,
            )
        await session.rollback()

        assert await session.get(Ideation, entity_ids[0]) is not None
        assert await session.get(Refinement, entity_ids[1]) is not None
        assert await session.get(Spec, entity_ids[2]) is not None
        intent_count = await session.scalar(
            select(func.count())
            .select_from(ConsolidationQueue)
            .where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.work_kind == "stale_reconcile",
            )
        )
        tombstone_count = await session.scalar(
            select(func.count())
            .select_from(ArtifactDeletionTombstone)
            .where(ArtifactDeletionTombstone.board_id == board_id)
        )
        activity_count = await session.scalar(
            select(func.count())
            .select_from(ActivityLog)
            .where(
                ActivityLog.board_id == board_id,
                ActivityLog.action.in_(
                    ("spec_deleted", "refinement_deleted", "ideation_deleted")
                ),
            )
        )
        assert (intent_count, tombstone_count, activity_count) == (0, 0, 0)
    finally:
        await session.rollback()
        await session.execute(text(f'DROP TRIGGER IF EXISTS "{trigger_name}"'))
        await session.commit()


@pytest.mark.asyncio
async def test_ts_09c73b12_commit_survives_fresh_session(db_factory):
    """Foundation for kill-after-2xx: the response commit owns both records."""

    async with db_factory() as writer:
        board = await _seed_board(writer)
        card = await _seed_card_with_legacy_work(writer, board.id)
        board_id, card_id = board.id, card.id
        assert await CardService(writer).delete_card(card_id, USER_ID)
        await writer.commit()

    async with db_factory() as reader:
        assert await reader.get(Card, card_id) is None
        tombstone = (
            await reader.execute(
                select(ArtifactDeletionTombstone).where(
                    ArtifactDeletionTombstone.board_id == board_id,
                    ArtifactDeletionTombstone.artifact_type == "card",
                    ArtifactDeletionTombstone.artifact_id == card_id,
                )
            )
        ).scalars().one()
        intent = (
            await reader.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == "card",
                    ConsolidationQueue.artifact_id == card_id,
                    ConsolidationQueue.work_kind == "stale_reconcile",
                )
            )
        ).scalars().one()
        assert tombstone.generation == intent.generation == 1
        assert tombstone.delete_event_id == intent.delete_event_id
