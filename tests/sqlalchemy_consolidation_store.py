"""Test-only SQLAlchemy consolidation persistence adapter."""

import uuid
from typing import Any, Sequence

from sqlalchemy import case, delete, exists, func, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import selectinload

from sqlalchemy_test_models import (
    AmendmentHotfixRevision,
    ArtifactDeletionTombstone,
    Board,
    CanonicalDebt,
    Card,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    Ideation,
    Refinement,
    Spec,
    Sprint,
    Story,
)
from okto_pulse.core.ports.consolidation import (
    ConsolidationPoisonRow,
    ConsolidationProjectionInputs,
    ConsolidationQueueRecord,
)
from okto_pulse.core.ports.reconcile_intent import (
    ReconcileIntentCreate,
    ReconcileIntentReceipt,
)
from okto_pulse.core.ports.tombstone import (
    DeletionTombstoneAdvance,
    DeletionTombstoneReceipt,
)


_MODELS = {
    "story": Story,
    "ideation": Ideation,
    "refinement": Refinement,
    "spec": Spec,
    "sprint": Sprint,
    "card": Card,
    "amendment_hotfix_revision": AmendmentHotfixRevision,
}


def _record(row: Any) -> ConsolidationQueueRecord:
    return ConsolidationQueueRecord(
        id=str(row.id),
        board_id=str(row.board_id),
        artifact_type=str(row.artifact_type),
        artifact_id=str(row.artifact_id),
        status=str(row.status),
        attempts=int(row.attempts or 0),
        last_error=row.last_error,
        next_retry_at=row.next_retry_at,
        claimed_at=row.claimed_at,
        claim_timeout_at=row.claim_timeout_at,
        worker_id=row.worker_id,
        claimed_by_session_id=row.claimed_by_session_id,
        triggered_at=row.triggered_at,
        priority=str(getattr(row.priority, "value", row.priority)),
        source=str(getattr(row, "source", None) or "state_transition"),
        work_kind=str(row.work_kind),
        generation=int(row.generation or 0),
        payload=dict(row.payload) if isinstance(row.payload, dict) else row.payload,
        delete_event_id=row.delete_event_id,
        claim_token=row.claim_token,
    )


def _apply(row: Any, record: ConsolidationQueueRecord) -> None:
    for name in (
        "status",
        "attempts",
        "last_error",
        "next_retry_at",
        "claimed_at",
        "claim_timeout_at",
        "worker_id",
        "claimed_by_session_id",
        "claim_token",
    ):
        setattr(row, name, getattr(record, name))


class TestSqlAlchemyConsolidationPersistence:
    __test__ = False

    async def load_artifact(
        self, context, *, artifact_type: str, artifact_id: str
    ) -> Any | None:
        model = _MODELS.get(artifact_type)
        if model is None:
            return None
        statement = select(model).where(model.id == artifact_id)
        if artifact_type == "ideation":
            statement = statement.options(selectinload(Ideation.story_links))
        elif artifact_type == "spec":
            statement = statement.options(selectinload(Spec.architecture_designs))
        elif artifact_type == "sprint":
            statement = statement.options(selectinload(Sprint.spec))
        elif artifact_type == "card":
            statement = statement.options(selectinload(Card.architecture_designs))
        return (await context.execute(statement)).scalars().first()

    async def load_projection_inputs(
        self,
        context,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        artifact: Any,
    ) -> ConsolidationProjectionInputs:
        del context, board_id, artifact_type, artifact_id, artifact
        return ConsolidationProjectionInputs()

    async def list_artifacts(
        self,
        context,
        *,
        artifact_type: str,
        artifact_ids: Sequence[str],
        board_id: str | None = None,
    ) -> tuple[Any, ...]:
        model = _MODELS.get(artifact_type)
        if model is None or not artifact_ids:
            return ()
        statement = select(model).where(model.id.in_(tuple(artifact_ids)))
        if board_id is not None and hasattr(model, "board_id"):
            statement = statement.where(model.board_id == board_id)
        return tuple((await context.execute(statement)).scalars().all())

    async def list_stale_claims(
        self, context, *, now, legacy_cutoff
    ) -> tuple[ConsolidationQueueRecord, ...]:
        rows = (
            (
                await context.execute(
                    select(ConsolidationQueue).where(
                        ConsolidationQueue.status == "claimed",
                        or_(
                            ConsolidationQueue.claim_token.is_(None),
                            ConsolidationQueue.claim_timeout_at.is_not(None)
                            & (ConsolidationQueue.claim_timeout_at < now),
                            ConsolidationQueue.claim_timeout_at.is_(None)
                            & ConsolidationQueue.claimed_at.is_not(None)
                            & (ConsolidationQueue.claimed_at < legacy_cutoff),
                        ),
                    )
                )
            )
            .scalars()
            .all()
        )
        return tuple(_record(row) for row in rows)

    async def count_pending(self, context) -> int:
        value = await context.scalar(
            select(func.count()).where(ConsolidationQueue.status == "pending")
        )
        return int(value or 0)

    async def list_claimed_board_ids(self, context) -> frozenset[str]:
        rows = (
            (
                await context.execute(
                    select(ConsolidationQueue.board_id).where(
                        ConsolidationQueue.status == "claimed"
                    )
                )
            )
            .scalars()
            .all()
        )
        return frozenset(str(value) for value in rows)

    async def list_ready_pending(
        self, context, *, now
    ) -> tuple[ConsolidationQueueRecord, ...]:
        rows = (
            (
                await context.execute(
                    select(ConsolidationQueue)
                    .where(
                        ConsolidationQueue.status == "pending",
                        ConsolidationQueue.work_kind.in_(
                            ("consolidate", "stale_reconcile")
                        ),
                        or_(
                            ConsolidationQueue.next_retry_at.is_(None),
                            ConsolidationQueue.next_retry_at <= now,
                        ),
                    )
                    .order_by(
                        ConsolidationQueue.priority.asc(),
                        ConsolidationQueue.triggered_at.asc(),
                    )
                )
            )
            .scalars()
            .all()
        )
        return tuple(_record(row) for row in rows)

    async def get_queue_entry(
        self, context, *, entry_id: str
    ) -> ConsolidationQueueRecord | None:
        row = await context.get(ConsolidationQueue, entry_id)
        return _record(row) if row is not None else None

    async def queue_claim_is_current_and_unfenced(
        self,
        context,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        work_kind: str,
        source: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        if not entry_id or not claim_token:
            return False
        claim_predicates = (
            ConsolidationQueue.id == entry_id,
            ConsolidationQueue.status == "claimed",
            ConsolidationQueue.claim_token == claim_token,
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.artifact_type == artifact_type,
            ConsolidationQueue.artifact_id == artifact_id,
            ConsolidationQueue.work_kind == work_kind,
            ConsolidationQueue.source == source,
            ConsolidationQueue.generation == generation,
            (
                ConsolidationQueue.delete_event_id.is_(None)
                if delete_event_id is None
                else ConsolidationQueue.delete_event_id == delete_event_id
            ),
        )
        tombstone_key = (
            ArtifactDeletionTombstone.board_id == board_id,
            ArtifactDeletionTombstone.artifact_type == artifact_type,
            ArtifactDeletionTombstone.artifact_id == artifact_id,
        )
        if work_kind == "consolidate":
            if generation != 0 or delete_event_id is not None:
                return False
            deletion_fence = ~exists(select(1).where(*tombstone_key))
        elif work_kind == "stale_reconcile":
            if generation < 1 or delete_event_id is None:
                return False
            deletion_fence = exists(
                select(1).where(
                    *tombstone_key,
                    ArtifactDeletionTombstone.generation == generation,
                    ArtifactDeletionTombstone.delete_event_id == delete_event_id,
                )
            )
        else:
            return False
        return bool(
            await context.scalar(
                select(exists().where(*claim_predicates, deletion_fence))
            )
        )

    async def ack_claimed_queue_entry(
        self,
        context,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        if not entry_id or not claim_token:
            return False
        delete_event_predicate = (
            ConsolidationQueue.delete_event_id.is_(None)
            if delete_event_id is None
            else ConsolidationQueue.delete_event_id == delete_event_id
        )
        result = await context.execute(
            delete(ConsolidationQueue).where(
                ConsolidationQueue.id == entry_id,
                ConsolidationQueue.status == "claimed",
                ConsolidationQueue.claim_token == claim_token,
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.source == source,
                ConsolidationQueue.work_kind == work_kind,
                ConsolidationQueue.generation == generation,
                delete_event_predicate,
            )
        )
        return int(result.rowcount or 0) == 1

    async def repend_claimed_queue_entry(
        self,
        context,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        delete_event_predicate = (
            ConsolidationQueue.delete_event_id.is_(None)
            if delete_event_id is None
            else ConsolidationQueue.delete_event_id == delete_event_id
        )
        result = await context.execute(
            update(ConsolidationQueue)
            .where(
                ConsolidationQueue.id == entry_id,
                ConsolidationQueue.status == "claimed",
                ConsolidationQueue.claim_token == claim_token,
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.source == source,
                ConsolidationQueue.work_kind == work_kind,
                ConsolidationQueue.generation == generation,
                delete_event_predicate,
            )
            .values(
                status="pending",
                claimed_at=None,
                claim_timeout_at=None,
                worker_id=None,
                claimed_by_session_id=None,
                claim_token=None,
            )
        )
        return int(result.rowcount or 0) == 1

    async def save_queue_entries(
        self, context, entries: Sequence[ConsolidationQueueRecord]
    ) -> None:
        for entry in entries:
            row = await context.get(ConsolidationQueue, entry.id)
            if row is not None:
                _apply(row, entry)
        await context.flush()

    async def delete_queue_entry(self, context, *, entry_id: str) -> None:
        row = await context.get(ConsolidationQueue, entry_id)
        if row is not None:
            await context.delete(row)
            await context.flush()

    async def discard_artifact_work(
        self,
        context,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
    ) -> None:
        await context.execute(
            delete(ConsolidationQueue).where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.artifact_type == artifact_type,
                ConsolidationQueue.artifact_id == artifact_id,
                ConsolidationQueue.work_kind == "consolidate",
            )
        )
        for model in (ConsolidationDeadLetter, CanonicalDebt):
            await context.execute(
                delete(model).where(
                    model.board_id == board_id,
                    model.artifact_type == artifact_type,
                    model.artifact_id == artifact_id,
                )
            )
        await context.flush()

    async def advance_deletion_tombstone(
        self,
        context: Any,
        request: DeletionTombstoneAdvance,
    ) -> DeletionTombstoneReceipt:
        _validate_deletion_identity(
            artifact_type=request.artifact_type,
            artifact_id=request.artifact_id,
            delete_event_id=request.delete_event_id,
        )
        statement = (
            sqlite_insert(ArtifactDeletionTombstone)
            .values(
                id=str(uuid.uuid4()),
                board_id=request.board_id,
                artifact_type=request.artifact_type,
                artifact_id=request.artifact_id,
                generation=1,
                delete_event_id=request.delete_event_id,
            )
            .on_conflict_do_update(
                index_elements=["board_id", "artifact_type", "artifact_id"],
                set_={
                    "generation": case(
                        (
                            ArtifactDeletionTombstone.delete_event_id
                            == request.delete_event_id,
                            ArtifactDeletionTombstone.generation,
                        ),
                        else_=ArtifactDeletionTombstone.generation + 1,
                    ),
                    "delete_event_id": request.delete_event_id,
                    "updated_at": func.now(),
                },
            )
            .returning(
                ArtifactDeletionTombstone.generation,
                ArtifactDeletionTombstone.delete_event_id,
            )
        )
        generation, delete_event_id = (await context.execute(statement)).one()
        return DeletionTombstoneReceipt(
            generation=int(generation),
            delete_event_id=str(delete_event_id),
        )

    async def persist_reconcile_intent(
        self,
        context: Any,
        request: ReconcileIntentCreate,
    ) -> ReconcileIntentReceipt:
        _validate_deletion_identity(
            artifact_type=request.artifact_type,
            artifact_id=request.artifact_id,
            delete_event_id=request.delete_event_id,
        )
        expected_refs = (f"{request.artifact_type}:{request.artifact_id}",)
        if request.generation < 1 or request.source_refs != expected_refs:
            raise ValueError("invalid_reconcile_intent_identity")

        tombstone = (
            (
                await context.execute(
                    select(ArtifactDeletionTombstone).where(
                        ArtifactDeletionTombstone.board_id == request.board_id,
                        ArtifactDeletionTombstone.artifact_type
                        == request.artifact_type,
                        ArtifactDeletionTombstone.artifact_id == request.artifact_id,
                    )
                )
            )
            .scalars()
            .one_or_none()
        )
        if (
            tombstone is None
            or int(tombstone.generation) != request.generation
            or str(tombstone.delete_event_id) != request.delete_event_id
        ):
            raise RuntimeError("reconcile_intent_tombstone_mismatch")

        intent_id = str(uuid.uuid4())
        payload = {
            "schema_version": 1,
            "delete_event_id": request.delete_event_id,
            "source_refs": list(request.source_refs),
        }
        statement = (
            sqlite_insert(ConsolidationQueue)
            .values(
                id=intent_id,
                board_id=request.board_id,
                artifact_type=request.artifact_type,
                artifact_id=request.artifact_id,
                work_kind="stale_reconcile",
                generation=request.generation,
                payload=payload,
                delete_event_id=request.delete_event_id,
                priority="high",
                source="governed_delete",
                status="pending",
                triggered_by_event=request.delete_event_id,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    "board_id",
                    "artifact_type",
                    "artifact_id",
                    "work_kind",
                    "generation",
                ],
                index_where=ConsolidationQueue.work_kind == "stale_reconcile",
            )
            .returning(ConsolidationQueue.id)
        )
        persisted_id = (await context.execute(statement)).scalar_one_or_none()
        if persisted_id is None:
            existing = (
                (
                    await context.execute(
                        select(ConsolidationQueue).where(
                            ConsolidationQueue.board_id == request.board_id,
                            ConsolidationQueue.artifact_type == request.artifact_type,
                            ConsolidationQueue.artifact_id == request.artifact_id,
                            ConsolidationQueue.work_kind == "stale_reconcile",
                            ConsolidationQueue.generation == request.generation,
                        )
                    )
                )
                .scalars()
                .one()
            )
            if (
                str(existing.delete_event_id) != request.delete_event_id
                or existing.payload != payload
            ):
                raise RuntimeError("reconcile_intent_replay_conflict")
            persisted_id = existing.id
        return ReconcileIntentReceipt(
            intent_id=str(persisted_id),
            generation=request.generation,
            delete_event_id=request.delete_event_id,
        )

    async def board_exists(self, context, *, board_id: str) -> bool:
        return await context.get(Board, board_id) is not None

    async def list_dlq_auto_drain_board_ids(self, context) -> tuple[str, ...]:
        rows = (await context.execute(select(Board))).scalars().all()
        return tuple(
            str(row.id)
            for row in rows
            if isinstance(row.settings, dict)
            and row.settings.get("dlq_auto_drain_enabled")
        )

    async def board_administrative_rebuild_source(
        self,
        context,
        *,
        board_id: str,
    ) -> str | None:
        # This test adapter has no administrative reservation table. Focused
        # reservation tests inject a store that implements the real lookup.
        del context, board_id
        return None

    async def count_dead_letters(self, context, *, board_id: str) -> int:
        value = await context.scalar(
            select(func.count()).where(ConsolidationDeadLetter.board_id == board_id)
        )
        return int(value or 0)

    async def delete_poison_dead_letters(
        self, context, *, board_id: str, max_attempts: int
    ) -> tuple[ConsolidationPoisonRow, ...]:
        rows = (
            (
                await context.execute(
                    select(ConsolidationDeadLetter).where(
                        ConsolidationDeadLetter.board_id == board_id,
                        ConsolidationDeadLetter.attempts >= max_attempts,
                    )
                )
            )
            .scalars()
            .all()
        )
        result = tuple(
            ConsolidationPoisonRow(str(row.id), int(row.attempts)) for row in rows
        )
        for row in rows:
            await context.delete(row)
        if rows and hasattr(context, "flush"):
            await context.flush()
        return result

    async def commit(self, context) -> None:
        await context.commit()

    async def rollback(self, context) -> None:
        await context.rollback()


__all__ = ["TestSqlAlchemyConsolidationPersistence"]


def _validate_deletion_identity(
    *, artifact_type: str, artifact_id: str, delete_event_id: str
) -> None:
    if artifact_type not in {"card", "spec", "ideation", "refinement", "sprint"}:
        raise ValueError("invalid_governed_deletion_artifact_type")
    if not artifact_id or not delete_event_id or len(delete_event_id) > 255:
        raise ValueError("invalid_governed_deletion_identity")
