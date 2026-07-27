"""Test-only SQLAlchemy adapter for historical Core integration coverage."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select, update

from okto_pulse.core.application.domain_event_delivery import (
    DomainEventDeliveryProcessor,
)
from okto_pulse.core.events.registry import resolve_handler
from sqlalchemy_test_models import (
    DomainEventHandlerExecution,
    DomainEventRow,
)
from okto_pulse.core.ports.domain_event_delivery import (
    CardBoostFacts,
    CognitiveCardFacts,
    CognitiveSpecFacts,
    DomainEventExecution,
    DomainEventFailure,
    StoredDomainEvent,
)
from sqlalchemy_test_models import Board, Card, Spec


class _DefaultClaimRepository:
    async def claim_domain_event_executions(self, session, *, limit, now):  # noqa: ANN001, ANN201
        result = await session.execute(
            select(
                DomainEventHandlerExecution.id,
                DomainEventHandlerExecution.event_id,
            )
            .join(
                DomainEventRow,
                DomainEventRow.id == DomainEventHandlerExecution.event_id,
            )
            .where(DomainEventHandlerExecution.status == "pending")
            .where(
                (DomainEventHandlerExecution.next_attempt_at.is_(None))
                | (DomainEventHandlerExecution.next_attempt_at <= now)
            )
            .order_by(DomainEventRow.occurred_at.asc(), DomainEventRow.id.asc())
            .limit(limit)
        )
        return list(result.all())


class TestSqlAlchemyDomainEventDeliveryStore:
    __test__ = False

    def __init__(self, session_factory, claim_repository=None) -> None:  # noqa: ANN001
        self._session_factory = session_factory
        self._claim_repository = claim_repository or _DefaultClaimRepository()

    async def recover_orphans(self) -> int:
        async with self._session_factory() as session:
            result = await session.execute(
                update(DomainEventHandlerExecution)
                .where(DomainEventHandlerExecution.status == "processing")
                .values(status="pending", next_attempt_at=None)
            )
            await session.commit()
        return int(getattr(result, "rowcount", 0) or 0)

    async def claim_ready(self, *, limit: int, now: datetime):  # noqa: ANN201
        async with self._session_factory() as session:
            return await self._claim_repository.claim_domain_event_executions(
                session,
                limit=limit,
                now=now,
            )

    async def begin_attempt(self, execution_id: str):  # noqa: ANN201
        async with self._session_factory() as session:
            execution = await session.get(
                DomainEventHandlerExecution,
                execution_id,
            )
            if execution is None or execution.status != "pending":
                return None
            execution.status = "processing"
            execution.attempts = (execution.attempts or 0) + 1
            await session.commit()
            return DomainEventExecution(
                execution_id=execution.id,
                event_id=execution.event_id,
                handler_name=execution.handler_name,
                attempts=execution.attempts,
            )

    async def load_event(self, event_id: str):  # noqa: ANN201
        async with self._session_factory() as session:
            row = await session.get(DomainEventRow, event_id)
            if row is None:
                return None
            return StoredDomainEvent(
                event_id=row.id,
                event_type=row.event_type,
                board_id=row.board_id,
                actor_id=row.actor_id,
                actor_type=row.actor_type,
                occurred_at=row.occurred_at,
                payload=(
                    dict(row.payload_json)
                    if isinstance(row.payload_json, dict)
                    else {}
                ),
            )

    async def invoke_handler(
        self,
        execution_id,
        handler,
        event,
        *,
        processed_at,
    ):  # noqa: ANN001, ANN201
        async with self._session_factory() as session:
            execution = await session.get(
                DomainEventHandlerExecution,
                execution_id,
            )
            if execution is None or execution.status != "processing":
                return
            await handler().handle(event, session)
            execution.status = "done"
            execution.processed_at = processed_at
            await session.commit()

    async def mark_event_missing(
        self, execution_id, *, processed_at
    ):  # noqa: ANN001, ANN201
        async with self._session_factory() as session:
            execution = await session.get(
                DomainEventHandlerExecution,
                execution_id,
            )
            if execution is None:
                return
            execution.status = "dlq"
            execution.last_error = "event row missing"
            execution.processed_at = processed_at
            await session.commit()

    async def mark_failed(
        self, execution_id, failure: DomainEventFailure
    ) -> None:  # noqa: ANN001
        async with self._session_factory() as session:
            execution = await session.get(
                DomainEventHandlerExecution,
                execution_id,
            )
            if execution is None:
                return
            execution.last_error = failure.error
            execution.status = "dlq" if failure.terminal else "pending"
            execution.processed_at = failure.processed_at
            execution.next_attempt_at = failure.next_attempt_at
            await session.commit()


class TestSqlAlchemyDomainEventPublisher:
    __test__ = False

    async def publish(
        self,
        context,
        *,
        event,
        handler_names,
    ) -> None:  # noqa: ANN001
        context.add(
            DomainEventRow(
                id=event.event_id,
                event_type=event.event_type,
                board_id=event.board_id,
                actor_id=event.actor_id,
                actor_type=event.actor_type,
                payload_json=event.payload_for_storage(),
                occurred_at=event.occurred_at,
            )
        )
        await context.flush()
        for handler_name in handler_names:
            context.add(
                DomainEventHandlerExecution(
                    event_id=event.event_id,
                    handler_name=handler_name,
                    status="pending",
                    attempts=0,
                )
            )
        await context.flush()


class TestSqlAlchemyDomainEventFactReader:
    __test__ = False

    async def load_card_boost_facts(
        self, context, *, card_id: str
    ):  # noqa: ANN001, ANN201
        card = await context.get(Card, card_id)
        if card is None:
            return None
        return CardBoostFacts(
            card_type=_enum_value(card.card_type),
            priority=_enum_value(card.priority),
            severity=_enum_value(card.severity),
        )

    async def load_cognitive_card_facts(
        self, context, *, card_id: str
    ):  # noqa: ANN001, ANN201
        card = await context.get(Card, card_id)
        if card is None:
            return None
        return CognitiveCardFacts(
            card_id=card.id,
            spec_id=card.spec_id,
            card_type=_enum_value(card.card_type),
            title=getattr(card, "title", None),
            action_plan=card.action_plan,
        )

    async def load_board_settings(
        self, context, *, board_id: str
    ):  # noqa: ANN001, ANN201
        board = await context.get(Board, board_id)
        return _settings_dict(board.settings) if board is not None else None

    async def load_cognitive_spec_facts(
        self, context, *, spec_id: str
    ):  # noqa: ANN001, ANN201
        spec = await context.get(Spec, spec_id)
        if spec is None:
            return None
        return CognitiveSpecFacts(spec_id=spec.id, context=spec.context)


def _enum_value(value):  # noqa: ANN001, ANN201
    return value.value if hasattr(value, "value") else value


def _settings_dict(value):  # noqa: ANN001, ANN201
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        return value.model_dump()
    except AttributeError:
        return None


def build_test_event_processor(
    session_factory,
    *,
    claim_repository=None,
    clock=None,
) -> DomainEventDeliveryProcessor:  # noqa: ANN001
    effective_clock = (
        clock.now
        if clock is not None and hasattr(clock, "now")
        else clock
    )
    return DomainEventDeliveryProcessor(
        TestSqlAlchemyDomainEventDeliveryStore(
            session_factory,
            claim_repository=claim_repository,
        ),
        handler_resolver=resolve_handler,
        clock=effective_clock or (lambda: datetime.now(timezone.utc)),
    )


__all__ = [
    "TestSqlAlchemyDomainEventDeliveryStore",
    "TestSqlAlchemyDomainEventPublisher",
    "TestSqlAlchemyDomainEventFactReader",
    "build_test_event_processor",
]
