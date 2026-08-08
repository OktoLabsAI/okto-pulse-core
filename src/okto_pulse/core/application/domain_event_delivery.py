"""Storage-neutral domain-event delivery processor.

Core owns claim consumption, handler resolution and retry/DLQ policy.  Editions
own the durable rows and transaction used to invoke a handler.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from okto_pulse.core.domain.worker_policy import RetryPolicy
from okto_pulse.core.events.types import DomainEvent, resolve_event_class
from okto_pulse.core.ports.domain_event_delivery import (
    DomainEventDeliveryStore,
    DomainEventFailure,
    StoredDomainEvent,
)

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
BACKOFF_BASE = 2
BACKOFF_CAP_SECONDS = 300
DRAIN_BATCH_SIZE = 50

Clock = Callable[[], datetime]
HandlerResolver = Callable[[str, str], type]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def resolve_registered_handler(handler_name: str, event_type: str) -> type:
    """Resolve a handler through the Core-owned semantic registry."""
    from okto_pulse.core.events.registry import resolve_handler

    return resolve_handler(handler_name, event_type)


def _same_stored_top_level(
    field_name: str,
    payload_value: object,
    row_value: object,
) -> bool:
    if field_name != "occurred_at":
        return payload_value == row_value
    try:
        payload_time = datetime.fromisoformat(
            str(payload_value).replace("Z", "+00:00")
        )
    except (TypeError, ValueError):
        return False
    if (
        payload_time.tzinfo is None
        or payload_time.utcoffset() is None
        or not isinstance(row_value, datetime)
        or row_value.tzinfo is None
        or row_value.utcoffset() is None
    ):
        return False
    return payload_time.astimezone(timezone.utc) == row_value.astimezone(
        timezone.utc
    )


def _event_payload_without_proven_top_level(
    row: StoredDomainEvent,
) -> dict[str, Any]:
    """Accept legacy full payloads only after proving column equality."""

    payload = dict(row.payload)
    top_level = {
        "event_id": row.event_id,
        "board_id": row.board_id,
        "actor_id": row.actor_id,
        "actor_type": row.actor_type,
        "occurred_at": row.occurred_at,
    }
    for field_name, row_value in top_level.items():
        if field_name not in payload:
            continue
        if not _same_stored_top_level(
            field_name,
            payload[field_name],
            row_value,
        ):
            raise ValueError(f"stored_event_top_level_mismatch:{field_name}")
        payload.pop(field_name)
    return payload


def event_from_stored(row: StoredDomainEvent) -> DomainEvent:
    cls = resolve_event_class(row.event_type)
    if cls is None:
        raise ValueError(f"Unknown event_type: {row.event_type}")
    return cls(
        event_id=row.event_id,
        board_id=row.board_id,
        actor_id=row.actor_id,
        actor_type=row.actor_type,
        occurred_at=row.occurred_at,
        **_event_payload_without_proven_top_level(row),
    )


class DomainEventDeliveryProcessor:
    def __init__(
        self,
        store: DomainEventDeliveryStore,
        *,
        handler_resolver: HandlerResolver = resolve_registered_handler,
        clock: Clock = _utcnow,
        retry_policy: RetryPolicy | None = None,
        batch_size: int = DRAIN_BATCH_SIZE,
    ) -> None:
        self._store = store
        self._handler_resolver = handler_resolver
        self._clock = clock
        self._retry_policy = retry_policy or RetryPolicy(
            max_attempts=MAX_ATTEMPTS,
            base=BACKOFF_BASE,
            cap_seconds=BACKOFF_CAP_SECONDS,
        )
        self._batch_size = batch_size

    async def recover_orphans(self) -> int:
        return await self._store.recover_orphans()

    async def process_batch(self) -> int:
        pairs = await self._store.claim_ready(
            limit=self._batch_size,
            now=self._clock(),
        )
        for execution_id, event_id in pairs:
            await self._process_one(execution_id, event_id)
        return len(pairs)

    async def _process_one(self, execution_id: str, event_id: str) -> None:
        execution = await self._store.begin_attempt(execution_id)
        if execution is None:
            return

        event_row = await self._store.load_event(event_id)
        if event_row is None:
            await self._store.mark_event_missing(
                execution_id,
                processed_at=self._clock(),
            )
            return

        try:
            event = event_from_stored(event_row)
            handler = self._handler_resolver(
                execution.handler_name,
                event.event_type,
            )
            await self._store.invoke_handler(
                execution_id,
                handler,
                event,
                processed_at=self._clock(),
            )
        except Exception as exc:  # noqa: BLE001 - one handler cannot stop the batch
            decision = self._retry_policy.after_failure(execution.attempts)
            now = self._clock()
            failure = DomainEventFailure(
                error=str(exc)[:500],
                terminal=decision.terminal,
                processed_at=now if decision.terminal else None,
                next_attempt_at=(
                    None
                    if decision.terminal
                    else now + timedelta(seconds=decision.delay_seconds)
                ),
            )
            await self._store.mark_failed(execution_id, failure)
            log = logger.error if decision.terminal else logger.warning
            log(
                "domain event handler failed handler=%s attempts=%d event_id=%s "
                "terminal=%s error=%s",
                execution.handler_name,
                execution.attempts,
                event_id,
                decision.terminal,
                exc,
            )
            return

__all__ = [
    "BACKOFF_BASE",
    "BACKOFF_CAP_SECONDS",
    "DRAIN_BATCH_SIZE",
    "DomainEventDeliveryProcessor",
    "MAX_ATTEMPTS",
    "event_from_stored",
    "resolve_registered_handler",
]
