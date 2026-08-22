"""Pure construction and caller-owned outbox staging for traceability events."""

from __future__ import annotations

import hashlib
from typing import Any, Literal, TypeVar

from okto_pulse.core.events.types import CodeTraceabilityDomainEvent


TraceabilityActorType = Literal["agent", "user", "system"]
TraceabilityEventT = TypeVar(
    "TraceabilityEventT",
    bound=CodeTraceabilityDomainEvent,
)


def code_traceability_event_digest(value: str) -> str:
    """Return a metadata-safe digest for a reason or justification.

    Callers use the digest in events instead of publishing the free text.  The
    authoritative text remains in its governed relational row.
    """

    if not isinstance(value, str) or not value:
        raise ValueError("code_traceability_event_digest_input_invalid")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def make_code_traceability_event(
    event_class: type[TraceabilityEventT],
    /,
    *,
    board_id: str,
    actor_id: str | None,
    actor_type: TraceabilityActorType,
    **metadata: Any,
) -> TraceabilityEventT:
    """Build one exact closed event from metadata-only keyword arguments.

    Pydantic's event schema rejects unknown fields and bounds every value.
    Passing operational locators (path/symbol/excerpt/challenge/secret) is
    therefore impossible for the 17 declared event classes.
    """

    if not isinstance(event_class, type) or not issubclass(
        event_class,
        CodeTraceabilityDomainEvent,
    ):
        raise TypeError("code_traceability_event_class_invalid")
    return event_class(
        board_id=board_id,
        actor_id=actor_id,
        actor_type=actor_type,
        **metadata,
    )


async def publish_code_traceability_mutation(
    uow: object,
    event: CodeTraceabilityDomainEvent,
    *,
    replayed: bool = False,
) -> bool:
    """Stage an event in the mutation's caller-owned UoW; never commit.

    Mutating use cases call this after persistence succeeds and before their
    existing ``commit(uow)``.  Idempotent replays return ``False`` and do not
    duplicate outbox/audit rows.
    """

    if not isinstance(event, CodeTraceabilityDomainEvent):
        raise TypeError("code_traceability_event_invalid")
    if not isinstance(replayed, bool):
        raise TypeError("code_traceability_event_replayed_invalid")
    if replayed:
        return False
    services = getattr(uow, "services", None)
    publish = getattr(services, "publish_domain_event", None)
    if not callable(publish):
        raise RuntimeError("code_traceability_event_publisher_unavailable")
    await publish(event)
    return True


__all__ = [
    "TraceabilityActorType",
    "code_traceability_event_digest",
    "make_code_traceability_event",
    "publish_code_traceability_mutation",
]
