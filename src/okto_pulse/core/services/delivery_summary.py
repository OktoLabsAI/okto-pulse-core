"""Pure presentation over canonical facts, current Card metadata and one generation.

Authorization, source consistency and cursor authentication belong to the read
adapter. This service performs no reads, writes, proof evaluation or transitions.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from hashlib import sha256
import orjson

from okto_pulse.core.models.delivery_readiness import DeliveryReadinessContext
from okto_pulse.core.models.delivery_summary import (
    DeliveryGroup, DeliverySummary, DeliverySummaryCard, DeliverySummaryCardFacts,
    DeliverySummaryGroup, DeliverySummaryProjection, DeliverySummaryScope, DeliverySummaryTask,
)

GROUPS = ('delivered', 'in_progress', 'pending', 'optional_or_waived')


@dataclass(frozen=True, slots=True)
class DeliverySummaryPopulation:
    scope: DeliverySummaryScope
    projection: DeliverySummaryProjection
    complete: bool
    unique_obligation_count: int | None
    items: tuple[DeliverySummaryTask, ...]
    population_id: str


def _check_scope(context, scope):
    value = context.scope
    if ((value.board_id, value.spec_id, value.spec_edition)
            != (scope.board_id, scope.spec_id, scope.spec_edition)
            or value.spec_version > scope.spec_version):
        raise ValueError('delivery_summary_scope_mismatch')


def _known(context):
    return (context.scope_complete and context.evaluation_complete
        and context.implementation.status != 'unknown' and context.verification.status != 'unknown')


def _proof_complete(context):
    # A percent is never completion authority. Preserve the canonical outcome
    # and also refuse an internally inconsistent context.
    return (context.proof_complete is True and _known(context)
        and context.implementation.unresolved == context.verification.unresolved == 0
        and context.delivery_gate.status == 'allowed'
        and context.lifecycle.qualification_is_current is True
        and context.delivery_state in {'delivered', 'not_applicable'})


def _task(card, facts, *, current):
    context = facts.context if facts else None
    aligned = bool(context and context.scope.card_id == card.card_id
        and context.scope.card_type == card.card_type
        and context.scope.card_version == card.card_version
        and context.lifecycle.stored_status == card.status)
    applicable = facts.requirement if current and aligned and facts else None
    requirement = applicable.decision if applicable and applicable.current else 'unknown'
    group, reason = 'pending', 'unavailable'
    if requirement in {'optional', 'waived'}:
        group, reason = 'optional_or_waived', requirement
    elif card.status == 'in_progress':
        group, reason = 'in_progress', 'in_progress'
    elif card.status == 'validation':
        reason = 'awaiting_validation'
    elif card.status == 'on_hold':
        reason = 'paused'
    elif card.status == 'rejected':
        reason = 'needs_rework'
    elif card.status == 'cancelled':
        reason = 'cancelled'
    elif card.status == 'not_started':
        reason = 'awaiting_dependency' if card.dependency_blocked is True else 'not_started'
    elif card.status == 'done':
        if current and aligned and requirement == 'required' and _proof_complete(context):
            group, reason = 'delivered', 'delivered'
        else:
            reason = 'awaiting_confirmation' if current else 'updating'
    return DeliverySummaryTask(card_id=card.card_id, title=card.title, card_type=card.card_type,
        status=card.status, assignee_id=card.assignee_id, updated_at=card.updated_at,
        group=group, reason=reason, requirement=requirement, historical_done=card.status == 'done',
        exception_applied=bool(current and aligned and context.delivery_gate.exception_applied))


def build_delivery_summary_population(*, scope: DeliverySummaryScope,
        projection: DeliverySummaryProjection, cards: Sequence[DeliverySummaryCard],
        facts: Sequence[DeliverySummaryCardFacts], spec_context: DeliveryReadinessContext | None,
        population_complete: bool) -> DeliverySummaryPopulation:
    if type(population_complete) is not bool:
        raise ValueError('delivery_summary_population_invalid')
    by_card = {card.card_id: card for card in cards}
    by_fact = {row.context.scope.card_id: row for row in facts}
    if len(by_card) != len(cards) or len(by_fact) != len(facts) or None in by_fact:
        raise ValueError('delivery_summary_duplicate_or_unscoped_card')
    if any((card.board_id, card.spec_id) != (scope.board_id, scope.spec_id) for card in cards):
        raise ValueError('delivery_summary_scope_mismatch')
    for row in facts:
        _check_scope(row.context, scope)
        if row.context.snapshot_id != projection.snapshot_id:
            raise ValueError('delivery_summary_snapshot_mismatch')
    if spec_context is not None:
        _check_scope(spec_context, scope)
        if spec_context.snapshot_id != projection.snapshot_id:
            raise ValueError('delivery_summary_snapshot_mismatch')
        if (spec_context.scope.card_id is not None or spec_context.implementation.unit != 'obligation'
                or spec_context.verification.unit != 'obligation'):
            raise ValueError('delivery_summary_global_scope_invalid')
    current = projection.freshness == 'current'
    if current and any(row.context.scope.spec_version != scope.spec_version for row in facts):
        raise ValueError('delivery_summary_current_version_mismatch')
    if current and spec_context and spec_context.scope.spec_version != scope.spec_version:
        raise ValueError('delivery_summary_current_version_mismatch')
    items = tuple(_task(card, by_fact.get(card.card_id), current=current) for card in cards)
    aligned = all(row.context.scope.card_type == by_card[identity].card_type
        and row.context.scope.card_version == by_card[identity].card_version
        and row.context.lifecycle.stored_status == by_card[identity].status
        and row.requirement is not None and row.requirement.current
        # A current human decision is sufficient to classify an exception.
        # Technical proof is still required to classify required work and to
        # grant delivery credit; it is not evidence that this decision is stale.
        and (row.requirement.decision in {'optional', 'waived'} or _known(row.context))
        for identity, row in by_fact.items() if identity in by_card)
    complete = bool(current and population_complete and set(by_card) == set(by_fact)
        and aligned and spec_context is not None and _known(spec_context))
    total = spec_context.implementation.total if complete else None
    items = tuple(sorted(items, key=lambda item: (item.title.casefold(), item.card_id)))
    # Current titles/statuses can change while the previous generation remains
    # visible as updating. A generation ID alone cannot fence sorted Card pages.
    population_id = sha256(orjson.dumps(dict(contract_version='delivery-summary-population/v1',
        scope=scope.model_dump(mode='json'), projection=projection.model_dump(mode='json'),
        complete=complete, unique_obligation_count=total,
        items=[item.model_dump(mode='json') for item in items]), option=orjson.OPT_SORT_KEYS)).hexdigest()
    return DeliverySummaryPopulation(scope, projection, complete, total, items, population_id)


def page_delivery_summary(population: DeliverySummaryPopulation, *, group: DeliveryGroup | None = None,
        offset: int = 0, limit: int = 10,
        encode_cursor: Callable[[str, int], str] | None = None) -> DeliverySummary:
    if (group is not None and group not in GROUPS or type(offset) is not int or offset < 0
            or type(limit) is not int or not 1 <= limit <= 20 or group is None and offset != 0):
        raise ValueError('delivery_summary_page_invalid')
    rows = {identity: tuple(item for item in population.items if item.group == identity) for identity in GROUPS}
    pages = []
    for identity in GROUPS:
        # The initial three visible sections are bounded independently. The
        # exception list remains unopened until the user requests that section.
        loaded = identity == group if group is not None else identity != 'optional_or_waived'
        start = offset if group == identity else 0
        size = limit if group is not None else min(limit, 5)
        items = rows[identity][start:start + size] if loaded else ()
        has_more = loaded and start + len(items) < len(rows[identity])
        if has_more and encode_cursor is None:
            raise ValueError('delivery_summary_cursor_encoder_required')
        cursor = encode_cursor(identity, start + len(items)) if has_more else None
        pages.append(DeliverySummaryGroup(id=identity,
            count=len(rows[identity]) if population.complete else None,
            loaded=loaded, items=items, has_more=has_more, next_cursor=cursor))
    return DeliverySummary(scope=population.scope, projection=population.projection,
        population_id=population.population_id,
        complete=population.complete, task_count=len(population.items) if population.complete else None,
        required_task_count=sum(item.group != 'optional_or_waived' for item in population.items)
            if population.complete else None,
        unique_obligation_count=population.unique_obligation_count, groups=tuple(pages))
