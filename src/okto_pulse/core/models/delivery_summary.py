"""Human delivery presentation. These derived reads grant no write authority."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field, StrictBool, model_validator

from okto_pulse.core.domain.enums import CardStatus, CardType
from .delivery_readiness import (
    Count, Digest, Identity, ReadinessDTO, Version, DeliveryReadinessContext,
)

DeliveryGroup = Literal['delivered', 'in_progress', 'pending', 'optional_or_waived']
RequirementChoice = Literal['required', 'optional', 'waived']
DeliveryReason = Literal['delivered', 'in_progress', 'not_started', 'awaiting_validation',
    'paused', 'needs_rework', 'cancelled', 'awaiting_dependency', 'awaiting_confirmation',
    'updating', 'unavailable', 'optional', 'waived']


class DeliverySummaryQuery(ReadinessDTO):
    board_id: Identity
    spec_id: Identity
    group: DeliveryGroup | None = None
    limit: Annotated[int, Field(strict=True, ge=1, le=20)] = 10
    cursor: Annotated[str, Field(strict=True, min_length=1, max_length=8192)] | None = None

    @model_validator(mode='after')
    def continuation_has_one_group(self):
        if self.cursor is not None and self.group is None:
            raise ValueError('delivery_summary_cursor_group_required')
        return self


class DeliverySummaryCursorBinding(ReadinessDTO):
    actor_id: Identity
    actor_kind: Identity
    realm_id: Identity | None
    board_id: Identity
    spec_id: Identity
    spec_edition: Version
    population_id: Digest
    group: DeliveryGroup
    limit: Annotated[int, Field(strict=True, ge=1, le=20)]


class DeliverySummaryScope(ReadinessDTO):
    board_id: Identity
    spec_id: Identity
    spec_edition: Version
    spec_version: Version


class DeliverySummaryCard(ReadinessDTO):
    """Current, authorized Card metadata, loaded together without proof evaluation."""
    board_id: Identity
    spec_id: Identity
    card_id: Identity
    card_version: Version
    card_type: CardType
    status: CardStatus
    title: Annotated[str, Field(strict=True, min_length=1, max_length=500)]
    assignee_id: Identity | None = None
    updated_at: datetime | None = None
    dependency_blocked: StrictBool | None = None


class TaskRequirementSummary(ReadinessDTO):
    decision: RequirementChoice
    current: StrictBool
    decision_id: Identity | None = None


class DeliverySummaryCardFacts(ReadinessDTO):
    """One existing canonical assessment and its captured requirement axis."""
    context: DeliveryReadinessContext
    requirement: TaskRequirementSummary | None = None


class DeliverySummaryTask(ReadinessDTO):
    card_id: Identity
    title: Annotated[str, Field(strict=True, min_length=1, max_length=500)]
    card_type: CardType
    status: CardStatus
    assignee_id: Identity | None
    updated_at: datetime | None
    group: DeliveryGroup
    reason: DeliveryReason
    requirement: Literal['required', 'optional', 'waived', 'unknown']
    historical_done: StrictBool
    exception_applied: StrictBool


class DeliverySummaryProjection(ReadinessDTO):
    generation_id: Identity | None
    snapshot_id: Digest | None
    freshness: Literal['current', 'updating', 'pending_initial', 'unavailable']

    @model_validator(mode='after')
    def current_has_a_base(self):
        if self.freshness == 'current' and (self.generation_id is None or self.snapshot_id is None):
            raise ValueError('delivery_summary_current_base_required')
        return self


class DeliverySummaryGroup(ReadinessDTO):
    id: DeliveryGroup
    count: Count | None
    loaded: StrictBool
    items: Annotated[tuple[DeliverySummaryTask, ...], Field(max_length=20)]
    has_more: StrictBool
    next_cursor: Annotated[str, Field(strict=True, min_length=1, max_length=8192)] | None

    @model_validator(mode='after')
    def page_is_coherent(self):
        if self.has_more != bool(self.next_cursor):
            raise ValueError('delivery_summary_cursor_invalid')
        if not self.loaded and (self.items or self.has_more or self.next_cursor):
            raise ValueError('delivery_summary_unopened_group_has_items')
        if any(item.group != self.id for item in self.items):
            raise ValueError('delivery_summary_group_mismatch')
        if self.count is not None and self.count < len(self.items):
            raise ValueError('delivery_summary_count_invalid')
        return self


class DeliverySummary(ReadinessDTO):
    contract_version: Literal['delivery-summary/v1'] = 'delivery-summary/v1'
    scope: DeliverySummaryScope
    projection: DeliverySummaryProjection
    population_id: Digest
    complete: StrictBool
    task_count: Count | None
    required_task_count: Count | None
    unique_obligation_count: Count | None
    groups: Annotated[tuple[DeliverySummaryGroup, ...], Field(min_length=4, max_length=4)]
    mutation_reauthorization_required: Literal[True] = True

    @model_validator(mode='after')
    def counts_have_one_population(self):
        if tuple(group.id for group in self.groups) != ('delivered', 'in_progress', 'pending', 'optional_or_waived'):
            raise ValueError('delivery_summary_groups_invalid')
        totals = (self.task_count, self.required_task_count, self.unique_obligation_count,
                  *(group.count for group in self.groups))
        if self.complete:
            if any(value is None for value in totals):
                raise ValueError('delivery_summary_complete_counts_required')
            if (sum(group.count for group in self.groups) != self.task_count
                    or sum(group.count for group in self.groups[:3]) != self.required_task_count):
                raise ValueError('delivery_summary_population_mismatch')
        elif any(value is not None for value in totals):
            raise ValueError('delivery_summary_incomplete_counts_must_be_unknown')
        return self
