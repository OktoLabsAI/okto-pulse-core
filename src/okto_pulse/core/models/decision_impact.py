"""Closed informational Decision impact variant of the traceability facade."""
from typing import Literal
from pydantic import Field
from okto_pulse.core.models.spec_coverage_query import Closed, Completeness, Freshness


class DecisionImpactRequest(Closed):
    view: Literal['impact']
    subject_ref: str = Field(pattern=r'^spec:[^:\s]+:decision:[^:\s]+$', max_length=4096)
    limit: int = Field(default=200, ge=1, le=1000)
    cursor: str | None = Field(default=None, max_length=256)
    max_depth: int = Field(default=3, ge=1, le=8)
    timeout_ms: int | None = Field(default=None, ge=1, le=30000)


class ImpactPathStep(Closed):
    source_ref: str
    relation: str
    target_ref: str
    direction: Literal['incoming', 'outgoing']
    rule_id: str
    layer: str
    created_by: str
    source_confirmed: bool
    graph_observed: bool


class ImpactItem(Closed):
    target_ref: str
    target_type: str
    title: str = Field(max_length=240)
    status: str
    reach: Literal['direct', 'indirect']
    certainty: Literal['potential', 'confirmed_link']
    interpretation: Literal['potential_shared_card_reach', 'graph_only_observation', 'declared_link_not_proven_change']
    path: list[ImpactPathStep] = Field(max_length=8)


class DecisionHistoryItem(Closed):
    subject_ref: str
    title: str = Field(max_length=240)
    status: str
    supersedes_ref: str | None


class ImpactScope(Closed):
    spec_ref: str
    max_depth: int
    path_selection: Literal['one_representative_path_per_target_prefer_confirmed_link']


class ImpactCounts(Closed):
    observed_targets: int
    confirmed_link_targets: int
    potential_targets: int


class DecisionImpactResponse(Closed):
    view: Literal['impact']
    subject_ref: str
    authority: Literal['informational']
    decision_status: str
    current_decision: bool
    data_source: Literal['relational', 'composed']
    scope: ImpactScope
    projection_freshness: Freshness
    completeness: Completeness
    counts: ImpactCounts
    frontier_refs: list[str]
    history: list[DecisionHistoryItem]
    items: list[ImpactItem]
    next_cursor: str | None
