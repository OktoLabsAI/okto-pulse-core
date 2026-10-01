"""Closed source-lineage query in the existing traceability facade."""
from typing import Literal
from pydantic import Field
from okto_pulse.core.models.spec_coverage_query import Closed, Freshness


class LineageRequest(Closed):
    view: Literal['lineage']
    subject_ref: str = Field(pattern=r'^(spec|card|ideation|refinement|story|amendment_hotfix_revision):[^:\s]+$', max_length=4096)
    limit: int = Field(default=200, ge=1, le=1000)
    cursor: str | None = Field(default=None, max_length=256)
    max_depth: int = Field(default=3, ge=1, le=32)
    timeout_ms: int | None = Field(default=None, ge=1, le=30000)


class LineagePathStep(Closed):
    source_ref: str
    target_ref: str
    relation: Literal['precedes', 'derived_from', 'belongs_to', 'originates_bug', 'feeds_ideation', 'amendment_of', 'affects', 'regression_test']
    provenance_ref: str
    direction: Literal['incoming', 'outgoing']


class LineageItem(Closed):
    subject_ref: str
    entity_type: str
    title: str = Field(max_length=240)
    status: str
    depth: int = Field(ge=1, le=32)
    path: list[LineagePathStep] = Field(max_length=32)


class LineageScope(Closed):
    max_depth: int
    path_selection: Literal['one_shortest_path_per_target']
    interpretation: Literal['workflow_origins_dependencies_and_amendments_not_execution_or_delivery']


class LineageCounts(Closed):
    source_nodes: int
    source_relations: int
    reached_targets: int


class LineageCompleteness(Closed):
    complete_for_scope: bool
    truncated: bool
    limitations: list[str]


class LineageResponse(Closed):
    view: Literal['lineage']
    subject_ref: str
    authority: Literal['informational']
    data_source: Literal['relational']
    scope: LineageScope
    projection_freshness: Freshness
    completeness: LineageCompleteness
    counts: LineageCounts
    frontier_refs: list[str]
    items: list[LineageItem]
    next_cursor: str | None
