"""Closed informational response shared by Bug cluster transports."""
from typing import Literal
from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field

from okto_pulse.core.ports.bug_clusters import BugClusterGrouping, ProjectionFreshnessState
from okto_pulse.core.ports.analytics_foundation import AnalyticsUtcWindow
from okto_pulse.core.services.analytics_contract import parse_analytics_datetime


class _ClosedModel(BaseModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=True)


class BugClustersRequest(_ClosedModel):
    """Closed variant of the existing traceability facade, never arbitrary JSON."""
    view: Literal['bugs']
    date_from: str | None = None
    date_to: str | None = None
    group_by: BugClusterGrouping = 'proxy'
    status: str | None = Field(default=None, max_length=64)
    severity: str | None = Field(default=None, max_length=64)
    limit: int = Field(default=200, ge=1, le=1000)
    cursor: str | None = Field(default=None, max_length=256)
    timeout_ms: int | None = Field(default=None, ge=1, le=30000)

    def window(self, now: datetime) -> AnalyticsUtcWindow:
        if self.cursor and (self.date_from is None or self.date_to is None):
            raise ValueError('bug_clusters_cursor_window_required')
        end = parse_analytics_datetime(self.date_to, end_exclusive=True) if self.date_to is not None else now
        start = parse_analytics_datetime(self.date_from) if self.date_from is not None else end - timedelta(days=15) if end else None
        if start is None or end is None:
            raise ValueError('bug_clusters_window_invalid')
        return AnalyticsUtcWindow(start, end)


class BugClustersWindow(_ClosedModel):
    from_inclusive: str = Field(alias='from')
    to_exclusive: str = Field(alias='to')


class BugClustersFreshness(_ClosedModel):
    state: ProjectionFreshnessState
    graph_generation: str | None
    source_checkpoint: str | None
    projection_checkpoint: str | None
    checked_at: str


class BugClustersCompleteness(_ClosedModel):
    complete_for_scope: bool
    truncated: bool
    limitations: list[str]


class BugClusterItem(_ClosedModel):
    target_ref: str | None
    title: str = Field(max_length=240)
    validity: Literal['source', 'current', 'previous', 'unknown']
    assertion_basis: Literal['origin_proxy', 'recorded_learning', 'origin_spec', 'source_severity']
    causal_conclusion: Literal['not_established']
    distinct_bug_count: int | None = Field(ge=0)
    observed_bug_count: int = Field(ge=0)
    observed_done_count: int = Field(ge=0)
    observed_resolution_timestamp_count: int = Field(ge=0)
    observed_median_resolution_hours: float | None = Field(ge=0)
    bug_refs: list[str]
    provenance_refs: list[str]
    projection_freshness: ProjectionFreshnessState | Literal['not_applicable']


class BugClustersResponse(_ClosedModel):
    view: Literal['bugs']
    subject_ref: str
    group_by: BugClusterGrouping
    window: BugClustersWindow
    authority: Literal['informational']
    data_source: Literal['mixed_relational_graph', 'relational']
    projection_freshness: BugClustersFreshness
    completeness: BugClustersCompleteness
    distinct_bug_count: int | None = Field(ge=0)
    observed_bug_count: int = Field(ge=0)
    cluster_count: int | None = Field(ge=0)
    observed_cluster_count: int = Field(ge=0)
    resolution_semantics: Literal['latest_verified_done_transition_for_current_done_bugs; missing_timestamp_is_unknown']
    items: list[BugClusterItem]
    next_cursor: str | None
