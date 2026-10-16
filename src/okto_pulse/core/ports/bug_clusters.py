"""Authorized, bounded facts for KG §6/§9 Bug cluster reads.

Editions supply a source inventory before graph joins. A source checkpoint must
cover that inventory AND all grouping inputs (origin links, Spec children and
Learning applicability), including mutations that do not increment Spec.version.
Queue depth, graph node count and a reconstruction timestamp are not checkpoints.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol, runtime_checkable

from okto_pulse.core.ports.analytics_foundation import AnalyticsUtcWindow, require_utc_datetime
from okto_pulse.core.ports.kg_query_policy import DEFAULT_QUERY_ROWS, query_row_limit
from okto_pulse.core.domain.enums import BugSeverity, CardStatus

BugClusterGrouping = Literal["proxy", "spec", "learning", "severity"]
ProjectionFreshnessState = Literal["current", "lagging", "incomplete", "unavailable", "unknown"]
MAX_CLUSTER_BUGS = 1000
MAX_CLUSTER_ASSOCIATIONS = 10000
MAX_CLUSTER_RESPONSE_BYTES = 512 * 1024


@dataclass(frozen=True, slots=True)
class BugClustersQuery:
    board_id: str
    actor_scope_ref: str
    window: AnalyticsUtcWindow
    group_by: BugClusterGrouping = "proxy"
    status: str | None = None
    severity: str | None = None
    limit: int = DEFAULT_QUERY_ROWS
    cursor: str | None = None

    def __post_init__(self):
        if any(type(value) is not str or not value.strip() or len(value) > 4096
               for value in (self.board_id, self.actor_scope_ref)):
            raise ValueError("bug_clusters_scope_invalid")
        if not isinstance(self.window, AnalyticsUtcWindow):
            raise ValueError("bug_clusters_window_invalid")
        if self.group_by not in ("proxy", "spec", "learning", "severity"):
            raise ValueError("bug_clusters_grouping_invalid")
        for value in (self.status, self.severity):
            if value is not None and (type(value) is not str or not value or len(value) > 64):
                raise ValueError("bug_clusters_filter_invalid")
        try:
            if self.status is not None:
                CardStatus(self.status)
            if self.severity is not None:
                BugSeverity(self.severity)
        except ValueError as exc:
            raise ValueError("bug_clusters_filter_invalid") from exc
        object.__setattr__(self, "limit", query_row_limit(self.limit))
        if self.cursor is not None and (type(self.cursor) is not str or len(self.cursor) > 256):
            raise ValueError("bug_clusters_cursor_invalid")

    @classmethod
    def recent(cls, *, board_id: str, actor_scope_ref: str, now: datetime, **options):
        end = require_utc_datetime(now, field="bug_clusters_now")
        return cls(board_id, actor_scope_ref, AnalyticsUtcWindow(end - timedelta(days=15), end), **options)


@dataclass(frozen=True, slots=True)
class ClusterBugFact:
    """One authorized source Card; dates are source dates, not KG write dates.

    resolved_at uses latest_resolution_time over lifecycle transitions, and is
    absent after reopening or when chronology cannot be verified. Never borrow
    updated_at or a previous Done episode as a resolution time.
    """
    bug_id: str
    title: str
    source_created_at: datetime
    status: str
    severity: str | None
    # Q06: the Spec of the origin Card (Bug -> Card -> Spec), not an unrelated
    # Spec attached directly to the Bug for its regression work.
    spec_ref: str | None
    source_revision: str
    resolved_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class BugClusterAssociation:
    """One graph observation, already authorized at BOTH endpoints.

    Proxy observations must be qualified by bug_origin_proxy_read_metadata.
    Learning validity is a read result, never inferred from edge presence alone.
    A previous/unknown interpretation remains labelled as such.
    """
    bug_id: str
    kind: Literal["proxy", "learning"]
    target_ref: str
    title: str
    provenance_ref: str
    validity: Literal["current", "previous", "unknown"]


@dataclass(frozen=True, slots=True)
class BugClustersSnapshot:
    board_id: str
    actor_scope_ref: str
    checked_at: datetime
    bugs: tuple[ClusterBugFact, ...]
    # Denominator of the complete authorized, filtered source scope, not a page
    # count. None means the edition cannot verify that denominator.
    expected_bug_count: int | None
    source_inventory_complete: bool
    associations: tuple[BugClusterAssociation, ...] = ()
    graph_available: bool = True
    graph_generation: str | None = None
    expected_projection_checkpoint: str | None = None
    projection_checkpoint: str | None = None
    projected_bug_ids: tuple[str, ...] = ()
    associations_truncated: bool = False


class BugClustersReadPort(Protocol):
    async def read(self, query: BugClustersQuery, *, timeout_ms: int) -> BugClustersSnapshot:
        """Read authorized source inventory and bounded graph observations.

        Authorization must precede aggregation and be checked again on every
        page. Reject foreign/denied endpoints before returning facts. Bound source
        rows, joins, payload and native execution; cancellation must not leave
        unaccounted work. No repair, commit, projection or agent invocation.
        """
        ...


@dataclass(frozen=True, slots=True)
class BugClusterGraphFacts:
    projected_bug_ids: tuple[str, ...]
    associations: tuple[BugClusterAssociation, ...]
    graph_generation: str | None = None
    truncated: bool = False


@runtime_checkable
class BugClustersGraphReadPort(Protocol):
    def read_bug_cluster_graph(
        self, board_id: str, bugs: tuple[ClusterBugFact, ...], *, group_by: BugClusterGrouping,
    ) -> BugClusterGraphFacts:
        """Read within the caller's shared native deadline and immutable route.

        Only requested source Bugs and authorized domain endpoint families may
        participate. Return a durable route generation, never a wall clock or
        process identity. Missing or stale source metadata is not a current Bug.
        This observation does not prove full projection completeness.
        """
        ...
