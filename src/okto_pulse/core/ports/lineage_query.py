"""Bounded source lineage facts supplied by an edition-owned reader."""
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Literal, Protocol
from okto_pulse.core.ports.spec_coverage_query import SpecCoverageQuery

MAX_LINEAGE_NODES = 2000
MAX_LINEAGE_EDGES = 10000
MAX_LINEAGE_BYTES = 512 * 1024
LineageRelationKind = Literal['precedes', 'derived_from', 'belongs_to', 'originates_bug', 'feeds_ideation', 'amendment_of', 'affects', 'regression_test']


@dataclass(frozen=True, slots=True)
class LineageQuery:
    board_id: str
    subject_ref: str
    actor_scope_ref: str
    limit: int = 200
    cursor: str | None = None
    max_depth: int = 3

    def __post_init__(self):
        if not re.fullmatch(r'(spec|card|ideation|refinement|story|amendment_hotfix_revision):[^:\s]+', self.subject_ref):
            raise ValueError('lineage_reference_invalid')
        SpecCoverageQuery(self.board_id, self.subject_ref.split(':')[1], self.actor_scope_ref,
            self.limit, self.cursor, read_delivery=False, read_graph=False)
        if type(self.max_depth) is not int or not 1 <= self.max_depth <= 32:
            raise ValueError('lineage_depth_invalid')


@dataclass(frozen=True, slots=True)
class LineageNode:
    subject_ref: str
    entity_type: str
    title: str
    status: str


@dataclass(frozen=True, slots=True)
class LineageRelation:
    source_ref: str
    target_ref: str
    relation: LineageRelationKind
    provenance_ref: str


@dataclass(frozen=True, slots=True)
class LineageSnapshot:
    board_id: str
    subject_ref: str
    actor_scope_ref: str
    source_revision: str
    checked_at: datetime
    nodes: tuple[LineageNode, ...]
    relations: tuple[LineageRelation, ...]
    source_complete: bool
    limitations: tuple[str, ...] = ()


class LineageReadPort(Protocol):
    async def read_lineage_snapshot(self, context: object, query: LineageQuery, *, timeout_ms: int) -> LineageSnapshot: ...
