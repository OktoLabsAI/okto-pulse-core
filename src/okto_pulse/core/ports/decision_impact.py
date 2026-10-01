"""Bounded informational Decision impact over current authorized source facts."""
from dataclasses import dataclass
from okto_pulse.core.ports.spec_coverage_query import SpecCoverageQuery


@dataclass(frozen=True, slots=True)
class DecisionImpactQuery:
    board_id: str
    spec_id: str
    decision_id: str
    actor_scope_ref: str
    limit: int = 200
    cursor: str | None = None
    max_depth: int = 3

    def __post_init__(self):
        self.source_query()  # Reuse the shared scope/row/cursor validation.
        if type(self.decision_id) is not str or not self.decision_id or len(self.decision_id) > 4096 or self.decision_id.strip() != self.decision_id or ':' in self.decision_id:
            raise ValueError('decision_impact_reference_invalid')
        if type(self.max_depth) is not int or not 1 <= self.max_depth <= 8:
            raise ValueError('decision_impact_depth_invalid')

    def source_query(self):
        return SpecCoverageQuery(self.board_id, self.spec_id, self.actor_scope_ref,
            self.limit, self.cursor, read_delivery=False, read_graph=True)
