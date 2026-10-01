"""Authorized source facts for the Spec coverage view (KG §6.4).

The edition reads the entire bounded Spec scope before pagination. Source
revision must cover collection/link/Card changes, including those which leave
Spec.version unchanged. Delivery facts come from the existing admitted-proof
reader; graph edges and Card status are never substitutes for those facts.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from okto_pulse.core.domain.delivery_evidence import DeliveryEvidenceSnapshot, DeliveryScope
from okto_pulse.core.ports.kg_query_policy import query_row_limit


MAX_SPEC_COVERAGE_ITEMS = 1000
MAX_SPEC_COVERAGE_FACTS = 10000
MAX_SPEC_COVERAGE_BYTES = 512 * 1024


@dataclass(frozen=True, slots=True)
class SpecCoverageQuery:
    board_id: str
    spec_id: str
    actor_scope_ref: str
    limit: int = 200
    cursor: str | None = None
    read_delivery: bool = True

    def __post_init__(self):
        for value in (self.board_id, self.spec_id, self.actor_scope_ref):
            if type(value) is not str or not value.strip() or len(value) > 4096:
                raise ValueError('spec_coverage_scope_invalid')
        object.__setattr__(self, 'limit', query_row_limit(self.limit))
        if type(self.read_delivery) is not bool:
            raise ValueError('spec_coverage_delivery_authority_invalid')
        if self.cursor is not None and (type(self.cursor) is not str or len(self.cursor) > 256):
            raise ValueError('spec_coverage_cursor_invalid')


@dataclass(frozen=True, slots=True)
class SpecCoverageSnapshot:
    """Internal edition DTO, not a request schema or a client attestation.

    spec/cards expose the domain attributes consumed by spec_coverage_summary.
    They contain authorized relational facts, with no ORM mechanics in Core.
    delivery is the same edition-scoped snapshot used by the delivery gate.
    A missing/restricted proof read carries neither rows nor hidden counts.
    """
    scope: DeliveryScope
    actor_scope_ref: str
    source_revision: str
    checked_at: datetime
    spec: object
    cards: tuple[object, ...]
    source_complete: bool
    delivery: DeliveryEvidenceSnapshot | None
    delivery_state: Literal['available', 'restricted', 'unavailable'] = 'available'


class SpecCoverageReadPort(Protocol):
    async def read(self, query: SpecCoverageQuery, *, timeout_ms: int) -> SpecCoverageSnapshot:
        """Read only: no graph repair, proof execution, waiver or source mutation."""
        ...
