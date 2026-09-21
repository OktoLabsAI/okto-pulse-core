"""Public, read-only preparation of the live deterministic projection contract.

An expected projection is neither a reconciliation receipt nor proof of graph
materialization. Offline migration must still build, verify and cut over its
candidate under its own source and writer fences.
"""

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from typing import Protocol

from .consolidation import ConsolidationPersistencePort


@dataclass(frozen=True, slots=True)
class DeterministicProjectionSource:
    board_id: str
    artifact_type: str
    artifact_id: str

    def __post_init__(self):
        if (any(type(value) is not str or not value.strip() or len(value) > 256
                for value in (self.board_id, self.artifact_id))
                or type(self.artifact_type) is not str or self.artifact_type not in {'story', 'ideation', 'refinement', 'spec', 'card',
                    'amendment_hotfix_revision', 'code_investigation_receipt', 'code_evidence', 'implementation_target'}):
            raise ValueError('deterministic_projection_source_invalid')


@dataclass(frozen=True, slots=True)
class DeterministicProjectionPlan:
    """Immutable canonical document; includes unresolved links and cleanup intent."""

    document: bytes

    def __post_init__(self):
        if type(self.document) is not bytes or not 0 < len(self.document) <= 16 * 1024 * 1024:
            raise ValueError('deterministic_projection_plan_limit')
        value = json.loads(self.document)
        if (type(value) is not dict or value.get('format') != 'deterministic-projection-plan/v1'
                or json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode() != self.document):
            raise ValueError('deterministic_projection_plan_invalid')

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.document).hexdigest()


class DeterministicProjectionPlanner(Protocol):
    async def prepare(self, context: object, source: DeterministicProjectionSource) -> DeterministicProjectionPlan: ...

    async def prepare_board(self, context: object, *, board_id: str, source_rows: tuple[dict, ...],
            cognitive_rows: tuple[dict, ...], captured_at: datetime) -> bytes:
        """Canonical bounded census and expected emissions, never graph completion."""
        ...


class DeterministicProjectionDependencies(Protocol):
    def resolve(self, *, board_id: str, sources: tuple[dict, ...]) -> tuple[dict, ...]:
        """Revalidate manifest identities and select required historical endpoints."""
        ...


def make_deterministic_projection_planner(persistence: ConsolidationPersistencePort, *,
        dependencies: DeterministicProjectionDependencies | None = None) -> DeterministicProjectionPlanner:
    from okto_pulse.core.application.deterministic_projection import CoreDeterministicProjectionPlanner
    return CoreDeterministicProjectionPlanner(persistence, dependencies=dependencies)
