"""Edition-neutral values for atomic architecture classification persistence.

Decisions are append-only source witnesses, local to a Spec edition. They do
not waive IRs or confer permission to edit a locked Spec. Core prepares them
after authorization/source validation; the edition owns transactional storage.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Literal


ArchitectureDisposition = Literal[
    "promote_to_ir", "associate_existing_ir", "context_only"
]


@dataclass(frozen=True, slots=True)
class ArchitectureDecisionRecord:
    id: str
    spec_id: str
    spec_edition: int
    spec_version: int
    candidate_id: str
    source_digest: str
    root_design_id: str
    interface_id: str
    # Canonical JSON retains the complete analyzed contract, not only the
    # clauses expressible in the IR schema. No adapter fetches schema_ref.
    source_contract_json: str
    adopted_sources: tuple[tuple[str, int], ...]
    actor_id: str
    classified_at: datetime
    disposition: ArchitectureDisposition
    integration_requirement_ids: tuple[str, ...]
    scope_paths: tuple[str, ...] = ("",)
    reason: str | None = None
    remainder_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ArchitectureClassificationReceipt:
    board_id: str
    spec_id: str
    actor_id: str
    idempotency_key: str
    request_digest: str
    result: dict[str, Any]


class ArchitectureClassificationPersistenceState(str, Enum):
    APPLIED = "applied"
    REPLAYED = "replayed"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    VERSION_CONFLICT = "version_conflict"


@dataclass(frozen=True, slots=True)
class ArchitectureClassificationPersistenceResult:
    state: ArchitectureClassificationPersistenceState
    receipt: ArchitectureClassificationReceipt | None = None
