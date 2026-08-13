"""Relational persistence boundary for the consolidation processor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, Sequence

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


@dataclass(slots=True)
class ConsolidationQueueRecord:
    id: str
    board_id: str
    artifact_type: str
    artifact_id: str
    status: str
    attempts: int
    last_error: str | None
    next_retry_at: datetime | None
    claimed_at: datetime | None
    claim_timeout_at: datetime | None
    worker_id: str | None
    claimed_by_session_id: str | None
    triggered_at: datetime | None
    priority: str
    work_kind: str = "consolidate"
    generation: int = 0
    payload: dict[str, Any] | None = None
    delete_event_id: str | None = None
    claim_token: str | None = None
    triggered_by_event: str | None = None


@dataclass(frozen=True, slots=True)
class ConsolidationPoisonRow:
    id: str
    attempts: int


@dataclass(frozen=True, slots=True)
class CurrentQualityAssessmentSummary:
    """Current quality head joined to its immutable receipt."""

    board_id: str
    subject_type: str
    subject_id: str
    subject_version: int
    subject_edition: int | None
    assessment_kind: str
    receipt_id: str
    head_revision: int
    outcome: str
    score: float
    justification: str
    scale_kind: str
    scale_minimum: float
    scale_maximum: float
    scale_direction: str
    content_digest: str
    clarification_digest: str
    ruleset_digest: str
    taxonomy_digest: str
    policy_digest: str
    input_digest: str
    canonicalization_version: str
    ruleset_version: str
    taxonomy_version: str
    analyzer_version: str
    policy_version: str
    created_at: datetime
    updated_at: datetime
    projection_fingerprint: str

    def to_worker_dict(self) -> dict[str, Any]:
        """Return the bounded root-summary shape consumed by the pure worker."""

        return {
            "board_id": self.board_id,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "subject_version": self.subject_version,
            "subject_edition": self.subject_edition,
            "assessment_kind": self.assessment_kind,
            "receipt_id": self.receipt_id,
            "head_revision": self.head_revision,
            "outcome": self.outcome,
            "score": self.score,
            "justification": self.justification,
            "scale_kind": self.scale_kind,
            "scale_minimum": self.scale_minimum,
            "scale_maximum": self.scale_maximum,
            "scale_direction": self.scale_direction,
            "input_digest": self.input_digest,
            "ruleset_version": self.ruleset_version,
            "taxonomy_version": self.taxonomy_version,
            "analyzer_version": self.analyzer_version,
            "policy_version": self.policy_version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "projection_fingerprint": self.projection_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class CurrentResearchDecisionSummary:
    """Current RDL head joined to its immutable entry."""

    board_id: str
    refinement_id: str
    refinement_version: int
    ledger_id: str
    entry_id: str
    head_revision: int
    predecessor_entry_id: str | None
    unknown: str
    status: str
    anchor_type: str
    anchor_ref: str
    evidence_refs: tuple[str, ...]
    alternatives: tuple[str, ...]
    decision: str | None
    rationale: str | None
    confidence: float | None
    evidence_absence_justification: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    projection_fingerprint: str

    def to_worker_dict(self) -> dict[str, Any]:
        """Return the current-head shape consumed by the pure worker."""

        return {
            "board_id": self.board_id,
            "refinement_id": self.refinement_id,
            "refinement_version": self.refinement_version,
            "ledger_id": self.ledger_id,
            "entry_id": self.entry_id,
            "head_revision": self.head_revision,
            "predecessor_entry_id": self.predecessor_entry_id,
            "unknown": self.unknown,
            "status": self.status,
            "anchor_type": self.anchor_type,
            "anchor_ref": self.anchor_ref,
            "evidence_refs": list(self.evidence_refs),
            "alternatives": list(self.alternatives),
            "decision": self.decision,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "evidence_absence_justification": (
                self.evidence_absence_justification
            ),
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "projection_fingerprint": self.projection_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class CurrentSpecDependencyProjection:
    """One authoritative active Spec dependency plus endpoint projection."""

    dependency_id: str
    board_id: str
    dependent_spec_id: str
    prerequisite_spec_id: str
    prerequisite_title: str
    prerequisite_status: str
    prerequisite_version: int

    def to_worker_dict(self) -> dict[str, Any]:
        return {
            "dependency_id": self.dependency_id,
            "board_id": self.board_id,
            "dependent_spec_id": self.dependent_spec_id,
            "prerequisite_spec_id": self.prerequisite_spec_id,
            "prerequisite_title": self.prerequisite_title,
            "prerequisite_status": self.prerequisite_status,
            "prerequisite_version": self.prerequisite_version,
        }


@dataclass(frozen=True, slots=True)
class ConsolidationProjectionInputs:
    """Bounded relational projections loaded alongside one source artifact."""

    quality_assessments: tuple[CurrentQualityAssessmentSummary, ...] = ()
    research_decisions: tuple[CurrentResearchDecisionSummary, ...] = ()
    spec_dependencies: tuple[CurrentSpecDependencyProjection, ...] = ()


class ConsolidationPersistencePort(Protocol):
    async def load_artifact(
        self,
        context: Any,
        *,
        artifact_type: str,
        artifact_id: str,
    ) -> Any | None: ...

    async def load_projection_inputs(
        self,
        context: Any,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        artifact: Any,
    ) -> ConsolidationProjectionInputs:
        """Load current relational heads for an already-loaded artifact."""

        ...

    async def list_artifacts(
        self,
        context: Any,
        *,
        artifact_type: str,
        artifact_ids: Sequence[str],
        board_id: str | None = None,
    ) -> tuple[Any, ...]: ...

    async def list_stale_claims(
        self,
        context: Any,
        *,
        now: datetime,
        legacy_cutoff: datetime,
    ) -> tuple[ConsolidationQueueRecord, ...]: ...

    async def count_pending(self, context: Any) -> int: ...

    async def list_claimed_board_ids(self, context: Any) -> frozenset[str]: ...

    async def list_ready_pending(
        self, context: Any, *, now: datetime
    ) -> tuple[ConsolidationQueueRecord, ...]: ...

    async def get_queue_entry(
        self, context: Any, *, entry_id: str
    ) -> ConsolidationQueueRecord | None: ...

    async def queue_claim_is_current_and_unfenced(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        """Validate claim ownership and the governed-deletion generation fence.

        ``consolidate`` is valid only while no tombstone exists for the
        artifact. ``stale_reconcile`` is valid only while the tombstone has
        the exact generation and delete-event identity carried by the work
        row. Implementations must evaluate the claim and tombstone predicates
        in one storage statement/snapshot.
        """
        ...

    async def ack_claimed_queue_entry(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        """Delete one claimed row by compare-and-swap identity.

        Returns ``True`` only when exactly one row matched the id, claimed
        status, claim token, generation and null-safe delete-event identity.
        """
        ...

    async def save_queue_entries(
        self, context: Any, entries: Sequence[ConsolidationQueueRecord]
    ) -> None: ...

    async def delete_queue_entry(self, context: Any, *, entry_id: str) -> None: ...

    async def discard_artifact_work(
        self,
        context: Any,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
    ) -> None:
        """Discard legacy KG work without committing the caller's unit of work."""
        ...

    async def board_exists(self, context: Any, *, board_id: str) -> bool: ...

    async def list_dlq_auto_drain_board_ids(
        self, context: Any
    ) -> tuple[str, ...]: ...

    async def count_dead_letters(self, context: Any, *, board_id: str) -> int: ...

    async def delete_poison_dead_letters(
        self, context: Any, *, board_id: str, max_attempts: int
    ) -> tuple[ConsolidationPoisonRow, ...]: ...

    async def commit(self, context: Any) -> None: ...

    async def rollback(self, context: Any) -> None: ...


_RUNTIME_KEY = "ports.consolidation.port"


def register_consolidation_persistence_port(
    port: ConsolidationPersistencePort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_consolidation_persistence_port() -> ConsolidationPersistencePort:
    return require_runtime_value(_RUNTIME_KEY, "consolidation_persistence_port_not_configured")


def reset_consolidation_persistence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ConsolidationPersistencePort",
    "ConsolidationPoisonRow",
    "ConsolidationProjectionInputs",
    "ConsolidationQueueRecord",
    "CurrentQualityAssessmentSummary",
    "CurrentResearchDecisionSummary",
    "CurrentSpecDependencyProjection",
    "get_consolidation_persistence_port",
    "register_consolidation_persistence_port",
    "reset_consolidation_persistence_port_for_tests",
]
