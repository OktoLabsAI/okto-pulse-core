"""Relational persistence boundary for the consolidation processor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
import re
from typing import Any, Callable, Protocol, Sequence

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


_EXACT_REBUILD_SOURCE_ARTIFACT_TYPES = frozenset(
    {
        "story",
        "ideation",
        "refinement",
        "spec",
        "sprint",
        "task",
        "test",
        "bug",
        "card",
        "amendment_hotfix_revision",
        "code_investigation_receipt",
        "code_evidence",
        "implementation_target",
    }
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
    # Added after the original positional surface.  Keep this at the tail so
    # integrations that still construct queue records positionally do not
    # silently shift ``work_kind``/generation/payload into the wrong fields.
    source: str = "state_transition"


@dataclass(frozen=True, slots=True)
class ConsolidationClaimScope:
    """Exact queue membership admitted by an offline recovery processor.

    This scope is intentionally narrower than the ordinary runner contract:
    one board, one rebuild source fence and only consolidation work.  It is
    consumed directly by recovery tooling; it does not authorize global
    stale-claim recovery or DLQ auto-drain.
    """

    board_id: str
    source: str
    work_kind: str = "consolidate"
    # Stable across a crash/reacquired reservation for this exact governed
    # rebuild run.  The current ephemeral reservation token/epoch is proven
    # separately by ``reservation_authority_probe`` at every mutation fence.
    reservation_lineage_id: str | None = None

    def __post_init__(self) -> None:
        if not self.board_id.strip():
            raise ValueError("consolidation_claim_scope_board_id_required")
        if self.work_kind != "consolidate":
            raise ValueError("consolidation_claim_scope_work_kind_invalid")
        if (
            not self.source.startswith("rebuild:")
            or not self.source.removeprefix("rebuild:").strip()
        ):
            raise ValueError("consolidation_claim_scope_source_invalid")
        if self.reservation_lineage_id is not None and (
            type(self.reservation_lineage_id) is not str
            or len(self.reservation_lineage_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.reservation_lineage_id
            )
        ):
            raise ValueError("consolidation_claim_scope_reservation_lineage_invalid")

    def admits(self, entry: ConsolidationQueueRecord) -> bool:
        return (
            entry.board_id == self.board_id
            and entry.source == self.source
            and entry.work_kind == self.work_kind
        )


class ExactConsolidationDisposition(str, Enum):
    ACKED = "acked"
    RETRY_SCHEDULED = "retry_scheduled"
    TERMINAL_FAILURE = "terminal_failure"
    NEUTRAL_FENCE_LOSS = "neutral_fence_loss"


class ExactConsolidationResultOrigin(str, Enum):
    NEW = "new"
    REPLAYED = "replayed"


class ExactConsolidationMutationState(str, Enum):
    UNCHANGED = "unchanged"
    COMMITTED = "committed"
    COMPENSATED = "compensated"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class ExactConsolidationRowResult:
    queue_id: str
    board_id: str
    source: str
    reservation_lineage_id: str
    work_kind: str
    artifact_type: str
    artifact_id: str
    generation: int
    membership_source_ref: str
    membership_source_version: str
    membership_content_hash: str
    attempt_ordinal: int
    disposition: ExactConsolidationDisposition
    origin: ExactConsolidationResultOrigin
    mutation_state: ExactConsolidationMutationState
    error_code: str | None = None
    error_message: str | None = None
    next_retry_at: datetime | None = None
    diagnostic_json: str | None = None

    def __post_init__(self) -> None:
        string_fields = (
            self.queue_id,
            self.board_id,
            self.source,
            self.reservation_lineage_id,
            self.work_kind,
            self.artifact_type,
            self.artifact_id,
            self.membership_source_ref,
            self.membership_source_version,
            self.membership_content_hash,
        )
        if any(type(value) is not str or not value for value in string_fields):
            raise TypeError("exact_consolidation_row_identity_invalid")
        membership_artifact_type, separator, membership_artifact_id = (
            self.membership_source_ref.partition(":")
        )
        membership_queue_type = (
            "card"
            if membership_artifact_type in {"task", "test", "bug", "card"}
            else membership_artifact_type
        )
        if type(self.generation) is not int or self.generation < 0:
            raise TypeError("exact_consolidation_row_generation_invalid")
        if type(self.attempt_ordinal) is not int or self.attempt_ordinal < 1:
            raise TypeError("exact_consolidation_row_attempt_invalid")
        if type(self.disposition) is not ExactConsolidationDisposition:
            raise TypeError("exact_consolidation_row_disposition_invalid")
        if type(self.origin) is not ExactConsolidationResultOrigin:
            raise TypeError("exact_consolidation_row_origin_invalid")
        if type(self.mutation_state) is not ExactConsolidationMutationState:
            raise TypeError("exact_consolidation_row_mutation_state_invalid")
        if (self.error_code is None) is not (self.error_message is None):
            raise ValueError("exact_consolidation_row_error_invalid")
        if self.error_code is not None and (
            type(self.error_code) is not str
            or not self.error_code
            or len(self.error_code) > 128
            or re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:-]{0,127}", self.error_code) is None
            or type(self.error_message) is not str
            or not self.error_message
            or len(self.error_message) > 480
        ):
            raise ValueError("exact_consolidation_row_error_invalid")
        if self.diagnostic_json is not None:
            if (
                type(self.diagnostic_json) is not str
                or not self.diagnostic_json
                or len(self.diagnostic_json) > 16384
            ):
                raise ValueError("exact_consolidation_row_diagnostic_invalid")
            try:
                diagnostic = json.loads(self.diagnostic_json)
            except (TypeError, ValueError) as exc:
                raise ValueError("exact_consolidation_row_diagnostic_invalid") from exc
            if (
                type(diagnostic) is not dict
                or json.dumps(
                    diagnostic,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                != self.diagnostic_json
            ):
                raise ValueError("exact_consolidation_row_diagnostic_invalid")
        if self.next_retry_at is not None and (
            type(self.next_retry_at) is not datetime
            or self.next_retry_at.tzinfo is None
            or self.next_retry_at.utcoffset() is None
        ):
            raise TypeError("exact_consolidation_row_retry_at_invalid")
        if (
            not self.source.startswith("rebuild:")
            or not self.source.removeprefix("rebuild:").strip()
            or self.work_kind != "consolidate"
            or len(self.reservation_lineage_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.reservation_lineage_id
            )
            or separator != ":"
            or ":" in membership_artifact_id
            or membership_artifact_id != self.artifact_id
            or membership_artifact_type not in _EXACT_REBUILD_SOURCE_ARTIFACT_TYPES
            or membership_queue_type != self.artifact_type
            or not self.membership_source_version.strip()
            or len(self.membership_content_hash) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.membership_content_hash
            )
        ):
            raise ValueError("exact_consolidation_row_binding_invalid")
        has_error = self.error_code is not None
        if self.disposition is ExactConsolidationDisposition.ACKED:
            valid_semantics = (
                self.origin is ExactConsolidationResultOrigin.NEW
                and self.mutation_state is ExactConsolidationMutationState.COMMITTED
                and not has_error
                and self.next_retry_at is None
                and self.diagnostic_json is None
            )
        elif self.disposition is ExactConsolidationDisposition.RETRY_SCHEDULED:
            valid_semantics = (
                self.origin
                in {
                    ExactConsolidationResultOrigin.NEW,
                    ExactConsolidationResultOrigin.REPLAYED,
                }
                and self.mutation_state is ExactConsolidationMutationState.UNCHANGED
                and has_error
                and self.next_retry_at is not None
            )
        elif self.disposition is ExactConsolidationDisposition.TERMINAL_FAILURE:
            valid_semantics = (
                self.origin
                in {
                    ExactConsolidationResultOrigin.NEW,
                    ExactConsolidationResultOrigin.REPLAYED,
                }
                and self.mutation_state
                in {
                    ExactConsolidationMutationState.UNCHANGED,
                    ExactConsolidationMutationState.COMPENSATED,
                    ExactConsolidationMutationState.AMBIGUOUS,
                }
                and has_error
                and self.next_retry_at is None
            )
        else:
            valid_semantics = (
                self.origin is ExactConsolidationResultOrigin.NEW
                and self.mutation_state
                in {
                    ExactConsolidationMutationState.UNCHANGED,
                    ExactConsolidationMutationState.COMPENSATED,
                    ExactConsolidationMutationState.AMBIGUOUS,
                }
                and has_error
                and self.next_retry_at is None
            )
        if not valid_semantics:
            raise ValueError("exact_consolidation_row_semantics_invalid")


@dataclass(frozen=True, slots=True)
class ExactConsolidationBatchResult:
    claim_scope: ConsolidationClaimScope
    rows: tuple[ExactConsolidationRowResult, ...]

    def __post_init__(self) -> None:
        if type(self.claim_scope) is not ConsolidationClaimScope:
            raise TypeError("exact_consolidation_batch_scope_invalid")
        if type(self.rows) is not tuple or any(
            type(row) is not ExactConsolidationRowResult for row in self.rows
        ):
            raise TypeError("exact_consolidation_batch_rows_invalid")
        if any(
            row.board_id != self.claim_scope.board_id
            or row.source != self.claim_scope.source
            or row.work_kind != self.claim_scope.work_kind
            or row.reservation_lineage_id != self.claim_scope.reservation_lineage_id
            for row in self.rows
        ):
            raise ValueError("exact_consolidation_batch_scope_mismatch")
        if self.claim_scope.reservation_lineage_id is None:
            raise ValueError("exact_consolidation_batch_reservation_lineage_required")
        if len({row.queue_id for row in self.rows}) != len(self.rows):
            raise ValueError("exact_consolidation_batch_duplicate_queue_id")

    @property
    def acked_count(self) -> int:
        return sum(
            row.disposition is ExactConsolidationDisposition.ACKED for row in self.rows
        )

    @property
    def new_attempt_count(self) -> int:
        return sum(
            row.origin is ExactConsolidationResultOrigin.NEW for row in self.rows
        )

    @property
    def replayed_count(self) -> int:
        return sum(
            row.origin is ExactConsolidationResultOrigin.REPLAYED for row in self.rows
        )

    @property
    def terminal_failures(self) -> tuple[ExactConsolidationRowResult, ...]:
        return tuple(
            row
            for row in self.rows
            if row.disposition is ExactConsolidationDisposition.TERMINAL_FAILURE
        )

    @property
    def retry_scheduled(self) -> tuple[ExactConsolidationRowResult, ...]:
        return tuple(
            row
            for row in self.rows
            if row.disposition is ExactConsolidationDisposition.RETRY_SCHEDULED
        )

    @property
    def earliest_retry_at(self) -> datetime | None:
        values = [
            row.next_retry_at
            for row in self.retry_scheduled
            if row.next_retry_at is not None
        ]
        return min(values) if values else None


class ExactConsolidationPostCommitError(RuntimeError):
    """A durable exact row outcome whose graph cleanup could not complete.

    ``batch_result`` is the authoritative partial result through the durable
    transaction, including the affected row. Recovery orchestration must use
    it for totality/gating and then compensate the enclosing F06 operation;
    it must never infer committed identities from queue-depth deltas.
    """

    __slots__ = ("_batch_result", "_error_code", "_failed_queue_id")

    def __init__(
        self,
        *,
        batch_result: ExactConsolidationBatchResult,
        failed_queue_id: str,
        error_code: str,
    ) -> None:
        if type(batch_result) is not ExactConsolidationBatchResult:
            raise TypeError("exact_post_commit_batch_result_invalid")
        if type(failed_queue_id) is not str or not failed_queue_id:
            raise TypeError("exact_post_commit_queue_id_invalid")
        if type(error_code) is not str or not error_code:
            raise TypeError("exact_post_commit_error_code_invalid")
        matching = tuple(
            row for row in batch_result.rows if row.queue_id == failed_queue_id
        )
        if (
            len(matching) != 1
            or matching[0].origin is not ExactConsolidationResultOrigin.NEW
        ):
            raise ValueError("exact_post_commit_failed_row_invalid")
        self._batch_result = batch_result
        self._failed_queue_id = failed_queue_id
        self._error_code = error_code
        super().__init__(error_code)

    @property
    def batch_result(self) -> ExactConsolidationBatchResult:
        return self._batch_result

    @property
    def failed_queue_id(self) -> str:
        return self._failed_queue_id

    @property
    def error_code(self) -> str:
        return self._error_code


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
            "evidence_absence_justification": (self.evidence_absence_justification),
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

    async def list_ready_pending_exact(
        self,
        context: Any,
        *,
        now: datetime,
        board_id: str,
        source: str,
        work_kind: str,
    ) -> tuple[ConsolidationQueueRecord, ...]:
        """List only exact recovery membership before any row is claimed."""

        ...

    async def list_pending_exact(
        self,
        context: Any,
        *,
        board_id: str,
        source: str,
        work_kind: str,
    ) -> tuple[ConsolidationQueueRecord, ...]:
        """List all pending exact members, including delayed dispositions."""

        ...

    async def list_claimed_exact(
        self,
        context: Any,
        *,
        board_id: str,
        source: str,
        work_kind: str,
    ) -> tuple[ConsolidationQueueRecord, ...]:
        """List only claimed members of one offline recovery fence."""

        ...

    async def claim_ready_pending_exact(
        self,
        context: Any,
        *,
        entry_id: str,
        board_id: str,
        source: str,
        work_kind: str,
        generation: int,
        now: datetime,
        claim_timeout_at: datetime,
        worker_id: str,
        claim_token: str,
    ) -> ConsolidationQueueRecord | None:
        """CAS one exact ready row into claimed state."""

        ...

    async def board_administrative_rebuild_source(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> str | None: ...

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
        source: str,
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
        board_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        """Delete one claimed row by compare-and-swap identity.

        Returns ``True`` only when exactly one row matched the id, claimed
        status, claim token, generation and null-safe delete-event identity.
        """
        ...

    async def repend_claimed_queue_entry(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        """Neutrally release an exact claim after an administrative fence.

        Implementations must compare every supplied identity in one atomic
        update, preserve source/payload/attempts and clear claim ownership and
        timing fields while returning the row to ``pending``.
        """

        ...

    async def save_exact_rebuild_disposition(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
        expected_attempts: int,
        expected_last_error: str | None,
        expected_next_retry_at: datetime | None,
        expected_payload: dict[str, Any],
        reservation_authority_probe: Callable[[], bool],
        payload: dict[str, Any],
        attempts: int,
        last_error: str,
        next_retry_at: datetime | None,
    ) -> ConsolidationQueueRecord | None:
        """CAS a claimed exact row to pending with its durable disposition.

        Implementations must compare the complete claim identity and exact
        prior payload, then invoke the token/epoch-bound reservation probe
        immediately before mutation in the same transaction. The durable
        marker binds the stable run lineage instead of that ephemeral token,
        so a governed successor lease for the same run can replay it. They
        replace payload/retry fields, clear claim ownership, and return the
        stored row. ``None`` is a neutral ownership/fence loss. Core re-proves
        the current ephemeral authority immediately before and after commit.
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

    async def list_dlq_auto_drain_board_ids(self, context: Any) -> tuple[str, ...]: ...

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
    return require_runtime_value(
        _RUNTIME_KEY, "consolidation_persistence_port_not_configured"
    )


def reset_consolidation_persistence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ConsolidationClaimScope",
    "ConsolidationPersistencePort",
    "ConsolidationPoisonRow",
    "ConsolidationProjectionInputs",
    "ConsolidationQueueRecord",
    "CurrentQualityAssessmentSummary",
    "CurrentResearchDecisionSummary",
    "CurrentSpecDependencyProjection",
    "ExactConsolidationBatchResult",
    "ExactConsolidationDisposition",
    "ExactConsolidationMutationState",
    "ExactConsolidationPostCommitError",
    "ExactConsolidationResultOrigin",
    "ExactConsolidationRowResult",
    "get_consolidation_persistence_port",
    "register_consolidation_persistence_port",
    "reset_consolidation_persistence_port_for_tests",
]
