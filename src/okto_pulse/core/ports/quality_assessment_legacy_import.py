"""Edition-neutral persistence boundary for the one-shot legacy import."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.quality_assessment_legacy_import import (
    LegacyImportCandidateOutcome,
    LegacyImportCandidateWork,
    LegacyImportCheckpoint,
    LegacyImportCompletionExpectation,
    LegacyImportPlan,
    LegacyImportPostcondition,
    LegacyImportSourceSnapshot,
)


class LegacyImportPersistenceError(RuntimeError):
    code = "quality_assessment_legacy_import_persistence_error"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)


class LegacyImportAdapterMissing(LegacyImportPersistenceError):
    code = "quality_assessment_legacy_import_adapter_missing"


class LegacyImportPlanConflict(LegacyImportPersistenceError):
    code = "quality_assessment_legacy_import_plan_conflict"


class LegacyImportCheckpointConflict(LegacyImportPersistenceError):
    code = "quality_assessment_legacy_import_checkpoint_conflict"


class LegacyImportPostconditionConflict(LegacyImportPersistenceError):
    code = "quality_assessment_legacy_import_postcondition_conflict"


@runtime_checkable
class QualityAssessmentLegacySourcePort(Protocol):
    """Capture source rows using one stable cutoff.

    The adapter must return only the requested board's source material needed by the
    strict Core selectors.  Capturing the snapshot is read-only.  Rows newer
    than ``cutoff`` may be returned for defensive filtering, but can never
    become candidates.
    """

    async def capture_legacy_snapshot(
        self,
        *,
        board_id: str,
        cutoff: datetime,
    ) -> LegacyImportSourceSnapshot: ...


@runtime_checkable
class QualityAssessmentLegacyImportPersistencePort(Protocol):
    """Durable, operation-transactional epoch ledger.

    Implementations must obey these invariants:

    * ``acquire_or_build_plan`` executes source reads, the supplied pure
      deterministic/no-I/O planner callback, and the insert under one SQLite
      ``BEGIN IMMEDIATE`` (or equivalent write lock), inserting the plan only
      once;
    * a previously stored plan is immutable; a concurrent creator may win and
      its authoritative plan is returned instead of overwriting it.  Every
      loaded plan must pass its cutoff, code-digest, candidate-digest, ordered
      candidate, and plan-digest validations;
    * ``apply_candidate_checkpoint`` atomically rechecks whether a native
      receipt/head already exists, lets native data win, otherwise inserts the
      legacy receipt plus event/history/outbox, and advances the checkpoint in
      that same unit of work, using the exact Core-owned deterministic receipt,
      event, history, outbox, idempotency, run, request, and authority
      identities carried by ``work.write_identity``;
    * each mutating method is one explicit durable transaction boundary:
      ``acquire_or_build_plan`` commits the frozen plan,
      ``apply_candidate_checkpoint`` commits exactly one candidate resolution,
      and ``verify_and_complete`` commits epoch closure only after its physical
      postcondition succeeds;
    * a method may never split its documented bundle across transactions, and
      a failure rolls back that operation in full; and
    * completion is durable only after its physical postcondition succeeds.

    This exception to request-UoW adapters is deliberate: a one-shot startup
    migration needs checkpoints durable *between* candidates for real
    crash-resume.  Every run/plan/checkpoint/completion is scoped by
    ``(board_id, epoch)`` so board erasure cannot corrupt another board's
    digest. Dry-run execution never invokes this port.
    """

    async def load_plan(
        self,
        *,
        board_id: str,
        epoch: str,
    ) -> LegacyImportPlan | None: ...

    async def acquire_or_build_plan(
        self,
        *,
        board_id: str,
        cutoff: datetime,
        planner: Callable[[LegacyImportSourceSnapshot], LegacyImportPlan],
    ) -> LegacyImportPlan: ...

    async def load_checkpoint(
        self,
        *,
        board_id: str,
        epoch: str,
        plan_digest: str,
    ) -> LegacyImportCheckpoint | None: ...

    async def apply_candidate_checkpoint(
        self,
        work: LegacyImportCandidateWork,
    ) -> LegacyImportCandidateOutcome: ...

    async def verify_and_complete(
        self,
        expectation: LegacyImportCompletionExpectation,
    ) -> LegacyImportPostcondition: ...
