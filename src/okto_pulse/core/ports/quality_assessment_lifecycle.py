"""Edition-neutral lifecycle and purge persistence boundary."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.quality_assessment_lifecycle import (
    AssessmentLifecycleHead,
    AssessmentLifecyclePlan,
    AssessmentLifecycleReceipt,
    AssessmentPurgePlan,
    AssessmentPurgePostcondition,
)


class AssessmentLifecyclePersistenceError(RuntimeError):
    code = "quality_assessment_lifecycle_persistence_error"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)


class AssessmentLifecycleAdapterMissing(AssessmentLifecyclePersistenceError):
    code = "quality_assessment_lifecycle_adapter_missing"


class AssessmentLifecycleCasConflict(AssessmentLifecyclePersistenceError):
    code = "quality_assessment_lifecycle_cas_conflict"


class AssessmentPurgePostconditionConflict(
    AssessmentLifecyclePersistenceError
):
    code = "quality_assessment_purge_postcondition_conflict"


@runtime_checkable
class QualityAssessmentLifecyclePersistencePort(Protocol):
    """Apply lifecycle reconciliation in the caller's unit of work.

    The adapter must atomically mutate/rebuild heads, stage lifecycle
    event/history/outbox, and reconcile the targeted projection/KG intent.  It
    fences replay by ``transition.idempotency_key`` plus
    ``transition.transition_digest`` and records each
    ``stale_transition_key`` at most once.  A valid stale receipt is historical
    head state, not an orphan.  The adapter never commits, rolls back, closes
    the transaction, or opens another UoW.
    """

    async def load_lifecycle_state(
        self,
        *,
        board_id: str,
        subject_type: str,
        subject_id: str,
    ) -> tuple[
        tuple[AssessmentLifecycleHead, ...],
        tuple[AssessmentLifecycleReceipt, ...],
    ]: ...

    async def apply_lifecycle_plan(
        self,
        plan: AssessmentLifecyclePlan,
    ) -> None: ...


@runtime_checkable
class QualityAssessmentPurgePersistencePort(Protocol):
    """Execute the inner quality purge and prove its zero residuals.

    Subject purge preserves the durable legacy-import epoch.  Board purge must
    run with the supplied transaction-local ``BoardErasurePermit`` and is the
    only purge allowed to remove epoch rows.  This adapter runs *inside* the
    permit lifetime and therefore must never claim that the permit was
    released.  The outer board-erasure orchestrator owns that separate final
    completion proof after every purge has succeeded.
    """

    async def apply_purge_plan(
        self,
        plan: AssessmentPurgePlan,
    ) -> AssessmentPurgePostcondition: ...
