"""Read-only ports for lifecycle-edition validation summaries."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.quality_assessment import AssessmentSubjectType
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.domain.validation_cycle import (
    ValidationCycleResultType,
    ValidationCycleSummary,
    ValidationCycleSubjectRef,
    ValidationTechnicalAudit,
)
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


class ValidationCycleReadError(RuntimeError):
    code = "validation_cycle_read_error"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)


class ValidationCycleSubjectNotFound(ValidationCycleReadError):
    code = "validation_subject_not_found"


class ValidationCycleResultNotFound(ValidationCycleReadError):
    code = "validation_result_not_found"


class ValidationCycleReadAccessDenied(ValidationCycleReadError):
    code = "validation_cycle_access_denied"


@runtime_checkable
class ValidationCycleReadPort(Protocol):
    """Resolve bounded summaries without eager-loading technical evidence.

    ``include_previous=False`` must not load result history. When true, the
    adapter loads at most ``limit`` summaries starting at ``offset`` and still
    returns the total previous-result count. Technical receipts and digest
    payloads are resolved only by ``get_result_technical_audit``.
    """

    async def get_validation_cycle(
        self,
        *,
        subject_type: AssessmentSubjectType,
        subject_id: str,
        include_previous: bool,
        offset: int,
        limit: int,
        actor_id: str,
        realm_scope: RealmScope,
    ) -> ValidationCycleSummary: ...

    async def get_validation_cycles(
        self,
        *,
        subjects: tuple[ValidationCycleSubjectRef, ...],
        actor_id: str,
        realm_scope: RealmScope,
    ) -> tuple[ValidationCycleSummary, ...]:
        """Return summary-only cycles for 1..50 subjects in request order."""

        ...

    async def get_result_technical_audit(
        self,
        *,
        subject_type: AssessmentSubjectType,
        subject_id: str,
        result_id: str,
        result_type: ValidationCycleResultType,
        actor_id: str,
        realm_scope: RealmScope,
    ) -> ValidationTechnicalAudit:
        """Return an audit carrying the exact subject and result identity.

        The caller validates ``subject_type``, ``subject_id``, ``result_id`` and
        ``result_type`` before projecting any technical evidence.
        """

        ...


_RUNTIME_KEY = "ports.validation_cycle.reader"


def register_validation_cycle_reader(reader: ValidationCycleReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_validation_cycle_reader() -> ValidationCycleReadPort:
    return require_runtime_value(_RUNTIME_KEY, "validation_cycle_reader_not_configured")


def reset_validation_cycle_reader_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ValidationCycleReadAccessDenied",
    "ValidationCycleReadError",
    "ValidationCycleReadPort",
    "ValidationCycleResultNotFound",
    "ValidationCycleSubjectNotFound",
    "get_validation_cycle_reader",
    "register_validation_cycle_reader",
    "reset_validation_cycle_reader_for_tests",
]
