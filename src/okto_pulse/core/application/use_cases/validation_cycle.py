"""Transport-neutral validation-cycle summary queries."""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.quality_assessment import AssessmentSubjectType
from okto_pulse.core.domain.validation_cycle import (
    ValidationCycleResultType,
    ValidationCycleSummary,
    ValidationCycleSubjectRef,
    ValidationTechnicalAudit,
)
from okto_pulse.core.ports.validation_cycle import ValidationCycleReadPort


def _required_id(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 255:
        raise ValueError(code)
    return value.strip()


@dataclass(frozen=True, slots=True)
class GetValidationCycleCommand:
    subject_type: AssessmentSubjectType
    subject_id: str
    include_previous: bool = False
    offset: int = 0
    limit: int = 25

    def __post_init__(self) -> None:
        if not isinstance(self.subject_type, AssessmentSubjectType):
            raise ValueError("validation_cycle_subject_type_invalid")
        object.__setattr__(
            self,
            "subject_id",
            _required_id(self.subject_id, "validation_cycle_subject_id_required"),
        )
        if not isinstance(self.include_previous, bool):
            raise ValueError("validation_cycle_include_previous_invalid")
        if (
            not isinstance(self.offset, int)
            or isinstance(self.offset, bool)
            or self.offset < 0
        ):
            raise ValueError("validation_cycle_offset_invalid")
        if (
            not isinstance(self.limit, int)
            or isinstance(self.limit, bool)
            or not 1 <= self.limit <= 100
        ):
            raise ValueError("validation_cycle_limit_invalid")


@dataclass(frozen=True, slots=True)
class GetValidationCyclesCommand:
    subjects: tuple[ValidationCycleSubjectRef, ...]

    def __post_init__(self) -> None:
        subjects = tuple(self.subjects)
        if not 1 <= len(subjects) <= 50:
            raise ValueError("validation_cycle_batch_size_invalid")
        if any(not isinstance(item, ValidationCycleSubjectRef) for item in subjects):
            raise ValueError("validation_cycle_batch_subject_invalid")
        identities = [(item.subject_type, item.subject_id) for item in subjects]
        if len(set(identities)) != len(identities):
            raise ValueError("validation_cycle_batch_subject_duplicate")
        object.__setattr__(self, "subjects", subjects)


@dataclass(frozen=True, slots=True)
class GetValidationTechnicalAuditCommand:
    subject_type: AssessmentSubjectType
    subject_id: str
    result_id: str
    result_type: ValidationCycleResultType

    def __post_init__(self) -> None:
        if not isinstance(self.subject_type, AssessmentSubjectType):
            raise ValueError("validation_cycle_subject_type_invalid")
        object.__setattr__(
            self,
            "subject_id",
            _required_id(self.subject_id, "validation_cycle_subject_id_required"),
        )
        object.__setattr__(
            self,
            "result_id",
            _required_id(self.result_id, "validation_cycle_result_id_required"),
        )
        if not isinstance(self.result_type, ValidationCycleResultType):
            raise ValueError("validation_cycle_result_type_invalid")


class ValidationCycleReadUseCases:
    def __init__(self, *, reader: ValidationCycleReadPort) -> None:
        self._reader = reader

    async def get_cycle(
        self,
        command: GetValidationCycleCommand,
        *,
        actor: ActorContext,
    ) -> ValidationCycleSummary:
        result = await self._reader.get_validation_cycle(
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            include_previous=command.include_previous,
            offset=command.offset,
            limit=command.limit,
            actor_id=actor.actor_id,
            realm_scope=actor.require_realm_scope(),
        )
        if not isinstance(result, ValidationCycleSummary):
            raise RuntimeError("validation_cycle_reader_result_invalid")
        if (
            result.subject_type is not command.subject_type
            or result.subject_id != command.subject_id
            or (not command.include_previous and result.previous_results)
            or len(result.previous_results) > command.limit
        ):
            raise RuntimeError("validation_cycle_reader_scope_mismatch")
        return result

    async def get_many(
        self,
        command: GetValidationCyclesCommand,
        *,
        actor: ActorContext,
    ) -> tuple[ValidationCycleSummary, ...]:
        results = tuple(
            await self._reader.get_validation_cycles(
                subjects=command.subjects,
                actor_id=actor.actor_id,
                realm_scope=actor.require_realm_scope(),
            )
        )
        if any(not isinstance(item, ValidationCycleSummary) for item in results):
            raise RuntimeError("validation_cycle_batch_reader_result_invalid")
        expected = [(item.subject_type, item.subject_id) for item in command.subjects]
        actual = [(item.subject_type, item.subject_id) for item in results]
        if (
            len(results) != len(command.subjects)
            or actual != expected
            or any(item.previous_results for item in results)
        ):
            raise RuntimeError("validation_cycle_batch_reader_scope_mismatch")
        return results

    async def get_technical_audit(
        self,
        command: GetValidationTechnicalAuditCommand,
        *,
        actor: ActorContext,
    ) -> ValidationTechnicalAudit:
        result = await self._reader.get_result_technical_audit(
            subject_type=command.subject_type,
            subject_id=command.subject_id,
            result_id=command.result_id,
            result_type=command.result_type,
            actor_id=actor.actor_id,
            realm_scope=actor.require_realm_scope(),
        )
        if not isinstance(result, ValidationTechnicalAudit):
            raise RuntimeError("validation_audit_reader_result_invalid")
        if (
            result.subject_type is not command.subject_type
            or result.subject_id != command.subject_id
            or result.result_id != command.result_id
            or result.result_type is not command.result_type
        ):
            raise RuntimeError("validation_audit_reader_scope_mismatch")
        return result


__all__ = [
    "GetValidationCycleCommand",
    "GetValidationCyclesCommand",
    "GetValidationTechnicalAuditCommand",
    "ValidationCycleReadUseCases",
]
