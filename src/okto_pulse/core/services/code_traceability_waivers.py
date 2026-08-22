"""Human waiver lifecycle for Code Traceability gates.

Waivers are explicit governance records and never stand in for agent
attestation, source currentness, or repository access.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from uuid import uuid4

from okto_pulse.core.domain.code_traceability import (
    CodeTraceabilityContractError,
    CodeTraceabilityLocked,
    CodeTraceabilityWaiver,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverScope,
)
from okto_pulse.core.models.code_traceability import (
    CodeTraceabilityWaiverClearInput,
    CodeTraceabilityWaiverInput,
)
from okto_pulse.core.ports.code_traceability import CodeTraceabilityStore


Clock = Callable[[], datetime]
IdFactory = Callable[[str], str]


_ALLOWED_SCOPES_BY_ENTITY: dict[
    CodeTraceabilityWaiverEntityType,
    frozenset[CodeTraceabilityWaiverScope],
] = {
    CodeTraceabilityWaiverEntityType.REFINEMENT: frozenset(
        {CodeTraceabilityWaiverScope.CODE_EVIDENCE}
    ),
    CodeTraceabilityWaiverEntityType.SPEC: frozenset(
        {
            CodeTraceabilityWaiverScope.CODE_EVIDENCE,
            CodeTraceabilityWaiverScope.EVIDENCE_LINKAGE,
        }
    ),
    CodeTraceabilityWaiverEntityType.CARD: frozenset(
        {
            CodeTraceabilityWaiverScope.IMPLEMENTATION_TARGET,
            CodeTraceabilityWaiverScope.TARGET_RESOLUTION,
            CodeTraceabilityWaiverScope.TARGET_OVERLAP,
        }
    ),
    CodeTraceabilityWaiverEntityType.SPEC_ENTITY: frozenset(
        {
            CodeTraceabilityWaiverScope.EVIDENCE_LINKAGE,
            CodeTraceabilityWaiverScope.IMPLEMENTATION_TARGET,
        }
    ),
}


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CodeTraceabilityContractError("code_traceability_waiver_clock_invalid")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class CodeTraceabilityWaiverMutationResult:
    waiver: CodeTraceabilityWaiver
    replayed: bool = False


def validate_waiver_scope(
    entity_type: CodeTraceabilityWaiverEntityType,
    scope: CodeTraceabilityWaiverScope,
) -> None:
    if scope not in _ALLOWED_SCOPES_BY_ENTITY[entity_type]:
        raise CodeTraceabilityContractError(
            "code_traceability_waiver_scope_incompatible",
            details={"entity_type": entity_type.value, "scope": scope.value},
        )


class CodeTraceabilityWaiverService:
    def __init__(
        self,
        *,
        clock: Clock = _default_clock,
        id_factory: IdFactory = _default_id_factory,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def mark_not_applicable(
        self,
        submission: CodeTraceabilityWaiverInput,
        *,
        created_by: str,
        store: CodeTraceabilityStore,
    ) -> CodeTraceabilityWaiverMutationResult:
        validate_waiver_scope(submission.entity_type, submission.scope)
        existing = await store.get_active_waiver(
            board_id=submission.board_id,
            entity_type=submission.entity_type,
            entity_id=submission.entity_id,
            scope=submission.scope,
        )
        if existing is not None:
            if (
                existing.reason_code is submission.reason_code
                and existing.justification == submission.justification
                and existing.created_by == created_by
            ):
                return CodeTraceabilityWaiverMutationResult(
                    waiver=existing,
                    replayed=True,
                )
            raise CodeTraceabilityLocked(
                details={
                    "reason": "active_waiver_conflict",
                    "waiver_id": existing.id,
                }
            )
        waiver = CodeTraceabilityWaiver(
            id=self._id_factory("code-traceability-waiver"),
            board_id=submission.board_id,
            entity_type=submission.entity_type,
            entity_id=submission.entity_id,
            scope=submission.scope,
            reason_code=submission.reason_code,
            justification=submission.justification,
            active=True,
            created_by=created_by,
            created_at=_aware_utc(self._clock()),
            cleared_by=None,
            cleared_at=None,
        )
        return CodeTraceabilityWaiverMutationResult(
            waiver=await store.create_waiver(waiver)
        )

    async def clear_not_applicable(
        self,
        submission: CodeTraceabilityWaiverClearInput,
        *,
        cleared_by: str,
        store: CodeTraceabilityStore,
    ) -> CodeTraceabilityWaiverMutationResult:
        existing = await store.get_waiver(
            board_id=submission.board_id,
            waiver_id=submission.waiver_id,
        )
        if existing is None:
            raise CodeTraceabilityContractError(
                "code_traceability_waiver_not_found",
                details={"waiver_id": submission.waiver_id},
            )
        if not existing.active:
            return CodeTraceabilityWaiverMutationResult(
                waiver=existing,
                replayed=True,
            )
        cleared = replace(
            existing,
            active=False,
            cleared_by=cleared_by,
            cleared_at=_aware_utc(self._clock()),
        )
        return CodeTraceabilityWaiverMutationResult(
            waiver=await store.clear_waiver(cleared)
        )


__all__ = [
    "CodeTraceabilityWaiverMutationResult",
    "CodeTraceabilityWaiverService",
    "validate_waiver_scope",
]
