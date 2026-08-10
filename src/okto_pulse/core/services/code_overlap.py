"""Policy for overlaps derived from current, agent-attested resolutions.

The service compares only structured records already persisted by Pulse.  It
does not discover targets or inspect a repository, filesystem, provider, or
language runtime.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from uuid import uuid4

from okto_pulse.core.domain.code_traceability import (
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    ImplementationTargetInvalid,
    TargetOverlap,
    TargetOverlapAcknowledgement,
    TargetOverlapSeverity,
    classify_implementation_target_overlap,
)
from okto_pulse.core.models.code_traceability import (
    TargetOverlapAcknowledgementInput,
)
from okto_pulse.core.ports.code_traceability import (
    CodeTraceabilityReadPort,
    CodeTraceabilityStore,
    TargetOverlapQuery,
)


Clock = Callable[[], datetime]
IdFactory = Callable[[str], str]


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CodeTraceabilityContractError("target_overlap_clock_invalid")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class TargetOverlapAcknowledgementResult:
    overlap: TargetOverlap
    acknowledgement: TargetOverlapAcknowledgement
    replayed: bool = False


class CodeOverlapService:
    """Read and acknowledge exact current resolution pairs."""

    def __init__(
        self,
        *,
        clock: Clock = _default_clock,
        id_factory: IdFactory = _default_id_factory,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def get_overlaps(
        self,
        query: TargetOverlapQuery,
        *,
        read_port: CodeTraceabilityReadPort,
    ) -> tuple[TargetOverlap, ...]:
        overlaps = await read_port.overlap_report(query)
        if not isinstance(overlaps, tuple):
            raise CodeTraceabilityContractError(
                "target_overlap_projection_invalid",
            )
        for overlap in overlaps:
            if not isinstance(overlap, TargetOverlap) or overlap.board_id != query.board_id:
                raise CodeTraceabilityContractError(
                    "target_overlap_projection_invalid",
                )
            if overlap.severity is TargetOverlapSeverity.NONE:
                raise CodeTraceabilityContractError(
                    "target_overlap_projection_none_forbidden",
                )
            if (
                not query.include_informational
                and overlap.severity is TargetOverlapSeverity.INFORMATIONAL
            ):
                raise CodeTraceabilityContractError(
                    "target_overlap_projection_filter_invalid",
                )
        return overlaps

    async def acknowledge(
        self,
        submission: TargetOverlapAcknowledgementInput,
        *,
        created_by: str,
        store: CodeTraceabilityStore,
    ) -> TargetOverlapAcknowledgementResult:
        target_a = await store.get_target(
            board_id=submission.board_id,
            target_id=submission.target_a_id,
        )
        target_b = await store.get_target(
            board_id=submission.board_id,
            target_id=submission.target_b_id,
        )
        if target_a is None or target_b is None:
            raise ImplementationTargetInvalid(
                details={"reason": "overlap_target_not_found"},
            )
        if (
            target_a.board_id != submission.board_id
            or target_b.board_id != submission.board_id
            or target_a.card_id == target_b.card_id
            or submission.card_id not in {target_a.card_id, target_b.card_id}
            or target_a.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE
            or target_b.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE
        ):
            raise ImplementationTargetInvalid(
                details={"reason": "overlap_target_scope_mismatch"},
            )
        if (
            target_a.current_resolution_id != submission.resolution_a_id
            or target_b.current_resolution_id != submission.resolution_b_id
        ):
            raise CodeTraceabilityContractError(
                "implementation_overlap_ack_stale",
                details={"reason": "current_resolution_changed"},
            )
        resolution_a = await store.get_resolution(
            board_id=submission.board_id,
            resolution_id=submission.resolution_a_id,
        )
        resolution_b = await store.get_resolution(
            board_id=submission.board_id,
            resolution_id=submission.resolution_b_id,
        )
        if (
            resolution_a is None
            or resolution_b is None
            or resolution_a.target_id != target_a.id
            or resolution_b.target_id != target_b.id
        ):
            raise CodeTraceabilityContractError(
                "implementation_overlap_ack_stale",
                details={"reason": "resolution_target_mismatch"},
            )
        overlap = classify_implementation_target_overlap(
            target_a,
            resolution_a,
            target_b,
            resolution_b,
        )
        if overlap.severity is TargetOverlapSeverity.NONE:
            raise ImplementationTargetInvalid(
                details={"reason": "overlap_not_present"},
            )

        existing = await store.list_overlap_acknowledgements(
            board_id=submission.board_id,
            card_id=submission.card_id,
        )
        canonical_targets = {target_a.id, target_b.id}
        canonical_resolutions = {resolution_a.id, resolution_b.id}
        for acknowledgement in existing:
            if (
                {acknowledgement.target_a_id, acknowledgement.target_b_id}
                == canonical_targets
                and {
                    acknowledgement.resolution_a_id,
                    acknowledgement.resolution_b_id,
                }
                == canonical_resolutions
            ):
                if (
                    acknowledgement.disposition is submission.disposition
                    and acknowledgement.justification == submission.justification
                    and acknowledgement.created_by == created_by
                ):
                    return TargetOverlapAcknowledgementResult(
                        overlap=replace(
                            overlap,
                            acknowledgement=acknowledgement,
                        ),
                        acknowledgement=acknowledgement,
                        replayed=True,
                    )
                raise CodeTraceabilityContractError(
                    "implementation_overlap_ack_conflict",
                    details={"acknowledgement_id": acknowledgement.id},
                )

        acknowledgement = TargetOverlapAcknowledgement(
            id=self._id_factory("target-overlap-acknowledgement"),
            board_id=submission.board_id,
            target_a_id=submission.target_a_id,
            target_b_id=submission.target_b_id,
            resolution_a_id=submission.resolution_a_id,
            resolution_b_id=submission.resolution_b_id,
            disposition=submission.disposition,
            justification=submission.justification,
            created_by=created_by,
            created_at=_aware_utc(self._clock()),
        )
        persisted = await store.add_overlap_acknowledgement(acknowledgement)
        return TargetOverlapAcknowledgementResult(
            overlap=replace(overlap, acknowledgement=persisted),
            acknowledgement=persisted,
        )


__all__ = [
    "CodeOverlapService",
    "TargetOverlapAcknowledgementResult",
]
