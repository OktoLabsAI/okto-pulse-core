"""Resolve and seal actionable semantic guideline assessments v2."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.application.use_cases.policy_governance import (
    ASSESSMENTS_RECORD,
    ASSESSMENTS_READ,
    require_policy_assessment_lifecycle,
    require_policy_governance_capabilities,
)
from okto_pulse.core.application.use_cases.semantic_guideline_governance import (
    GetCurrentSemanticGuidelineAssessmentCommand,
    GetCurrentSemanticGuidelineAssessmentUseCase,
)
from okto_pulse.core.domain.guideline_semantic_findings_v2 import (
    SemanticAssessmentReceiptProjectionV2,
)

from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentContractError,
)

if TYPE_CHECKING:
    from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.domain.guideline_semantic_v2 import (
    SemanticAnchorAvailability,
    SemanticAssessmentDraftV2,
    SemanticAssessmentRequestV2,
    SemanticMetricAssessmentV2,
    semantic_metric_result_digest_v2,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.ports.semantic_subject_projection import (
    SemanticAssessmentV2PersistencePort,
    SemanticAssessmentV2CapabilityPort,
    SemanticAssessmentV2ReadPort,
    SemanticAssessmentV2WriterUnavailable,
    SemanticAssessmentV2PersistenceResult,
    SemanticSubjectProjectionError,
    SemanticSubjectProjectionFailure,
    SemanticSubjectProjectionPort,
    SemanticSubjectProjectionRequest,
)


@dataclass(frozen=True, slots=True)
class SealSemanticGuidelineAssessmentV2Command:
    board_id: str
    actor_id: str
    draft: SemanticAssessmentDraftV2

    def __post_init__(self) -> None:
        if not isinstance(self.board_id, str) or not self.board_id.strip():
            raise SemanticAssessmentContractError(
                "semantic_assessment_board_id_required"
            )
        if not isinstance(self.actor_id, str) or not self.actor_id.strip():
            raise SemanticAssessmentContractError(
                "semantic_assessment_actor_id_required"
            )
        if not isinstance(self.draft, SemanticAssessmentDraftV2):
            raise SemanticAssessmentContractError(
                "semantic_assessment_v2_draft_invalid"
            )
        object.__setattr__(self, "board_id", self.board_id.strip())
        object.__setattr__(self, "actor_id", self.actor_id.strip())
        if self.draft.subject.board_id != self.board_id:
            raise SemanticAssessmentContractError(
                "semantic_assessment_subject_board_mismatch"
            )
        if self.draft.assessor.agent_id != self.actor_id:
            raise SemanticAssessmentContractError(
                "semantic_assessment_assessor_mismatch"
            )


@dataclass(frozen=True, slots=True)
class SealSemanticGuidelineAssessmentV2Result:
    request: SemanticAssessmentRequestV2
    persistence: SemanticAssessmentV2PersistenceResult


@dataclass(frozen=True, slots=True)
class GetCurrentSemanticGuidelineAssessmentAnyResult:
    contract_version: str
    assessment: object

    def __post_init__(self) -> None:
        if self.contract_version not in {"v1", "v2"}:
            raise TypeError("semantic_assessment_contract_version_invalid")


def _metric_projection_v2(receipt: SemanticAssessmentReceiptProjectionV2) -> list[dict[str, object]]:
    return [
        {
            "metric_result_id": metric.metric_result_id,
            "metric_result_digest": semantic_metric_result_digest_v2(metric),
            "metric_id": metric.metric_id,
            "metric_code": metric.metric_code,
            "score": metric.score,
            "direction": metric.direction.value,
            "default_threshold": metric.default_threshold,
            "effective_threshold": metric.effective_threshold,
            "threshold_source": metric.threshold_source.value,
            "outcome": metric.outcome.value,
            "blocking": any(
                pinpoint.blocking_for(metric.outcome)
                for pinpoint in metric.pinpoints
            ),
            "pinpoints": [
                {
                    "contract_version": "v2",
                    "pinpoint_key": pinpoint.pinpoint_key,
                    "kind": pinpoint.kind.value,
                    "title": pinpoint.title,
                    "detail": pinpoint.detail,
                    "severity": (
                        pinpoint.severity.value if pinpoint.severity else None
                    ),
                    "remediation": pinpoint.remediation,
                    "anchor": {
                        "anchor_type": pinpoint.anchor.anchor_type.value,
                        "anchor_ref": pinpoint.anchor.anchor_ref,
                        "excerpt_hash": pinpoint.anchor.excerpt_hash,
                    },
                    "anchor_snapshot": {
                        "label": pinpoint.anchor_snapshot.label,
                        "excerpt": pinpoint.anchor_snapshot.excerpt,
                        "source_version": pinpoint.anchor_snapshot.source_version,
                        "availability_at_seal": (
                            pinpoint.anchor_snapshot.availability_at_seal.value
                        ),
                    },
                    "blocking": pinpoint.blocking_for(metric.outcome),
                }
                for pinpoint in metric.pinpoints
            ],
        }
        for metric in receipt.metric_results
    ]


def semantic_assessment_v2_write_projection(
    result: SealSemanticGuidelineAssessmentV2Result,
) -> dict[str, object]:
    """Canonical success projection shared byte-for-byte by REST and MCP."""

    if not isinstance(result, SealSemanticGuidelineAssessmentV2Result):
        raise TypeError("semantic_assessment_v2_result_invalid")
    receipt = result.persistence.receipt
    if receipt is None:
        raise TypeError("semantic_assessment_v2_receipt_projection_missing")
    return {
        "contract_version": "v2",
        "receipt_id": receipt.receipt_id,
        "request_digest": result.persistence.request_digest,
        "receipt_digest": receipt.receipt_digest,
        "currentness": "current",
        "validation_edition": receipt.subject.subject_edition,
        "lifecycle_state": "current",
        "metrics": _metric_projection_v2(receipt),
    }


def semantic_assessment_v2_current_projection(
    receipt: SemanticAssessmentReceiptProjectionV2,
) -> dict[str, object]:
    if not isinstance(receipt, SemanticAssessmentReceiptProjectionV2):
        raise TypeError("semantic_assessment_v2_receipt_projection_invalid")
    return {
        "receipt_id": receipt.receipt_id,
        "receipt_digest": receipt.receipt_digest,
        "currentness": "current",
        "board_id": receipt.subject.board_id,
        "subject_type": receipt.subject.entity_type.value,
        "subject_id": receipt.subject.subject_id,
        "subject_version": receipt.subject.subject_version,
        "validation_edition": receipt.subject.subject_edition,
        "lifecycle_state": "current",
        "binding_id": receipt.binding_id,
        "guideline_id": receipt.guideline_id,
        "guideline_revision_id": receipt.guideline_revision_id,
        "confidence": receipt.confidence,
        "recorded_at": receipt.recorded_at,
        "metrics": _metric_projection_v2(receipt),
    }


class GetCurrentSemanticGuidelineAssessmentAnyUseCase:
    """Choose the newest live-current receipt across the immutable v1/v2 ledgers."""

    async def execute(
        self,
        command: GetCurrentSemanticGuidelineAssessmentCommand,
        *,
        actor: ActorContext,
        uow: "PulseUnitOfWork",
    ) -> GetCurrentSemanticGuidelineAssessmentAnyResult:
        require_policy_governance_capabilities(actor, ASSESSMENTS_READ)
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise SemanticSubjectProjectionError(
                SemanticSubjectProjectionFailure.FORBIDDEN
            )
        reader = uow.semantic_assessment_v2_reader
        if not isinstance(reader, SemanticAssessmentV2ReadPort):
            raise TypeError("semantic_assessment_v2_reader_missing")
        semantic_port = uow.services.guidelines.semantic_policy_persistence()
        subject = await semantic_port.resolve_policy_subject_snapshot(
            board_id=command.board_id,
            entity_type=command.entity_type,
            subject_id=command.subject_id,
            lock=False,
        )
        if subject is None:
            raise EntityNotFoundError("policy_subject", command.subject_id)
        v2 = await reader.get_current_semantic_assessment_v2(
            board_id=command.board_id,
            entity_type=command.entity_type.value,
            subject_id=command.subject_id,
            binding_id=command.binding_id,
            subject_edition=subject.subject.subject_edition,
        )
        try:
            v1_result = await GetCurrentSemanticGuidelineAssessmentUseCase().execute(
                command,
                actor=actor,
                uow=uow,
            )
            v1 = v1_result.assessment
        except EntityNotFoundError:
            v1 = None
        if v1 is None and v2 is None:
            raise EntityNotFoundError(
                "current_semantic_guideline_assessment",
                f"{command.subject_id}:{command.binding_id}",
            )
        if v2 is not None and (
            v1 is None or v2.recorded_at >= getattr(v1, "recorded_at")
        ):
            return GetCurrentSemanticGuidelineAssessmentAnyResult(
                contract_version="v2",
                assessment=semantic_assessment_v2_current_projection(v2),
            )
        return GetCurrentSemanticGuidelineAssessmentAnyResult(
            contract_version="v1",
            assessment=v1,
        )


class SealSemanticGuidelineAssessmentV2UseCase:
    """Resolve every anchor before the single persistence-port invocation."""

    def __init__(
        self,
        *,
        subject_projection: SemanticSubjectProjectionPort | None = None,
        persistence: SemanticAssessmentV2PersistencePort | None = None,
    ) -> None:
        if subject_projection is not None and not isinstance(
            subject_projection, SemanticSubjectProjectionPort
        ):
            raise TypeError("semantic_subject_projection_adapter_missing")
        if persistence is not None and not isinstance(
            persistence, SemanticAssessmentV2PersistencePort
        ):
            raise TypeError("semantic_assessment_v2_persistence_adapter_missing")
        self._subject_projection = subject_projection
        self._persistence = persistence

    async def execute(
        self,
        command: SealSemanticGuidelineAssessmentV2Command,
        *,
        actor: ActorContext | None = None,
        uow: "PulseUnitOfWork | None" = None,
    ) -> SealSemanticGuidelineAssessmentV2Result:
        if not isinstance(command, SealSemanticGuidelineAssessmentV2Command):
            raise SemanticAssessmentContractError(
                "semantic_assessment_v2_command_invalid"
            )

        if (actor is None) != (uow is None):
            raise TypeError("semantic_assessment_v2_execution_context_incomplete")
        subject_projection = self._subject_projection
        persistence = self._persistence
        if actor is not None and uow is not None:
            require_policy_governance_capabilities(actor, ASSESSMENTS_RECORD)
            if actor.actor_id != command.actor_id:
                raise SemanticSubjectProjectionError(
                    SemanticSubjectProjectionFailure.FORBIDDEN
                )
            if await load_accessible_board(
                uow,
                command.board_id,
                actor,
                allowed_share_permissions={"editor", "admin"},
            ) is None:
                raise SemanticSubjectProjectionError(
                    SemanticSubjectProjectionFailure.FORBIDDEN
                )
            await require_policy_assessment_lifecycle(
                uow,
                subject=command.draft.subject,
            )
            subject_projection = uow.semantic_subject_projection
            persistence = uow.semantic_assessment_v2
            capability = uow.semantic_assessment_v2_capability
            if not isinstance(capability, SemanticAssessmentV2CapabilityPort):
                raise TypeError("semantic_assessment_v2_capability_adapter_missing")
            capability_snapshot = (
                await capability.semantic_assessment_v2_capabilities()
            )
            if not capability_snapshot.writer_active:
                raise SemanticAssessmentV2WriterUnavailable(capability_snapshot)
        if not isinstance(subject_projection, SemanticSubjectProjectionPort):
            raise TypeError("semantic_subject_projection_adapter_missing")
        if not isinstance(persistence, SemanticAssessmentV2PersistencePort):
            raise TypeError("semantic_assessment_v2_persistence_adapter_missing")

        resolved_metrics: list[SemanticMetricAssessmentV2] = []
        for metric in command.draft.metric_results:
            resolved_pinpoints = []
            for pinpoint in metric.pinpoints:
                try:
                    snapshot = await subject_projection.resolve_semantic_anchor(
                        SemanticSubjectProjectionRequest(
                            subject=command.draft.subject,
                            anchor=pinpoint.anchor,
                            actor_id=command.actor_id,
                        )
                    )
                except SemanticSubjectProjectionError:
                    raise
                except (TypeError, ValueError) as exc:
                    raise SemanticSubjectProjectionError(
                        SemanticSubjectProjectionFailure.MALFORMED
                    ) from exc
                if (
                    snapshot.availability_at_seal
                    is not SemanticAnchorAvailability.AVAILABLE
                ):
                    raise SemanticSubjectProjectionError(
                        SemanticSubjectProjectionFailure.MISSING
                    )
                if pinpoint.anchor.excerpt_hash is not None and (
                    snapshot.excerpt is None
                    or canonical_sha256(snapshot.excerpt)
                    != pinpoint.anchor.excerpt_hash
                ):
                    raise SemanticSubjectProjectionError(
                        SemanticSubjectProjectionFailure.MALFORMED
                    )
                resolved_pinpoints.append(pinpoint.seal(snapshot))
            resolved_metrics.append(
                SemanticMetricAssessmentV2(
                    metric_id=metric.metric_id,
                    score=metric.score,
                    rationale=metric.rationale,
                    evidence_refs=metric.evidence_refs,
                    pinpoints=tuple(resolved_pinpoints),
                )
            )

        request = SemanticAssessmentRequestV2(
            subject=command.draft.subject,
            binding_id=command.draft.binding_id,
            expected_binding_revision=command.draft.expected_binding_revision,
            guideline_revision_id=command.draft.guideline_revision_id,
            idempotency_key=command.draft.idempotency_key,
            confidence=command.draft.confidence,
            assessor=command.draft.assessor,
            metric_results=tuple(resolved_metrics),
        )
        persisted = await persistence.save_semantic_assessment_v2(request)
        if not isinstance(persisted, SemanticAssessmentV2PersistenceResult):
            raise TypeError("semantic_assessment_v2_persistence_result_invalid")
        if uow is not None:
            await uow.commit()
        return SealSemanticGuidelineAssessmentV2Result(
            request=request,
            persistence=persisted,
        )


__all__ = [
    "SealSemanticGuidelineAssessmentV2Command",
    "SealSemanticGuidelineAssessmentV2Result",
    "SealSemanticGuidelineAssessmentV2UseCase",
    "semantic_assessment_v2_write_projection",
    "GetCurrentSemanticGuidelineAssessmentAnyResult",
    "GetCurrentSemanticGuidelineAssessmentAnyUseCase",
    "semantic_assessment_v2_current_projection",
]
