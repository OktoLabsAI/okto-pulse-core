"""Public Core ports for authorized semantic anchor projection and v2 writes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal, Protocol, runtime_checkable

from okto_pulse.core.domain.guideline_policy import PolicySubjectRef
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentContractError,
)
from okto_pulse.core.domain.guideline_semantic_findings_v2 import (
    SemanticAssessmentReceiptProjectionV2,
)
from okto_pulse.core.domain.guideline_semantic_v2 import (
    AnchorSnapshot,
    SemanticAssessmentRequestV2,
    SemanticPinpointV2,
)
from okto_pulse.core.domain.quality_assessment import UnboundFindingAnchor
from okto_pulse.core.ports.guideline_policy import GuidelinePolicyAdapterMissing


class SemanticSubjectProjectionFailure(str, Enum):
    MISSING = "missing"
    MALFORMED = "malformed"
    FORBIDDEN = "forbidden"


class SemanticSubjectProjectionError(SemanticAssessmentContractError):
    """Closed resolution failure raised before any persistence operation."""

    def __init__(self, reason: SemanticSubjectProjectionFailure) -> None:
        if not isinstance(reason, SemanticSubjectProjectionFailure):
            raise TypeError("semantic_subject_projection_reason_invalid")
        self.reason = reason
        super().__init__(f"semantic_subject_projection_{reason.value}")


@dataclass(frozen=True, slots=True)
class SemanticSubjectProjectionRequest:
    subject: PolicySubjectRef
    anchor: UnboundFindingAnchor
    actor_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_subject_projection_subject_invalid"
            )
        if not isinstance(self.anchor, UnboundFindingAnchor):
            raise SemanticAssessmentContractError(
                "semantic_subject_projection_anchor_invalid"
            )
        if not isinstance(self.actor_id, str) or not self.actor_id.strip():
            raise SemanticAssessmentContractError(
                "semantic_subject_projection_actor_id_required"
            )
        object.__setattr__(self, "actor_id", self.actor_id.strip())


@runtime_checkable
class SemanticSubjectProjectionPort(Protocol):
    async def resolve_semantic_anchor(
        self,
        request: SemanticSubjectProjectionRequest,
    ) -> AnchorSnapshot:
        """Return an authorized snapshot or raise one closed projection error."""
        ...


@dataclass(frozen=True, slots=True)
class SemanticAssessmentV2PersistenceResult:
    receipt_id: str
    request_digest: str
    receipt: SemanticAssessmentReceiptProjectionV2 | None = None
    contract_version: Literal[2] = 2

    def __post_init__(self) -> None:
        if self.contract_version != 2:
            raise SemanticAssessmentContractError(
                "semantic_assessment_persistence_version_invalid"
            )
        if not isinstance(self.receipt_id, str) or not self.receipt_id.strip():
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_id_required"
            )
        if (
            not isinstance(self.request_digest, str)
            or len(self.request_digest) != 64
            or any(
                character not in "0123456789abcdef" for character in self.request_digest
            )
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_request_digest_invalid"
            )
        object.__setattr__(self, "receipt_id", self.receipt_id.strip())
        if self.receipt is not None and (
            not isinstance(self.receipt, SemanticAssessmentReceiptProjectionV2)
            or self.receipt.receipt_id != self.receipt_id
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_persistence_receipt_invalid"
            )


@runtime_checkable
class SemanticAssessmentV2PersistencePort(Protocol):
    async def save_semantic_assessment_v2(
        self,
        request: SemanticAssessmentRequestV2,
    ) -> SemanticAssessmentV2PersistenceResult:
        """Persist exactly one already resolved v2 request atomically."""
        ...


@runtime_checkable
class SemanticAssessmentV2ReadPort(Protocol):
    async def get_current_semantic_assessment_v2(
        self,
        *,
        board_id: str,
        entity_type: str,
        subject_id: str,
        binding_id: str,
    ) -> SemanticAssessmentReceiptProjectionV2 | None:
        """Return only a live-current v2 receipt for the exact subject fence."""
        ...


@dataclass(frozen=True, slots=True)
class SemanticAssessmentV2CapabilitySnapshot:
    readers_ready: bool
    storage_ready: bool
    triggers_ready: bool
    rest_transport_ready: bool
    mcp_transport_ready: bool
    writer_requested: bool

    @property
    def writer_active(self) -> bool:
        return all(
            (
                self.readers_ready,
                self.storage_ready,
                self.triggers_ready,
                self.rest_transport_ready,
                self.mcp_transport_ready,
                self.writer_requested,
            )
        )

    @property
    def reason_code(self) -> str | None:
        if self.writer_active:
            return None
        if not self.writer_requested:
            return "unsupported_contract_version"
        return "v2_writer_not_ready"

    @property
    def state(self) -> str:
        if not self.writer_requested:
            return "disabled"
        if not self.readers_ready:
            return "readers_not_ready"
        if not self.storage_ready:
            return "storage_not_ready"
        if not self.triggers_ready:
            return "triggers_not_ready"
        if not self.rest_transport_ready or not self.mcp_transport_ready:
            return "transports_not_ready"
        return "active"


class SemanticAssessmentV2WriterUnavailable(GuidelinePolicyAdapterMissing):
    def __init__(self, snapshot: SemanticAssessmentV2CapabilitySnapshot) -> None:
        if not isinstance(snapshot, SemanticAssessmentV2CapabilitySnapshot):
            raise TypeError("semantic_assessment_v2_capability_snapshot_invalid")
        self.snapshot = snapshot
        self.code = snapshot.reason_code or "v2_writer_not_ready"
        super().__init__(
            self.code,
            details=(
                ("readers_ready", str(snapshot.readers_ready).lower()),
                ("storage_ready", str(snapshot.storage_ready).lower()),
                ("triggers_ready", str(snapshot.triggers_ready).lower()),
                ("rest_transport_ready", str(snapshot.rest_transport_ready).lower()),
                ("mcp_transport_ready", str(snapshot.mcp_transport_ready).lower()),
                ("writer_requested", str(snapshot.writer_requested).lower()),
                ("capability_state", snapshot.state),
            ),
        )


@runtime_checkable
class SemanticAssessmentV2CapabilityPort(Protocol):
    async def semantic_assessment_v2_capabilities(
        self,
    ) -> SemanticAssessmentV2CapabilitySnapshot: ...


@dataclass(frozen=True, slots=True)
class SemanticPinpointProjectionV1:
    anchor_type: str
    anchor_ref: str | None
    excerpt_hash: str | None
    contract_version: Literal[1] = 1


@dataclass(frozen=True, slots=True)
class SemanticPinpointProjectionV2:
    pinpoint_key: str
    kind: str
    title: str
    detail: str
    severity: str | None
    remediation: str | None
    anchor_type: str
    anchor_ref: str | None
    excerpt_hash: str | None
    label: str
    excerpt: str | None
    source_version: str
    availability_at_seal: str
    blocking: bool
    contract_version: Literal[2] = 2

    @classmethod
    def from_domain(
        cls,
        pinpoint: SemanticPinpointV2,
        *,
        blocking: bool,
    ) -> SemanticPinpointProjectionV2:
        if not isinstance(pinpoint, SemanticPinpointV2):
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_projection_v2_invalid"
            )
        if not isinstance(blocking, bool):
            raise SemanticAssessmentContractError(
                "semantic_pinpoint_projection_blocking_invalid"
            )
        return cls(
            pinpoint_key=pinpoint.pinpoint_key,
            kind=pinpoint.kind.value,
            title=pinpoint.title,
            detail=pinpoint.detail,
            severity=pinpoint.severity.value if pinpoint.severity else None,
            remediation=pinpoint.remediation,
            anchor_type=pinpoint.anchor.anchor_type.value,
            anchor_ref=pinpoint.anchor.anchor_ref,
            excerpt_hash=pinpoint.anchor.excerpt_hash,
            label=pinpoint.anchor_snapshot.label,
            excerpt=pinpoint.anchor_snapshot.excerpt,
            source_version=pinpoint.anchor_snapshot.source_version,
            availability_at_seal=(pinpoint.anchor_snapshot.availability_at_seal.value),
            blocking=blocking,
        )


SemanticPinpointProjection = SemanticPinpointProjectionV1 | SemanticPinpointProjectionV2


__all__ = [
    "SemanticAssessmentV2PersistencePort",
    "SemanticAssessmentV2ReadPort",
    "SemanticAssessmentV2CapabilityPort",
    "SemanticAssessmentV2CapabilitySnapshot",
    "SemanticAssessmentV2WriterUnavailable",
    "SemanticAssessmentV2PersistenceResult",
    "SemanticPinpointProjection",
    "SemanticPinpointProjectionV1",
    "SemanticPinpointProjectionV2",
    "SemanticSubjectProjectionError",
    "SemanticSubjectProjectionFailure",
    "SemanticSubjectProjectionPort",
    "SemanticSubjectProjectionRequest",
]
