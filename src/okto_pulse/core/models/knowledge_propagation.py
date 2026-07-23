"""Public transport schemas for selective Knowledge Base propagation v2.

The request envelope is intentionally explicit about the three selection
states.  In particular, an omitted envelope remains the legacy v1 path while
``selection_state="omitted"`` inside an envelope is an authoritative v2
choice.  Transport adapters must therefore test whether the envelope itself
was supplied; they must not infer intent from an empty list.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from okto_pulse.core.domain.knowledge_selection import (
    KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
    KnowledgeAssignmentState,
    KnowledgePropagationContractError,
    KnowledgePropagationMode,
    KnowledgeRelevanceEntityType,
    KnowledgeRelevanceLink,
    KnowledgeSelection,
    KnowledgeSelectionState,
)


NonEmptyText = Annotated[str, Field(min_length=1)]
NonNegativeRevision = Annotated[int, Field(ge=0)]


def _canonical_ids(values: list[str]) -> list[str]:
    canonical = sorted({value.strip() for value in values if value.strip()})
    if len(canonical) != len(values):
        raise ValueError("knowledge_ids must contain unique non-empty IDs")
    return canonical


def _normalized_required_text(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


class KnowledgeRelevanceLinkRequest(BaseModel):
    """Functional traceability link attached to a card assignment."""

    model_config = ConfigDict(extra="forbid")

    entity_type: KnowledgeRelevanceEntityType
    entity_id: NonEmptyText

    @field_validator("entity_id")
    @classmethod
    def _normalize_entity_id(cls, value: str) -> str:
        return _normalized_required_text(value, "entity_id")

    def to_domain(self) -> KnowledgeRelevanceLink:
        return KnowledgeRelevanceLink(
            entity_type=self.entity_type,
            entity_id=self.entity_id,
        )


class KnowledgePropagationEnvelopeV2(BaseModel):
    """Authoritative v2 selection envelope used by derive/create operations."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal[2] = KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    selection_state: KnowledgeSelectionState
    mode: KnowledgePropagationMode | None = None
    knowledge_ids: list[str] = Field(default_factory=list)
    justification: str | None = None
    idempotency_key: NonEmptyText
    expected_revision: Literal[0] | None = None
    relevance_links: list[KnowledgeRelevanceLinkRequest] = Field(
        default_factory=list,
        validation_alias=AliasChoices("relevance_links", "linkage"),
    )

    @field_validator("idempotency_key")
    @classmethod
    def _normalize_idempotency_key(cls, value: str) -> str:
        return _normalized_required_text(value, "idempotency_key")

    @field_validator("knowledge_ids")
    @classmethod
    def _validate_ids(cls, value: list[str]) -> list[str]:
        return _canonical_ids(value)

    @field_validator("justification")
    @classmethod
    def _normalize_justification(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def _validate_selection_contract(self) -> "KnowledgePropagationEnvelopeV2":
        # Reuse the public domain contract so REST and MCP reject the exact same
        # incoherent tri-state/mode combinations before a target can be created.
        KnowledgeSelection(
            selection_state=self.selection_state,
            mode=self.mode,
            knowledge_ids=tuple(self.knowledge_ids),
        )
        if (
            self.selection_state is not KnowledgeSelectionState.OMITTED
            and self.justification is None
        ):
            raise ValueError(
                "justification is required for non-omitted knowledge propagation"
            )
        return self

    def to_selection(self) -> KnowledgeSelection:
        return KnowledgeSelection(
            selection_state=self.selection_state,
            mode=self.mode,
            knowledge_ids=tuple(self.knowledge_ids),
        )

    def to_relevance_links(self) -> tuple[KnowledgeRelevanceLink, ...]:
        return tuple(link.to_domain() for link in self.relevance_links)


class DeriveSpecKnowledgeRequest(BaseModel):
    """Published v2 body for refinement derive; an absent body remains v1."""

    model_config = ConfigDict(extra="forbid")

    knowledge_propagation: KnowledgePropagationEnvelopeV2
    # Detection-only legacy slot.  The REST boundary rejects this field when
    # it co-occurs with ``knowledge_propagation`` before opening the target
    # transaction; v1 itself remains the body-absent path.
    kb_ids: list[str] | None = None


class _KnowledgeMutationRequestBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: Literal[2] = KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    knowledge_ids: list[str]
    justification: NonEmptyText
    idempotency_key: NonEmptyText
    expected_revision: NonNegativeRevision

    @field_validator("justification", "idempotency_key")
    @classmethod
    def _normalize_required_text(
        cls,
        value: str,
        info: Any,
    ) -> str:
        return _normalized_required_text(value, info.field_name)

    @field_validator("knowledge_ids")
    @classmethod
    def _validate_ids(cls, value: list[str]) -> list[str]:
        canonical = _canonical_ids(value)
        if not canonical:
            raise ValueError("knowledge_ids must contain at least one ID")
        return canonical


class KnowledgeAssignmentReplaceRequest(_KnowledgeMutationRequestBase):
    """Replace a card's effective selection with reference/snapshot assignments."""

    mode: Literal["reference", "snapshot"]
    linkage: list[KnowledgeRelevanceLinkRequest] = Field(
        default_factory=list,
        validation_alias=AliasChoices("linkage", "relevance_links"),
    )

    def to_envelope(self) -> KnowledgePropagationEnvelopeV2:
        # Existing-target optimistic revision belongs to the mutation command,
        # not to the creation-only envelope used here as a canonical selection
        # and linkage carrier.
        return KnowledgePropagationEnvelopeV2(
            selection_state=KnowledgeSelectionState.EXPLICIT_IDS,
            mode=self.mode,
            knowledge_ids=self.knowledge_ids,
            justification=self.justification,
            idempotency_key=self.idempotency_key,
            expected_revision=0,
            relevance_links=self.linkage,
        )


class KnowledgeAssignmentDropRequest(BaseModel):
    """Drop selected stable Knowledge roots from a card."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal[2] = KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    knowledge_ids: list[str] = Field(default_factory=list)
    justification: str | None = None
    idempotency_key: NonEmptyText
    expected_revision: NonNegativeRevision

    @field_validator("idempotency_key")
    @classmethod
    def _normalize_required_text(
        cls,
        value: str,
        info: Any,
    ) -> str:
        return _normalized_required_text(value, info.field_name)

    @field_validator("justification")
    @classmethod
    def _normalize_justification(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("knowledge_ids")
    @classmethod
    def _validate_ids(cls, value: list[str]) -> list[str]:
        return _canonical_ids(value)

    def to_envelope(self) -> KnowledgePropagationEnvelopeV2:
        # See ``KnowledgeAssignmentReplaceRequest.to_envelope``: the caller
        # passes this request's real expected_revision to the mutation command.
        if self.justification is None:
            raise KnowledgePropagationContractError(
                "knowledge_drop_justification_required",
                "drop operations require a non-empty justification",
            )
        return KnowledgePropagationEnvelopeV2(
            selection_state=(
                KnowledgeSelectionState.EXPLICIT_IDS
                if self.knowledge_ids
                else KnowledgeSelectionState.EXPLICIT_EMPTY
            ),
            mode=KnowledgePropagationMode.DROP,
            knowledge_ids=self.knowledge_ids,
            justification=self.justification,
            idempotency_key=self.idempotency_key,
            expected_revision=0,
        )


class KnowledgeAssignmentRefreshRequest(BaseModel):
    """Refresh selected snapshot assignments by stable Knowledge root ID."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal[2] = KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    knowledge_ids: list[str]
    idempotency_key: NonEmptyText
    expected_revision: NonNegativeRevision

    @field_validator("idempotency_key")
    @classmethod
    def _normalize_idempotency_key(cls, value: str) -> str:
        return _normalized_required_text(value, "idempotency_key")

    @field_validator("knowledge_ids")
    @classmethod
    def _validate_ids(cls, value: list[str]) -> list[str]:
        canonical = _canonical_ids(value)
        if not canonical:
            raise ValueError("knowledge_ids must contain at least one ID")
        return canonical


class KnowledgeMutationAssignmentResponse(BaseModel):
    """Stable flat assignment projection shared by REST and MCP."""

    model_config = ConfigDict(extra="forbid")

    root_knowledge_id: str
    source_knowledge_id: str
    mode: KnowledgePropagationMode
    state: KnowledgeAssignmentState
    stale: bool


class KnowledgeMutationResponse(BaseModel):
    """Canonical mutation projection stored with, and rebuilt from, the receipt."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal[2] = KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    target_type: Literal["spec", "card"]
    target_id: str
    operation_id: str
    revision: int
    replayed: bool
    selection_state: KnowledgeSelectionState
    assignments: list[KnowledgeMutationAssignmentResponse] = Field(
        default_factory=list
    )


class DeriveSpecKnowledgeMutationResponse(KnowledgeMutationResponse):
    """V2 response for a refinement→spec derivation."""

    target_type: Literal["spec"] = "spec"
    spec_id: str


class CardCreateKnowledgeMutationResponse(BaseModel):
    """Additive create response; ``card`` is the immutable receipt projection."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal[2] = KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    card: dict[str, Any]
    operation_id: str
    revision: int
    replayed: bool
    selection_state: KnowledgeSelectionState
    assignments: list[KnowledgeMutationAssignmentResponse] = Field(
        default_factory=list
    )


class KnowledgeRefreshItemResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    root_knowledge_id: str
    source_revision: str
    source_content_sha256: str
    stale: Literal[False] = False


class KnowledgeRefreshResponse(BaseModel):
    """Canonical result_v2 for explicit snapshot refresh."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal[2] = KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    operation_id: str
    revision: int
    replayed: bool
    refreshed: list[KnowledgeRefreshItemResponse] = Field(default_factory=list)


class KnowledgeAssignmentTechnicalProjection(BaseModel):
    model_config = ConfigDict(extra="allow")

    root_knowledge_id: str
    mode: KnowledgePropagationMode
    origin_class: str
    state: str
    stale: bool


class KnowledgeTechnicalReadResponse(BaseModel):
    """Technical dual-read projection for operators and diagnostic clients."""

    model_config = ConfigDict(extra="allow")

    contract_version: Literal[2] = KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    target: dict[str, str]
    revision: int
    v2_active: bool
    selection_state: KnowledgeSelectionState | None
    assignments: list[KnowledgeAssignmentTechnicalProjection] = Field(
        default_factory=list
    )
    scope_revision: int | None = None
    resolved_assignments: list[dict[str, Any]] = Field(default_factory=list)
    effective_assignment_ids: list[str] = Field(default_factory=list)
    effective_legacy_attachments: list[dict[str, Any]] = Field(default_factory=list)
    history_assignments: list[dict[str, Any]] = Field(default_factory=list)
    history_legacy_attachments: list[dict[str, Any]] = Field(default_factory=list)
    tombstones: list[dict[str, Any]] = Field(default_factory=list)
    snapshots: list[dict[str, Any]] = Field(default_factory=list)
    effective_count: int


__all__ = [
    "CardCreateKnowledgeMutationResponse",
    "DeriveSpecKnowledgeMutationResponse",
    "DeriveSpecKnowledgeRequest",
    "KnowledgeAssignmentDropRequest",
    "KnowledgeAssignmentRefreshRequest",
    "KnowledgeAssignmentReplaceRequest",
    "KnowledgeMutationResponse",
    "KnowledgeMutationAssignmentResponse",
    "KnowledgePropagationEnvelopeV2",
    "KnowledgeRefreshItemResponse",
    "KnowledgeRefreshResponse",
    "KnowledgeRelevanceLinkRequest",
    "KnowledgeAssignmentTechnicalProjection",
    "KnowledgeTechnicalReadResponse",
]
