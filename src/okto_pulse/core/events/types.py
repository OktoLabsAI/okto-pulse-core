"""Domain event types for the internal event bus.

Every state change in the domain (card created, spec moved, sprint closed,
etc.) is modelled as a typed event. Publishers in services/main.py publish
these via EventBus.publish(); handlers react asynchronously via the
EventDispatcher worker.

See core/events/README.md for the full list of events and how to add a
new one.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Annotated, ClassVar, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from okto_pulse.core.domain.guideline_impact import (
    GUIDELINE_ADOPTION_EVENT_TYPE,
    GUIDELINE_RETIREMENT_EVENT_TYPE,
    GuidelineBindingChangeEvent,
    GuidelineRetirementBoardEvent,
)
from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    GuidelineBindingProvenance,
    GuidelineBindingState,
    GuidelineEnforcement,
    GUIDELINE_IMPACT_CONTRACT_VERSION,
)

POLICY_BINDING_MATERIALIZED_EVENT_TYPE = (
    "board.semantic_policy_binding_materialized.v2"
)
POLICY_BINDING_MATERIALIZED_SCHEMA_VERSION = (
    "policy-binding-materialized/v2"
)
SEMANTIC_GUIDELINE_PROJECTION_EVENT_TYPE = (
    "guideline.semantic_kg_projection_changed.v1"
)
SEMANTIC_GUIDELINE_PROJECTION_SCHEMA_VERSION = (
    "semantic-guideline-kg-projection/v1"
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DomainEvent(BaseModel):
    """Base class for every domain event.

    Common fields (stored as dedicated columns on domain_events, NOT in
    payload_json): event_id, event_type, board_id, actor_id, actor_type,
    occurred_at. Subclasses add event-specific payload fields.
    """

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=False)

    event_type: ClassVar[str] = ""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    board_id: str
    actor_id: Optional[str] = None
    actor_type: str = "user"
    occurred_at: datetime = Field(default_factory=_utcnow)

    def payload_for_storage(self) -> dict:
        """Return only the event-specific fields for the payload_json column.

        The top-level columns (event_id/board_id/actor_id/actor_type/
        occurred_at) live in dedicated columns and are excluded here.
        event_type is also excluded since it's a dedicated column populated
        from the ClassVar.
        """
        return self.model_dump(
            mode="json",
            exclude={
                "event_id",
                "board_id",
                "actor_id",
                "actor_type",
                "occurred_at",
            },
        )


class PolicyAdoptionChanged(DomainEvent):
    """Closed delivery companion for adoption and unlink evidence.

    B08 persists the immutable pure-domain event.  This Pydantic companion is
    the delivery boundary consumed by the event worker and derived KG
    projection; its after-validator delegates semantic normalization to the
    canonical domain value instead of reimplementing those invariants.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=False,
        extra="forbid",
        frozen=True,
    )

    event_type: ClassVar[str] = GUIDELINE_ADOPTION_EVENT_TYPE
    event_schema_version: Literal["guideline-impact/v2"]
    actor_id: str
    actor_type: Literal["agent", "user", "system"]
    operation: Literal["adopt", "unlink"]
    guideline_id: str
    binding_id: str
    previous_binding_revision: int | None
    binding_revision: int
    from_revision_id: str | None
    from_semantic_version: str | None
    from_revision_digest: str | None
    to_revision_id: str | None
    to_semantic_version: str | None
    to_revision_digest: str | None
    impact_receipt_id: str | None
    impact_digest: str | None
    binding_digest_before: str
    binding_head_digest_before: str
    binding_head_digest_after: str
    policy_set_digest_before: str
    policy_set_digest_after: str
    policy_set_digest: str
    added_metric_ids: tuple[str, ...] = ()
    changed_metric_ids: tuple[str, ...] = ()
    removed_metric_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_domain_evidence(self) -> PolicyAdoptionChanged:
        if (
            self.event_schema_version != GUIDELINE_IMPACT_CONTRACT_VERSION
            or self.policy_set_digest != self.policy_set_digest_after
        ):
            raise ValueError("policy_adoption_event_evidence_invalid")
        normalized = GuidelineBindingChangeEvent(
            event_id=self.event_id,
            event_type=self.event_type,
            operation=self.operation,
            board_id=self.board_id,
            guideline_id=self.guideline_id,
            binding_id=self.binding_id,
            previous_binding_revision=self.previous_binding_revision,
            binding_revision=self.binding_revision,
            from_revision_id=self.from_revision_id,
            from_semantic_version=self.from_semantic_version,
            from_revision_digest=self.from_revision_digest,
            to_revision_id=self.to_revision_id,
            to_semantic_version=self.to_semantic_version,
            to_revision_digest=self.to_revision_digest,
            impact_receipt_id=self.impact_receipt_id,
            impact_digest=self.impact_digest,
            binding_digest_before=self.binding_digest_before,
            binding_head_digest_before=self.binding_head_digest_before,
            binding_head_digest_after=self.binding_head_digest_after,
            policy_set_digest_before=self.policy_set_digest_before,
            policy_set_digest_after=self.policy_set_digest_after,
            added_metric_ids=self.added_metric_ids,
            changed_metric_ids=self.changed_metric_ids,
            removed_metric_ids=self.removed_metric_ids,
            actor_id=self.actor_id,
            actor_type=self.actor_type,
            occurred_at=self.occurred_at,
        )
        for name in (
            "operation",
            "guideline_id",
            "binding_id",
            "previous_binding_revision",
            "binding_revision",
            "from_revision_id",
            "from_semantic_version",
            "from_revision_digest",
            "to_revision_id",
            "to_semantic_version",
            "to_revision_digest",
            "impact_receipt_id",
            "impact_digest",
            "binding_digest_before",
            "binding_head_digest_before",
            "binding_head_digest_after",
            "policy_set_digest_before",
            "policy_set_digest_after",
            "added_metric_ids",
            "changed_metric_ids",
            "removed_metric_ids",
            "actor_id",
            "actor_type",
            "occurred_at",
        ):
            object.__setattr__(self, name, getattr(normalized, name))
        object.__setattr__(
            self,
            "policy_set_digest",
            normalized.policy_set_digest_after,
        )
        return self

    @property
    def exact_revision_id(self) -> str:
        """Revision represented by this materialized policy transition."""

        value = (
            self.to_revision_id
            if self.operation == "adopt"
            else self.from_revision_id
        )
        assert value is not None
        return value


class PolicyRetirementChanged(DomainEvent):
    """Closed delivery companion for one board-policy retirement tombstone."""

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=False,
        extra="forbid",
        frozen=True,
    )

    event_type: ClassVar[str] = GUIDELINE_RETIREMENT_EVENT_TYPE
    event_schema_version: Literal["guideline-impact/v2"]
    actor_id: str
    actor_type: Literal["agent", "user", "system"]
    operation: Literal["retire"]
    guideline_id: str
    retirement_id: str
    retirement_status: Literal["retired", "superseded"]
    superseded_by_guideline_id: str | None
    binding_id: str
    binding_revision: int
    revision_id: str
    revision_number: int
    semantic_version: str
    revision_digest: str
    binding_digest_before: str
    binding_head_digest_before: str
    binding_head_digest_after: str
    policy_set_digest_before: str
    policy_set_digest_after: str
    policy_set_digest: str
    removed_metric_ids: tuple[str, ...] = ()
    request_digest: str

    @model_validator(mode="after")
    def validate_domain_evidence(self) -> PolicyRetirementChanged:
        if (
            self.event_schema_version != GUIDELINE_IMPACT_CONTRACT_VERSION
            or self.policy_set_digest != self.policy_set_digest_after
        ):
            raise ValueError("policy_retirement_event_evidence_invalid")
        normalized = GuidelineRetirementBoardEvent(
            event_id=self.event_id,
            event_type=self.event_type,
            operation=self.operation,
            board_id=self.board_id,
            guideline_id=self.guideline_id,
            retirement_id=self.retirement_id,
            retirement_status=self.retirement_status,
            superseded_by_guideline_id=self.superseded_by_guideline_id,
            binding_id=self.binding_id,
            binding_revision=self.binding_revision,
            revision_id=self.revision_id,
            revision_number=self.revision_number,
            semantic_version=self.semantic_version,
            revision_digest=self.revision_digest,
            binding_digest_before=self.binding_digest_before,
            binding_head_digest_before=self.binding_head_digest_before,
            binding_head_digest_after=self.binding_head_digest_after,
            policy_set_digest_before=self.policy_set_digest_before,
            policy_set_digest_after=self.policy_set_digest_after,
            removed_metric_ids=self.removed_metric_ids,
            actor_id=self.actor_id,
            actor_type=self.actor_type,
            occurred_at=self.occurred_at,
            request_digest=self.request_digest,
        )
        for name in (
            "operation",
            "guideline_id",
            "retirement_id",
            "retirement_status",
            "superseded_by_guideline_id",
            "binding_id",
            "binding_revision",
            "revision_id",
            "revision_number",
            "semantic_version",
            "revision_digest",
            "binding_digest_before",
            "binding_head_digest_before",
            "binding_head_digest_after",
            "policy_set_digest_before",
            "policy_set_digest_after",
            "removed_metric_ids",
            "actor_id",
            "actor_type",
            "occurred_at",
            "request_digest",
        ):
            object.__setattr__(self, name, getattr(normalized, name))
        object.__setattr__(
            self,
            "policy_set_digest",
            normalized.policy_set_digest_after,
        )
        return self

    @property
    def exact_revision_id(self) -> str:
        return self.revision_id


class PolicyBindingMaterialized(DomainEvent):
    """Closed companion for an ACTIVE inline/default binding materialization."""

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=False,
        extra="forbid",
        frozen=True,
        strict=True,
    )

    event_type: ClassVar[str] = POLICY_BINDING_MATERIALIZED_EVENT_TYPE
    event_id: str = Field(min_length=1, max_length=64)
    event_schema_version: Literal["policy-binding-materialized/v2"]
    actor_id: str
    actor_type: Literal["agent", "user", "system"]
    operation: Literal["adopt"]
    guideline_id: str
    binding_id: str
    binding_revision: int
    revision_id: str
    semantic_version: str
    revision_digest: str
    source_kind: Literal["native", "default_materialization"]
    enforcement: Literal["advisory", "blocking"]
    minimum_confidence: int = Field(ge=0, le=100)
    metric_threshold_overrides: dict[str, int]
    priority: int

    @model_validator(mode="after")
    def validate_binding_evidence(self) -> PolicyBindingMaterialized:
        if self.event_schema_version != (
            POLICY_BINDING_MATERIALIZED_SCHEMA_VERSION
        ):
            raise ValueError("policy_binding_materialized_evidence_invalid")
        normalized = BoardGuidelineBinding(
            binding_id=self.binding_id,
            board_id=self.board_id,
            guideline_id=self.guideline_id,
            revision_id=self.revision_id,
            semantic_version=self.semantic_version,
            revision_digest=self.revision_digest,
            priority=self.priority,
            binding_revision=self.binding_revision,
            adopted_by=self.actor_id,
            adopted_at=self.occurred_at,
            enforcement=GuidelineEnforcement(self.enforcement),
            minimum_confidence=self.minimum_confidence,
            metric_threshold_overrides=self.metric_threshold_overrides,
            state=GuidelineBindingState.ACTIVE,
            source_kind=GuidelineBindingProvenance(self.source_kind),
        )
        for name in (
            "board_id",
            "guideline_id",
            "binding_id",
            "binding_revision",
            "revision_id",
            "semantic_version",
            "revision_digest",
            "priority",
            "actor_id",
            "occurred_at",
        ):
            source_name = (
                "adopted_by"
                if name == "actor_id"
                else "adopted_at"
                if name == "occurred_at"
                else name
            )
            object.__setattr__(
                self,
                name,
                getattr(normalized, source_name),
            )
        object.__setattr__(
            self,
            "enforcement",
            normalized.enforcement.value,
        )
        object.__setattr__(
            self,
            "minimum_confidence",
            normalized.minimum_confidence,
        )
        object.__setattr__(
            self,
            "metric_threshold_overrides",
            dict(normalized.metric_threshold_overrides),
        )
        object.__setattr__(
            self,
            "source_kind",
            normalized.source_kind.value,
        )
        return self

    @property
    def exact_revision_id(self) -> str:
        return self.revision_id


class SemanticGuidelineProjectionChanged(DomainEvent):
    """Durable board-KG projection intent for semantic guideline evidence.

    Producers append this event and its handler execution in the same
    relational unit of work as the authoritative mutation.  The dispatcher
    can therefore observe it only after commit, while a rollback removes both
    the authority and its projection intent.  ``entity_digest`` is the exact
    immutable/head digest used by the edition adapter to fail closed before
    reconciling the graph.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=False,
        extra="forbid",
        frozen=True,
        strict=True,
    )

    event_type: ClassVar[str] = SEMANTIC_GUIDELINE_PROJECTION_EVENT_TYPE
    event_id: str = Field(min_length=1, max_length=64)
    event_schema_version: Literal["semantic-guideline-kg-projection/v1"]
    actor_id: str = Field(min_length=1, max_length=255)
    actor_type: Literal["agent", "user", "system"]
    entity_kind: Literal[
        "revision",
        "metric_definition",
        "binding_configuration",
        "assessment_receipt",
        "metric_result",
        "waiver",
        "skip",
    ]
    causation_id: str = Field(min_length=1, max_length=255)
    entity_id: str = Field(min_length=1, max_length=255)
    entity_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation: Literal["upsert", "terminate"]

    @model_validator(mode="after")
    def validate_projection_intent(self) -> SemanticGuidelineProjectionChanged:
        if (
            self.event_schema_version
            != SEMANTIC_GUIDELINE_PROJECTION_SCHEMA_VERSION
        ):
            raise ValueError("semantic_guideline_projection_evidence_invalid")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("semantic_guideline_projection_time_invalid")
        return self


PolicyConstraintChanged = (
    PolicyAdoptionChanged
    | PolicyRetirementChanged
    | PolicyBindingMaterialized
    | SemanticGuidelineProjectionChanged
)


class ArtifactArchiveChanged(DomainEvent):
    """Reversible archive/restore signal for KG-backed SDLC artifacts."""

    event_type: ClassVar[str] = "artifact.archive_changed"
    artifact_type: Literal["story", "ideation", "refinement", "spec", "card", "sprint"]
    artifact_id: str
    archived: bool


# --- Card lifecycle ---


class CardCreated(DomainEvent):
    event_type: ClassVar[str] = "card.created"
    card_id: str
    spec_id: str
    sprint_id: Optional[str] = None
    card_type: str = "normal"
    priority: str = "none"


class CardMoved(DomainEvent):
    event_type: ClassVar[str] = "card.moved"
    card_id: str
    from_status: str
    to_status: str
    spec_id: Optional[str] = None
    moved_by: Optional[str] = None


class CardCompletionRejected(DomainEvent):
    """A governed completion attempt produced an actionable rework cause."""

    event_type: ClassVar[str] = "card.completion_rejected"
    card_id: str
    spec_id: Optional[str] = None
    cause_kind: Literal["task_validation", "completion_gate"]
    cause_id: str
    cause_code: str
    cause_summary: str
    reason_codes: tuple[str, ...] = ()
    rejected_by: Optional[str] = None


class CardConclusionAdded(DomainEvent):
    """Fired when a card receives a non-empty execution conclusion.

    The handler enqueues the parent spec consolidation so that the KG
    reflects the card's narrative outcome alongside its final state.
    """

    event_type: ClassVar[str] = "card.conclusion_added"
    card_id: str
    spec_id: Optional[str] = None
    conclusion_excerpt: str = ""
    added_by: Optional[str] = None


class CardCancelled(DomainEvent):
    event_type: ClassVar[str] = "card.cancelled"
    card_id: str
    previous_status: str


class CardRestored(DomainEvent):
    event_type: ClassVar[str] = "card.restored"
    card_id: str
    to_status: str
    from_status: str = "cancelled"


# --- Spec lifecycle ---


class SpecCreated(DomainEvent):
    event_type: ClassVar[str] = "spec.created"
    spec_id: str
    source: Literal["manual", "derived_ideation", "derived_refinement"] = "manual"
    origin_id: Optional[str] = None


class SpecMoved(DomainEvent):
    event_type: ClassVar[str] = "spec.moved"
    spec_id: str
    from_status: str
    to_status: str


class SpecVersionBumped(DomainEvent):
    event_type: ClassVar[str] = "spec.version_bumped"
    spec_id: str
    old_version: int
    new_version: int
    changed_fields: list[str] = Field(default_factory=list)


class SpecDependencyAdded(DomainEvent):
    event_type: ClassVar[str] = "spec.dependency_added"
    spec_id: str
    dependency_id: str
    target_spec_id: str
    projection_owner_spec_id: str
    source_version: int
    source_status_on_create: str
    resolved_on_create: bool

    @model_validator(mode="after")
    def _projection_owner_is_dependent(self) -> "SpecDependencyAdded":
        if self.projection_owner_spec_id != self.spec_id:
            raise ValueError("spec_dependency_projection_owner_invalid")
        return self


class SpecDependencyRemoved(DomainEvent):
    event_type: ClassVar[str] = "spec.dependency_removed"
    spec_id: str
    dependency_id: str
    target_spec_id: str
    projection_owner_spec_id: str
    source_version: int
    removal_reason: str

    @model_validator(mode="after")
    def _projection_owner_is_dependent(self) -> "SpecDependencyRemoved":
        if self.projection_owner_spec_id != self.spec_id:
            raise ValueError("spec_dependency_projection_owner_invalid")
        return self


class SpecSemanticChanged(DomainEvent):
    """Fired when semantic spec content changes WITHOUT bumping version.

    Covers fields that affect KG extraction but are intentionally excluded
    from `content_fields` in update_spec (decisions, business_rules,
    api_contracts, test_scenarios, screen_mockups, architecture_designs).
    The ConsolidationEnqueuer
    handler maps this event to a spec consolidation enqueue so the KG stays
    in sync with structured-section mutations.
    """

    event_type: ClassVar[str] = "spec.semantic_changed"
    spec_id: str
    changed_fields: list[str] = Field(default_factory=list)


class StructuredSpecEntityEvent(DomainEvent):
    """Base payload for structured spec child entity changes.

    The event row columns carry board_id, actor_id and occurred_at. The
    payload stores stable child metadata so Discovery and deterministic KG
    handlers can reprocess the parent spec without depending on list indexes.
    """

    spec_id: str
    entity_type: str
    entity_id: str
    child_ref: str
    operation: str
    changed_fields: list[str] = Field(default_factory=list)
    spec_version: int


class StructuredSpecEntityCreated(StructuredSpecEntityEvent):
    event_type: ClassVar[str] = "structured_entity.created"


class StructuredSpecEntityUpdated(StructuredSpecEntityEvent):
    event_type: ClassVar[str] = "structured_entity.updated"


class StructuredSpecEntityRevoked(StructuredSpecEntityEvent):
    event_type: ClassVar[str] = "structured_entity.revoked"


class RefinementSemanticChanged(DomainEvent):
    """Fired when semantic refinement content changes.

    Mirrors SpecSemanticChanged for refinements. Triggers re-consolidation
    via ConsolidationEnqueuer → consolidation_worker (artifact_type=refinement).
    """

    event_type: ClassVar[str] = "refinement.semantic_changed"
    refinement_id: str
    changed_fields: list[str] = Field(default_factory=list)


class RefinementMoved(DomainEvent):
    event_type: ClassVar[str] = "refinement.moved"
    refinement_id: str
    from_status: str
    to_status: str


class QualityAssessmentRecorded(DomainEvent):
    """Version-bearing SK-A event staged with the relational assessment UoW."""

    event_type: ClassVar[str] = "quality.assessment_recorded.v1"
    event_schema_version: Literal[1] = 1
    subject_type: Literal["ideation", "refinement", "spec"]
    subject_id: str
    subject_version: int = Field(..., ge=1)
    assessment_kind: Literal[
        "ambiguity",
        "spec_validation",
        "requirement_lint",
    ]
    receipt_id: str
    input_digest: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    request_fingerprint: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    authority_digest: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    head_revision: int = Field(..., ge=1)


class QualityClarificationChanged(DomainEvent):
    """A subject Q&A mutation that may change assessment currentness."""

    event_type: ClassVar[str] = "quality.clarification_changed.v1"
    event_schema_version: Literal[1] = 1
    subject_type: Literal["ideation", "refinement", "spec"]
    subject_id: str
    subject_version: int = Field(..., ge=1)
    qa_id: str | None = None
    operation: Literal["created", "answered", "deleted"]


class ResearchDecisionChanged(DomainEvent):
    """Common v1 payload for an append-only research-decision head change."""

    event_schema_version: Literal[1] = 1
    contract_version: Literal["research-decision-ledger/v1"] = (
        "research-decision-ledger/v1"
    )
    refinement_id: str
    refinement_version: int = Field(..., ge=1)
    ledger_id: str
    entry_id: str
    head_revision: int = Field(..., ge=1)
    status: Literal["open", "investigating", "resolved", "deferred"]


class ResearchDecisionAppended(ResearchDecisionChanged):
    """A new relational RDL head was appended for a Refinement."""

    event_type: ClassVar[str] = "research_decision.appended"


class ResearchDecisionSuperseded(ResearchDecisionChanged):
    """An existing relational RDL head advanced to an immutable successor."""

    event_type: ClassVar[str] = "research_decision.superseded"


class ChecklistBindingChanged(DomainEvent):
    """Append-only A3 governance audit staged with the binding transaction."""

    event_type: ClassVar[str] = "checklist.binding_changed.v1"
    event_schema_version: Literal[1] = 1
    target_type: Literal["spec"] = "spec"
    phase: Literal["spec_validation"] = "spec_validation"
    template_version: Literal["/specify/v1"] = "/specify/v1"
    mode: Literal["off", "advisory", "blocking"]
    binding_version: int = Field(..., ge=1)
    binding_digest: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    previous_mode: Literal["off", "advisory", "blocking"] | None = None
    previous_binding_version: int | None = Field(default=None, ge=1)
    change_source: Literal["board_bootstrap", "board_governance"]


class CardLinkedToSpec(DomainEvent):
    """Fired when a card is linked to a spec via link_card_to_spec.

    The handler enqueues a spec consolidation (NOT card) — the card extractor
    does not reference spec_id, but the spec extractor must reflect the new
    cards list.
    """

    event_type: ClassVar[str] = "card.linked_to_spec"
    card_id: str
    spec_id: str


class CardUnlinkedFromSpec(DomainEvent):
    """Fired when a card is unlinked from a spec.

    Symmetric to CardLinkedToSpec — spec re-consolidation reflects the
    removal in the cards list.
    """

    event_type: ClassVar[str] = "card.unlinked_from_spec"
    card_id: str
    spec_id: str


# --- Sprint lifecycle ---


class SprintCreated(DomainEvent):
    event_type: ClassVar[str] = "sprint.created"
    sprint_id: str
    spec_id: str


class SprintMoved(DomainEvent):
    event_type: ClassVar[str] = "sprint.moved"
    sprint_id: str
    from_status: str
    to_status: str


class SprintClosed(DomainEvent):
    event_type: ClassVar[str] = "sprint.closed"
    sprint_id: str


# --- Derivation events ---


class IdeationMoved(DomainEvent):
    """Fired whenever an ideation changes lifecycle status."""

    event_type: ClassVar[str] = "ideation.moved"
    ideation_id: str
    from_status: str
    to_status: str


class IdeationDerivedToSpec(DomainEvent):
    event_type: ClassVar[str] = "ideation.derived_to_spec"
    ideation_id: str
    spec_id: str


class RefinementDerivedToSpec(DomainEvent):
    event_type: ClassVar[str] = "refinement.derived_to_spec"
    refinement_id: str
    spec_id: str


# --- Story lifecycle ---


class StoryCreated(DomainEvent):
    event_type: ClassVar[str] = "story.created"
    story_id: str
    topic_id: str
    status: str = "draft"


class StoryUpdated(DomainEvent):
    event_type: ClassVar[str] = "story.updated"
    story_id: str
    changed_fields: list[str] = Field(default_factory=list)


class StoryMoved(DomainEvent):
    event_type: ClassVar[str] = "story.moved"
    story_id: str
    from_status: str
    to_status: str


class StoryLinkedToIdeation(DomainEvent):
    event_type: ClassVar[str] = "story.linked_to_ideation"
    story_id: str
    ideation_id: str


# --- Code Traceability (external-agent attestations; metadata-only events) ---


_TraceabilityId = Annotated[str, Field(min_length=1, max_length=255)]
_TraceabilityState = Annotated[str, Field(min_length=1, max_length=128)]
_TraceabilityDigest = Annotated[
    str,
    Field(min_length=64, max_length=64, pattern=r"^[0-9a-fA-F]{64}$"),
]
_TraceabilityCount = Annotated[int, Field(ge=0, le=1_000_000)]
_TraceabilityVersion = Annotated[int, Field(ge=1, le=2_147_483_647)]


class CodeTraceabilityDomainEvent(DomainEvent):
    """Closed, bounded event envelope with no operational code locator.

    Event-specific fields are limited to identifiers, states, digests and
    counts.  Repository paths, symbols, snippets, challenges and secrets are
    deliberately absent; consumers needing those details read the governed
    relational projection under their own authorization.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=False,
        extra="forbid",
        frozen=True,
    )

    actor_type: Literal["agent", "user", "system"] = "system"


class CodeInvestigationRequested(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_investigation.requested"
    investigation_request_id: _TraceabilityId
    subject_type: _TraceabilityState
    subject_id: _TraceabilityId
    subject_version: _TraceabilityVersion
    expected_head_generation: _TraceabilityCount
    required_capability_count: _TraceabilityCount
    selector_scope_digest: _TraceabilityDigest
    request_payload_sha256: _TraceabilityDigest


class CodeInvestigationReceiptSubmitted(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_investigation.receipt_submitted"
    investigation_request_id: _TraceabilityId
    investigation_receipt_id: _TraceabilityId
    acceptance_status: _TraceabilityState
    outcome: _TraceabilityState
    trust_level: _TraceabilityState
    generation: _TraceabilityVersion
    omission_count: _TraceabilityCount
    observation_sha256: _TraceabilityDigest
    payload_sha256: _TraceabilityDigest


class CodeInvestigationReceiptRevoked(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_investigation.receipt_revoked"
    investigation_receipt_id: _TraceabilityId
    revocation_id: _TraceabilityId
    reason_code: _TraceabilityState
    head_state: _TraceabilityState


class CodeEvidenceCreated(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_evidence.created"
    evidence_id: _TraceabilityId
    investigation_receipt_id: _TraceabilityId
    parent_type: _TraceabilityState
    parent_id: _TraceabilityId
    lifecycle_status: _TraceabilityState
    attestation_state: _TraceabilityState
    payload_sha256: _TraceabilityDigest


class CodeEvidenceSuperseded(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_evidence.superseded"
    superseded_evidence_id: _TraceabilityId
    superseding_evidence_id: _TraceabilityId
    investigation_receipt_id: _TraceabilityId
    payload_sha256: _TraceabilityDigest


class CodeEvidenceRevoked(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_evidence.revoked"
    evidence_id: _TraceabilityId
    lifecycle_status: _TraceabilityState
    reason_code: _TraceabilityState
    reason_sha256: _TraceabilityDigest


class CodeEvidenceLinked(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_evidence.linked"
    evidence_id: _TraceabilityId
    link_id: _TraceabilityId
    spec_id: _TraceabilityId
    entity_type: _TraceabilityState
    entity_id: _TraceabilityId
    relation_type: _TraceabilityState
    evidence_content_sha256: _TraceabilityDigest


class CodeEvidenceUnlinked(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_evidence.unlinked"
    evidence_id: _TraceabilityId
    link_id: _TraceabilityId
    spec_id: _TraceabilityId
    entity_type: _TraceabilityState
    entity_id: _TraceabilityId
    relation_type: _TraceabilityState
    reason_sha256: _TraceabilityDigest


class CodeEvidenceDispositionChanged(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_evidence.disposition_changed"
    evidence_id: _TraceabilityId
    disposition_id: _TraceabilityId
    spec_id: _TraceabilityId
    disposition: _TraceabilityState
    active_state: _TraceabilityState
    spec_version: _TraceabilityVersion


class ImplementationTargetCreated(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "implementation_target.created"
    target_id: _TraceabilityId
    card_id: _TraceabilityId
    lifecycle_status: _TraceabilityState
    revision: _TraceabilityVersion
    payload_sha256: _TraceabilityDigest


class ImplementationTargetUpdated(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "implementation_target.updated"
    target_id: _TraceabilityId
    card_id: _TraceabilityId
    lifecycle_status: _TraceabilityState
    previous_revision: _TraceabilityVersion
    revision: _TraceabilityVersion
    change_reason_sha256: _TraceabilityDigest


class ImplementationTargetRevoked(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "implementation_target.revoked"
    target_id: _TraceabilityId
    card_id: _TraceabilityId
    lifecycle_status: _TraceabilityState
    revision: _TraceabilityVersion
    reason_sha256: _TraceabilityDigest


class ImplementationTargetResolutionSubmitted(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "implementation_target.resolution_submitted"
    target_id: _TraceabilityId
    resolution_id: _TraceabilityId
    investigation_receipt_id: _TraceabilityId
    resolution_state: _TraceabilityState
    target_revision: _TraceabilityVersion
    receipt_generation: _TraceabilityVersion
    candidate_count: _TraceabilityCount
    selector_fingerprint: _TraceabilityDigest
    payload_sha256: _TraceabilityDigest


class ImplementationTargetExecutionReceiptSubmitted(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = (
        "implementation_target.execution_receipt_submitted"
    )
    execution_record_id: _TraceabilityId
    target_id: _TraceabilityId
    card_id: _TraceabilityId
    result_investigation_receipt_id: _TraceabilityId
    disposition: _TraceabilityState
    target_revision: _TraceabilityVersion
    payload_sha256: _TraceabilityDigest


class ImplementationOverlapAcknowledged(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "implementation_overlap.acknowledged"
    acknowledgement_id: _TraceabilityId
    target_a_id: _TraceabilityId
    target_b_id: _TraceabilityId
    resolution_a_id: _TraceabilityId
    resolution_b_id: _TraceabilityId
    disposition: _TraceabilityState
    overlap_fingerprint: _TraceabilityDigest


class CodeTraceabilityWaiverCreated(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_traceability.waiver_created"
    waiver_id: _TraceabilityId
    subject_type: _TraceabilityState
    subject_id: _TraceabilityId
    subject_version: _TraceabilityVersion
    waiver_state: _TraceabilityState
    justification_sha256: _TraceabilityDigest


class CodeTraceabilityWaiverCleared(CodeTraceabilityDomainEvent):
    event_type: ClassVar[str] = "code_traceability.waiver_cleared"
    waiver_id: _TraceabilityId
    subject_type: _TraceabilityState
    subject_id: _TraceabilityId
    subject_version: _TraceabilityVersion
    waiver_state: _TraceabilityState
    reason_sha256: _TraceabilityDigest


# Exact public event vocabulary for handler registration and adapter seams.
# Keeping this tuple beside the closed event schemas prevents a downstream
# projection from silently missing a newly-added mutation event.
CODE_TRACEABILITY_EVENT_TYPES: tuple[str, ...] = (
    CodeInvestigationRequested.event_type,
    CodeInvestigationReceiptSubmitted.event_type,
    CodeInvestigationReceiptRevoked.event_type,
    CodeEvidenceCreated.event_type,
    CodeEvidenceSuperseded.event_type,
    CodeEvidenceRevoked.event_type,
    CodeEvidenceLinked.event_type,
    CodeEvidenceUnlinked.event_type,
    CodeEvidenceDispositionChanged.event_type,
    ImplementationTargetCreated.event_type,
    ImplementationTargetUpdated.event_type,
    ImplementationTargetRevoked.event_type,
    ImplementationTargetResolutionSubmitted.event_type,
    ImplementationTargetExecutionReceiptSubmitted.event_type,
    ImplementationOverlapAcknowledged.event_type,
    CodeTraceabilityWaiverCreated.event_type,
    CodeTraceabilityWaiverCleared.event_type,
)


# --- KG operational events (spec 28583299 — Ideação #4) ---


class KGHitFlushed(DomainEvent):
    """Fired when KGService._flush_hits persists a batch of query_hits to graph backend.

    The handler reacts by recomputing the node's relevance_score so the
    refreshed hit count immediately participates in ranking. Decoupling
    via DomainEvent (vs sync recompute on the read path) keeps the search
    hot path free of graph backend MATCH/COUNT pressure — see dec_3a6eb8ad.
    """

    event_type: ClassVar[str] = "kg.hit_flushed"
    node_type: str
    node_id: str
    hits_delta: int
    flushed_at: str  # ISO datetime string for replay determinism


class CardPriorityChanged(DomainEvent):
    """Fired when a card's priority changes via update_card.

    The handler recomputes the priority_boost on the card's root KG entity
    node and triggers a relevance_score recompute. Auditoria of significant
    boost changes (|delta| > 0.05) is recorded as a Decision node in the KG
    rather than a SQL audit table — see dec_cb956457.
    """

    event_type: ClassVar[str] = "card.priority_changed"
    card_id: str
    old_priority: Optional[str] = None
    new_priority: Optional[str] = None
    spec_id: Optional[str] = None
    changed_by: Optional[str] = None


class CardSeverityChanged(DomainEvent):
    """Fired when a Bug card's severity changes via update_card.

    Only emitted for ``card_type == 'bug'`` (BR1) — feature/task/chore cards
    have no severity semantics. Handler symmetry with CardPriorityChanged:
    recomputes priority_boost via MAX(priority, severity) and persists.
    """

    event_type: ClassVar[str] = "card.severity_changed"
    card_id: str
    old_severity: Optional[str] = None
    new_severity: Optional[str] = None
    spec_id: Optional[str] = None
    changed_by: Optional[str] = None


class BugRegressionScenarioReuseDecision(DomainEvent):
    """Bounded audit event for bug regression scenario reuse decisions."""

    event_type: ClassVar[str] = "bug_regression_scenario_reuse_decision"
    bug_id: str
    spec_id: str
    decision: Literal["eligible", "rejected", "semantic_gap"]
    reason_code: str
    # Path B coverage state (G2 / card 966c7e7c): bounded diagnostic on the audit
    # event (path_b_ready / coverage_pending / not_applicable). Empty for Path A.
    coverage_state: str = ""
    scenario_count: int = 0
    test_task_count: int = 0


class KGDailyTick(DomainEvent):
    """Fired by the active scheduler adapter to drive global decay.

    The trigger interval is ``kg_decay_tick_interval_minutes`` (configured in
    ``config.py``; registered through the SchedulerControl port). Uses
    ``board_id="*"`` as a global sentinel because the handler iterates every
    active board. Only the leader replica emits the event (advisory lock);
    other replicas log a skip — see dec_bc0eaeec.
    """

    event_type: ClassVar[str] = "kg.tick.daily"
    tick_id: str  # uuid4 per tick run, propagates into kg_tick_runs row
    scheduled_at: str  # ISO datetime when the scheduler fired the trigger
    # Manual full rebuilds are executed by the durable handler, after the
    # delivery execution has been committed as ``processing``.  Keeping this
    # intent in the event closes the recovery-admission race that existed when
    # the API reset graph revisions before publishing the event.
    # A forced rebuild must use KGFullRebuildTick's distinct event type.  Old
    # consumers ignore additive payload fields, so accepting True here would
    # allow the intent to be silently downgraded to an ordinary daily tick.
    force_full_rebuild: Literal[False] = False


class KGFullRebuildTick(KGDailyTick):
    """Fail-closed durable intent for a forced full KG recomputation.

    Deploy consumers before producers (or drain this event type before a
    downgrade). Older consumers do not know this type and therefore retry it
    instead of silently acknowledging an ordinary tick without the reset.
    """

    event_type: ClassVar[str] = "kg.tick.full_rebuild"
    force_full_rebuild: Literal[True] = True


class KGDeliveryRedriveTick(DomainEvent):
    """Durable bounded continuation of tick-owned delivery-debt recovery.

    A daily tick starts the chain.  Each event owns exactly one budgeted,
    globally fair redrive run and transactionally publishes its successor
    only while due debt remains.  ``checkpoint_version`` identifies the
    durable checkpoint produced by the run that scheduled this continuation.
    """

    event_type: ClassVar[str] = "kg.tick.delivery_redrive"
    run_id: str
    scheduled_at: str
    checkpoint_version: int = 0


# Ordered list of all event_type strings known to the MVP. The dispatcher
# uses this to resolve DomainEventRow → subclass during reconstruction.
EVENT_TYPES: list[str] = [
    PolicyAdoptionChanged.event_type,
    PolicyRetirementChanged.event_type,
    PolicyBindingMaterialized.event_type,
    SemanticGuidelineProjectionChanged.event_type,
    ArtifactArchiveChanged.event_type,
    CardCreated.event_type,
    CardMoved.event_type,
    CardCompletionRejected.event_type,
    CardConclusionAdded.event_type,
    CardCancelled.event_type,
    CardRestored.event_type,
    CardLinkedToSpec.event_type,
    CardUnlinkedFromSpec.event_type,
    SpecCreated.event_type,
    SpecMoved.event_type,
    SpecVersionBumped.event_type,
    SpecDependencyAdded.event_type,
    SpecDependencyRemoved.event_type,
    SpecSemanticChanged.event_type,
    StructuredSpecEntityCreated.event_type,
    StructuredSpecEntityUpdated.event_type,
    StructuredSpecEntityRevoked.event_type,
    RefinementSemanticChanged.event_type,
    RefinementMoved.event_type,
    QualityAssessmentRecorded.event_type,
    QualityClarificationChanged.event_type,
    ResearchDecisionAppended.event_type,
    ResearchDecisionSuperseded.event_type,
    ChecklistBindingChanged.event_type,
    SprintCreated.event_type,
    SprintMoved.event_type,
    SprintClosed.event_type,
    IdeationMoved.event_type,
    IdeationDerivedToSpec.event_type,
    RefinementDerivedToSpec.event_type,
    StoryCreated.event_type,
    StoryUpdated.event_type,
    StoryMoved.event_type,
    StoryLinkedToIdeation.event_type,
    CodeInvestigationRequested.event_type,
    CodeInvestigationReceiptSubmitted.event_type,
    CodeInvestigationReceiptRevoked.event_type,
    CodeEvidenceCreated.event_type,
    CodeEvidenceSuperseded.event_type,
    CodeEvidenceRevoked.event_type,
    CodeEvidenceLinked.event_type,
    CodeEvidenceUnlinked.event_type,
    CodeEvidenceDispositionChanged.event_type,
    ImplementationTargetCreated.event_type,
    ImplementationTargetUpdated.event_type,
    ImplementationTargetRevoked.event_type,
    ImplementationTargetResolutionSubmitted.event_type,
    ImplementationTargetExecutionReceiptSubmitted.event_type,
    ImplementationOverlapAcknowledged.event_type,
    CodeTraceabilityWaiverCreated.event_type,
    CodeTraceabilityWaiverCleared.event_type,
    KGHitFlushed.event_type,
    CardPriorityChanged.event_type,
    CardSeverityChanged.event_type,
    BugRegressionScenarioReuseDecision.event_type,
    KGDailyTick.event_type,
    KGFullRebuildTick.event_type,
    KGDeliveryRedriveTick.event_type,
]


_EVENT_CLASS_BY_TYPE: dict[str, type[DomainEvent]] = {
    PolicyAdoptionChanged.event_type: PolicyAdoptionChanged,
    PolicyRetirementChanged.event_type: PolicyRetirementChanged,
    PolicyBindingMaterialized.event_type: PolicyBindingMaterialized,
    SemanticGuidelineProjectionChanged.event_type: SemanticGuidelineProjectionChanged,
    ArtifactArchiveChanged.event_type: ArtifactArchiveChanged,
    CardCreated.event_type: CardCreated,
    CardMoved.event_type: CardMoved,
    CardCompletionRejected.event_type: CardCompletionRejected,
    CardConclusionAdded.event_type: CardConclusionAdded,
    CardCancelled.event_type: CardCancelled,
    CardRestored.event_type: CardRestored,
    CardLinkedToSpec.event_type: CardLinkedToSpec,
    CardUnlinkedFromSpec.event_type: CardUnlinkedFromSpec,
    SpecCreated.event_type: SpecCreated,
    SpecMoved.event_type: SpecMoved,
    SpecVersionBumped.event_type: SpecVersionBumped,
    SpecDependencyAdded.event_type: SpecDependencyAdded,
    SpecDependencyRemoved.event_type: SpecDependencyRemoved,
    SpecSemanticChanged.event_type: SpecSemanticChanged,
    StructuredSpecEntityCreated.event_type: StructuredSpecEntityCreated,
    StructuredSpecEntityUpdated.event_type: StructuredSpecEntityUpdated,
    StructuredSpecEntityRevoked.event_type: StructuredSpecEntityRevoked,
    RefinementSemanticChanged.event_type: RefinementSemanticChanged,
    RefinementMoved.event_type: RefinementMoved,
    QualityAssessmentRecorded.event_type: QualityAssessmentRecorded,
    QualityClarificationChanged.event_type: QualityClarificationChanged,
    ResearchDecisionAppended.event_type: ResearchDecisionAppended,
    ResearchDecisionSuperseded.event_type: ResearchDecisionSuperseded,
    ChecklistBindingChanged.event_type: ChecklistBindingChanged,
    SprintCreated.event_type: SprintCreated,
    SprintMoved.event_type: SprintMoved,
    SprintClosed.event_type: SprintClosed,
    IdeationMoved.event_type: IdeationMoved,
    IdeationDerivedToSpec.event_type: IdeationDerivedToSpec,
    RefinementDerivedToSpec.event_type: RefinementDerivedToSpec,
    StoryCreated.event_type: StoryCreated,
    StoryUpdated.event_type: StoryUpdated,
    StoryMoved.event_type: StoryMoved,
    StoryLinkedToIdeation.event_type: StoryLinkedToIdeation,
    CodeInvestigationRequested.event_type: CodeInvestigationRequested,
    CodeInvestigationReceiptSubmitted.event_type: CodeInvestigationReceiptSubmitted,
    CodeInvestigationReceiptRevoked.event_type: CodeInvestigationReceiptRevoked,
    CodeEvidenceCreated.event_type: CodeEvidenceCreated,
    CodeEvidenceSuperseded.event_type: CodeEvidenceSuperseded,
    CodeEvidenceRevoked.event_type: CodeEvidenceRevoked,
    CodeEvidenceLinked.event_type: CodeEvidenceLinked,
    CodeEvidenceUnlinked.event_type: CodeEvidenceUnlinked,
    CodeEvidenceDispositionChanged.event_type: CodeEvidenceDispositionChanged,
    ImplementationTargetCreated.event_type: ImplementationTargetCreated,
    ImplementationTargetUpdated.event_type: ImplementationTargetUpdated,
    ImplementationTargetRevoked.event_type: ImplementationTargetRevoked,
    ImplementationTargetResolutionSubmitted.event_type: (
        ImplementationTargetResolutionSubmitted
    ),
    ImplementationTargetExecutionReceiptSubmitted.event_type: (
        ImplementationTargetExecutionReceiptSubmitted
    ),
    ImplementationOverlapAcknowledged.event_type: ImplementationOverlapAcknowledged,
    CodeTraceabilityWaiverCreated.event_type: CodeTraceabilityWaiverCreated,
    CodeTraceabilityWaiverCleared.event_type: CodeTraceabilityWaiverCleared,
    KGHitFlushed.event_type: KGHitFlushed,
    CardPriorityChanged.event_type: CardPriorityChanged,
    CardSeverityChanged.event_type: CardSeverityChanged,
    BugRegressionScenarioReuseDecision.event_type: BugRegressionScenarioReuseDecision,
    KGDailyTick.event_type: KGDailyTick,
    KGFullRebuildTick.event_type: KGFullRebuildTick,
    KGDeliveryRedriveTick.event_type: KGDeliveryRedriveTick,
}


def resolve_event_class(event_type: str) -> type[DomainEvent] | None:
    """Return the DomainEvent subclass that matches event_type, or None."""
    return _EVENT_CLASS_BY_TYPE.get(event_type)
