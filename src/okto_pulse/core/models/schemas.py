"""Pydantic schemas for API request/response models."""

import re
from collections.abc import Mapping
from datetime import datetime
from enum import Enum as PyEnum
from typing import Any, Generic, Literal, TypeAlias, TypeVar

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    computed_field,
    field_validator,
    model_validator,
)

from okto_pulse.core.discovery_params_schema import (
    DiscoveryParamsSchema,
    normalize_discovery_params_schema,
)
from okto_pulse.core.domain.code_traceability import (
    CodeTraceabilityEnforcement,
    DirectSpecDeliveryContextProvenance,
    DeliveryContext,
    SpecDeliveryContextProvenance,
)
from okto_pulse.core.domain.card_completion import (
    REJECTION_CODE_MAX_LENGTH,
    REJECTION_ID_MAX_LENGTH,
    REJECTION_SUMMARY_MAX_LENGTH,
)
from okto_pulse.core.domain.enums import (
    BugSeverity,
    CardPriority,
    CardStatus,
    CardType,
    IdeationComplexity,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintLaneType,
    SprintStatus,
    StoryStatus,
    TestScenarioStatus,
)
from okto_pulse.core.domain.knowledge_governance import (
    project_knowledge_governance,
)
from okto_pulse.core.domain.permissions import (
    PermissionContractViolation,
    validate_strict_permission_flags,
)
from okto_pulse.core.domain.quality_assessment import (
    AssessmentKind,
    AssessmentScaleKind,
    ScoreDirection,
)
from okto_pulse.core.domain.test_scenarios import (
    DEFAULT_SCENARIO_TYPE,
    SCENARIO_TYPE_DESCRIPTION,
    ScenarioType,
)
from okto_pulse.core.models.knowledge_propagation import (
    CardCreateKnowledgeMutationResponse,
    DeriveSpecKnowledgeMutationResponse,
    KnowledgePropagationEnvelopeV2,
)

# ============================================================================
# Base Schemas
# ============================================================================


class BaseSchema(BaseModel):
    """Base schema with common configuration.

    `extra="ignore"` is set explicitly so that legacy serialised payloads
    carrying removed fields (e.g. snapshots that still include the dropped
    `skills` field) are accepted silently — the unknown key is dropped
    without warning, without log, without error. This is the reader-side
    half of spec e12c4c20 (Skills removal).
    """

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class KnowledgeGovernanceResponseSchema(BaseSchema):
    """Shared additive read projection for every Knowledge Base surface."""

    governance_metadata: Any | None = None

    @computed_field(return_type=dict[str, Any])
    @property
    def governance(self) -> dict[str, Any]:
        return project_knowledge_governance(self.governance_metadata).as_dict()


# ============================================================================
# Agent Schemas
# ============================================================================


class AgentCreate(BaseModel):
    """Schema for creating a new agent."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    objective: str | None = None
    permissions: list[str] | None = None
    preset_id: str | None = None
    permission_flags: dict[str, Any] | None = None

    @field_validator("permission_flags")
    @classmethod
    def validate_permission_flags(
        cls, value: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        try:
            validate_strict_permission_flags(value)
        except PermissionContractViolation as exc:
            raise ValueError(str(exc)) from exc
        return value


class AgentUpdate(BaseModel):
    """Schema for updating an agent."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    objective: str | None = None
    is_active: bool | None = None
    permissions: list[str] | None = None
    preset_id: str | None = None
    permission_flags: dict[str, Any] | None = None

    @field_validator("permission_flags")
    @classmethod
    def validate_permission_flags(
        cls, value: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        try:
            validate_strict_permission_flags(value)
        except PermissionContractViolation as exc:
            raise ValueError(str(exc)) from exc
        return value


class AgentSelfUpdate(BaseModel):
    """Schema for agent self-updating its own profile."""

    description: str | None = None
    objective: str | None = None


class AgentResponse(BaseSchema):
    """Schema for agent response without recoverable credentials."""

    id: str
    name: str
    description: str | None
    objective: str | None = None
    is_active: bool
    permissions: list[str] | None
    preset_id: str | None = None
    permission_flags: dict[str, Any] | None = None
    created_by: str
    created_at: datetime
    last_used_at: datetime | None


class AgentSummary(BaseSchema):
    """Schema for agent summary (without sensitive data)."""

    id: str
    name: str
    description: str | None
    objective: str | None = None
    is_active: bool
    preset_id: str | None = None
    permission_flags: dict[str, Any] | None = None
    # Raw board ceiling, kept distinct from the agent's direct delta above.
    # This is an owner-facing projection used to edit the actual effective
    # permissions without materializing the delta as a new base snapshot.
    permission_overrides: dict[str, Any] | None = None
    created_at: datetime
    last_used_at: datetime | None


class AgentRevealResponse(BaseSchema):
    """Create/rotate response that exposes the raw credential exactly once."""

    agent: AgentResponse
    reveal_once_secret: str
    message: str | None = None


class AgentBoardResponse(BaseSchema):
    """Schema for agent-board grant."""

    id: str
    agent_id: str
    board_id: str
    granted_by: str
    granted_at: datetime
    permission_overrides: dict[str, Any] | None = None


class AgentBoardOverridesUpdate(BaseModel):
    """Schema for updating board-level permission overrides."""

    permission_overrides: dict[str, Any] | None = None

    @field_validator("permission_overrides")
    @classmethod
    def validate_permission_overrides(
        cls, value: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        try:
            validate_strict_permission_flags(value)
        except PermissionContractViolation as exc:
            raise ValueError(str(exc)) from exc
        return value


# ============================================================================
# Attachment Schemas
# ============================================================================


class AttachmentResponse(BaseSchema):
    """Schema for attachment response."""

    id: str
    card_id: str
    filename: str
    original_filename: str
    mime_type: str
    size: int
    uploaded_by: str
    created_at: datetime


class AttachmentUpload(BaseModel):
    """Schema for attachment upload response."""

    id: str
    filename: str
    original_filename: str
    mime_type: str
    size: int
    url: str


# ============================================================================
# QA Schemas
# ============================================================================


class QACreate(BaseModel):
    """Schema for creating a Q&A item."""

    question: str = Field(..., min_length=1)


class QAAnswer(BaseModel):
    """Schema for answering a Q&A item."""

    answer: str = Field(..., min_length=1)


class QAResponse(BaseSchema):
    """Schema for Q&A response."""

    id: str
    card_id: str
    question: str
    answer: str | None
    asked_by: str
    answered_by: str | None
    created_at: datetime
    answered_at: datetime | None


# ============================================================================
# Comment Schemas
# ============================================================================


class ChoiceOption(BaseModel):
    """A single option in a choice board."""

    id: str
    label: str
    recommended: bool = False
    tradeoff: str | None = None


class ChoiceResponse(BaseModel):
    """A response to a choice board."""

    responder_id: str
    responder_name: str
    selected: list[str] = []  # IDs of selected options
    free_text: str | None = None


class CommentCreate(BaseModel):
    """Schema for creating a comment.

    For text comments, only ``content`` is needed.
    For choice boards, set ``comment_type`` to "choice" or "multi_choice"
    and provide ``choices``.
    """

    content: str = Field(..., min_length=1)
    comment_type: str = "text"  # text | choice | multi_choice
    choices: list[ChoiceOption] | None = None
    allow_free_text: bool = False


class CommentUpdate(BaseModel):
    """Schema for updating a comment."""

    content: str = Field(..., min_length=1)


class CommentResponse(BaseSchema):
    """Schema for comment response."""

    id: str
    card_id: str
    content: str
    author_id: str
    comment_type: str = "text"
    choices: list[ChoiceOption] | None = None
    responses: list[ChoiceResponse] | None = None
    allow_free_text: bool = False
    created_at: datetime
    updated_at: datetime


# ============================================================================
# Test Scenario Schema
# ============================================================================


class TestEvidenceAssertionV2(BaseModel):
    """One machine-checkable assertion observed during a product execution."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    name: str = Field(..., min_length=1)
    expected: Any
    observed: Any
    status: Literal["passed", "failed"]
    message: str | None = None


class TestEvidenceProvenanceV2(BaseModel):
    """Identity of the Community adapter that produced an attestation."""

    model_config = ConfigDict(extra="forbid")

    producer: str = Field(..., min_length=1)
    producer_version: str = Field(..., min_length=1)
    adapter: str = Field(..., min_length=1)
    environment: str = Field(..., min_length=1)


class TestExecutionAttestationV2(BaseModel):
    """Evidence V2 result emitted after exercising the real product runtime.

    The CORE owns this transport-neutral contract and its pure verification
    rules.  Reading/running the manifest belongs to a concrete Community
    adapter; the adapter binds that execution to this payload with the manifest
    digest and the deterministic ``attestation_sha256``.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2] = 2
    run_id: str = Field(..., min_length=1)
    executed_at: str = Field(..., min_length=1)
    scenario_id: str = Field(..., min_length=1)
    # Optional only for lossless reads of pre-hardening V2 rows. Every new
    # gated write requires a valid digest in the semantic verifier.
    scenario_sha256: str | None = None
    outcome: Literal["passed", "failed"]
    product_runtime_exercised: bool
    manifest_sha256: str = Field(..., min_length=1)
    assertions: list[TestEvidenceAssertionV2] = Field(..., min_length=1)
    provenance: TestEvidenceProvenanceV2
    attestation_sha256: str = Field(..., min_length=1)


class TestScenarioEvidence(BaseModel):
    """Structured proof that a test scenario exists or was executed.

    Spec 9e0bf979 — re-executable validation evidence contract. ``evidence_class``
    classifies the KIND of proof (see ``test_scenario_lifecycle.EVIDENCE_CLASSES``)
    so a validator can rerun or inspect the artifact instead of trusting a raw
    run log. All new fields are additive and optional. Legacy evidence stays
    readable for backward compatibility; specifically, historical free-form
    MCP manifests are reader-only/unverified until a V2 execution attestation
    is produced.
    """

    model_config = ConfigDict(extra="allow")

    # Minimal/legacy fields (NC-9). Preserved verbatim for backward compatibility.
    test_file_path: str | None = None
    test_function: str | None = None
    last_run_at: str | None = None
    test_run_id: str | None = None
    output_snippet: str | None = None
    # Re-executable evidence contract (spec 9e0bf979, tr_61dabab8).
    evidence_class: str | None = None
    replay_command: str | None = None
    # Deprecated reader-only alias. Historical rows may contain either a string
    # or the free-form object accepted by the old status endpoint. Both remain
    # serializable so a GET -> SpecUpdate round-trip never loses data, but the
    # Evidence V2 gate treats them as ``legacy_unverified``.
    mcp_replay_manifest: str | dict[str, Any] | None = None
    # Evidence V2 canonical contract. New MCP replay writes use these two
    # fields; ``manifest_ref`` is always a reference, never an embedded object.
    manifest_ref: str | None = None
    execution_attestation: TestExecutionAttestationV2 | None = None
    # Opaque installation-issued receipt. CORE never derives or trusts this
    # value itself; the concrete edition authenticates it at every write.
    execution_receipt: str | None = None
    manual_checklist_ref: str | None = None
    expected_output_snapshot: str | None = None
    replay_should_exist: bool | None = None
    non_replayable_justification: str | None = None


class TestScenario(BaseModel):
    """Read-tolerant test scenario projection.

    ``scenario_type`` intentionally remains ``str`` here so persisted values
    from older releases can still be returned explicitly. Write requests use
    :class:`TestScenarioWrite`, whose JSON schema is the closed five-value
    taxonomy.
    """

    id: str
    title: str
    linked_criteria: list[str] | None = None  # indices or text of acceptance criteria
    scenario_type: str = DEFAULT_SCENARIO_TYPE
    given: str = ""  # precondition
    when: str = ""  # action
    then: str = ""  # expected result
    notes: str | None = None
    status: str = "draft"  # draft | ready | automated | passed | failed
    linked_task_ids: list[str] | None = (
        None  # card IDs that implement/automate this test
    )
    evidence: TestScenarioEvidence | None = None
    latest_evidence: TestScenarioEvidence | None = None


class TestScenarioWrite(TestScenario):
    """Write-facing scenario with a closed scenario-type interface.

    The default remains visible to API schema consumers, while Pydantic tracks
    whether it was omitted through ``model_fields_set``. Whole-list updates use
    that distinction to default new scenarios and preserve existing scenarios.
    """

    model_config = ConfigDict(extra="forbid")

    scenario_type: ScenarioType = Field(
        DEFAULT_SCENARIO_TYPE,
        description=SCENARIO_TYPE_DESCRIPTION,
    )
    status: TestScenarioStatus = Field(
        TestScenarioStatus.DRAFT,
        validate_default=True,
    )


# ============================================================================
# Screen Mockup Schemas
# ============================================================================


class MockupAnnotation(BaseModel):
    """A design note attached to a screen."""

    id: str
    text: str
    author_id: str | None = None


class ScreenMockup(BaseModel):
    """A single screen/view in the mockup set. Contains HTML+Tailwind content."""

    id: str
    title: str
    description: str | None = None
    screen_type: str = "page"  # page | modal | drawer | popover | panel
    html_content: str = ""
    annotations: list[MockupAnnotation] | None = None
    order: int = 0
    # Design System consumption metadata (spec 3a006f65 / card 0192f58d). Stored
    # normalized as {design_system_id, version}; the MockupDesignSystemGate cross-checks
    # it against the board's REAL effective Design System (the payload is never the
    # source of identity). Both default None so legacy mockups + off-mode boards are
    # unaffected.
    design_system_ref: dict[str, Any] | None = None
    design_system_evidence: Any | None = None


# ============================================================================
# Stories Schemas
# ============================================================================


class TopicCreate(BaseModel):
    """Schema for creating a board-scoped Story Topic."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None


class TopicUpdate(BaseModel):
    """Schema for updating a Story Topic."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    archived: bool | None = None


class TopicResponse(BaseSchema):
    """Schema for Topic responses."""

    id: str
    board_id: str
    name: str
    description: str | None
    archived: bool = False
    created_by: str
    created_at: datetime
    updated_at: datetime


class TopicSummary(TopicResponse):
    """Lightweight Topic summary. Kept separate for forward compatibility."""

    story_count: int = 0
    active_count: int = 0
    archived_count: int = 0
    total_associated_count: int = 0


class TopicDeleteResponse(BaseModel):
    """Response for safe Topic deletion."""

    success: bool = True
    deleted_topic_id: str


class TopicMergeRequest(BaseModel):
    """Request body for merging one Topic into another."""

    target_topic_id: str = Field(..., min_length=1)


class TopicMergeResponse(BaseModel):
    """Response for a Topic merge operation."""

    success: bool = True
    source: TopicSummary
    target: TopicSummary
    moved_count: int
    active_count: int
    archived_count: int
    target_total_before: int = 0
    target_total_after: int = 0


class StoryIdeationLinkResponse(BaseSchema):
    """Schema for the simple Story-Ideation link."""

    id: str
    board_id: str
    story_id: str
    ideation_id: str
    created_by: str
    created_at: datetime


class StoryCreate(BaseModel):
    """Schema for creating a lightweight Story."""

    title: str = Field(..., min_length=1, max_length=500)
    description: str = Field(..., min_length=1)
    topic_id: str
    actor: str | None = None
    goal: str | None = None
    benefit: str | None = None
    labels: list[str] | None = None
    status: StoryStatus = StoryStatus.DRAFT
    assignee_id: str | None = None
    screen_mockups: list[ScreenMockup] | None = None


class StoryUpdate(BaseModel):
    """Schema for updating a Story."""

    title: str | None = Field(None, min_length=1, max_length=500)
    description: str | None = Field(None, min_length=1)
    topic_id: str | None = None
    actor: str | None = None
    goal: str | None = None
    benefit: str | None = None
    labels: list[str] | None = None
    assignee_id: str | None = None
    screen_mockups: list[ScreenMockup] | None = None


class StoryMove(BaseModel):
    """Schema for changing Story status."""

    status: StoryStatus


class StoryLinkCreate(BaseModel):
    """Schema for linking a Story to an Ideation."""

    ideation_id: str


class StoryConversionRequest(BaseModel):
    """Schema for creating/linking Ideations from selected Stories."""

    story_ids: list[str] = Field(..., min_length=1)
    ideation_id: str | None = None
    title: str | None = Field(None, max_length=500)
    description: str | None = None
    problem_statement: str | None = None
    proposed_approach: str | None = None
    mockup_ids: list[str] | None = None
    mark_converted: bool = True


_PageItemT = TypeVar("_PageItemT")


class PageEnvelope(BaseSchema, Generic[_PageItemT]):
    """Paginated list envelope (spec 8b33f9a8, FR1/DR9).

    Returned by the list routes ONLY when the caller opts in with
    ``offset``/``limit``; without them the legacy shapes stay byte-identical.
    Both totals are ALWAYS server-computed, window-independent (KG
    dec-s05-01): ``total_filtered`` counts the filtered scope that produced
    ``items``; ``total_overall`` counts the base scope (board + archived
    policy) regardless of discretionary filters.
    """

    items: list[_PageItemT]
    total_filtered: int
    total_overall: int
    offset: int
    limit: int


class LookupItem(BaseSchema):
    """Lean entity identity returned by the board lookup endpoints."""

    id: str
    title: str
    status: str


class LookupResponse(BaseSchema):
    """Bounded response shared by spec and ideation lookups."""

    items: list[LookupItem]
    total: int = Field(..., ge=0)
    offset: int = Field(..., ge=0)
    limit: int = Field(..., ge=1, le=50)


class StoryPageItem(BaseSchema):
    """Lean Story projection for paginated lists (FR4/br_0ec07efd).

    The heavy ``screen_mockups`` HTML array is REPLACED by
    ``screen_mockups_count`` (badge-sufficient, derived from the loaded row
    without extra queries); ``ideation_links`` is likewise omitted from the
    paginated projection.
    """

    id: str
    board_id: str
    topic_id: str
    title: str
    description: str
    actor: str | None = None
    goal: str | None = None
    benefit: str | None = None
    labels: list[str] | None = None
    status: StoryStatus
    assignee_id: str | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime
    archived: bool = False
    screen_mockups_count: int = 0


class QualityScaleSummary(BaseSchema):
    """Closed scale projection embedded in paginated entity summaries."""

    kind: AssessmentScaleKind
    min: float
    max: float
    direction: ScoreDirection


class QualityAssessmentCurrentResultSummary(BaseSchema):
    """The one human-current quality result for the subject edition."""

    score: float
    scale: QualityScaleSummary


class QualityAssessmentSummary(BaseSchema):
    """Summary-first projection used by entity lists and workspace badges.

    Technical receipt/version/head fields deliberately do not belong to this
    projection.  They remain available from the lazy Technical audit surface.
    """

    edition: int = Field(..., ge=1)
    state: Literal["not_started", "current"]
    previous_count: int = Field(default=0, ge=0)
    current_result: QualityAssessmentCurrentResultSummary | None = None

    @model_validator(mode="after")
    def _current_result_matches_state(self) -> "QualityAssessmentSummary":
        if self.state == "current" and self.current_result is None:
            raise ValueError("quality_summary_current_result_required")
        if self.state == "not_started" and self.current_result is not None:
            raise ValueError("quality_summary_current_result_forbidden")
        return self


QualitySummaryMap: TypeAlias = dict[AssessmentKind, QualityAssessmentSummary]


class IdeationPageItem(BaseSchema):
    """Lean Ideation projection for paginated lists (FR4).

    Relationship collections such as ``architecture_designs`` stay off the
    paginated projection. Bounded SQL-derived badge counts are projected
    explicitly; ``scope_assessment`` rides the ORM column.
    """

    id: str
    board_id: str
    title: str
    description: str | None = None
    problem_statement: str | None = None
    complexity: IdeationComplexity | None = None
    status: IdeationStatus
    edition: int = Field(1, ge=1)
    version: int
    assignee_id: str | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None = None
    archived: bool = False
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    scope_assessment: dict | None = None
    quality_summaries: QualitySummaryMap | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )


class RefinementPageItem(BaseSchema):
    """Lean Refinement projection for paginated lists (FR4)."""

    id: str
    ideation_id: str
    board_id: str
    title: str
    description: str | None = None
    status: RefinementStatus
    edition: int = Field(1, ge=1)
    version: int
    assignee_id: str | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None = None
    archived: bool = False
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    quality_summaries: QualitySummaryMap | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )


class BoardRefinementPageItem(RefinementPageItem):
    """Board-wide refinement row with its parent title projected in SQL."""

    ideation_title: str


class SpecPageItem(BaseSchema):
    """Lean Spec projection for paginated lists (FR4)."""

    id: str
    board_id: str
    ideation_id: str | None = None
    refinement_id: str | None = None
    title: str
    description: str | None = None
    status: SpecStatus
    edition: int = Field(
        1,
        ge=1,
        description="Human-facing Spec edition; advances only when re-entering draft.",
    )
    version: int = Field(
        ...,
        description="Technical revision used for concurrency and currentness.",
    )
    assignee_id: str | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None = None
    archived: bool = False
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    quality_summaries: QualitySummaryMap | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )


class SprintPageItem(BaseSchema):
    """Lean Sprint projection for paginated lists (FR4)."""

    id: str
    spec_id: str
    board_id: str
    title: str
    description: str | None = None
    objective: str | None = None
    expected_outcome: str | None = None
    status: SprintStatus
    created_by: str
    created_at: datetime
    updated_at: datetime
    archived: bool = False
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )


class StorySummary(BaseSchema):
    """Schema for Story list responses."""

    id: str
    board_id: str
    topic_id: str
    title: str
    description: str
    actor: str | None
    goal: str | None
    benefit: str | None
    labels: list[str] | None
    status: StoryStatus
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    archived: bool = False
    pre_archive_status: str | None = None
    screen_mockups: list[ScreenMockup] | None = None
    ideation_links: list[StoryIdeationLinkResponse] = []


class StoryResponse(StorySummary):
    """Full Story response."""

    topic: TopicResponse | None = None


class StoryConversionResponse(BaseModel):
    """Response for Story conversion/linking."""

    success: bool
    ideation: dict[str, Any]
    links: list[StoryIdeationLinkResponse]
    propagated_mockups: int = 0


# ============================================================================
# Business Rule & API Contract Schemas
# ============================================================================


class BusinessRule(BaseModel):
    """A business rule that governs system behavior."""

    id: str
    title: str
    rule: str
    when: str
    then: str
    linked_requirements: list[str] | None = None  # canonical FR ids
    linked_task_ids: list[str] | None = None  # Card IDs linked to this rule
    status: Literal["active", "superseded", "revoked"] = "active"
    notes: str | None = None


# Real HTTP verbs accepted when contract_type == "http" (RFC 7231 + PATCH).
_HTTP_METHODS = frozenset(
    {"GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"}
)
# Legacy non-HTTP method tokens that historically encoded the contract kind in
# the `method` slot. They migrate to the contract_type discriminator on read so
# the http verb enum can stay verb-only.
_LEGACY_METHOD_TO_CONTRACT_TYPE = {
    "TOOL": "in_process",
    "COMPONENT": "in_process",
    "EVENT": "event",
}


class ApiContract(BaseModel):
    """An API contract describing an endpoint or interaction.

    The ``contract_type`` discriminator (default ``"http"``) lets a contract
    model a non-HTTP interaction (in-process call, gRPC, event) without inventing
    a fake HTTP method/path. For ``contract_type == "http"`` the ``method`` is
    constrained to a real HTTP verb and ``path`` is required — but that
    strictness is enforced only on WRITE (the four api-contract entry points pass
    ``context={"on_write": True}``). Read-back/deserialization stays tolerant so a
    pre-existing stored contract with an invalid method (e.g. "CALL") still loads
    and ``list``/``get`` never crash. JSON-field shapes are asymmetric by design:
    ``response_errors`` is a LIST while ``request_body``/``response_success`` are
    OBJECTs.
    """

    id: str
    contract_type: Literal["http", "in_process", "grpc", "event"] = "http"
    method: str | None = (
        None  # HTTP verb when contract_type=="http"; optional otherwise
    )
    path: str | None = None  # required for http; optional for non-http
    description: str = ""
    request_body: dict[str, Any] | None = None  # OBJECT
    response_success: dict[str, Any] | None = None  # OBJECT
    response_errors: list[dict[str, Any]] | None = None  # LIST (asymmetry, by design)
    linked_requirements: list[str] | None = None
    linked_rules: list[str] | None = None
    linked_task_ids: list[str] | None = None  # Card IDs linked to this contract
    status: Literal["active", "superseded", "revoked", "not_applicable"] = "active"
    notes: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _infer_contract_type_from_legacy_method(cls, data: Any) -> Any:
        """Migrate a legacy non-HTTP method token to the contract_type discriminator.

        Runs on every construction (read + write) and is idempotent. Fires only
        when ``contract_type`` is absent from the input, so an explicit value
        always wins; the legacy ``method`` value is preserved (being non-http it
        escapes the verb enum).
        """
        if isinstance(data, dict) and not data.get("contract_type"):
            method = data.get("method")
            if isinstance(method, str):
                inferred = _LEGACY_METHOD_TO_CONTRACT_TYPE.get(method.strip().upper())
                if inferred is not None:
                    data["contract_type"] = inferred
        return data

    @model_validator(mode="after")
    def _validate_http_shape(self, info: ValidationInfo) -> "ApiContract":
        """Enforce real-verb method + required path for http contracts ON WRITE only.

        Gated on the ``on_write`` validation-context flag (the four api-contract
        write entry points pass it). Read-back/deserialization constructs without
        the flag and stays tolerant of pre-existing invalid contracts so list/get
        never crash; a legacy-invalid contract is corrected on its next write.
        """
        if self.contract_type == "http" and (info.context or {}).get("on_write"):
            if not self.method:
                raise ValueError(
                    f"contract_type='http' requires a method (one of {sorted(_HTTP_METHODS)})"
                )
            if self.method.strip().upper() not in _HTTP_METHODS:
                raise ValueError(
                    f"invalid http method {self.method!r}; expected one of {sorted(_HTTP_METHODS)}"
                )
            if not self.path:
                raise ValueError("contract_type='http' requires a path")
        return self

    @model_validator(mode="after")
    def _require_na_justification(self) -> "ApiContract":
        if self.status == "not_applicable" and not (self.notes or "").strip():
            raise ValueError(
                "status='not_applicable' requires a justification in notes"
            )
        return self


IntegrationRequirementStatus = Literal[
    "active", "superseded", "revoked", "not_applicable"
]
IntegrationRequirementType = Literal[
    "api",
    "queue",
    "stored_procedure",
    "data_contract",
    "event",
    "file",
    "external_service",
    "mcp_tool",
    "other",
]


class IntegrationRequirement(BaseModel):
    """An integration requirement for APIs, queues, SPs, MCP tools, events, or data contracts."""

    id: str
    title: str
    integration_type: IntegrationRequirementType = "api"
    description: str = ""
    provider: str | None = None
    consumer: str | None = None
    contract_ref: str | None = None
    endpoint: str | None = None
    method: str | None = None
    data_contract: dict[str, Any] | None = None
    linked_requirements: list[str] | None = None  # canonical FR/TR ids
    linked_api_contracts: list[str] | None = None  # ApiContract IDs
    linked_task_ids: list[str] | None = None  # Card IDs linked to this IR
    status: IntegrationRequirementStatus = "active"
    notes: str | None = None

    @model_validator(mode="after")
    def _require_na_justification(self) -> "IntegrationRequirement":
        if self.status == "not_applicable" and not (self.notes or "").strip():
            raise ValueError(
                "status='not_applicable' requires a justification in notes"
            )
        return self


ObservabilityRequirementStatus = Literal[
    "active", "superseded", "revoked", "not_applicable"
]
ObservabilitySignalType = Literal[
    "metric",
    "log",
    "trace",
    "dashboard",
    "alert",
    "slo",
    "other",
]


class ObservabilityRequirement(BaseModel):
    """An observability requirement for dashboards, metrics, alerts, and thresholds."""

    id: str
    title: str
    signal_type: ObservabilitySignalType = "metric"
    description: str = ""
    target: str | None = None
    metric_name: str | None = None
    threshold: str | None = None
    severity: str | None = None
    owner: str | None = None
    linked_requirements: list[str] | None = None  # canonical FR/TR ids
    linked_integration_requirements: list[str] | None = (
        None  # IntegrationRequirement IDs
    )
    linked_task_ids: list[str] | None = None  # Card IDs linked to this OR
    status: ObservabilityRequirementStatus = "active"
    notes: str | None = None

    @model_validator(mode="after")
    def _require_na_justification(self) -> "ObservabilityRequirement":
        if self.status == "not_applicable" and not (self.notes or "").strip():
            raise ValueError(
                "status='not_applicable' requires a justification in notes"
            )
        return self


DecisionStatus = Literal["active", "superseded", "revoked"]


class Decision(BaseModel):
    """A decision formalized on a spec — causal/contextual choice.

    Different from BusinessRule (which is prescriptive, "system DEVE do X"):
    a Decision records *why* a choice was made, with alternatives and
    supersedence. Formalized so the spec carries full traceability and the
    validation gate can optionally require linked tasks.
    """

    id: str  # "dec_" + 8 hex
    title: str
    rationale: str  # why this choice was made
    context: str | None = None  # when/where it applies
    alternatives_considered: list[str] | None = None
    supersedes_decision_id: str | None = None  # id of a Decision on the same spec
    linked_requirements: list[str] | None = None  # canonical FR/TR ids
    linked_task_ids: list[str] | None = None
    status: DecisionStatus = "active"
    notes: str | None = None


# ============================================================================
# Architecture Design Schemas
# ============================================================================


ArchitectureParentType = Literal["ideation", "refinement", "spec", "card"]
ArchitectureDiagramType = Literal[
    "context",
    "container",
    "component",
    "sequence",
    "deployment",
    "data_flow",
    "other",
]
ArchitectureDiagramFormat = Literal[
    "excalidraw_json",
    "mermaid",
    "svg",
    "plantuml",
    "c4",
    "raw",
]


class ArchitectureEntity(BaseModel):
    """Structured architecture entity description."""

    id: str | None = None
    name: str = Field(..., min_length=1, max_length=255)
    entity_type: str | None = None
    responsibility: str | None = None
    boundaries: str | None = None
    technologies: list[str] = Field(default_factory=list)
    relationships: list[str] = Field(default_factory=list)
    notes: str | None = None


class ArchitectureInterface(BaseModel):
    """Structured architecture interface or contract boundary."""

    id: str | None = None
    name: str = Field(..., min_length=1, max_length=255)
    endpoint: str | None = None
    description: str | None = None
    participants: list[str] = Field(default_factory=list)
    direction: str | None = None
    protocol: str | None = None
    contract_type: str | None = None
    request_schema: dict[str, Any] | None = None
    response_schema: dict[str, Any] | None = None
    event_schema: dict[str, Any] | None = None
    error_contract: dict[str, Any] | list[dict[str, Any]] | str | None = None
    schema_ref: str | None = None
    notes: str | None = None


class ArchitectureDiagramPayloadRef(BaseModel):
    """Reference to a separately stored diagram payload."""

    adapter_payload_ref: str | None = None
    content_hash: str | None = None
    size_bytes: int | None = None
    storage_backend: str | None = None
    storage_key: str | None = None


class ArchitectureDiagram(BaseModel):
    """Diagram metadata embedded in the architecture envelope."""

    id: str | None = None
    title: str = Field(..., min_length=1, max_length=255)
    diagram_type: ArchitectureDiagramType = "other"
    format: ArchitectureDiagramFormat = "excalidraw_json"
    is_conceptual: bool = False
    connectivity_justifications: dict[str, str] | None = None
    adapter_payload_ref: str | None = None
    adapter_payload: dict[str, Any] | list[Any] | str | None = None
    description: str | None = None
    order_index: int = 0
    content_hash: str | None = None
    preview_ref: str | None = None
    render_metadata: dict[str, Any] | None = None
    size_bytes: int | None = None
    source_diagram_id: str | None = None
    source_payload_ref: str | None = None


class ArchitectureDesignBase(BaseSchema):
    """Shared architecture design fields."""

    title: str = Field(..., min_length=1, max_length=500)
    global_description: str = Field(..., min_length=1)
    entities: list[ArchitectureEntity] = Field(default_factory=list)
    interfaces: list[ArchitectureInterface] = Field(default_factory=list)
    diagrams: list[ArchitectureDiagram] = Field(default_factory=list)
    source_ref: str | None = None
    source_version: int | None = None
    source_design_id: str | None = None
    stale: bool = False
    breaking_change_flag: bool = False
    requires_arch_review: bool = False

    @field_validator("global_description")
    @classmethod
    def _description_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("global_description cannot be blank")
        return value


class ArchitectureWarningAcknowledgementRequest(BaseModel):
    """Explicit authoring acknowledgement for warning-bearing architecture saves."""

    accepted: bool = False
    warning_keys: list[str] = Field(default_factory=list)
    statement: str | None = None


class ArchitectureDesignCreate(ArchitectureDesignBase):
    """Request body for creating an Architecture Design on a parent."""

    architecture_warning_acknowledgement: (
        ArchitectureWarningAcknowledgementRequest | None
    ) = None


class ArchitectureDesignUpdate(BaseModel):
    """Patch body for Architecture Design updates."""

    title: str | None = Field(None, min_length=1, max_length=500)
    global_description: str | None = Field(None, min_length=1)
    entities: list[ArchitectureEntity] | None = None
    interfaces: list[ArchitectureInterface] | None = None
    diagrams: list[ArchitectureDiagram] | None = None
    stale: bool | None = None
    breaking_change_flag: bool | None = None
    requires_arch_review: bool | None = None
    source_ref: str | None = None
    source_version: int | None = None
    source_design_id: str | None = None
    change_summary: str | None = None
    architecture_warning_acknowledgement: (
        ArchitectureWarningAcknowledgementRequest | None
    ) = None

    @field_validator("global_description")
    @classmethod
    def _optional_description_not_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("global_description cannot be blank")
        return value


class ArchitectureDesignSummary(BaseSchema):
    """Lightweight architecture design summary without heavy diagram payloads."""

    id: str
    board_id: str
    parent_type: ArchitectureParentType
    parent_id: str
    title: str
    version: int
    source_ref: str | None = None
    source_design_id: str | None = None
    source_version: int | None = None
    stale: bool = False
    breaking_change_flag: bool = False
    requires_arch_review: bool = False
    diagrams_count: int = 0
    adapter_payload_refs: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class ArchitectureDesignResponse(ArchitectureDesignBase):
    """Full Architecture Design response."""

    id: str
    board_id: str
    parent_type: ArchitectureParentType
    parent_id: str
    version: int
    created_by: str
    created_at: datetime
    updated_at: datetime


class ArchitectureDiagramPayloadResponse(BaseSchema):
    """Response for a loaded diagram payload."""

    design_id: str
    diagram_id: str
    format: ArchitectureDiagramFormat
    content_hash: str
    size_bytes: int
    payload: dict[str, Any] | list[Any] | str | None


class ArchitectureDiffResponse(BaseModel):
    """Structural diff between two architecture design versions."""

    design_id: str
    from_version: int
    to_version: int
    changed_fields: list[str] = Field(default_factory=list)
    semantic_changes: list[dict[str, Any]] = Field(default_factory=list)
    layout_changes: list[dict[str, Any]] = Field(default_factory=list)
    breaking_change_flag: bool = False
    requires_arch_review: bool = False


# ============================================================================
# Ideation Schemas
# ============================================================================


def _validate_ideation_complexity(value: str | None) -> str | None:
    """Valida complexity contra o enum real (IdeationComplexity).

    Doc-drift fix (2026-06-10): a description anunciava low/medium/high/
    very_high, mas o enum sempre foi small/medium/large — e um valor
    inválido só explodia como ValueError 500 dentro do service. Validar no
    schema devolve 422 com a lista correta.
    """
    if value is None:
        return value
    allowed = tuple(c.value for c in IdeationComplexity)
    if value not in allowed:
        raise ValueError(f"complexity must be one of: {', '.join(allowed)}")
    return value


class IdeationCreate(BaseModel):
    """Schema for creating an ideation."""

    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Titulo da ideacao (1-500 chars).",
    )
    description: str | None = Field(
        None, description="Descricao geral da ideia ou oportunidade."
    )
    problem_statement: str | None = Field(
        None, description="Enunciado do problema que a ideacao pretende resolver."
    )
    proposed_approach: str | None = Field(
        None, description="Abordagem proposta para solucionar o problema."
    )
    scope_assessment: dict | None = Field(
        None,
        description="Avaliacao do escopo em formato livre (impacto, esforco, etc).",
    )
    complexity: str | None = Field(
        None,
        description="Complexidade estimada: small, medium, large (enum IdeationComplexity).",
    )
    assignee_id: str | None = Field(None, description="ID do responsavel pela ideacao.")
    labels: list[str] | None = Field(
        None, description="Labels de categorizacao da ideacao."
    )
    screen_mockups: list[ScreenMockup] | None = Field(
        None, description="Mockups de tela associados a ideacao."
    )

    @field_validator("complexity")
    @classmethod
    def _check_complexity(cls, value: str | None) -> str | None:
        return _validate_ideation_complexity(value)


class IdeationUpdate(BaseModel):
    """Schema for updating an ideation."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=500,
        description="Novo titulo da ideacao (opcional).",
    )
    description: str | None = Field(
        None, description="Nova descricao geral da ideia (opcional)."
    )
    problem_statement: str | None = Field(
        None, description="Novo enunciado do problema (opcional)."
    )
    proposed_approach: str | None = Field(
        None, description="Nova abordagem proposta (opcional)."
    )
    scope_assessment: dict | None = Field(
        None, description="Nova avaliacao de escopo em formato livre (opcional)."
    )
    complexity: str | None = Field(
        None,
        description="Nova complexidade estimada: small, medium, large (enum IdeationComplexity, opcional).",
    )
    assignee_id: str | None = Field(
        None, description="Novo ID do responsavel pela ideacao (opcional)."
    )
    labels: list[str] | None = Field(
        None, description="Novas labels de categorizacao (opcional)."
    )
    screen_mockups: list[ScreenMockup] | None = Field(
        None, description="Novos mockups de tela (opcional)."
    )

    @field_validator("complexity")
    @classmethod
    def _check_complexity(cls, value: str | None) -> str | None:
        return _validate_ideation_complexity(value)


class IdeationMove(BaseModel):
    """Schema for changing ideation status."""

    status: IdeationStatus
    cancellation_reason: str | None = Field(
        None,
        description="Justificativa do cancelamento. Obrigatoria quando status='cancelled'; ignorada nos demais.",
    )


class IdeationAmbiguityGateSkipUpdate(BaseModel):
    """Dedicated payload for the per-ideation Max ambiguity gate skip write path.

    Spec 2485780b (TR5/FR5): this is the ONLY field this endpoint accepts —
    extra='forbid' guarantees the path cannot be used to smuggle unrelated
    edits past the generic update_ideation draft-only guard. The write works
    while the ideation is in evaluating status.
    """

    model_config = ConfigDict(extra="forbid")

    skip_ambiguity_gate: bool
    reason: str = Field(..., min_length=1, max_length=2000)
    expected_ideation_version: int = Field(..., ge=1)
    expected_ideation_edition: int = Field(..., ge=1)

    @field_validator("reason")
    @classmethod
    def _validate_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("reason must not be blank")
        return normalized


class IdeationSummary(BaseSchema):
    """Schema for ideation summary."""

    # Evaluation scores {domains, ambiguity, dependencies} (1-5 each), present after
    # evaluate_ideation runs — surfaced on the list card as score badges. Rides
    # from_attributes off the ORM column (no service change needed).
    scope_assessment: dict | None = None
    # Count of unanswered Q&A (answered_at IS NULL) — drives the "open Q&A" badge.
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    # Count of non-archived, non-cancelled child refinements — drives the
    # "No refinement" derivation-pending badge.
    active_refinement_count: int = 0
    # Count of non-archived, non-cancelled direct specs (no refinement) — drives
    # the "No spec" derivation-pending badge for done small ideations.
    active_spec_count: int = 0
    id: str
    board_id: str
    title: str
    description: str | None
    problem_statement: str | None
    complexity: IdeationComplexity | None
    status: IdeationStatus
    edition: int = Field(1, ge=1)
    version: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    architecture_designs: list[ArchitectureDesignSummary] = []
    archived: bool = False
    pre_archive_status: str | None = None
    # Per-ideation opt-out of the board Max ambiguity gate (spec 2485780b).
    skip_ambiguity_gate: bool = False
    skip_ambiguity_gate_edition: int | None = Field(default=None, ge=1)


# ============================================================================
# Ideation Snapshot Schemas
# ============================================================================


class IdeationSnapshotResponse(BaseSchema):
    """Schema for an ideation snapshot — immutable version."""

    id: str
    ideation_id: str
    version: int
    title: str
    description: str | None
    problem_statement: str | None
    proposed_approach: str | None
    scope_assessment: dict | None
    complexity: str | None
    labels: list[str] | None
    qa_snapshot: list[dict] | None
    created_by: str
    created_at: datetime


class IdeationSnapshotSummary(BaseSchema):
    """Lightweight snapshot summary for listing."""

    id: str
    version: int
    title: str
    complexity: str | None
    created_by: str
    created_at: datetime


# ============================================================================
# Ideation History Schemas
# ============================================================================


class IdeationHistoryChange(BaseModel):
    """A single field-level change."""

    field: str
    old: Any = None
    new: Any = None


class IdeationHistoryResponse(BaseSchema):
    """Schema for an ideation history entry."""

    id: str
    ideation_id: str
    action: str
    actor_type: str
    actor_id: str
    actor_name: str
    changes: list[IdeationHistoryChange] | None = None
    summary: str | None = None
    version: int | None = None
    created_at: datetime


# ============================================================================
# Ideation Q&A Schemas
# ============================================================================


class IdeationQAChoiceOption(BaseModel):
    """A single option in an ideation Q&A choice question."""

    id: str
    label: str
    recommended: bool = False
    tradeoff: str | None = None


class IdeationQACreate(BaseModel):
    """Schema for creating a Q&A item on an ideation."""

    question: str = Field(..., min_length=1)
    question_type: str = "text"
    choices: list[IdeationQAChoiceOption] | None = None
    allow_free_text: bool = False


class IdeationQAAnswer(BaseModel):
    """Schema for answering an ideation Q&A item."""

    answer: str | None = None
    selected: list[str] | None = None


class IdeationQAResponse(BaseSchema):
    """Schema for ideation Q&A response."""

    id: str
    ideation_id: str
    question: str
    question_type: str = "text"
    choices: list[IdeationQAChoiceOption] | None = None
    allow_free_text: bool = False
    answer: str | None
    selected: list[str] | None = None
    asked_by: str
    answered_by: str | None
    created_at: datetime
    answered_at: datetime | None


# ============================================================================
# Ideation Knowledge Base Schemas
# ============================================================================


class IdeationKnowledgeCreate(BaseModel):
    """Schema for creating an ideation knowledge base item."""

    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Titulo do KB item da ideacao (1-500 chars).",
    )
    description: str | None = Field(
        None, description="Descricao resumida do KB item da ideacao."
    )
    content: str = Field(
        ..., min_length=1, description="Conteudo do KB item (markdown por padrao)."
    )
    mime_type: str = Field(
        "text/markdown",
        description="MIME type do conteudo: 'text/markdown', 'text/plain', etc.",
    )
    governance_metadata: Any | None = Field(
        None,
        description=(
            "Envelope opcional e versionado de governanca semantica. "
            "A validacao canonica ocorre no servico de aplicacao."
        ),
    )


class IdeationKnowledgeUpdate(BaseModel):
    """Schema for updating an ideation knowledge base item."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=500,
        description="Novo titulo do KB item da ideacao (opcional).",
    )
    description: str | None = Field(
        None, description="Nova descricao resumida (opcional)."
    )
    content: str | None = Field(
        None, min_length=1, description="Novo conteudo do KB item (opcional)."
    )
    mime_type: str | None = Field(
        None, description="Novo MIME type do conteudo (opcional)."
    )
    governance_metadata: Any | None = Field(
        None,
        description=(
            "Novo envelope de governanca; omitido preserva o valor atual e "
            "null remove o envelope."
        ),
    )


class IdeationKnowledgeResponse(KnowledgeGovernanceResponseSchema):
    """Full ideation knowledge base item response."""

    id: str
    ideation_id: str
    title: str
    description: str | None
    content: str
    mime_type: str
    source_type: str | None = None
    source_id: str | None = None
    source_title: str | None = None
    source_version: int | None = None
    source_kb_id: str | None = None
    root_source_kb_id: str | None = None
    immediate_parent_kb_id: str | None = None
    content_hash: str | None = None
    governance_metadata: Any | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime


class IdeationKnowledgeSummary(KnowledgeGovernanceResponseSchema):
    """Lightweight ideation KB summary (without content)."""

    id: str
    ideation_id: str
    title: str
    description: str | None
    mime_type: str
    source_type: str | None = None
    source_id: str | None = None
    source_title: str | None = None
    source_version: int | None = None
    source_kb_id: str | None = None
    root_source_kb_id: str | None = None
    immediate_parent_kb_id: str | None = None
    content_hash: str | None = None
    governance_metadata: Any | None = None
    created_at: datetime


# ============================================================================
# Refinement Schemas
# ============================================================================


def _require_nonempty_in_scope(value: list[str] | None) -> list[str] | None:
    """Reject in_scope when it is provided but has no usable entry.
    A refinement without at least one non-whitespace in-scope item is
    semantically empty and would let downstream tools (derive_spec,
    get_refinement_context) work on a stub. None is allowed only on update
    as the "no change" signal — callers that require a value must check
    before dispatch.
    """
    if value is None:
        return None
    cleaned = [s for s in value if isinstance(s, str) and s.strip()]
    if not cleaned:
        raise ValueError(
            "in_scope must contain at least one non-empty item",
        )
    return value


class RefinementCreate(BaseModel):
    """Schema for creating a refinement."""

    ideation_id: str = Field(
        ..., description="ID da ideacao pai a qual este refinement pertence."
    )
    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Titulo do refinement (1-500 chars).",
    )
    description: str | None = Field(None, description="Descricao geral do refinement.")
    in_scope: list[str] | None = Field(
        None,
        description="Itens dentro do escopo deste refinement (pelo menos 1 se fornecido).",
    )
    out_of_scope: list[str] | None = Field(
        None, description="Itens explicitamente fora do escopo deste refinement."
    )
    analysis: str | None = Field(
        None, description="Analise tecnica e de negocio do refinement."
    )
    decisions: list[str] | None = Field(
        None, description="Decisoes registradas durante o refinamento."
    )
    delivery_context: DeliveryContext = Field(
        ...,
        description=(
            "Contexto de entrega explicitamente classificado: brownfield, "
            "greenfield ou hybrid. A leitura de registros legados permanece "
            "nullable, mas toda nova autoria deve classificar o contexto."
        ),
    )
    assignee_id: str | None = Field(
        None, description="ID do responsavel pelo refinement."
    )
    labels: list[str] | None = Field(
        None, description="Labels de categorizacao do refinement."
    )
    screen_mockups: list[ScreenMockup] | None = Field(
        None, description="Mockups de tela associados ao refinement."
    )
    # Artifact propagation filters (optional — None = propagate all from parent)
    mockup_ids: list[str] | None = Field(
        None,
        description="IDs dos mockups a propagar da ideacao (None = propagar todos).",
    )
    kb_ids: list[str] | None = Field(
        None,
        description="IDs dos KB items a propagar da ideacao (None = propagar todos).",
    )
    architecture_design_ids: list[str] | None = Field(
        None,
        description="IDs dos architecture designs a propagar (None = propagar todos).",
    )
    architecture_propagation_mode: str = Field(
        "copy",
        description=(
            "Modo de propagacao de arquitetura. Valores aceitos: 'copy', "
            "'derive', 'reference_only' ou 'none'. 'snapshot' nao e um modo; "
            "'copy'/'derive' copiam snapshots, enquanto 'reference_only'/'none' "
            "mantem apenas a ligacao com o pai."
        ),
    )

    @field_validator("in_scope")
    @classmethod
    def _validate_in_scope_on_create(cls, v: list[str] | None) -> list[str] | None:
        # On create, in_scope is allowed to stay None (the caller may fill it
        # in later via update before moving the refinement past draft). But
        # if the caller did send a list, it must have at least one usable
        # entry — an empty or whitespace-only list is always a mistake.
        if v is None:
            return None
        return _require_nonempty_in_scope(v)


class RefinementUpdate(BaseModel):
    """Schema for updating a refinement."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=500,
        description="Novo titulo do refinement (opcional).",
    )
    description: str | None = Field(
        None, description="Nova descricao geral do refinement (opcional)."
    )
    in_scope: list[str] | None = Field(
        None,
        description="Nova lista de itens em escopo (None = sem mudanca; lista vazia = erro).",
    )
    out_of_scope: list[str] | None = Field(
        None, description="Nova lista de itens fora de escopo (opcional)."
    )
    analysis: str | None = Field(
        None, description="Nova analise tecnica e de negocio (opcional)."
    )
    decisions: list[str] | None = Field(
        None, description="Novas decisoes do refinamento (opcional)."
    )
    delivery_context: DeliveryContext | None = Field(
        None,
        description=(
            "Novo contexto de entrega. Omitido preserva o valor atual; null "
            "explicito nao remove um contexto ja classificado."
        ),
    )
    assignee_id: str | None = Field(
        None, description="Novo ID do responsavel pelo refinement (opcional)."
    )
    labels: list[str] | None = Field(
        None, description="Novas labels de categorizacao (opcional)."
    )
    screen_mockups: list[ScreenMockup] | None = Field(
        None, description="Novos mockups de tela (opcional)."
    )

    @field_validator("in_scope")
    @classmethod
    def _validate_in_scope_on_update(cls, v: list[str] | None) -> list[str] | None:
        # On update, None means "no change" and is allowed. A provided list
        # must follow the same non-empty rule as create.
        if v is None:
            return None
        return _require_nonempty_in_scope(v)


class RefinementMove(BaseModel):
    """Schema for changing refinement status."""

    status: RefinementStatus
    cancellation_reason: str | None = Field(
        None,
        description="Justificativa do cancelamento. Obrigatoria quando status='cancelled'; ignorada nos demais.",
    )


class RefinementAmbiguityGateSkipUpdate(BaseModel):
    """Human-only, version-fenced Refinement ambiguity override."""

    model_config = ConfigDict(extra="forbid")

    skip_ambiguity_gate: bool
    reason: str = Field(..., min_length=1, max_length=2000)
    expected_refinement_version: int = Field(..., ge=1)
    expected_refinement_edition: int = Field(..., ge=1)

    @field_validator("reason")
    @classmethod
    def _validate_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("reason must not be blank")
        return normalized


class RefinementAmbiguityGateSkipResponse(BaseModel):
    skipped: bool
    activity_id: str
    version: int
    edition: int = Field(..., ge=1)


class RefinementSummary(BaseSchema):
    """Schema for refinement summary."""

    # Count of unanswered Q&A (answered_at IS NULL) — drives the "open Q&A" badge.
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    # Count of non-archived, non-cancelled child specs — drives the
    # "Sem spec" derivation-pending badge.
    active_spec_count: int = 0
    id: str
    ideation_id: str
    board_id: str
    title: str
    description: str | None
    status: RefinementStatus
    edition: int = Field(1, ge=1)
    version: int
    delivery_context: DeliveryContext | None = None
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    archived: bool = False
    pre_archive_status: str | None = None
    skip_ambiguity_gate: bool = False
    skip_ambiguity_gate_edition: int | None = Field(default=None, ge=1)


# ============================================================================
# Refinement History Schemas
# ============================================================================


class RefinementHistoryChange(BaseModel):
    """A single field-level change."""

    field: str
    old: Any = None
    new: Any = None


class RefinementHistoryResponse(BaseSchema):
    """Schema for a refinement history entry."""

    id: str
    refinement_id: str
    action: str
    actor_type: str
    actor_id: str
    actor_name: str
    changes: list[RefinementHistoryChange] | None = None
    summary: str | None = None
    version: int | None = None
    created_at: datetime


# ============================================================================
# Refinement Q&A Schemas
# ============================================================================


class RefinementQAChoiceOption(BaseModel):
    """A single option in a refinement Q&A choice question."""

    id: str
    label: str
    recommended: bool = False
    tradeoff: str | None = None


class RefinementQACreate(BaseModel):
    """Schema for creating a Q&A item on a refinement."""

    question: str = Field(..., min_length=1)
    question_type: str = "text"
    choices: list[RefinementQAChoiceOption] | None = None
    allow_free_text: bool = False


class RefinementQAAnswer(BaseModel):
    """Schema for answering a refinement Q&A item."""

    answer: str | None = None
    selected: list[str] | None = None


class RefinementQAResponse(BaseSchema):
    """Schema for refinement Q&A response."""

    id: str
    refinement_id: str
    question: str
    question_type: str = "text"
    choices: list[RefinementQAChoiceOption] | None = None
    allow_free_text: bool = False
    answer: str | None
    selected: list[str] | None = None
    asked_by: str
    answered_by: str | None
    created_at: datetime
    answered_at: datetime | None


# ============================================================================
# Refinement Snapshot Schemas
# ============================================================================


class RefinementSnapshotResponse(BaseSchema):
    """Schema for a refinement snapshot — immutable version."""

    id: str
    refinement_id: str
    version: int
    delivery_context: DeliveryContext | None = None
    title: str
    description: str | None
    in_scope: list[str] | None
    out_of_scope: list[str] | None
    analysis: str | None
    decisions: list[str] | None
    labels: list[str] | None
    qa_snapshot: list[dict] | None
    code_evidence_manifest: list[dict[str, Any]] | None = None
    source_context_manifest: dict[str, Any] | None = None
    source_context_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    created_by: str
    created_at: datetime


class RefinementSnapshotSummary(BaseSchema):
    """Lightweight snapshot summary for listing."""

    id: str
    version: int
    delivery_context: DeliveryContext | None = None
    title: str
    created_by: str
    created_at: datetime


# ============================================================================
# Refinement Knowledge Base Schemas
# ============================================================================


class RefinementKnowledgeCreate(BaseModel):
    """Schema for creating a refinement knowledge base item."""

    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Titulo do KB item do refinement (1-500 chars).",
    )
    description: str | None = Field(
        None, description="Descricao resumida do KB item do refinement."
    )
    content: str = Field(
        ..., min_length=1, description="Conteudo do KB item (markdown por padrao)."
    )
    mime_type: str = Field(
        "text/markdown",
        description="MIME type do conteudo: 'text/markdown', 'text/plain', etc.",
    )
    governance_metadata: Any | None = Field(
        None,
        description=(
            "Envelope opcional e versionado de governanca semantica. "
            "A validacao canonica ocorre no servico de aplicacao."
        ),
    )


class RefinementKnowledgeUpdate(BaseModel):
    """Schema for updating a refinement knowledge base item."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=500,
        description="Novo titulo do KB item (opcional).",
    )
    description: str | None = Field(
        None, description="Nova descricao resumida (opcional)."
    )
    content: str | None = Field(
        None, min_length=1, description="Novo conteudo do KB item (opcional)."
    )
    mime_type: str | None = Field(
        None, description="Novo MIME type do conteudo (opcional)."
    )
    governance_metadata: Any | None = Field(
        None,
        description=(
            "Novo envelope de governanca; omitido preserva o valor atual e "
            "null remove o envelope."
        ),
    )


class RefinementKnowledgeResponse(KnowledgeGovernanceResponseSchema):
    """Full refinement knowledge base item response."""

    id: str
    refinement_id: str
    title: str
    description: str | None
    content: str
    mime_type: str
    source_type: str | None = None
    source_id: str | None = None
    source_title: str | None = None
    source_version: int | None = None
    source_kb_id: str | None = None
    root_source_kb_id: str | None = None
    immediate_parent_kb_id: str | None = None
    content_hash: str | None = None
    governance_metadata: Any | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime


class RefinementKnowledgeSummary(KnowledgeGovernanceResponseSchema):
    """Lightweight refinement KB summary (without content)."""

    id: str
    refinement_id: str
    title: str
    description: str | None
    mime_type: str
    source_type: str | None = None
    source_id: str | None = None
    source_title: str | None = None
    source_version: int | None = None
    source_kb_id: str | None = None
    root_source_kb_id: str | None = None
    immediate_parent_kb_id: str | None = None
    content_hash: str | None = None
    governance_metadata: Any | None = None
    created_at: datetime


# ============================================================================
# Spec Schemas
# ============================================================================


class SpecCreate(BaseModel):
    """Schema for creating a spec."""

    title: str = Field(
        ..., min_length=1, max_length=500, description="Titulo da spec (1-500 chars)."
    )
    description: str | None = Field(
        None, description="Descricao resumida do que a spec cobre."
    )
    context: str | None = Field(
        None, description="Contexto de negocio e tecnico para a spec."
    )
    functional_requirements: list[str | dict] | None = Field(
        None,
        description="Lista de requisitos funcionais (FRs) em texto livre ou objetos estruturados {id, text, ...}.",
    )
    technical_requirements: list[str | dict] | None = Field(
        None,
        description="Requisitos tecnicos: string legada ou dict {id, text, linked_task_ids}.",
    )
    acceptance_criteria: list[str | dict] | None = Field(
        None,
        description="Criterios de aceite em texto livre ou objetos estruturados {id, text, ...}.",
    )
    test_scenarios: list[TestScenarioWrite] | None = Field(
        None, description="Cenarios de teste vinculados a spec."
    )
    screen_mockups: list[ScreenMockup] | None = Field(
        None, description="Mockups de tela associados a spec."
    )
    business_rules: list[BusinessRule] | None = Field(
        None, description="Regras de negocio que governam o comportamento do sistema."
    )
    api_contracts: list[ApiContract] | None = Field(
        None, description="Contratos de API (endpoints, metodos, schemas)."
    )
    integration_requirements: list[IntegrationRequirement] | None = Field(
        None, description="Requisitos de integracao com sistemas externos."
    )
    observability_requirements: list[ObservabilityRequirement] | None = Field(
        None, description="Requisitos de observabilidade (metricas, logs, alertas)."
    )
    decisions: list[Decision] | None = Field(
        None, description="Decisoes de design formalizadas nesta spec."
    )
    status: SpecStatus = Field(SpecStatus.DRAFT, description="Status inicial da spec.")
    assignee_id: str | None = Field(None, description="ID do responsavel pela spec.")
    labels: list[str] | None = Field(
        None, description="Labels de categorizacao da spec."
    )
    ideation_id: str | None = Field(
        None, description="ID da ideacao de origem desta spec."
    )
    refinement_id: str | None = Field(
        None, description="ID do refinement de origem desta spec."
    )
    delivery_context: DeliveryContext | None = Field(
        None,
        description=(
            "Contexto de entrega explicitamente classificado. Omitido somente "
            "quando um Refinement contextual fornece o valor herdado."
        ),
    )
    delivery_context_override_reason: str | None = Field(
        None,
        min_length=1,
        max_length=2000,
        description=(
            "Justificativa obrigatoria quando o contexto da Spec divergir do "
            "snapshot do Refinement."
        ),
    )


class SpecUpdate(BaseModel):
    """Schema for updating a spec."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=500,
        description="Novo titulo da spec (opcional).",
    )
    description: str | None = Field(
        None, description="Nova descricao resumida da spec (opcional)."
    )
    context: str | None = Field(
        None, description="Novo contexto de negocio e tecnico da spec (opcional)."
    )
    functional_requirements: list[str | dict] | None = Field(
        None, description="Nova lista de requisitos funcionais (substitui a existente)."
    )
    technical_requirements: list[str | dict] | None = Field(
        None,
        description="Novos requisitos tecnicos: string legada ou dict {id, text, linked_task_ids}.",
    )
    acceptance_criteria: list[str | dict] | None = Field(
        None, description="Novos criterios de aceite (substitui a lista existente)."
    )
    test_scenarios: list[TestScenarioWrite] | None = Field(
        None, description="Novos cenarios de teste vinculados a spec."
    )
    screen_mockups: list[ScreenMockup] | None = Field(
        None, description="Novos mockups de tela associados a spec."
    )
    business_rules: list[BusinessRule] | None = Field(
        None, description="Novas regras de negocio (substitui a lista existente)."
    )
    api_contracts: list[ApiContract] | None = Field(
        None, description="Novos contratos de API (substitui a lista existente)."
    )
    integration_requirements: list[IntegrationRequirement] | None = Field(
        None,
        description="Novos requisitos de integracao (substitui a lista existente).",
    )
    observability_requirements: list[ObservabilityRequirement] | None = Field(
        None,
        description="Novos requisitos de observabilidade (substitui a lista existente).",
    )
    decisions: list[Decision] | None = Field(
        None,
        description="Novas decisoes de design formalizadas (substitui a lista existente).",
    )
    skip_test_coverage: bool | None = Field(
        None, description="Se True, o gate de cobertura de test scenarios e ignorado."
    )
    skip_rules_coverage: bool | None = Field(
        None, description="Se True, o gate de cobertura de business rules e ignorado."
    )
    skip_trs_coverage: bool | None = Field(
        None,
        description="Se True, o gate de cobertura de technical requirements e ignorado.",
    )
    skip_contract_coverage: bool | None = Field(
        None, description="Se True, o gate de cobertura de API contracts e ignorado."
    )
    skip_ir_coverage: bool | None = Field(
        None,
        description="Se True, o gate de cobertura de integration requirements e ignorado.",
    )
    skip_or_coverage: bool | None = Field(
        None,
        description="Se True, o gate de cobertura de observability requirements e ignorado.",
    )
    skip_decisions_coverage: bool | None = Field(
        None, description="Se True, o gate de cobertura de decisions e ignorado."
    )
    skip_code_evidence_coverage: bool | None = Field(
        None,
        description=(
            "Se True, o gate de cobertura da Code Evidence Matrix e ignorado."
        ),
    )
    require_task_validation: bool | None = Field(
        None,
        description="Override da spec para exigir Task Validation; None herda do board.",
    )
    validation_min_confidence: int | None = Field(
        None,
        ge=0,
        le=100,
        description="Override 0-100 de confianca minima; None herda do board.",
    )
    validation_min_completeness: int | None = Field(
        None,
        ge=0,
        le=100,
        description="Override 0-100 de completude minima; None herda do board.",
    )
    validation_max_drift: int | None = Field(
        None,
        ge=0,
        le=100,
        description="Override 0-100 de drift maximo; None herda do board.",
    )
    assignee_id: str | None = Field(
        None, description="Novo ID do responsavel pela spec."
    )
    labels: list[str] | None = Field(
        None, description="Novas labels de categorizacao da spec."
    )
    ideation_id: str | None = Field(
        None, description="Novo ID da ideacao de origem desta spec."
    )
    refinement_id: str | None = Field(
        None, description="Novo ID do refinement de origem desta spec."
    )
    delivery_context: DeliveryContext | None = Field(
        None,
        description=(
            "Valor efetivo do contexto da Spec. Quando divergir do valor "
            "herdado do snapshot, delivery_context_override_reason e obrigatorio."
        ),
    )
    delivery_context_override_reason: str | None = Field(
        None,
        min_length=1,
        max_length=2000,
        description=(
            "Justificativa humana para um contexto efetivo diferente do "
            "snapshot herdado. Envie null ao reconciliar com o valor herdado."
        ),
    )


class SpecMove(BaseModel):
    """Schema for changing spec status."""

    status: SpecStatus
    cancellation_reason: str | None = Field(
        None,
        description="Justificativa do cancelamento. Obrigatoria quando status='cancelled'; ignorada nos demais.",
    )


class SpecDependencyAdd(BaseModel):
    """Create an operational dependency from one Spec to another."""

    target_spec_id: str = Field(..., min_length=1)
    expected_spec_version: int = Field(..., ge=1)
    expected_spec_edition: int = Field(..., ge=1)
    idempotency_key: str = Field(..., min_length=1, max_length=255)


class SpecDependencyRemove(BaseModel):
    """Tombstone a dependency while preserving its audit lifecycle."""

    reason: str = Field(..., min_length=1, max_length=2000)
    expected_spec_version: int = Field(..., ge=1)
    expected_spec_edition: int = Field(..., ge=1)
    idempotency_key: str = Field(..., min_length=1, max_length=255)

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("removal reason must not be blank")
        return normalized


class SpecDependencyBlockerResponse(BaseSchema):
    """Blocking prerequisite projected in Spec lifecycle readiness."""

    dependency_id: str
    dependent_spec_id: str
    prerequisite_spec_id: str
    target_title: str
    target_status: SpecStatus
    target_edition: int = Field(..., ge=1)
    target_version: int = Field(..., ge=1)
    target_archived: bool = False


class SpecDependencyReadinessResponse(BaseSchema):
    """Current, dynamically evaluated prerequisite readiness for a Spec."""

    spec_id: str
    board_id: str
    can_start: bool
    ready: bool
    reason_code: Literal["spec_dependencies_incomplete"] | None = None
    current_edition: int = Field(..., ge=1)
    last_started_edition: int | None = Field(default=None, ge=1)
    active_dependency_count: int = Field(..., ge=0)
    unmet_count: int = Field(..., ge=0)
    blocking_count: int = Field(..., ge=0)
    archived_blocking_count: int = Field(..., ge=0)
    unfinished_blocking_count: int = Field(..., ge=0)
    blockers_truncated: bool
    current_edition_started: bool
    blockers: list[SpecDependencyBlockerResponse] = Field(default_factory=list)


class SpecSummary(BaseSchema):
    """Schema for spec summary (without nested cards)."""

    # Count of unanswered Q&A (answered_at IS NULL) — drives the "open Q&A" badge.
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    id: str
    board_id: str
    title: str
    description: str | None
    status: SpecStatus
    edition: int = Field(
        1,
        ge=1,
        description="Human-facing Spec edition; advances only when re-entering draft.",
    )
    last_started_edition: int | None = Field(
        None,
        ge=1,
        description="Most recent human lifecycle edition that began execution.",
    )
    version: int = Field(
        ...,
        description="Technical revision used for concurrency and currentness.",
    )
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    ideation_id: str | None = None
    refinement_id: str | None = None
    source_refinement_snapshot_id: str | None = None
    source_refinement_version: int | None = Field(default=None, ge=1)
    delivery_context: DeliveryContext | None = None
    delivery_context_provenance: (
        SpecDeliveryContextProvenance
        | DirectSpecDeliveryContextProvenance
        | None
    ) = None
    source_context_manifest: dict[str, Any] | None = None
    source_context_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    architecture_designs: list[ArchitectureDesignSummary] = []
    archived: bool = False
    pre_archive_status: str | None = None


class IdeationResponse(BaseSchema):
    """Schema for full ideation response."""

    id: str
    board_id: str
    title: str
    description: str | None
    problem_statement: str | None
    proposed_approach: str | None
    scope_assessment: dict | None
    complexity: IdeationComplexity | None
    screen_mockups: list[ScreenMockup] | None = None
    status: IdeationStatus
    edition: int = Field(1, ge=1)
    version: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    archived: bool = False
    pre_archive_status: str | None = None
    # Per-ideation opt-out of the board Max ambiguity gate (spec 2485780b).
    skip_ambiguity_gate: bool = False
    skip_ambiguity_gate_edition: int | None = Field(default=None, ge=1)
    # Cancellation justification (ITEM 17) — set only while status == cancelled.
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    cancelled_by: str | None = None
    refinements: list[RefinementSummary] = []
    stories: list[StorySummary] = []
    specs: list[SpecSummary] = []
    knowledge_bases: list[IdeationKnowledgeSummary] = []
    architecture_designs: list[ArchitectureDesignSummary] = []
    qa_items: list[IdeationQAResponse] = []


class RefinementResponse(BaseSchema):
    """Schema for full refinement response."""

    id: str
    ideation_id: str
    board_id: str
    title: str
    description: str | None
    in_scope: list[str] | None
    out_of_scope: list[str] | None
    analysis: str | None
    decisions: list[str] | None
    screen_mockups: list[ScreenMockup] | None = None
    status: RefinementStatus
    edition: int = Field(1, ge=1)
    version: int
    delivery_context: DeliveryContext | None = None
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    archived: bool = False
    pre_archive_status: str | None = None
    skip_ambiguity_gate: bool = False
    skip_ambiguity_gate_edition: int | None = Field(default=None, ge=1)
    # Cancellation justification (ITEM 17) — set only while status == cancelled.
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    cancelled_by: str | None = None
    specs: list[SpecSummary] = []
    knowledge_bases: list[RefinementKnowledgeSummary] = []
    architecture_designs: list[ArchitectureDesignSummary] = []
    qa_items: list[RefinementQAResponse] = []


# ============================================================================
# Spec History Schemas
# ============================================================================


class SpecHistoryChange(BaseModel):
    """A single field-level change."""

    field: str
    old: Any = None
    new: Any = None


class SpecHistoryResponse(BaseSchema):
    """Schema for a spec history entry."""

    id: str
    spec_id: str
    action: str
    actor_type: str
    actor_id: str
    actor_name: str
    changes: list[SpecHistoryChange] | None = None
    summary: str | None = None
    version: int | None = None
    created_at: datetime


# ============================================================================
# Spec Q&A Schemas
# ============================================================================


class SpecQAChoiceOption(BaseModel):
    """A single option in a spec Q&A choice question."""

    id: str
    label: str
    recommended: bool = False
    tradeoff: str | None = None


class SpecQACreate(BaseModel):
    """Schema for creating a Q&A item on a spec.

    For text questions, only ``question`` is needed.
    For choice questions, set ``question_type`` and provide ``choices``.
    """

    question: str = Field(..., min_length=1)
    question_type: str = "text"  # text | choice | multi_choice
    choices: list[SpecQAChoiceOption] | None = None
    allow_free_text: bool = False


class SpecQAAnswer(BaseModel):
    """Schema for answering a spec Q&A item.

    For text questions, provide ``answer``.
    For choice questions, provide ``selected`` (list of option IDs) and optionally ``answer`` as free text.
    """

    answer: str | None = None
    selected: list[str] | None = None


class SpecQAResponse(BaseSchema):
    """Schema for spec Q&A response."""

    id: str
    spec_id: str
    question: str
    question_type: str = "text"
    choices: list[SpecQAChoiceOption] | None = None
    allow_free_text: bool = False
    answer: str | None
    selected: list[str] | None = None
    asked_by: str
    answered_by: str | None
    created_at: datetime
    answered_at: datetime | None


# ============================================================================
# Spec Knowledge Base Schemas
# ============================================================================


class SpecKnowledgeCreate(BaseModel):
    """Schema for creating a knowledge base item."""

    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Titulo do item de knowledge base (1-500 chars).",
    )
    description: str | None = Field(
        None, description="Descricao resumida do conteudo do KB item."
    )
    content: str = Field(
        ..., min_length=1, description="Conteudo do KB item (markdown por padrao)."
    )
    mime_type: str = Field(
        "text/markdown",
        description="MIME type do conteudo: 'text/markdown', 'text/plain', etc.",
    )
    governance_metadata: Any | None = Field(
        None,
        description=(
            "Envelope opcional e versionado de governanca semantica. "
            "A validacao canonica ocorre no servico de aplicacao."
        ),
    )


class SpecKnowledgeUpdate(BaseModel):
    """Schema for updating a knowledge base item."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=500,
        description="Novo titulo do KB item (opcional).",
    )
    description: str | None = Field(
        None, description="Nova descricao resumida (opcional)."
    )
    content: str | None = Field(
        None, description="Novo conteudo do KB item (opcional)."
    )
    mime_type: str | None = Field(
        None, description="Novo MIME type do conteudo (opcional)."
    )
    governance_metadata: Any | None = Field(
        None,
        description=(
            "Novo envelope de governanca; omitido preserva o valor atual e "
            "null remove o envelope."
        ),
    )


class SpecKnowledgeResponse(KnowledgeGovernanceResponseSchema):
    """Full knowledge base item response."""

    id: str
    spec_id: str
    title: str
    description: str | None
    content: str
    mime_type: str
    source_type: str | None = None
    source_id: str | None = None
    source_title: str | None = None
    source_version: int | None = None
    source_kb_id: str | None = None
    root_source_kb_id: str | None = None
    immediate_parent_kb_id: str | None = None
    content_hash: str | None = None
    governance_metadata: Any | None = None
    created_by: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SpecKnowledgeSummary(KnowledgeGovernanceResponseSchema):
    """Lightweight KB summary (without content)."""

    id: str
    spec_id: str
    title: str
    description: str | None
    mime_type: str
    source_type: str | None = None
    source_id: str | None = None
    source_title: str | None = None
    source_version: int | None = None
    source_kb_id: str | None = None
    root_source_kb_id: str | None = None
    immediate_parent_kb_id: str | None = None
    content_hash: str | None = None
    governance_metadata: Any | None = None
    created_at: datetime | None = None


class CardSummaryForSpec(BaseSchema):
    """Minimal card summary used inside spec responses."""

    id: str
    title: str
    status: CardStatus
    priority: CardPriority
    assignee_id: str | None
    card_type: str = "normal"
    sprint_id: str | None = None


class SpecResponse(BaseSchema):
    """Schema for full spec response."""

    id: str
    board_id: str
    title: str
    description: str | None
    context: str | None
    functional_requirements: list[str | dict] | None
    technical_requirements: (
        list[str | dict] | None
    )  # str (legacy) or {id, text, linked_task_ids}
    acceptance_criteria: list[str | dict] | None
    test_scenarios: list[TestScenario] | None = None
    screen_mockups: list[ScreenMockup] | None = None
    business_rules: list[BusinessRule] | None = None
    api_contracts: list[ApiContract] | None = None
    integration_requirements: list[IntegrationRequirement] | None = None
    observability_requirements: list[ObservabilityRequirement] | None = None
    decisions: list[Decision] | None = None
    skip_test_coverage: bool = False
    skip_rules_coverage: bool = False
    skip_decisions_coverage: bool = False  # default False (ideação #10 Fase 1 parity)
    skip_trs_coverage: bool = False
    skip_contract_coverage: bool = False
    skip_ir_coverage: bool = False
    skip_or_coverage: bool = False
    skip_code_evidence_coverage: bool = False
    require_task_validation: bool | None = None
    validation_min_confidence: int | None = None
    validation_min_completeness: int | None = None
    validation_max_drift: int | None = None
    archived: bool = False
    pre_archive_status: str | None = None
    # Cancellation justification (ITEM 17) — set only while status == cancelled.
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    cancelled_by: str | None = None
    status: SpecStatus
    edition: int = Field(
        1,
        ge=1,
        description="Human-facing Spec edition; advances only when re-entering draft.",
    )
    last_started_edition: int | None = Field(
        None,
        ge=1,
        description="Most recent human lifecycle edition that began execution.",
    )
    dependency_readiness: SpecDependencyReadinessResponse | None = None
    version: int = Field(
        ...,
        description="Technical revision used for concurrency and currentness.",
    )
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    ideation_id: str | None = None
    refinement_id: str | None = None
    source_refinement_snapshot_id: str | None = None
    source_refinement_version: int | None = Field(default=None, ge=1)
    delivery_context: DeliveryContext | None = None
    delivery_context_provenance: (
        SpecDeliveryContextProvenance
        | DirectSpecDeliveryContextProvenance
        | None
    ) = None
    source_context_manifest: dict[str, Any] | None = None
    source_context_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    cards: list[CardSummaryForSpec] = []
    knowledge_bases: list[SpecKnowledgeSummary] = []
    architecture_designs: list[ArchitectureDesignSummary] = []
    qa_items: list[SpecQAResponse] = []


# Keep the legacy response model untouched while allowing the refinement
# derive route to declare its authoritative v2 receipt projection.
DeriveSpecResponse: TypeAlias = SpecResponse | DeriveSpecKnowledgeMutationResponse


# ============================================================================
# Card Schemas
# ============================================================================


CardInitialStatus: TypeAlias = Literal[
    CardStatus.NOT_STARTED,
    CardStatus.STARTED,
]


class CardCreate(BaseModel):
    """Schema for creating a card."""

    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Titulo conciso do card (1-500 chars).",
    )
    description: str | None = Field(
        None, description="Resumo do objetivo ou contexto do card."
    )
    details: str | None = Field(
        None, description="Descricao tecnica detalhada, markdown suportado."
    )
    status: CardInitialStatus = Field(
        CardStatus.NOT_STARTED,
        description=(
            "Status inicial do card no board: somente not_started ou started. "
            "Use move_card para avancar o ciclo de vida."
        ),
    )
    priority: CardPriority = Field(
        CardPriority.NONE,
        description="Prioridade do card: none, low, medium, high, very_high, critical.",
    )
    assignee_id: str | None = Field(
        None, description="ID do agente ou usuario responsavel pelo card."
    )
    due_date: datetime | None = Field(
        None, description="Data limite para conclusao do card (ISO 8601)."
    )
    labels: list[str] | None = Field(
        None, description="Tags de categorizacao para filtragem e busca."
    )
    spec_id: str | None = Field(
        None, description="ID da spec a qual este card esta vinculado."
    )
    sprint_id: str | None = Field(
        None, description="ID do sprint ao qual este card pertence."
    )
    test_scenario_ids: list[str] | None = Field(
        None,
        description=(
            "IDs dos test scenarios associados a este card. Para card_type='test', "
            "e obrigatorio e limitado por board.settings.max_scenarios_per_card "
            "(default 3; boards podem configurar 2 ou outro valor)."
        ),
    )
    functional_requirement_ids: list[str] | None = Field(
        None,
        description="FR IDs to backlink atomically during card creation.",
    )
    business_rule_ids: list[str] | None = Field(
        None,
        description="Business-rule IDs to backlink atomically during card creation.",
    )
    screen_mockups: list[ScreenMockup] | None = Field(
        None, description="Mockups de tela vinculados ao card."
    )
    # Card type: "normal", "test", or "bug".
    card_type: str = Field(
        "normal", description="Tipo do card: 'normal', 'test' ou 'bug'."
    )
    origin_task_id: str | None = Field(
        None,
        description="ID do card de origem (para cards de bug derivados de outro card).",
    )
    severity: str | None = Field(
        None,
        description="Severidade do bug: 'critical', 'major' ou 'minor' (apenas bug cards).",
    )
    expected_behavior: str | None = Field(
        None, description="Comportamento esperado antes do bug (apenas bug cards)."
    )
    observed_behavior: str | None = Field(
        None, description="Comportamento observado/incorreto (apenas bug cards)."
    )
    steps_to_reproduce: str | None = Field(
        None, description="Passos para reproduzir o bug (apenas bug cards)."
    )
    action_plan: str | None = Field(
        None, description="Plano de acao para correcao do bug (apenas bug cards)."
    )
    knowledge_propagation: KnowledgePropagationEnvelopeV2 | None = Field(
        None,
        description=(
            "Selective Knowledge propagation v2. Omitted keeps the complete "
            "legacy card-create behavior; when supplied this envelope is "
            "authoritative and v1 Knowledge snapshot writes are disabled."
        ),
    )


class CardUpdate(BaseModel):
    """Schema for updating a card."""

    title: str | None = Field(
        None,
        min_length=1,
        max_length=500,
        description="Novo titulo do card (opcional, vazio = sem mudanca).",
    )
    description: str | None = Field(
        None, description="Nova descricao do card (opcional)."
    )
    details: str | None = Field(
        None, description="Novos detalhes tecnicos do card (opcional)."
    )
    status: CardStatus | None = Field(
        None,
        description=(
            "Reservado para compatibilidade de leitura; update_card rejeita "
            "alteracoes de status. Use move_card para toda transicao."
        ),
    )
    priority: CardPriority | None = Field(
        None,
        description="Nova prioridade: none, low, medium, high, very_high, critical.",
    )
    position: int | None = Field(
        None, description="Nova posicao do card dentro da coluna (zero-indexed)."
    )
    assignee_id: str | None = Field(
        None, description="Novo ID do responsavel pelo card."
    )
    due_date: datetime | None = Field(
        None, description="Nova data limite do card (ISO 8601)."
    )
    labels: list[str] | None = Field(
        None, description="Novas tags de categorizacao do card."
    )
    spec_id: str | None = Field(None, description="Novo ID da spec vinculada ao card.")
    sprint_id: str | None = Field(
        None, description="Novo ID do sprint ao qual o card pertence."
    )
    test_scenario_ids: list[str] | None = Field(
        None,
        description=(
            "Novos IDs de test scenarios vinculados ao card; respeita "
            "board.settings.max_scenarios_per_card."
        ),
    )
    screen_mockups: list[ScreenMockup] | None = Field(
        None, description="Novos mockups de tela vinculados ao card."
    )
    knowledge_bases: list[dict] | None = Field(
        None, description="Base de conhecimento vinculada ao card (lista de dicts)."
    )
    # Bug card fields (only updatable, not card_type or origin_task_id)
    severity: str | None = Field(
        None,
        description="Nova severidade do bug: 'critical', 'major' ou 'minor' (apenas bug cards).",
    )
    expected_behavior: str | None = Field(
        None, description="Comportamento esperado atualizado (apenas bug cards)."
    )
    observed_behavior: str | None = Field(
        None, description="Comportamento observado atualizado (apenas bug cards)."
    )
    steps_to_reproduce: str | None = Field(
        None, description="Passos para reproducao atualizados (apenas bug cards)."
    )
    action_plan: str | None = Field(
        None,
        description="Plano de acao atualizado para correcao do bug (apenas bug cards).",
    )
    linked_test_task_ids: list[str] | None = Field(
        None,
        description="IDs dos cards de teste vinculados a este bug (apenas bug cards).",
    )
    skip_task_requirement_link_gate: bool | None = Field(
        None,
        description=(
            "Bypass humano do gate que exige vinculo direto do task card a "
            "FR/TR/BR/IR/OR. Agentes MCP nao podem alterar este campo."
        ),
    )


def _slim_impact_schema(schema: dict[str, Any]) -> None:
    """Keep the published MCP contract lean (budget R1.1, FR-8).

    Auto-generated titles and docstring descriptions carry no contract:
    enums, caps, lengths and required/forbid ARE the contract and stay.
    """

    schema.pop("title", None)
    schema.pop("description", None)
    for prop in schema.get("properties", {}).values():
        prop.pop("title", None)


class _ImpactEvidenceInput(BaseModel):
    """Base for the write-strict impact_evidence family (SK-B2-S1, TR-2).

    INPUT models are closed (``extra="forbid"``): an unknown key rejects the
    whole move with a field-naming error instead of being silently dropped.
    Read tolerance lives in ``ConclusionEntry`` (a stored non-conform block
    normalizes to ``None``) — never here.
    """

    model_config = ConfigDict(extra="forbid", json_schema_extra=_slim_impact_schema)


_IMPACT_DRIVE_RE = re.compile(r"^[A-Za-z]:")


def _validate_impact_repo_path(path: str) -> str:
    """FR-2 path contract: repo-root-relative with forward slashes."""

    if "\\" in path:
        raise ValueError(
            "impact_evidence_path_invalid: backslashes are not allowed - "
            "use forward slashes"
        )
    if path.startswith("/"):
        raise ValueError(
            "impact_evidence_path_invalid: leading slash - paths are repo-root-relative"
        )
    if _IMPACT_DRIVE_RE.match(path):
        raise ValueError(
            "impact_evidence_path_invalid: drive letters are not allowed - "
            "paths are repo-root-relative"
        )
    if any(segment == ".." for segment in path.split("/")):
        raise ValueError("impact_evidence_path_invalid: '..' segments are not allowed")
    return path


class ImpactEvidenceFile(_ImpactEvidenceInput):
    repo: Literal["core", "community"]
    path: str = Field(..., min_length=1, max_length=500)
    change_kind: Literal["created", "modified", "deleted", "renamed"]
    previous_path: str | None = Field(None, min_length=1, max_length=500)
    note: str | None = Field(None, min_length=1, max_length=2000)

    @field_validator("path", "previous_path")
    @classmethod
    def _paths_repo_relative(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return _validate_impact_repo_path(value)

    @model_validator(mode="after")
    def _previous_path_iff_renamed(self) -> "ImpactEvidenceFile":
        if self.change_kind == "renamed" and self.previous_path is None:
            raise ValueError(
                "impact_evidence_previous_path_required: files.previous_path "
                "is required when change_kind='renamed'"
            )
        if self.change_kind != "renamed" and self.previous_path is not None:
            raise ValueError(
                "impact_evidence_previous_path_forbidden: files.previous_path "
                "is only allowed when change_kind='renamed'"
            )
        return self


class ImpactEvidenceSymbol(_ImpactEvidenceInput):
    name: str = Field(..., min_length=1, max_length=500)
    kind: Literal["function", "class", "method", "component", "port", "other"]
    action: Literal["created", "modified", "deleted"]
    repo: Literal["core", "community"]
    # Mandatory: a symbol claim without its file is not verifiable.
    file: str = Field(..., min_length=1, max_length=500)

    @field_validator("file")
    @classmethod
    def _file_repo_relative(cls, value: str) -> str:
        return _validate_impact_repo_path(value)


class ImpactEvidenceSurface(_ImpactEvidenceInput):
    kind: Literal[
        "rest_route",
        "mcp_tool",
        "mcp_resource",
        "ui_component",
        "table",
        "cli_command",
        "event",
        "migration",
        "other",
    ]
    identifier: str = Field(..., min_length=1, max_length=500)


class ImpactEvidenceTest(_ImpactEvidenceInput):
    action: Literal["added", "updated"]
    repo: Literal["core", "community"]
    test_file_path: str = Field(..., min_length=1, max_length=500)
    test_function: str | None = Field(None, min_length=1, max_length=500)
    scenario_id: str | None = Field(None, min_length=1, max_length=500)

    @field_validator("test_file_path")
    @classmethod
    def _test_path_repo_relative(cls, value: str) -> str:
        return _validate_impact_repo_path(value)


class ImpactEvidence(_ImpactEvidenceInput):
    """Declared execution impact (SK-B2-S1 shape v1) — a CLAIM, not authority.

    The validator keeps diffing reality; declared-vs-real divergence in
    either direction is a first-class validation finding. Lives inside the
    append-only ``cards.conclusions`` JSON — no relational migration (TR-6).
    """

    schema_version: Literal[1] = 1
    files: list[ImpactEvidenceFile] = Field(default_factory=list, max_length=200)
    symbols: list[ImpactEvidenceSymbol] = Field(default_factory=list, max_length=200)
    surfaces: list[ImpactEvidenceSurface] = Field(default_factory=list, max_length=50)
    tests: list[ImpactEvidenceTest] = Field(default_factory=list, max_length=100)
    evidence_refs: list[str] = Field(default_factory=list, max_length=50)

    @field_validator("evidence_refs")
    @classmethod
    def _evidence_refs_stripped_unique(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for ref in value:
            stripped = ref.strip()
            if not stripped:
                raise ValueError(
                    "impact_evidence_evidence_ref_empty: evidence_refs "
                    "entries must be non-empty after strip"
                )
            if len(stripped) > 500:
                raise ValueError(
                    "impact_evidence_evidence_ref_too_long: max 500 characters"
                )
            if stripped in cleaned:
                raise ValueError(
                    "impact_evidence_evidence_ref_duplicate: "
                    f"{stripped!r} appears more than once"
                )
            cleaned.append(stripped)
        return cleaned

    def is_minimally_populated(self) -> bool:
        """FR-6 require bar: at least one item in any of the four sections."""

        return bool(self.files or self.symbols or self.surfaces or self.tests)


class ConclusionEntrySummary(BaseModel):
    """Lean conclusion entry for paginated projections (FR-4/AC-5).

    Deliberately has NO ``impact_evidence`` field: the block is served only
    by the full projections (CardResponse, get_task_conclusions). Parsing a
    stored dict that carries the block simply ignores it here.
    """

    text: str
    author_id: str
    created_at: datetime
    completeness: int = 100  # 0-100
    completeness_justification: str = ""
    drift: int = 0  # 0-100
    drift_justification: str = ""
    # FR-9: declared provenance fields - legacy conclusions already carry
    # them in the JSON; declaring them stops the REST projection from
    # stripping them (the dead Legacy report badge bug).
    source: str | None = None
    validation_id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_shapes(cls, value: Any) -> Any:
        """Accept historical executor/MCP conclusion payload variants.

        `cards.conclusions` is an append-only JSON field and older/newer
        writers have used `description`/`body` and `author`/`author_agent_id`.
        The public card response keeps the stable `text` + `author_id` shape.
        """
        if not isinstance(value, dict):
            return value
        data = dict(value)
        if not data.get("text"):
            data["text"] = (
                data.get("description")
                or data.get("body")
                or data.get("summary")
                or data.get("conclusion")
                or data.get("message")
                or ""
            )
        if not data.get("author_id"):
            data["author_id"] = (
                data.get("author")
                or data.get("author_agent_id")
                or data.get("actor_id")
                or data.get("reviewer_id")
                or data.get("created_by")
                or "unknown"
            )
        if not data.get("created_at"):
            data["created_at"] = "1970-01-01T00:00:00+00:00"
        return data


class ConclusionEntry(ConclusionEntrySummary):
    """A single conclusion entry (full projection).

    Read-tolerant for ``impact_evidence`` (FR-3): a malformed or
    unknown-version stored block normalizes to ``None`` instead of failing
    the card read. Write strictness lives in ``CardMove``/MCP input parsing,
    never here.
    """

    impact_evidence: ImpactEvidence | None = None

    @model_validator(mode="before")
    @classmethod
    def _tolerate_stored_impact_evidence(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        block = value.get("impact_evidence")
        if block is None:
            return value
        data = dict(value)
        try:
            data["impact_evidence"] = ImpactEvidence.model_validate(block)
        except Exception:
            data["impact_evidence"] = None
        return data


CardMoveTargetStatus: TypeAlias = Literal[
    CardStatus.NOT_STARTED,
    CardStatus.STARTED,
    CardStatus.IN_PROGRESS,
    CardStatus.VALIDATION,
    CardStatus.ON_HOLD,
    CardStatus.DONE,
    CardStatus.CANCELLED,
    CardStatus.REJECTED,
]


class CardMove(BaseModel):
    """Schema for moving a card between columns.

    Placement selectors (spec 8b33f9a8, matriz v13): ``position`` (legacy
    positional; None/-1 = fim; < -1 rejeitado — estreitamento autorizado QA
    6afdc547), ``before_id``/``after_id`` (relativo a um card ATIVO da coluna
    destino) e ``placement`` (start|end). Mutuamente exclusivos.

    The published ``oneOf`` (below) is NULL-TOLERANT: excluded fields accept
    ABSENT or EXPLICIT NULL via ``{"type": "null"}`` — never
    ``{"const": null}``, which Pydantic's serializer DROPS (it becomes ``{}``
    and accepts anything). Every raw payload matches EXACTLY one variant or
    zero (422), and the runtime agrees case by case: ``position`` is a
    STRICT int (no ``"0"``/``true`` coercion) and anchors require a
    non-blank character (``pattern \\S``) exactly like the runtime strip
    check. TR4's ``dependentRequired`` is FORMALLY SUBSTITUTED by this
    oneOf: the selectors' co-occurrence rules are exclusions, which
    ``dependentRequired`` cannot express — publishing a vacuous or
    runtime-unenforced coupling would reintroduce schema/runtime divergence
    (substitution recorded on card c8218da8).
    """

    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {
                    "title": "positional",
                    "properties": {
                        "position": {
                            "anyOf": [
                                {"type": "null"},
                                {"type": "integer", "minimum": -1},
                            ]
                        },
                        "before_id": {"type": "null"},
                        "after_id": {"type": "null"},
                        "placement": {"type": "null"},
                    },
                },
                {
                    "title": "relative",
                    "properties": {
                        "position": {"type": "null"},
                        "placement": {"type": "null"},
                    },
                    "oneOf": [
                        {
                            "required": ["before_id"],
                            "properties": {
                                "before_id": {
                                    "type": "string",
                                    "minLength": 1,
                                    "pattern": "\\S",
                                },
                                "after_id": {"type": "null"},
                            },
                        },
                        {
                            "required": ["after_id"],
                            "properties": {
                                "after_id": {
                                    "type": "string",
                                    "minLength": 1,
                                    "pattern": "\\S",
                                },
                                "before_id": {"type": "null"},
                            },
                        },
                    ],
                },
                {
                    "title": "global",
                    "required": ["placement"],
                    "properties": {
                        "placement": {"enum": ["start", "end"]},
                        "position": {"type": "null"},
                        "before_id": {"type": "null"},
                        "after_id": {"type": "null"},
                    },
                },
            ]
        }
    )

    status: CardMoveTargetStatus = Field(
        ...,
        description="Novo status público do card: not_started, started, in_progress, validation, on_hold, done ou cancelled. rejected é aceito somente quando o card já está rejected, para reordenar dentro da mesma coluna; nenhuma transição pode ter rejected como destino manual.",
    )
    position: int | None = Field(
        None,
        description="Nova posicao na coluna de destino (-1 ou None = fim da coluna; < -1 = 422). bool/str sao rejeitados sem coercao; floats matematicamente integrais (1.0/-1.0/-0.0) normalizam para int — exatamente o conjunto que o schema draft 2020-12 aceita como 'integer'.",
    )

    @field_validator("position", mode="before")
    @classmethod
    def _position_integer_kinds(cls, value: object) -> object:
        """Agree with the published Draft 2020-12 ``integer`` semantics.

        ``"0"``/``true`` are rejected WITHOUT coercion (the schema matches
        zero variants for them), while mathematically integral floats
        (``1.0``, ``-1.0``, ``-0.0``) ARE ``integer`` in Draft 2020-12 and
        normalize to ``int``; fractional floats stay invalid.
        """
        if value is None or (isinstance(value, int) and not isinstance(value, bool)):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        raise ValueError(
            "position must be an integer (bool/str rejected; integral floats "
            "normalize; fractional floats invalid)"
        )

    before_id: str | None = Field(
        None,
        description="Ancora relativa: insere IMEDIATAMENTE ANTES deste card ativo da coluna destino. Exclui after_id/position/placement.",
    )
    after_id: str | None = Field(
        None,
        description="Ancora relativa: insere IMEDIATAMENTE DEPOIS deste card ativo da coluna destino. Exclui before_id/position/placement.",
    )
    placement: str | None = Field(
        None,
        description="Posicionamento global na coluna destino: 'start' ou 'end'. Exclui position/anchors.",
    )

    @model_validator(mode="after")
    def _validate_placement_selectors(self) -> "CardMove":
        """Preflight the placement contract at PARSE time — before any service
        read, mutation, policy or event runs (matriz v13; QA 6afdc547).

        ``position`` counts as a selector whenever it is an INT — including
        ``-1`` (explicit positional intent), so ``position=-1 + before_id`` is
        a conflict. An explicit ``position: null`` stays null-tolerant and
        combines freely with anchors/placement. ``position < -1`` is rejected
        here (422 at the REST boundary); anchors must be non-blank;
        ``placement`` only accepts ``start``/``end``.
        """
        if self.position is not None and self.position < -1:
            raise ValueError(
                "position_out_of_range: position must be None, -1 (end of column) or >= 0"
            )
        selectors = [
            name
            for name, value in (
                ("position", self.position),
                ("before_id", self.before_id),
                ("after_id", self.after_id),
                ("placement", self.placement),
            )
            if value is not None
        ]
        if len(selectors) > 1:
            raise ValueError(
                f"card_move_conflicting_placement: {'+'.join(selectors)} — "
                "position/before_id/after_id/placement are mutually exclusive"
            )
        if self.placement is not None and self.placement not in ("start", "end"):
            raise ValueError(
                "card_move_invalid_placement: placement must be 'start' or 'end'"
            )
        for name in ("before_id", "after_id"):
            value = getattr(self, name)
            if value is not None and not value.strip():
                raise ValueError(f"card_move_empty_anchor: {name} must be non-blank")
        return self

    conclusion: str | None = Field(
        None,
        description="Resumo obrigatorio ao mover para 'validation' ou 'done': o que foi feito, arquivos, decisoes e testes.",
    )
    completeness: int | None = Field(
        None,
        description="0-100: quanto do trabalho planejado foi implementado (obrigatorio em validation/done).",
    )
    completeness_justification: str | None = Field(
        None,
        description="Justificativa para o score de completeness (obrigatorio em validation/done).",
    )
    drift: int | None = Field(
        None,
        description="0-100: o quanto a implementacao desviou do plano (0=exato, 100=completamente diferente).",
    )
    drift_justification: str | None = Field(
        None,
        description="Justificativa para o score de drift — explica desvios do plano original.",
    )
    cancellation_reason: str | None = Field(
        None,
        description="Justificativa do cancelamento. Obrigatoria quando status='cancelled'; ignorada nos demais.",
    )
    impact_evidence: ImpactEvidence | None = Field(
        None,
        description=(
            "Evidencia declarada de impacto (schema v1, opcional): files/"
            "symbols/surfaces/tests/evidence_refs re-enumerados pelo autor "
            "no ato da conclusion. CLAIM, nao autoridade - o validador "
            "continua diffando a realidade. Fica FORA do oneOf de placement."
        ),
    )


class CardRejectionCauseResponse(BaseModel):
    """Bounded human-facing cause for a card currently awaiting rework."""

    kind: str
    id: str = Field(..., min_length=1, max_length=REJECTION_ID_MAX_LENGTH)
    code: str = Field(..., min_length=1, max_length=REJECTION_CODE_MAX_LENGTH)
    summary: str = Field(..., min_length=1, max_length=REJECTION_SUMMARY_MAX_LENGTH)


class CardResponse(BaseSchema):
    """Schema for card response."""

    id: str
    board_id: str
    spec_id: str | None = None
    sprint_id: str | None = None
    title: str
    description: str | None
    details: str | None
    status: CardStatus
    subject_version: int = Field(
        ...,
        ge=1,
        validation_alias=AliasChoices("subject_version", "policy_version"),
        description=(
            "Current card policy-subject revision; pass this value as "
            "expected_subject_version when recording an assessment."
        ),
    )
    priority: CardPriority
    position: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    due_date: datetime | None
    labels: list[str] | None
    test_scenario_ids: list[str] | None = None
    screen_mockups: list[ScreenMockup] | None = None
    knowledge_bases: list[dict] | None = None
    conclusions: list[ConclusionEntry] | None = None
    architecture_designs: list[ArchitectureDesignSummary] = []
    attachments: list[AttachmentResponse] = []
    qa_items: list[QAResponse] = []
    comments: list[CommentResponse] = []
    # Bug card fields
    card_type: str = "normal"
    origin_task_id: str | None = None
    severity: str | None = None
    expected_behavior: str | None = None
    observed_behavior: str | None = None
    steps_to_reproduce: str | None = None
    action_plan: str | None = None
    linked_test_task_ids: list[str] | None = None
    skip_task_requirement_link_gate: bool = False
    validations: list[dict] | None = None
    rejection_records: list[dict] = Field(default_factory=list)
    current_rejection_kind: str | None = None
    current_rejection_id: str | None = Field(
        default=None, max_length=REJECTION_ID_MAX_LENGTH
    )
    current_rejection_code: str | None = Field(
        default=None, max_length=REJECTION_CODE_MAX_LENGTH
    )
    current_rejection_summary: str | None = Field(
        default=None, max_length=REJECTION_SUMMARY_MAX_LENGTH
    )
    archived: bool = False
    pre_archive_status: str | None = None
    # Cancellation justification (ITEM 17) — set only while status == cancelled.
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    cancelled_by: str | None = None

    @field_validator("rejection_records", mode="before")
    @classmethod
    def normalize_rejection_records(cls, value: list[dict] | None) -> list[dict]:
        return list(value or [])

    @field_validator("validations", mode="before")
    @classmethod
    def project_public_task_validations(
        cls, value: list[dict] | None
    ) -> list[dict] | None:
        if value is None:
            return None
        return [project_task_validation_public(item) for item in value]

    @field_validator("knowledge_bases", mode="before")
    @classmethod
    def project_knowledge_base_governance(
        cls, value: list[dict] | None
    ) -> list[dict] | None:
        if value is None:
            return None
        return [
            {
                **item,
                "governance": project_knowledge_governance(
                    item.get("governance_metadata")
                ).as_dict(),
            }
            for item in value
        ]


CardCreateResponse: TypeAlias = CardResponse | CardCreateKnowledgeMutationResponse


class CardSummary(BaseSchema):
    """Canonical lean card projection used by all three columns shapes.

    Persisted fields are explicit at the transport boundary. Sensitive
    projections such as ``open_qa_count`` are omitted when the actor lacks
    their dedicated read capability.
    """

    id: str
    board_id: str
    spec_id: str | None
    title: str
    description: str | None
    status: CardStatus
    priority: CardPriority
    position: int
    assignee_id: str | None
    created_by: str | None
    created_at: datetime
    updated_at: datetime
    due_date: datetime | None
    labels: list[str]
    test_scenario_ids: list[str] | None
    # FR-4/AC-5: the paginated projection NEVER carries impact_evidence.
    conclusions: list[ConclusionEntrySummary] | None
    card_type: CardType
    origin_task_id: str | None
    severity: str | None
    linked_test_task_ids: list[str] | None
    archived: bool
    # Count of unanswered Q&A (answered_at IS NULL) — drives the badge.
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    current_rejection_kind: str | None = None
    current_rejection_id: str | None = Field(
        default=None, max_length=REJECTION_ID_MAX_LENGTH
    )
    current_rejection_code: str | None = Field(
        default=None, max_length=REJECTION_CODE_MAX_LENGTH
    )
    current_rejection_summary: str | None = Field(
        default=None, max_length=REJECTION_SUMMARY_MAX_LENGTH
    )


class CardPageItem(BaseSchema):
    """Authoritative lightweight DTO for the paginated board card list.

    Persisted fields are required in the projection, while nullable ORM
    columns and metrics that do not exist until a validation/conclusion occurs
    remain explicitly nullable. Sensitive derived fields are omitted when the
    actor lacks their dedicated read capability.
    """

    id: str
    board_id: str
    spec_id: str | None
    sprint_id: str | None
    title: str
    description: str | None
    status: CardStatus
    priority: CardPriority
    card_type: CardType
    position: int
    assignee_id: str | None
    labels: list[str] | None
    archived: bool
    created_by: str
    due_date: datetime | None
    severity: BugSeverity | None
    test_scenario_ids: list[str] | None
    linked_test_task_ids: list[str] | None
    validations_count: int = Field(..., ge=0)
    validations_fail_count: int = Field(..., ge=0)
    validations_has_pass: bool
    first_pass_confidence: int | None = Field(..., ge=0, le=100)
    first_pass_completeness: int | None = Field(..., ge=0, le=100)
    first_pass_drift: int | None = Field(..., ge=0, le=100)
    conclusions_count: int = Field(..., ge=0)
    last_conclusion_completeness: int | None = Field(..., ge=0, le=100)
    last_conclusion_drift: int | None = Field(..., ge=0, le=100)
    created_at: datetime
    updated_at: datetime
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    current_rejection_kind: str | None = None
    current_rejection_id: str | None = Field(
        default=None, max_length=REJECTION_ID_MAX_LENGTH
    )
    current_rejection_code: str | None = Field(
        default=None, max_length=REJECTION_CODE_MAX_LENGTH
    )
    current_rejection_summary: str | None = Field(
        default=None, max_length=REJECTION_SUMMARY_MAX_LENGTH
    )


# ============================================================================
# Task Validation Schemas
# ============================================================================


class TaskValidationSubmit(BaseModel):
    """Request body for submitting a task validation."""

    expected_subject_version: int = Field(
        ...,
        ge=1,
        validation_alias=AliasChoices(
            "expected_subject_version", "expected_card_version"
        ),
        description="Optimistic card subject-version fence.",
    )
    idempotency_key: str = Field(..., min_length=1, max_length=255)
    confidence: int = Field(
        ..., ge=0, le=100, description="Confianca do validador na avaliacao (0-100)."
    )
    confidence_justification: str = Field(
        ...,
        min_length=10,
        description="Justificativa para o nivel de confianca (min 10 chars).",
    )
    estimated_completeness: int = Field(
        ...,
        ge=0,
        le=100,
        description="Completeness estimado do trabalho entregue (0-100).",
    )
    completeness_justification: str = Field(
        ...,
        min_length=10,
        description="Justificativa para o score de completeness (min 10 chars).",
    )
    estimated_drift: int = Field(
        ...,
        ge=0,
        le=100,
        description="Drift estimado em relacao ao plano original (0-100).",
    )
    drift_justification: str = Field(
        ...,
        min_length=10,
        description="Justificativa para o score de drift (min 10 chars).",
    )
    general_justification: str = Field(
        ...,
        min_length=20,
        description="Justificativa geral da decisao de validacao (min 20 chars).",
    )
    recommendation: str = Field(
        ...,
        pattern="^(approve|reject)$",
        description="Recomendacao do validador: 'approve' ou 'reject'.",
    )


class TaskValidationResponse(BaseModel):
    """Response for a task validation."""

    model_config = ConfigDict(extra="ignore")

    id: str
    card_id: str
    board_id: str
    # Historical rows predating the governed validation contract can be sparse.
    # The submit input remains strict; public history reads are deliberately
    # tolerant while still stripping all internal ledger fields.
    reviewer_id: str | None = None
    reviewer_name: str | None = Field(default=None, max_length=255)
    evaluator_id: str | None = None
    evaluator_name: str | None = Field(default=None, max_length=255)
    confidence: int | None = None
    confidence_justification: str | None = None
    estimated_completeness: int | None = None
    completeness: int | None = None
    completeness_justification: str | None = None
    estimated_drift: int | None = None
    drift: int | None = None
    drift_justification: str | None = None
    general_justification: str | None = None
    summary: str | None = None
    recommendation: str | None = None
    outcome: str | None = None
    verdict: str | None = None
    validation_outcome: str | None = None
    completion_outcome: str | None = None
    threshold_violations: list[str] = Field(default_factory=list)
    created_at: str | None = None
    card_status: str | None = None
    resolved_thresholds: dict | None = None
    reviewer_separation: dict | None = None
    expected_subject_version: int | None = None
    completion_gate_failures: list[dict] = Field(default_factory=list)
    rejection_cause: CardRejectionCauseResponse | None = None
    subject_version: int | None = None
    replayed: bool = False


class TaskValidationListResponse(BaseModel):
    """Public reverse-chronological Task Validation history envelope."""

    card_id: str
    total: int = Field(..., ge=0)
    validations: list[TaskValidationResponse] = Field(default_factory=list)


_CARD_VALIDATION_REDACTION = {
    "validations": None,
    "rejection_records": [],
    "current_rejection_kind": None,
    "current_rejection_id": None,
    "current_rejection_code": None,
    "current_rejection_summary": None,
    "validations_count": 0,
    "validations_fail_count": 0,
    "validations_has_pass": False,
    "first_pass_confidence": None,
    "first_pass_completeness": None,
    "first_pass_drift": None,
}


def redact_card_validation_projection(value: Any) -> Any:
    """Remove validation bodies, causal summaries and aggregate score signals.

    The helper is shared by full-card and paginated/column boundaries.  It
    preserves lifecycle status (including ``rejected``) while making the
    dedicated ``card.validation.read`` permission authoritative for every
    human or agent-facing validation signal.
    """

    if isinstance(value, BaseModel):
        fields = value.__class__.model_fields
        updates = {
            key: item
            for key, item in _CARD_VALIDATION_REDACTION.items()
            if key in fields
        }
        return value.model_copy(update=updates)
    if isinstance(value, Mapping):
        projected = dict(value)
        for key, item in _CARD_VALIDATION_REDACTION.items():
            if key in projected:
                projected[key] = list(item) if isinstance(item, list) else item
        return projected
    return value


_TASK_VALIDATION_PRIVATE_FIELDS = frozenset(
    {"response", "request_digest", "idempotency_key"}
)


def project_task_validation_public(
    value: Mapping[str, Any] | BaseModel,
    *,
    card_id: str | None = None,
    board_id: str | None = None,
    replayed: bool | None = None,
) -> dict[str, Any]:
    """Project one persisted Task Validation into its public, replay-stable DTO.

    New entries validate through :class:`TaskValidationResponse`. Historical
    rows are intentionally read-tolerant: known clean aliases are normalized,
    context-known card/board identities are filled, and whatever canonical
    fields are actually present are returned. Ledger plumbing is removed on
    every path, including legacy nested ``response`` snapshots.
    """

    if isinstance(value, BaseModel):
        raw = value.model_dump(mode="python")
    elif isinstance(value, Mapping):
        raw = dict(value)
    else:
        return {}

    nested = raw.get("response")
    merged = {
        key: item
        for key, item in raw.items()
        if key not in _TASK_VALIDATION_PRIVATE_FIELDS
    }
    if isinstance(nested, Mapping):
        merged.update(
            {
                key: item
                for key, item in nested.items()
                if key not in _TASK_VALIDATION_PRIVATE_FIELDS
            }
        )
    if card_id and not merged.get("card_id"):
        merged["card_id"] = card_id
    if board_id and not merged.get("board_id"):
        merged["board_id"] = board_id

    alias_pairs = (
        ("reviewer_id", "evaluator_id"),
        ("reviewer_name", "evaluator_name"),
        ("estimated_completeness", "completeness"),
        ("estimated_drift", "drift"),
        ("general_justification", "summary"),
    )
    for canonical, alias in alias_pairs:
        if merged.get(canonical) is None and merged.get(alias) is not None:
            merged[canonical] = merged[alias]
        if merged.get(alias) is None and merged.get(canonical) is not None:
            merged[alias] = merged[canonical]
    if merged.get("outcome") is None and merged.get("verdict") in {"pass", "fail"}:
        merged["outcome"] = "success" if merged["verdict"] == "pass" else "failed"
    if merged.get("verdict") is None and merged.get("outcome") in {
        "success",
        "failed",
    }:
        merged["verdict"] = "pass" if merged["outcome"] == "success" else "fail"
    merged.setdefault("threshold_violations", [])
    merged.setdefault("completion_gate_failures", [])
    if replayed is not None:
        merged["replayed"] = replayed
    else:
        merged["replayed"] = bool(merged.get("replayed", False))

    try:
        return TaskValidationResponse.model_validate(merged).model_dump(
            mode="json",
            exclude_none=False,
        )
    except (TypeError, ValueError):
        # Legacy task validations can predate required identity/score fields.
        # Preserve only reviewed public keys; never fall back to the raw dict.
        allowed = set(TaskValidationResponse.model_fields)
        return {key: item for key, item in merged.items() if key in allowed}


# ============================================================================
# Spec Validation Gate Schemas
# ============================================================================


class SpecValidationPinpoint(BaseModel):
    """Closed evaluator-supplied location tagged with one quality metric."""

    model_config = ConfigDict(extra="forbid")

    metric: Literal[
        "confidence",
        "clarity",
        "assertiveness",
        "decidability",
        "ambiguity",
    ]
    anchor_type: Literal["whole_artifact", "field", "structured_child", "qa"]
    anchor_ref: str | None = Field(default=None, min_length=1, max_length=4096)
    detail: str = Field(..., min_length=1, max_length=4096)

    @model_validator(mode="after")
    def validate_anchor(self) -> "SpecValidationPinpoint":
        self.detail = self.detail.strip()
        if not self.detail:
            raise ValueError("spec_validation_pinpoint_detail_invalid")
        if self.anchor_type == "whole_artifact":
            if self.anchor_ref is not None:
                raise ValueError("spec_validation_pinpoint_anchor_ref_forbidden")
            return self
        if self.anchor_ref is None or not self.anchor_ref.strip():
            raise ValueError("spec_validation_pinpoint_anchor_ref_required")
        self.anchor_ref = self.anchor_ref.strip()
        return self


class SpecValidationAnchorSnapshotResponse(BaseModel):
    """Immutable human-readable anchor content stored with a validation."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["spec-validation-pinpoint-snapshot/v1"] = (
        "spec-validation-pinpoint-snapshot/v1"
    )
    availability_at_seal: Literal["available", "legacy_unavailable"]
    label: str | None = None
    text: str | None = None
    excerpt: str | None = None
    source_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    source_version: str | None = None


class SpecValidationPinpointResponse(SpecValidationPinpoint):
    """Read projection; old rows state that no sealed snapshot exists."""

    anchor_snapshot: SpecValidationAnchorSnapshotResponse = Field(
        default_factory=lambda: SpecValidationAnchorSnapshotResponse(
            availability_at_seal="legacy_unavailable"
        )
    )


class SpecValidationSubmit(BaseModel):
    """Canonical five-dimensional Spec Validation submission."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    expected_validation_edition: int = Field(
        ...,
        ge=1,
        validation_alias=AliasChoices(
            "expected_validation_edition",
            "expected_spec_edition",
        ),
    )
    expected_spec_version: int = Field(..., ge=1)
    expected_head_revision: int = Field(..., ge=0)
    confidence: int = Field(..., ge=0, le=100)
    confidence_justification: str = Field(..., min_length=10)
    clarity: int = Field(..., ge=0, le=100)
    clarity_justification: str = Field(..., min_length=10)
    assertiveness: int = Field(..., ge=0, le=100)
    assertiveness_justification: str = Field(..., min_length=10)
    decidability: int = Field(..., ge=0, le=100)
    decidability_justification: str = Field(..., min_length=10)
    ambiguity: int = Field(..., ge=0, le=100)
    ambiguity_justification: str = Field(..., min_length=10)
    recommendation: Literal["approve", "reject"]
    pinpoints: list[SpecValidationPinpoint] | None = None


class SpecValidationResponse(BaseModel):
    """History record; legacy evidence may predate lifecycle editions."""

    id: str | None = None
    validation_id: str | None = None
    validation_edition: int | None = Field(default=None, ge=1)
    is_current: bool = False
    spec_id: str | None = None
    board_id: str | None = None
    reviewer_id: str | None = None
    reviewer_name: str | None = None
    score: float | None = None
    summary: str | None = None
    confidence: int | None = None
    confidence_justification: str | None = None
    clarity: int | None = None
    clarity_justification: str | None = None
    decidability: int | None = None
    decidability_justification: str | None = None
    completeness: int | None = None
    completeness_justification: str | None = None
    assertiveness: int | None = None
    assertiveness_justification: str | None = None
    ambiguity: int | None = None
    ambiguity_justification: str | None = None
    general_justification: str | None = None
    recommendation: str | None = None
    pinpoints: list[SpecValidationPinpointResponse] | None = None
    outcome: str | None = None
    receipt_id: str | None = None
    subject_version: int | None = Field(default=None, ge=1)
    head_revision: int | None = Field(default=None, ge=1)
    digests: dict[str, str] | None = None
    threshold_violations: list[str] | None = None
    resolved_thresholds: dict | None = None
    created_at: str | None = None
    spec_status: str | None = None
    active: bool | None = None
    edition: int | None = Field(default=None, ge=1)
    lifecycle_state: Literal["current", "previous", "history_only"] | None = None

    @model_validator(mode="after")
    def require_compatible_identity(self) -> "SpecValidationResponse":
        if not self.id and not self.validation_id:
            raise ValueError("spec_validation_identity_required")
        if self.lifecycle_state == "current" and self.validation_edition is None:
            raise ValueError("spec_validation_current_edition_required")
        if (
            self.lifecycle_state == "history_only"
            and self.validation_edition is not None
        ):
            raise ValueError("spec_validation_history_only_edition_forbidden")
        return self


# ============================================================================
# Guideline Schemas
# ============================================================================


class GuidelineCreate(BaseModel):
    """Schema for creating a guideline."""

    title: str = Field(..., min_length=1, max_length=500)
    content: str = Field(..., min_length=1)
    tags: list[str] | None = None
    scope: str = "global"
    board_id: str | None = None
    priority: int = Field(default=0, ge=0)


class GuidelineUpdate(BaseModel):
    """Schema for updating a guideline."""

    title: str | None = Field(None, min_length=1, max_length=500)
    content: str | None = None
    tags: list[str] | None = None


class GuidelineResponse(BaseSchema):
    """Schema for guideline response."""

    id: str
    title: str
    content: str
    tags: list[str] | None
    scope: str
    board_id: str | None
    owner_id: str
    created_at: datetime
    updated_at: datetime


class BoardGuidelineLinkRequest(BaseModel):
    """Schema for linking a global guideline or creating an inline board guideline."""

    guideline_id: str | None = None
    title: str | None = Field(None, min_length=1, max_length=500)
    content: str | None = Field(None, min_length=1)
    tags: list[str] | None = None
    priority: int = 0


# ============================================================================
# Board Share Schemas
# ============================================================================


class BoardShareCreate(BaseModel):
    """Schema for sharing a board with a user."""

    user_id: str = Field(..., min_length=1, max_length=255)
    permission: str = Field(default="viewer", pattern="^(viewer|editor|admin)$")


class BoardShareUpdate(BaseModel):
    """Schema for updating a board share permission."""

    permission: str = Field(..., pattern="^(viewer|editor|admin)$")


class BoardShareResponse(BaseSchema):
    """Schema for board share response."""

    id: str
    board_id: str
    user_id: str
    realm_id: str
    permission: str
    shared_by: str
    created_at: datetime


# ============================================================================
# Board Schemas
# ============================================================================


class SpecResourceType(str, PyEnum):
    """Resource types supported by Spec-to-card auto propagation."""

    KNOWLEDGE_BASE = "knowledge_base"
    ARCHITECTURE = "architecture"
    MOCKUP = "mockup"


class CodeTraceabilitySettings(BaseModel):
    """Board policy for agent-attested code traceability.

    This policy never grants Pulse permission to inspect a repository.  It
    only governs bounded attestations submitted by an authenticated external
    agent and the projections derived from accepted receipts.
    """

    model_config = ConfigDict(extra="forbid")

    mode: CodeTraceabilityEnforcement = CodeTraceabilityEnforcement.ADVISORY
    evidence_attestation: Literal["none", "preferred", "required"] = "preferred"
    target_resolution: Literal[
        "advisory",
        "required",
        "required_current_receipt",
    ] = "advisory"
    accepted_attestor_policy: Literal[
        "granular_permission",
        "granular_permission_and_board_allowlist",
    ] = "granular_permission"
    minimum_trust: Literal["single_attestation", "corroborated"] = "single_attestation"
    # Closed server-owned range.  The configured value controls accepted
    # receipt currentness; it is never derived from agent-supplied observed_at.
    preflight_freshness_seconds: int = Field(default=1800, ge=60, le=86_400)
    overlap_policy: Literal["off", "warn", "block_parallel"] = "warn"
    observed_state_policy: Literal[
        "allow_dirty_attestation",
        "require_committed_attestation",
    ] = "allow_dirty_attestation"
    receipt_content: Literal["metadata_only", "safe_excerpt"] = "safe_excerpt"

    @classmethod
    def from_persisted(cls, value: object) -> "CodeTraceabilitySettings":
        """Read one historical policy without reopening the write contract.

        ``off`` and an explicit legacy ``null`` were valid before enforcement
        became mandatory.  They now resolve to Advisory so existing databases
        remain readable and agent-mediated checks still run.  Native model
        validation remains strict, so Board create/update and default-template
        writes cannot author either compatibility value.
        """

        if isinstance(value, cls):
            return value
        if value is None:
            return cls()
        if isinstance(value, Mapping) and value.get("mode") == "off":
            value = {
                **value,
                "mode": CodeTraceabilityEnforcement.ADVISORY.value,
            }
        return cls.model_validate(value)


FlowHealthOverrideState: TypeAlias = Literal[
    "backlog",
    "pending",
    "in_progress",
    "rejected",
    "done",
]


class FlowHealthSettings(BaseModel):
    """Closed, revisioned board policy for Flow Health analytics.

    ``version`` is the policy revision used by the governed settings write
    contract.  It is deliberately distinct from the fixed Analytics settings
    schema version.  Thresholds are whole hours and match the canonical Flow
    Health defaults published by Core.
    """

    model_config = ConfigDict(extra="forbid")

    version: int = Field(default=1, ge=1, strict=True)
    general_stale_hours: int = Field(default=72, ge=1, strict=True)
    rejected_stale_hours: int = Field(default=96, ge=1, strict=True)
    overrides: dict[FlowHealthOverrideState, int] = Field(default_factory=dict)

    @field_validator("overrides", mode="before")
    @classmethod
    def _validate_overrides(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            raise ValueError("flow_health overrides must be an object")
        for state, stale_hours in value.items():
            if state not in {
                "backlog",
                "pending",
                "in_progress",
                "rejected",
                "done",
            }:
                raise ValueError(f"unsupported flow_health override state: {state}")
            if type(stale_hours) is not int or stale_hours < 1:
                raise ValueError(
                    "flow_health override thresholds must be positive whole hours"
                )
        return dict(value)


class AnalyticsSettings(BaseModel):
    """Closed board-level Analytics policy envelope (schema version 1)."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    flow_health: FlowHealthSettings = Field(default_factory=FlowHealthSettings)


class FlowHealthSettingsUpdate(BaseModel):
    """Closed full-replacement payload for a CAS-protected policy save."""

    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1, strict=True)
    general_stale_hours: int = Field(ge=1, strict=True)
    rejected_stale_hours: int = Field(ge=1, strict=True)
    overrides: dict[FlowHealthOverrideState, int] = Field(default_factory=dict)

    @field_validator("overrides", mode="before")
    @classmethod
    def _validate_overrides(cls, value: object) -> object:
        return FlowHealthSettings._validate_overrides(value)


class FlowHealthSettingsRestore(BaseModel):
    """Closed CAS payload for restoring Core defaults without losing history."""

    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1, strict=True)


class BoardSettings(BaseModel):
    """Board-level settings for governance rules."""

    analytics: AnalyticsSettings = Field(default_factory=AnalyticsSettings)

    max_scenarios_per_card: int = 3  # max test scenarios a single card can be linked to
    skip_test_coverage_global: bool = (
        False  # if True, all specs bypass test coverage checks
    )
    skip_rules_coverage_global: bool = (
        False  # if True, all specs bypass FR→BR coverage checks
    )
    skip_trs_coverage_global: bool = (
        False  # if True, all specs bypass TR→Task coverage checks
    )
    skip_code_evidence_coverage_global: bool = (
        False  # if True, all specs bypass Code Evidence Matrix coverage checks
    )
    skip_contract_coverage_global: bool = (
        False  # if True, all specs bypass API contract coverage checks
    )
    skip_ir_coverage_global: bool = (
        False  # if True, all specs bypass IR→Task coverage checks
    )
    skip_or_coverage_global: bool = (
        False  # if True, all specs bypass OR→Task coverage checks
    )
    skip_task_requirement_link_gate_global: bool = (
        False  # if True, task cards may start without direct FR/TR/BR/IR/OR links
    )
    skip_decisions_coverage_global: bool = False  # if True, all specs bypass active-Decision→Task coverage checks (ideação #10 Fase 1)
    skip_cognitive_consolidation: bool = (
        False  # if True, done closeout bypasses active cognitive pending blockers
    )
    allow_agent_self_answering: bool = (
        False  # explicit opt-in that permits same-principal Q&A answers
    )
    require_full_context_for_critical_actions: bool = (
        True  # if True, critical mutations must resolve full entity context
    )
    qa_require_role_separation: bool = False  # if True, a Q&A question cannot be answered by the same principal who asked it
    # Task-validation and sprint reviewer/executor separation. Missing on legacy
    # persisted boards is resolved explicitly as ``off``; new boards and new
    # default-board template versions inject ``enforce`` unless the administrator
    # chooses another mode.
    reviewer_separation_mode: Literal["off", "warn", "enforce"] = "off"
    # Requirement-lint languages for this board's spec content. Each code
    # activates the built-in lexicon of that language; multiple codes are
    # evaluated as a deterministic UNION of lexicons. Empty (the legacy
    # default) keeps the neutral-only profile: no language guessing, only
    # numbers/comparators/units/technical terms count as signals.
    lint_languages: list[Literal["pt-BR", "en-US", "es-ES", "de-DE", "fr-FR"]] = Field(
        default_factory=list
    )
    # Impact-evidence enforcement on execution reports (SK-B2-S1, FR-5).
    # off = no effect; advisory = gated moves succeed but a missing block is
    # recorded in the activity log; require = gated moves reject a conclusion
    # without a minimally populated block. Write-time validation rejects
    # out-of-enum values; READ-side resolution of persisted legacy/tampered
    # values is fail-compat ('off') via resolve_impact_evidence_mode.
    impact_evidence_mode: Literal["off", "advisory", "require"] = "off"
    # Design System mockup gate mode (spec 3a006f65 / card 96f76a5f). CANONICAL source
    # of the board's Design System gate mode (the design_system_default_ref only carries
    # the DS identity; any gate_mode inside it is a derived mirror). off = no gate;
    # advisory = warn/audit; blocking = reject mockups without valid DS evidence. Legacy
    # boards with no field validate as 'off' (TR4 — never breaks an existing board).
    design_system_gate_mode: Literal["off", "advisory", "blocking"] = "off"
    # Agent-mediated Code Traceability is always evaluated. Historical absent,
    # null, or ``off`` policies resolve to Advisory on tolerant READ paths and
    # are converged by Community's startup migration. Native writes are closed
    # to Advisory or Blocking.
    code_traceability: CodeTraceabilitySettings = Field(
        default_factory=CodeTraceabilitySettings
    )
    # Task Validation Gate — board-level defaults (overridable at spec/sprint)
    require_task_validation: bool = (
        True  # if True, cards must pass validation before moving to done
    )
    min_confidence: int = 70  # min reviewer confidence score
    min_completeness: int = 80  # min reviewer completeness score
    max_drift: int = 50  # max reviewer drift score
    # Spec Validation Gate — board-level defaults
    require_spec_validation: bool = (
        True  # if True, approved→validated requires Spec Validation Gate submission
    )
    min_spec_confidence: int = Field(default=70, ge=0, le=100)
    min_spec_clarity: int = Field(default=80, ge=0, le=100)
    min_spec_assertiveness: int = Field(default=80, ge=0, le=100)
    min_spec_decidability: int = Field(default=80, ge=0, le=100)
    max_spec_ambiguity: int = Field(default=30, ge=0, le=100)
    # Compatibility-only setting for historical three-dimensional records.
    # It is not part of the canonical five-metric gate.
    min_spec_completeness: int = Field(default=80, ge=0, le=100)
    # Max ambiguity gate for ideation completion — opt-in (spec 2485780b).
    # When enabled, blocks ONLY the evaluating→done transition if the ideation
    # has no ambiguity score or scope_assessment.ambiguity exceeds the
    # configured threshold. Default disabled; threshold validated to 1-5.
    require_ideation_ambiguity_gate: bool = False
    max_ideation_ambiguity: int = 3  # max allowed ideation ambiguity (1-5)
    # Receipt-backed ambiguity gate for Refinement approved -> done.  Legacy
    # boards remain disabled with threshold 3; new board templates may opt in.
    require_refinement_ambiguity_gate: bool = False
    max_refinement_ambiguity: int = 3  # max allowed ambiguity (1-5)
    # Resource Gate - Level 2 spec resource-to-task coverage.
    require_spec_resource_task_coverage: bool = True
    # Spec resource automation — when enabled, selected resources are copied
    # from a linked Spec to newly-created or newly-linked cards.
    auto_derive_spec_resources_enabled: bool = False
    auto_derive_spec_resource_types: list[SpecResourceType] = Field(
        default_factory=list
    )
    # Bug Card Gate — NC-6 fix.
    # require_test_task_for_bug: when False, bug cards can advance to in_progress
    #   without a freshly-created linked test task. Default True (gate ATIVO).
    # bug_test_gate_min_severity: only bugs at this severity OR higher must pass
    #   the gate. "minor" (default) = sempre exige; "major" = pula minor;
    #   "critical" = só critical exige.
    require_test_task_for_bug: bool = True
    bug_test_gate_min_severity: str = "minor"  # one of: minor, major, critical
    # Test Theater Prevention Gate — Wave 2 NC-9 (spec 873e98cc).
    # When False (default), update_test_scenario_status with status in
    # {automated, passed, failed} requires structured evidence (test_file_path,
    # test_function for automated; explicit evidence_class with its required
    # replayable fields, or unclassed run-log evidence with last_run_at +
    # (output_snippet|test_run_id) + expected_output_snapshot +
    # non_replayable_justification for passed/failed). When True, gate is bypass
    # — any status accepted without evidence; audit log records every bypass for
    # forensics.
    skip_test_evidence_global: bool = False
    # Cognitive Extraction LLM config — opt-in (spec 3d907a87, FR7 / D5).
    # Schema (free-form dict so it can evolve without a migration):
    #   {"provider": "openai" | "anthropic" | ..., "model": "...",
    #    "api_key_env": "OPENAI_API_KEY", "max_tokens": 800, "timeout_s": 30}
    # Absent or None → CognitiveExtractionHandler skips Learning extraction
    # and emits log info. Alternative + Assumption (regex) run regardless.
    cognitive_llm_config: dict | None = None

    @field_validator("auto_derive_spec_resource_types")
    @classmethod
    def _deduplicate_auto_derive_resource_types(
        cls,
        value: list[SpecResourceType],
    ) -> list[SpecResourceType]:
        return list(dict.fromkeys(value or []))

    @model_validator(mode="after")
    def _validate_auto_derive_resource_selection(self) -> "BoardSettings":
        resource_types = self.auto_derive_spec_resource_types or []
        if self.auto_derive_spec_resources_enabled and not (
            1 <= len(resource_types) <= 3
        ):
            raise ValueError(
                "auto_derive_spec_resource_types must include between 1 and 3 resource types "
                "when auto_derive_spec_resources_enabled is true"
            )
        return self

    @field_validator("lint_languages")
    @classmethod
    def _validate_lint_languages(
        cls,
        value: list[str],
    ) -> list[str]:
        deduped: list[str] = []
        for code in value:
            if code not in deduped:
                deduped.append(code)
        return deduped

    @field_validator("max_ideation_ambiguity")
    @classmethod
    def _validate_max_ideation_ambiguity(cls, value: int) -> int:
        """Reject ideation ambiguity thresholds outside 1-5 (spec 2485780b TR2)."""
        if not 1 <= value <= 5:
            raise ValueError("max_ideation_ambiguity must be between 1 and 5")
        return value

    @field_validator("max_refinement_ambiguity")
    @classmethod
    def _validate_max_refinement_ambiguity(cls, value: int) -> int:
        if not 1 <= value <= 5:
            raise ValueError("max_refinement_ambiguity must be between 1 and 5")
        return value


class BoardCreate(BaseModel):
    """Schema for creating a board."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    settings: BoardSettings | None = None


class BoardUpdate(BaseModel):
    """Schema for updating a board."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    settings: BoardSettings | None = None


class BoardResponse(BaseSchema):
    """Schema for board response."""

    id: str
    name: str
    description: str | None
    owner_id: str
    realm_id: str | None = None
    settings: BoardSettings | None = None
    # Applied DefaultBoardConfiguration snapshot metadata (spec 9df814bc / TR11).
    # Null/absent for the no-active-template fallback path; the snapshot dict
    # (template_id, template_version, applied_at, applied_by, override_summary)
    # when a template was applied. Distinct from settings (governance payload).
    default_config_snapshot: dict | None = None
    created_at: datetime
    updated_at: datetime
    cards: list[CardResponse] = []
    agents: list[AgentSummary] = []
    counts: dict[str, int] | None = None


class BoardSummary(BaseSchema):
    """Schema for board summary (without nested items)."""

    id: str
    name: str
    description: str | None
    owner_id: str
    realm_id: str | None = None
    settings: BoardSettings | None = None
    created_at: datetime
    updated_at: datetime


class BoardListResponse(BaseSchema):
    """Schema for board list with cards grouped by status."""

    board: BoardSummary
    columns: dict[str, list[CardSummary]]


class ColumnFacets(BaseSchema):
    """Self-excluding facet counts for one kanban column."""

    model_config = ConfigDict(from_attributes=True, extra="allow")

    card_type: dict[str, int]


class ColumnMeta(BaseSchema):
    """Counts and facets accompanying a column's bounded card window."""

    model_config = ConfigDict(from_attributes=True, extra="allow")

    total_filtered: int = Field(..., ge=0)
    total_overall: int = Field(..., ge=0)
    has_more: bool
    facets: ColumnFacets


class ColumnsFacets(BaseSchema):
    """Board-wide facets shared by all columns in the batch response."""

    model_config = ConfigDict(from_attributes=True, extra="allow")

    # The established facet wire is a stable list of {value,count} entries;
    # ``value`` may be null for unassigned cards.
    assignee: list[dict[str, Any]]


class ColumnsMeta(BaseSchema):
    """Per-column metadata plus board-wide facets."""

    model_config = ConfigDict(from_attributes=True, extra="allow")

    columns: dict[str, ColumnMeta]
    facets: ColumnsFacets


def _forbid_shape_fields(
    value: object,
    *,
    forbidden: tuple[str, ...],
    shape: str,
) -> object:
    """Reject only reserved fields from competing shapes.

    Responses remain open to unrelated forward-compatible fields while the
    reserved shape fields make the published ``oneOf`` truly exclusive.
    """

    if isinstance(value, dict):
        present = sorted(set(value).intersection(forbidden))
        if present:
            raise ValueError(
                f"{shape} response cannot contain fields from another shape: "
                f"{', '.join(present)}"
            )
    return value


class ColumnsLegacyResponse(BaseSchema):
    """Literal legacy columns response (no pagination metadata)."""

    model_config = ConfigDict(
        from_attributes=True,
        extra="allow",
        json_schema_extra={
            "allOf": [
                {
                    "not": {
                        "anyOf": [
                            {"required": [field]}
                            for field in (
                                "columns_meta",
                                "column",
                                "items",
                                "meta",
                                "next_offset",
                            )
                        ]
                    }
                }
            ]
        },
    )

    board_id: str
    columns: dict[str, list[CardSummary]]

    @model_validator(mode="before")
    @classmethod
    def _exclude_other_shapes(cls, value: object) -> object:
        return _forbid_shape_fields(
            value,
            forbidden=("columns_meta", "column", "items", "meta", "next_offset"),
            shape="legacy",
        )


class ColumnsOptInResponse(BaseSchema):
    """Bounded windows for every column, with batch metadata and facets."""

    model_config = ConfigDict(
        from_attributes=True,
        extra="allow",
        json_schema_extra={
            "allOf": [
                {
                    "not": {
                        "anyOf": [
                            {"required": [field]}
                            for field in ("column", "items", "next_offset")
                        ]
                    }
                }
            ]
        },
    )

    board_id: str
    columns: dict[str, list[CardSummary]]
    columns_meta: ColumnsMeta

    @model_validator(mode="before")
    @classmethod
    def _exclude_other_shapes(cls, value: object) -> object:
        return _forbid_shape_fields(
            value,
            forbidden=("column", "items", "next_offset"),
            shape="opt-in",
        )


class ColumnPageResponse(BaseSchema):
    """One independently paged kanban column."""

    model_config = ConfigDict(
        from_attributes=True,
        extra="allow",
        json_schema_extra={
            "allOf": [
                {
                    "not": {
                        "anyOf": [
                            {"required": ["columns"]},
                            {"required": ["columns_meta"]},
                        ]
                    }
                }
            ]
        },
    )

    board_id: str
    column: CardStatus
    items: list[CardSummary]
    meta: ColumnMeta
    offset: int = Field(..., ge=0)
    limit: int = Field(..., ge=1, le=100)
    next_offset: int | None = Field(..., ge=0)

    @model_validator(mode="before")
    @classmethod
    def _exclude_other_shapes(cls, value: object) -> object:
        return _forbid_shape_fields(
            value,
            forbidden=("columns", "columns_meta"),
            shape="column page",
        )


def _publish_columns_one_of(schema: dict[str, Any]) -> None:
    """Publish the response union as JSON Schema ``oneOf``, not ``anyOf``."""

    variants = schema.pop("anyOf", None)
    if variants is not None:
        schema["oneOf"] = variants


class ColumnsResponseUnion(
    RootModel[ColumnsLegacyResponse | ColumnsOptInResponse | ColumnPageResponse]
):
    """OpenAPI-only union for the three mutually exclusive columns shapes."""

    model_config = ConfigDict(json_schema_extra=_publish_columns_one_of)


# ============================================================================
# Activity Log Schemas
# ============================================================================


class ActivityLogResponse(BaseSchema):
    """Schema for activity log response."""

    id: str
    board_id: str
    card_id: str | None
    action: str
    actor_type: str
    actor_id: str
    actor_name: str
    trigger: str | None = None
    summary: str = ""
    details: dict[str, Any] | None
    created_at: datetime


# ============================================================================
# Sprint Schemas
# ============================================================================


class SprintCreate(BaseModel):
    """Schema for creating a sprint."""

    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = None
    objective: str | None = None
    expected_outcome: str | None = None
    spec_id: str
    lane_type: SprintLaneType = SprintLaneType.NORMAL
    origin_sprint_id: str | None = None
    origin_bug_id: str | None = None
    test_scenario_ids: list[str] | None = None
    business_rule_ids: list[str] | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    labels: list[str] | None = None


class SprintUpdate(BaseModel):
    """Schema for updating a sprint."""

    title: str | None = Field(None, min_length=1, max_length=500)
    description: str | None = None
    objective: str | None = None
    expected_outcome: str | None = None
    lane_type: SprintLaneType | None = None
    origin_sprint_id: str | None = None
    origin_bug_id: str | None = None
    test_scenario_ids: list[str] | None = None
    business_rule_ids: list[str] | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    labels: list[str] | None = None
    skip_test_coverage: bool | None = None
    skip_rules_coverage: bool | None = None
    skip_qualitative_validation: bool | None = None
    validation_threshold: int | None = None
    require_task_validation: bool | None = Field(
        None,
        description="Override do sprint para exigir Task Validation; None herda da spec/board.",
    )
    validation_min_confidence: int | None = Field(None, ge=0, le=100)
    validation_min_completeness: int | None = Field(None, ge=0, le=100)
    validation_max_drift: int | None = Field(None, ge=0, le=100)
    expected_version: int | None = Field(
        None,
        ge=1,
        description="Optimistic-lock version read by the caller.",
    )


class SprintMove(BaseModel):
    """Schema for changing sprint status."""

    status: SprintStatus
    cancellation_reason: str | None = Field(
        None,
        description="Justificativa do cancelamento. Obrigatoria quando status='cancelled'; ignorada nos demais.",
    )
    expected_version: int | None = Field(
        None,
        ge=1,
        description="Optimistic-lock version read by the caller.",
    )


class SprintEvaluationCreate(BaseModel):
    """Schema for submitting a sprint evaluation (4 dimensions + overall)."""

    breakdown_completeness: int = Field(..., ge=0, le=100)
    breakdown_justification: str
    granularity: int = Field(..., ge=0, le=100)
    granularity_justification: str
    dependency_coherence: int = Field(..., ge=0, le=100)
    dependency_justification: str
    test_coverage_quality: int = Field(..., ge=0, le=100)
    test_coverage_justification: str
    overall_score: int = Field(..., ge=0, le=100)
    overall_justification: str
    recommendation: str = Field(..., pattern=r"^(approve|request_changes|reject)$")


class SprintQACreate(BaseModel):
    """Schema for asking a question on a sprint."""

    question: str = Field(..., min_length=1)
    question_type: str = "text"
    choices: list[dict] | None = None
    allow_free_text: bool = False


class SprintQAAnswer(BaseModel):
    """Schema for answering a sprint question."""

    answer: str | None = None
    selected: list[str] | None = None


class SprintQAResponse(BaseSchema):
    """Schema for sprint Q&A item response."""

    id: str
    sprint_id: str
    question: str
    question_type: str
    choices: list[dict] | None = None
    allow_free_text: bool = False
    answer: str | None = None
    selected: list[str] | None = None
    asked_by: str
    answered_by: str | None = None
    created_at: datetime
    answered_at: datetime | None = None


class SprintHistoryResponse(BaseSchema):
    """Schema for sprint history entry."""

    id: str
    sprint_id: str
    action: str
    actor_type: str
    actor_id: str
    actor_name: str
    changes: list | None = None
    summary: str | None = None
    version: int | None = None
    created_at: datetime


class SprintSummary(BaseSchema):
    """Schema for sprint summary (used in lists and spec responses)."""

    # Count of unanswered Q&A (answered_at IS NULL) — drives the "open Q&A" badge.
    open_qa_count: int | None = Field(
        default=None,
        ge=0,
        exclude_if=lambda value: value is None,
    )
    id: str
    spec_id: str
    board_id: str
    title: str
    description: str | None = None
    objective: str | None = None
    expected_outcome: str | None = None
    status: SprintStatus
    lane_type: SprintLaneType = SprintLaneType.NORMAL
    origin_sprint_id: str | None = None
    origin_bug_id: str | None = None
    normal_sprint_created: bool = False
    spec_version: int
    start_date: datetime | None = None
    end_date: datetime | None = None
    test_scenario_ids: list[str] | None = None
    business_rule_ids: list[str] | None = None
    version: int
    labels: list[str] | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime
    archived: bool = False
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    cancelled_by: str | None = None


class SprintResponse(BaseSchema):
    """Schema for full sprint response."""

    id: str
    spec_id: str
    board_id: str
    title: str
    description: str | None = None
    objective: str | None = None
    expected_outcome: str | None = None
    status: SprintStatus
    lane_type: SprintLaneType = SprintLaneType.NORMAL
    origin_sprint_id: str | None = None
    origin_bug_id: str | None = None
    normal_sprint_created: bool = False
    spec_version: int
    start_date: datetime | None = None
    end_date: datetime | None = None
    test_scenario_ids: list[str] | None = None
    business_rule_ids: list[str] | None = None
    evaluations: list | None = None
    skip_test_coverage: bool = False
    skip_rules_coverage: bool = False
    skip_qualitative_validation: bool = False
    validation_threshold: int | None = None
    require_task_validation: bool | None = None
    validation_min_confidence: int | None = None
    validation_min_completeness: int | None = None
    validation_max_drift: int | None = None
    version: int
    labels: list[str] | None = None
    archived: bool = False
    pre_archive_status: str | None = None
    # Cancellation justification (ITEM 17) — set only while status == cancelled.
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    cancelled_by: str | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime
    cards: list[CardSummaryForSpec] = []
    qa_items: list[SprintQAResponse] = []


# ============================================================================
# Pagination and List Schemas
# ============================================================================


class PaginatedResponse(BaseModel):
    """Generic paginated response."""

    total: int
    offset: int
    limit: int
    items: list[Any]


class ErrorResponse(BaseModel):
    """Error response schema."""

    detail: str
    code: str | None = None


# ============================================================================
# Discovery — intent catalog, saved searches, search history
# ============================================================================


class DiscoveryIntentResponse(BaseModel):
    """Response shape for a single catalog intent."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    label: str
    description: str | None = None
    category: str
    tool_binding: str
    params_schema: DiscoveryParamsSchema | None = None
    renderer: str = "table"
    min_permission: str | None = None
    active: bool = True
    is_seed: bool = False
    created_at: datetime
    updated_at: datetime

    @field_validator("params_schema", mode="before")
    @classmethod
    def _normalize_params_schema(
        cls, value: dict[str, Any] | None
    ) -> DiscoveryParamsSchema | None:
        return normalize_discovery_params_schema(value)


class DiscoverySavedSearchResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    board_id: str
    name: str
    query: str | None = None
    intent_id: str | None = None
    filters_json: dict[str, Any] | None = None
    created_by: str | None = None
    created_at: datetime


class DiscoverySearchHistoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    board_id: str
    user_id: str
    query: str | None = None
    intent_id: str | None = None
    result_count: int = 0
    searched_at: datetime
