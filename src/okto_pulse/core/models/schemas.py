"""Pydantic schemas for API request/response models."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from okto_pulse.core.models.db import (
    CardPriority,
    CardStatus,
    IdeationComplexity,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintStatus,
)


# ============================================================================
# Base Schemas
# ============================================================================


class BaseSchema(BaseModel):
    """Base schema with common configuration."""

    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# Agent Schemas
# ============================================================================


class AgentCreate(BaseModel):
    """Schema for creating a new agent."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    objective: str | None = None
    permissions: list[str] | None = None


class AgentUpdate(BaseModel):
    """Schema for updating an agent."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    objective: str | None = None
    is_active: bool | None = None
    permissions: list[str] | None = None


class AgentSelfUpdate(BaseModel):
    """Schema for agent self-updating its own profile."""

    description: str | None = None
    objective: str | None = None


class AgentResponse(BaseSchema):
    """Schema for agent response (global, always includes api_key)."""

    id: str
    name: str
    description: str | None
    objective: str | None = None
    api_key: str
    is_active: bool
    permissions: list[str] | None
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
    created_at: datetime
    last_used_at: datetime | None


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


class ChoiceResponse(BaseModel):
    """A response to a choice board."""

    responder_id: str
    responder_name: str
    selected: list[str] = []   # IDs of selected options
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


class TestScenario(BaseModel):
    """A test scenario linked to acceptance criteria and optionally to tasks."""

    id: str
    title: str
    linked_criteria: list[str] | None = None  # indices or text of acceptance criteria
    scenario_type: str = "integration"  # unit | integration | e2e | manual
    given: str = ""  # precondition
    when: str = ""  # action
    then: str = ""  # expected result
    notes: str | None = None
    status: str = "draft"  # draft | ready | automated | passed | failed
    linked_task_ids: list[str] | None = None  # card IDs that implement/automate this test


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
    linked_requirements: list[str] | None = None  # 0-based FR indices
    linked_task_ids: list[str] | None = None  # Card IDs linked to this rule
    notes: str | None = None


class ApiContract(BaseModel):
    """An API contract describing an endpoint or interaction."""

    id: str
    method: str  # GET, POST, PUT, DELETE, PATCH, TOOL, COMPONENT, EVENT
    path: str
    description: str = ""
    request_body: dict[str, Any] | None = None
    response_success: dict[str, Any] | None = None
    response_errors: list[dict[str, Any]] | None = None
    linked_requirements: list[str] | None = None
    linked_rules: list[str] | None = None
    linked_task_ids: list[str] | None = None  # Card IDs linked to this contract
    notes: str | None = None


# ============================================================================
# Ideation Schemas
# ============================================================================


class IdeationCreate(BaseModel):
    """Schema for creating an ideation."""

    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = None
    problem_statement: str | None = None
    proposed_approach: str | None = None
    scope_assessment: dict | None = None
    complexity: str | None = None
    assignee_id: str | None = None
    labels: list[str] | None = None
    screen_mockups: list[ScreenMockup] | None = None


class IdeationUpdate(BaseModel):
    """Schema for updating an ideation."""

    title: str | None = Field(None, min_length=1, max_length=500)
    description: str | None = None
    problem_statement: str | None = None
    proposed_approach: str | None = None
    scope_assessment: dict | None = None
    complexity: str | None = None
    assignee_id: str | None = None
    labels: list[str] | None = None
    screen_mockups: list[ScreenMockup] | None = None


class IdeationMove(BaseModel):
    """Schema for changing ideation status."""

    status: IdeationStatus


class IdeationSummary(BaseSchema):
    """Schema for ideation summary."""

    id: str
    board_id: str
    title: str
    description: str | None
    problem_statement: str | None
    complexity: IdeationComplexity | None
    status: IdeationStatus
    version: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    archived: bool = False
    pre_archive_status: str | None = None


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
# Refinement Schemas
# ============================================================================


class RefinementCreate(BaseModel):
    """Schema for creating a refinement."""

    ideation_id: str
    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = None
    in_scope: list[str] | None = None
    out_of_scope: list[str] | None = None
    analysis: str | None = None
    decisions: list[str] | None = None
    assignee_id: str | None = None
    labels: list[str] | None = None
    screen_mockups: list[ScreenMockup] | None = None
    # Artifact propagation filters (optional — None = propagate all from parent)
    mockup_ids: list[str] | None = None
    kb_ids: list[str] | None = None


class RefinementUpdate(BaseModel):
    """Schema for updating a refinement."""

    title: str | None = Field(None, min_length=1, max_length=500)
    description: str | None = None
    in_scope: list[str] | None = None
    out_of_scope: list[str] | None = None
    analysis: str | None = None
    decisions: list[str] | None = None
    assignee_id: str | None = None
    labels: list[str] | None = None
    screen_mockups: list[ScreenMockup] | None = None


class RefinementMove(BaseModel):
    """Schema for changing refinement status."""

    status: RefinementStatus


class RefinementSummary(BaseSchema):
    """Schema for refinement summary."""

    id: str
    ideation_id: str
    board_id: str
    title: str
    description: str | None
    status: RefinementStatus
    version: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    archived: bool = False
    pre_archive_status: str | None = None


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
    title: str
    description: str | None
    in_scope: list[str] | None
    out_of_scope: list[str] | None
    analysis: str | None
    decisions: list[str] | None
    labels: list[str] | None
    qa_snapshot: list[dict] | None
    created_by: str
    created_at: datetime


class RefinementSnapshotSummary(BaseSchema):
    """Lightweight snapshot summary for listing."""

    id: str
    version: int
    title: str
    created_by: str
    created_at: datetime


# ============================================================================
# Refinement Knowledge Base Schemas
# ============================================================================


class RefinementKnowledgeCreate(BaseModel):
    """Schema for creating a refinement knowledge base item."""

    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = None
    content: str = Field(..., min_length=1)
    mime_type: str = "text/markdown"


class RefinementKnowledgeResponse(BaseSchema):
    """Full refinement knowledge base item response."""

    id: str
    refinement_id: str
    title: str
    description: str | None
    content: str
    mime_type: str
    created_by: str
    created_at: datetime
    updated_at: datetime


class RefinementKnowledgeSummary(BaseSchema):
    """Lightweight refinement KB summary (without content)."""

    id: str
    refinement_id: str
    title: str
    description: str | None
    mime_type: str
    created_at: datetime


# ============================================================================
# Spec Schemas
# ============================================================================


class SpecCreate(BaseModel):
    """Schema for creating a spec."""

    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = None
    context: str | None = None
    functional_requirements: list[str] | None = None
    technical_requirements: list[str | dict] | None = None  # str (legacy) or {id, text, linked_task_ids}
    acceptance_criteria: list[str] | None = None
    test_scenarios: list[TestScenario] | None = None
    screen_mockups: list[ScreenMockup] | None = None
    business_rules: list[BusinessRule] | None = None
    api_contracts: list[ApiContract] | None = None
    status: SpecStatus = SpecStatus.DRAFT
    assignee_id: str | None = None
    labels: list[str] | None = None
    ideation_id: str | None = None
    refinement_id: str | None = None


class SpecUpdate(BaseModel):
    """Schema for updating a spec."""

    title: str | None = Field(None, min_length=1, max_length=500)
    description: str | None = None
    context: str | None = None
    functional_requirements: list[str] | None = None
    technical_requirements: list[str | dict] | None = None  # str (legacy) or {id, text, linked_task_ids}
    acceptance_criteria: list[str] | None = None
    test_scenarios: list[TestScenario] | None = None
    screen_mockups: list[ScreenMockup] | None = None
    business_rules: list[BusinessRule] | None = None
    api_contracts: list[ApiContract] | None = None
    skip_test_coverage: bool | None = None
    skip_rules_coverage: bool | None = None
    skip_trs_coverage: bool | None = None
    assignee_id: str | None = None
    labels: list[str] | None = None
    ideation_id: str | None = None
    refinement_id: str | None = None


class SpecMove(BaseModel):
    """Schema for changing spec status."""

    status: SpecStatus


class SpecSummary(BaseSchema):
    """Schema for spec summary (without nested cards)."""

    id: str
    board_id: str
    title: str
    description: str | None
    status: SpecStatus
    version: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    ideation_id: str | None = None
    refinement_id: str | None = None
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
    version: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    archived: bool = False
    pre_archive_status: str | None = None
    refinements: list[RefinementSummary] = []
    specs: list[SpecSummary] = []
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
    version: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    archived: bool = False
    pre_archive_status: str | None = None
    specs: list[SpecSummary] = []
    knowledge_bases: list[RefinementKnowledgeSummary] = []
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
# Spec Skill Schemas
# ============================================================================


class SkillSectionSchema(BaseModel):
    """A section within a skill."""

    id: str
    title: str
    description: str = ""
    level: str = "detail"  # summary | detail | full
    content: str = ""


class SpecSkillCreate(BaseModel):
    """Schema for creating a skill on a spec."""

    skill_id: str = Field(..., min_length=1, max_length=255)
    name: str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., min_length=1)
    type: str = "PROMPT"
    version: str = "2.0"
    tags: list[str] | None = None
    sections: list[SkillSectionSchema] | None = None


class SpecSkillUpdate(BaseModel):
    """Schema for updating a skill."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    type: str | None = None
    version: str | None = None
    tags: list[str] | None = None
    sections: list[SkillSectionSchema] | None = None


class SpecSkillResponse(BaseSchema):
    """Full skill response."""

    id: str
    spec_id: str
    skill_id: str
    name: str
    description: str
    type: str
    version: str
    tags: list[str] | None
    sections: list[SkillSectionSchema] | None
    created_by: str
    created_at: datetime
    updated_at: datetime


class SpecSkillSummary(BaseSchema):
    """Lightweight skill summary for RETRIEVE level."""

    skill_id: str
    name: str
    description: str
    type: str
    tags: list[str] | None


# ============================================================================
# Spec Knowledge Base Schemas
# ============================================================================


class SpecKnowledgeCreate(BaseModel):
    """Schema for creating a knowledge base item."""

    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = None
    content: str = Field(..., min_length=1)
    mime_type: str = "text/markdown"


class SpecKnowledgeUpdate(BaseModel):
    """Schema for updating a knowledge base item."""

    title: str | None = Field(None, min_length=1, max_length=500)
    description: str | None = None
    content: str | None = None
    mime_type: str | None = None


class SpecKnowledgeResponse(BaseSchema):
    """Full knowledge base item response."""

    id: str
    spec_id: str
    title: str
    description: str | None
    content: str
    mime_type: str
    created_by: str
    created_at: datetime
    updated_at: datetime


class SpecKnowledgeSummary(BaseSchema):
    """Lightweight KB summary (without content)."""

    id: str
    spec_id: str
    title: str
    description: str | None
    mime_type: str
    created_at: datetime


class CardSummaryForSpec(BaseSchema):
    """Minimal card summary used inside spec responses."""

    id: str
    title: str
    status: CardStatus
    priority: CardPriority
    assignee_id: str | None
    sprint_id: str | None = None


class SpecResponse(BaseSchema):
    """Schema for full spec response."""

    id: str
    board_id: str
    title: str
    description: str | None
    context: str | None
    functional_requirements: list[str] | None
    technical_requirements: list[str | dict] | None  # str (legacy) or {id, text, linked_task_ids}
    acceptance_criteria: list[str] | None
    test_scenarios: list[TestScenario] | None = None
    screen_mockups: list[ScreenMockup] | None = None
    business_rules: list[BusinessRule] | None = None
    api_contracts: list[ApiContract] | None = None
    skip_test_coverage: bool = False
    skip_rules_coverage: bool = False
    skip_trs_coverage: bool = False
    archived: bool = False
    pre_archive_status: str | None = None
    status: SpecStatus
    version: int
    assignee_id: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    labels: list[str] | None
    ideation_id: str | None = None
    refinement_id: str | None = None
    cards: list[CardSummaryForSpec] = []
    skills: list[SpecSkillSummary] = []
    knowledge_bases: list[SpecKnowledgeSummary] = []
    qa_items: list[SpecQAResponse] = []


# ============================================================================
# Card Schemas
# ============================================================================


class CardCreate(BaseModel):
    """Schema for creating a card."""

    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = None
    details: str | None = None
    status: CardStatus = CardStatus.NOT_STARTED
    priority: CardPriority = CardPriority.NONE
    assignee_id: str | None = None
    due_date: datetime | None = None
    labels: list[str] | None = None
    spec_id: str | None = None
    sprint_id: str | None = None
    test_scenario_ids: list[str] | None = None
    screen_mockups: list[ScreenMockup] | None = None
    # Bug card fields
    card_type: str = "normal"  # "normal" or "bug"
    origin_task_id: str | None = None
    severity: str | None = None  # "critical", "major", "minor"
    expected_behavior: str | None = None
    observed_behavior: str | None = None
    steps_to_reproduce: str | None = None
    action_plan: str | None = None


class CardUpdate(BaseModel):
    """Schema for updating a card."""

    title: str | None = Field(None, min_length=1, max_length=500)
    description: str | None = None
    details: str | None = None
    status: CardStatus | None = None
    priority: CardPriority | None = None
    position: int | None = None
    assignee_id: str | None = None
    due_date: datetime | None = None
    labels: list[str] | None = None
    spec_id: str | None = None
    sprint_id: str | None = None
    test_scenario_ids: list[str] | None = None
    screen_mockups: list[ScreenMockup] | None = None
    knowledge_bases: list[dict] | None = None
    # Bug card fields (only updatable, not card_type or origin_task_id)
    severity: str | None = None
    expected_behavior: str | None = None
    observed_behavior: str | None = None
    steps_to_reproduce: str | None = None
    action_plan: str | None = None
    linked_test_task_ids: list[str] | None = None


class ConclusionEntry(BaseModel):
    """A single conclusion entry."""

    text: str
    author_id: str
    created_at: datetime
    completeness: int = 100  # 0-100
    completeness_justification: str = ""
    drift: int = 0  # 0-100
    drift_justification: str = ""


class CardMove(BaseModel):
    """Schema for moving a card between columns."""

    status: CardStatus
    position: int | None = None
    conclusion: str | None = None  # Required when moving to Done
    completeness: int | None = None  # 0-100, required when status=done
    completeness_justification: str | None = None
    drift: int | None = None  # 0-100, required when status=done
    drift_justification: str | None = None


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
    validations: list[dict] | None = None
    archived: bool = False
    pre_archive_status: str | None = None


class CardSummary(BaseSchema):
    """Schema for card summary (without nested items)."""

    id: str
    board_id: str
    spec_id: str | None = None
    sprint_id: str | None = None
    title: str
    description: str | None
    status: CardStatus
    priority: CardPriority
    position: int
    assignee_id: str | None
    created_at: datetime
    updated_at: datetime
    due_date: datetime | None
    labels: list[str] | None
    test_scenario_ids: list[str] | None = None
    # Bug card fields (for kanban display)
    card_type: str = "normal"
    origin_task_id: str | None = None
    severity: str | None = None
    linked_test_task_ids: list[str] | None = None
    archived: bool = False
    pre_archive_status: str | None = None


# ============================================================================
# Task Validation Schemas
# ============================================================================


class TaskValidationSubmit(BaseModel):
    """Request body for submitting a task validation."""

    confidence: int = Field(..., ge=0, le=100)
    confidence_justification: str = Field(..., min_length=10)
    estimated_completeness: int = Field(..., ge=0, le=100)
    completeness_justification: str = Field(..., min_length=10)
    estimated_drift: int = Field(..., ge=0, le=100)
    drift_justification: str = Field(..., min_length=10)
    general_justification: str = Field(..., min_length=20)
    recommendation: str = Field(..., pattern="^(approve|reject)$")


class TaskValidationResponse(BaseModel):
    """Response for a task validation."""

    id: str
    card_id: str
    board_id: str
    reviewer_id: str
    confidence: int
    confidence_justification: str
    estimated_completeness: int
    completeness_justification: str
    estimated_drift: int
    drift_justification: str
    general_justification: str
    recommendation: str
    outcome: str
    threshold_violations: list[str]
    created_at: str
    card_status: str | None = None
    resolved_thresholds: dict | None = None


# ============================================================================
# Spec Validation Gate Schemas
# ============================================================================


class SpecValidationSubmit(BaseModel):
    """Request body for submitting a spec validation.

    Mirrors TaskValidationSubmit but with the 3 spec-specific dimensions:
    completeness, assertiveness, ambiguity (lower is better for ambiguity).
    """

    completeness: int = Field(..., ge=0, le=100)
    completeness_justification: str = Field(..., min_length=10)
    assertiveness: int = Field(..., ge=0, le=100)
    assertiveness_justification: str = Field(..., min_length=10)
    ambiguity: int = Field(..., ge=0, le=100)
    ambiguity_justification: str = Field(..., min_length=10)
    general_justification: str = Field(..., min_length=20)
    recommendation: str = Field(..., pattern="^(approve|reject)$")


class SpecValidationResponse(BaseModel):
    """Response for a spec validation."""

    id: str
    spec_id: str
    board_id: str
    reviewer_id: str
    reviewer_name: str | None = None
    completeness: int
    completeness_justification: str
    assertiveness: int
    assertiveness_justification: str
    ambiguity: int
    ambiguity_justification: str
    general_justification: str
    recommendation: str
    outcome: str
    threshold_violations: list[str]
    resolved_thresholds: dict | None = None
    created_at: str
    spec_status: str | None = None
    active: bool | None = None


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
    """Schema for linking a guideline to a board."""

    guideline_id: str
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


class BoardSettings(BaseModel):
    """Board-level settings for governance rules."""

    max_scenarios_per_card: int = 3  # max test scenarios a single card can be linked to
    skip_test_coverage_global: bool = False  # if True, all specs bypass test coverage checks
    skip_rules_coverage_global: bool = False  # if True, all specs bypass FR→BR coverage checks
    skip_trs_coverage_global: bool = False  # if True, all specs bypass TR→Task coverage checks
    # Task Validation Gate — board-level defaults (overridable at spec/sprint)
    require_task_validation: bool = False  # if True, cards must pass validation before moving to done
    validation_min_confidence: int = 70  # min reviewer confidence score
    validation_min_completeness: int = 80  # min reviewer completeness score
    validation_max_drift: int = 50  # max reviewer drift score


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
    created_at: datetime
    updated_at: datetime
    cards: list[CardResponse] = []
    agents: list[AgentSummary] = []


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
    test_scenario_ids: list[str] | None = None
    business_rule_ids: list[str] | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    labels: list[str] | None = None
    skip_test_coverage: bool | None = None
    skip_rules_coverage: bool | None = None
    skip_qualitative_validation: bool | None = None
    validation_threshold: int | None = None


class SprintMove(BaseModel):
    """Schema for changing sprint status."""

    status: SprintStatus


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

    id: str
    spec_id: str
    board_id: str
    title: str
    description: str | None = None
    objective: str | None = None
    expected_outcome: str | None = None
    status: SprintStatus
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
    version: int
    labels: list[str] | None = None
    archived: bool = False
    pre_archive_status: str | None = None
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
