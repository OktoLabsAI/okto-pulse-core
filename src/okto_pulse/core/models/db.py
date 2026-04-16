"""SQLAlchemy database models."""

import uuid
from datetime import datetime
from enum import Enum as PyEnum
from typing import TYPE_CHECKING

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from okto_pulse.core.infra.database import Base

if TYPE_CHECKING:
    pass


class IdeationStatus(str, PyEnum):
    """Ideation lifecycle status."""

    DRAFT = "draft"
    REVIEW = "review"
    APPROVED = "approved"
    EVALUATING = "evaluating"
    DONE = "done"
    CANCELLED = "cancelled"


class IdeationComplexity(str, PyEnum):
    """Ideation complexity level — determines whether refinements are needed."""

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class RefinementStatus(str, PyEnum):
    """Refinement lifecycle status."""

    DRAFT = "draft"
    REVIEW = "review"
    APPROVED = "approved"
    DONE = "done"
    CANCELLED = "cancelled"


class SprintStatus(str, PyEnum):
    """Sprint lifecycle status."""

    DRAFT = "draft"
    ACTIVE = "active"
    REVIEW = "review"
    CLOSED = "closed"
    CANCELLED = "cancelled"


class SpecStatus(str, PyEnum):
    """Spec lifecycle status."""

    DRAFT = "draft"
    REVIEW = "review"
    APPROVED = "approved"
    VALIDATED = "validated"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    CANCELLED = "cancelled"


class CardStatus(str, PyEnum):
    """Card status enum matching Kanban columns."""

    NOT_STARTED = "not_started"
    STARTED = "started"
    IN_PROGRESS = "in_progress"
    VALIDATION = "validation"
    ON_HOLD = "on_hold"
    DONE = "done"
    CANCELLED = "cancelled"


class CardPriority(str, PyEnum):
    """Card priority levels."""

    CRITICAL = "critical"
    VERY_HIGH = "very_high"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class CardType(str, PyEnum):
    """Card type enum — normal task, bug, or test."""

    NORMAL = "normal"
    BUG = "bug"
    TEST = "test"


class BugSeverity(str, PyEnum):
    """Bug severity levels."""

    CRITICAL = "critical"
    MAJOR = "major"
    MINOR = "minor"


class CardTypeType(TypeDecorator):
    """SQLAlchemy type that stores CardType as a string."""

    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, CardType) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return CardType(value)


class BugSeverityType(TypeDecorator):
    """SQLAlchemy type that stores BugSeverity as a string."""

    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, BugSeverity) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return BugSeverity(value)


class CardPriorityType(TypeDecorator):
    """SQLAlchemy type that stores CardPriority as a string but returns the enum on load."""

    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, CardPriority) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return CardPriority(value)


class IdeationStatusType(TypeDecorator):
    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, IdeationStatus) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return IdeationStatus(value)


class IdeationComplexityType(TypeDecorator):
    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, IdeationComplexity) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return IdeationComplexity(value)


class RefinementStatusType(TypeDecorator):
    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, RefinementStatus) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return RefinementStatus(value)


class SprintStatusType(TypeDecorator):
    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, SprintStatus) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return SprintStatus(value)


class SpecStatusType(TypeDecorator):
    """SQLAlchemy type that stores SpecStatus as a string but returns the enum on load."""

    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, SpecStatus) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return SpecStatus(value)


class CardStatusType(TypeDecorator):
    """SQLAlchemy type that stores CardStatus as a string but returns the enum on load."""

    impl = String(50)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.value if isinstance(value, CardStatus) else value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return CardStatus(value)


class Board(Base):
    """Board model - represents a Kanban board."""

    __tablename__ = "boards"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    realm_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    # Board settings (JSON): {max_scenarios_per_card: int, skip_test_coverage_global: bool}
    settings: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    cards: Mapped[list["Card"]] = relationship(
        "Card", back_populates="board", cascade="all, delete-orphan"
    )
    ideations: Mapped[list["Ideation"]] = relationship(
        "Ideation", back_populates="board", cascade="all, delete-orphan"
    )
    specs: Mapped[list["Spec"]] = relationship(
        "Spec", back_populates="board", cascade="all, delete-orphan"
    )
    sprints: Mapped[list["Sprint"]] = relationship(
        "Sprint", back_populates="board", cascade="all, delete-orphan"
    )
    agent_grants: Mapped[list["AgentBoard"]] = relationship(
        "AgentBoard", back_populates="board", cascade="all, delete-orphan"
    )
    shares: Mapped[list["BoardShare"]] = relationship(
        "BoardShare", back_populates="board", cascade="all, delete-orphan"
    )


# ============================================================================
# IDEATION
# ============================================================================


class Ideation(Base):
    """Ideation — the starting point of the framework. A raw idea that may be refined into specs."""

    __tablename__ = "ideations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    board_id: Mapped[str] = mapped_column(String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    problem_statement: Mapped[str | None] = mapped_column(Text, nullable=True)
    proposed_approach: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Scope assessment: {"domains": 1-5, "ambiguity": 1-5, "dependencies": 1-5}
    scope_assessment: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    complexity: Mapped[IdeationComplexity | None] = mapped_column(IdeationComplexityType(), nullable=True)
    status: Mapped[IdeationStatus] = mapped_column(IdeationStatusType(), default=IdeationStatus.DRAFT, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    assignee_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    labels: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Screen mockups: [{id, title, description, screen_type, html_content, annotations, order}]
    screen_mockups: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Archive support
    archived: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    pre_archive_status: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Relationships
    board: Mapped["Board"] = relationship("Board", back_populates="ideations")
    refinements: Mapped[list["Refinement"]] = relationship("Refinement", back_populates="ideation", cascade="all, delete-orphan")
    specs: Mapped[list["Spec"]] = relationship("Spec", back_populates="ideation")
    qa_items: Mapped[list["IdeationQAItem"]] = relationship("IdeationQAItem", back_populates="ideation", cascade="all, delete-orphan")
    history: Mapped[list["IdeationHistory"]] = relationship("IdeationHistory", back_populates="ideation", cascade="all, delete-orphan")
    snapshots: Mapped[list["IdeationSnapshot"]] = relationship("IdeationSnapshot", back_populates="ideation", cascade="all, delete-orphan")


class IdeationSnapshot(Base):
    """Immutable snapshot of an ideation at a specific version. Created when status moves to 'done'."""

    __tablename__ = "ideation_snapshots"
    __table_args__ = (
        UniqueConstraint("ideation_id", "version", name="uq_ideation_snapshot_version"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    ideation_id: Mapped[str] = mapped_column(String(36), ForeignKey("ideations.id", ondelete="CASCADE"), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    # Full state capture
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    problem_statement: Mapped[str | None] = mapped_column(Text, nullable=True)
    proposed_approach: Mapped[str | None] = mapped_column(Text, nullable=True)
    scope_assessment: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    complexity: Mapped[str | None] = mapped_column(String(50), nullable=True)
    labels: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Q&A snapshot (list of {question, answer, asked_by, answered_by})
    qa_snapshot: Mapped[list | None] = mapped_column(JSON, nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    ideation: Mapped["Ideation"] = relationship("Ideation", back_populates="snapshots")


class IdeationHistory(Base):
    """Change history for an ideation."""

    __tablename__ = "ideation_history"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    ideation_id: Mapped[str] = mapped_column(String(36), ForeignKey("ideations.id", ondelete="CASCADE"), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(50), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    actor_name: Mapped[str] = mapped_column(String(255), nullable=False)
    changes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    ideation: Mapped["Ideation"] = relationship("Ideation", back_populates="history")


class IdeationQAItem(Base):
    """Q&A on an ideation — same pattern as spec Q&A with text + choice support."""

    __tablename__ = "ideation_qa_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    ideation_id: Mapped[str] = mapped_column(String(36), ForeignKey("ideations.id", ondelete="CASCADE"), nullable=False, index=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    question_type: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'text'"))
    choices: Mapped[list | None] = mapped_column(JSON, nullable=True)
    allow_free_text: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected: Mapped[list | None] = mapped_column(JSON, nullable=True)
    asked_by: Mapped[str] = mapped_column(String(255), nullable=False)
    answered_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    answered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    ideation: Mapped["Ideation"] = relationship("Ideation", back_populates="qa_items")


# ============================================================================
# REFINEMENT
# ============================================================================


class Refinement(Base):
    """Refinement — a focused analysis of one aspect of an ideation."""

    __tablename__ = "refinements"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    ideation_id: Mapped[str] = mapped_column(String(36), ForeignKey("ideations.id", ondelete="CASCADE"), nullable=False, index=True)
    board_id: Mapped[str] = mapped_column(String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    in_scope: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    out_of_scope: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    analysis: Mapped[str | None] = mapped_column(Text, nullable=True)
    decisions: Mapped[list | None] = mapped_column(JSON, nullable=True)
    status: Mapped[RefinementStatus] = mapped_column(RefinementStatusType(), default=RefinementStatus.DRAFT, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    assignee_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    labels: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Screen mockups: [{id, title, description, screen_type, html_content, annotations, order}]
    screen_mockups: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Archive support
    archived: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    pre_archive_status: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Relationships
    ideation: Mapped["Ideation"] = relationship("Ideation", back_populates="refinements")
    specs: Mapped[list["Spec"]] = relationship("Spec", back_populates="refinement")
    qa_items: Mapped[list["RefinementQAItem"]] = relationship("RefinementQAItem", back_populates="refinement", cascade="all, delete-orphan")
    history: Mapped[list["RefinementHistory"]] = relationship("RefinementHistory", back_populates="refinement", cascade="all, delete-orphan")
    snapshots: Mapped[list["RefinementSnapshot"]] = relationship("RefinementSnapshot", back_populates="refinement", cascade="all, delete-orphan")
    knowledge_bases: Mapped[list["RefinementKnowledgeBase"]] = relationship("RefinementKnowledgeBase", back_populates="refinement", cascade="all, delete-orphan")


class RefinementSnapshot(Base):
    """Immutable snapshot of a refinement at a specific version. Created when status moves to 'done'."""

    __tablename__ = "refinement_snapshots"
    __table_args__ = (
        UniqueConstraint("refinement_id", "version", name="uq_refinement_snapshot_version"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    refinement_id: Mapped[str] = mapped_column(String(36), ForeignKey("refinements.id", ondelete="CASCADE"), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    in_scope: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    out_of_scope: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    analysis: Mapped[str | None] = mapped_column(Text, nullable=True)
    decisions: Mapped[list | None] = mapped_column(JSON, nullable=True)
    labels: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    qa_snapshot: Mapped[list | None] = mapped_column(JSON, nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    refinement: Mapped["Refinement"] = relationship("Refinement", back_populates="snapshots")


class RefinementKnowledgeBase(Base):
    """Knowledge base item attached to a refinement."""

    __tablename__ = "refinement_knowledge_bases"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    refinement_id: Mapped[str] = mapped_column(String(36), ForeignKey("refinements.id", ondelete="CASCADE"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    mime_type: Mapped[str] = mapped_column(String(100), nullable=False, default="text/markdown")
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    refinement: Mapped["Refinement"] = relationship("Refinement", back_populates="knowledge_bases")


class RefinementHistory(Base):
    """Change history for a refinement."""

    __tablename__ = "refinement_history"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    refinement_id: Mapped[str] = mapped_column(String(36), ForeignKey("refinements.id", ondelete="CASCADE"), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(50), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    actor_name: Mapped[str] = mapped_column(String(255), nullable=False)
    changes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    refinement: Mapped["Refinement"] = relationship("Refinement", back_populates="history")


class RefinementQAItem(Base):
    """Q&A on a refinement — same pattern with text + choice support."""

    __tablename__ = "refinement_qa_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    refinement_id: Mapped[str] = mapped_column(String(36), ForeignKey("refinements.id", ondelete="CASCADE"), nullable=False, index=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    question_type: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'text'"))
    choices: Mapped[list | None] = mapped_column(JSON, nullable=True)
    allow_free_text: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected: Mapped[list | None] = mapped_column(JSON, nullable=True)
    asked_by: Mapped[str] = mapped_column(String(255), nullable=False)
    answered_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    answered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    refinement: Mapped["Refinement"] = relationship("Refinement", back_populates="qa_items")


# ============================================================================
# SPEC
# ============================================================================


class Spec(Base):
    """Spec model - represents a specification that drives card creation."""

    __tablename__ = "specs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    board_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    ideation_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("ideations.id", ondelete="SET NULL"), nullable=True, index=True
    )
    refinement_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("refinements.id", ondelete="SET NULL"), nullable=True, index=True
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    context: Mapped[str | None] = mapped_column(Text, nullable=True)
    functional_requirements: Mapped[list | None] = mapped_column(JSON, nullable=True)
    technical_requirements: Mapped[list | None] = mapped_column(JSON, nullable=True)
    acceptance_criteria: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Test scenarios: [{id, title, linked_criteria, scenario_type, given, when, then, notes, status, linked_task_ids}]
    test_scenarios: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Screen mockups: [{id, title, description, screen_type, html_content, annotations, order}]
    screen_mockups: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Business rules: [{id, title, rule, when, then, linked_requirements, notes}]
    business_rules: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # API contracts: [{id, method, path, description, request_body, response_success, response_errors, linked_requirements, linked_rules, notes}]
    api_contracts: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # If true, spec can move to Done without full test coverage — set by user only
    skip_test_coverage: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    # If true, cards can start without full FR→BR coverage — set by user only
    skip_rules_coverage: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    # If true, cards can start without full TR→Task coverage
    skip_trs_coverage: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    # If true, spec can move to validated without full API contract coverage
    skip_contract_coverage: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    # If true, spec can skip qualitative validation (validated→in_progress without evaluations)
    skip_qualitative_validation: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    # Minimum avg score for qualitative validation (None = use board or default 70)
    validation_threshold: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Task validation gate: when True, cards must pass through "validation" before "done"
    require_task_validation: Mapped[bool | None] = mapped_column(nullable=True)
    # Threshold overrides for task validation (null = inherit from board)
    validation_min_confidence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    validation_min_completeness: Mapped[int | None] = mapped_column(Integer, nullable=True)
    validation_max_drift: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Qualitative evaluations: [{id, evaluator_id, evaluator_name, evaluator_type, dimensions, overall_score, overall_justification, recommendation, stale, created_at}]
    evaluations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Spec Validation Gate — append-only history of validation records.
    # Each record: {id, spec_id, board_id, reviewer_id, reviewer_name,
    #  completeness, completeness_justification, assertiveness, assertiveness_justification,
    #  ambiguity, ambiguity_justification, general_justification, recommendation,
    #  outcome, threshold_violations, resolved_thresholds, created_at}
    validations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Pointer to the current active validation id — NULL when cleared by backward move.
    # Content lock is ACTIVE when this is non-NULL and the pointed record has outcome='success'.
    current_validation_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # Archive support
    archived: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    pre_archive_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status: Mapped[SpecStatus] = mapped_column(
        SpecStatusType(), default=SpecStatus.DRAFT, nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    assignee_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    labels: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    # Relationships
    board: Mapped["Board"] = relationship("Board", back_populates="specs")
    ideation: Mapped["Ideation | None"] = relationship("Ideation", back_populates="specs")
    refinement: Mapped["Refinement | None"] = relationship("Refinement", back_populates="specs")
    cards: Mapped[list["Card"]] = relationship("Card", back_populates="spec")
    sprints: Mapped[list["Sprint"]] = relationship("Sprint", back_populates="spec", cascade="all, delete-orphan")
    skills: Mapped[list["SpecSkill"]] = relationship(
        "SpecSkill", back_populates="spec", cascade="all, delete-orphan"
    )
    knowledge_bases: Mapped[list["SpecKnowledgeBase"]] = relationship(
        "SpecKnowledgeBase", back_populates="spec", cascade="all, delete-orphan"
    )
    qa_items: Mapped[list["SpecQAItem"]] = relationship(
        "SpecQAItem", back_populates="spec", cascade="all, delete-orphan"
    )
    history: Mapped[list["SpecHistory"]] = relationship(
        "SpecHistory", back_populates="spec", cascade="all, delete-orphan"
    )


class SpecHistory(Base):
    """Detailed change history for a spec — tracks every modification with field-level diffs."""

    __tablename__ = "spec_history"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    spec_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("specs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    # e.g. "created", "updated", "status_changed", "cards_derived",
    #      "skill_added", "skill_removed", "knowledge_added", "knowledge_removed",
    #      "qa_added", "qa_answered"
    actor_type: Mapped[str] = mapped_column(String(50), nullable=False)  # "user" | "agent"
    actor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    actor_name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Field-level changes: [{"field": "title", "old": "...", "new": "..."}, ...]
    changes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Optional summary/description of the change
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Version of the spec at this point
    version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    spec: Mapped["Spec"] = relationship("Spec", back_populates="history")


class SpecQAItem(Base):
    """Q&A item on a spec — bidirectional communication between humans and agents during spec refinement.

    Supports three question types:
    - text: Free-text question with free-text answer (default)
    - choice: Single-select question with predefined options
    - multi_choice: Multi-select question with predefined options

    For choice/multi_choice, the answer is stored as JSON with selected option IDs.
    """

    __tablename__ = "spec_qa_items"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    spec_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("specs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    question: Mapped[str] = mapped_column(Text, nullable=False)
    question_type: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'text'")
    )  # "text" | "choice" | "multi_choice"
    # Format: [{"id": "opt_1", "label": "Option A"}, ...]
    choices: Mapped[list | None] = mapped_column(JSON, nullable=True)
    allow_free_text: Mapped[bool] = mapped_column(
        nullable=False, server_default=text("false")
    )
    answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    # For choice answers: ["opt_1", "opt_2"]
    selected: Mapped[list | None] = mapped_column(JSON, nullable=True)
    asked_by: Mapped[str] = mapped_column(String(255), nullable=False)
    answered_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    answered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    spec: Mapped["Spec"] = relationship("Spec", back_populates="qa_items")


class SpecSkill(Base):
    """Skill attached to a spec — structured instructions for AI agents.

    Follows the 3-level loading pattern: RETRIEVE (catalog), INSPECT (index), LOAD (content).
    Sections are stored as JSON: [{"id": "...", "title": "...", "description": "...", "level": "...", "content": "..."}]
    """

    __tablename__ = "spec_skills"
    __table_args__ = (
        UniqueConstraint("spec_id", "skill_id", name="uq_spec_skill"),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    spec_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("specs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    skill_id: Mapped[str] = mapped_column(String(255), nullable=False)  # Slug identifier
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False, default="PROMPT")
    version: Mapped[str] = mapped_column(String(20), nullable=False, default="2.0")
    tags: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    sections: Mapped[list | None] = mapped_column(JSON, nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    spec: Mapped["Spec"] = relationship("Spec", back_populates="skills")


class SpecKnowledgeBase(Base):
    """Knowledge base item attached to a spec — reference documents and context for AI agents."""

    __tablename__ = "spec_knowledge_bases"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    spec_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("specs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    mime_type: Mapped[str] = mapped_column(String(100), nullable=False, default="text/markdown")
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    spec: Mapped["Spec"] = relationship("Spec", back_populates="knowledge_bases")


# ============================================================================
# SPRINT
# ============================================================================


class Sprint(Base):
    """Sprint — an incremental delivery slice of a spec."""

    __tablename__ = "sprints"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    spec_id: Mapped[str] = mapped_column(String(36), ForeignKey("specs.id", ondelete="CASCADE"), nullable=False, index=True)
    board_id: Mapped[str] = mapped_column(String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    spec_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[SprintStatus] = mapped_column(SprintStatusType(), default=SprintStatus.DRAFT, nullable=False)
    # Dates
    start_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Sprint-specific fields
    objective: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_outcome: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Scoped test scenario IDs from spec
    test_scenario_ids: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Scoped business rule IDs from spec
    business_rule_ids: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Qualitative evaluations: [{id, evaluator_id, evaluator_name, evaluator_type, dimensions, overall_score, overall_justification, recommendation, stale, created_at}]
    evaluations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Skip flags (same pattern as Spec)
    skip_test_coverage: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    skip_rules_coverage: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    skip_qualitative_validation: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    validation_threshold: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Task validation gate override (null = inherit from spec/board)
    require_task_validation: Mapped[bool | None] = mapped_column(nullable=True)
    validation_min_confidence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    validation_min_completeness: Mapped[int | None] = mapped_column(Integer, nullable=True)
    validation_max_drift: Mapped[int | None] = mapped_column(Integer, nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    labels: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    archived: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    pre_archive_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    spec: Mapped["Spec"] = relationship("Spec", back_populates="sprints")
    board: Mapped["Board"] = relationship("Board", back_populates="sprints")
    cards: Mapped[list["Card"]] = relationship("Card", back_populates="sprint")
    qa_items: Mapped[list["SprintQAItem"]] = relationship("SprintQAItem", back_populates="sprint", cascade="all, delete-orphan")
    history: Mapped[list["SprintHistory"]] = relationship("SprintHistory", back_populates="sprint", cascade="all, delete-orphan")


class SprintHistory(Base):
    """Change history for a sprint."""

    __tablename__ = "sprint_history"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    sprint_id: Mapped[str] = mapped_column(String(36), ForeignKey("sprints.id", ondelete="CASCADE"), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(50), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    actor_name: Mapped[str] = mapped_column(String(255), nullable=False)
    changes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    sprint: Mapped["Sprint"] = relationship("Sprint", back_populates="history")


class SprintQAItem(Base):
    """Q&A on a sprint — same pattern as spec/ideation/refinement Q&A."""

    __tablename__ = "sprint_qa_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    sprint_id: Mapped[str] = mapped_column(String(36), ForeignKey("sprints.id", ondelete="CASCADE"), nullable=False, index=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    question_type: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'text'"))
    choices: Mapped[list | None] = mapped_column(JSON, nullable=True)
    allow_free_text: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected: Mapped[list | None] = mapped_column(JSON, nullable=True)
    asked_by: Mapped[str] = mapped_column(String(255), nullable=False)
    answered_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    answered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    sprint: Mapped["Sprint"] = relationship("Sprint", back_populates="qa_items")


# ============================================================================
# CARD
# ============================================================================


class Card(Base):
    """Card model - represents a task/item in the Kanban board."""

    __tablename__ = "cards"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    board_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    spec_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("specs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    sprint_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("sprints.id", ondelete="SET NULL"), nullable=True, index=True
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)  # Rich text/HTML
    status: Mapped[CardStatus] = mapped_column(
        CardStatusType(), default=CardStatus.NOT_STARTED, nullable=False
    )
    priority: Mapped[CardPriority] = mapped_column(
        CardPriorityType(), default=CardPriority.NONE, nullable=False, server_default="none"
    )
    position: Mapped[int] = mapped_column(Integer, default=0)  # Order within column
    assignee_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    due_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    labels: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Test scenario IDs from the linked spec that this card addresses
    test_scenario_ids: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Conclusions: [{text, author_id, created_at}] — required when moving to Done
    conclusions: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Screen mockups: [{id, title, description, screen_type, html_content, annotations, order}]
    screen_mockups: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Knowledge bases: [{id, title, description, content, mime_type, source}]
    knowledge_bases: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Task validations: [{id, card_id, board_id, reviewer_id, confidence, confidence_justification,
    # estimated_completeness, completeness_justification, estimated_drift, drift_justification,
    # general_justification, recommendation, outcome, threshold_violations, created_at}]
    validations: Mapped[list | None] = mapped_column(JSON, nullable=True)

    # --- Bug card fields ---
    card_type: Mapped[CardType] = mapped_column(
        CardTypeType(), default=CardType.NORMAL, nullable=False, server_default="normal"
    )
    # ID of the task that originated this bug (required when card_type=bug)
    origin_task_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("cards.id", ondelete="SET NULL"), nullable=True
    )
    severity: Mapped[BugSeverity | None] = mapped_column(
        BugSeverityType(), nullable=True
    )
    expected_behavior: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_behavior: Mapped[str | None] = mapped_column(Text, nullable=True)
    steps_to_reproduce: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_plan: Mapped[str | None] = mapped_column(Text, nullable=True)
    # IDs of test task cards linked to this bug for unblocking
    linked_test_task_ids: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Archive support
    archived: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    pre_archive_status: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Relationships
    board: Mapped["Board"] = relationship("Board", back_populates="cards")
    spec: Mapped["Spec | None"] = relationship("Spec", back_populates="cards")
    sprint: Mapped["Sprint | None"] = relationship("Sprint", back_populates="cards")
    attachments: Mapped[list["Attachment"]] = relationship(
        "Attachment", back_populates="card", cascade="all, delete-orphan"
    )
    qa_items: Mapped[list["QAItem"]] = relationship(
        "QAItem", back_populates="card", cascade="all, delete-orphan"
    )
    comments: Mapped[list["Comment"]] = relationship(
        "Comment", back_populates="card", cascade="all, delete-orphan"
    )
    # Dependencies: cards this card depends on
    dependencies: Mapped[list["CardDependency"]] = relationship(
        "CardDependency",
        foreign_keys="CardDependency.card_id",
        back_populates="card",
        cascade="all, delete-orphan",
    )
    # Dependents: cards that depend on this card
    dependents: Mapped[list["CardDependency"]] = relationship(
        "CardDependency",
        foreign_keys="CardDependency.depends_on_id",
        back_populates="depends_on",
        cascade="all, delete-orphan",
    )


class CardDependency(Base):
    """Junction table for card dependencies."""

    __tablename__ = "card_dependencies"
    __table_args__ = (
        UniqueConstraint("card_id", "depends_on_id", name="uq_card_dependency"),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    card_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("cards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    depends_on_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("cards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    card: Mapped["Card"] = relationship(
        "Card", foreign_keys=[card_id], back_populates="dependencies"
    )
    depends_on: Mapped["Card"] = relationship(
        "Card", foreign_keys=[depends_on_id], back_populates="dependents"
    )


class Attachment(Base):
    """Attachment model - files attached to cards."""

    __tablename__ = "attachments"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    card_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("cards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(500), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(100), nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False)  # bytes
    path: Mapped[str] = mapped_column(String(1000), nullable=False)  # Storage path
    uploaded_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    card: Mapped["Card"] = relationship("Card", back_populates="attachments")


class QAItem(Base):
    """Q&A item model - questions and answers within a card."""

    __tablename__ = "qa_items"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    card_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("cards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    question: Mapped[str] = mapped_column(Text, nullable=False)
    answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    asked_by: Mapped[str] = mapped_column(String(255), nullable=False)
    answered_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    answered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    card: Mapped["Card"] = relationship("Card", back_populates="qa_items")


class CommentType(str, PyEnum):
    """Comment type enum."""

    TEXT = "text"
    CHOICE = "choice"              # Single-select choice board
    MULTI_CHOICE = "multi_choice"  # Multi-select choice board


class Comment(Base):
    """Comment model - comments on cards.

    Supports three types:
    - text: Free-text comment (default, backward compatible)
    - choice: Single-select choice board (poll)
    - multi_choice: Multi-select choice board
    """

    __tablename__ = "comments"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    card_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("cards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    author_id: Mapped[str] = mapped_column(String(255), nullable=False)
    comment_type: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'text'")
    )
    # Choice board data (null for text comments)
    # Format: [{"id": "opt_1", "label": "Option A"}, ...]
    choices: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Responses to choice boards
    # Format: [{"responder_id": "...", "responder_name": "...", "selected": ["opt_1"], "free_text": ""}]
    responses: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # Whether the choice board accepts a free-text response in addition to selections
    allow_free_text: Mapped[bool] = mapped_column(
        nullable=False, server_default=text("false")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    card: Mapped["Card"] = relationship("Card", back_populates="comments")


class Agent(Base):
    """Agent model - AI agents with API keys for MCP access."""

    __tablename__ = "agents"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    board_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="SET NULL"), nullable=True, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    objective: Mapped[str | None] = mapped_column(Text, nullable=True)
    api_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    api_key_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True)
    permissions: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    # Granular permission flags (new system) — JSON dict with nested flags
    permission_flags: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # Preset ID — FK to permission_presets (nullable, agent may have custom flags without preset)
    preset_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    board_grants: Mapped[list["AgentBoard"]] = relationship(
        "AgentBoard", back_populates="agent", cascade="all, delete-orphan"
    )


class AgentBoard(Base):
    """Junction table for agent-board access (N:N)."""

    __tablename__ = "agent_boards"
    __table_args__ = (UniqueConstraint("agent_id", "board_id", name="uq_agent_board"),)

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    agent_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("agents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    board_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    granted_by: Mapped[str] = mapped_column(String(255), nullable=False)
    granted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    # Board-scoped permission overrides (AND with agent flags — can only restrict)
    permission_overrides: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Relationships
    agent: Mapped["Agent"] = relationship("Agent", back_populates="board_grants")
    board: Mapped["Board"] = relationship("Board", back_populates="agent_grants")


class PermissionPreset(Base):
    """Permission preset — reusable set of permission flags."""

    __tablename__ = "permission_presets"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    owner_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_builtin: Mapped[bool] = mapped_column(default=False)
    base_preset_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    flags: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class AgentSeenItem(Base):
    """Tracks which items an agent has marked as seen."""

    __tablename__ = "agent_seen_items"
    __table_args__ = (
        UniqueConstraint("agent_id", "item_type", "item_id", name="uq_agent_seen_item"),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    agent_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("agents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    item_type: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # "comment", "qa", "activity", "card"
    item_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class BoardShare(Base):
    """Board sharing - grants other users access to a board."""

    __tablename__ = "board_shares"
    __table_args__ = (UniqueConstraint("board_id", "user_id", name="uq_board_share"),)

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    board_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    realm_id: Mapped[str] = mapped_column(String(255), nullable=False)
    permission: Mapped[str] = mapped_column(
        String(50), nullable=False, default="viewer"
    )  # "viewer" | "editor" | "admin"
    shared_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    board: Mapped["Board"] = relationship("Board", back_populates="shares")


class Guideline(Base):
    """Reusable guideline — can be global or board-scoped."""

    __tablename__ = "guidelines"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    tags: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    scope: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'global'")
    )
    board_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=True, index=True
    )
    owner_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    board_links: Mapped[list["BoardGuideline"]] = relationship(
        "BoardGuideline", back_populates="guideline", cascade="all, delete-orphan"
    )


class BoardGuideline(Base):
    """Association table linking guidelines to boards."""

    __tablename__ = "board_guidelines"
    __table_args__ = (UniqueConstraint("board_id", "guideline_id", name="uq_board_guideline"),)

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    board_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="CASCADE"), nullable=False, index=True
    )
    guideline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("guidelines.id", ondelete="CASCADE"), nullable=False, index=True
    )
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    board: Mapped["Board"] = relationship("Board")
    guideline: Mapped["Guideline"] = relationship("Guideline", back_populates="board_links")


class ActivityLog(Base):
    """Activity log for board actions."""

    __tablename__ = "activity_logs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    board_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    card_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(50), nullable=False)  # "user" or "agent"
    actor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    actor_name: Mapped[str] = mapped_column(String(255), nullable=False)
    details: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


# ---------------------------------------------------------------------------
# Knowledge Graph Foundation (MVP Fase 0)
# ---------------------------------------------------------------------------
# Four operational tables that bridge SQLite state to the per-board Kùzu
# graphs: consolidation_queue (pending triggers), consolidation_audit (session
# history + undo), kuzu_node_refs (back-references for compensating delete),
# global_update_outbox (transactional outbox for the global discovery layer).


class ConsolidationQueue(Base):
    """Pending consolidation triggers — populated by state transitions,
    consumed by the agent on-demand via the primitives MCP."""

    __tablename__ = "consolidation_queue"
    __table_args__ = (
        UniqueConstraint(
            "board_id", "artifact_type", "artifact_id",
            name="uq_queue_board_artifact",
        ),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    board_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    artifact_type: Mapped[str] = mapped_column(String(50), nullable=False)
    artifact_id: Mapped[str] = mapped_column(String(36), nullable=False)
    priority: Mapped[str] = mapped_column(
        String(10), nullable=False, default="high"
    )  # "high" (runtime trigger) | "low" (historical backfill)
    source: Mapped[str] = mapped_column(
        String(50), nullable=False, default="state_transition"
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True,
    )  # pending | claimed | done | paused | failed
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    triggered_by_event: Mapped[str | None] = mapped_column(
        String(100), nullable=True
    )
    claimed_by_session_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True
    )
    claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class ConsolidationAudit(Base):
    """Per-session audit trail — primary log of every consolidation commit.
    session_id is the PK because everything else (kuzu_node_refs, undo chain)
    joins back here."""

    __tablename__ = "consolidation_audit"

    session_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    board_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("boards.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    artifact_id: Mapped[str] = mapped_column(String(36), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(50), nullable=False)
    agent_id: Mapped[str] = mapped_column(String(255), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    committed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True,
    )
    nodes_added: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    nodes_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    nodes_superseded: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    edges_added: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    summary_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    undo_status: Mapped[str] = mapped_column(
        String(20), default="none", nullable=False
    )  # none | undone | undo_blocked
    undone_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_details: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class KuzuNodeRef(Base):
    """Back-reference from SQLite to Kùzu nodes created by a session.
    Powers compensating delete on abort and undo on demand."""

    __tablename__ = "kuzu_node_refs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    session_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("consolidation_audit.session_id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    board_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    kuzu_node_id: Mapped[str] = mapped_column(String(64), nullable=False)
    kuzu_node_type: Mapped[str] = mapped_column(String(50), nullable=False)
    operation: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # add | update | supersede
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class GlobalUpdateOutbox(Base):
    """Transactional outbox for the global discovery layer sync worker.
    Events are INSERTed in the same SQLite transaction as the audit row;
    a background worker later drains them into the global Kùzu meta-graph
    with retry + dead-letter semantics."""

    __tablename__ = "global_update_outbox"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    event_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    board_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True,
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
