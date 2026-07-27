"""Agnostic domain enums (status / priority / type / lane / complexity).

Leaf module — stdlib only (``enum``). It imports NO database/session/SQLAlchemy
code, so Pydantic schemas (``models/schemas.py``), the MCP server, services and
workers can read these enums WITHOUT pulling the ORM / ``models/db.py``.
``models/db.py`` (the ORM tables + TypeDecorators), ``models/schemas.py`` and
runtime consumers import from HERE; never the reverse.

R01C FR1 (card 91bf32db): these 12 enums were extracted verbatim from
``models/db.py`` to break the transitive ``schemas.py -> db.py -> sqlalchemy``
import that violated AC1. The serialized string values are FROZEN — they are the
on-the-wire / on-disk contract (DB column values, JSON payloads, MCP enums) and
MUST stay byte-identical to the pre-extraction definitions. See sibling leaf
``amendment_eligibility`` for the same pattern.
"""

from __future__ import annotations

from enum import Enum as PyEnum


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


class StoryStatus(str, PyEnum):
    """Story lifecycle status."""

    DRAFT = "draft"
    TRIAGE = "triage"
    READY = "ready"
    CONVERTED = "converted"


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


class SprintLaneType(str, PyEnum):
    """Sprint lane type for normal delivery and post-closure hotfix work."""

    NORMAL = "normal"
    HOTFIX = "hotfix"


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


class CommentType(str, PyEnum):
    """Comment type enum."""

    TEXT = "text"
    CHOICE = "choice"              # Single-select choice board
    MULTI_CHOICE = "multi_choice"  # Multi-select choice board


__all__ = [
    "IdeationStatus",
    "IdeationComplexity",
    "StoryStatus",
    "RefinementStatus",
    "SprintStatus",
    "SprintLaneType",
    "SpecStatus",
    "CardStatus",
    "CardPriority",
    "CardType",
    "BugSeverity",
    "CommentType",
]
