"""Pure lifecycle-edition rules shared by human validation domains.

``edition`` identifies the human rework cycle.  It is deliberately independent
from the technical ``version``/digest fences used for optimistic concurrency and
audit.  Evidence without an edition predates this contract and is history-only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SUBJECT_EDIT_REQUIRES_DRAFT = "subject_edit_requires_draft"
SUBJECT_LIFECYCLE_TRANSITION_CONFLICT = "subject_lifecycle_transition_conflict"


class SubjectEditRequiresDraftError(ValueError):
    """Stable conflict raised before a non-Draft subject is mutated."""

    code = SUBJECT_EDIT_REQUIRES_DRAFT

    def __init__(self, subject_type: str, subject_id: str, status: str) -> None:
        self.subject_type = str(subject_type)
        self.subject_id = str(subject_id)
        self.status = str(status)
        super().__init__(
            f"{self.code}: {self.subject_type} {self.subject_id} can only be edited "
            f"while in draft (current status: {self.status})"
        )


class LifecycleTransitionConflictError(ValueError):
    """Retryable conflict raised when a lifecycle subject changed mid-command."""

    code = SUBJECT_LIFECYCLE_TRANSITION_CONFLICT

    def __init__(self, subject_type: str, subject_id: str) -> None:
        self.subject_type = str(subject_type)
        self.subject_id = str(subject_id)
        self.details = {
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "next_action": "refresh_and_retry",
        }
        super().__init__(
            f"{self.code}: {self.subject_type} {self.subject_id} changed while "
            "the lifecycle transition was being evaluated; refresh and retry"
        )

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "outcome": "error",
            "error": self.code,
            "code": self.code,
            "error_code": self.code,
            "message": str(self),
            "category": "conflict",
            "retryable": True,
            "next_action": "refresh_and_retry",
            "details": dict(self.details),
        }


def lifecycle_edition(value: Any, *, code: str = "subject_edition_invalid") -> int:
    """Return a positive edition, rejecting bools and inferred legacy values."""

    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(code)
    return value


def next_lifecycle_edition(
    current: Any,
    *,
    from_status: Any,
    to_status: Any,
) -> int:
    """Increment exactly once when a successful transition enters Draft."""

    edition = lifecycle_edition(current)
    before = getattr(from_status, "value", from_status)
    after = getattr(to_status, "value", to_status)
    return edition + 1 if str(before) != "draft" and str(after) == "draft" else edition


def require_draft_mutation(subject: Any, *, subject_type: str) -> None:
    """Fail before writes when a lifecycle subject is outside Draft."""

    status = getattr(getattr(subject, "status", None), "value", getattr(subject, "status", None))
    if str(status) != "draft":
        raise SubjectEditRequiresDraftError(
            subject_type,
            str(getattr(subject, "id", "unknown")),
            str(status),
        )


def is_current_edition(record_edition: Any, current_edition: Any) -> bool:
    """True only for explicit, positive, equal editions; legacy NULL is false."""

    if record_edition is None:
        return False
    try:
        return lifecycle_edition(record_edition) == lifecycle_edition(current_edition)
    except ValueError:
        return False


@dataclass(frozen=True, slots=True)
class HumanValidationCycleRef:
    """Small transport-independent reference for summary/read ports."""

    subject_type: str
    subject_id: str
    edition: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "edition", lifecycle_edition(self.edition))


__all__ = [
    "HumanValidationCycleRef",
    "LifecycleTransitionConflictError",
    "SUBJECT_EDIT_REQUIRES_DRAFT",
    "SUBJECT_LIFECYCLE_TRANSITION_CONFLICT",
    "SubjectEditRequiresDraftError",
    "is_current_edition",
    "lifecycle_edition",
    "next_lifecycle_edition",
    "require_draft_mutation",
]
