"""Stable transport-neutral projection for Draft-only mutation conflicts."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.human_validation_cycle import (
    SUBJECT_EDIT_REQUIRES_DRAFT,
    SubjectEditRequiresDraftError,
)


def project_subject_edit_requires_draft_error(
    error: SubjectEditRequiresDraftError,
) -> dict[str, Any]:
    """Project the same 409 semantics for REST and MCP adapters.

    Callers must branch by exception type, never by parsing ``str(error)``.
    """

    if not isinstance(error, SubjectEditRequiresDraftError):
        raise TypeError("subject_edit_requires_draft_error_required")
    return {
        "outcome": "error",
        "error": SUBJECT_EDIT_REQUIRES_DRAFT,
        "code": SUBJECT_EDIT_REQUIRES_DRAFT,
        "error_code": SUBJECT_EDIT_REQUIRES_DRAFT,
        "message": (
            f"{error.subject_type} {error.subject_id} can only be edited "
            f"while in draft (current status: {error.status})"
        ),
        "category": "conflict",
        "retryable": False,
        "next_action": "move_subject_to_draft",
        "details": {
            "subject_type": error.subject_type,
            "subject_id": error.subject_id,
            "current_status": error.status,
            "required_status": "draft",
        },
    }


__all__ = ["project_subject_edit_requires_draft_error"]
