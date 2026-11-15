"""One completion predicate for planning questions and bounded diagnostics."""

from typing import Any, Mapping

from okto_pulse.core.domain.human_validation_cycle import require_draft_mutation

PENDING_QA_LIMIT = 50


def planning_qa_transition_requires_answers(subject_type: str, to_status: Any) -> bool:
    status = getattr(to_status, "value", to_status)
    return status in {
        "ideation": {"approved", "evaluating", "done"},
        "refinement": {"approved", "done"},
    }.get(subject_type, set())


def _value(row: Any, name: str, default: Any = None) -> Any:
    return (
        row.get(name, default)
        if isinstance(row, Mapping)
        else getattr(row, name, default)
    )


def qa_is_pending(qa: Any) -> bool:
    # Text is optional for choice answers. A proposed answer without the native
    # answer timestamp does not resolve a question or authorize a transition.
    return _value(qa, "answered_at") is None


def pending_qa_projection(
    *,
    subject_type: str,
    subject_id: str,
    status: Any,
    pending_ids: list[str],
    total: int,
) -> dict[str, Any]:
    historical = getattr(status, "value", status) == "done" and total > 0
    return {
        "contract_version": "planning-qa-readiness/v1",
        "subject_type": subject_type,
        "subject_id": subject_id,
        "state": "historical_source_incomplete"
        if historical
        else "pending"
        if total
        else "ready",
        "pending_count": total,
        "pending_ids": pending_ids[:PENDING_QA_LIMIT],
        "truncated": total > min(len(pending_ids), PENDING_QA_LIMIT),
        "next_action": (
            "Reopen the source as a new edition and answer its pending questions before deriving. Historical snapshots remain unchanged."
            if historical
            else "Answer the pending questions in Draft or Review before approval, finalization or derivation."
            if total
            else None
        ),
    }


def planning_qa_readiness(subject: Any, *, subject_type: str) -> dict[str, Any]:
    ids = sorted(
        str(_value(qa, "id", ""))
        for qa in (_value(subject, "qa_items", []) or [])
        if qa_is_pending(qa)
    )
    return pending_qa_projection(
        subject_type=subject_type,
        subject_id=str(_value(subject, "id", "")),
        status=_value(subject, "status"),
        pending_ids=ids,
        total=len(ids),
    )


class PendingQuestionsError(ValueError):
    code = "planning_pending_questions"

    def __init__(self, details: dict[str, Any]) -> None:
        self.details = details
        super().__init__(
            f"{details['pending_count']} unanswered Q&A item(s) prevent this action. {details['next_action']}"
        )

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "message": str(self),
            "retryable": False,
            "details": dict(self.details),
        }


def require_planning_qa_answer(subject: Any, *, subject_type: str) -> None:
    status = _value(subject, "status")
    if (
        subject_type in {"ideation", "refinement"}
        and getattr(status, "value", status) == "review"
    ):
        return
    require_draft_mutation(subject, subject_type=subject_type)
