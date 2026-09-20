"""Pure historical-context triage; never infer resolution from prose or scores.

These concerns block an internal migration until an explicit, provenance-bound
disposition exists. They neither approve content nor introduce product gates.
"""

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True, slots=True)
class RetirementContextConcern:
    path: tuple[str | int, ...]
    reason: str


def _text(value: object) -> bool:
    if value is not None and type(value) is not str:
        raise ValueError("retirement_context_text_invalid")
    return bool(value and value.strip())


def inspect_retirement_context(kind: str, facts: Mapping[str, object]) -> tuple[RetirementContextConcern, ...]:
    """Identify possible substantive content without interpreting its meaning.

    Sprint evaluations have no per-finding resolution contract. Closed/stale/
    approve, or a score, cannot make their justification safe to discard. An
    answered Q&A can still hold a decision; only the unanswered label follows
    the original answered_at-is-null rule, including choice-only answers.
    """
    if kind == "sprint":
        concerns = [RetirementContextConcern((name,), "text_requires_disposition")
            for name in ("description", "objective", "expected_outcome") if _text(facts[name])]
        evaluations = facts["evaluations"]
        if evaluations is not None:
            if type(evaluations) is not list or any(type(value) is not dict for value in evaluations):
                raise ValueError("retirement_context_evaluations_invalid")
            concerns.extend(RetirementContextConcern(("evaluations", index), "evaluation_requires_disposition")
                for index in range(len(evaluations)))
        return tuple(concerns)
    if kind == "qa":
        if not _text(facts["question"]):
            raise ValueError("retirement_context_question_invalid")
        return (RetirementContextConcern((), "unanswered_question" if facts["answered_at"] is None
            else "answered_question_requires_disposition"),)
    if kind == "history":
        changes = facts["changes"]
        if changes is not None and type(changes) not in (list, dict):
            raise ValueError("retirement_context_history_invalid")
        return (RetirementContextConcern((), "history_requires_disposition"),) if _text(facts["summary"]) or changes else ()
    raise ValueError("retirement_context_kind_invalid")


__all__ = ["RetirementContextConcern", "inspect_retirement_context"]
