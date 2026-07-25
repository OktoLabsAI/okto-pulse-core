"""Typed validation for Q&A and comment choice selections."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


class QASelectionError(ValueError):
    """A selection cannot be applied to the target choice question."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        selected: Iterable[str],
        allowed: Iterable[str],
    ) -> None:
        self.code = code
        self.selected = tuple(selected)
        self.allowed = tuple(allowed)
        super().__init__(message)

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "message": str(self),
            "details": {
                "selected": list(self.selected),
                "allowed_option_ids": list(self.allowed),
            },
            "mutation_applied": False,
        }


def validate_choice_selection(
    question_type: str,
    selected: Iterable[str],
    choices: Iterable[Mapping[str, Any]] | None,
) -> list[str]:
    """Validate option membership and single-choice cardinality."""

    values = list(selected)
    allowed = {
        str(choice.get("id"))
        for choice in (choices or ())
        if choice.get("id") is not None
    }
    invalid = [value for value in values if value not in allowed]
    if invalid:
        raise QASelectionError(
            "invalid_qa_selection",
            f"Unknown choice option id(s): {invalid}.",
            selected=values,
            allowed=sorted(allowed),
        )
    if question_type in {"choice", "single_choice"} and len(values) > 1:
        raise QASelectionError(
            "single_choice_multiple_selection",
            "A single-choice question accepts exactly one selected option.",
            selected=values,
            allowed=sorted(allowed),
        )
    return values


__all__ = ["QASelectionError", "validate_choice_selection"]
