"""Validation helpers for ideation scope scores.

The REST and MCP adapters accept different wire types, but the application
contract is one exact integer score from 1 through 5 for each supplied scope
dimension.  Keep the coercion and the machine-readable error envelope here so
no adapter leaks ``ValueError`` or applies a partial scope mutation.
"""

from __future__ import annotations

import re
from typing import Any, Mapping


SCOPE_SCORE_FIELDS = ("domains", "ambiguity", "dependencies")
SCOPE_SCORE_MIN = 1
SCOPE_SCORE_MAX = 5
_ASCII_INTEGER = re.compile(r"[+-]?[0-9]+\Z")


class IdeationScopeValidationError(ValueError):
    """A supplied scope score is not an integer in the supported range."""

    code = "ideation_scope_score_invalid"

    def __init__(self, field: str, value: Any) -> None:
        self.field = field
        self.value = value
        super().__init__(
            f"Scope score '{field}' must be an integer between "
            f"{SCOPE_SCORE_MIN} and {SCOPE_SCORE_MAX}."
        )

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "message": str(self),
            "details": {
                "field": self.field,
                "value": self.value,
                "minimum": SCOPE_SCORE_MIN,
                "maximum": SCOPE_SCORE_MAX,
            },
            "mutation_applied": False,
        }


class IdeationScopeLegacyDataError(IdeationScopeValidationError):
    """An untouched persisted score is invalid and must be corrected explicitly."""

    code = "ideation_scope_legacy_score_invalid"

    def __init__(
        self,
        field: str,
        value: Any,
        *,
        submitted_fields: list[str],
    ) -> None:
        self.submitted_fields = tuple(submitted_fields)
        super().__init__(field, value)
        self.args = (
            (
                f"Persisted scope score '{field}' is outside the supported integer "
                f"range {SCOPE_SCORE_MIN}..{SCOPE_SCORE_MAX}. Include a valid "
                f"'{field}' score in this evaluation request."
            ),
        )

    def to_error_dict(self) -> dict[str, Any]:
        payload = super().to_error_dict()
        payload["details"]["submitted_fields"] = list(self.submitted_fields)
        payload["details"]["remediation"] = (
            f"resubmit_with_valid_{self.field}_score"
        )
        return payload


def coerce_scope_score(field: str, value: Any) -> int:
    """Return a validated score.

    Integer strings are accepted because MCP tool arguments are strings.
    Booleans and integral-looking floats are deliberately rejected: accepting
    either would weaken the public ``integer`` contract.
    """

    score: int
    if isinstance(value, bool):
        raise IdeationScopeValidationError(field, value)
    if isinstance(value, int):
        score = value
    elif isinstance(value, str):
        candidate = value.strip()
        if not _ASCII_INTEGER.fullmatch(candidate):
            raise IdeationScopeValidationError(field, value)
        try:
            score = int(candidate)
        except ValueError as exc:
            raise IdeationScopeValidationError(field, value) from exc
    else:
        raise IdeationScopeValidationError(field, value)

    if not SCOPE_SCORE_MIN <= score <= SCOPE_SCORE_MAX:
        raise IdeationScopeValidationError(field, value)
    return score


def validate_scope_assessment(scope: Mapping[str, Any] | None) -> dict[str, Any]:
    """Copy an assessment and validate every score dimension it contains."""

    validated = dict(scope or {})
    for field in SCOPE_SCORE_FIELDS:
        if field in validated:
            validated[field] = coerce_scope_score(field, validated[field])
    return validated


def merge_scope_assessment(
    current: Mapping[str, Any] | None,
    submitted: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Validate first, then return an atomic merged assessment copy."""

    incoming = dict(submitted or {})
    validated_scores = {
        field: coerce_scope_score(field, incoming[field])
        for field in SCOPE_SCORE_FIELDS
        if field in incoming
    }
    merged = dict(current or {})
    merged.update(validated_scores)
    for field in SCOPE_SCORE_FIELDS:
        justification = f"{field}_justification"
        if justification in incoming:
            merged[justification] = incoming[justification]
    try:
        return validate_scope_assessment(merged)
    except IdeationScopeValidationError as exc:
        if exc.field not in incoming:
            raise IdeationScopeLegacyDataError(
                exc.field,
                exc.value,
                submitted_fields=sorted(incoming),
            ) from exc
        raise
