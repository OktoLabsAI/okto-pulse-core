"""Validation of native MCP arrays and structured error envelopes."""

from __future__ import annotations

import json
from typing import NotRequired

from pydantic import ConfigDict, StrictBool, StrictStr
from typing_extensions import TypedDict


class ChoiceOptionInput(TypedDict):
    """A choice authored in the current MCP contract."""

    __pydantic_config__ = ConfigDict(extra="forbid")
    label: StrictStr
    recommended: NotRequired[StrictBool]
    tradeoff: NotRequired[StrictStr | None]


def validate_string_list(values: list[str] | None) -> list[str]:
    """Validate native strings, trim outer whitespace and omit empty items."""
    if values is None:
        return []
    if not isinstance(values, list):
        raise ValueError("expected a native array of strings")
    for index, item in enumerate(values):
        if not isinstance(item, str):
            raise ValueError(f"expected string item at index {index}")
    return [item.strip() for item in values if item.strip()]


def validate_choice_options(values: list[ChoiceOptionInput]) -> list[ChoiceOptionInput]:
    """Validate authored options without converting strings or scalar values."""
    if not isinstance(values, list) or not values:
        raise ValueError("At least one option in a native array is required")
    result: list[ChoiceOptionInput] = []
    for index, item in enumerate(values):
        if not isinstance(item, dict):
            raise ValueError(f"expected option object at index {index}")
        if set(item) - {"label", "recommended", "tradeoff"}:
            raise ValueError(f"unknown option fields at index {index}")
        label = item.get("label")
        if not isinstance(label, str) or not label.strip():
            raise ValueError(f"expected non-empty label at index {index}")
        recommended = item.get("recommended", False)
        if not isinstance(recommended, bool):
            raise ValueError(f"expected recommended boolean at index {index}")
        tradeoff = item.get("tradeoff")
        if tradeoff is not None and not isinstance(tradeoff, str):
            raise ValueError(f"expected tradeoff string or null at index {index}")
        result.append({"label": label.strip(), "recommended": recommended, "tradeoff": tradeoff})
    return result


def _structured_error(
    error_code: str,
    supported: list,
    suggested_tool: str | None,
    error_msg: str | None = None,
    *,
    invalid_keys: list[str] | None = None,
) -> str:
    """Return a machine-readable structured error JSON string.

    Used by the consolidated polymorphic list handlers (TR-B3).
    Keeps backward compat: always includes the legacy ``"error"`` key so
    existing callers that check ``result["error"]`` keep working.

    Parameters
    ----------
    error_code:
        Short stable identifier, e.g. ``"unsupported_entity"`` or
        ``"invalid_filter"``.
    supported:
        List of values that ARE supported (for agent self-correction).
    suggested_tool:
        Replacement tool name when the agent used an old tool, or ``None``.
    error_msg:
        Human-readable detail; surfaced once under ``"error"`` for compat. The
        machine code travels in ``"error_code"``.
    """
    # FR7 dedup: the human string lived in BOTH ``error`` and ``detail`` — the
    # same value twice. Keep ``error`` (back-compat) + ``error_code`` (machine);
    # drop the redundant ``detail`` copy.
    payload: dict = {
        "error": error_msg or error_code,
        "error_code": error_code,
        "supported": supported,
    }
    if suggested_tool is not None:
        payload["suggested_tool"] = suggested_tool
    if invalid_keys is not None:
        payload["invalid_keys"] = invalid_keys
    return json.dumps(payload)


__all__ = ["ChoiceOptionInput", "validate_string_list", "validate_choice_options", "_structured_error"]
