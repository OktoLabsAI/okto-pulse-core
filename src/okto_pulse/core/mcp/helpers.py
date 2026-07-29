"""Shared helpers for MCP tool parameter parsing and structured errors.

History
-------
- v1 (ideação 75d4b2a3): introduced ``parse_multi_value`` accepting
  pipe-separated string or JSON-array string, replacing 18 ad-hoc
  ``text.split("|")`` callsites in ``mcp/server.py``.
- v2 (bug 65c9c631): 8 choice/answer/respond tools migrated from raw
  ``options.split(",")`` to ``parse_multi_value``.
- v3 (spec 4b429bf0 — Sprint 1): adds native ``list[str]`` input
  handling (now the canonical wire format thanks to FastMCP Pydantic
  Union support — see Decision dec_fdc3804d) and a ``strict_mode``
  switch that REJECTS comma-only input. ``coerce_to_list_str`` is a
  convenience entry point for tool handlers using the new
  ``param: list[str] | str = ""`` signature.
- v4: strict mode accepts a bare single value (e.g. ``"1"``) while still
  rejecting comma-only multi-value strings. This keeps simple MCP calls
  ergonomic without reintroducing ambiguous comma splitting.
- v5 (spec P0.B — TR-B3): adds ``_structured_error()`` for the
  polymorphic consolidated list handlers.
- v6 (spec bfb30e51 — R5 IMPL-2 — FR3/TR3): adds
  ``parse_options_json()`` — a dedicated parser for the ``options_json``
  parameter accepted by the 4 choice-question handlers.  Accepts a
  JSON-array of ``{label, recommended?, tradeoff?}`` objects and returns
  a list of normalised dicts ready for ``*ChoiceOption`` construction.
  ``parse_multi_value`` is string-list-only and rejects non-string
  items, so a separate parser is required for object-typed items.

Design
------
After the FastMCP 2.14.7 spike (card S1.0) confirmed that Pydantic
generates a clean ``anyOf [array of string, string]`` schema for
``list[str] | str`` parameters, every migrated tool can declare its
multi-value parameters as a Union and let the framework dispatch:

* New MCP clients send a JSON array → handler receives a Python ``list``.
* Legacy clients send a string → handler receives a ``str`` and routes
  through ``parse_multi_value(raw, strict_mode=True)`` for explicit
  parsing or REJECT.

The original idea of an ``@accepts_legacy_string`` decorator was dropped
because Pydantic Union already provides type discrimination at the
framework boundary — a small inline ``isinstance`` check, or
``coerce_to_list_str``, is enough.

The ``strict_mode`` switch defaults to ``False`` during the rollout
window (Sprints 1-4) so existing 17+ already-migrated callsites keep
their lenient behaviour. Sprint 5 flips the default to ``True`` after
all migrations are done and the pre-flip audit (FR7) is recorded.
"""

from __future__ import annotations

import json
from typing import NotRequired

from typing_extensions import TypedDict


class ChoiceOptionInput(TypedDict):
    """Canonical native input for an MCP choice option."""

    label: str
    recommended: NotRequired[bool]
    tradeoff: NotRequired[str | None]


def _clean_str_list(values: list[str]) -> list[str]:
    """Strip whitespace and drop empty strings; preserve original order.

    Centralises the ``[v.strip() for v in xs if v.strip()]`` idiom that
    today is duplicated inline at every callsite.
    """
    return [v.strip() for v in values if isinstance(v, str) and v.strip()]


def parse_multi_value(
    raw: str | list[str] | None,
    strict_mode: bool = True,
) -> list[str]:
    """Parse a multi-value MCP tool parameter.

    Accepted input shapes (auto-detected):

    * **list[str]** (preferred / native FastMCP wire format) — items
      validated as strings; pass-through after :func:`_clean_str_list`.
    * **JSON-array string** ``'["a","b, c"]'`` — autodetected by a
      leading ``[`` after ``.strip()``. Decoded via ``json.loads``;
      every item must be a string. The JSON form is the only way to
      pass a literal ``|`` or ``,`` inside an item.
    * **Pipe-separated string** ``"a|b|c"`` — split on ``|``, strip
      each item, drop empties, and replace the literal two-character
      ``\\n`` escape with a real newline (legacy behaviour preserved
      from the original ``_split`` helper).
    * **None** / empty / whitespace-only → returns ``[]``.

    Args:
        raw: The raw parameter value (list, str, or None).
        strict_mode: Controls how a string with neither ``[`` nor ``|``
            is treated when it may contain commas.

            * ``False``: treat as a single-token
              list — return ``[raw.strip()]`` after newline-escape
              substitution. Preserves the v1 helper contract.
            * ``True``: reject comma-only strings because comma splitting
              is ambiguous. A bare single value is accepted as one item,
              so callers may pass either ``"1"`` or ``["1"]`` for
              single-item fields.

    Returns:
        A list of non-empty stripped strings.

    Raises:
        ValueError: Malformed JSON when the JSON path is engaged
            (``"malformed JSON ..."``); decoded value is not a list
            (``"expected list ..."``); any item is not a string
            (``"expected string items ..."``); or the input is a
            comma-only string and ``strict_mode`` is True.
    """
    if raw is None:
        return []

    # Native list[str] — preferred wire format from FastMCP Pydantic Union.
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            if not isinstance(item, str):
                raise ValueError(
                    f"malformed multi-value: expected string items, "
                    f"got {type(item).__name__} at index {idx}"
                )
        return _clean_str_list(raw)

    if not isinstance(raw, str):
        return []
    if not raw:
        return []

    stripped = raw.strip()
    if not stripped:
        return []

    # JSON array path — leading bracket after trim.
    if stripped.startswith("["):
        try:
            decoded = json.loads(stripped)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"malformed JSON for multi-value param: {e.msg} "
                f"(at pos {e.pos})"
            ) from e
        if not isinstance(decoded, list):
            raise ValueError(
                f"malformed multi-value: expected list, got "
                f"{type(decoded).__name__}"
            )
        for idx, item in enumerate(decoded):
            if not isinstance(item, str):
                raise ValueError(
                    f"malformed multi-value: expected string items, "
                    f"got {type(item).__name__} at index {idx}"
                )
        return _clean_str_list(decoded)

    # Pipe path — legacy behaviour preserved bit-for-bit.
    if "|" in stripped:
        return [
            item.strip().replace("\\n", "\n")
            for item in raw.split("|")
            if item.strip()
        ]

    # No pipe, no bracket. Under strict_mode, only comma-containing strings
    # are rejected; a bare single value is unambiguous and remains valid.
    if strict_mode and "," in stripped:
        raise ValueError(
            "multi-value input must be a JSON array (e.g. '[\"a\", \"b\"]') "
            "or pipe-separated (e.g. 'a|b') — comma-separated input is "
            "rejected by REJECT policy. Pass a native list[str] argument "
            "(preferred) or one of the two string formats above."
        )

    return [stripped.replace("\\n", "\n")]


def coerce_to_list_str(
    value: str | list[str] | None,
    strict_mode: bool = True,
) -> list[str]:
    """Centralised handler-side helper for ``param: list[str] | str = ""``.

    Use at the top of every migrated tool body to coerce the FastMCP
    Pydantic Union argument into a normalised ``list[str]``::

        async def my_tool(items: list[str] | str = "") -> str:
            try:
                items = coerce_to_list_str(items)
            except ValueError as e:
                return json.dumps({"error": f"Invalid items: {e}"})
            ...

    If ``value`` is already a list, items are validated as strings and
    cleaned (strip + drop empties). Otherwise, routed through
    :func:`parse_multi_value` with ``strict_mode`` (default ``True``
    here because callers adopting the new Union signature should reject
    legacy comma input by default; pass ``False`` to opt into the
    lenient v1 behaviour).
    """
    if value is None:
        return []
    if isinstance(value, list):
        for idx, item in enumerate(value):
            if not isinstance(item, str):
                raise ValueError(
                    f"expected string items, got {type(item).__name__} "
                    f"at index {idx}"
                )
        return _clean_str_list(value)
    return parse_multi_value(value, strict_mode=strict_mode)


def parse_options_json(
    raw: list[ChoiceOptionInput] | str | None,
) -> list[ChoiceOptionInput] | None:
    """Parse the ``options_json`` parameter accepted by the 4 choice handlers.

    This is a dedicated parser for **object-typed** option items — distinct
    from :func:`parse_multi_value` which is string-list-only (spec bfb30e51,
    FR3/TR3).

    Accepted input shapes:

    * **None** or empty/whitespace-only string → returns ``None`` (caller
      falls back to the legacy ``options`` label-list path).
    * **JSON-array string** ``'[{"label": "A", "recommended": true}]'`` or
      **native list** (when FastMCP's Pydantic Union delivers it already
      decoded) — each element is validated as an object with at least a
      non-empty ``label`` string.

    Normalisation rules applied to each item:

    * ``label`` — required; must be a non-empty string after stripping
      whitespace.  Missing or empty ``label`` raises ``ValueError``.
    * ``recommended`` — optional native boolean; the legacy string parser
      accepts only the explicit literals ``true``/``false`` (and ``1``/``0``)
      instead of Python truthiness.  Defaults to ``False`` when absent.
    * ``tradeoff`` — optional; must be a string or ``None``/absent; defaults
      to ``None``.  Non-string, non-``None`` values are coerced via ``str()``.

    Returns:
        ``None`` when the input is absent/empty (signals "use legacy path").
        Otherwise a list of normalised dicts with keys ``label``
        (``str``), ``recommended`` (``bool``), ``tradeoff``
        (``str | None``).  The caller is responsible for assigning
        ``id=f"opt_{i}"`` when constructing the ``*ChoiceOption`` instances.

    Raises:
        ValueError: Malformed JSON, decoded value is not a list, any item is
            not a dict, or any item has a missing/empty ``label``.
    """
    if raw is None:
        return None

    # Normalise: accept either a pre-decoded list (FastMCP Pydantic Union) or
    # a JSON string.
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        try:
            decoded = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"malformed JSON for options_json: {exc.msg} (at pos {exc.pos})"
            ) from exc
    elif isinstance(raw, list):
        decoded = raw
    else:
        raise ValueError(
            f"options_json must be a JSON-array string or list, "
            f"got {type(raw).__name__}"
        )

    if not isinstance(decoded, list):
        raise ValueError(
            f"options_json must decode to a list, got {type(decoded).__name__}"
        )

    # Empty list → treat as absent (caller uses legacy path).
    if not decoded:
        return None

    result: list[ChoiceOptionInput] = []
    for idx, item in enumerate(decoded):
        if not isinstance(item, dict):
            raise ValueError(
                f"options_json item at index {idx} must be an object, "
                f"got {type(item).__name__}"
            )
        raw_label = item.get("label")
        if raw_label is None or not str(raw_label).strip():
            raise ValueError(
                f"options_json item at index {idx} is missing a non-empty 'label'"
            )
        label = str(raw_label).strip()

        recommended_raw = item.get("recommended", False)
        if isinstance(recommended_raw, bool):
            recommended = recommended_raw
        elif isinstance(recommended_raw, int) and recommended_raw in (0, 1):
            recommended = bool(recommended_raw)
        elif isinstance(recommended_raw, str):
            normalized = recommended_raw.strip().lower()
            if normalized in {"true", "1"}:
                recommended = True
            elif normalized in {"false", "0"}:
                recommended = False
            else:
                raise ValueError(
                    "options_json item at index "
                    f"{idx} has invalid 'recommended'; expected a boolean"
                )
        else:
            raise ValueError(
                "options_json item at index "
                f"{idx} has invalid 'recommended'; expected a boolean"
            )

        tradeoff_raw = item.get("tradeoff")
        if tradeoff_raw is None:
            tradeoff: str | None = None
        else:
            tradeoff = str(tradeoff_raw)

        result.append({"label": label, "recommended": recommended, "tradeoff": tradeoff})

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


__all__ = [
    "ChoiceOptionInput",
    "parse_multi_value",
    "parse_options_json",
    "coerce_to_list_str",
    "_clean_str_list",
    "_structured_error",
]
