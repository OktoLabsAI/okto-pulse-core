"""Shared helpers for MCP tool parameter parsing.

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


__all__ = [
    "parse_multi_value",
    "coerce_to_list_str",
    "_clean_str_list",
]
