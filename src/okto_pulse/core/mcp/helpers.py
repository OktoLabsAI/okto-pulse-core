"""Shared helpers for MCP tool parameter parsing (ideação 75d4b2a3).

The previous pattern was ``text.split("|")`` scattered across 18
callsites in ``mcp/server.py``. Any MCP caller passing a multi-value
string containing a literal ``|`` — the Python union type hint
``str | None``, a markdown table, a regex alternation — had its
content silently split in half, bloating list counts and corrupting
specs. The coverage gate treated the fragments as distinct items.

``parse_multi_value`` solves it by accepting BOTH the legacy
pipe-separated format AND a JSON array. Autodetection is a simple
``startswith("[")`` test on the trimmed input. Callers that can no
longer avoid ``|`` in their text just pass a JSON array instead.
Callers that never had the problem keep sending pipes as before.
"""

from __future__ import annotations

import json


def parse_multi_value(raw: str | None) -> list[str]:
    """Parse a multi-value MCP tool parameter.

    Supports two formats, autodetected by the input:

    - **Pipe-separated** (legacy): ``"a|b|c"`` — split on ``|``, strip
      each item, drop empties, and replace the literal two-character
      ``\\n`` escape with a real newline (feature inherited from the
      legacy ``_split`` helper).
    - **JSON array**: ``'["a", "b", "c"]'`` — any input whose
      ``.strip()`` begins with ``[`` takes this path. ``json.loads``
      is applied and the result must be a list of strings. The JSON
      form is the only way to pass a literal ``|`` inside an item.

    Args:
        raw: The raw parameter value, typically a single string from
            an MCP tool argument. ``None`` and empty strings both
            return ``[]``.

    Returns:
        A list of non-empty stripped strings.

    Raises:
        ValueError: if the JSON path is taken and ``json.loads`` fails
            ("malformed JSON"), the decoded value is not a list
            ("expected list"), or any item is not a string
            ("expected string items"). Callers translate this into
            the MCP-level error payload.
    """
    if raw is None:
        return []
    if not isinstance(raw, str):
        return []
    if not raw:
        return []

    stripped = raw.strip()
    if not stripped:
        return []

    # JSON path — detected by leading bracket after trim.
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
        out: list[str] = []
        for idx, item in enumerate(decoded):
            if not isinstance(item, str):
                raise ValueError(
                    f"malformed multi-value: expected string items, "
                    f"got {type(item).__name__} at index {idx}"
                )
            cleaned = item.strip()
            if cleaned:
                out.append(cleaned)
        return out

    # Pipe path — legacy behaviour preserved bit-for-bit.
    return [
        item.strip().replace("\\n", "\n")
        for item in raw.split("|")
        if item.strip()
    ]


__all__ = ["parse_multi_value"]
