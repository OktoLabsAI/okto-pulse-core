"""Regression tests for bug card 65c9c631 — the 8 MCP choice/answer/respond
tools that previously parsed `options` and `selected` with `split(",")`,
fragmenting any natural-language label that contained an internal comma.

Tools covered:
- okto_pulse_ask_ideation_choice_question     (server.py ~3918)
- okto_pulse_ask_refinement_choice_question   (server.py ~4785)
- okto_pulse_ask_spec_choice_question         (server.py ~9257)
- okto_pulse_answer_ideation_question         (server.py ~3988)
- okto_pulse_answer_refinement_question       (server.py ~4855)
- okto_pulse_answer_spec_question             (server.py ~9327)
- okto_pulse_add_choice_comment               (server.py ~2604)
- okto_pulse_respond_to_choice                (server.py ~2669)

Strategy: rather than spinning the full MCP stack (auth context, DB session
factory, FastMCP wrappers), we read the source file and assert structural
invariants:
  - the legacy `split(",")` calls on `options` / `selected` are GONE,
  - every affected tool now routes through `parse_multi_value`,
  - each docstring documents the JSON-array preference and the pipe fallback.

We also exercise `parse_multi_value` end-to-end with the exact payload
shapes a caller would send, including the JSON-array form with literal
commas inside labels — the regression scenario from board 0.1.13.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okto_pulse.core.mcp.helpers import (
    _clean_str_list,
    coerce_to_list_str,
    parse_multi_value,
)


SERVER_PY = (
    Path(__file__).resolve().parent.parent
    / "src" / "okto_pulse" / "core" / "mcp" / "server.py"
)

CHOICE_TOOL_NAMES = (
    "okto_pulse_ask_ideation_choice_question",
    "okto_pulse_ask_refinement_choice_question",
    "okto_pulse_ask_spec_choice_question",
    "okto_pulse_answer_ideation_question",
    "okto_pulse_answer_refinement_question",
    "okto_pulse_answer_spec_question",
    "okto_pulse_add_choice_comment",
    "okto_pulse_respond_to_choice",
)


@pytest.fixture(scope="module")
def server_source() -> str:
    return SERVER_PY.read_text(encoding="utf-8")


def _slice_tool(source: str, tool_name: str) -> str:
    """Return the source slice from the tool's `async def` to the next one."""
    start_marker = f"async def {tool_name}("
    start = source.find(start_marker)
    assert start != -1, f"tool not found: {tool_name}"
    next_start = source.find("\nasync def ", start + 1)
    if next_start == -1:
        next_start = len(source)
    return source[start:next_start]


# ---------------------------------------------------------------------------
# Structural invariants on server.py
# ---------------------------------------------------------------------------

def test_no_legacy_options_split_on_comma_in_choice_tools(server_source: str):
    """No `options.split(",")` or `selected.split(",")` should remain in the
    8 affected tools — they all must route through `parse_multi_value`.

    A grep across the whole file is the strongest possible regression guard
    and immediately catches any future tool that copy-pastes the old idiom.
    """
    bad_idioms = ('options.split(",")', 'selected.split(",")')
    offenders: list[tuple[int, str]] = []
    for i, line in enumerate(server_source.splitlines(), start=1):
        for idiom in bad_idioms:
            if idiom in line:
                offenders.append((i, line.strip()))
    assert not offenders, (
        "Found legacy split(',') calls that should have been migrated to "
        "parse_multi_value:\n" + "\n".join(f"  L{n}: {ln}" for n, ln in offenders)
    )


@pytest.mark.parametrize("tool_name", CHOICE_TOOL_NAMES)
def test_each_tool_routes_through_parse_multi_value(server_source: str, tool_name: str):
    """Every choice/answer tool must contain `parse_multi_value(`."""
    body = _slice_tool(server_source, tool_name)
    assert "parse_multi_value(" in body, (
        f"{tool_name} must call parse_multi_value (see helpers.py)"
    )


@pytest.mark.parametrize("tool_name", CHOICE_TOOL_NAMES)
def test_each_tool_handles_value_error_from_parse_multi_value(
    server_source: str, tool_name: str
):
    """Each tool must catch ValueError from parse_multi_value (malformed JSON
    etc.) and translate it to a structured error response, not let it bubble
    up as an MCP tool exception."""
    body = _slice_tool(server_source, tool_name)
    assert "ValueError" in body, (
        f"{tool_name} must catch ValueError from parse_multi_value"
    )


@pytest.mark.parametrize("tool_name", CHOICE_TOOL_NAMES)
def test_docstrings_document_json_and_pipe_formats(
    server_source: str, tool_name: str
):
    """Docstring for the multi-value param must document at least the JSON
    array form so callers know the escape hatch for labels with commas."""
    body = _slice_tool(server_source, tool_name)
    docstring_end = body.find('"""', body.find('"""') + 3)
    docstring = body[: docstring_end]
    assert "JSON array" in docstring, (
        f"{tool_name} docstring must mention 'JSON array' as preferred input"
    )
    assert "parse_multi_value" in docstring, (
        f"{tool_name} docstring must reference parse_multi_value helper"
    )


# ---------------------------------------------------------------------------
# Behavioural — parse_multi_value with the exact payloads from the bug repro
# ---------------------------------------------------------------------------

def test_json_array_preserves_internal_commas():
    """The exact regression case from board 0.1.13: each option label has
    several internal commas inside parentheses. With the legacy parser this
    fragmented into 15+ rows; via parse_multi_value it stays at 5."""
    raw = (
        '['
        '"Mermaid (text-based, lightweight, GitHub-renderable, easy diff)",'
        '"ExcaliDraw JSON (interactive editor, visually rich, heavier bundle)",'
        '"SVG inline (universal renderer, manual editing painful)",'
        '"PlantUML or C4 (text-structured, strong system architecture, server render)",'
        '"HTML sandbox iframe (max control, verbose, no extra deps)"'
        ']'
    )
    out = parse_multi_value(raw)
    assert len(out) == 5, f"expected 5 atomic options, got {len(out)}: {out}"
    assert out[0] == "Mermaid (text-based, lightweight, GitHub-renderable, easy diff)"
    assert out[3] == "PlantUML or C4 (text-structured, strong system architecture, server render)"


def test_pipe_separated_legacy_path():
    raw = "Option A|Option B|Option C"
    assert parse_multi_value(raw) == ["Option A", "Option B", "Option C"]


def test_pipe_separated_drops_empty_segments():
    raw = "Option A||Option C|"
    assert parse_multi_value(raw) == ["Option A", "Option C"]


def test_legacy_comma_input_now_uses_pipe_path_so_remains_single_string():
    """A caller that still sends the OLD comma format with NO pipes inside —
    this is the silent backward-compat trap. parse_multi_value falls into
    the pipe path and returns a single element with the entire comma string.

    This is the documented behaviour of the helper (spec 6a2b02ab): comma is
    NOT a separator. Callers stuck on the old style must migrate to JSON or
    pipe. Test asserts the new behaviour explicitly so a future regression
    that re-introduces comma-splitting is caught.
    """
    raw = "Option A,Option B,Option C"
    # Post-Sprint-5 flip: default strict_mode=True rejects comma-only.
    # The lenient single-string behaviour is opt-in via strict_mode=False.
    out = parse_multi_value(raw, strict_mode=False)
    assert out == ["Option A,Option B,Option C"], (
        f"comma-only input must NOT be split; got {out}"
    )


def test_json_array_with_simple_labels():
    raw = '["opt_0", "opt_2"]'
    assert parse_multi_value(raw) == ["opt_0", "opt_2"]


def test_json_array_strips_whitespace_and_drops_empties():
    raw = '["  Option A  ", "", "  ", "Option B"]'
    assert parse_multi_value(raw) == ["Option A", "Option B"]


def test_malformed_json_raises_value_error():
    raw = '["Option A", "Option B"'  # missing closing bracket
    with pytest.raises(ValueError, match="malformed JSON"):
        parse_multi_value(raw)


def test_json_array_path_only_engaged_when_input_starts_with_bracket():
    """A JSON object payload `{"key": "value"}` does NOT begin with `[`,
    so parse_multi_value falls through to the pipe path and returns the
    raw string as a single element. This is the documented helper contract
    (see helpers.py docstring) — only `[`-prefixed input takes the JSON
    branch. A future regression that changes this behaviour silently would
    be very surprising for callers passing markdown / JSON object text in
    a multi-value param."""
    raw = '{"key": "value"}'
    # Single-token strings are unambiguous and are accepted as one item.
    assert parse_multi_value(raw) == ['{"key": "value"}']


def test_json_array_with_object_member_raises_value_error():
    """When the JSON path IS engaged (input starts with `[`) but the
    decoded list contains a non-string item (here, a dict), the helper
    raises a typed ValueError that callers translate to a structured
    MCP error response."""
    raw = '[{"not": "a string"}]'
    with pytest.raises(ValueError, match="expected string items"):
        parse_multi_value(raw)


def test_json_array_of_ints_raises_value_error():
    """Input starts with `[` so JSON path is engaged. Decoded to list of
    ints, which fails the per-item string check."""
    with pytest.raises(ValueError, match="expected string items"):
        parse_multi_value('[1, 2, 3]')


def test_json_array_with_non_string_item_raises_value_error():
    raw = '["Option A", 42, "Option B"]'
    with pytest.raises(ValueError, match="expected string items"):
        parse_multi_value(raw)


def test_empty_string_returns_empty_list():
    assert parse_multi_value("") == []
    assert parse_multi_value(None) == []
    assert parse_multi_value("   ") == []


def test_pipe_format_decodes_literal_newline_escape():
    """Legacy feature inherited from the old _split helper — `\\n` becomes
    a real newline in pipe mode. JSON mode does not need this because JSON
    handles its own escapes."""
    raw = "line one\\nline two|other"
    out = parse_multi_value(raw)
    assert out == ["line one\nline two", "other"]


# ---------------------------------------------------------------------------
# v3 additions (Sprint 1, spec 4b429bf0): native list[str] input,
# strict_mode flag, coerce_to_list_str helper, _clean_str_list.
# ---------------------------------------------------------------------------

def test_native_list_input_pass_through():
    """Pydantic Union path delivers a Python list; helper must accept it."""
    raw = ["Mermaid (text-based, lightweight)", "ExcaliDraw (heavy)"]
    assert parse_multi_value(raw) == raw


def test_native_list_input_strips_and_drops_empties():
    raw = ["  Option A  ", "", "  ", "Option B"]
    assert parse_multi_value(raw) == ["Option A", "Option B"]


def test_native_list_input_with_non_string_item_raises():
    with pytest.raises(ValueError, match="expected string items"):
        parse_multi_value(["ok", 42, "also ok"])


def test_strict_mode_rejects_comma_only_input():
    """Post-Sprint-5 default: comma-only string is NOT a separator and
    is rejected with a structured error (REJECT policy, FR1)."""
    with pytest.raises(ValueError, match="rejected by REJECT policy"):
        parse_multi_value("a,b,c", strict_mode=True)


def test_strict_mode_accepts_single_token_input():
    """A bare single value under strict_mode is accepted as one item.

    Only comma-containing strings are rejected because they are ambiguous.
    """
    assert parse_multi_value("just one token", strict_mode=True) == ["just one token"]


def test_strict_mode_accepts_json_array():
    """JSON-array input is the canonical non-list form; passes strict_mode."""
    assert parse_multi_value('["a", "b"]', strict_mode=True) == ["a", "b"]


def test_strict_mode_accepts_pipe_separated():
    """Pipe-separated remains accepted under strict_mode (legacy escape hatch)."""
    assert parse_multi_value("a|b|c", strict_mode=True) == ["a", "b", "c"]


def test_strict_mode_does_not_affect_native_list():
    """Native list input bypasses the strict_mode check entirely (the
    Pydantic Union already discriminated; we only validate item types)."""
    assert parse_multi_value(["a", "b"], strict_mode=True) == ["a", "b"]


def test_strict_mode_does_not_affect_none_or_empty():
    assert parse_multi_value(None, strict_mode=True) == []
    assert parse_multi_value("", strict_mode=True) == []
    assert parse_multi_value("   ", strict_mode=True) == []


def test_coerce_to_list_str_with_list():
    """When FastMCP delivered a list (new client), pass-through after clean."""
    assert coerce_to_list_str(["a", "b"]) == ["a", "b"]


def test_coerce_to_list_str_with_string_defaults_strict():
    """coerce_to_list_str defaults strict_mode=True — comma input rejected."""
    with pytest.raises(ValueError, match="rejected by REJECT policy"):
        coerce_to_list_str("a,b,c")


def test_coerce_to_list_str_with_string_lenient_opt_in():
    """Caller can opt back into lenient mode if needed."""
    assert coerce_to_list_str("a,b,c", strict_mode=False) == ["a,b,c"]


def test_coerce_to_list_str_with_pipe_string():
    """Pipe input always works regardless of strict_mode."""
    assert coerce_to_list_str("a|b") == ["a", "b"]


def test_coerce_to_list_str_with_json_string():
    """JSON-encoded string always works regardless of strict_mode."""
    assert coerce_to_list_str('["a", "b, c"]') == ["a", "b, c"]


def test_coerce_to_list_str_with_none():
    assert coerce_to_list_str(None) == []


def test_coerce_to_list_str_with_non_string_list_item_raises():
    with pytest.raises(ValueError, match="expected string items"):
        coerce_to_list_str(["ok", 42])


def test_clean_str_list_strips_and_drops_empties():
    assert _clean_str_list(["  a  ", "", " b ", "  "]) == ["a", "b"]


def test_clean_str_list_skips_non_strings_silently():
    """_clean_str_list is intentionally permissive — type validation is the
    caller's job (parse_multi_value/coerce_to_list_str do the raise)."""
    assert _clean_str_list(["a", None, "b", 42]) == ["a", "b"]
