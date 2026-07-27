"""Unit tests for Story 4 validate_and_persist combo helpers.

Focus on pure helpers + signature contracts. End-to-end behavior of
`okto_pulse_validate_architecture_design_payload` (commit/include_design
flags, dry-run shape, persist flow, lock check) is covered by integration
smoke (TC6a/TC6b, see ts_5b4f6570/ts_97cccbf8/ts_98f69dd0/ts_9a57e8cd/
ts_281494d5/ts_ed3530bc).

These tests target the pure JSON-parsing helper that drives input
normalization and the public tool signature that exposes the combo flags.
"""

from __future__ import annotations

import inspect

from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.server import _parse_json_arg


# ---------------------------------------------------------------------------
# _parse_json_arg — input normalization helper (used by 3 fields:
# entities, interfaces, diagrams)
# ---------------------------------------------------------------------------


def test_parse_json_arg_none_returns_default():
    """None input falls back to the supplied default with no error."""
    result, err = _parse_json_arg(None, ["fallback"])
    assert result == ["fallback"]
    assert err is None


def test_parse_json_arg_empty_string_returns_default():
    """Empty string is treated as 'not provided' — preserve dry-run default."""
    result, err = _parse_json_arg("", None)
    assert result is None
    assert err is None


def test_parse_json_arg_native_list_passes_through():
    """Already-decoded list input must not be re-parsed."""
    payload = [{"id": "e1", "name": "X"}]
    result, err = _parse_json_arg(payload, None)
    assert result is payload  # same object — no copy
    assert err is None


def test_parse_json_arg_native_dict_passes_through():
    """Same as list — native dict is accepted as-is."""
    payload = {"k": 1}
    result, err = _parse_json_arg(payload, None)
    assert result is payload
    assert err is None


def test_parse_json_arg_valid_json_string_decodes():
    """JSON string is decoded into the corresponding Python object."""
    result, err = _parse_json_arg('[{"a": 1}, {"b": 2}]', None)
    assert result == [{"a": 1}, {"b": 2}]
    assert err is None


def test_parse_json_arg_invalid_json_returns_error_message():
    """Bad JSON returns (None, '<error message>') — caller surfaces it."""
    result, err = _parse_json_arg("not valid json", None)
    assert result is None
    assert err is not None
    assert "Invalid JSON argument" in err


def test_parse_json_arg_invalid_json_does_not_raise():
    """Helper must NOT raise — error path is returned tuple-second."""
    # If this raises, the test fails (pytest catches exceptions as failures).
    _parse_json_arg("{malformed", None)
    _parse_json_arg("[1, 2,", None)
    _parse_json_arg("'single quotes'", None)


# ---------------------------------------------------------------------------
# okto_pulse_validate_architecture_design_payload — public signature contract
# ---------------------------------------------------------------------------


def test_validate_payload_tool_is_registered_as_mcp_tool():
    """The tool must be exposed via @mcp.tool() — accessible as `.fn`."""
    tool = getattr(server, "okto_pulse_validate_architecture_design_payload", None)
    assert tool is not None, "Tool must be registered in the server module"
    assert hasattr(tool, "fn"), "Tool must be an MCP-decorated FunctionTool"


def test_validate_payload_signature_exposes_commit_and_include_design_flags():
    """Story 4 FR1: commit and include_design must be opt-in boolean flags."""
    tool = server.okto_pulse_validate_architecture_design_payload
    # Resolve original signature through the XML safety wrapper if needed
    fn = getattr(tool.fn, "__wrapped__", tool.fn)
    sig = inspect.signature(fn)
    params = sig.parameters

    assert "commit" in params, "commit flag missing from signature"
    assert "include_design" in params, "include_design flag missing from signature"


def test_validate_payload_commit_defaults_to_false():
    """Default behavior is dry-run — commit must default to False (FR2)."""
    tool = server.okto_pulse_validate_architecture_design_payload
    fn = getattr(tool.fn, "__wrapped__", tool.fn)
    sig = inspect.signature(fn)
    assert sig.parameters["commit"].default is False, (
        "commit default must be False so existing callers keep dry-run behavior"
    )


def test_validate_payload_include_design_defaults_to_false():
    """Verbose echo is opt-in — include_design defaults to False (FR5)."""
    tool = server.okto_pulse_validate_architecture_design_payload
    fn = getattr(tool.fn, "__wrapped__", tool.fn)
    sig = inspect.signature(fn)
    assert sig.parameters["include_design"].default is False, (
        "include_design default must be False so combo returns minimal envelope"
    )


def test_validate_payload_accepts_create_mode_params():
    """Create mode requires parent_type + parent_id (FR7)."""
    tool = server.okto_pulse_validate_architecture_design_payload
    fn = getattr(tool.fn, "__wrapped__", tool.fn)
    sig = inspect.signature(fn)
    assert "parent_type" in sig.parameters
    assert "parent_id" in sig.parameters


def test_validate_payload_accepts_update_mode_params():
    """Update mode dispatches on design_id (FR7)."""
    tool = server.okto_pulse_validate_architecture_design_payload
    fn = getattr(tool.fn, "__wrapped__", tool.fn)
    sig = inspect.signature(fn)
    assert "design_id" in sig.parameters


def test_validate_payload_accepts_three_collection_fields():
    """entities / interfaces / diagrams — the 3 normalizable fields."""
    tool = server.okto_pulse_validate_architecture_design_payload
    fn = getattr(tool.fn, "__wrapped__", tool.fn)
    sig = inspect.signature(fn)
    for field in ("entities", "interfaces", "diagrams"):
        assert field in sig.parameters, f"{field} param missing"


def test_validate_payload_is_async():
    """Tool must be async — DB + permission checks are async."""
    tool = server.okto_pulse_validate_architecture_design_payload
    fn = getattr(tool.fn, "__wrapped__", tool.fn)
    assert inspect.iscoroutinefunction(fn), (
        "validate_architecture_design_payload must be async — uses await for DB calls"
    )
