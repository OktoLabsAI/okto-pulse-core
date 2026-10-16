"""Native architecture inputs and unchanged dry-run/commit flags."""
from __future__ import annotations
import inspect
import pytest
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.server import _validate_native_json_arg


@pytest.mark.parametrize(("kind", "value"), [
    ("object", ""), ("object", "{}"), ("object", []), ("object", False),
    ("array", ""), ("array", "[]"), ("array", {}), ("array", [1]),
])
def test_native_argument_rejects_other_shapes(kind, value):
    result, error = _validate_native_json_arg(value, None, kind)
    assert result is None
    assert error


@pytest.mark.parametrize(("kind", "value"), [
    ("object", {}), ("object", {"key": "value"}),
    ("array", []), ("array", [{"name": "entity"}]),
])
def test_native_argument_preserves_payload(kind, value):
    result, error = _validate_native_json_arg(value, None, kind)
    assert result is value
    assert error is None


def test_native_argument_omission_uses_creation_or_update_default():
    assert _validate_native_json_arg(None, [], "array") == ([], None)
    assert _validate_native_json_arg(None, None, "array") == (None, None)


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
