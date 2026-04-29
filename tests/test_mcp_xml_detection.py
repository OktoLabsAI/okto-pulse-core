"""XML tag detection middleware - spec 44415298.

Cobre helper `_detect_nested_parameter_xml` e decorator
`_xml_safety_log_decorator` aplicado a 100% das MCP tools via patch.
"""

from __future__ import annotations

import logging

import pytest


# ---------------------------------------------------------------------------
# Helper: detection True (TS5e21db81 + TS7f080fce)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        pytest.param('<parameter name="X">value</parameter>', id="parameter_open_close"),
        pytest.param("<parameter>", id="parameter_open_bare"),
        pytest.param("</parameter>", id="parameter_close_bare"),
        pytest.param("<parameter name='Y'>", id="parameter_open_singlequote"),
        pytest.param("text with <parameter name=\"X\">value</parameter> mixed", id="parameter_in_prose"),
    ],
)
def test_detect_true_for_parameter_tags(value: str) -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml(value) is True


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("<function_calls>", id="function_calls_open"),
        pytest.param("</function_calls>", id="function_calls_close"),
        pytest.param('<invoke name="foo">', id="invoke_open_with_name"),
        pytest.param("</invoke>", id="invoke_close_bare"),
        pytest.param("nested <invoke name=\"bar\">x</invoke>", id="invoke_in_prose"),
        pytest.param("tail </function_calls>", id="function_calls_close_in_prose"),
    ],
)
def test_detect_true_for_function_calls_invoke_tags(value: str) -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml(value) is True


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("<parameter>", id="antml_prefix_parameter"),
        pytest.param("</invoke>", id="antml_prefix_close_invoke"),
        pytest.param("text <invoke>x</invoke> end", id="antml_prefix_in_prose"),
    ],
)
def test_detect_true_for_antml_prefix_tags(value: str) -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml(value) is True


def test_detect_true_case_insensitive() -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml("<PARAMETER>") is True
    assert _detect_nested_parameter_xml("<Function_Calls>") is True
    assert _detect_nested_parameter_xml("</INVOKE>") is True


# ---------------------------------------------------------------------------
# Helper: detection False (TSb81833ca + TS6f8f854f)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("text with &lt;parameter&gt; escaped", id="html_escape_parameter"),
        pytest.param("&lt;/parameter&gt;", id="html_escape_close_parameter"),
        pytest.param("html escape: &lt;function_calls&gt;", id="html_escape_function_calls"),
    ],
)
def test_detect_false_for_html_escape(value: str) -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml(value) is False


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("the parameter tag is used to wrap arguments", id="prose_parameter_tag_is"),
        pytest.param("use the function_calls block to invoke tools", id="prose_use_function_calls"),
        pytest.param("an invoke wraps parameters", id="prose_invoke_wraps"),
        pytest.param("this parameter is critical", id="prose_parameter_critical"),
    ],
)
def test_detect_false_for_natural_language_prose(value: str) -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml(value) is False


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("normal string sem tags", id="normal_string"),
        pytest.param("regular content with no XML", id="no_xml_content"),
        pytest.param("Lorem ipsum dolor sit amet", id="lorem_ipsum"),
        pytest.param("1234567890", id="digits_only"),
    ],
)
def test_detect_false_for_clean_strings(value: str) -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml(value) is False


# ---------------------------------------------------------------------------
# Helper: edge cases (TSf77a1c06)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="none"),
        pytest.param("", id="empty_string"),
        pytest.param("   ", id="three_spaces"),
        pytest.param("\n\t  ", id="whitespace_chars"),
    ],
)
def test_detect_safe_with_none_empty_whitespace(value: object) -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml(value) is False  # type: ignore[arg-type]


def test_detect_safe_with_non_string_input() -> None:
    from okto_pulse.core.mcp.server import _detect_nested_parameter_xml

    assert _detect_nested_parameter_xml(123) is False  # type: ignore[arg-type]
    assert _detect_nested_parameter_xml(["<parameter>"]) is False  # type: ignore[arg-type]
    assert _detect_nested_parameter_xml({"x": "<parameter>"}) is False  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Decorator behavior (TSb628b4ba)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_decorator_emits_log_on_suspicious_arg(caplog: pytest.LogCaptureFixture) -> None:
    from okto_pulse.core.mcp.server import _xml_safety_log_decorator

    sentinel = "ok-result"

    async def fake_tool(**kwargs):
        return sentinel

    fake_tool.__name__ = "fake_tool_demo"
    wrapped = _xml_safety_log_decorator(fake_tool)

    with caplog.at_level(logging.WARNING, logger="okto_pulse.mcp.parser_safety"):
        result = await wrapped(description="<parameter name=\"X\">value</parameter>")

    assert result == sentinel
    matching = [rec for rec in caplog.records if rec.message == "mcp.tool.suspicious_xml_field"]
    assert len(matching) == 1
    rec = matching[0]
    assert rec.tool_name == "fake_tool_demo"  # type: ignore[attr-defined]
    assert rec.field_name == "description"  # type: ignore[attr-defined]
    assert "<parameter" in rec.value_preview  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_decorator_silent_with_clean_args(caplog: pytest.LogCaptureFixture) -> None:
    from okto_pulse.core.mcp.server import _xml_safety_log_decorator

    sentinel = {"status": "ok", "n": 42}

    async def fake_tool(**kwargs):
        return sentinel

    fake_tool.__name__ = "fake_tool_clean"
    wrapped = _xml_safety_log_decorator(fake_tool)

    with caplog.at_level(logging.WARNING, logger="okto_pulse.mcp.parser_safety"):
        result = await wrapped(description="clean text", title="normal title")

    assert result == sentinel
    matching = [rec for rec in caplog.records if rec.message == "mcp.tool.suspicious_xml_field"]
    assert matching == []


def test_decorator_preserves_signature() -> None:
    from okto_pulse.core.mcp.server import _xml_safety_log_decorator

    async def original_tool(**kwargs):
        """Original docstring."""
        return None

    wrapped = _xml_safety_log_decorator(original_tool)
    assert wrapped.__name__ == "original_tool"
    assert wrapped.__doc__ == "Original docstring."
    assert wrapped.__wrapped__ is original_tool  # type: ignore[attr-defined]
    assert getattr(wrapped, "_xml_safety_wrapped", False) is True


@pytest.mark.asyncio
async def test_decorator_does_not_alter_return_value() -> None:
    from okto_pulse.core.mcp.server import _xml_safety_log_decorator

    payload = {"complex": [1, 2, 3], "nested": {"k": "v"}}

    async def fake_tool(**kwargs):
        return payload

    wrapped = _xml_safety_log_decorator(fake_tool)
    result_clean = await wrapped(x="clean")
    result_dirty = await wrapped(x="<parameter>")
    assert result_clean is payload
    assert result_dirty is payload


# ---------------------------------------------------------------------------
# Inventory: 100% coverage of registered tools (TS75b57cdb)
# ---------------------------------------------------------------------------


def test_inventory_all_tools_decorated_via_counter() -> None:
    """The patched mcp.tool() bumps _XML_SAFETY_DECORATED_COUNT for each
    `@mcp.tool()` call. By import time the counter must reflect 100% of
    registered tools (>= 160 at the time of the spec; test allows growth).
    """
    from okto_pulse.core.mcp import server as mcp_server

    assert mcp_server._XML_SAFETY_DECORATED_COUNT >= 160


def test_mcp_tool_is_patched() -> None:
    from okto_pulse.core.mcp import server as mcp_server

    assert getattr(mcp_server.mcp.tool, "_xml_safety_patched", False) is True
