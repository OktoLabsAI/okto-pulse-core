from __future__ import annotations

from okto_pulse.core.domain.mcp_permission_registry import (
    MCP_TOOL_ADMISSION_CLASSES,
    McpAdmissionClass,
    resolve_mcp_tool_admission_class,
)
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.catalog import CoreMcpCatalog


def test_every_live_tool_publishes_closed_core_admission_metadata() -> None:
    tools = server.mcp.resolve().iter_tools()
    tools_by_name = {tool.name: tool for tool in tools}

    assert set(MCP_TOOL_ADMISSION_CLASSES) == set(tools_by_name)
    assert all(
        isinstance(tool.admission_class, McpAdmissionClass)
        for tool in tools_by_name.values()
    )
    assert all(
        tool.admission_class is MCP_TOOL_ADMISSION_CLASSES[tool_name]
        for tool_name, tool in tools_by_name.items()
    )


def test_effectful_read_authorized_tools_remain_explicit_writers() -> None:
    assert (
        MCP_TOOL_ADMISSION_CLASSES["okto_pulse_get_allowed_transitions"]
        is McpAdmissionClass.WRITER
    )
    for tool_name in (
        "okto_pulse_kg_query_cypher",
        "okto_pulse_kg_query_global",
        "okto_pulse_kg_query_natural",
        "okto_pulse_kg_query_reflective",
    ):
        assert MCP_TOOL_ADMISSION_CLASSES[tool_name] is McpAdmissionClass.WRITER

    assert (
        MCP_TOOL_ADMISSION_CLASSES["okto_pulse_get_board"]
        is McpAdmissionClass.READER
    )


def test_unknown_tool_fails_closed_as_writer_in_resolver_and_catalog() -> None:
    unknown_name = "okto_pulse_get_future_unknown"

    assert (
        resolve_mcp_tool_admission_class(unknown_name)
        is McpAdmissionClass.WRITER
    )

    catalog = CoreMcpCatalog(name="unknown-admission", version="1")

    @catalog.tool(name=unknown_name)
    def unknown_tool() -> None:
        return None

    assert unknown_tool.admission_class is McpAdmissionClass.WRITER
