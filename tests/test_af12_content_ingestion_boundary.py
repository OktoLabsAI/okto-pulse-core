from __future__ import annotations

import inspect
import textwrap

from okto_pulse.core.application.boundary.storage_bypass_gate import (
    KIND_MCP_CONCRETE_SOURCE_PARAM,
    KIND_MCP_CONTENT_FETCH,
    run_storage_bypass_gate,
)
from okto_pulse.core.mcp import server as mcp_server


def test_af12_core_mcp_tools_expose_abstract_content_reference_only():
    tool_names = (
        "okto_pulse_upload_attachment",
        "okto_pulse_add_ideation_knowledge",
        "okto_pulse_add_spec_knowledge",
        "okto_pulse_add_refinement_knowledge",
    )
    for tool_name in tool_names:
        tool = getattr(mcp_server, tool_name)
        params = set(inspect.signature(tool.fn).parameters)
        schema_params = set(tool.parameters["properties"])
        assert "content_reference" in params
        assert "content_reference" in schema_params
        assert "file_path" not in params
        assert "file_url" not in params
        assert "file_path" not in schema_params
        assert "file_url" not in schema_params


def test_af12_storage_bypass_gate_covers_real_core_api_and_mcp():
    report = run_storage_bypass_gate()
    assert report.ok, [v.as_dict() for v in report.violations]
    assert "core\\api" in report.guarded_path or "core/api" in report.guarded_path
    assert "core\\mcp" in report.guarded_path or "core/mcp" in report.guarded_path


def test_af12_gate_flags_mcp_concrete_sources_and_fetch(tmp_path):
    bad = tmp_path / "bad_mcp.py"
    bad.write_text(
        textwrap.dedent(
            """
            @mcp.tool()
            async def bad_tool(file_path: str | None = None, file_url: str | None = None):
                return await client.get(file_url)
            """
        ),
        encoding="utf-8",
    )

    report = run_storage_bypass_gate(tmp_path)
    kinds = {v.kind for v in report.violations}
    assert KIND_MCP_CONCRETE_SOURCE_PARAM in kinds
    assert KIND_MCP_CONTENT_FETCH in kinds
