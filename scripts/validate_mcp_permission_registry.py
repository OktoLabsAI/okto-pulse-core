"""Fail CI when the live MCP catalog drifts from its permission manifest."""

from __future__ import annotations

from okto_pulse.core.domain.permissions import validate_registry_vs_tools
from okto_pulse.core.mcp import server


def main() -> None:
    catalog = server.mcp.resolve()
    tool_names = [tool.name for tool in catalog.iter_tools()]
    report = validate_registry_vs_tools(tool_names, strict=True)
    print(report.render())


if __name__ == "__main__":
    main()
