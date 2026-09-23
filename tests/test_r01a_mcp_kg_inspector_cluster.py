"""F4 retires inspector tools; MCP persistence must still use application UoW."""
import ast
from pathlib import Path
from okto_pulse.core.mcp import server as mcp_server


def test_retired_inspectors_have_no_hidden_handlers():
    for name in ("okto_pulse_kg_stale_canonical_parity_list", "okto_pulse_kg_queue_drilldown"):
        assert not hasattr(mcp_server, name)


def test_mcp_server_has_no_raw_get_db_for_mcp_access() -> None:
    """Every MCP handler must enter persistence through the application UoW."""
    tree = ast.parse(Path(mcp_server.__file__).read_text(encoding="utf-8"))
    raw_uses = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "get_db_for_mcp"
    ]
    assert raw_uses == []
