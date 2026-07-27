from __future__ import annotations

import ast
from pathlib import Path

from okto_pulse.core.application.boundary.inbound_adapter_gate import (
    scan_inbound_boundaries,
)


CORE_ROOT = Path(__file__).resolve().parents[1] / "src/okto_pulse/core"


def test_f12_rest_and_mcp_have_zero_relational_access() -> None:
    report = scan_inbound_boundaries(CORE_ROOT)

    assert report.ok, report.as_dict()
    assert report.findings == ()
    assert report.as_dict()["allowances"] == []


def test_f12_mcp_uses_public_uow_catalog_and_removed_legacy_scopes() -> None:
    path = CORE_ROOT / "mcp/server.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert "get_db_for_mcp" not in names
    assert "get_uow_session_for_mcp" not in names
    uow_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "get_unit_of_work_factory_for_mcp"
    ]
    assert len(uow_calls) >= 190


def test_f12_gate_rejects_session_sql_and_concrete_constructor(tmp_path: Path) -> None:
    api = tmp_path / "api"
    mcp = tmp_path / "mcp"
    api.mkdir()
    mcp.mkdir()
    (api / "rogue.py").write_text(
        "from sqlalchemy.ext.asyncio import AsyncSession\n"
        "from sqlalchemy import select\n"
        "from okto_pulse.core.infra.database import get_db\n"
        "async def route(db: AsyncSession):\n"
        "    await db.execute(select(object))\n",
        encoding="utf-8",
    )
    (mcp / "server.py").write_text(
        "def tool():\n"
        "    return SQLAlchemyRepository()\n",
        encoding="utf-8",
    )

    report = scan_inbound_boundaries(tmp_path)
    reasons = {finding.reason for finding in report.findings}

    assert report.ok is False
    assert {
        "forbidden_concrete_import",
        "forbidden_session_symbol",
        "forbidden_session_call",
        "forbidden_native_query",
        "forbidden_concrete_constructor",
    } <= reasons
