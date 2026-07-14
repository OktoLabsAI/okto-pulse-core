"""S02: Community REST adapters remain thin UoW-based orchestration."""

from __future__ import annotations

import ast
from pathlib import Path

_REST_FILES = (
    "kg_cognitive_candidate_commands.py",
    "kg_rebuild.py",
    "kg_tick.py",
)


def _module_tree(filename: str) -> ast.Module:
    path = (
        Path(__file__).resolve().parents[2]
        / "okto_labs_pulse_community"
        / "src"
        / "okto_pulse"
        / "community"
        / "api"
        / filename
    )
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_s02_migrated_rest_handlers_do_not_import_raw_session_dependencies() -> None:
    for filename in _REST_FILES:
        tree = _module_tree(filename)
        imported_modules = {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        imported_names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        assert "sqlalchemy.ext.asyncio" not in imported_modules, filename
        assert "AsyncSession" not in imported_names, filename
        assert "get_db" not in imported_names, filename


def test_s02_uow_context_bridge_is_absent() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "application"
        / "use_cases"
        / "base.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"))
    function_names = {
        node.name for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    assert "session_of" not in function_names
    assert "relational_context_from_uow" not in function_names


def test_s02_generic_events_hub_has_no_sqlalchemy_or_database_factory() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "application"
        / "kg_events_hub.py"
    )
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    modules = {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }

    assert not any(module.startswith("sqlalchemy") for module in modules)
    assert "okto_pulse.core.infra.database" not in modules
    assert "get_session_factory" not in source
