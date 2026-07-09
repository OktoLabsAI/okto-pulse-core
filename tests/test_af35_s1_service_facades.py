"""AF35-S1 regression guards for migrated service facades.

The public Resource Gate, traceability and runtime-settings service modules keep
their historical import paths, but relational implementation details must live
behind explicit SQLAlchemy adapters.
"""

from __future__ import annotations

import ast
from pathlib import Path

from okto_pulse.core.repositories.core_orm_import_gate import (
    CORE_ORM_IMPORT_ALLOWLIST,
    run_core_orm_import_gate,
)

ROOT = Path(__file__).resolve().parents[1]

SERVICE_MODULES = (
    ROOT / "src/okto_pulse/core/services/resource_gate.py",
    ROOT / "src/okto_pulse/core/services/traceability.py",
    ROOT / "src/okto_pulse/core/services/settings_service.py",
)

ADAPTER_ALLOWLIST = {
    "src/okto_pulse/core/repositories/sqlalchemy/resource_gate_service.py",
    "src/okto_pulse/core/repositories/sqlalchemy/runtime_settings_service.py",
    "src/okto_pulse/core/repositories/sqlalchemy/traceability_read_model.py",
}

FORBIDDEN_NAMES = {"AsyncSession", "select", "flag_modified", "get_db"}
FORBIDDEN_SNIPPETS = ("session.execute", "Depends(get_db)", "from sqlalchemy")


def test_af35_s1_service_facades_have_no_direct_relational_symbols() -> None:
    for path in SERVICE_MODULES:
        source = path.read_text(encoding="utf-8")
        for snippet in FORBIDDEN_SNIPPETS:
            assert snippet not in source, f"{path.name} still contains {snippet}"

        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("sqlalchemy"), path.name
                assert node.module != "okto_pulse.core.models.db", path.name
            elif isinstance(node, ast.Import):
                assert all(
                    not alias.name.startswith("sqlalchemy")
                    for alias in node.names
                ), path.name
            elif isinstance(node, ast.Name):
                assert node.id not in FORBIDDEN_NAMES, (path.name, node.id)


def test_af35_s1_relational_bodies_are_explicit_repository_adapters() -> None:
    for adapter in ADAPTER_ALLOWLIST:
        assert CORE_ORM_IMPORT_ALLOWLIST.get(adapter) == "repository"

    for service in SERVICE_MODULES:
        rel = service.relative_to(ROOT).as_posix()
        assert rel not in CORE_ORM_IMPORT_ALLOWLIST

    report = run_core_orm_import_gate()
    assert report.ok, [violation.as_dict() for violation in report.violations]
