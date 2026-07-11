from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

from okto_pulse.core.domain.entities import Board, Ideation, Spec
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.repositories.interfaces.repositories import (
    BoardRepository,
    IdeationRepository,
    SpecRepository,
)


class _MemoryRepository:
    def __init__(self) -> None:
        self.rows: dict[str, object] = {}
        self.realm_scope = RealmScope.local()

    async def get(self, entity_id: str):
        return self.rows.get(entity_id)

    async def add(self, entity) -> None:
        self.rows[entity.id] = entity


def test_f01_repository_ports_use_pure_domain_entities() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
    for relative in (
        "domain/entities.py",
        "repositories/interfaces/repositories.py",
        "repositories/interfaces/unit_of_work.py",
    ):
        tree = ast.parse((root / relative).read_text(encoding="utf-8"))
        imports = {
            alias.name.split(".", 1)[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            (node.module or "").split(".", 1)[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        assert "sqlalchemy" not in imports
        assert "okto_pulse.core.models.db" not in (root / relative).read_text(
            encoding="utf-8"
        )


def test_f01_core_production_tree_has_zero_orm_ownership() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
    violations: list[str] = []
    retired_modules = (
        "okto_pulse.core.models.db",
        "okto_pulse.core.repositories.sqlalchemy",
    )
    mapped_primitives = {
        "DeclarativeBase",
        "Mapped",
        "TypeDecorator",
        "declarative_base",
        "mapped_column",
        "relationship",
    }
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        relative = path.relative_to(root).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("sqlalchemy") or alias.name.startswith(
                        retired_modules
                    ):
                        violations.append(f"{relative}:{node.lineno}:{alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("sqlalchemy") or module.startswith(
                    retired_modules
                ):
                    violations.append(f"{relative}:{node.lineno}:{module}")
            elif isinstance(node, ast.Call):
                name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
                if name in mapped_primitives:
                    violations.append(f"{relative}:{node.lineno}:{name}")
            elif isinstance(node, ast.ClassDef):
                for base in node.bases:
                    name = getattr(base, "id", None) or getattr(base, "attr", None)
                    if name in {"DeclarativeBase", "TypeDecorator"}:
                        violations.append(f"{relative}:{node.lineno}:{name}")

    assert not (root / "models" / "db.py").exists()
    assert not (root / "repositories" / "sqlalchemy").exists()
    assert violations == []


def test_f01_repository_protocols_accept_standard_library_fakes() -> None:
    fake = _MemoryRepository()
    assert isinstance(fake, BoardRepository)
    assert isinstance(fake, IdeationRepository)
    assert isinstance(fake, SpecRepository)


def test_f01_domain_entities_keep_aggregate_shapes_without_orm() -> None:
    board = Board(name="Board", owner_id="owner")
    ideation = Ideation(board_id=board.id, title="Idea", created_by="owner")
    spec = Spec(board_id=board.id, title="Spec", created_by="owner")

    assert board.id and ideation.status.value == "draft"
    assert spec.status.value == "draft"


def test_f01_repository_contracts_import_without_sqlalchemy_runtime() -> None:
    code = (
        "import sys\n"
        "from okto_pulse.core.domain.entities import Board, Ideation, Spec\n"
        "from okto_pulse.core.repositories.interfaces import PulseUnitOfWork\n"
        "assert not any(name == 'sqlalchemy' or name.startswith('sqlalchemy.') "
        "for name in sys.modules), sorted(sys.modules)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
