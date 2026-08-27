"""The logical transfer package stays Core-pure: no backend, no filesystem.

This is a static gate rather than a convention.  The package's whole value is
that the same format and the same certification apply in both transfer
directions, and that only holds while it cannot name either backend.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import okto_pulse.core.kg.logical_transfer as package


FORBIDDEN_ROOTS = {
    # editions and backends
    "okto_pulse.community",
    "community",
    "kuzu",
    "ladybug",
    "ladybugdb",
    "okto_grafx",
    # physical IO
    "os",
    "pathlib",
    "shutil",
    "tempfile",
    "io",
    "sqlite3",
    "sqlalchemy",
}

FORBIDDEN_CALLS = {"open", "fsync", "replace", "rename", "unlink", "mkdir"}


def package_modules() -> list[Path]:
    root = Path(package.__file__).parent
    return sorted(root.glob("*.py"))


def module_ids() -> list[str]:
    return [path.name for path in package_modules()]


def parsed(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_the_package_has_modules_to_audit() -> None:
    # Guards the gate itself: a glob that matched nothing would pass silently.
    assert len(package_modules()) >= 6


@pytest.mark.parametrize("path", package_modules(), ids=module_ids())
def test_no_module_imports_a_backend_or_the_filesystem(path: Path) -> None:
    offenders: list[str] = []
    for node in ast.walk(parsed(path)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _root(alias.name) in FORBIDDEN_ROOTS:
                    offenders.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                continue  # relative imports stay inside this package
            if node.module and _root(node.module) in FORBIDDEN_ROOTS:
                offenders.append(node.module)
    assert offenders == [], f"{path.name} imports {offenders}"


@pytest.mark.parametrize("path", package_modules(), ids=module_ids())
def test_no_module_performs_physical_io(path: Path) -> None:
    offenders: list[str] = []
    for node in ast.walk(parsed(path)):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        name = None
        if isinstance(target, ast.Name):
            name = target.id
        elif isinstance(target, ast.Attribute):
            name = target.attr
        if name in FORBIDDEN_CALLS:
            offenders.append(name)
    assert offenders == [], f"{path.name} performs IO via {offenders}"


@pytest.mark.parametrize("path", package_modules(), ids=module_ids())
def test_no_module_imports_okto_pulse_outside_this_package(path: Path) -> None:
    # Core-pure also means self-contained: the transfer package must not reach
    # back into the wider application, or the boundary would only be nominal.
    offenders: list[str] = []
    for node in ast.walk(parsed(path)):
        if isinstance(node, ast.ImportFrom) and not node.level and node.module:
            if node.module.startswith("okto_pulse"):
                offenders.append(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("okto_pulse"):
                    offenders.append(alias.name)
    assert offenders == [], f"{path.name} reaches outside the package: {offenders}"


def test_the_public_surface_is_exported_deliberately() -> None:
    exported = set(package.__all__)
    assert len(exported) == len(package.__all__), "duplicate name in __all__"
    for name in exported:
        assert hasattr(package, name), f"{name} is exported but missing"


def test_no_scope_beyond_the_frozen_milestone_leaked_in() -> None:
    # M-PULSE-6/7 vocabulary must not appear: binding, routing, cutover and
    # resume are explicitly other milestones' work.
    banned = (
        "provider",
        "router",
        "binding",
        "cutover",
        "canary",
        "shadow",
        "journal",
        "outbox",
        "resume_candidate",
    )
    for path in package_modules():
        source = path.read_text(encoding="utf-8").lower()
        for word in banned:
            # Prose may say what is NOT done; a definition may not.
            assert f"def {word}" not in source, f"{path.name} defines {word}"
            assert f"class {word}" not in source, f"{path.name} defines {word}"


def _root(module: str) -> str:
    head = module.split(".")[0]
    if module.startswith("okto_pulse.community"):
        return "okto_pulse.community"
    return head
