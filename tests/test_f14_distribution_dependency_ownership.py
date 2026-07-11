from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from okto_pulse.core.application.boundary.distribution_dependency_ownership import (
    COMMUNITY_DISTRIBUTION,
    CORE_DISTRIBUTION,
    audit_distribution_dependencies,
    build_distribution_dependency_ledger,
)
from okto_pulse.core.ports.f14 import EditionPort


CORE_REPO = Path(__file__).resolve().parents[1]
COMMUNITY_REPO = CORE_REPO.parent / "okto_labs_pulse_community"
CORE_WHEEL = CORE_REPO / "dist" / "okto_pulse_core-0.3.0-py3-none-any.whl"
COMMUNITY_WHEEL = (
    COMMUNITY_REPO / "dist" / "okto_pulse-0.3.0-py3-none-any.whl"
)


def test_f14_contract_and_all_distribution_surfaces_are_conformant() -> None:
    assert EditionPort is not None
    report = audit_distribution_dependencies(
        core_repo=CORE_REPO,
        community_repo=COMMUNITY_REPO,
        core_wheel=CORE_WHEEL,
        community_wheel=COMMUNITY_WHEEL,
    )

    assert report.ok, report.as_dict()
    assert report.observed[CORE_DISTRIBUTION]["manifest"] == (
        "pydantic",
        "pydantic-settings",
        "pyyaml",
    )
    assert report.observed[CORE_DISTRIBUTION]["manifest"] == report.observed[
        CORE_DISTRIBUTION
    ]["lock"]
    assert report.observed[CORE_DISTRIBUTION]["manifest"] == report.observed[
        CORE_DISTRIBUTION
    ]["wheel"]
    assert report.observed[COMMUNITY_DISTRIBUTION]["manifest"] == report.observed[
        COMMUNITY_DISTRIBUTION
    ]["lock"]
    assert report.observed[COMMUNITY_DISTRIBUTION]["manifest"] == report.observed[
        COMMUNITY_DISTRIBUTION
    ]["wheel"]


def test_f14_gate_fails_closed_when_a_runtime_dependency_loses_ownership() -> None:
    ledger = tuple(
        entry
        for entry in build_distribution_dependency_ledger()
        if not (
            entry.declared_by == COMMUNITY_DISTRIBUTION
            and entry.normalized_distribution == "fastapi"
        )
    )

    report = audit_distribution_dependencies(
        core_repo=CORE_REPO,
        community_repo=COMMUNITY_REPO,
        ledger=ledger,
    )

    codes = {(finding.code, finding.dependency) for finding in report.findings}
    assert ("dependency_unowned", "fastapi") in codes
    assert ("source_import_unowned", "fastapi") in codes


@pytest.mark.skipif(
    os.environ.get("OKTO_RUN_F14_WHEEL_SMOKE") != "1",
    reason="Set OKTO_RUN_F14_WHEEL_SMOKE=1 for clean-wheel acceptance.",
)
def test_core_wheel_imports_in_clean_environment_without_edition_runtimes(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "core-venv"
    subprocess.run(
        ["uv", "venv", str(venv), "--python", sys.executable],
        check=True,
        cwd=CORE_REPO,
    )
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run(
        ["uv", "pip", "install", "--python", str(python), "--no-deps", str(CORE_WHEEL)],
        check=True,
        cwd=CORE_REPO,
    )
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "pydantic>=2.5,<2.14",
            "pydantic-settings>=2.1,<3",
            "PyYAML>=6,<7",
        ],
        check=True,
        cwd=CORE_REPO,
    )
    script = r"""
import importlib
import importlib.util
import pkgutil
import okto_pulse.core

blocked = ("fastapi", "starlette", "sqlalchemy", "aiosqlite", "mcp", "fastmcp")
present = [name for name in blocked if importlib.util.find_spec(name) is not None]
assert not present, present
loaded = []
for module in pkgutil.walk_packages(okto_pulse.core.__path__, "okto_pulse.core."):
    importlib.import_module(module.name)
    loaded.append(module.name)
assert loaded
print(f"core_clean_imports={len(loaded)} blocked_present={present}")
"""
    result = subprocess.run(
        [str(python), "-c", script],
        check=True,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": ""},
    )
    assert "blocked_present=[]" in result.stdout
