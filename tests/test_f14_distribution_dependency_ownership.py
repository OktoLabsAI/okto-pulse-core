from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

import okto_pulse.core.application.boundary.distribution_dependency_ownership as dependency_ownership
from okto_pulse.core.application.boundary.distribution_dependency_ownership import (
    COMMUNITY_DISTRIBUTION,
    CORE_DISTRIBUTION,
    audit_distribution_dependencies,
    build_distribution_dependency_ledger,
)
from okto_pulse.core.application.boundary.repository_checkout import (
    resolve_repository_checkout,
)
from okto_pulse.core.ports.f14 import EditionPort


CORE_REPO = Path(__file__).resolve().parents[1]
_COMMUNITY_CHECKOUT = resolve_repository_checkout(
    "community",
    anchor_repo=CORE_REPO,
)
assert _COMMUNITY_CHECKOUT is not None
COMMUNITY_REPO = _COMMUNITY_CHECKOUT.repo_root
CORE_WHEEL = CORE_REPO / "dist" / "okto_pulse_core-0.3.2-py3-none-any.whl"
COMMUNITY_WHEEL = (
    COMMUNITY_REPO / "dist" / "okto_pulse-0.3.2-py3-none-any.whl"
)


def _write_core_wheel(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "okto_pulse_core-0.3.2.dist-info/METADATA",
            "Metadata-Version: 2.3\n"
            "Name: okto-pulse-core\n"
            "Version: 0.3.2\n"
            "Requires-Dist: pydantic>=2.12,<2.14\n"
            "Requires-Dist: pydantic-core>=2.14.1,<3\n"
            "Requires-Dist: typing-extensions>=4.14.1,<5\n"
            "Requires-Dist: PyYAML>=6,<7\n",
        )
        for member, content in members.items():
            archive.writestr(member, content)


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
        "pydantic-core",
        "pyyaml",
        "typing-extensions",
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


def test_f14_source_scan_detects_community_imports_without_literal_false_positives(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "core"
    source_root.mkdir()
    (source_root / "clean_docs.py").write_text(
        '"""okto_pulse.community is named only as architecture documentation."""\n'
        'EXAMPLE = "from okto_pulse.community.docs import Example"\n',
        encoding="utf-8",
    )
    (source_root / "rogue.py").write_text(
        "import okto_pulse.community.runtime\n"
        "from okto_pulse.community.adapters import Repository\n"
        "from okto_pulse import community\n",
        encoding="utf-8",
    )

    imports = dependency_ownership._source_imports(
        source_root,
        forbidden_prefixes=("okto_pulse.community",),
    )

    assert set(imports) == {"okto_pulse.community"}
    assert len(imports["okto_pulse.community"]) == 3
    assert all("rogue.py:" in row for row in imports["okto_pulse.community"])


def test_f14_dependency_graph_rejects_core_to_community_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = dependency_ownership._source_imports

    def source_imports_with_forbidden_edge(root: Path, **kwargs):
        observed = original(root, **kwargs)
        if root.name == "core":
            observed = {
                **observed,
                "okto_pulse.community": (
                    "src/okto_pulse/core/rogue.py:1",
                ),
            }
        return observed

    monkeypatch.setattr(
        dependency_ownership,
        "_source_imports",
        source_imports_with_forbidden_edge,
    )

    report = audit_distribution_dependencies(
        core_repo=CORE_REPO,
        community_repo=COMMUNITY_REPO,
    )

    assert (
        "forbidden_distribution_edge",
        "okto_pulse.community",
    ) in {(finding.code, finding.dependency) for finding in report.findings}


def test_f14_tampered_core_wheel_rejects_community_package_member(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "okto_pulse_core-0.3.2-py3-none-any.whl"
    _write_core_wheel(
        wheel,
        {
            "okto_pulse/core/clean.py": "VALUE = 'okto_pulse.community docs'\n",
            "okto_pulse/community/rogue.json": "{}",
        },
    )

    report = audit_distribution_dependencies(
        core_repo=CORE_REPO,
        community_repo=COMMUNITY_REPO,
        core_wheel=wheel,
    )

    assert any(
        finding.code == "forbidden_wheel_package_path"
        and finding.surface == "wheel"
        and finding.detail == "okto_pulse/community/rogue.json"
        for finding in report.findings
    )
    assert not any(
        finding.code == "forbidden_distribution_edge"
        and finding.surface == "wheel"
        for finding in report.findings
    )


def test_f14_tampered_core_wheel_ast_rejects_imports_but_not_literals(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "okto_pulse_core-0.3.2-py3-none-any.whl"
    _write_core_wheel(
        wheel,
        {
            "okto_pulse/core/clean_docs.py": (
                '"""okto_pulse.community is architecture documentation."""\n'
                'EXAMPLE = "from okto_pulse.community import adapter"\n'
            ),
            "okto_pulse/core/rogue.py": (
                "from typing import TYPE_CHECKING\n"
                "if TYPE_CHECKING:\n"
                "    from okto_pulse.community import adapter\n"
                "from okto_pulse import community\n"
            ),
        },
    )

    report = audit_distribution_dependencies(
        core_repo=CORE_REPO,
        community_repo=COMMUNITY_REPO,
        core_wheel=wheel,
    )

    wheel_edges = [
        finding
        for finding in report.findings
        if finding.code == "forbidden_distribution_edge"
        and finding.surface == "wheel"
    ]
    assert len(wheel_edges) == 2
    assert all("okto_pulse/core/rogue.py:" in row.detail for row in wheel_edges)
    assert not any("clean_docs.py" in row.detail for row in wheel_edges)
    assert not any(
        finding.code == "forbidden_wheel_package_path"
        for finding in report.findings
    )


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
            "pydantic>=2.12,<2.14",
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
