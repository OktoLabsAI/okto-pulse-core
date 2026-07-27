from __future__ import annotations

import ast
import json
from pathlib import Path
import tomllib

import pytest


CORE_ROOT = Path(__file__).resolve().parents[1]


def test_af41_core_mcp_server_does_not_import_uvicorn_at_module_load() -> None:
    server_path = CORE_ROOT / "src/okto_pulse/core/mcp/server.py"
    tree = ast.parse(server_path.read_text(encoding="utf-8"))

    top_level_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.Import | ast.ImportFrom)
    ]

    for node in top_level_imports:
        if isinstance(node, ast.Import):
            imported = {alias.name for alias in node.names}
            assert "uvicorn" not in imported
        else:
            assert node.module != "uvicorn"


def test_af41_core_mcp_facade_does_not_export_legacy_runner() -> None:
    import okto_pulse.core.mcp as core_mcp

    assert "run_mcp_server" not in core_mcp.__all__
    assert not hasattr(core_mcp, "run_mcp_server")


def test_af41_legacy_runner_warns_and_rejects_core_listener_startup() -> None:
    from okto_pulse.core.mcp import server

    with pytest.warns(DeprecationWarning, match="retired"):
        with pytest.raises(RuntimeError, match="Core cannot start an MCP listener"):
            server.run_mcp_server()


def test_af41_core_mcp_runtime_ownership_gate_passes_real_repo() -> None:
    from okto_pulse.core.application.boundary.mcp_runtime_ownership_gate import (
        run_mcp_runtime_ownership_gate,
    )

    report = run_mcp_runtime_ownership_gate()

    assert report.ok, [finding.as_dict() for finding in report.findings]
    assert set(report.surfaces_audited) >= {"manifest", "lock", "source"}
    assert report.allowed_source_imports == ()


def test_af41_core_dist_wheel_metadata_does_not_require_server_runtime() -> None:
    from okto_pulse.core.application.boundary.mcp_runtime_ownership_gate import (
        run_mcp_runtime_ownership_gate,
    )

    wheels = sorted((CORE_ROOT / "dist").glob("okto_pulse_core-*.whl"))
    if not wheels:
        pytest.skip("Core wheel artifact not present")

    report = run_mcp_runtime_ownership_gate(wheel_metadata_path=wheels[-1])
    wheel_findings = [
        finding.as_dict()
        for finding in report.findings
        if finding.surface == "wheel"
    ]

    assert not wheel_findings


def test_af41_runtime_ownership_gate_fails_closed_on_core_packaging_and_source(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.application.boundary.mcp_runtime_ownership_gate import (
        run_mcp_runtime_ownership_gate,
    )

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        "[project]\n"
        'name = "synthetic-core"\n'
        'version = "0"\n'
        'dependencies = ["fastmcp>=2.0", "uvicorn[standard]>=0.27", "wsproto>=1.2"]\n',
        encoding="utf-8",
    )
    lock = tmp_path / "uv.lock"
    lock.write_text(
        "version = 1\n"
        "[[package]]\n"
        'name = "okto-pulse-core"\n'
        'version = "0"\n'
        "dependencies = [\n"
        '  { name = "fastmcp" },\n'
        '  { name = "uvicorn", extra = ["standard"] },\n'
        '  { name = "wsproto" },\n'
        "]\n"
        "[package.metadata]\n"
        "requires-dist = [\n"
        '  { name = "fastmcp", specifier = ">=2.0" },\n'
        '  { name = "uvicorn", extras = ["standard"], specifier = ">=0.27" },\n'
        '  { name = "wsproto", specifier = ">=1.2" },\n'
        "]\n",
        encoding="utf-8",
    )
    source_root = tmp_path / "src"
    core_dir = source_root / "okto_pulse" / "core"
    core_dir.mkdir(parents=True)
    (core_dir / "bad_runtime.py").write_text(
        "from fastmcp import FastMCP\nimport uvicorn\nfrom wsproto import WSConnection\n",
        encoding="utf-8",
    )
    metadata = tmp_path / "METADATA"
    metadata.write_text(
        "Metadata-Version: 2.1\n"
        "Name: okto-pulse-core\n"
        "Version: 0\n"
        "Requires-Dist: fastmcp>=2.0\n"
        "Requires-Dist: uvicorn[standard]>=0.27\n"
        "Requires-Dist: wsproto>=1.2\n",
        encoding="utf-8",
    )

    report = run_mcp_runtime_ownership_gate(
        repo_root=tmp_path,
        source_root=source_root,
        wheel_metadata_path=metadata,
    )

    assert report.ok is False
    hits = {
        (finding.surface, finding.dependency, finding.diagnostic_code)
        for finding in report.findings
    }
    for surface in ("manifest", "lock", "source", "wheel"):
        for dependency in ("fastmcp", "uvicorn", "wsproto"):
            expected_code = (
                "core_runtime_import_present"
                if surface == "source"
                else "core_runtime_dependency_present"
            )
            assert (surface, dependency, expected_code) in hits


def test_af41_core_manifest_and_lock_do_not_directly_declare_server_runtime() -> None:
    pyproject = tomllib.loads((CORE_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    deps = {
        dep.split("[", 1)[0].split(">", 1)[0].split("<", 1)[0].strip().lower()
        for dep in pyproject["project"]["dependencies"]
    }
    assert "fastmcp" not in deps
    assert "uvicorn" not in deps
    assert "wsproto" not in deps

    lock = tomllib.loads((CORE_ROOT / "uv.lock").read_text(encoding="utf-8"))
    core = next(pkg for pkg in lock["package"] if pkg["name"] == "okto-pulse-core")
    direct = {dep["name"] for dep in core.get("dependencies", [])}
    metadata = {dep["name"] for dep in core.get("metadata", {}).get("requires-dist", [])}

    assert "fastmcp" not in direct
    assert "uvicorn" not in direct
    assert "wsproto" not in direct
    assert "fastmcp" not in metadata
    assert "uvicorn" not in metadata
    assert "wsproto" not in metadata


def test_af41_boundary_cli_exposes_mcp_runtime_ownership_gate(capsys) -> None:
    from okto_pulse.core.application.boundary.cli import main

    code = main(["mcp-runtime-ownership", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["ok"] is True
    assert payload["status"] == "passed"
    assert "manifest" in payload["surfaces_audited"]
    assert "source" in payload["surfaces_audited"]
