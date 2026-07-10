"""AF41 MCP runtime ownership gate.

The Community edition owns productive ASGI serving. Core may expose port-backed
MCP composition facades, but it must not ship or import concrete server runtime
dependencies.
"""

from __future__ import annotations

import ast
import tomllib
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .report import GateReport

FORBIDDEN_CORE_MCP_RUNTIME_DEPENDENCIES: tuple[str, ...] = (
    "fastmcp",
    "uvicorn",
    "wsproto",
)
McpRuntimeSurface = Literal["manifest", "lock", "source", "wheel"]


@dataclass(frozen=True)
class McpRuntimeOwnershipFinding:
    surface: McpRuntimeSurface
    dependency: str
    diagnostic_code: str
    location: str
    remediation: str

    def as_dict(self) -> dict[str, object]:
        return {
            "surface": self.surface,
            "dependency": self.dependency,
            "diagnostic_code": self.diagnostic_code,
            "location": self.location,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class McpRuntimeOwnershipReport:
    ok: bool
    surfaces_audited: tuple[str, ...]
    analyzed_paths: tuple[str, ...]
    findings: tuple[McpRuntimeOwnershipFinding, ...]
    allowed_source_imports: tuple[str, ...]

    @property
    def status(self) -> str:
        return "passed" if self.ok else "blocking"

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "status": self.status,
            "surfaces_audited": list(self.surfaces_audited),
            "analyzed_paths": list(self.analyzed_paths),
            "findings": [finding.as_dict() for finding in self.findings],
            "allowed_source_imports": list(self.allowed_source_imports),
            "forbidden_core_mcp_runtime_dependencies": list(
                FORBIDDEN_CORE_MCP_RUNTIME_DEPENDENCIES
            ),
            "allowed_legacy_uvicorn_import": None,
        }

    def to_gate_report(self) -> GateReport:
        return GateReport(
            gate_id="af41_mcp_runtime_ownership",
            subject="Core MCP server runtime ownership",
            status=self.status,  # type: ignore[arg-type]
            severity="high" if self.findings else "low",
            owner="okto-pulse-core/inbound-mcp" if self.findings else None,
            evidence=self.as_dict(),
            observed_value=[finding.as_dict() for finding in self.findings],
            expected_value=[],
            remediation_hint=(
                self.findings[0].remediation
                if self.findings
                else None
            ),
        )


@dataclass(frozen=True)
class _ImportReference:
    root: str
    path: Path
    line: int
    function_stack: tuple[str, ...]


class _ImportVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.function_stack: list[str] = []
        self.references: list[_ImportReference] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._record(alias.name.split(".", 1)[0], node.lineno)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level == 0 and node.module:
            self._record(node.module.split(".", 1)[0], node.lineno)

    def _record(self, root: str, line: int) -> None:
        self.references.append(
            _ImportReference(
                root=root.lower().replace("_", "-"),
                path=self.path,
                line=line,
                function_stack=tuple(self.function_stack),
            )
        )


def _default_source_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _default_repo_root() -> Path:
    return _default_source_root().parent


def _dependency_name(spec: str) -> str:
    token = spec.strip()
    for sep in ("[", ">", "<", "=", "!", "~", ";", " ", "(", ")", "@"):
        idx = token.find(sep)
        if idx != -1:
            token = token[:idx]
    return token.strip().lower().replace("_", "-")


def _project_dependency_names(pyproject_path: Path) -> set[str]:
    if not pyproject_path.exists():
        return set()
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    return {
        _dependency_name(spec)
        for spec in data.get("project", {}).get("dependencies", []) or []
        if _dependency_name(spec)
    }


def _lock_dependency_entries(
    lock_path: Path,
    *,
    package_name: str,
) -> tuple[tuple[str, str], ...] | None:
    if not lock_path.exists():
        return None
    data = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    package = next(
        (pkg for pkg in data.get("package", []) or [] if pkg.get("name") == package_name),
        None,
    )
    if package is None:
        return None
    entries: list[tuple[str, str]] = []
    for dep in package.get("dependencies", []) or []:
        name = dep.get("name")
        if name:
            entries.append((_dependency_name(name), "lock:dependencies"))
    for dep in package.get("metadata", {}).get("requires-dist", []) or []:
        name = dep.get("name")
        if name:
            entries.append((_dependency_name(name), "lock:metadata.requires-dist"))
    return tuple(entries)


def _wheel_requires_dist_names(wheel_metadata_path: Path) -> tuple[set[str] | None, str]:
    path = Path(wheel_metadata_path)
    if path.is_dir():
        path = path / "METADATA"
    if path.suffix == ".whl":
        if not path.exists():
            return None, f"wheel_not_found:{path.as_posix()}"
        try:
            with zipfile.ZipFile(path) as wheel:
                metadata_name = next(
                    (
                        name
                        for name in wheel.namelist()
                        if name.endswith(".dist-info/METADATA")
                    ),
                    None,
                )
                if metadata_name is None:
                    return None, f"wheel_metadata_not_found:{path.as_posix()}"
                text = wheel.read(metadata_name).decode("utf-8")
        except (OSError, zipfile.BadZipFile, UnicodeDecodeError) as exc:
            return None, f"wheel_metadata_unreadable:{path.as_posix()}:{type(exc).__name__}"
        source = f"wheel:{path.as_posix()}!{metadata_name}"
    else:
        if not path.exists():
            return None, f"metadata_not_found:{path.as_posix()}"
        text = path.read_text(encoding="utf-8")
        source = f"metadata:{path.as_posix()}"
    names = {
        _dependency_name(line[len("Requires-Dist:"):].strip())
        for line in text.splitlines()
        if line.startswith("Requires-Dist:")
    }
    return {name for name in names if name}, source


def _core_dir_from_source_root(source_root: Path) -> Path:
    candidate = source_root / "okto_pulse" / "core"
    if candidate.exists():
        return candidate
    return source_root


def _location(path: Path, source_root: Path, line: int) -> str:
    try:
        rel = path.relative_to(source_root).as_posix()
    except ValueError:
        rel = path.as_posix()
    return f"{rel}:{line}"


def _scan_source_imports(source_root: Path) -> tuple[list[_ImportReference], tuple[str, ...]]:
    core_dir = _core_dir_from_source_root(source_root)
    references: list[_ImportReference] = []
    analyzed: list[str] = []
    if not core_dir.exists():
        return references, tuple(analyzed)
    for path in sorted(core_dir.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        analyzed.append(path.as_posix())
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, ValueError):
            continue
        visitor = _ImportVisitor(path)
        visitor.visit(tree)
        references.extend(visitor.references)
    return references, tuple(analyzed)


def _runtime_dep_remediation(dep: str) -> str:
    return (
        f"'{dep}' is a Community-owned MCP/ASGI serving dependency. Remove it "
        "from Core packaging and declare it in the Community runtime package; "
        "Core may expose build_mcp_asgi_app/mount_mcp only."
    )


def run_mcp_runtime_ownership_gate(
    *,
    repo_root: Path | None = None,
    pyproject_path: Path | None = None,
    lock_path: Path | None = None,
    source_root: Path | None = None,
    wheel_metadata_path: Path | None = None,
    lock_package_name: str = "okto-pulse-core",
) -> McpRuntimeOwnershipReport:
    repo = repo_root or _default_repo_root()
    pyproject = pyproject_path or (repo / "pyproject.toml")
    lock = lock_path or (repo / "uv.lock")
    source = source_root or (repo / "src")

    findings: list[McpRuntimeOwnershipFinding] = []
    surfaces: list[str] = []
    analyzed: list[str] = []
    forbidden = set(FORBIDDEN_CORE_MCP_RUNTIME_DEPENDENCIES)

    surfaces.append("manifest")
    analyzed.append(pyproject.as_posix())
    for dep in sorted(_project_dependency_names(pyproject) & forbidden):
        findings.append(
            McpRuntimeOwnershipFinding(
                surface="manifest",
                dependency=dep,
                diagnostic_code="core_runtime_dependency_present",
                location="pyproject.toml:[project.dependencies]",
                remediation=_runtime_dep_remediation(dep),
            )
        )

    lock_entries = _lock_dependency_entries(lock, package_name=lock_package_name)
    if lock_entries is not None:
        surfaces.append("lock")
        analyzed.append(lock.as_posix())
        for dep, location in sorted(set(lock_entries)):
            if dep in forbidden:
                findings.append(
                    McpRuntimeOwnershipFinding(
                        surface="lock",
                        dependency=dep,
                        diagnostic_code="core_runtime_dependency_present",
                        location=location,
                        remediation=_runtime_dep_remediation(dep),
                    )
                )

    surfaces.append("source")
    imports, source_paths = _scan_source_imports(source)
    analyzed.extend(source_paths)
    for ref in imports:
        if ref.root not in forbidden:
            continue
        location = _location(ref.path, source, ref.line)
        findings.append(
            McpRuntimeOwnershipFinding(
                surface="source",
                dependency=ref.root,
                diagnostic_code="core_runtime_import_present",
                location=location,
                remediation=(
                    f"Move the '{ref.root}' import out of Core. Community owns "
                    "the concrete MCP/ASGI listener."
                ),
            )
        )

    if wheel_metadata_path is not None:
        surfaces.append("wheel")
        wheel_deps, wheel_source = _wheel_requires_dist_names(wheel_metadata_path)
        analyzed.append(wheel_source)
        if wheel_deps is not None:
            for dep in sorted(wheel_deps & forbidden):
                findings.append(
                    McpRuntimeOwnershipFinding(
                        surface="wheel",
                        dependency=dep,
                        diagnostic_code="core_runtime_dependency_present",
                        location=wheel_source,
                        remediation=(
                            _runtime_dep_remediation(dep)
                            + " Rebuild the Core wheel from the cleaned manifest."
                        ),
                    )
                )
        else:
            findings.append(
                McpRuntimeOwnershipFinding(
                    surface="wheel",
                    dependency="wheel_metadata",
                    diagnostic_code="wheel_metadata_unavailable",
                    location=wheel_source,
                    remediation="Provide a readable Core wheel or dist-info METADATA path.",
                )
            )

    return McpRuntimeOwnershipReport(
        ok=not findings,
        surfaces_audited=tuple(surfaces),
        analyzed_paths=tuple(analyzed),
        findings=tuple(findings),
        allowed_source_imports=(),
    )


def render_mcp_runtime_ownership_report(report: McpRuntimeOwnershipReport) -> str:
    status = "CONFORMANT" if report.ok else "NON-CONFORMANT"
    lines = [
        f"AF41 MCP runtime ownership: {status}",
        f"  surfaces : {', '.join(report.surfaces_audited)}",
        f"  findings : {len(report.findings)}",
    ]
    if report.allowed_source_imports:
        lines.append("  allowed_source_imports:")
        for location in report.allowed_source_imports:
            lines.append(f"    - {location}")
    for finding in report.findings:
        lines.append(
            f"  - [{finding.diagnostic_code}] {finding.surface} "
            f"{finding.dependency} @ {finding.location}"
        )
        lines.append(f"      remediation: {finding.remediation}")
    return "\n".join(lines)


__all__ = [
    "FORBIDDEN_CORE_MCP_RUNTIME_DEPENDENCIES",
    "McpRuntimeOwnershipFinding",
    "McpRuntimeOwnershipReport",
    "run_mcp_runtime_ownership_gate",
    "render_mcp_runtime_ownership_report",
]
