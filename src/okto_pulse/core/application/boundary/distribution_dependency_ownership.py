"""Executable ownership contract for Core and Community distributions.

The audit joins four independent surfaces: source imports, project manifests,
lock package metadata, and built wheel metadata. A dependency is conformant only
when all available surfaces agree with the versioned ownership ledger below.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


CORE_DISTRIBUTION = "okto-pulse-core"
COMMUNITY_DISTRIBUTION = "okto-pulse"
LEDGER_VERSION = "F14.1"
CORE_FORBIDDEN_DISTRIBUTION_IMPORT_PREFIXES: tuple[str, ...] = (
    "okto_pulse.community",
)

SourceOwner = Literal["core", "community"]
ImportMode = Literal["direct", "dynamic", "distribution"]


def normalize_distribution(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value.strip().lower())


@dataclass(frozen=True, slots=True)
class DistributionDependency:
    """One direct dependency and the source responsibility that requires it."""

    distribution: str
    declared_by: str
    source_owner: SourceOwner
    import_tokens: tuple[str, ...]
    import_mode: ImportMode
    source_paths: tuple[str, ...]
    required_extras: tuple[str, ...]
    rationale: str
    removal_criterion: str

    @property
    def normalized_distribution(self) -> str:
        return normalize_distribution(self.distribution)


@dataclass(frozen=True, slots=True)
class DependencyFinding:
    code: str
    owner: str
    surface: str
    dependency: str
    detail: str


@dataclass(frozen=True, slots=True)
class DistributionDependencyReport:
    ledger_version: str
    findings: tuple[DependencyFinding, ...]
    observed: dict[str, dict[str, tuple[str, ...]]] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.findings

    def as_dict(self) -> dict[str, object]:
        return {
            "ledger_version": self.ledger_version,
            "ok": self.ok,
            "findings": [
                {
                    "code": finding.code,
                    "owner": finding.owner,
                    "surface": finding.surface,
                    "dependency": finding.dependency,
                    "detail": finding.detail,
                }
                for finding in self.findings
            ],
            "observed": self.observed,
        }


def _entry(
    distribution: str,
    declared_by: str,
    source_owner: SourceOwner,
    import_tokens: tuple[str, ...],
    import_mode: ImportMode,
    source_paths: tuple[str, ...],
    *,
    extras: tuple[str, ...] = (),
    rationale: str,
    removal_criterion: str,
) -> DistributionDependency:
    return DistributionDependency(
        distribution=distribution,
        declared_by=declared_by,
        source_owner=source_owner,
        import_tokens=import_tokens,
        import_mode=import_mode,
        source_paths=source_paths,
        required_extras=extras,
        rationale=rationale,
        removal_criterion=removal_criterion,
    )


def build_distribution_dependency_ledger() -> tuple[DistributionDependency, ...]:
    """Return the terminal F14 direct-dependency ownership ledger."""

    core = CORE_DISTRIBUTION
    community = COMMUNITY_DISTRIBUTION
    return (
        _entry(
            "pydantic", core, "core", ("pydantic",), "direct",
            ("src/okto_pulse/core/application/use_cases/import_export.py",),
            rationale="Domain DTOs and application commands use Pydantic contracts.",
            removal_criterion="Replace all public validation models with stdlib contracts.",
        ),
        _entry(
            "pydantic-core",
            core,
            "core",
            ("pydantic_core",),
            "direct",
            ("src/okto_pulse/core/mcp/server.py",),
            rationale=(
                "The MCP argument boundary emits JSON-safe custom validation "
                "errors without embedding exception objects in error context."
            ),
            removal_criterion=(
                "Pydantic exposes an equivalent public custom-error contract, "
                "or the MCP host no longer requires the raw-envelope validator."
            ),
        ),
        _entry(
            "typing-extensions",
            core,
            "core",
            ("typing_extensions",),
            "direct",
            (
                "src/okto_pulse/core/discovery_params_schema.py",
                "src/okto_pulse/core/mcp/helpers.py",
                "src/okto_pulse/core/mcp/filters.py",
            ),
            rationale=(
                "Python 3.11 requires the backported TypedDict implementation "
                "for Pydantic-compatible runtime schema materialization."
            ),
            removal_criterion=(
                "Raise the minimum supported Python version to one whose stdlib "
                "TypedDict satisfies every supported Pydantic runtime."
            ),
        ),
        _entry(
            "PyYAML", core, "core", ("yaml",), "direct",
            ("src/okto_pulse/core/application/processors/deterministic_kg.py",),
            rationale="The deterministic processor reads the packaged taxonomy resource.",
            removal_criterion="Compile the taxonomy into a stdlib-readable packaged format.",
        ),
        _entry(
            CORE_DISTRIBUTION, community, "community", ("okto_pulse",), "distribution",
            ("src/okto_pulse/community/adapters/composition.py",),
            rationale="Community composes public Core ports and application services.",
            removal_criterion="Never while Community remains an edition of this Core.",
        ),
        _entry(
            "fastapi", community, "community", ("fastapi",), "direct",
            ("src/okto_pulse/community/api/agents.py",),
            rationale="Community owns the local HTTP inbound adapter and application host.",
            removal_criterion="Replace the Community HTTP adapter implementation.",
        ),
        _entry(
            "starlette", community, "community", ("starlette",), "direct",
            ("src/okto_pulse/community/adapters/mcp_host.py",),
            rationale="Community ASGI middleware and response adapters import Starlette.",
            removal_criterion="Remove direct Starlette usage from Community source.",
        ),
        _entry(
            "pydantic", community, "community", ("pydantic",), "direct",
            ("src/okto_pulse/community/api/allowed_transitions.py",),
            rationale="HTTP request and response schemas are Community transport models.",
            removal_criterion="Generate transport schemas without direct Pydantic usage.",
        ),
        _entry(
            "pydantic-settings", community, "community", ("pydantic_settings",), "direct",
            ("src/okto_pulse/community/config.py",),
            rationale="Community owns environment and .env parsing for local operation.",
            removal_criterion="Replace the Community environment settings loader.",
        ),
        _entry(
            "python-multipart", community, "community", ("multipart",), "dynamic",
            ("src/okto_pulse/community/api/attachments.py",),
            rationale="FastAPI loads multipart support for the attachment upload route.",
            removal_criterion="Remove multipart upload handling from the Community host.",
        ),
        _entry(
            "sqlalchemy", community, "community", ("sqlalchemy",), "direct",
            ("src/okto_pulse/community/adapters/coordination.py",), extras=("asyncio",),
            rationale="Community owns the local relational UoW and repository adapters.",
            removal_criterion="Replace all Community relational adapters with another stack.",
        ),
        _entry(
            "aiosqlite", community, "community", ("aiosqlite",), "dynamic",
            ("src/okto_pulse/community/config.py",),
            rationale="SQLAlchemy resolves the local sqlite+aiosqlite driver by URL.",
            removal_criterion="Stop supporting the local SQLite relational adapter.",
        ),
        _entry(
            "anyio", community, "community", ("anyio",), "direct",
            ("src/okto_pulse/community/adapters/storage.py",),
            rationale="The local storage adapter executes blocking filesystem work safely.",
            removal_criterion="Remove direct AnyIO use from Community adapters.",
        ),
        _entry(
            "filelock", community, "community", ("filelock",), "direct",
            (
                "src/okto_pulse/community/adapters/rebuild_audit_storage.py",
            ),
            rationale=(
                "Community owns cross-process locking for durable rebuild audit "
                "artifacts."
            ),
            removal_criterion=(
                "Replace the Community filesystem rebuild artifact store or its "
                "interprocess locking implementation."
            ),
        ),
        _entry(
            "httpx", community, "community", ("httpx",), "direct",
            ("src/okto_pulse/community/adapters/content_ingestion.py",),
            rationale="The local content ingestion adapter performs outbound HTTP reads.",
            removal_criterion="Replace or remove the Community ingestion HTTP adapter.",
        ),
        _entry(
            "fastmcp", community, "community", ("fastmcp",), "direct",
            ("src/okto_pulse/community/adapters/mcp_host.py",),
            rationale="Community owns the concrete MCP runtime host.",
            removal_criterion="Replace the Community MCP runtime implementation.",
        ),
        _entry(
            "authlib", community, "community", ("authlib",), "dynamic",
            ("src/okto_pulse/community/adapters/mcp_host.py",),
            rationale=(
                "FastMCP 2.x imports Authlib's JWT compatibility namespace; "
                "Community pins the supported 1.6 line until the host migrates "
                "to FastMCP 3 and joserfc."
            ),
            removal_criterion=(
                "Remove the compatibility pin after the concrete Community MCP "
                "host no longer loads authlib.jose."
            ),
        ),
        _entry(
            "mcp", community, "community", ("mcp",), "direct",
            ("src/okto_pulse/community/adapters/mcp_host.py",),
            rationale="Community owns the concrete MCP protocol response adapter.",
            removal_criterion="Remove direct MCP protocol-type usage from Community source.",
        ),
        _entry(
            "uvicorn", community, "community", ("uvicorn",), "direct",
            ("src/okto_pulse/community/main.py",), extras=("standard",),
            rationale="The Community CLI owns the local ASGI server process.",
            removal_criterion="Replace Uvicorn as the local Community server.",
        ),
        _entry(
            "wsproto", community, "community", ("wsproto",), "dynamic",
            ("src/okto_pulse/community/main.py",),
            rationale="The Uvicorn host selects wsproto explicitly at runtime.",
            removal_criterion="Stop selecting the wsproto WebSocket backend.",
        ),
        _entry(
            "apscheduler", community, "community", ("apscheduler",), "direct",
            ("src/okto_pulse/community/adapters/scheduler.py",),
            rationale="Community owns scheduled local worker execution.",
            removal_criterion="Replace the Community scheduler adapter.",
        ),
        _entry(
            "ladybug", community, "community", ("ladybug",), "direct",
            ("src/okto_pulse/community/adapters/kg_runtime.py",),
            rationale="Community owns the embedded local-first graph implementation.",
            removal_criterion="Replace the embedded graph adapter.",
        ),
        _entry(
            "requests", community, "community", ("requests",), "direct",
            ("src/okto_pulse/community/adapters/telemetry_sender.py",),
            rationale="The opt-in Community telemetry sender owns its HTTP transport.",
            removal_criterion="Replace the requests-based telemetry transport.",
        ),
        _entry(
            "chardet", community, "community", ("chardet",), "dynamic",
            ("src/okto_pulse/community/main.py",),
            rationale="The Community requests transport pins its charset companion explicitly.",
            removal_criterion="Remove the requests telemetry transport compatibility pin.",
        ),
        _entry(
            "sentence-transformers", community, "community",
            ("sentence_transformers",), "direct",
            ("src/okto_pulse/community/adapters/embedding.py",),
            rationale="Community owns concrete local embedding and reranking providers.",
            removal_criterion="Replace the local sentence-transformers adapters.",
        ),
    )


def _parse_requirement(spec: str) -> tuple[str, tuple[str, ...]]:
    match = re.match(r"\s*([A-Za-z0-9_.-]+)(?:\[([^]]+)\])?", spec)
    if match is None:
        return "", ()
    extras = tuple(sorted(part.strip().lower() for part in (match.group(2) or "").split(",") if part.strip()))
    return normalize_distribution(match.group(1)), extras


def _manifest_dependencies(path: Path) -> dict[str, tuple[str, ...]]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    rows: dict[str, tuple[str, ...]] = {}
    for requirement in data.get("project", {}).get("dependencies", ()) or ():
        name, extras = _parse_requirement(requirement)
        if name:
            rows[name] = extras
    return rows


def _lock_dependencies(path: Path, package_name: str) -> dict[str, tuple[str, ...]]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    package = next(
        (row for row in data.get("package", ()) if row.get("name") == package_name),
        None,
    )
    if package is None:
        return {}
    rows: dict[str, tuple[str, ...]] = {}
    for dependency in package.get("dependencies", ()) or ():
        extras = dependency.get("extra", dependency.get("extras", ())) or ()
        rows[normalize_distribution(dependency["name"])] = tuple(sorted(extras))
    return rows


def _wheel_dependencies(path: Path) -> dict[str, tuple[str, ...]]:
    with zipfile.ZipFile(path) as archive:
        metadata_name = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = archive.read(metadata_name).decode("utf-8")
    rows: dict[str, tuple[str, ...]] = {}
    for line in metadata.splitlines():
        if not line.startswith("Requires-Dist:"):
            continue
        name, extras = _parse_requirement(line.removeprefix("Requires-Dist:"))
        if name:
            rows[name] = extras
    return rows


def _ast_import_tokens(
    tree: ast.AST,
    *,
    forbidden_prefixes: tuple[str, ...] = (),
) -> tuple[tuple[str, int], ...]:
    observed: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        modules: list[str] = []
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules.append(node.module)
            modules.extend(
                prefix
                for prefix in forbidden_prefixes
                for parent, separator, child in (prefix.rpartition("."),)
                if separator
                and node.module == parent
                and any(alias.name == child for alias in node.names)
            )
        for module in modules:
            forbidden_prefix = next(
                (
                    prefix
                    for prefix in forbidden_prefixes
                    if module == prefix or module.startswith(prefix + ".")
                ),
                None,
            )
            token = forbidden_prefix or module.split(".")[0]
            if token in sys.stdlib_module_names or token == "okto_pulse":
                continue
            observed.append((token, node.lineno))
    return tuple(observed)


def _wheel_imports(
    path: Path,
    *,
    package_prefix: str,
    forbidden_prefixes: tuple[str, ...] = (),
) -> dict[str, tuple[str, ...]]:
    """AST-scan packaged Python, independent of the source checkout."""

    observed: dict[str, list[str]] = {}
    with zipfile.ZipFile(path) as archive:
        for member in sorted(archive.namelist()):
            normalized = member.replace("\\", "/")
            if not (
                normalized.startswith(package_prefix.rstrip("/") + "/")
                and normalized.endswith(".py")
            ):
                continue
            source = archive.read(member).decode("utf-8")
            tree = ast.parse(source, filename=normalized)
            for token, line in _ast_import_tokens(
                tree,
                forbidden_prefixes=forbidden_prefixes,
            ):
                observed.setdefault(token, []).append(f"{normalized}:{line}")
    return {token: tuple(rows) for token, rows in sorted(observed.items())}


def _wheel_forbidden_package_paths(
    path: Path,
    *,
    forbidden_prefixes: tuple[str, ...],
) -> tuple[str, ...]:
    with zipfile.ZipFile(path) as archive:
        return tuple(
            sorted(
                member
                for member in archive.namelist()
                if any(
                    member.replace("\\", "/") == prefix.rstrip("/")
                    or member.replace("\\", "/").startswith(
                        prefix.rstrip("/") + "/"
                    )
                    for prefix in forbidden_prefixes
                )
            )
        )


def _source_imports(
    root: Path,
    *,
    forbidden_prefixes: tuple[str, ...] = (),
) -> dict[str, tuple[str, ...]]:
    observed: dict[str, list[str]] = {}
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for token, line in _ast_import_tokens(
            tree,
            forbidden_prefixes=forbidden_prefixes,
        ):
            observed.setdefault(token, []).append(f"{path.as_posix()}:{line}")
    return {token: tuple(locations) for token, locations in sorted(observed.items())}


def _compare_surface(
    *,
    owner: str,
    surface: str,
    expected: dict[str, tuple[str, ...]],
    actual: dict[str, tuple[str, ...]],
    findings: list[DependencyFinding],
) -> None:
    for dependency in sorted(expected.keys() - actual.keys()):
        findings.append(DependencyFinding("dependency_missing", owner, surface, dependency, "Expected direct dependency is absent."))
    for dependency in sorted(actual.keys() - expected.keys()):
        findings.append(DependencyFinding("dependency_unowned", owner, surface, dependency, "Direct dependency has no ownership-ledger entry."))
    for dependency in sorted(expected.keys() & actual.keys()):
        if expected[dependency] != actual[dependency]:
            findings.append(
                DependencyFinding(
                    "dependency_extra_drift",
                    owner,
                    surface,
                    dependency,
                    f"Expected extras {expected[dependency]!r}; observed {actual[dependency]!r}.",
                )
            )


def audit_distribution_dependencies(
    *,
    core_repo: Path,
    community_repo: Path,
    core_wheel: Path | None = None,
    community_wheel: Path | None = None,
    ledger: tuple[DistributionDependency, ...] | None = None,
) -> DistributionDependencyReport:
    """Audit ownership and parity across source, manifest, lock, and wheels."""

    entries = ledger or build_distribution_dependency_ledger()
    findings: list[DependencyFinding] = []
    repos = {CORE_DISTRIBUTION: core_repo, COMMUNITY_DISTRIBUTION: community_repo}
    owners = {
        "core": core_repo / "src" / "okto_pulse" / "core",
        "community": community_repo / "src" / "okto_pulse" / "community",
    }
    wheels = {CORE_DISTRIBUTION: core_wheel, COMMUNITY_DISTRIBUTION: community_wheel}
    observed: dict[str, dict[str, tuple[str, ...]]] = {}

    ledger_keys: set[tuple[str, str]] = set()
    for entry in entries:
        key = (entry.declared_by, entry.normalized_distribution)
        if key in ledger_keys:
            findings.append(DependencyFinding("ledger_duplicate", entry.declared_by, "ledger", entry.distribution, "Duplicate distribution ownership row."))
        ledger_keys.add(key)
        if not entry.rationale.strip() or not entry.removal_criterion.strip() or not entry.source_paths:
            findings.append(DependencyFinding("ledger_incomplete", entry.declared_by, "ledger", entry.distribution, "Rationale, source paths, and removal criterion are mandatory."))
        repo = repos.get(entry.declared_by)
        if repo is None:
            findings.append(DependencyFinding("ledger_owner_unknown", entry.declared_by, "ledger", entry.distribution, "Unknown declaring distribution."))
            continue
        for source_path in entry.source_paths:
            if not (repo / source_path).exists():
                findings.append(DependencyFinding("ledger_source_missing", entry.declared_by, "ledger", entry.distribution, source_path))

    for distribution, repo in repos.items():
        expected = {
            entry.normalized_distribution: tuple(sorted(entry.required_extras))
            for entry in entries
            if entry.declared_by == distribution
        }
        manifest = _manifest_dependencies(repo / "pyproject.toml")
        lock = _lock_dependencies(repo / "uv.lock", distribution)
        observed[distribution] = {
            "expected": tuple(sorted(expected)),
            "manifest": tuple(sorted(manifest)),
            "lock": tuple(sorted(lock)),
        }
        _compare_surface(owner=distribution, surface="manifest", expected=expected, actual=manifest, findings=findings)
        _compare_surface(owner=distribution, surface="lock", expected=expected, actual=lock, findings=findings)
        wheel = wheels[distribution]
        if wheel is not None:
            if not wheel.exists():
                findings.append(DependencyFinding("wheel_missing", distribution, "wheel", distribution, wheel.as_posix()))
            else:
                wheel_dependencies = _wheel_dependencies(wheel)
                observed[distribution]["wheel"] = tuple(sorted(wheel_dependencies))
                _compare_surface(owner=distribution, surface="wheel", expected=expected, actual=wheel_dependencies, findings=findings)
                if distribution == CORE_DISTRIBUTION:
                    forbidden_paths = _wheel_forbidden_package_paths(
                        wheel,
                        forbidden_prefixes=("okto_pulse/community",),
                    )
                    observed[distribution]["wheel_forbidden_paths"] = forbidden_paths
                    for forbidden_path in forbidden_paths:
                        findings.append(
                            DependencyFinding(
                                "forbidden_wheel_package_path",
                                distribution,
                                "wheel",
                                "okto_pulse.community",
                                forbidden_path,
                            )
                        )
                    wheel_imports = _wheel_imports(
                        wheel,
                        package_prefix="okto_pulse/core",
                        forbidden_prefixes=(
                            CORE_FORBIDDEN_DISTRIBUTION_IMPORT_PREFIXES
                        ),
                    )
                    observed[distribution]["wheel_imports"] = tuple(
                        sorted(wheel_imports)
                    )
                    for prefix in CORE_FORBIDDEN_DISTRIBUTION_IMPORT_PREFIXES:
                        for location in wheel_imports.get(prefix, ()):
                            findings.append(
                                DependencyFinding(
                                    "forbidden_distribution_edge",
                                    distribution,
                                    "wheel",
                                    prefix,
                                    location,
                                )
                            )

    for owner, source_root in owners.items():
        imports = _source_imports(
            source_root,
            forbidden_prefixes=(
                CORE_FORBIDDEN_DISTRIBUTION_IMPORT_PREFIXES
                if owner == "core"
                else ()
            ),
        )
        observed.setdefault(owner, {})["source_imports"] = tuple(sorted(imports))
        import_index = {
            token: entry
            for entry in entries
            if entry.source_owner == owner and entry.import_mode == "direct"
            for token in entry.import_tokens
        }
        for token, locations in imports.items():
            if (
                owner == "core"
                and token in CORE_FORBIDDEN_DISTRIBUTION_IMPORT_PREFIXES
            ):
                findings.append(
                    DependencyFinding(
                        "forbidden_distribution_edge",
                        owner,
                        "source",
                        token,
                        locations[0],
                    )
                )
                continue
            entry = import_index.get(token)
            if entry is None:
                findings.append(DependencyFinding("source_import_unowned", owner, "source", token, locations[0]))
                continue
            manifest_owner = _manifest_dependencies(repos[entry.declared_by] / "pyproject.toml")
            if entry.normalized_distribution not in manifest_owner:
                findings.append(DependencyFinding("hidden_transitive_dependency", owner, "source", token, locations[0]))
        for token, entry in import_index.items():
            if token not in imports:
                findings.append(DependencyFinding("ledger_direct_import_stale", owner, "source", entry.distribution, token))

    return DistributionDependencyReport(
        ledger_version=LEDGER_VERSION,
        findings=tuple(findings),
        observed=observed,
    )


__all__ = [
    "COMMUNITY_DISTRIBUTION",
    "CORE_DISTRIBUTION",
    "LEDGER_VERSION",
    "DependencyFinding",
    "DistributionDependency",
    "DistributionDependencyReport",
    "audit_distribution_dependencies",
    "build_distribution_dependency_ledger",
    "normalize_distribution",
]
