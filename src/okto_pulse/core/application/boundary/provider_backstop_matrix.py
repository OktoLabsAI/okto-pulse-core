"""R16 provider backstop matrix and structural fake-provider audit.

R16 closes the gap between the FCC-07D runtime guard and the older registry
fail-closed tests: sanctioned fakes must stay available for tests, but productive
bootstrap/runtime code must not import ``providers.testing`` or wire the KG
registry through ``defaults_factory`` / ``_build_defaults``.

The module is core-pure: it scans source with ``ast`` and carries Community
ownership as string metadata, never as imports.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import dataclass
from importlib import util as importlib_util
from pathlib import Path
from typing import Iterable, Literal, Sequence

from .conformance_matrix import TESTING_PROVIDER_PREFIX
from .report import GateReport
from .repository_checkout import resolve_repository_checkout

ProviderBackstopPolicy = Literal[
    "sanctioned_test_only",
    "sanctioned_test_bootstrap",
    "community_owned_productive",
    "core_contract_tombstone",
]

REQUIRED_BACKSTOP_FIELDS: tuple[str, ...] = (
    "provider_key",
    "owner",
    "module",
    "policy",
    "runtime_allowed",
    "test_allowed",
    "decision_ref",
    "removal_gate",
    "migration_path",
)

REQUIRED_BACKSTOP_KEYS: frozenset[str] = frozenset(
    {
        "testing_provider_namespace",
        "kg_registry_defaults_factory",
        "kg_registry_build_defaults",
        "settings_config",
        "mcp_auth_context",
    }
)

_REGISTRY_MODULE = "okto_pulse.core.kg.interfaces.registry"
_REGISTRY_REL = "okto_pulse/core/kg/interfaces/registry.py"
_TESTING_PROVIDER_REL_PREFIX = "okto_pulse/core/kg/providers/testing/"


@dataclass(frozen=True)
class ProviderBackstopEntry:
    """One canonical row in the R16 provider ownership/backstop matrix."""

    provider_key: str
    owner: str
    module: str
    policy: ProviderBackstopPolicy
    runtime_allowed: bool
    test_allowed: bool
    decision_ref: str
    removal_gate: str
    migration_path: str
    notes: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "provider_key": self.provider_key,
            "owner": self.owner,
            "module": self.module,
            "policy": self.policy,
            "runtime_allowed": self.runtime_allowed,
            "test_allowed": self.test_allowed,
            "decision_ref": self.decision_ref,
            "removal_gate": self.removal_gate,
            "migration_path": self.migration_path,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class MatrixViolation:
    provider_key: str
    missing_fields: tuple[str, ...]
    diagnostic_code: str
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "provider_key": self.provider_key,
            "missing_fields": list(self.missing_fields),
            "diagnostic_code": self.diagnostic_code,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ProviderBackstopFinding:
    file: str
    line: int
    symbol: str
    diagnostic_code: str
    reason: str
    remediation: str

    def as_dict(self) -> dict[str, object]:
        return {
            "file": self.file,
            "line": self.line,
            "symbol": self.symbol,
            "diagnostic_code": self.diagnostic_code,
            "reason": self.reason,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class ProviderBackstopAuditReport:
    """Combined R16 matrix + source-audit report."""

    entries: tuple[ProviderBackstopEntry, ...]
    matrix_violations: tuple[MatrixViolation, ...]
    source_findings: tuple[ProviderBackstopFinding, ...]

    @property
    def ok(self) -> bool:
        return not self.matrix_violations and not self.source_findings

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "entries": [entry.as_dict() for entry in self.entries],
            "matrix_violations": [
                violation.as_dict() for violation in self.matrix_violations
            ],
            "source_findings": [
                finding.as_dict() for finding in self.source_findings
            ],
        }

    def as_gate_report(self) -> GateReport:
        status = "passed" if self.ok else "blocking"
        finding_count = len(self.matrix_violations) + len(self.source_findings)
        return GateReport(
            gate_id="R16_PROVIDER_BACKSTOP",
            subject="fake/test-provider runtime backstop",
            status=status,
            severity="high" if finding_count else "medium",
            owner="okto-pulse-core/boundary",
            evidence={
                "entries": [entry.as_dict() for entry in self.entries],
                "matrix_violations": [
                    violation.as_dict() for violation in self.matrix_violations
                ],
                "source_findings": [
                    finding.as_dict() for finding in self.source_findings
                ],
            },
            observed_value=finding_count,
            expected_value=0,
            promotion_criteria=(
                "All sanctioned fake/default providers are inventoried, and "
                "productive bootstrap code uses only composition-supplied "
                "providers."
            ),
            remediation_hint=(
                "Complete the R16 matrix and move any productive fake/default "
                "provider usage behind Community/runtime composition. Tests must "
                "use the explicit sanctioned route."
            )
            if finding_count
            else None,
        )


def build_provider_backstop_matrix() -> tuple[ProviderBackstopEntry, ...]:
    """Return the canonical R16 provider backstop matrix."""

    entries = (
        ProviderBackstopEntry(
            provider_key="testing_provider_namespace",
            owner="okto-pulse-core/testing",
            module=TESTING_PROVIDER_PREFIX,
            policy="sanctioned_test_only",
            runtime_allowed=False,
            test_allowed=True,
            decision_ref="R16:ts_c814ac43",
            removal_gate="testing_provider_policy + runtime_composition_guard",
            migration_path=(
                "Tests may import providers.testing explicitly; productive "
                "runtime must receive registered composition providers."
            ),
        ),
        ProviderBackstopEntry(
            provider_key="kg_registry_defaults_factory",
            owner="okto-pulse-core/testing",
            module=f"{_REGISTRY_MODULE}.configure_kg_registry(defaults_factory=...)",
            policy="sanctioned_test_bootstrap",
            runtime_allowed=False,
            test_allowed=True,
            decision_ref="R16:ts_5ec04dd2",
            removal_gate="provider_backstop_matrix.audit_source",
            migration_path=(
                "Real runtime calls configure_kg_registry(base_registry=...); "
                "tests use defaults_factory explicitly through test helpers."
            ),
        ),
        ProviderBackstopEntry(
            provider_key="kg_registry_build_defaults",
            owner="okto-pulse-core/testing",
            module=f"{_REGISTRY_MODULE}._build_defaults",
            policy="sanctioned_test_bootstrap",
            runtime_allowed=False,
            test_allowed=True,
            decision_ref="R16:ts_5ec04dd2",
            removal_gate="provider_backstop_matrix.audit_source",
            migration_path=(
                "_build_defaults remains a test-only fake builder; production "
                "modules must never import or call it."
            ),
        ),
        ProviderBackstopEntry(
            provider_key="settings_config",
            owner="okto-pulse-community/data",
            module="okto_pulse.community.adapters.data.CommunityKGConfig",
            policy="community_owned_productive",
            runtime_allowed=True,
            test_allowed=True,
            decision_ref="R07/R16:ts_335131b5",
            removal_gate="data_provider_ownership_gate",
            migration_path=(
                "Community supplies KGConfig in the real registry; the old "
                "SettingsKGConfig core concrete is allowed only inside the "
                "test-only _build_defaults route."
            ),
            notes=(
                "Legacy core concrete: "
                "okto_pulse.core.kg.providers.testing.settings_config.SettingsKGConfig."
            ),
        ),
        ProviderBackstopEntry(
            provider_key="mcp_auth_context",
            owner="okto-pulse-community/inbound-mcp",
            module="okto_pulse.community.adapters.mcp_auth.MCPAuthContext",
            policy="core_contract_tombstone",
            runtime_allowed=True,
            test_allowed=True,
            decision_ref="R06/R16:ts_335131b5",
            removal_gate="mcp_credential_usage_gate + adapter_readiness_inventory",
            migration_path=(
                "Core keeps AuthContext contracts/tombstone only; Community "
                "registers auth_context_factory, and consumers fail closed when "
                "it is absent."
            ),
        ),
    )
    return tuple(sorted(entries, key=lambda entry: entry.provider_key))


def validate_provider_backstop_matrix(
    entries: Iterable[ProviderBackstopEntry] | None = None,
) -> tuple[MatrixViolation, ...]:
    """Return matrix violations for missing keys, duplicates, and empty fields."""

    rows = tuple(entries if entries is not None else build_provider_backstop_matrix())
    violations: list[MatrixViolation] = []
    keys = [row.provider_key for row in rows]
    counts = Counter(keys)

    for key in sorted(REQUIRED_BACKSTOP_KEYS - set(keys)):
        violations.append(
            MatrixViolation(
                provider_key=key,
                missing_fields=(),
                diagnostic_code="missing_matrix_row",
                reason=f"Required R16 provider backstop row is missing: {key}.",
            )
        )

    for key, count in sorted(counts.items()):
        if count > 1:
            violations.append(
                MatrixViolation(
                    provider_key=key,
                    missing_fields=(),
                    diagnostic_code="duplicate_matrix_row",
                    reason=f"R16 provider backstop row is duplicated: {key}.",
                )
            )

    for row in rows:
        missing = tuple(
            field
            for field in REQUIRED_BACKSTOP_FIELDS
            if _is_empty(getattr(row, field))
        )
        if missing:
            violations.append(
                MatrixViolation(
                    provider_key=row.provider_key or "<missing>",
                    missing_fields=missing,
                    diagnostic_code="incomplete_matrix_row",
                    reason=(
                        "R16 provider backstop row is structurally incomplete: "
                        f"{row.provider_key or '<missing>'}."
                    ),
                )
            )
    return tuple(violations)


def audit_source_for_provider_backstop_leaks(
    source_roots: Sequence[str | Path] | None = None,
) -> tuple[ProviderBackstopFinding, ...]:
    """Audit productive source for test-only provider/default bootstrap usage."""

    configured_roots = default_source_roots() if source_roots is None else source_roots
    roots = tuple(Path(root) for root in configured_roots)
    findings: list[ProviderBackstopFinding] = []
    for root in roots:
        if not root.exists():
            findings.append(
                ProviderBackstopFinding(
                    file=str(root),
                    line=0,
                    symbol="<source_root>",
                    diagnostic_code="source_root_missing",
                    reason="Configured source root does not exist.",
                    remediation="Point the R16 backstop gate at an existing src root.",
                )
            )
            continue
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = _relative_posix(path, root)
            if _is_test_path(rel) or rel.startswith(_TESTING_PROVIDER_REL_PREFIX):
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (OSError, SyntaxError) as exc:
                findings.append(
                    ProviderBackstopFinding(
                        file=rel,
                        line=getattr(exc, "lineno", 0) or 0,
                        symbol="<parse>",
                        diagnostic_code="source_parse_error",
                        reason=f"Could not parse source for R16 provider audit: {exc}.",
                        remediation="Fix the syntax/read error so the audit can run.",
                    )
                )
                continue
            visitor = _ProviderBackstopVisitor(rel)
            visitor.visit(tree)
            findings.extend(visitor.findings)
    return tuple(sorted(findings, key=lambda f: (f.file, f.line, f.symbol)))


def audit_provider_backstop(
    *,
    entries: Iterable[ProviderBackstopEntry] | None = None,
    source_roots: Sequence[str | Path] | None = None,
) -> ProviderBackstopAuditReport:
    rows = tuple(entries if entries is not None else build_provider_backstop_matrix())
    return ProviderBackstopAuditReport(
        entries=tuple(sorted(rows, key=lambda entry: entry.provider_key)),
        matrix_violations=validate_provider_backstop_matrix(rows),
        source_findings=audit_source_for_provider_backstop_leaks(source_roots),
    )


def run_provider_backstop_gate(
    *,
    entries: Iterable[ProviderBackstopEntry] | None = None,
    source_roots: Sequence[str | Path] | None = None,
) -> GateReport:
    """Run the R16 gate and return the shared boundary ``GateReport``."""

    return audit_provider_backstop(
        entries=entries, source_roots=source_roots
    ).as_gate_report()


def default_source_roots() -> tuple[Path, ...]:
    """Return core src plus importable Community src, de-duplicated."""

    roots: list[Path] = [Path(__file__).resolve().parents[4]]
    community = _community_src_root()
    if community is not None:
        roots.append(community)
    unique: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        resolved = root.resolve()
        if resolved in seen:
            continue
        unique.append(resolved)
        seen.add(resolved)
    return tuple(unique)


class _ProviderBackstopVisitor(ast.NodeVisitor):
    def __init__(self, rel: str) -> None:
        self.rel = rel
        self.function_stack: list[str] = []
        self.findings: list[ProviderBackstopFinding] = []

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
            if _is_testing_provider_module(alias.name) and not self._allow_testing_import():
                self._add_testing_provider_finding(node.lineno, alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if _is_testing_provider_module(module) and not self._allow_testing_import():
            self._add_testing_provider_finding(node.lineno, module)
        if module == _REGISTRY_MODULE and not self._in_registry_module():
            for alias in node.names:
                if alias.name == "_build_defaults":
                    self.findings.append(
                        ProviderBackstopFinding(
                            file=self.rel,
                            line=node.lineno,
                            symbol="_build_defaults",
                            diagnostic_code="production_build_defaults_import",
                            reason=(
                                "Production source imports the test-only KG "
                                "_build_defaults fake builder."
                            ),
                            remediation=(
                                "Use configure_kg_registry(base_registry=...) in "
                                "productive composition; keep _build_defaults in "
                                "test helpers only."
                            ),
                        )
                    )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        call_name = _call_name(node.func)
        if call_name.endswith("._build_defaults") or call_name == "_build_defaults":
            if not self._allow_testing_import():
                self.findings.append(
                    ProviderBackstopFinding(
                        file=self.rel,
                        line=node.lineno,
                        symbol="_build_defaults",
                        diagnostic_code="production_build_defaults_call",
                        reason=(
                            "Production source calls the test-only KG "
                            "_build_defaults fake builder."
                        ),
                        remediation=(
                            "Build a real base_registry in the composition root; "
                            "do not call test fake builders in runtime code."
                        ),
                    )
                )
        if call_name.endswith("configure_kg_registry") and any(
            keyword.arg == "defaults_factory" for keyword in node.keywords
        ):
            if not self._in_registry_module():
                self.findings.append(
                    ProviderBackstopFinding(
                        file=self.rel,
                        line=node.lineno,
                        symbol="defaults_factory",
                        diagnostic_code="production_defaults_factory_bootstrap",
                        reason=(
                            "Production source configures the KG registry through "
                            "defaults_factory, the sanctioned test-only route."
                        ),
                        remediation=(
                            "Use configure_kg_registry(base_registry=...) from "
                            "Community/runtime composition."
                        ),
                    )
                )
        self.generic_visit(node)

    def _in_registry_module(self) -> bool:
        return self.rel == _REGISTRY_REL

    def _allow_testing_import(self) -> bool:
        return self.rel == _REGISTRY_REL and self.function_stack == ["_build_defaults"]

    def _add_testing_provider_finding(self, line: int, module: str) -> None:
        self.findings.append(
            ProviderBackstopFinding(
                file=self.rel,
                line=line,
                symbol=module,
                diagnostic_code="production_testing_provider_import",
                reason=(
                    "Production source imports the sanctioned test-only provider "
                    f"namespace {TESTING_PROVIDER_PREFIX}."
                ),
                remediation=(
                    "Move the provider behind composition or into a test helper. "
                    "Only tests and registry._build_defaults may import "
                    "providers.testing."
                ),
            )
        )


def _community_src_root() -> Path | None:
    # A configured/current workspace checkout takes precedence during
    # cross-repository validation. Legacy checkout names are a last resort.
    # A normal installed package has no ``src`` ancestor, so returning
    # ``package.parents[1]`` there would scan the entire site-packages tree.
    core_repo = Path(__file__).resolve().parents[5]
    checkout = resolve_repository_checkout(
        "community",
        anchor_repo=core_repo,
        required=False,
    )
    if checkout is not None:
        return checkout.source_root

    spec = importlib_util.find_spec("okto_pulse.community")
    if spec is None or spec.submodule_search_locations is None:
        return None
    for location in spec.submodule_search_locations:
        package = Path(location).resolve()
        if package.exists() and package.name == "community":
            source_root = package.parents[1]
            if (source_root / "okto_pulse" / "community").is_dir():
                return source_root
            # Wheel installation: constrain the scan to the distribution
            # package, never its full site-packages parent.
            return package.parent
    return None


def _relative_posix(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _is_test_path(rel: str) -> bool:
    parts = rel.split("/")
    return "tests" in parts or any(part.startswith("test_") for part in parts)


def _is_testing_provider_module(module: str) -> bool:
    return module == TESTING_PROVIDER_PREFIX or module.startswith(
        TESTING_PROVIDER_PREFIX + "."
    )


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _call_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _is_empty(value: object) -> bool:
    if isinstance(value, str):
        return value.strip() == ""
    return value is None


__all__ = [
    "ProviderBackstopPolicy",
    "REQUIRED_BACKSTOP_FIELDS",
    "REQUIRED_BACKSTOP_KEYS",
    "ProviderBackstopEntry",
    "MatrixViolation",
    "ProviderBackstopFinding",
    "ProviderBackstopAuditReport",
    "build_provider_backstop_matrix",
    "validate_provider_backstop_matrix",
    "audit_source_for_provider_backstop_leaks",
    "audit_provider_backstop",
    "run_provider_backstop_gate",
    "default_source_roots",
]
