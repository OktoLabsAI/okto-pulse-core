"""The four core-pure boundary gates (spec #12, card 19f982ec).

- :class:`PackageManifestGate`     — api_8c46772c
- :class:`ImportBoundaryGate`      — api_24add273
- :class:`DependencyAuditGate`     — api_591b0aeb
- :class:`ImportSideEffectSmokeGate` — api_f406228c

Each returns a deterministic :class:`GateReport`. The gates perform only static
analysis (``ast`` / ``tomllib`` / ``pathlib``) or a controlled subprocess import;
they never initialise a runtime. The Community rebuild/core-smoke-install gates
are a SEPARATE card (bed19910) and are intentionally not implemented here.
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from .import_matrix import (
    MATRIX_VERSION,
    AllowDenyRule,
    rule_for,
)
from .layer_resolver import (
    APPLICATION,
    DOMAIN,
    FUTURE_TARGET,
    LAYER_RESOLVER_VERSION,
    PORTS,
    UNCLASSIFIED,
    LayerResolver,
)
from .report import GateException, GateReport, GateStatus, enforce_exception_policy

#: ``okto_pulse.tools`` is edition-owned and must not ship in the Core package.
TOOLS_LOC_BASELINE = 0

#: Versioned default dependency policy for the core-pure target (api_591b0aeb).
#: ``forbidden`` are transport/persistence/scheduler frameworks the core-pure
#: package must move behind adapter editions; until then they are bootstrap debt.
DEFAULT_CORE_PURE_FORBIDDEN_DEPENDENCIES: tuple[str, ...] = (
    "fastapi",
    "starlette",
    "uvicorn",
    "wsproto",
    "fastmcp",
    "mcp",
    "sqlalchemy",
    "aiosqlite",
    "asyncpg",
    "ladybug",
    "apscheduler",
)

#: Status precedence, worst first, for aggregating many findings into one report.
_STATUS_RANK: dict[str, int] = {
    "reject": 4,
    "blocking": 3,
    "xfail_advisory": 2,
    "baseline": 1,
    "passed": 0,
}


def _worst(statuses: list[str]) -> GateStatus:
    if not statuses:
        return "passed"
    return max(statuses, key=lambda s: _STATUS_RANK.get(s, 0))  # type: ignore[return-value]


#: Layers that must be clean IMMEDIATELY — their violations stay ``blocking`` even
#: in bootstrap mode. Adapter layers (inbound/outbound/event) carry known
#: pre-refactor coupling that bootstrap records as ``baseline`` debt.
_PURE_LAYERS: frozenset[str] = frozenset({DOMAIN, APPLICATION, PORTS, FUTURE_TARGET})

_BOOTSTRAP_PROMOTION = (
    "Strangler debt: migrate this adapter onto a port before the gate runs in "
    "blocking mode; tracked as baseline until then."
)

_RELATIONAL_BASELINE_CATEGORIES: frozenset[str] = frozenset(
    {"sqlalchemy", "aiosqlite", "asyncpg"}
)


@dataclass(frozen=True)
class ImportBaselineLedgerEntry:
    owner: str
    reason: str
    removal_criterion: str
    spec_or_wave: str
    risk: str


IMPORT_BOUNDARY_BASELINE_LEDGER: dict[str, ImportBaselineLedgerEntry] = {
}


def _default_source_root() -> Path:
    # ``.../src/okto_pulse/core/application/boundary/gates.py`` -> ``.../src``
    return Path(__file__).resolve().parents[4]


# --------------------------------------------------------------------------- #
# import_boundary
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ImportViolation:
    file: str
    line: int
    layer: str
    imported: str
    rule: str
    status: str
    owner: str | None
    promotion_criteria: str | None
    remediation_hint: str
    baseline_key: str | None = None
    baseline_reason: str | None = None
    removal_criterion: str | None = None
    spec_or_wave: str | None = None
    risk: str | None = None


@dataclass(frozen=True)
class ImportBoundaryGateInput:
    source_root: Path | None = None
    mode: str = "bootstrap"  # bootstrap | blocking


class ImportBoundaryGate:
    """Static import-matrix gate over the current source tree (api_24add273)."""

    gate_id = "import_boundary"

    def __init__(self, resolver: LayerResolver | None = None) -> None:
        self._resolver = resolver or LayerResolver()

    def run(self, gate_input: ImportBoundaryGateInput | None = None) -> GateReport:
        gate_input = gate_input or ImportBoundaryGateInput()
        root = Path(gate_input.source_root) if gate_input.source_root else _default_source_root()
        core_root = root / "okto_pulse"
        violations: list[ImportViolation] = []
        unclassified: set[str] = set()
        scanned = 0
        for path in sorted(core_root.rglob("*.py")) if core_root.exists() else []:
            if "__pycache__" in path.parts:
                continue
            try:
                rel = path.relative_to(root).as_posix()
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (OSError, SyntaxError, ValueError):
                continue
            scanned += 1
            resolution = self._resolver.resolve(rel)
            if resolution.layer == UNCLASSIFIED:
                unclassified.add(rel)
            rule = rule_for(resolution.layer)
            if rule is None:
                continue
            violations.extend(self._scan_file(rel, resolution.layer, tree, rule))
        if gate_input.mode == "bootstrap":
            violations = [self._bootstrap_downgrade(v) for v in violations]
        statuses = [v.status for v in violations]
        if unclassified:
            statuses.append("blocking")
        status = _worst(statuses)
        evidence = {
            "violations": [self._violation_dict(v) for v in violations],
            "unclassified_layers": sorted(unclassified),
            "matrix_version": MATRIX_VERSION,
            "layer_resolver_version": LAYER_RESOLVER_VERSION,
            "scanned_files": scanned,
            "mode": gate_input.mode,
        }
        return GateReport(
            gate_id=self.gate_id,
            subject="core import boundary matrix",
            status=status,
            severity="high" if status in ("blocking", "reject") else "medium",
            owner="okto-pulse-core/architecture",
            evidence=evidence,
            promotion_criteria=(
                "Resolve every blocking import-matrix violation and classify all "
                "unclassified paths before the gate can be promoted to passed."
            )
            if status != "passed"
            else None,
            observed_value=(
                len([v for v in violations if v.status == "blocking"])
                + len(unclassified)
            ),
            expected_value=0,
            remediation_hint=(
                "Move framework/persistence imports out of pure layers; depend on "
                "ports and wire concretes only in the composition root."
            )
            if status in ("blocking", "reject")
            else None,
        )

    def _scan_file(
        self, rel: str, layer: str, tree: ast.AST, rule: AllowDenyRule
    ) -> list[ImportViolation]:
        out: list[ImportViolation] = []
        # Imports guarded by ``if TYPE_CHECKING:`` are compile-time only — they are
        # type-hint references, not runtime coupling — so a pure layer may use a
        # concrete type purely for annotations without violating the matrix.
        type_checking_imports = self._type_checking_import_nodes(tree)
        for node in ast.walk(tree):
            if id(node) in type_checking_imports:
                continue
            if isinstance(node, ast.Import):
                for alias in node.names:
                    out.extend(self._check_module(rel, layer, rule, alias.name, node.lineno))
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level == 0 and module:
                    out.extend(self._check_module(rel, layer, rule, module, node.lineno))
                for alias in node.names:
                    if alias.name in rule.forbidden_symbols:
                        out.append(
                            self._violation(
                                rel, layer, rule,
                                f"from {module} import {alias.name}",
                                node.lineno, "forbidden_symbol",
                            )
                        )
        return out

    @staticmethod
    def _type_checking_import_nodes(tree: ast.AST) -> set[int]:
        """ids of Import/ImportFrom nodes nested under ``if TYPE_CHECKING:``."""
        guarded: set[int] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.If) and ImportBoundaryGate._is_type_checking_test(node.test):
                for child in node.body:
                    for sub in ast.walk(child):
                        if isinstance(sub, (ast.Import, ast.ImportFrom)):
                            guarded.add(id(sub))
        return guarded

    @staticmethod
    def _is_type_checking_test(test: ast.expr) -> bool:
        if isinstance(test, ast.Name):
            return test.id == "TYPE_CHECKING"
        if isinstance(test, ast.Attribute):
            return test.attr == "TYPE_CHECKING"
        return False

    def _check_module(
        self, rel: str, layer: str, rule: AllowDenyRule, dotted: str, line: int
    ) -> list[ImportViolation]:
        out: list[ImportViolation] = []
        root = dotted.split(".")[0]
        if root in rule.forbidden_import_roots:
            out.append(self._violation(rel, layer, rule, dotted, line, "forbidden_import_root"))
        if any(dotted == p or dotted.startswith(p + ".") for p in rule.forbidden_module_prefixes):
            out.append(self._violation(rel, layer, rule, dotted, line, "forbidden_module_prefix"))
        if rule.forbidden_target_layers and dotted.startswith("okto_pulse"):
            target = self._resolver.resolve_module(dotted)
            if target.layer in rule.forbidden_target_layers:
                out.append(
                    self._violation(rel, layer, rule, dotted, line, f"forbidden_target_layer:{target.layer}")
                )
        return out

    @staticmethod
    def _bootstrap_downgrade(v: ImportViolation) -> ImportViolation:
        """In bootstrap mode, adapter-layer blocking debt is recorded as baseline.

        Pure layers (domain/application/ports) are never downgraded — they must be
        clean from day one.
        """
        if v.status != "blocking" or v.layer in _PURE_LAYERS:
            return v
        key = ImportBoundaryGate._baseline_key(v)
        category = ImportBoundaryGate._violation_category(v)
        if category in _RELATIONAL_BASELINE_CATEGORIES:
            return ImportViolation(
                file=v.file,
                line=v.line,
                layer=v.layer,
                imported=v.imported,
                rule=v.rule,
                status="baseline",
                owner=v.owner or "okto-pulse-core/architecture",
                promotion_criteria=v.promotion_criteria or _BOOTSTRAP_PROMOTION,
                remediation_hint=v.remediation_hint,
                baseline_key=key,
                baseline_reason="relational ratchet baseline preserved by R01B/R01C",
                removal_criterion=v.promotion_criteria or _BOOTSTRAP_PROMOTION,
                spec_or_wave="R01B/R01C relational ratchet",
                risk="tracked relational coupling debt",
            )
        entry = IMPORT_BOUNDARY_BASELINE_LEDGER.get(key)
        if entry is None:
            return ImportViolation(
                file=v.file,
                line=v.line,
                layer=v.layer,
                imported=v.imported,
                rule=v.rule,
                status="blocking",
                owner=None,
                promotion_criteria=None,
                remediation_hint=(
                    f"Missing non-relational import-boundary ledger entry for {key}. "
                    "Add owner, reason, removal criterion, source spec/wave and risk "
                    "before accepting this baseline."
                ),
                baseline_key=key,
            )
        return ImportViolation(
            file=v.file,
            line=v.line,
            layer=v.layer,
            imported=v.imported,
            rule=v.rule,
            status="baseline",
            owner=entry.owner,
            promotion_criteria=entry.removal_criterion,
            remediation_hint=v.remediation_hint,
            baseline_key=key,
            baseline_reason=entry.reason,
            removal_criterion=entry.removal_criterion,
            spec_or_wave=entry.spec_or_wave,
            risk=entry.risk,
        )

    @staticmethod
    def _violation(
        rel: str, layer: str, rule: AllowDenyRule, imported: str, line: int, kind: str
    ) -> ImportViolation:
        return ImportViolation(
            file=rel,
            line=line,
            layer=layer,
            imported=imported,
            rule=kind,
            status=rule.status_on_violation,
            owner=rule.owner,
            promotion_criteria=rule.promotion_criteria,
            remediation_hint=(
                f"'{imported}' is prohibited for layer '{layer}' ({kind}). Depend on a "
                f"port/abstraction and wire the concrete in the composition root."
            ),
        )

    @staticmethod
    def _violation_dict(v: ImportViolation) -> dict:
        return {
            "file": v.file,
            "line": v.line,
            "layer": v.layer,
            "imported": v.imported,
            "category": ImportBoundaryGate._violation_category(v),
            "rule": v.rule,
            "status": v.status,
            "owner": v.owner,
            "promotion_criteria": v.promotion_criteria,
            "baseline_key": v.baseline_key or ImportBoundaryGate._baseline_key(v),
            "baseline_reason": v.baseline_reason,
            "removal_criterion": v.removal_criterion,
            "retirement_criterion": v.removal_criterion,
            "source_spec": v.spec_or_wave,
            "spec_or_wave": v.spec_or_wave,
            "risk": v.risk,
            "remediation_hint": v.remediation_hint,
        }

    @staticmethod
    def _baseline_key(v: ImportViolation) -> str:
        return f"{v.file}|{v.imported}|{v.rule}"

    @staticmethod
    def _violation_category(v: ImportViolation) -> str:
        imported = v.imported
        if imported.startswith("okto_pulse.core.kg"):
            return "kg"
        if imported.startswith("okto_pulse.core.models.schemas"):
            return "schemas"
        if imported.startswith("okto_pulse.core.infra.permissions"):
            return "permissions"
        if imported.startswith("sqlalchemy"):
            return "sqlalchemy"
        if v.rule.startswith("forbidden_target_layer:"):
            return v.rule.removeprefix("forbidden_target_layer:")
        if v.rule == "forbidden_import_root":
            return imported.split(".", 1)[0]
        return v.rule


# --------------------------------------------------------------------------- #
# package_manifest
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PackageManifestGateInput:
    pyproject_path: Path | None = None
    source_root: Path | None = None
    tools_loc_baseline: int = TOOLS_LOC_BASELINE
    wheel_record_path: Path | None = None
    exception: GateException | None = None
    today: date | None = None


class PackageManifestGate:
    """Audits the wheel/build manifest and keeps edition tools out of Core."""

    gate_id = "package_manifest"

    def run(self, gate_input: PackageManifestGateInput | None = None) -> GateReport:
        gate_input = gate_input or PackageManifestGateInput()
        root = Path(gate_input.source_root) if gate_input.source_root else _default_source_root()
        pyproject = (
            Path(gate_input.pyproject_path)
            if gate_input.pyproject_path
            else root.parent / "pyproject.toml"
        )
        if not pyproject.exists():
            return GateReport(
                gate_id=self.gate_id,
                subject="core wheel/build manifest",
                status="blocking",
                severity="high",
                evidence={"error": "manifest_not_found", "pyproject_path": str(pyproject)},
                remediation_hint="pyproject.toml could not be read.",
            )
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        wheel = (
            data.get("tool", {}).get("hatch", {}).get("build", {}).get("targets", {}).get("wheel", {})
        )
        force_includes = sorted((wheel.get("force-include") or {}).keys())
        packages = wheel.get("packages") or []
        # tr_a711faf8 / ac_9d655671: read the installed wheel's dist-info/RECORD
        # when supplied, list the included files and flag unexpected runtime files.
        # A requested-but-unreadable RECORD fails closed, never silent pass.
        wheel_files: list[str] | None = None
        unexpected: list[str] = []
        if gate_input.wheel_record_path is not None:
            record = Path(gate_input.wheel_record_path)
            if not record.exists():
                return GateReport(
                    gate_id=self.gate_id,
                    subject="core wheel RECORD",
                    status="blocking",
                    severity="high",
                    evidence={
                        "error": "manifest_not_found",
                        "wheel_record_path": str(record),
                        "force_includes": force_includes,
                    },
                    observed_value=str(record),
                    remediation_hint="dist-info/RECORD of the installed wheel could not be read.",
                )
            wheel_files = self._parse_record(record)
            unexpected = self._unexpected_runtime_files(wheel_files)
        tools_dir = root / "okto_pulse" / "tools"
        observed_loc, tools_files = self._count_loc(tools_dir)
        expected = gate_input.tools_loc_baseline
        evidence = {
            "force_includes": force_includes,
            "packages": packages,
            "tools_files": tools_files,
            "tools_classification": "ops/edition",
            "tools_loc_observed": observed_loc,
            "tools_loc_expected": expected,
            "wheel_record_audited": wheel_files is not None,
            "wheel_files": wheel_files,
        }
        if unexpected:
            return GateReport(
                gate_id=self.gate_id,
                subject="core wheel included files",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "unexpected_runtime_file"},
                observed_value=unexpected,
                expected_value=[],
                remediation_hint=(
                    "A non-okto_pulse runtime file was included in the core wheel; "
                    "exclude it or declare it as an explicit force-include exception."
                ),
            )
        if observed_loc != expected:
            report = GateReport(
                gate_id=self.gate_id,
                subject="edition-owned tools present in Core",
                status="blocking",
                severity="medium",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "tools_loc_drift"},
                exception=gate_input.exception,
                promotion_criteria=(
                    "Remove edition-owned tools from Core or supply a valid temporary "
                    "exception (owner/deadline/promotion_criteria)."
                ),
                observed_value=observed_loc,
                expected_value=expected,
                remediation_hint="move_tools_to_edition_adapter",
            )
            return enforce_exception_policy(report, today=gate_input.today or date.today())
        return GateReport(
            gate_id=self.gate_id,
            subject="core wheel/build manifest",
            status="passed",
            severity="low",
            owner="okto-pulse-core/architecture",
            evidence=evidence,
            observed_value=observed_loc,
            expected_value=expected,
        )

    @staticmethod
    def _count_loc(tools_dir: Path) -> tuple[int, dict[str, int]]:
        per_file: dict[str, int] = {}
        if not tools_dir.exists():
            return 0, per_file
        total = 0
        for path in sorted(tools_dir.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            try:
                lines = len(path.read_text(encoding="utf-8").splitlines())
            except OSError:
                continue
            per_file[path.name] = lines
            total += lines
        return total, per_file

    @staticmethod
    def _parse_record(record: Path) -> list[str]:
        """Parse a wheel ``dist-info/RECORD`` (CSV ``path,hash,size``) -> paths."""
        files: list[str] = []
        try:
            for raw in record.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if line:
                    files.append(line.split(",", 1)[0])
        except OSError:
            return []
        return sorted(files)

    @staticmethod
    def _unexpected_runtime_files(wheel_files: list[str]) -> list[str]:
        """Runtime ``.py`` shipped outside the ``okto_pulse`` package / metadata."""
        out: list[str] = []
        for entry in wheel_files:
            top = entry.replace("\\", "/").split("/", 1)[0]
            if top.startswith("okto_pulse") or top.endswith((".dist-info", ".data")):
                continue
            if entry.endswith(".py"):
                out.append(entry)
        return sorted(out)


# --------------------------------------------------------------------------- #
# dependency_audit
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DependencyAuditGateInput:
    pyproject_path: Path | None = None
    dependency_policy_ref: str | None = None
    allowed_dependencies: list[str] | None = None
    forbidden_dependencies: list[str] | None = None
    mode: str = "blocking"  # bootstrap | blocking


class DependencyAuditGate:
    """Validates declared deps against an EXPLICIT catalog (api_591b0aeb).

    No catalog => ``dependency_policy_missing`` (never infer a core-pure group).
    """

    gate_id = "dependency_audit"

    def run(self, gate_input: DependencyAuditGateInput) -> GateReport:
        root = _default_source_root()
        pyproject = (
            Path(gate_input.pyproject_path)
            if gate_input.pyproject_path
            else root.parent / "pyproject.toml"
        )
        has_policy = bool(
            gate_input.dependency_policy_ref
            or gate_input.allowed_dependencies
            or gate_input.forbidden_dependencies
        )
        if not has_policy:
            return GateReport(
                gate_id=self.gate_id,
                subject="core dependency policy",
                status="blocking",
                severity="high",
                evidence={"error": "dependency_policy_missing"},
                remediation_hint=(
                    "Supply an explicit allowed/forbidden dependency catalog or "
                    "dependency_policy_ref; do not assume a core-pure group exists."
                ),
            )
        if not pyproject.exists():
            return GateReport(
                gate_id=self.gate_id,
                subject="core dependency manifest",
                status="blocking",
                severity="high",
                evidence={"error": "dependency_manifest_missing", "pyproject_path": str(pyproject)},
            )
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        declared = self._dependency_names(data.get("project", {}).get("dependencies") or [])
        forbidden = set(gate_input.forbidden_dependencies or [])
        allowed = set(gate_input.allowed_dependencies or [])
        forbidden_matches = sorted(declared & forbidden)
        unclassified = sorted(declared - allowed - forbidden) if allowed else []
        evidence = {
            "dependencies": sorted(declared),
            "forbidden_matches": forbidden_matches,
            "unclassified_dependencies": unclassified,
            "policy_ref": gate_input.dependency_policy_ref or "inline",
        }
        if forbidden_matches:
            bootstrap = gate_input.mode == "bootstrap"
            return GateReport(
                gate_id=self.gate_id,
                subject="core forbidden dependency",
                status="baseline" if bootstrap else "blocking",
                severity="medium" if bootstrap else "high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "forbidden_dependency", "mode": gate_input.mode},
                observed_value=forbidden_matches,
                expected_value=[],
                promotion_criteria=(
                    "Strangler debt: move each forbidden runtime dependency behind an "
                    "adapter edition before the gate runs in blocking mode."
                )
                if bootstrap
                else None,
                remediation_hint="Remove the forbidden runtime dependency or move it behind an adapter edition.",
            )
        status: GateStatus = "xfail_advisory" if unclassified else "passed"
        return GateReport(
            gate_id=self.gate_id,
            subject="core dependency policy",
            status=status,
            severity="low",
            owner="okto-pulse-core/architecture" if unclassified else None,
            evidence=evidence if not unclassified else {**evidence, "error": "unclassified_dependency"},
            promotion_criteria="Classify every dependency in the policy catalog." if unclassified else None,
        )

    @staticmethod
    def _dependency_names(deps: list[str]) -> set[str]:
        names: set[str] = set()
        for spec in deps:
            token = spec.strip()
            for sep in ("[", ">", "<", "=", "!", "~", ";", " "):
                idx = token.find(sep)
                if idx != -1:
                    token = token[:idx]
            if token:
                names.add(token.strip().lower())
        return names


# --------------------------------------------------------------------------- #
# import_side_effect_smoke
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ImportSideEffectSmokeInput:
    modules: list[str] = field(default_factory=list)
    forbidden_side_effects: list[str] = field(default_factory=list)
    #: Extra paths prepended to ``sys.path`` inside the probe subprocess (lets a
    #: test/fixture module be importable without polluting the real environment).
    extra_sys_path: tuple[str, ...] = ()
    #: Modules with KNOWN, catalogued import-time side-effect debt. Their detected
    #: side effects are recorded as ``baseline`` (owner + promotion_criteria) — a
    #: NEW side effect outside this list always blocks; nothing passes silently.
    known_side_effect_debt: tuple[str, ...] = ()


class ImportSideEffectSmokeGate:
    """Imports each module in a fresh subprocess and flags RUNTIME side effects.

    The signal of a concrete-runtime side effect is a NEW thread started by the
    import (scheduler/worker/MCP server) — not a mere library import, which is the
    import_boundary gate's concern. Callers may additionally pass
    ``forbidden_side_effects`` module markers to assert specific concretes were not
    initialised. A pure module starts no thread and trips nothing.
    """

    gate_id = "import_side_effect_smoke"

    def run(self, gate_input: ImportSideEffectSmokeInput) -> GateReport:
        markers = list(gate_input.forbidden_side_effects)
        per_module: dict[str, list[str]] = {}
        import_errors: dict[str, str] = {}
        for module in gate_input.modules:
            detected, error = self._probe(module, markers, gate_input.extra_sys_path)
            if error:
                import_errors[module] = error
            if detected:
                per_module[module] = detected
        known = set(gate_input.known_side_effect_debt)
        blocking_modules = {m: d for m, d in per_module.items() if m not in known}
        baseline_modules = {m: d for m, d in per_module.items() if m in known}
        evidence = {
            "imported_modules": list(gate_input.modules),
            "forbidden_side_effects": markers,
            "side_effects_detected": {k: per_module[k] for k in sorted(per_module)},
            "baseline_side_effect_debt": {k: baseline_modules[k] for k in sorted(baseline_modules)},
            "import_errors": import_errors,
        }
        if blocking_modules:
            return GateReport(
                gate_id=self.gate_id,
                subject="core import side effects",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "import_side_effect"},
                observed_value=sorted(blocking_modules),
                expected_value=[],
                remediation_hint=(
                    "Importing a pure module must not initialise DB/KG/scheduler/MCP; "
                    "defer concrete construction to the composition root."
                ),
            )
        if baseline_modules:
            return GateReport(
                gate_id=self.gate_id,
                subject="core import side effects",
                status="baseline",
                severity="medium",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "import_side_effect"},
                observed_value=sorted(baseline_modules),
                expected_value=[],
                promotion_criteria=(
                    "Known import-time side-effect debt: move the concrete init into the "
                    "composition root before promotion; tracked as baseline until then."
                ),
                remediation_hint=(
                    "Importing this module initialises runtime; defer the concrete "
                    "construction to the composition root."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="core import side effects",
            status="passed",
            severity="low",
            evidence=evidence,
        )

    @staticmethod
    def _probe(
        module: str, markers: list[str], extra_sys_path: tuple[str, ...] = ()
    ) -> tuple[list[str], str | None]:
        script = (
            "import sys, json, threading\n"
            f"sys.path[:0] = {list(extra_sys_path)!r}\n"
            "base = {t.ident for t in threading.enumerate()}\n"
            "before = set(sys.modules)\n"
            "err = None\n"
            "try:\n"
            f"    __import__({module!r})\n"
            "except Exception as exc:\n"
            "    err = type(exc).__name__ + ': ' + str(exc)\n"
            "after = set(sys.modules)\n"
            "threads = sorted(t.name for t in threading.enumerate() if t.ident not in base)\n"
            f"markers = {markers!r}\n"
            "mods = sorted({mk for m in (after - before) for mk in markers "
            "if m == mk or m.startswith(mk + '.')})\n"
            "print(json.dumps({'threads': threads, 'modules': mods, 'err': err}))\n"
        )
        try:
            proc = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                text=True,
                timeout=60,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            return [], f"probe_failed: {exc}"
        out = proc.stdout.strip().splitlines()
        if not out:
            return [], f"probe_unparseable: rc={proc.returncode} stderr={proc.stderr[:200]}"
        try:
            payload = json.loads(out[-1])
        except ValueError:
            return [], f"probe_unparseable: rc={proc.returncode} stderr={proc.stderr[:200]}"
        detected = [f"thread:{t}" for t in payload.get("threads") or []]
        detected += [f"module:{m}" for m in payload.get("modules") or []]
        return detected, payload.get("err")
