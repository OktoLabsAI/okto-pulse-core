"""R08C runtime worker boundary gate.

Blocks direct worker startup wiring in app entry points. Concrete worker
factories belong in edition adapters/registries; app modules may call only the
registry or public runtime contracts.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

FORBIDDEN_DIRECT_WORKER_SYMBOLS: tuple[str, ...] = (
    "EventDispatcher",
    "ConsolidationWorker",
    "OutboxWorker",
    "build_default_runtime_worker_registry",
    "get_cleanup_worker",
    "get_cognitive_closeout_worker",
    "get_consolidation_worker",
    "get_outbox_worker",
    "sweep_board_schemas",
)

FORBIDDEN_CORE_APP_PRIVATE_TICK_HELPERS: tuple[str, ...] = (
    "_compute_tick_catch_up_next_run",
    "_emit_daily_tick",
)

FORBIDDEN_WORKER_MODULE_PREFIXES: tuple[str, ...] = (
    "okto_pulse.core.events.dispatcher",
    "okto_pulse.core.infra.default_runtime_workers",
    "okto_pulse.core.kg.global_discovery.outbox_worker",
    "okto_pulse.core.kg.workers",
)


@dataclass(frozen=True)
class RuntimeWorkerBoundaryFinding:
    file: str
    line: int
    kind: str
    symbol: str

    def as_dict(self) -> dict[str, object]:
        return {
            "file": self.file,
            "line": self.line,
            "kind": self.kind,
            "symbol": self.symbol,
        }


def _default_source_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _package_root(source_root: Path | None, package: str) -> Path | None:
    root = source_root or _default_source_root()
    direct = root / "okto_pulse" / package
    if direct.exists():
        return direct
    nested = root / "src" / "okto_pulse" / package
    if nested.exists():
        return nested
    return None


class RuntimeWorkerBoundaryGate:
    """Blocks direct worker starts in app entry points."""

    gate_id = "runtime_worker_boundary"

    def run(
        self,
        *,
        source_root: Path | None = None,
        community_source_root: Path | None = None,
    ) -> GateReport:
        scan_targets: list[Path] = []
        core_root = _package_root(source_root, "core")
        if core_root is not None:
            scan_targets.append(core_root / "app.py")
            processor_root = core_root / "application" / "processors"
            if processor_root.exists():
                scan_targets.extend(sorted(processor_root.glob("*.py")))
        community_root = _package_root(
            community_source_root if community_source_root is not None else source_root,
            "community",
        )
        if community_root is not None:
            scan_targets.append(community_root / "main.py")

        findings: list[RuntimeWorkerBoundaryFinding] = []
        scanned_files: list[str] = []
        for path in scan_targets:
            if not path.exists():
                continue
            rel = _rel(path)
            scanned_files.append(rel)
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except SyntaxError as exc:
                findings.append(
                    RuntimeWorkerBoundaryFinding(
                        file=rel,
                        line=exc.lineno or 0,
                        kind="parse_error",
                        symbol="<syntax_error>",
                    )
                )
                continue
            findings.extend(_scan_tree(rel, tree))
            if "/application/processors/" in f"/{rel}":
                findings.extend(_scan_processor_runtime(rel, tree))

        evidence = {
            "forbidden_worker_symbols": list(FORBIDDEN_DIRECT_WORKER_SYMBOLS),
            "forbidden_core_app_private_tick_helpers": list(
                FORBIDDEN_CORE_APP_PRIVATE_TICK_HELPERS
            ),
            "forbidden_worker_module_prefixes": list(
                FORBIDDEN_WORKER_MODULE_PREFIXES
            ),
            "scanned_files": scanned_files,
            "offenders": [finding.as_dict() for finding in findings],
        }
        if findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="runtime worker app-entry boundary",
                status="blocking",
                severity="high",
                owner="okto-pulse-community/runtime",
                evidence={**evidence, "error": "direct_worker_start_boundary"},
                observed_value=[finding.as_dict() for finding in findings],
                expected_value=[],
                remediation_hint=(
                    "Move concrete worker start/stop wiring into an edition "
                    "RuntimeWorkerRegistry adapter. Core app entry points may "
                    "receive an explicitly injected registry and call public "
                    "daily_tick use cases only."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="runtime worker app-entry boundary",
            status="passed",
            severity="low",
            owner="okto-pulse-community/runtime",
            evidence=evidence,
        )


def _scan_tree(rel: str, tree: ast.AST) -> list[RuntimeWorkerBoundaryFinding]:
    findings: list[RuntimeWorkerBoundaryFinding] = []
    forbidden_workers = set(FORBIDDEN_DIRECT_WORKER_SYMBOLS)
    forbidden_private_tick = set(FORBIDDEN_CORE_APP_PRIVATE_TICK_HELPERS)
    core_app_aliases: set[str] = set()
    worker_module_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                symbol = alias.asname or alias.name.split(".", maxsplit=1)[0]
                if alias.name == "okto_pulse.community.app":
                    core_app_aliases.add(symbol)
                    findings.append(
                        RuntimeWorkerBoundaryFinding(
                            file=rel,
                            line=node.lineno,
                            kind="private_tick_module_import",
                            symbol=alias.name,
                        )
                    )
                elif _is_forbidden_worker_module(alias.name):
                    worker_module_aliases.add(symbol)
                    findings.append(
                        RuntimeWorkerBoundaryFinding(
                            file=rel,
                            line=node.lineno,
                            kind="direct_worker_module_import",
                            symbol=alias.name,
                        )
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                symbol = alias.name
                if module == "okto_pulse.community.app" and symbol in forbidden_private_tick:
                    findings.append(
                        RuntimeWorkerBoundaryFinding(
                            file=rel,
                            line=node.lineno,
                            kind="private_tick_import",
                            symbol=symbol,
                        )
                    )
                if symbol in forbidden_workers or _is_forbidden_worker_module(module):
                    findings.append(
                        RuntimeWorkerBoundaryFinding(
                            file=rel,
                            line=node.lineno,
                            kind="direct_worker_import",
                            symbol=symbol,
                        )
                    )
        elif isinstance(node, ast.Name) and node.id in forbidden_workers:
            findings.append(
                RuntimeWorkerBoundaryFinding(
                    file=rel,
                    line=node.lineno,
                    kind="direct_worker_reference",
                    symbol=node.id,
                )
            )
        elif isinstance(node, ast.Attribute):
            _scan_attribute(
                rel,
                node,
                forbidden_workers,
                forbidden_private_tick,
                core_app_aliases,
                worker_module_aliases,
                findings,
            )
        elif isinstance(node, ast.Call):
            _scan_call(rel, node, forbidden_workers, findings)
    return findings


def _scan_processor_runtime(
    rel: str,
    tree: ast.AST,
) -> list[RuntimeWorkerBoundaryFinding]:
    findings: list[RuntimeWorkerBoundaryFinding] = []
    forbidden_classes = {"EventDispatcher", "ConsolidationWorker", "OutboxWorker"}
    forbidden_factories = {
        "get_cleanup_worker",
        "get_consolidation_worker",
        "get_outbox_worker",
        "reset_cleanup_worker_for_tests",
        "reset_consolidation_worker_for_tests",
        "reset_outbox_worker_for_tests",
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name in forbidden_classes:
            findings.append(
                RuntimeWorkerBoundaryFinding(
                    file=rel,
                    line=node.lineno,
                    kind="core_runtime_worker_class",
                    symbol=node.name,
                )
            )
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in forbidden_factories:
                findings.append(
                    RuntimeWorkerBoundaryFinding(
                        file=rel,
                        line=node.lineno,
                        kind="core_runtime_worker_factory",
                        symbol=node.name,
                    )
                )
        elif isinstance(node, ast.Call):
            dotted = _dotted_name(node.func)
            if dotted.endswith(".create_task") or dotted.endswith(".ensure_future"):
                findings.append(
                    RuntimeWorkerBoundaryFinding(
                        file=rel,
                        line=node.lineno,
                        kind="core_processor_task_creation",
                        symbol=dotted,
                    )
                )
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(target, ast.Name) and target.id == "_singleton" for target in targets):
                findings.append(
                    RuntimeWorkerBoundaryFinding(
                        file=rel,
                        line=node.lineno,
                        kind="core_processor_singleton",
                        symbol="_singleton",
                    )
                )
    return findings


def _scan_attribute(
    rel: str,
    node: ast.Attribute,
    forbidden_workers: set[str],
    forbidden_private_tick: set[str],
    core_app_aliases: set[str],
    worker_module_aliases: set[str],
    findings: list[RuntimeWorkerBoundaryFinding],
) -> None:
    owner = _root_name(node.value)
    dotted = _dotted_name(node)
    if node.attr in forbidden_private_tick and (
        owner in core_app_aliases or dotted.startswith("okto_pulse.community.app.")
    ):
        findings.append(
            RuntimeWorkerBoundaryFinding(
                file=rel,
                line=node.lineno,
                kind="private_tick_attribute_reference",
                symbol=node.attr,
            )
        )
    if node.attr in forbidden_workers and (
        owner in worker_module_aliases or _contains_forbidden_worker_symbol(dotted)
    ):
        findings.append(
            RuntimeWorkerBoundaryFinding(
                file=rel,
                line=node.lineno,
                kind="direct_worker_attribute_reference",
                symbol=node.attr,
            )
        )


def _scan_call(
    rel: str,
    node: ast.Call,
    forbidden_workers: set[str],
    findings: list[RuntimeWorkerBoundaryFinding],
) -> None:
    func = node.func
    if isinstance(func, ast.Name) and func.id in forbidden_workers:
        findings.append(
            RuntimeWorkerBoundaryFinding(
                file=rel,
                line=node.lineno,
                kind="direct_worker_call",
                symbol=func.id,
            )
        )
    if isinstance(func, ast.Attribute) and func.attr == "create_task":
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and child.id in forbidden_workers:
                findings.append(
                    RuntimeWorkerBoundaryFinding(
                        file=rel,
                        line=node.lineno,
                        kind="direct_worker_create_task",
                        symbol=child.id,
                    )
                )


def _is_forbidden_worker_module(module: str) -> bool:
    return any(
        module == prefix or module.startswith(f"{prefix}.")
        for prefix in FORBIDDEN_WORKER_MODULE_PREFIXES
    )


def _contains_forbidden_worker_symbol(dotted: str) -> bool:
    parts = dotted.split(".")
    return any(part in FORBIDDEN_DIRECT_WORKER_SYMBOLS for part in parts)


def _root_name(node: ast.AST) -> str | None:
    while isinstance(node, ast.Attribute):
        node = node.value
    if isinstance(node, ast.Name):
        return node.id
    return None


def _dotted_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _rel(path: Path) -> str:
    parts = path.parts
    if "okto_pulse" in parts:
        idx = parts.index("okto_pulse")
        return Path(*parts[idx:]).as_posix()
    return path.as_posix()


__all__ = [
    "FORBIDDEN_CORE_APP_PRIVATE_TICK_HELPERS",
    "FORBIDDEN_DIRECT_WORKER_SYMBOLS",
    "FORBIDDEN_WORKER_MODULE_PREFIXES",
    "RuntimeWorkerBoundaryFinding",
    "RuntimeWorkerBoundaryGate",
]
