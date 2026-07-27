"""Gate that keeps the concrete SchedulerControl adapter out of Core runtime."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

FORBIDDEN_SCHEDULER_CONCRETE = "Singleton" "SchedulerControl"


@dataclass(frozen=True)
class SchedulerControlSymbolFinding:
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


def _core_root(source_root: Path | None) -> Path:
    root = source_root or _default_source_root()
    direct = root / "okto_pulse" / "core"
    if direct.exists():
        return direct
    return root / "src" / "okto_pulse" / "core"


def _is_runtime_file(path: Path) -> bool:
    parts = set(path.parts)
    return not ({"tests", "test", "fakes", "fixtures"} & parts)


class SchedulerControlSymbolGate:
    """Blocks the concrete scheduler-control class or imports in Core runtime."""

    gate_id = "scheduler_control_symbol"

    def run(self, *, source_root: Path | None = None) -> GateReport:
        root = _core_root(source_root)
        findings: list[SchedulerControlSymbolFinding] = []
        scanned_files: list[str] = []

        if not root.exists():
            return GateReport(
                gate_id=self.gate_id,
                subject="core scheduler control concrete symbol",
                status="xfail_advisory",
                severity="low",
                owner="okto-pulse-core/runtime",
                evidence={"error": "core_source_not_found", "source_root": str(root)},
                promotion_criteria="Run against a source root containing okto_pulse/core.",
            )

        for path in sorted(root.rglob("*.py")):
            if not _is_runtime_file(path):
                continue
            rel = path.relative_to(root.parent.parent).as_posix()
            scanned_files.append(rel)
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except SyntaxError as exc:
                findings.append(
                    SchedulerControlSymbolFinding(
                        file=rel,
                        line=exc.lineno or 0,
                        kind="parse_error",
                        symbol=FORBIDDEN_SCHEDULER_CONCRETE,
                    )
                )
                continue

            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef) and node.name == FORBIDDEN_SCHEDULER_CONCRETE:
                    findings.append(
                        SchedulerControlSymbolFinding(
                            file=rel,
                            line=node.lineno,
                            kind="class_def",
                            symbol=node.name,
                        )
                    )
                elif isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        if (
                            alias.name == FORBIDDEN_SCHEDULER_CONCRETE
                            or alias.asname == FORBIDDEN_SCHEDULER_CONCRETE
                        ):
                            findings.append(
                                SchedulerControlSymbolFinding(
                                    file=rel,
                                    line=node.lineno,
                                    kind="from_import",
                                    symbol=alias.asname or alias.name,
                                )
                            )
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.asname == FORBIDDEN_SCHEDULER_CONCRETE:
                            findings.append(
                                SchedulerControlSymbolFinding(
                                    file=rel,
                                    line=node.lineno,
                                    kind="import_alias",
                                    symbol=alias.asname,
                                )
                            )
                elif isinstance(node, ast.Name) and node.id == FORBIDDEN_SCHEDULER_CONCRETE:
                    findings.append(
                        SchedulerControlSymbolFinding(
                            file=rel,
                            line=node.lineno,
                            kind="name_reference",
                            symbol=node.id,
                        )
                    )
                elif (
                    isinstance(node, ast.Attribute)
                    and node.attr == FORBIDDEN_SCHEDULER_CONCRETE
                ):
                    findings.append(
                        SchedulerControlSymbolFinding(
                            file=rel,
                            line=node.lineno,
                            kind="attribute_reference",
                            symbol=node.attr,
                        )
                    )

        evidence = {
            "forbidden_symbol": FORBIDDEN_SCHEDULER_CONCRETE,
            "source_root": str(root),
            "scanned_files": scanned_files,
            "offenders": [finding.as_dict() for finding in findings],
        }
        if findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="core scheduler control concrete symbol",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/runtime",
                evidence={**evidence, "error": "core_scheduler_concrete_symbol"},
                observed_value=[finding.as_dict() for finding in findings],
                expected_value=[],
                remediation_hint=(
                    "Keep the concrete scheduler-control adapter in the edition "
                    "adapter package; Core may expose only the SchedulerControl port."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="core scheduler control concrete symbol",
            status="passed",
            severity="low",
            owner="okto-pulse-core/runtime",
            evidence=evidence,
        )


__all__ = [
    "FORBIDDEN_SCHEDULER_CONCRETE",
    "SchedulerControlSymbolFinding",
    "SchedulerControlSymbolGate",
]
