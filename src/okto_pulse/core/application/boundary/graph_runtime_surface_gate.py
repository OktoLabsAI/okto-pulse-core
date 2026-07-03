"""AF17 graph runtime surface gate.

Blocks physical storage details from new core graph runtime contracts while
keeping the reviewed legacy compatibility shims explicit.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from .report import GateReport


@dataclass(frozen=True)
class GraphRuntimeSurfaceGateInput:
    source_root: Path | None = None
    mode: str = "blocking"


class GraphRuntimeSurfaceGate:
    gate_id = "graph_runtime_surface"

    _FORBIDDEN_TERMS: tuple[str, ...] = (
        "Path",
        "graph.lbug",
        "board_kuzu_path",
        "ladybug",
        "neptune",
    )
    _COMPATIBILITY_ALLOWLIST: tuple[str, ...] = (
        "okto_pulse/core/kg/interfaces/board_graph_runtime.py",
        "okto_pulse/core/kg/schema.py",
    )
    _SKIPPED_LEGACY_CONTRACTS: frozenset[str] = frozenset({
        "okto_pulse/core/kg/interfaces/board_graph_runtime.py",
        "okto_pulse/core/kg/schema.py",
    })
    _PATH_TOKEN = re.compile(r"\bPath\b")

    def run(self, data: GraphRuntimeSurfaceGateInput | None = None) -> GateReport:
        data = data or GraphRuntimeSurfaceGateInput()
        root = self._source_root(data.source_root)
        files = self._scan_targets(root)
        violations = []
        for file_path in files:
            rel = self._rel(file_path)
            if rel in self._SKIPPED_LEGACY_CONTRACTS:
                continue
            try:
                lines = file_path.read_text(encoding="utf-8").splitlines()
            except OSError as exc:
                violations.append({
                    "file": rel,
                    "line": 0,
                    "term": "<read_error>",
                    "detail": str(exc),
                })
                continue
            for line_no, line in enumerate(lines, start=1):
                lower = line.lower()
                for term in self._FORBIDDEN_TERMS:
                    matched = bool(self._PATH_TOKEN.search(line)) if term == "Path" else term in lower
                    if matched:
                        violations.append({
                            "file": rel,
                            "line": line_no,
                            "term": term,
                        })

        evidence = {
            "forbidden_terms": list(self._FORBIDDEN_TERMS),
            "scanned_files": [self._rel(p) for p in files],
            "compatibility_allowlist": list(self._COMPATIBILITY_ALLOWLIST),
            "violations": violations,
        }
        if violations:
            return GateReport(
                gate_id=self.gate_id,
                subject="graph runtime core contract surface",
                status="blocking" if data.mode == "blocking" else "xfail_advisory",
                severity="high",
                owner="okto-pulse-core/kg",
                evidence=evidence,
                observed_value=violations,
                expected_value=[],
                remediation_hint=(
                    "Move physical storage and backend-specific symbols to an "
                    "edition adapter or a reviewed compatibility shim."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="graph runtime core contract surface",
            status="passed",
            severity="low",
            owner="okto-pulse-core/kg",
            evidence=evidence,
        )

    def _source_root(self, source_root: Path | None) -> Path:
        root = source_root or Path(__file__).resolve().parents[4]
        root = Path(root)
        if (root / "okto_pulse" / "core").exists():
            return root
        if (root / "src" / "okto_pulse" / "core").exists():
            return root / "src"
        return root

    def _scan_targets(self, source_root: Path) -> list[Path]:
        core_root = source_root / "okto_pulse" / "core"
        targets: list[Path] = []
        interfaces = core_root / "kg" / "interfaces"
        if interfaces.exists():
            targets.extend(sorted(interfaces.glob("*.py")))
        schema_contract = core_root / "kg" / "schema_contract.py"
        if schema_contract.exists():
            targets.append(schema_contract)
        return targets

    def _rel(self, path: Path) -> str:
        parts = path.parts
        if "okto_pulse" in parts:
            idx = parts.index("okto_pulse")
            return Path(*parts[idx:]).as_posix()
        return path.as_posix()


__all__ = ["GraphRuntimeSurfaceGate", "GraphRuntimeSurfaceGateInput"]
