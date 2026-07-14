"""Fail-closed boundary for graph backend details in production Core code."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
import re

from .report import GateReport


@dataclass(frozen=True)
class GraphRuntimeSurfaceGateInput:
    source_root: Path | None = None
    mode: str = "blocking"


@dataclass(frozen=True)
class GraphRuntimeCompatibilityEntry:
    """Deprecated ledger row retained only for import compatibility."""

    token: str
    legacy_surface: str
    neutral_surface: str
    files: tuple[str, ...]
    owner: str
    reason: str
    removal_criterion: str
    validation_oracle: str

    def as_dict(self) -> dict[str, object]:
        return {
            "token": self.token,
            "legacy_surface": self.legacy_surface,
            "neutral_surface": self.neutral_surface,
            "files": list(self.files),
            "owner": self.owner,
            "reason": self.reason,
            "removal_criterion": self.removal_criterion,
            "validation_oracle": self.validation_oracle,
        }


REQUIRED_COMPATIBILITY_FIELDS: tuple[str, ...] = (
    "token",
    "legacy_surface",
    "neutral_surface",
    "files",
    "owner",
    "reason",
    "removal_criterion",
    "validation_oracle",
)

# Terminal state: a compatibility ledger no longer authorizes backend details.
LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER: tuple[
    GraphRuntimeCompatibilityEntry, ...
] = ()


class GraphRuntimeSurfaceGate:
    """Reject graph-driver imports, dialect, storage markers and helper leaks."""

    gate_id = "graph_runtime_surface"

    _FORBIDDEN_IMPORT_ROOTS: frozenset[str] = frozenset({
        "kuzu",
        "ladybug",
    })
    _FORBIDDEN_SYMBOLS: frozenset[str] = frozenset({
        "_is_ladybug_corruption_error",
        "_open_kuzu_db",
        "apply_ladybug_lifecycle_step",
        "board_kuzu_path",
        "load_vector_extension",
        "open_kuzu_db",
        "get_column_names",
        "get_next",
        "has_next",
    })
    _FORBIDDEN_IDENTIFIER_FRAGMENTS: tuple[str, ...] = ("kuzu", "ladybug")
    _FORBIDDEN_LITERAL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
        (
            "CALL QUERY_VECTOR_INDEX",
            re.compile(r"\bCALL\s+QUERY_VECTOR_INDEX\b", re.IGNORECASE),
        ),
        (
            "CALL SHOW_TABLES",
            re.compile(r"\bCALL\s+SHOW_TABLES\b", re.IGNORECASE),
        ),
        (
            "CALL TABLE_INFO",
            re.compile(r"\bCALL\s+TABLE_INFO\b", re.IGNORECASE),
        ),
        (
            "Could not set lock on file",
            re.compile(r"could\s+not\s+set\s+lock\s+on\s+file", re.IGNORECASE),
        ),
        ("wal_record.cpp", re.compile(r"wal_record\.cpp", re.IGNORECASE)),
        ("graph.lbug", re.compile(r"\.lbug\b", re.IGNORECASE)),
        (".kuzu", re.compile(r"\.kuzu\b", re.IGNORECASE)),
        ("kuzu", re.compile(r"kuzu", re.IGNORECASE)),
        ("ladybug", re.compile(r"ladybug", re.IGNORECASE)),
    )

    # Governance modules describe forbidden dependencies and therefore must be
    # auditable without being mistaken for application/runtime behavior.
    _EXCLUDED_PREFIXES: tuple[str, ...] = (
        "okto_pulse/core/application/boundary/",
        "okto_pulse/core/testing/",
        "okto_pulse/core/kg/providers/testing/",
    )
    _EXCLUDED_FILES: frozenset[str] = frozenset({
        "okto_pulse/core/infra/relational_lifecycle_decomposition.py",
        "okto_pulse/core/kg/data_provider_ownership_gate.py",
        "okto_pulse/core/kg/schema_import_classification_gate.py",
        "okto_pulse/core/ports/mcp_resources.py",
        "okto_pulse/core/repositories/orm_consumer_split_inventory.py",
        "okto_pulse/core/repositories/relational_boundary_gate.py",
        "okto_pulse/core/repositories/relational_consumer_inventory.py",
    })

    def run(self, data: GraphRuntimeSurfaceGateInput | None = None) -> GateReport:
        data = data or GraphRuntimeSurfaceGateInput()
        root = self._source_root(data.source_root)
        files, excluded = self._scan_targets(root)
        violations: list[dict[str, object]] = []
        for file_path in files:
            violations.extend(self._scan_file(file_path))

        evidence = {
            "forbidden_import_roots": sorted(self._FORBIDDEN_IMPORT_ROOTS),
            "forbidden_symbols": sorted(self._FORBIDDEN_SYMBOLS),
            "forbidden_identifier_fragments": list(
                self._FORBIDDEN_IDENTIFIER_FRAGMENTS
            ),
            "forbidden_terms": [
                label for label, _pattern in self._FORBIDDEN_LITERAL_PATTERNS
            ],
            "scanned_files": [self._rel(path) for path in files],
            "excluded_governance_files": excluded,
            "compatibility_allowlist": [],
            "compatibility_ledger": [],
            "compatibility_ledger_findings": [],
            "violations": violations,
        }
        if violations:
            return GateReport(
                gate_id=self.gate_id,
                subject="graph backend details in production Core",
                status="blocking" if data.mode == "blocking" else "xfail_advisory",
                severity="high",
                owner="okto-pulse-core/kg",
                evidence=evidence,
                observed_value=violations,
                expected_value=[],
                remediation_hint=(
                    "Move driver imports, dialect, physical storage markers and "
                    "backend error mapping to an edition adapter."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="graph backend details in production Core",
            status="passed",
            severity="low",
            owner="okto-pulse-core/kg",
            evidence=evidence,
        )

    def _scan_file(self, file_path: Path) -> list[dict[str, object]]:
        rel = self._rel(file_path)
        try:
            source = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            return [{
                "file": rel,
                "line": 0,
                "term": "<read_error>",
                "detail": str(exc),
            }]
        try:
            tree = ast.parse(source, filename=str(file_path))
        except SyntaxError as exc:
            return [{
                "file": rel,
                "line": int(exc.lineno or 0),
                "term": "<syntax_error>",
                "detail": str(exc),
            }]

        violations: list[dict[str, object]] = []
        seen: set[tuple[int, str, str]] = set()

        def record(node: ast.AST, term: str, category: str) -> None:
            line = int(getattr(node, "lineno", 0) or 0)
            key = (line, term, category)
            if key in seen:
                return
            seen.add(key)
            violations.append({
                "file": rel,
                "line": line,
                "term": term,
                "category": category,
            })

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = alias.name.split(".", 1)[0].lower()
                    if root in self._FORBIDDEN_IMPORT_ROOTS:
                        record(node, root, "driver_import")
                    self._record_identifier_fragment(node, alias.name, record)
                    if alias.asname:
                        self._record_identifier_fragment(node, alias.asname, record)
            elif isinstance(node, ast.ImportFrom):
                root = (node.module or "").split(".", 1)[0].lower()
                if root in self._FORBIDDEN_IMPORT_ROOTS:
                    record(node, root, "driver_import")
                self._record_identifier_fragment(node, node.module or "", record)
                for alias in node.names:
                    self._record_identifier_fragment(node, alias.name, record)
                    if alias.asname:
                        self._record_identifier_fragment(node, alias.asname, record)
            elif isinstance(node, (ast.Name, ast.Attribute)):
                symbol = node.id if isinstance(node, ast.Name) else node.attr
                if symbol in self._FORBIDDEN_SYMBOLS:
                    record(node, symbol, "backend_symbol")
                self._record_identifier_fragment(node, symbol, record)
            elif (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            ):
                if node.name in self._FORBIDDEN_SYMBOLS:
                    record(node, node.name, "backend_symbol")
                self._record_identifier_fragment(node, node.name, record)
            elif isinstance(node, ast.arg):
                self._record_identifier_fragment(node, node.arg, record)
            elif isinstance(node, ast.keyword) and node.arg:
                self._record_identifier_fragment(node, node.arg, record)
            elif (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
            ):
                for label, pattern in self._FORBIDDEN_LITERAL_PATTERNS:
                    if pattern.search(node.value):
                        record(node, label, "backend_literal")

        return violations

    def _record_identifier_fragment(self, node, identifier, record) -> None:
        normalized = identifier.lower()
        for fragment in self._FORBIDDEN_IDENTIFIER_FRAGMENTS:
            if fragment in normalized:
                record(node, fragment, "backend_identifier")

    def _source_root(self, source_root: Path | None) -> Path:
        root = Path(source_root or Path(__file__).resolve().parents[4])
        if (root / "okto_pulse" / "core").exists():
            return root
        if (root / "src" / "okto_pulse" / "core").exists():
            return root / "src"
        return root

    def _scan_targets(self, source_root: Path) -> tuple[list[Path], list[str]]:
        core_root = source_root / "okto_pulse" / "core"
        targets: list[Path] = []
        excluded: list[str] = []
        for path in sorted(core_root.rglob("*.py")) if core_root.exists() else []:
            rel = self._rel(path)
            if rel in self._EXCLUDED_FILES or any(
                rel.startswith(prefix) for prefix in self._EXCLUDED_PREFIXES
            ):
                excluded.append(rel)
                continue
            targets.append(path)
        return targets, excluded

    @staticmethod
    def _rel(path: Path) -> str:
        parts = path.parts
        if "okto_pulse" in parts:
            return Path(*parts[parts.index("okto_pulse"):]).as_posix()
        return path.as_posix()


__all__ = [
    "GraphRuntimeCompatibilityEntry",
    "GraphRuntimeSurfaceGate",
    "GraphRuntimeSurfaceGateInput",
    "LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER",
    "REQUIRED_COMPATIBILITY_FIELDS",
]
