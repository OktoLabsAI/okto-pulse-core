"""R08B lifecycle fallback gate.

Blocks concrete Community/runtime fallback wiring from being introduced in
``core.app`` or ``core.composition``. Existing forward-only debt in the default
lifespan is handled by its own reviewed specs; this gate prevents the R08B
strict shell from gaining new concrete provider shortcuts.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

FORBIDDEN_LIFECYCLE_FALLBACK_SYMBOLS: tuple[str, ...] = (
    "CommunityOutboxEventBus",
    "CommunitySettings",
    "CommunityStubEmbeddingProvider",
    "LocalAuthProvider",
    "SingletonSchedulerControl",
    "community_storage_provider",
    "configure_community_kg_registry",
    "install_community_sqlite_pragmas",
    "register_community_relational_schema_lifecycle",
)

_TARGET_FILES: tuple[str, ...] = ("app.py", "composition.py")


@dataclass(frozen=True)
class LifecycleFallbackFinding:
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


class LifecycleFallbackGate:
    """Blocks concrete lifecycle fallbacks in core app/composition files."""

    gate_id = "lifecycle_fallback"

    def run(self, *, source_root: Path | None = None) -> GateReport:
        root = _core_root(source_root)
        if not root.exists():
            return GateReport(
                gate_id=self.gate_id,
                subject="core lifecycle fallback wiring",
                status="xfail_advisory",
                severity="low",
                owner="okto-pulse-core/runtime",
                evidence={"error": "core_source_not_found", "source_root": str(root)},
                promotion_criteria="Run against a source root containing okto_pulse/core.",
            )

        findings: list[LifecycleFallbackFinding] = []
        scanned_files: list[str] = []
        for name in _TARGET_FILES:
            path = root / name
            if not path.exists():
                continue
            rel = path.relative_to(root.parent.parent).as_posix()
            scanned_files.append(rel)
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except SyntaxError as exc:
                findings.append(
                    LifecycleFallbackFinding(
                        file=rel,
                        line=exc.lineno or 0,
                        kind="parse_error",
                        symbol="<syntax_error>",
                    )
                )
                continue
            findings.extend(_scan_tree(rel, tree))

        evidence = {
            "forbidden_symbols": list(FORBIDDEN_LIFECYCLE_FALLBACK_SYMBOLS),
            "scanned_files": scanned_files,
            "offenders": [finding.as_dict() for finding in findings],
        }
        if findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="core lifecycle fallback wiring",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/runtime",
                evidence={**evidence, "error": "lifecycle_fallback_concrete_wiring"},
                observed_value=[finding.as_dict() for finding in findings],
                expected_value=[],
                remediation_hint=(
                    "Keep Community/runtime concrete lifecycle providers in the "
                    "edition composition root; core.app/core.composition may expose "
                    "only the RuntimeComposition contract and explicit shell guard."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="core lifecycle fallback wiring",
            status="passed",
            severity="low",
            owner="okto-pulse-core/runtime",
            evidence=evidence,
        )


def _scan_tree(rel: str, tree: ast.AST) -> list[LifecycleFallbackFinding]:
    forbidden = set(FORBIDDEN_LIFECYCLE_FALLBACK_SYMBOLS)
    findings: list[LifecycleFallbackFinding] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                _append_if_forbidden(
                    findings,
                    rel,
                    node.lineno,
                    "from_import",
                    alias.name,
                    forbidden,
                )
                if alias.asname:
                    _append_if_forbidden(
                        findings,
                        rel,
                        node.lineno,
                        "from_import_alias",
                        alias.asname,
                        forbidden,
                    )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    _append_if_forbidden(
                        findings,
                        rel,
                        node.lineno,
                        "import_alias",
                        alias.asname,
                        forbidden,
                    )
        elif isinstance(node, ast.Name):
            _append_if_forbidden(
                findings, rel, node.lineno, "name_reference", node.id, forbidden
            )
        elif isinstance(node, ast.Attribute):
            _append_if_forbidden(
                findings,
                rel,
                node.lineno,
                "attribute_reference",
                node.attr,
                forbidden,
            )
    return findings


def _append_if_forbidden(
    findings: list[LifecycleFallbackFinding],
    rel: str,
    line: int,
    kind: str,
    symbol: str,
    forbidden: set[str],
) -> None:
    if symbol in forbidden:
        findings.append(
            LifecycleFallbackFinding(file=rel, line=line, kind=kind, symbol=symbol)
        )


__all__ = [
    "FORBIDDEN_LIFECYCLE_FALLBACK_SYMBOLS",
    "LifecycleFallbackFinding",
    "LifecycleFallbackGate",
]
