"""TelemetryImportGate (spec R10-A, IMP3) — block NEW direct imports/uses of the
telemetry RUNTIME classes (``TelemetryService`` / ``LocalTelemetryStore``)
outside the allowlisted shims/definitions, so ``core.ports`` is the single
contract entry-point and the runtime can move to Community in R10-B/C/D.

ALIAS-AWARE (the R05-D lesson — mirrors ``data_provider_ownership_gate`` /
``mcp_credential_usage_gate``): the AST resolves import-as + from-import +
qualified attribute + ASSIGNMENT-CHAIN aliases to the canonical symbol BEFORE the
allowlist check, so ``from x import TelemetryService as TS; TS()`` or
``S = TelemetryService; T = S`` cannot bypass the gate.

Fail-closed register-before-remove: the current importers (the telemetry package
itself + the app-layer shims) are allowlisted with a removal criterion; a NEW
importer outside the allowlist BLOCKS. String literals (e.g. the names listed in
other gates / docstrings) are ``ast.Constant`` and are never flagged.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

#: Telemetry runtime classes whose direct import/use is restricted.
#: R10-E Pass 2: LocalTelemetryStore removed from core → no longer in SENSITIVE_SYMBOLS.
#: TelemetryService stays in core (the facade) but call-sites now go via the registry.
SENSITIVE_SYMBOLS: tuple[str, ...] = ("TelemetryService",)
_SENSITIVE = frozenset(SENSITIVE_SYMBOLS)

#: Files/prefixes (norm path under ``okto_pulse/core``) allowed to reference the
#: runtime classes. The telemetry package is the home (definitions + internal wiring).
#: R10-E Pass 2: app.py / api/metrics.py / mcp/server.py now use get_telemetry_port()
#: (registry), not direct TelemetryService(settings) — removed from ALLOWLISTED_FILES.
ALLOWLISTED_PREFIXES: tuple[str, ...] = ("telemetry/",)
ALLOWLISTED_FILES: frozenset[str] = frozenset()

#: Removal criterion: R10-E Pass 2 fulfilled — all direct call-site constructions
#: removed; community/adapters/telemetry_port.py is the only remaining non-telemetry-
#: package file that constructs TelemetryService directly (in community, not core).
REMOVAL_CRITERION = (
    "R10-E Pass 2 FULFILLED: all direct TelemetryService(settings) call-sites in core "
    "rewired to get_telemetry_port(settings) (registry); LocalTelemetryStore removed "
    "from core. Only core.telemetry.* modules may reference TelemetryService directly."
)


@dataclass(frozen=True)
class TelemetryImportFinding:
    file: str
    symbol: str
    allowlisted: bool


@dataclass
class TelemetryImportReport:
    ok: bool
    findings: tuple[TelemetryImportFinding, ...] = ()
    violations: tuple[TelemetryImportFinding, ...] = ()
    allowlist_files: tuple[str, ...] = ()
    allowlist_prefixes: tuple[str, ...] = ()

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "violations": [{"file": v.file, "symbol": v.symbol} for v in self.violations],
            "findings": [
                {"file": f.file, "symbol": f.symbol, "allowlisted": f.allowlisted}
                for f in self.findings
            ],
            "allowlist_files": list(self.allowlist_files),
            "allowlist_prefixes": list(self.allowlist_prefixes),
            "removal_criterion": REMOVAL_CRITERION,
        }


def _default_core_root() -> Path:
    # src/okto_pulse/core/application/boundary/telemetry_import_gate.py -> core/
    return Path(__file__).resolve().parents[2]


def _build_alias_map(tree: ast.AST) -> dict[str, str]:
    """Map every local name referring to a sensitive symbol to its canonical
    name (import renames + assignment chains, R05-D pattern)."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name in _SENSITIVE:
                    aliases[a.asname or a.name] = a.name
        elif isinstance(node, ast.Import):
            for a in node.names:
                last = a.name.rsplit(".", 1)[-1]
                if last in _SENSITIVE:
                    aliases[a.asname or last] = last

    def _resolve(name: str) -> str | None:
        return name if name in _SENSITIVE else aliases.get(name)

    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
                canonical = _resolve(node.value.id)
                if canonical is None:
                    continue
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name) and aliases.get(tgt.id) != canonical:
                        aliases[tgt.id] = canonical
                        changed = True
    return aliases


def _sensitive_symbols_in(tree: ast.AST) -> set[str]:
    alias_map = _build_alias_map(tree)
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            canonical = node.id if node.id in _SENSITIVE else alias_map.get(node.id)
            if canonical is not None:
                found.add(canonical)
        elif isinstance(node, ast.Attribute) and node.attr in _SENSITIVE:
            found.add(node.attr)
    return found


def _is_allowlisted(rel: str) -> bool:
    return rel in ALLOWLISTED_FILES or any(
        rel.startswith(p) for p in ALLOWLISTED_PREFIXES
    )


def run_telemetry_import_gate(core_root: str | Path | None = None) -> TelemetryImportReport:
    """Scan ``core_root`` (the ``okto_pulse/core`` dir) for direct references to
    the telemetry runtime classes; any outside the allowlist is a violation."""
    root = Path(core_root) if core_root is not None else _default_core_root()
    self_file = Path(__file__).resolve()
    findings: list[TelemetryImportFinding] = []
    for py in sorted(root.rglob("*.py")):
        if "__pycache__" in py.parts or py.resolve() == self_file:
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        rel = py.relative_to(root).as_posix()
        for symbol in sorted(_sensitive_symbols_in(tree)):
            findings.append(
                TelemetryImportFinding(
                    file=rel, symbol=symbol, allowlisted=_is_allowlisted(rel)
                )
            )
    violations = tuple(f for f in findings if not f.allowlisted)
    return TelemetryImportReport(
        ok=not violations,
        findings=tuple(findings),
        violations=violations,
        allowlist_files=tuple(sorted(ALLOWLISTED_FILES)),
        allowlist_prefixes=ALLOWLISTED_PREFIXES,
    )


__all__ = [
    "SENSITIVE_SYMBOLS",
    "ALLOWLISTED_FILES",
    "ALLOWLISTED_PREFIXES",
    "REMOVAL_CRITERION",
    "TelemetryImportFinding",
    "TelemetryImportReport",
    "run_telemetry_import_gate",
]
