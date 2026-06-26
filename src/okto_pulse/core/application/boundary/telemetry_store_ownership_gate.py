"""TelemetryStoreOwnershipGate (spec R10-B, IMP4 / TS09 — R10-E Pass 2 FULFILLED).

``LocalTelemetryStore`` has been REMOVED from core (R10-E Pass 2). The gate now
enforces ZERO references to ``LocalTelemetryStore`` anywhere in the core runtime
(the ledger is empty — there are no longer any allowlisted exception sites). Any
NEW reference to the deleted class is a violation.

The Community edition IS the sole authoritative concrete TelemetryEventStore:
``okto_pulse.community.adapters.telemetry_store.CommunityLocalTelemetryStore``.
The core runtime obtains the store through
``event_store_registry.get_telemetry_event_store`` (fail-closed after R10-E Pass 2).

ALIAS-AWARE (R05-D lesson — mirrors ``data_provider_ownership_gate`` /
``telemetry_import_gate``): the AST resolves import-as + from-import + qualified
attribute + ASSIGNMENT-CHAIN aliases to the canonical symbol BEFORE the allowlist
check, so ``from x import LocalTelemetryStore as S; S()`` or ``A = LocalTelemetry
Store; B = A`` cannot bypass the gate. String literals (docstrings, the symbol
names listed in other gates) are ``ast.Constant`` and are never flagged; a bare
``class LocalTelemetryStore`` definition is an ``ast.ClassDef`` (not a reference)
and is likewise not flagged.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

#: The concrete store class whose CORE-runtime instantiation/use is restricted.
SENSITIVE_SYMBOLS: tuple[str, ...] = ("LocalTelemetryStore",)
_SENSITIVE = frozenset(SENSITIVE_SYMBOLS)

#: The NAMED wave that fulfilled the removal criterion (single source of truth).
R10E_REMOVAL_CRITERION = (
    "R10-E Pass 2 FULFILLED: LocalTelemetryStore removed from core "
    "(core.telemetry.store is now a stub-docstring module); the "
    "event_store_registry fallback is removed (fail-closed). "
    "Community owns the concrete TelemetryEventStore. "
    "The gate now enforces ZERO core references (empty ledger)."
)

#: Per-file LEDGER of allowed exception sites — EMPTY after R10-E Pass 2.
#: LocalTelemetryStore is deleted; there are no longer any permitted references.
LEDGERED_STORE_FALLBACK: dict[str, dict] = {}

#: Backwards-friendly view (the set of ledgered file keys).
ALLOWLISTED_FILES: frozenset[str] = frozenset(LEDGERED_STORE_FALLBACK)

#: Single-string projection of the (uniform) removal criterion.
REMOVAL_CRITERION = R10E_REMOVAL_CRITERION


@dataclass(frozen=True)
class StoreOwnershipFinding:
    file: str
    symbol: str
    allowlisted: bool


@dataclass
class StoreOwnershipReport:
    ok: bool
    findings: tuple[StoreOwnershipFinding, ...] = ()
    violations: tuple[StoreOwnershipFinding, ...] = ()
    allowlist_files: tuple[str, ...] = ()

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "violations": [
                {"file": v.file, "symbol": v.symbol} for v in self.violations
            ],
            "findings": [
                {"file": f.file, "symbol": f.symbol, "allowlisted": f.allowlisted}
                for f in self.findings
            ],
            "allowlist_files": list(self.allowlist_files),
            "ledger": LEDGERED_STORE_FALLBACK,
            "removal_criterion": REMOVAL_CRITERION,
        }


def _default_core_root() -> Path:
    # core/application/boundary/telemetry_store_ownership_gate.py -> core/
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


def run_telemetry_store_ownership_gate(
    core_root: str | Path | None = None,
) -> StoreOwnershipReport:
    """Scan ``core_root`` (the ``okto_pulse/core`` dir) for references to the
    concrete ``LocalTelemetryStore``; any outside the allowlist is a violation."""
    root = Path(core_root) if core_root is not None else _default_core_root()
    self_file = Path(__file__).resolve()
    findings: list[StoreOwnershipFinding] = []
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
                StoreOwnershipFinding(
                    file=rel, symbol=symbol, allowlisted=rel in ALLOWLISTED_FILES
                )
            )
    violations = tuple(f for f in findings if not f.allowlisted)
    return StoreOwnershipReport(
        ok=not violations,
        findings=tuple(findings),
        violations=violations,
        allowlist_files=tuple(sorted(ALLOWLISTED_FILES)),
    )


__all__ = [
    "SENSITIVE_SYMBOLS",
    "ALLOWLISTED_FILES",
    "REMOVAL_CRITERION",
    "StoreOwnershipFinding",
    "StoreOwnershipReport",
    "run_telemetry_store_ownership_gate",
]
