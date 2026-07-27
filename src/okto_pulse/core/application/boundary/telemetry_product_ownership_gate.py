"""TelemetryProductOwnershipGate (spec R10-D, IMP1 — R10-E Pass 2 FULFILLED).

``ProductTelemetryAggregator`` has been REMOVED from core (R10-E Pass 2). The gate
now enforces ZERO references to ``ProductTelemetryAggregator`` anywhere in the core
runtime (the ledger is empty — no more allowlisted exception sites). Any NEW
reference to the deleted class is a violation.

The Community edition IS the sole authoritative concrete ProductAggregationPort:
``okto_pulse.community.adapters.product_telemetry.CommunityProductTelemetryAggregator``.
The core runtime obtains the aggregator through
``product_aggregator_registry.get_product_aggregator`` (fail-closed after R10-E Pass 2).

ALIAS-AWARE (R05-D lesson — mirrors ``telemetry_store_ownership_gate`` /
``data_provider_ownership_gate``): the AST resolves import-as + from-import +
qualified attribute + ASSIGNMENT-CHAIN aliases to the canonical symbol BEFORE the
ledger check, so ``from x import ProductTelemetryAggregator as A; A()`` or
``S = ProductTelemetryAggregator; T = S`` cannot bypass the gate. String literals
(docstrings, the symbol names in other gates) are ``ast.Constant`` and are never
flagged; a bare ``class ProductTelemetryAggregator`` definition is an
``ast.ClassDef`` (not a reference) and is likewise not flagged.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

#: The concrete aggregator class whose CORE-runtime instantiation/use is restricted.
SENSITIVE_SYMBOLS: tuple[str, ...] = ("ProductTelemetryAggregator",)
_SENSITIVE = frozenset(SENSITIVE_SYMBOLS)

#: The NAMED wave that fulfilled the removal criterion (single source of truth).
R10E_REMOVAL_CRITERION = (
    "R10-E Pass 2 FULFILLED: ProductTelemetryAggregator removed from core "
    "(core.telemetry.product now re-exports pure constants only); the "
    "product_aggregator_registry fallback is removed (fail-closed). "
    "Community owns the concrete ProductAggregationPort. "
    "The gate now enforces ZERO core references (empty ledger)."
)

#: Per-file LEDGER of allowed exception sites — EMPTY after R10-E Pass 2.
#: ProductTelemetryAggregator is deleted; no permitted references remain.
LEDGERED_PRODUCT_FALLBACK: dict[str, dict] = {}

#: Backwards-friendly view (the set of ledgered file keys).
ALLOWLISTED_FILES: frozenset[str] = frozenset(LEDGERED_PRODUCT_FALLBACK)
REMOVAL_CRITERION = R10E_REMOVAL_CRITERION


@dataclass(frozen=True)
class ProductOwnershipFinding:
    file: str
    symbol: str
    allowlisted: bool


@dataclass
class ProductOwnershipReport:
    ok: bool
    findings: tuple[ProductOwnershipFinding, ...] = ()
    violations: tuple[ProductOwnershipFinding, ...] = ()
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
            "ledger": LEDGERED_PRODUCT_FALLBACK,
            "removal_criterion": REMOVAL_CRITERION,
        }


def _default_core_root() -> Path:
    # core/application/boundary/telemetry_product_ownership_gate.py -> core/
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


def run_telemetry_product_ownership_gate(
    core_root: str | Path | None = None,
) -> ProductOwnershipReport:
    """Scan ``core_root`` (the ``okto_pulse/core`` dir) for references to the
    concrete ``ProductTelemetryAggregator``; any outside the ledger is a violation."""
    root = Path(core_root) if core_root is not None else _default_core_root()
    self_file = Path(__file__).resolve()
    findings: list[ProductOwnershipFinding] = []
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
                ProductOwnershipFinding(
                    file=rel, symbol=symbol, allowlisted=rel in ALLOWLISTED_FILES
                )
            )
    violations = tuple(f for f in findings if not f.allowlisted)
    return ProductOwnershipReport(
        ok=not violations,
        findings=tuple(findings),
        violations=violations,
        allowlist_files=tuple(sorted(ALLOWLISTED_FILES)),
    )


__all__ = [
    "SENSITIVE_SYMBOLS",
    "ALLOWLISTED_FILES",
    "LEDGERED_PRODUCT_FALLBACK",
    "REMOVAL_CRITERION",
    "ProductOwnershipFinding",
    "ProductOwnershipReport",
    "run_telemetry_product_ownership_gate",
]
