"""AntiSingletonGate — block NEW module-global singletons in core (spec #15).

fr_95d98ef5: no module-global singleton may be introduced in the core; every
detected process authority blocks. The former register-before-remove ledger is
empty after the F16 terminal migration.

Detection is deterministic and NARROW (AST, no import): a module-global is a
singleton when it is reassigned via a ``global`` statement (mutated process
state), is a ``ContextVar``, or is provider-bridge cache/lock state in a
``llm_provider_bridges.py`` module. Module constants — ``__all__``, lookup
tables, metric sample buffers — are NOT singletons and are never flagged.

``BASELINE_SINGLETONS`` remains empty. Runtime authorities must be injected
through composition providers/ports; task-local ``ContextVar`` state is the
only explicit safe mechanism.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

# Terminal state: no process-wide singleton is authorized in Core. Context-local
# values below are scoped runtime mechanisms and do not share mutable authority.
SINGLETON_LEDGER: dict[str, dict[str, str]] = {}
RUNTIME_SINGLETON_BASELINE_LEDGER: dict[str, dict[str, str]] = {}
BASELINE_SINGLETONS: frozenset[str] = frozenset()
BASELINE_SINGLETONS_WITHOUT_RUNTIME_LEDGER: frozenset[str] = frozenset()
SAFE_CONTEXT_LOCAL_STATE: frozenset[str] = frozenset(
    {
        "okto_pulse/core/composition.py::_active_runtime_composition",
        "okto_pulse/core/kg/global_discovery_writer.py::_active_lease",
        "okto_pulse/core/kg/write_barrier.py::_active_guards",
        "okto_pulse/core/runtime_context.py::_active_runtime_values",
    }
)

_REQUIRED_RUNTIME_LEDGER_FIELDS = ("owner", "target_provider", "retirement_criterion")


@dataclass(frozen=True)
class SingletonOccurrence:
    """A module-global singleton found by the scanner."""

    name: str
    file: str
    kind: str  # "global_mutation" | "contextvar" | "provider_bridge_global_state"

    @property
    def key(self) -> str:
        return f"{self.file}::{self.name}"


@dataclass(frozen=True)
class AntiSingletonGateInput:
    source_root: Path | None = None
    #: extra ``file::name`` keys to treat as already-baselined (tests).
    extra_baseline: tuple[str, ...] = ()
    #: scan only these files (relative posix); () = whole core tree.
    only_files: tuple[str, ...] = ()


def _default_source_root() -> Path:
    # src/okto_pulse/core/application/boundary/singleton_gate.py -> src/
    return Path(__file__).resolve().parents[4]


def _is_contextvar(value: ast.expr | None) -> bool:
    if not isinstance(value, ast.Call):
        return False
    func = value.func
    return (isinstance(func, ast.Name) and func.id == "ContextVar") or (
        isinstance(func, ast.Attribute) and func.attr == "ContextVar"
    )


def _module_level_targets(tree: ast.Module) -> dict[str, ast.expr | None]:
    out: dict[str, ast.expr | None] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id.startswith("_"):
                    out[tgt.id] = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id.startswith("_"):
                out[node.target.id] = node.value
    return out


_PROVIDER_BRIDGE_STATE_NAMES = frozenset({"_bridge_cache", "_bridge_lock"})
_MUTATING_METHODS = frozenset(
    {
        "add",
        "append",
        "clear",
        "discard",
        "extend",
        "pop",
        "popitem",
        "remove",
        "setdefault",
        "update",
    }
)


def _is_provider_bridge_file(rel: str) -> bool:
    return rel.startswith("okto_pulse/core/") and rel.endswith("/llm_provider_bridges.py")


def _is_module_constant_name(name: str) -> bool:
    stripped = name.strip("_")
    return bool(stripped) and stripped.upper() == stripped


def _call_name(expr: ast.expr | None) -> str:
    if not isinstance(expr, ast.Call):
        return ""
    func = expr.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _is_mutable_or_lock_value(value: ast.expr | None) -> bool:
    if isinstance(value, (ast.Dict, ast.List, ast.Set)):
        return True
    call_name = _call_name(value)
    return call_name in {
        "BoundedCounterSampleBuffer",
        "BoundedSampleBuffer",
        "Counter",
        "Event",
        "Lock",
        "OrderedDict",
        "RLock",
        "defaultdict",
        "deque",
    }


def _root_name(expr: ast.AST) -> str | None:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Subscript):
        return _root_name(expr.value)
    if isinstance(expr, ast.Attribute):
        return _root_name(expr.value)
    return None


def _mutated_module_names(tree: ast.Module) -> set[str]:
    mutated: set[str] = set()
    runtime_roots = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    for node in (
        descendant
        for root in runtime_roots
        for descendant in ast.walk(root)
    ):
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets: list[ast.AST]
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            else:
                targets = [node.target]
            for target in targets:
                if isinstance(target, (ast.Subscript, ast.Attribute)):
                    name = _root_name(target)
                    if name:
                        mutated.add(name)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            name = _root_name(node.func.value)
            if name and node.func.attr in _MUTATING_METHODS:
                mutated.add(name)
    return mutated


def _is_provider_bridge_state_name(name: str) -> bool:
    if name in _PROVIDER_BRIDGE_STATE_NAMES:
        return True
    if _is_module_constant_name(name):
        return False
    lower = name.lower()
    return name.startswith("_") and "bridge" in lower and (
        "cache" in lower or "lock" in lower
    )


def _provider_bridge_occurrences(
    rel: str,
    tree: ast.Module,
    module_names: dict[str, ast.expr | None],
) -> list[SingletonOccurrence]:
    if not _is_provider_bridge_file(rel):
        return []
    mutated_names = _mutated_module_names(tree)
    found: list[SingletonOccurrence] = []
    for name, value in module_names.items():
        if not _is_provider_bridge_state_name(name):
            continue
        if name in _PROVIDER_BRIDGE_STATE_NAMES:
            found.append(
                SingletonOccurrence(
                    name=name,
                    file=rel,
                    kind="provider_bridge_global_state",
                )
            )
            continue
        if name in mutated_names or _is_mutable_or_lock_value(value):
            found.append(
                SingletonOccurrence(
                    name=name,
                    file=rel,
                    kind="provider_bridge_global_state",
                )
            )
    return found


def _scan_module(rel: str, tree: ast.Module) -> list[SingletonOccurrence]:
    module_names = _module_level_targets(tree)
    mutated_names = _mutated_module_names(tree)
    global_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Global):
            global_names.update(node.names)
    found: list[SingletonOccurrence] = []
    for name, value in module_names.items():
        if name in global_names:
            kind = "global_mutation"
        elif _is_contextvar(value):
            kind = "contextvar"
        elif _is_mutable_or_lock_value(value) and (
            name in mutated_names
            or _call_name(value)
            in {
                "BoundedCounterSampleBuffer",
                "BoundedSampleBuffer",
                "Event",
                "Lock",
                "RLock",
            }
        ):
            kind = "module_runtime_state"
        else:
            continue
        occurrence = SingletonOccurrence(name=name, file=rel, kind=kind)
        if occurrence.key not in SAFE_CONTEXT_LOCAL_STATE:
            found.append(occurrence)
    found.extend(_provider_bridge_occurrences(rel, tree, module_names))
    return found


def _runtime_ledger_entry(o: SingletonOccurrence) -> dict[str, str] | None:
    keyed = RUNTIME_SINGLETON_BASELINE_LEDGER.get(o.key)
    if keyed is not None:
        return keyed if keyed.get("file") == o.file else None
    named = SINGLETON_LEDGER.get(o.name)
    if named is not None and named.get("file") == o.file:
        return named
    return None


def _missing_runtime_ledger_entries(
    occurrences: list[SingletonOccurrence],
    baseline: set[str],
) -> list[dict[str, str]]:
    missing: list[dict[str, str]] = []
    for occurrence in occurrences:
        if occurrence.key not in baseline:
            continue
        if occurrence.key in BASELINE_SINGLETONS_WITHOUT_RUNTIME_LEDGER:
            continue
        entry = _runtime_ledger_entry(occurrence)
        reason = ""
        if entry is None:
            reason = "missing_runtime_ledger"
        elif any(not entry.get(field) for field in _REQUIRED_RUNTIME_LEDGER_FIELDS):
            reason = "missing_runtime_ledger_metadata"
        if reason:
            missing.append(
                {
                    "key": occurrence.key,
                    "name": occurrence.name,
                    "file": occurrence.file,
                    "kind": occurrence.kind,
                    "reason": reason,
                }
            )
    return sorted(missing, key=lambda item: item["key"])


class AntiSingletonGate:
    """Blocks new module-global singletons; ledgers the known ones."""

    gate_id = "anti_singleton"

    def run(self, gate_input: AntiSingletonGateInput | None = None) -> GateReport:
        gate_input = gate_input or AntiSingletonGateInput()
        root = gate_input.source_root or _default_source_root()
        core = root / "okto_pulse" / "core"
        baseline = set(BASELINE_SINGLETONS) | set(gate_input.extra_baseline)

        occurrences: list[SingletonOccurrence] = []
        for py in sorted(core.rglob("*.py")):
            rel = py.relative_to(root).as_posix()
            if gate_input.only_files and rel not in gate_input.only_files:
                continue
            try:
                tree = ast.parse(py.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            occurrences.extend(_scan_module(rel, tree))

        new_singletons = [o for o in occurrences if o.key not in baseline]
        missing_runtime_ledger = _missing_runtime_ledger_entries(occurrences, baseline)
        ledger_view = {
            name: {**meta, "status": "ledgered"}
            for name, meta in SINGLETON_LEDGER.items()
        }
        runtime_ledger_view = {
            key: {**meta, "status": "ledgered"}
            for key, meta in RUNTIME_SINGLETON_BASELINE_LEDGER.items()
        }
        evidence = {
            "ledger": ledger_view,
            "runtime_baseline_ledger": runtime_ledger_view,
            "non_runtime_baseline_exemptions": sorted(
                BASELINE_SINGLETONS_WITHOUT_RUNTIME_LEDGER
            ),
            "baseline_count": len(baseline),
            "detected_count": len(occurrences),
            "new_singletons": [
                {"name": o.name, "file": o.file, "kind": o.kind}
                for o in sorted(new_singletons, key=lambda o: o.key)
            ],
            "missing_runtime_ledger": missing_runtime_ledger,
            "scanned_root": core.relative_to(root).as_posix(),
        }

        if missing_runtime_ledger:
            return GateReport(
                gate_id=self.gate_id,
                subject="core module-global singletons",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "missing_singleton_ledger"},
                observed_value=[entry["key"] for entry in missing_runtime_ledger],
                expected_value=[],
                remediation_hint=(
                    "A baselined runtime singleton is missing owner, target provider "
                    "or retirement criterion. Add it to SINGLETON_LEDGER or the "
                    "per-occurrence RUNTIME_SINGLETON_BASELINE_LEDGER before accepting "
                    "the baseline."
                ),
            )

        if new_singletons:
            return GateReport(
                gate_id=self.gate_id,
                subject="core module-global singletons",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "new_singleton"},
                observed_value=sorted(o.key for o in new_singletons),
                expected_value=[],
                remediation_hint=(
                    "A new module-global singleton was introduced. Inject the "
                    "dependency through a RuntimeComposition provider/port instead. "
                    "If it is unavoidable transitional debt, register it in "
                    "BASELINE_SINGLETONS (and SINGLETON_LEDGER when it owns a runtime "
                    "resource) with owner, target provider and retirement criterion "
                    "(register-before-remove)."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="core module-global singletons",
            status="passed",
            severity="medium",
            owner="okto-pulse-core/architecture",
            evidence={**evidence, "safe_context_local_state": sorted(SAFE_CONTEXT_LOCAL_STATE)},
            observed_value=0,
            expected_value=0,
        )
