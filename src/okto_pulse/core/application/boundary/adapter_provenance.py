"""Fail-closed provenance audit for edition-owned adapters.

The scanner is pure stdlib and treats every undeclared Core surface as private.
It therefore cannot turn an implementation reach-in into a valid adapter merely
by renaming, reexporting or constructing it from Community code.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

from okto_pulse.core.ports.f13 import (
    AdapterBridgeLedgerEntry,
    AdapterProvenanceRegistration,
)

PUBLIC_CONTRACT = "public_contract"
GOVERNED_BRIDGE = "governed_temporary_bridge"
UNGOVERNED_BRIDGE = "ungoverned_private_bridge"


@dataclass(frozen=True, slots=True)
class CoreReference:
    file_path: str
    scope: str
    target: str
    line: int
    reference_kind: str


@dataclass(frozen=True, slots=True)
class AdapterBridge:
    file_path: str
    scope: str
    bridge_kind: str
    target: str
    line: int

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.file_path, self.scope, self.bridge_kind, self.target)


def _matches(target: str, prefixes: Sequence[str]) -> bool:
    return any(target == item or target.startswith(item + ".") for item in prefixes)


def _is_core(target: str) -> bool:
    return target == "okto_pulse.core" or target.startswith("okto_pulse.core.")


def _is_public(
    target: str,
    public_surfaces: Sequence[str],
    private_surfaces: Sequence[str] = (),
) -> bool:
    return _matches(target, public_surfaces) and not _matches(target, private_surfaces)


def _scope(stack: Sequence[str]) -> str:
    return ".".join(stack) if stack else "<module>"


class _ProvenanceVisitor(ast.NodeVisitor):
    def __init__(
        self,
        *,
        file_path: str,
        public_surfaces: Sequence[str],
        private_surfaces: Sequence[str],
    ) -> None:
        self.file_path = file_path
        self.public_surfaces = public_surfaces
        self.private_surfaces = private_surfaces
        self.scope_stack: list[str] = []
        self.bindings: dict[str, str] = {}
        self.references: list[CoreReference] = []
        self.bridges: list[AdapterBridge] = []
        self._bridge_keys: set[tuple[str, str, str, str, int]] = set()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope_stack.append(node.name)
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.scope_stack.append(node.name)
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.scope_stack.append(node.name)
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            target = alias.name
            local_name = alias.asname or alias.name.split(".", 1)[0]
            if alias.asname:
                self.bindings[local_name] = target
            if _is_core(target):
                self._reference(node.lineno, target, "import")
                if not self._public(target):
                    kind = "import_alias" if alias.asname else "import"
                    self._bridge(node.lineno, kind, target)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if not node.module:
            return
        for alias in node.names:
            target = f"{node.module}.{alias.name}"
            local_name = alias.asname or alias.name
            self.bindings[local_name] = target
            if _is_core(target):
                self._reference(node.lineno, target, "import_from")
                if not self._public(target):
                    kind = "import_alias" if alias.asname else "import_from"
                    self._bridge(node.lineno, kind, target)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets):
            self._capture_reexports(node)
        resolved = self._resolve_expr(node.value)
        if isinstance(node.value, ast.Call):
            resolved = self._dynamic_import_target(node.value) or resolved
        if resolved and _is_core(resolved) and not self._public(resolved):
            for target in node.targets:
                for name in self._assigned_names(target):
                    if name != "__all__":
                        self.bindings[name] = resolved
                        self._bridge(node.lineno, "assignment_alias", resolved)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            resolved = self._resolve_expr(node.value)
            if isinstance(node.value, ast.Call):
                resolved = self._dynamic_import_target(node.value) or resolved
            if resolved and _is_core(resolved) and not self._public(resolved):
                for name in self._assigned_names(node.target):
                    self.bindings[name] = resolved
                    self._bridge(node.lineno, "assignment_alias", resolved)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        dynamic_target = self._dynamic_import_target(node)
        if dynamic_target is not None and _is_core(dynamic_target):
            self._reference(node.lineno, dynamic_target, "dynamic_import")
            if not self._public(dynamic_target):
                self._bridge(node.lineno, "dynamic_import", dynamic_target)
            self.generic_visit(node)
            return

        getattr_target = self._dynamic_getattr_target(node)
        if getattr_target is not None and _is_core(getattr_target):
            self._reference(node.lineno, getattr_target, "dynamic_getattr")
            if not self._public(getattr_target):
                self._bridge(node.lineno, "dynamic_getattr", getattr_target)

        constructor_target = self._resolve_expr(node.func)
        if (
            constructor_target
            and _is_core(constructor_target)
            and not self._public(constructor_target)
        ):
            self._bridge(node.lineno, "constructor_target", constructor_target)
        self.generic_visit(node)

    def _capture_reexports(self, node: ast.Assign) -> None:
        values: Iterable[ast.expr]
        if isinstance(node.value, (ast.List, ast.Tuple, ast.Set)):
            values = node.value.elts
        else:
            values = ()
        for item in values:
            if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
                continue
            resolved = self.bindings.get(item.value)
            if resolved and _is_core(resolved) and not self._public(resolved):
                self._bridge(node.lineno, "dunder_all_reexport", resolved)

    def _dynamic_import_target(self, node: ast.Call) -> str | None:
        name = self._resolve_expr(node.func)
        is_import = name in {
            "importlib.import_module",
            "import_module",
            "__import__",
        }
        if not is_import or not node.args:
            return None
        value = node.args[0]
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
        return None

    def _dynamic_getattr_target(self, node: ast.Call) -> str | None:
        if not isinstance(node.func, ast.Name) or node.func.id != "getattr":
            return None
        if len(node.args) < 2:
            return None
        owner = self._resolve_expr(node.args[0])
        symbol = node.args[1]
        if owner is None:
            return None
        if isinstance(symbol, ast.Constant) and isinstance(symbol.value, str):
            return f"{owner}.{symbol.value}"
        return f"{owner}.<dynamic>"

    def _resolve_expr(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            if node.id == "importlib":
                return "importlib"
            return self.bindings.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            owner = self._resolve_expr(node.value)
            return f"{owner}.{node.attr}" if owner else None
        return None

    @staticmethod
    def _assigned_names(node: ast.AST) -> tuple[str, ...]:
        if isinstance(node, ast.Name):
            return (node.id,)
        if isinstance(node, (ast.Tuple, ast.List)):
            return tuple(
                name
                for item in node.elts
                for name in _ProvenanceVisitor._assigned_names(item)
            )
        return ()

    def _public(self, target: str) -> bool:
        return _is_public(target, self.public_surfaces, self.private_surfaces)

    def _reference(self, line: int, target: str, kind: str) -> None:
        self.references.append(
            CoreReference(self.file_path, _scope(self.scope_stack), target, line, kind)
        )

    def _bridge(self, line: int, kind: str, target: str) -> None:
        key = (self.file_path, _scope(self.scope_stack), kind, target, line)
        if key in self._bridge_keys:
            return
        self._bridge_keys.add(key)
        self.bridges.append(
            AdapterBridge(self.file_path, _scope(self.scope_stack), kind, target, line)
        )


def _community_root(source_root: Path) -> Path:
    candidate = source_root / "src" / "okto_pulse" / "community"
    return candidate if candidate.exists() else source_root


def _scan(
    source_root: Path,
    public_surfaces: Sequence[str],
    private_surfaces: Sequence[str],
) -> tuple[tuple[CoreReference, ...], tuple[AdapterBridge, ...], tuple[dict[str, object], ...]]:
    references: list[CoreReference] = []
    bridges: list[AdapterBridge] = []
    parse_violations: list[dict[str, object]] = []
    package_root = _community_root(source_root)
    for path in sorted(package_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            relative = path.relative_to(source_root).as_posix()
        except ValueError:
            relative = path.as_posix()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError) as exc:
            parse_violations.append(
                {"file_path": relative, "category": "source_not_parseable", "detail": str(exc)}
            )
            continue
        visitor = _ProvenanceVisitor(
            file_path=relative,
            public_surfaces=public_surfaces,
            private_surfaces=private_surfaces,
        )
        visitor.visit(tree)
        references.extend(visitor.references)
        bridges.extend(visitor.bridges)
    return tuple(references), tuple(bridges), tuple(parse_violations)


def _registration_violations(
    source_root: Path,
    registrations: Sequence[AdapterProvenanceRegistration],
    public_surfaces: Sequence[str],
    private_surfaces: Sequence[str],
) -> tuple[dict[str, object], ...]:
    violations: list[dict[str, object]] = []
    seen: set[str] = set()
    for registration in registrations:
        if registration.adapter_key in seen:
            violations.append(
                {"adapter_key": registration.adapter_key, "category": "duplicate_adapter_key"}
            )
        seen.add(registration.adapter_key)
        required = (
            registration.adapter_key,
            registration.owner,
            registration.implementation_module,
            registration.implementation_symbol,
            registration.port_module,
            registration.port_symbol,
            registration.contract_test,
        )
        if not all(required):
            violations.append(
                {"adapter_key": registration.adapter_key, "category": "incomplete_registration"}
            )
            continue
        if not registration.implementation_module.startswith("okto_pulse.community."):
            violations.append(
                {"adapter_key": registration.adapter_key, "category": "implementation_not_community_owned"}
            )
        if not _is_public(
            registration.port_target, public_surfaces, private_surfaces
        ):
            violations.append(
                {"adapter_key": registration.adapter_key, "category": "port_not_public", "target": registration.port_target}
            )
        invalid_dependencies = tuple(
            dependency
            for dependency in registration.dependencies
            if _is_core(dependency)
            and not _is_public(dependency, public_surfaces, private_surfaces)
        )
        if invalid_dependencies:
            violations.append(
                {
                    "adapter_key": registration.adapter_key,
                    "category": "private_core_dependency",
                    "dependencies": invalid_dependencies,
                }
            )

        module_path = source_root / "src" / Path(*registration.implementation_module.split("."))
        module_file = module_path.with_suffix(".py")
        if not module_file.exists():
            violations.append(
                {"adapter_key": registration.adapter_key, "category": "implementation_module_missing", "path": module_file.as_posix()}
            )
            continue
        tree = ast.parse(module_file.read_text(encoding="utf-8"), filename=str(module_file))
        local_definitions = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        }
        if registration.implementation_symbol not in local_definitions:
            violations.append(
                {
                    "adapter_key": registration.adapter_key,
                    "category": "implementation_symbol_not_locally_defined",
                    "symbol": registration.implementation_symbol,
                }
            )
        test_path_text, separator, test_symbol = registration.contract_test.partition("::")
        test_path = source_root / test_path_text
        if not separator or not test_path.exists():
            violations.append(
                {
                    "adapter_key": registration.adapter_key,
                    "category": "contract_test_missing",
                    "contract_test": registration.contract_test,
                }
            )
        else:
            test_tree = ast.parse(
                test_path.read_text(encoding="utf-8"), filename=str(test_path)
            )
            test_definitions = {
                node.name
                for node in test_tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            if test_symbol not in test_definitions:
                violations.append(
                    {
                        "adapter_key": registration.adapter_key,
                        "category": "contract_test_function_missing",
                        "contract_test": registration.contract_test,
                    }
                )
    return tuple(violations)


def audit_adapter_provenance(
    source_root: str | Path,
    *,
    public_core_surfaces: Sequence[str],
    private_core_surfaces: Sequence[str] = (),
    registrations: Sequence[AdapterProvenanceRegistration] = (),
    bridge_ledger: Sequence[AdapterBridgeLedgerEntry] = (),
    bridge_budget: int = 0,
) -> dict[str, object]:
    """Return an executable full-inventory and bridge/provenance verdict."""

    root = Path(source_root)
    references, bridges, parse_violations = _scan(
        root, public_core_surfaces, private_core_surfaces
    )
    ledger_by_key = {entry.key: entry for entry in bridge_ledger}
    bridge_by_key = {bridge.key: bridge for bridge in bridges}
    inventory = tuple(
        {
            **asdict(reference),
            "classification": (
                PUBLIC_CONTRACT
                if _is_public(
                    reference.target, public_core_surfaces, private_core_surfaces
                )
                else (
                    GOVERNED_BRIDGE
                    if any(
                        bridge.target == reference.target and bridge.file_path == reference.file_path
                        and bridge.key in ledger_by_key
                        for bridge in bridges
                    )
                    else UNGOVERNED_BRIDGE
                )
            ),
        }
        for reference in references
    )
    unledgered = tuple(
        asdict(bridge) for bridge in bridges if bridge.key not in ledger_by_key
    )
    stale_ledger = tuple(
        asdict(entry) for entry in bridge_ledger if entry.key not in bridge_by_key
    )
    incomplete_ledger = tuple(
        asdict(entry)
        for entry in bridge_ledger
        if not all(
            (
                entry.file_path,
                entry.scope,
                entry.bridge_kind,
                entry.target,
                entry.owner,
                entry.target_port,
                entry.removal_path,
                entry.withdrawal_criterion,
            )
        )
    )
    invalid_surfaces = tuple(
        surface
        for surface in public_core_surfaces
        if surface in {"okto_pulse.core", "okto_pulse.core.kg", "okto_pulse.core.services"}
    )
    budget_violation = (
        {
            "category": "bridge_budget_exceeded",
            "bridge_budget": bridge_budget,
            "observed": len(bridges),
        },
    ) if len(bridges) > bridge_budget else ()
    registration_violations = _registration_violations(
        root, registrations, public_core_surfaces, private_core_surfaces
    )
    violations = (
        parse_violations
        + unledgered
        + stale_ledger
        + incomplete_ledger
        + budget_violation
        + registration_violations
        + tuple(
            {"category": "invalid_public_surface", "surface": surface}
            for surface in invalid_surfaces
        )
    )
    return {
        "ok": not violations,
        "bridge_budget": bridge_budget,
        "bridge_count": len(bridges),
        "bridges": tuple(asdict(item) for item in bridges),
        "ledger_count": len(bridge_ledger),
        "stale_ledger": stale_ledger,
        "incomplete_ledger": incomplete_ledger,
        "registration_count": len(registrations),
        "registration_violations": registration_violations,
        "inventory_count": len(inventory),
        "inventory": inventory,
        "inventory_by_classification": dict(
            sorted(Counter(item["classification"] for item in inventory).items())
        ),
        "violations": violations,
    }


__all__ = [
    "GOVERNED_BRIDGE",
    "PUBLIC_CONTRACT",
    "UNGOVERNED_BRIDGE",
    "AdapterBridge",
    "CoreReference",
    "audit_adapter_provenance",
]
