"""Static guard against native graph calls after asynchronous health starters."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


_NATIVE_GRAPH_CALLS = frozenset(
    {"bootstrap_board_graph", "open_board_connection", "open_board_connection_raw"}
)
_HEALTH_STARTERS = frozenset({"commit_consolidation", "get_kg_health"})
_BLOCKING_BRIDGES = frozenset({"run_blocking_graph_io", "run_sync", "to_thread"})


@dataclass(frozen=True)
class _FunctionKey:
    module: str
    qualname: str
    lineno: int


@dataclass(frozen=True)
class _Call:
    lineno: int
    name: str
    destination: _FunctionKey | None
    node: ast.Call


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _nearest_function(
    node: ast.AST,
    parents: dict[ast.AST, ast.AST],
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current
    return None


def _build_inventory():
    test_root = Path(__file__).parent
    paths = {path.stem: path for path in test_root.glob("*.py")}
    trees: dict[str, ast.Module] = {}
    parents_by_module: dict[str, dict[ast.AST, ast.AST]] = {}
    keys_by_node: dict[ast.AST, _FunctionKey] = {}
    top_level: dict[tuple[str, str], _FunctionKey] = {}
    nested: dict[tuple[ast.AST, str], _FunctionKey] = {}
    imported: dict[tuple[str, str], tuple[str, str]] = {}
    aliases: dict[tuple[str, str], str] = {}

    for module, path in paths.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        trees[module] = tree
        parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                parents[child] = parent
        parents_by_module[module] = parents

        function_nodes = [
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        for node in function_nodes:
            parts = [node.name]
            owner = _nearest_function(node, parents)
            current_owner = owner
            while current_owner is not None:
                parts.append(current_owner.name)
                current_owner = _nearest_function(current_owner, parents)
            key = _FunctionKey(module, ".".join(reversed(parts)), node.lineno)
            keys_by_node[node] = key
            if owner is None:
                top_level[(module, node.name)] = key
            else:
                nested[(owner, node.name)] = key

        for node in tree.body:
            if not isinstance(node, ast.ImportFrom):
                continue
            for imported_name in node.names:
                local_name = imported_name.asname or imported_name.name
                if node.module in paths:
                    imported[(module, local_name)] = (
                        node.module,
                        imported_name.name,
                    )
                elif imported_name.name in (
                    _NATIVE_GRAPH_CALLS | _HEALTH_STARTERS | _BLOCKING_BRIDGES
                ):
                    aliases[(module, local_name)] = imported_name.name

    def resolve(
        module: str,
        owner: ast.FunctionDef | ast.AsyncFunctionDef,
        name: str,
    ) -> _FunctionKey | None:
        scope: ast.FunctionDef | ast.AsyncFunctionDef | None = owner
        while scope is not None:
            destination = nested.get((scope, name))
            if destination is not None:
                return destination
            scope = _nearest_function(scope, parents_by_module[module])
        destination = top_level.get((module, name))
        if destination is not None:
            return destination
        source = imported.get((module, name))
        return top_level.get(source) if source is not None else None

    calls_by_function: dict[_FunctionKey, list[_Call]] = {}
    for module, tree in trees.items():
        parents = parents_by_module[module]
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            owner = _nearest_function(node, parents)
            if owner is None:
                continue
            name = _call_name(node)
            if name is None:
                continue
            canonical_name = aliases.get((module, name), name)
            key = keys_by_node[owner]
            calls_by_function.setdefault(key, []).append(
                _Call(
                    lineno=node.lineno,
                    name=canonical_name,
                    destination=resolve(module, owner, name),
                    node=node,
                )
            )

    return (
        calls_by_function,
        keys_by_node,
        parents_by_module,
        aliases,
        resolve,
    )


def test_async_tests_offload_native_graph_io_after_health_starters() -> None:
    (
        calls_by_function,
        keys_by_node,
        parents_by_module,
        aliases,
        resolve,
    ) = _build_inventory()
    nodes_by_key = {key: node for node, key in keys_by_node.items()}

    bridge_wrappers = {
        key
        for key, calls in calls_by_function.items()
        if any(call.name in _BLOCKING_BRIDGES for call in calls)
    }

    def is_protected(key: _FunctionKey, call: _Call) -> bool:
        owner = nodes_by_key[key]
        parents = parents_by_module[key.module]
        current: ast.AST = call.node
        while current in parents and parents[current] is not owner:
            current = parents[current]
            if not isinstance(current, ast.Call):
                continue
            name = _call_name(current)
            if name is None:
                continue
            canonical_name = aliases.get((key.module, name), name)
            destination = resolve(key.module, owner, name)
            if canonical_name in _BLOCKING_BRIDGES or destination in bridge_wrappers:
                return True
        return False

    unsafe_native: set[_FunctionKey] = set()
    probe_starters: set[_FunctionKey] = set()
    changed = True
    while changed:
        changed = False
        for key, calls in calls_by_function.items():
            if key not in unsafe_native and any(
                not is_protected(key, call)
                and (
                    call.name in _NATIVE_GRAPH_CALLS
                    or call.destination in unsafe_native
                )
                for call in calls
            ):
                unsafe_native.add(key)
                changed = True
            if key not in probe_starters and any(
                call.name in _HEALTH_STARTERS
                or call.destination in probe_starters
                for call in calls
            ):
                probe_starters.add(key)
                changed = True

    violations: list[str] = []
    for key, calls in calls_by_function.items():
        if not isinstance(nodes_by_key[key], ast.AsyncFunctionDef):
            continue
        preceding_starters: list[_Call] = []
        for call in sorted(calls, key=lambda item: (item.lineno, item.name)):
            if (
                call.name in _HEALTH_STARTERS
                or call.destination in probe_starters
            ):
                preceding_starters.append(call)
            is_native = (
                call.name in _NATIVE_GRAPH_CALLS
                or call.destination in unsafe_native
            )
            if (
                is_native
                and not is_protected(key, call)
                and any(starter.lineno < call.lineno for starter in preceding_starters)
            ):
                starter = preceding_starters[-1]
                violations.append(
                    f"{key.module}.py:{call.lineno} {key.qualname}: "
                    f"{call.name} after {starter.name}@{starter.lineno}"
                )

    assert violations == [], (
        "Native graph I/O can overlap a daemon health probe; dispatch it with "
        "run_blocking_graph_io:\n" + "\n".join(violations)
    )
