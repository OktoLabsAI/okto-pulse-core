"""F09 acceptance oracles for graph-neutral Core contracts."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from okto_pulse.core.application.boundary import GraphRuntimeSurfaceGate
from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphStatementResult,
    GraphTransactionScope,
)
from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry
from okto_pulse.core.kg.schema_import_classification_gate import (
    run_kg_schema_import_classification_gate,
)


CORE_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = CORE_ROOT / "src" / "okto_pulse" / "core"
PUBLIC_GRAPH_CONTRACTS = (
    *sorted((SOURCE_ROOT / "kg" / "interfaces").glob("*.py")),
    SOURCE_ROOT / "ports" / "global_outbox.py",
    SOURCE_ROOT / "ports" / "kg_operational.py",
)
FORBIDDEN_NAMES = {"path", "database", "connection"}
FORBIDDEN_FRAGMENTS = ("kuzu", "ladybug")


def _signature_tokens(path: Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    tokens: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            tokens.append((node.lineno, node.name))
            args = (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
            for arg in args:
                tokens.append((arg.lineno, arg.arg))
                if arg.annotation is not None:
                    tokens.append((arg.lineno, ast.unparse(arg.annotation)))
            if node.args.vararg is not None:
                tokens.append((node.args.vararg.lineno, node.args.vararg.arg))
            if node.args.kwarg is not None:
                tokens.append((node.args.kwarg.lineno, node.args.kwarg.arg))
            if node.returns is not None:
                tokens.append((node.lineno, ast.unparse(node.returns)))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            tokens.append((node.lineno, node.target.id))
            tokens.append((node.lineno, ast.unparse(node.annotation)))
    return tokens


def test_f09_public_core_graph_signatures_are_backend_neutral() -> None:
    violations: list[str] = []
    for path in PUBLIC_GRAPH_CONTRACTS:
        for line, token in _signature_tokens(path):
            normalized = token.lower()
            words = {part for part in normalized.replace("[", " ").replace("]", " ").replace(".", " ").replace(",", " ").split()}
            if words & FORBIDDEN_NAMES or any(
                fragment in normalized for fragment in FORBIDDEN_FRAGMENTS
            ):
                violations.append(f"{path.relative_to(CORE_ROOT)}:{line}:{token}")
    assert violations == []


def test_f09_concrete_core_wrappers_are_physically_removed() -> None:
    retired = (
        SOURCE_ROOT / "kg" / "schema.py",
        SOURCE_ROOT / "kg" / "connection_pool.py",
        SOURCE_ROOT / "kg" / "interfaces" / "board_graph_runtime.py",
        SOURCE_ROOT / "kg" / "interfaces" / "graph_path_resolver.py",
        SOURCE_ROOT / "kg" / "hybrid_search" / "kuzu_adapter.py",
        SOURCE_ROOT / "kg" / "stress_chaos_executor.py",
    )
    assert [str(path) for path in retired if path.exists()] == []
    fields = KGProviderRegistry.__dataclass_fields__
    assert {
        "board_graph_runtime",
        "graph_path_resolver",
        "safe_write_step_adapter",
    }.isdisjoint(fields)


def test_f09_transactions_return_materialized_core_results() -> None:
    signature = inspect.signature(GraphTransactionScope.execute)
    assert signature.return_annotation == "GraphStatementResult"
    result = GraphStatementResult.from_rows([["n1", 1]], columns=["id", "score"])
    assert result.rows == (("n1", 1),)
    assert result.columns == ("id", "score")
    assert list(result) == [["n1", 1]]


def test_f09_core_schema_contract_contains_no_ddl() -> None:
    source = (SOURCE_ROOT / "kg" / "schema_contract.py").read_text(encoding="utf-8")
    assert "CREATE NODE TABLE" not in source
    assert "CREATE REL TABLE" not in source
    assert "BoardGraphHandle" not in source


def test_f09_consumer_inventory_is_zero_and_has_no_allowance() -> None:
    report = run_kg_schema_import_classification_gate()
    assert report.ok is True
    assert report.importers == []
    assert report.violations == []
    assert set(report.recounted_refs["current"].values()) == {0}
    assert report.allowlist == {
        "embedded_prefix": "",
        "migration_files": [],
        "migration_cli": "",
        "ledgered_exceptions": [],
    }
    surface = GraphRuntimeSurfaceGate().run()
    assert surface.status == "passed"
    assert surface.evidence["compatibility_allowlist"] == []
