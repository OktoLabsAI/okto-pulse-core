"""TEST 6ae316b8 — fail-closed import gate for Community-owned KG runtime.

The core may expose ports and schema contracts, but it must not import the
Community-owned Ladybug/Kuzu runtime or the Community package. This test is AST
based so strings, comments and documentation do not count as executable leaks.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


CORE_RUNTIME_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"

FORBIDDEN_SCHEMA_RUNTIME_SYMBOLS = frozenset(
    {
        "open_board_connection",
        "open_board_connection_raw",
        "BoardConnection",
        "close_all_connections",
        "close_board_db_cache",
        "bootstrap_board_graph",
        "ensure_board_graph_bootstrapped",
        "purge_board_graph_storage",
        "migrate_schema_for_board",
        "apply_ladybug_lifecycle_step",
        "board_kuzu_path",
        "_open_kuzu_db",
        "load_vector_extension",
        "_is_ladybug_corruption_error",
    }
)


@dataclass(frozen=True)
class RuntimeImportViolation:
    file: str
    line: int
    import_ref: str
    reason: str


def _is_forbidden_module(module: str) -> str | None:
    if module in {"ladybug", "kuzu"} or module.startswith(("ladybug.", "kuzu.")):
        return "direct graph-runtime dependency"
    if module in {"sentence_transformers", "torch"} or module.startswith(
        ("sentence_transformers.", "torch.")
    ):
        return "concrete ML provider dependency belongs to the edition adapter"
    if module == "okto_pulse.community" or module.startswith("okto_pulse.community."):
        return "core must not import edition package"
    if module.startswith("okto_pulse.core.kg.providers.embedded.kuzu"):
        return "embedded Kuzu provider belongs to the edition adapter"
    return None


def _runtime_import_violations(root: Path) -> list[RuntimeImportViolation]:
    violations: list[RuntimeImportViolation] = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel = path.relative_to(root).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    reason = _is_forbidden_module(alias.name)
                    if reason is not None:
                        violations.append(
                            RuntimeImportViolation(rel, node.lineno, alias.name, reason)
                        )
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                module = node.module or ""
                reason = _is_forbidden_module(module)
                if reason is not None:
                    violations.append(
                        RuntimeImportViolation(rel, node.lineno, module, reason)
                    )
                    continue
                if module == "okto_pulse.core.kg.providers.embedded":
                    for alias in node.names:
                        if alias.name.startswith("kuzu"):
                            violations.append(
                                RuntimeImportViolation(
                                    rel,
                                    node.lineno,
                                    f"{module}.{alias.name}",
                                    "embedded Kuzu provider belongs to the edition adapter",
                                )
                            )
                if module == "okto_pulse.core.kg.schema":
                    for alias in node.names:
                        if alias.name in FORBIDDEN_SCHEMA_RUNTIME_SYMBOLS:
                            violations.append(
                                RuntimeImportViolation(
                                    rel,
                                    node.lineno,
                                    f"{module}.{alias.name}",
                                    "core consumer must use a KG port instead of schema runtime shim",
                                )
                            )
    return violations


def test_6ae316b8_core_runtime_import_gate_is_clean_for_real_core():
    violations = _runtime_import_violations(CORE_RUNTIME_ROOT)
    assert violations == []


def test_6ae316b8_core_runtime_import_gate_blocks_recontamination(tmp_path):
    (tmp_path / "ok.py").write_text(
        'DOC = "import ladybug as kuzu is only text"\n'
        "# from okto_pulse.community.adapters import kg_runtime\n"
        "from okto_pulse.core.kg.schema_contract import NODE_TYPES\n",
        encoding="utf-8",
    )
    (tmp_path / "bad.py").write_text(
        "import ladybug as kuzu\n"
        "import torch\n"
        "from sentence_transformers import SentenceTransformer\n"
        "from okto_pulse.community.adapters import kg_runtime\n"
        "from okto_pulse.core.kg.providers.embedded import kuzu_graph_store\n"
        "from okto_pulse.core.kg.schema import open_board_connection\n",
        encoding="utf-8",
    )

    violations = _runtime_import_violations(tmp_path)

    assert [v.import_ref for v in violations] == [
        "ladybug",
        "torch",
        "sentence_transformers",
        "okto_pulse.community.adapters",
        "okto_pulse.core.kg.providers.embedded.kuzu_graph_store",
        "okto_pulse.core.kg.schema.open_board_connection",
    ]
