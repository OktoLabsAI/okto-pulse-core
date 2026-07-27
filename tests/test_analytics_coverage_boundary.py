"""Boundary tests for the Analytics coverage calculator slice."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COVERAGE_CALCULATOR = (
    ROOT / "src" / "okto_pulse" / "core" / "services" / "coverage_calculator.py"
)

FORBIDDEN_IMPORT_PREFIXES = (
    "fastapi",
    "sqlalchemy",
    "okto_pulse.core.mcp",
    "okto_pulse.community.api",
    "okto_pulse.core.models",
    "okto_pulse.community",
)

FORBIDDEN_GATE_TERMS = (
    "submit_spec_evaluation",
    "submit_sprint_evaluation",
    "SprintEvaluationCreate",
    "breakdown_completeness",
    "overall_score",
)


def _imported_modules(tree: ast.AST) -> list[str]:
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def test_coverage_calculator_has_no_rest_db_mcp_or_model_imports():
    tree = ast.parse(COVERAGE_CALCULATOR.read_text(encoding="utf-8"))

    offenders = [
        module
        for module in _imported_modules(tree)
        if any(
            module == prefix or module.startswith(f"{prefix}.")
            for prefix in FORBIDDEN_IMPORT_PREFIXES
        )
    ]
    assert offenders == []


def test_coverage_calculator_does_not_reference_manual_evaluation_gate_terms():
    source = COVERAGE_CALCULATOR.read_text(encoding="utf-8")

    for term in FORBIDDEN_GATE_TERMS:
        assert term not in source
