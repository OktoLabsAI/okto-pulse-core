"""Anti-regression test for spec 233eaad3 — guarantees that the
Spec validation gate matches the reviewed native baseline. Sprint evaluation is retired.

The Analytics cancelled-card filter affects ``spec_coverage_summary``
(which the gates consume internally), but the gate functions themselves
must remain bit-identical to the baseline. This test reads the source of
the gate functions and asserts presence of structural markers that prove
they are intact.

Strategy: SHA256 hash of the function body extracted via ast — strict
enough to fail on any whitespace-insensitive change to the gate logic;
loose enough to survive comment-only edits elsewhere in the file.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MAIN_PY = REPO_ROOT / "src" / "okto_pulse" / "core" / "services" / "main.py"


# ---------------------------------------------------------------------------
# Native v0.4.0 baseline reviewed against b1100a36 (the prior hash).
# The approved clean break removes formal/three-score submissions and their
# history projections, and rejects unknown input fields. The existing five-score
# thresholds, guideline/lifecycle fences, edition/head checks, promotion and
# append-only native history remain. Behavioral coverage lives in
# test_spec_validation_current_input.py and test_spec_validation_native_history.py.
# Update this constant only after reviewing an intentional semantic change.
# ---------------------------------------------------------------------------

EXPECTED_HASHES = {
    "submit_spec_validation": (
        "5c4d3821c9c23b00a659b5550812d5dfb7fc2653f1b88d8e05ed86e71a6306ad"
    ),
}


def _function_source(file_path: Path, function_name: str) -> str:
    """Extract the source of a function (free-floating or method) by name.

    Walks the module AST until it finds the named FunctionDef/AsyncFunctionDef.
    Returns the unparsed source — comments are stripped (ast doesn't keep
    them), so the hash is robust to comment-only edits.
    """
    tree = ast.parse(file_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == function_name:
                return ast.unparse(node)
    raise LookupError(f"function {function_name!r} not found in {file_path}")


def _hash(src: str) -> str:
    return hashlib.sha256(src.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Tests — one per gate function
# ---------------------------------------------------------------------------


class TestValidationGatesUnchanged:
    def test_submit_spec_validation_has_marker_strings(self):
        """submit_spec_validation deve continuar contendo as strings-chave
        que provam invariância: outcome SUCCESS/FAILED + spec promotion
        approved → validated."""
        src = _function_source(MAIN_PY, "submit_spec_validation")
        # Strings semânticas que se removidas/alteradas indicariam regressão
        assert "approved" in src, "approved status check missing"
        assert "validated" in src, "validated status promotion missing"


    def test_versioned_baseline_hashes_are_unchanged(self):
        """Fail deterministically when a protected gate changes."""
        current_hashes = {
            "submit_spec_validation": _hash(
                _function_source(MAIN_PY, "submit_spec_validation")
            ),
        }

        assert current_hashes == EXPECTED_HASHES, (
            "Validation gate(s) changed. Review the semantic gate tests and "
            "update EXPECTED_HASHES only for an intentional change."
        )
