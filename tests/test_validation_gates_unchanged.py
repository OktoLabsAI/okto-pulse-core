"""Anti-regression test for spec 233eaad3 — guarantees that the
validation gates (submit_spec_validation, submit_spec_evaluation, and
submit_sprint_evaluation) were NOT modified.

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
# Baselines updated after the 2026-07-14 sprint reviewer-separation and
# optimistic-versioning contract, then after the 2026-07-25 append-only
# SpecHistory audit for validation submissions, after the 2026-07-27 SK-A A3
# checklist predicate, and after the 2026-07-31 SK-B3 semantic policy transition
# gate was deliberately added to successful Spec Validation. The 2026-08-12 SK-M
# lifecycle fence now serializes a successful validation with the board's Spec
# dependency graph before mutating the validation head. The canonical five-score
# quality contract intentionally replaced the old three-score gate while keeping
# legacy record compatibility. If a gate changes intentionally, review its
# semantic tests and update this versioned constant in the same change.
# ---------------------------------------------------------------------------

EXPECTED_HASHES = {
    "submit_spec_validation": (
        "e919bc3d104549153ca9b05fd6089117e2ca5477a29a37abbd0f88283025cccc"
    ),
    "submit_evaluation": (
        "90dc97c780b0c0f2297f7be6c627708bfaddc42f8f1a62b1138a80aea675ef9a"
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

    def test_submit_evaluation_has_marker_strings(self):
        """SprintService.submit_evaluation (sprint_evaluation) deve continuar
        gravando sprint_evaluation_submitted no activity log."""
        src = _function_source(MAIN_PY, "submit_evaluation")
        assert "sprint_evaluation_submitted" in src, (
            "sprint_evaluation_submitted activity log marker missing"
        )

    def test_versioned_baseline_hashes_are_unchanged(self):
        """Fail deterministically when a protected gate changes."""
        current_hashes = {
            "submit_spec_validation": _hash(
                _function_source(MAIN_PY, "submit_spec_validation")
            ),
            "submit_evaluation": _hash(_function_source(MAIN_PY, "submit_evaluation")),
        }

        assert current_hashes == EXPECTED_HASHES, (
            "Validation gate(s) changed. Review the semantic gate tests and "
            "update EXPECTED_HASHES only for an intentional change."
        )
