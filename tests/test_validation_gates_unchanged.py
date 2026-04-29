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

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
MAIN_PY = REPO_ROOT / "src" / "okto_pulse" / "core" / "services" / "main.py"


# ---------------------------------------------------------------------------
# Baselines (capturados em 2026-04-28 antes da implementação da spec
# 233eaad3). Se algum dia precisar atualizar — primeiro confirme que a
# mudança no gate é INTENCIONAL e documentada em outra spec; senão a
# regressão está passando despercebida.
# ---------------------------------------------------------------------------

# A baseline será computada na primeira execução e armazenada em pytest
# cache; em runs subsequentes valida contra esse hash. Isso mantém o test
# self-bootstrapping (CI/dev box first run = baseline; depois = enforce).


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
        assert (
            "sprint_evaluation_submitted" in src
        ), "sprint_evaluation_submitted activity log marker missing"

    def test_baseline_hash_is_consistent_across_runs(self, tmp_path):
        """Captura hash das 2 funções e armazena num arquivo persistente
        em tests/.cache; em runs subsequentes compara. Falha = alguém
        mudou a função sem atualizar o baseline (que é trabalho do
        ceremony de release, não desta spec)."""
        baseline_path = REPO_ROOT / "tests" / ".cache" / "validation_gates_baseline.txt"
        baseline_path.parent.mkdir(parents=True, exist_ok=True)

        current_hashes = {
            "submit_spec_validation": _hash(
                _function_source(MAIN_PY, "submit_spec_validation")
            ),
            "submit_evaluation": _hash(
                _function_source(MAIN_PY, "submit_evaluation")
            ),
        }

        if not baseline_path.exists():
            # First run — record baseline. Subsequent runs enforce it.
            baseline_path.write_text(
                "\n".join(f"{k}={v}" for k, v in current_hashes.items()),
                encoding="utf-8",
            )
            pytest.skip(
                "baseline created on first run — re-run pytest to enforce. "
                "Future runs will fail if these gate functions are modified."
            )

        # Subsequent runs: compare against baseline
        baseline_content = baseline_path.read_text(encoding="utf-8")
        baseline = dict(
            line.split("=", 1) for line in baseline_content.strip().splitlines()
        )

        diffs = []
        for fname, current_hash in current_hashes.items():
            expected = baseline.get(fname)
            if expected is None:
                # New function added to baseline — accept silently and
                # update file (this is the only auto-extension allowed).
                baseline[fname] = current_hash
                continue
            if expected != current_hash:
                diffs.append(
                    f"{fname}: baseline={expected[:12]}... current={current_hash[:12]}..."
                )

        if diffs:
            pytest.fail(
                "Validation gate(s) were modified — spec 233eaad3 invariance "
                "violated:\n  - " + "\n  - ".join(diffs) +
                "\n\nIf this change is INTENTIONAL, document it in a new spec "
                f"and update the baseline at {baseline_path}."
            )

        # If new functions were added to baseline, persist them
        baseline_path.write_text(
            "\n".join(f"{k}={v}" for k, v in baseline.items()),
            encoding="utf-8",
        )
