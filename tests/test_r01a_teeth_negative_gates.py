"""Spec R01A TEST-TEETH 1 — negative/removal gate evidence (card 461195f1).

Two adversarial ("teeth") scenarios that must FAIL on a reintroduced/removed
regression, not merely pass on the happy path:

- ts_4160b5c5: an AST fixture copies ``api/agents.py`` and strips
  ``invalidate_agent_cache`` from ONLY ``update_agent`` or
  ``update_board_overrides``; the check must flag that exact function (with a
  line), while the clean source — where grant/revoke/delete are intentionally
  NOT invalidation points — keeps passing.
- ts_8e14d173: a rogue use case with ``Depends(get_db)`` / ``get_db_for_mcp`` /
  ``AsyncSession`` is dropped into a use-cases-shaped root; the EXISTING
  ``run_relational_boundary_gate`` must fail with the rogue file + line, while a
  clean port-based use case (and the real use_cases root) still passes.

Safe-teeth: the removal is done on an in-memory AST copy and the rogue file lives
under ``tmp_path`` — never the real ``src/`` tree — so a concurrent gate run by
the validator can never observe a polluted working tree (carry-forward:
[[feedback_teeth_shared_file_race]]).
"""

from __future__ import annotations

import ast
from pathlib import Path

from okto_pulse.core.repositories.relational_boundary_gate import (
    default_use_cases_path,
    run_relational_boundary_gate,
)

# ===========================================================================
# ts_4160b5c5 — update_agent / update_board_overrides MUST invalidate the cache
# ===========================================================================

_CACHE_REQUIRED = ("update_agent", "update_board_overrides")


def _agents_source() -> str:
    from okto_pulse.community.api import agents

    return Path(agents.__file__).read_text(encoding="utf-8")


def _calls_invalidate(node: ast.AST) -> bool:
    return any(
        isinstance(c, ast.Call)
        and isinstance(c.func, ast.Name)
        and c.func.id == "invalidate_agent_cache"
        for c in ast.walk(node)
    )


def _functions_missing_cache_invalidation(source: str) -> list[tuple[str, int]]:
    """The teeth check: every function in ``_CACHE_REQUIRED`` MUST call
    ``invalidate_agent_cache``. Returns ``(name, lineno)`` for each that does not
    — the diagnostic the suite reports as ``agents.py:<function>``."""
    tree = ast.parse(source)
    missing: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
            and node.name in _CACHE_REQUIRED
            and not _calls_invalidate(node)
        ):
            missing.append((node.name, node.lineno))
    return missing


class _StripInvalidation(ast.NodeTransformer):
    """Remove the ``invalidate_agent_cache(...)`` call from ONE named function —
    the AST copy fixture from the scenario Given."""

    def __init__(self, func_name: str) -> None:
        self.func_name = func_name

    def _strip(self, node):
        if node.name == self.func_name:
            node.body = [
                stmt
                for stmt in node.body
                if not (
                    isinstance(stmt, ast.Expr)
                    and isinstance(stmt.value, ast.Call)
                    and isinstance(stmt.value.func, ast.Name)
                    and stmt.value.func.id == "invalidate_agent_cache"
                )
            ]
        return node

    def visit_FunctionDef(self, node):
        return self._strip(node)

    def visit_AsyncFunctionDef(self, node):
        return self._strip(node)


def _source_without_invalidation_in(func_name: str) -> str:
    tree = _StripInvalidation(func_name).visit(ast.parse(_agents_source()))
    ast.fix_missing_locations(tree)
    return ast.unparse(tree)


def test_real_agents_update_functions_invalidate_cache() -> None:
    """Clean variant: the real agents.py keeps both invalidation points."""
    assert _functions_missing_cache_invalidation(_agents_source()) == []


def test_removing_invalidation_from_update_agent_is_caught() -> None:
    violating = _source_without_invalidation_in("update_agent")
    missing = _functions_missing_cache_invalidation(violating)
    names = {name for name, _ in missing}
    # the failure points exactly at update_agent (with a line) ...
    assert "update_agent" in names
    assert all(line > 0 for _, line in missing)
    # ... and update_board_overrides is untouched (still invalidates).
    assert "update_board_overrides" not in names


def test_removing_invalidation_from_update_board_overrides_is_caught() -> None:
    violating = _source_without_invalidation_in("update_board_overrides")
    names = {name for name, _ in _functions_missing_cache_invalidation(violating)}
    assert "update_board_overrides" in names
    assert "update_agent" not in names


def test_grant_revoke_delete_are_not_invalidation_points() -> None:
    """ac_8e695cf2: grant/revoke/delete are intentionally NOT invalidation points;
    the clean source has them WITHOUT invalidate_agent_cache yet still passes —
    so the check never silently promotes them into scope."""
    source = _agents_source()
    tree = ast.parse(source)
    out_of_scope = {"grant_board_access", "revoke_board_access", "delete_agent"}
    present = {
        n.name
        for n in ast.walk(tree)
        if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef)) and n.name in out_of_scope
    }
    # they exist and do NOT invalidate — yet the teeth check stays green.
    assert present == out_of_scope
    for n in ast.walk(tree):
        if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef)) and n.name in out_of_scope:
            assert not _calls_invalidate(n), n.name
    assert _functions_missing_cache_invalidation(source) == []


# ===========================================================================
# ts_8e14d173 — relational_boundary_gate blocks a new direct relational consumer
# ===========================================================================

_ROGUE = (
    "from fastapi import Depends\n"
    "from sqlalchemy.ext.asyncio import AsyncSession\n"
    "from okto_pulse.core.infra.database import get_db\n"
    "from okto_pulse.core.mcp.server import get_db_for_mcp\n"
    "\n\n"
    "async def rogue_relational_read(db: AsyncSession = Depends(get_db)) -> object:\n"
    "    async with get_db_for_mcp() as other:\n"
    "        return other\n"
)

_CLEAN_PORT = (
    "class CleanPortUseCase:\n"
    "    async def execute(self, command, *, actor, uow) -> object:\n"
    "        return uow\n"
)


def _use_cases_root(tmp_path) -> Path:
    root = tmp_path / "use_cases"
    root.mkdir()
    (root / "__init__.py").write_text("", encoding="utf-8")
    return root


def test_boundary_gate_catches_rogue_relational_consumer(tmp_path) -> None:
    root = _use_cases_root(tmp_path)
    (root / "rogue_relational_consumer.py").write_text(_ROGUE, encoding="utf-8")

    report = run_relational_boundary_gate(root=str(root))

    assert not report.ok
    rogue_violations = [
        v for v in report.violations if "rogue_relational_consumer.py" in v.file
    ]
    assert rogue_violations, report.violations
    # the diagnostic carries file + a real line number.
    assert all(v.line > 0 for v in rogue_violations)


def test_boundary_gate_passes_clean_port_use_case(tmp_path) -> None:
    root = _use_cases_root(tmp_path)
    (root / "clean_port_consumer.py").write_text(_CLEAN_PORT, encoding="utf-8")

    report = run_relational_boundary_gate(root=str(root))

    assert report.ok, [(_v.file, _v.line, _v.symbol) for _v in report.violations]


def test_real_use_cases_root_is_relationally_clean() -> None:
    """The clean variant on the exact root the scenario names: the real
    use_cases tree carries no direct relational coupling."""
    report = run_relational_boundary_gate(root=default_use_cases_path())
    assert report.ok, [(v.file, v.line, v.symbol) for v in report.violations]
