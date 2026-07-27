"""QueryScope fail-closed inventory gate (HND-4).

The gate is deliberately static and narrow: any non-owner ``QueryScope`` call
must either pass an explicit ``allowed_board_ids`` value (empty list means no
access) or opt into the explicit all-board sentinel. Owner-only calls keep the
legacy ``allowed_board_ids=None`` contract because the service layer adds the
``Board.owner_id`` predicate.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class QueryScopePolicyViolation:
    file: str
    line: int
    call: str
    reason: str

    def as_dict(self) -> dict[str, str | int]:
        return {
            "file": self.file,
            "line": self.line,
            "call": self.call,
            "reason": self.reason,
        }


def _callee_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _callee_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return None


def _keyword_value(call: ast.Call, name: str) -> ast.AST | None:
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    return None


def _is_bool_literal(node: ast.AST | None, expected: bool) -> bool:
    return isinstance(node, ast.Constant) and node.value is expected


def _is_none_literal(node: ast.AST | None) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _has_concrete_allowed_boards(call: ast.Call) -> bool:
    value = _keyword_value(call, "allowed_board_ids")
    return value is not None and not _is_none_literal(value)


def query_scope_policy_violations(
    source: str,
    *,
    file: str = "<memory>",
) -> tuple[QueryScopePolicyViolation, ...]:
    tree = ast.parse(source)
    violations: list[QueryScopePolicyViolation] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = _callee_name(node.func)
        if callee != "QueryScope" and not (callee or "").endswith(".query_scope"):
            continue
        non_owner = _is_bool_literal(_keyword_value(node, "require_ownership"), False)
        if not non_owner:
            continue
        if _has_concrete_allowed_boards(node) or _is_bool_literal(
            _keyword_value(node, "allow_all_boards"), True
        ):
            continue
        violations.append(
            QueryScopePolicyViolation(
                file=file,
                line=getattr(node, "lineno", 0),
                call=callee or "<unknown>",
                reason=(
                    "non-owner QueryScope must pass allowed_board_ids or the "
                    "explicit allow_all_boards sentinel"
                ),
            )
        )

    return tuple(violations)


def run_query_scope_policy_gate(root: Path) -> tuple[QueryScopePolicyViolation, ...]:
    source_root = root / "src" / "okto_pulse" / "core"
    if not source_root.exists():
        source_root = root

    violations: list[QueryScopePolicyViolation] = []
    for path in sorted(source_root.rglob("*.py")):
        if any(part in {"__pycache__", ".venv", "build", "dist"} for part in path.parts):
            continue
        relative = path.relative_to(root).as_posix() if path.is_relative_to(root) else path.as_posix()
        violations.extend(
            query_scope_policy_violations(
                path.read_text(encoding="utf-8"),
                file=relative,
            )
        )
    return tuple(violations)


__all__ = [
    "QueryScopePolicyViolation",
    "query_scope_policy_violations",
    "run_query_scope_policy_gate",
]
