"""AF22 — ActorScope/QueryScope board-scoped query contract."""

from __future__ import annotations

import ast
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.application.boundary.query_scope_gate import (
    query_scope_policy_violations,
    run_query_scope_policy_gate,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.infra.database import get_session_factory
from sqlalchemy_test_models import Board
from okto_pulse.core.services import BoardService
from okto_pulse.core.services.main import _board_scope_clauses


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _attr_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Attribute):
        parent = _attr_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _find_async_function(source: str, name: str) -> ast.AsyncFunctionDef | None:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    return None


def _board_scope_guard_violations(
    service_source: str,
    use_case_sources: dict[str, str],
) -> list[str]:
    violations: list[str] = []
    tree = ast.parse(service_source)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        names = {_attr_name(node.left)}
        names.update(_attr_name(comparator) for comparator in node.comparators)
        if {"user_id", "Board.owner_id"} <= names or {"user_id", "board.owner_id"} <= names:
            violations.append("raw_owner_user_compare")

    expected_service_methods = (
        "get_board",
        "list_boards",
        "update_board",
        "create_card",
        "create_spec",
        "create_ideation",
        "create_refinement",
        "create_sprint",
        "convert_stories",
        "derive_spec",
    )
    for method_name in expected_service_methods:
        method = _find_async_function(service_source, method_name)
        if method is None:
            violations.append(f"{method_name}:missing")
            continue
        arg_names = {arg.arg for arg in method.args.args}
        arg_names.update(arg.arg for arg in method.args.kwonlyargs)
        if "query_scope" not in arg_names:
            violations.append(f"{method_name}:missing_query_scope")

    for relative_path, source in use_case_sources.items():
        if "ActorScope.from_context(actor).query_scope" not in source:
            violations.append(f"{relative_path}:missing_actor_query_scope")

    return violations


def test_scope_contract_is_transport_and_persistence_free() -> None:
    source = (CORE_ROOT / "application" / "scope.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)

    forbidden_roots = {
        "fastapi",
        "sqlalchemy",
        "okto_pulse.community",
        "okto_pulse.community.api",
        "okto_pulse.core.infra",
        "okto_pulse.core.models",
        "okto_pulse.core.repositories",
        "okto_pulse.core.services",
    }
    assert not {
        module
        for module in imports
        if any(module == root or module.startswith(f"{root}.") for root in forbidden_roots)
    }


def test_actor_scope_derives_normalized_query_scope() -> None:
    actor = ActorContext(
        "user-af22",
        "rest",
        actor_name="AF22 User",
        board_id="board-af22",
        realm_id="realm-af22",
        permissions={"board.read": True},
        roles=("owner",),
    )

    scope = ActorScope.from_context(actor).query_scope(
        allowed_board_ids=["board-af22", "board-af22"],
    )

    assert scope.actor_id == "user-af22"
    assert scope.source == "rest"
    assert scope.actor_name == "AF22 User"
    assert scope.realm_id == "realm-af22"
    assert scope.target_board_id == "board-af22"
    assert scope.allowed_board_ids == frozenset({"board-af22"})
    assert scope.allows_board_id("board-af22")
    assert not scope.allows_board_id("other-board")


def test_query_scope_default_is_not_all_boards() -> None:
    scope = QueryScope(actor_id="user-hnd4", source="test")

    assert not scope.allows_board_id("board-hnd4")
    assert not scope.allows_board_id(None)


def test_query_scope_empty_list_denies_without_fallback() -> None:
    scope = QueryScope(
        actor_id="user-hnd4",
        source="test",
        allowed_board_ids=frozenset(),
        require_ownership=False,
    )

    assert not scope.allows_board_id("board-hnd4")


def test_query_scope_explicit_global_requires_capability() -> None:
    actor = ActorContext("user-hnd4", "rest")

    with pytest.raises(PermissionError):
        ActorScope.from_context(actor).query_scope(
            require_ownership=False,
            allow_all_boards=True,
        )

    admin_actor = ActorContext(
        "admin-hnd4",
        "rest",
        realm_scope=RealmScope.local(),
        permissions={"board.read.all": True},
    )
    scope = ActorScope.from_context(admin_actor).query_scope(
        require_ownership=False,
        allow_all_boards=True,
    )

    assert scope.allow_all_boards is True
    assert scope.allows_board_id("board-hnd4")


def test_board_scope_clauses_fail_closed_for_non_owner_none() -> None:
    scope = QueryScope(
        actor_id="user-hnd4",
        source="test",
        require_ownership=False,
    )

    assert (
        _board_scope_clauses(
            board_id="board-hnd4",
            user_id="user-hnd4",
            query_scope=scope,
            require_ownership=False,
        )
        is None
    )


def test_board_scope_clauses_preserve_owner_only_none() -> None:
    scope = QueryScope(actor_id="user-hnd4", source="test")

    clauses = _board_scope_clauses(
        board_id="board-hnd4",
        user_id="user-hnd4",
        query_scope=scope,
        require_ownership=True,
    )

    assert clauses is not None
    assert len(clauses) == 2


def test_query_scope_policy_gate_accepts_current_tree() -> None:
    assert run_query_scope_policy_gate(CORE_ROOT.parents[2]) == ()


def test_query_scope_policy_gate_blocks_non_owner_none() -> None:
    source = """
async def bad(actor_scope):
    return actor_scope.query_scope(require_ownership=False)

async def good_owner(actor_scope):
    return actor_scope.query_scope()

async def good_empty(actor_scope):
    return actor_scope.query_scope(allowed_board_ids=[], require_ownership=False)
"""

    violations = query_scope_policy_violations(source, file="synthetic.py")

    assert [violation.line for violation in violations] == [3]
    assert "non-owner QueryScope" in violations[0].reason


@pytest.mark.asyncio
async def test_board_service_query_scope_filters_allowed_boards_and_realm() -> None:
    owner_id = _new_id("owner-af22")
    allowed_board = _new_id("board-af22-allowed")
    forbidden_board = _new_id("board-af22-forbidden")
    other_realm_board = _new_id("board-af22-realm")

    async with get_session_factory()() as db:
        db.add_all(
            [
                Board(
                    id=allowed_board,
                    name="AF22 allowed",
                    owner_id=owner_id,
                    realm_id="realm-a",
                ),
                Board(
                    id=forbidden_board,
                    name="AF22 forbidden",
                    owner_id=owner_id,
                    realm_id="realm-a",
                ),
                Board(
                    id=other_realm_board,
                    name="AF22 other realm",
                    owner_id=owner_id,
                    realm_id="realm-b",
                ),
            ]
        )
        await db.commit()

    scope = QueryScope(
        actor_id=owner_id,
        source="rest",
        realm_id="realm-a",
        allowed_board_ids=frozenset({allowed_board}),
    )

    async with get_session_factory()() as db:
        service = BoardService(db)
        allowed = await service.get_board(
            allowed_board,
            owner_id,
            query_scope=scope.with_target_board(allowed_board),
        )
        forbidden = await service.get_board(
            forbidden_board,
            owner_id,
            query_scope=scope.with_target_board(forbidden_board),
        )
        wrong_realm = await service.get_board(
            other_realm_board,
            owner_id,
            query_scope=QueryScope(
                actor_id=owner_id,
                source="rest",
                realm_id="realm-a",
                allowed_board_ids=frozenset({other_realm_board}),
                target_board_id=other_realm_board,
            ),
        )
        boards, total = await service.list_boards(owner_id, query_scope=scope)

    assert allowed is not None
    assert allowed.id == allowed_board
    assert forbidden is None
    assert wrong_realm is None
    assert total == 1
    assert [board.id for board in boards] == [allowed_board]


def test_board_seed_inventory_uses_query_scope_helpers() -> None:
    service_source = (CORE_ROOT / "services" / "main.py").read_text(encoding="utf-8")
    use_case_sources = {
        relative_path: (CORE_ROOT / relative_path).read_text(encoding="utf-8")
        for relative_path in (
            "application/use_cases/boards_crud.py",
            "application/use_cases/ideations_crud.py",
            "application/use_cases/refinements_crud.py",
            "application/use_cases/spec_crud.py",
            "application/use_cases/sprints_crud.py",
            "application/use_cases/stories_crud.py",
        )
    }

    assert _board_scope_guard_violations(service_source, use_case_sources) == []


def test_board_seed_scope_guard_bites_raw_board_query() -> None:
    bad_service_source = """
async def get_board(board_id, user_id):
    return Board.owner_id == user_id

async def list_boards(user_id, query_scope=None): pass
async def update_board(user_id, query_scope=None): pass
async def create_card(user_id, query_scope=None): pass
async def create_spec(user_id, query_scope=None): pass
async def create_ideation(user_id, query_scope=None): pass
async def create_refinement(user_id, query_scope=None): pass
async def create_sprint(user_id, query_scope=None): pass
async def convert_stories(user_id, query_scope=None): pass
async def derive_spec(user_id, query_scope=None): pass
"""
    bad_use_cases = {
        "application/use_cases/boards_crud.py": "async def execute(actor): pass",
    }

    violations = _board_scope_guard_violations(bad_service_source, bad_use_cases)

    assert "raw_owner_user_compare" in violations
    assert "get_board:missing_query_scope" in violations
    assert (
        "application/use_cases/boards_crud.py:missing_actor_query_scope"
        in violations
    )
