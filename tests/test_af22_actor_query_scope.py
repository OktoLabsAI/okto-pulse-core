"""AF22 — ActorScope/QueryScope board-scoped query contract."""

from __future__ import annotations

import ast
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.db import Board
from okto_pulse.core.services import BoardService


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


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
        "okto_pulse.core.api",
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
    for forbidden in (
        "Board.owner_id == user_id",
        "board.owner_id == user_id",
        "board.owner_id != user_id",
        "query = query.where(Board.owner_id",
    ):
        assert forbidden not in service_source

    expected_service_signatures = (
        "async def get_board(",
        "async def list_boards(",
        "async def update_board(",
        "async def create_card(",
        "async def create_spec(",
        "async def create_ideation(",
        "async def create_refinement(",
        "async def create_sprint(",
        "async def convert_stories(",
        "async def derive_spec(",
    )
    for signature in expected_service_signatures:
        start = service_source.find(signature)
        assert start != -1, signature
        end = service_source.find(") ->", start)
        assert "query_scope" in service_source[start:end], signature

    for relative_path in (
        "application/use_cases/boards_crud.py",
        "application/use_cases/ideations_crud.py",
        "application/use_cases/refinements_crud.py",
        "application/use_cases/spec_crud.py",
        "application/use_cases/sprints_crud.py",
        "application/use_cases/stories_crud.py",
    ):
        source = (CORE_ROOT / relative_path).read_text(encoding="utf-8")
        assert "ActorScope.from_context(actor).query_scope" in source, relative_path
