"""R01A MCP-FU6 (family: card) — strangler oracle.

Proves the MCP card tools were migrated off ``get_db_for_mcp`` / direct
``CardService`` onto the MCP UnitOfWork + ``mcp_card_crud`` use cases WITHOUT
behavior drift, per Codex's option-A-with-adapter-envelope decision:

- AST: every migrated card tool delegates to its ``Mcp*UseCase`` (or the reused
  ``AddCardDependencyUseCase``) and no longer opens ``get_db_for_mcp`` nor builds
  ``CardService`` directly.
- board-scope: a cross-board id returns the legacy ``{"error": "Card not found"}``.
- atomic activity log: ``create_card``/``update_card``/``delete_card`` each
  write exactly one correctly attributed ``ActivityLog`` row in the SAME
  transaction.
- MCP envelopes: ``remove_card_dependency`` preserves the non-enumerating
  ``{"success": bool}`` contract, while ``add_card_dependency`` emits typed
  conflicts.
- ``create_card`` keeps the bidirectional ``test_scenarios.linked_task_ids``
  backlink.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import event, inspect, select
from sqlalchemy.orm import Session

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.domain.enums import CardPriority, CardStatus
from okto_pulse.core.models.schemas import CardResponse
from sqlalchemy_test_models import ActivityLog, Board, Card, Spec, SpecStatus

BOARD_A = "r01a-mcpcard-a"
BOARD_B = "r01a-mcpcard-b"
USER_ID = "r01a-mcpcard-agent"

_TOOL_USE_CASE = {
    "create_card": "McpCreateCardUseCase",
    "get_card": "McpGetCardUseCase",
    "update_card": "McpUpdateCardUseCase",
    "move_card": "McpMoveCardUseCase",
    "delete_card": "McpDeleteCardUseCase",
    "add_card_dependency": "AddCardDependencyUseCase",
    "remove_card_dependency": "McpRemoveCardDependencyUseCase",
    "get_card_dependencies": "McpGetCardDependenciesUseCase",
    "copy_knowledge_to_card": "McpCopyKnowledgeToCardUseCase",
}


# --- AST proof (no DB) ------------------------------------------------------


def test_card_tools_strangled_and_delegate():
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    seen = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.AsyncFunctionDef):
            short = n.name.replace("okto_pulse_", "")
            if short not in _TOOL_USE_CASE:
                continue
            seen.add(short)
            block = ast.get_source_segment(src, n) or ""
            assert "async with get_db_for_mcp" not in block, (
                f"{short} still opens get_db_for_mcp"
            )
            assert "CardService(" not in block, (
                f"{short} still builds CardService directly"
            )
            assert _TOOL_USE_CASE[short] in block, (
                f"{short} must delegate to {_TOOL_USE_CASE[short]}"
            )
    assert seen == set(_TOOL_USE_CASE), f"missing tools: {set(_TOOL_USE_CASE) - seen}"


# --- runtime harness --------------------------------------------------------


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": "mcp-card-test", "permissions": ["*"]},
    )()


@pytest.fixture(autouse=True)
def _auth():
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())
    ), patch.object(mcp_server, "check_permission", return_value=None):
        yield


@pytest.fixture
async def _seed():
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        for bid in (BOARD_A, BOARD_B):
            if await db.get(Board, bid) is None:
                db.add(Board(id=bid, name=f"MCP Card {bid}", owner_id=USER_ID))
        await db.flush()
        spec_id = str(uuid.uuid4())
        scen_id = "scen-" + uuid.uuid4().hex[:8]
        db.add(
            Spec(
                id=spec_id,
                board_id=BOARD_A,
                title="Spec A",
                status=SpecStatus.APPROVED,
                created_by=USER_ID,
                functional_requirements=["FR1"],
                acceptance_criteria=["AC1"],
                test_scenarios=[
                    {"id": scen_id, "title": "S", "type": "happy", "linked_task_ids": []}
                ],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.commit()
    return spec_id, scen_id


async def _call(tool_name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(tool_name)
    return json.loads(await tool.fn(**kwargs))


async def _create_card(spec_id: str, **over) -> dict:
    kwargs = {"board_id": BOARD_A, "title": "C", "spec_id": spec_id}
    kwargs.update(over)
    return await _call("okto_pulse_create_card", **kwargs)


async def _activity_rows(card_id: str, action: str) -> list:
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        res = await db.execute(
            select(ActivityLog).where(
                ActivityLog.card_id == card_id, ActivityLog.action == action
            )
        )
        return list(res.scalars().all())


# --- board-scope (cross-board → legacy "Card not found") --------------------


@pytest.mark.asyncio
async def test_cross_board_returns_card_not_found(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    for tool, extra in (
        ("okto_pulse_get_card", {}),
        ("okto_pulse_update_card", {"title": "x"}),
        ("okto_pulse_move_card", {"status": "in_progress"}),
        ("okto_pulse_delete_card", {}),
    ):
        payload = await _call(tool, board_id=BOARD_B, card_id=card_id, **extra)
        assert payload.get("error") == "Card not found", (tool, payload)


def test_card_response_projects_policy_version_as_subject_version() -> None:
    now = datetime.now(timezone.utc)
    response = CardResponse.model_validate(
        {
            "id": "card-1",
            "board_id": BOARD_A,
            "title": "Versioned card",
            "description": None,
            "details": None,
            "status": CardStatus.NOT_STARTED,
            "policy_version": 7,
            "priority": CardPriority.NONE,
            "position": 0,
            "assignee_id": None,
            "created_by": USER_ID,
            "created_at": now,
            "updated_at": now,
            "due_date": None,
            "labels": None,
        }
    )

    payload = response.model_dump()
    assert payload["subject_version"] == 7
    assert "policy_version" not in payload


@pytest.mark.asyncio
async def test_move_card_refreshes_inside_transaction_before_commit() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.mcp_card_crud import (
        McpMoveCardCommand,
        McpMoveCardUseCase,
    )

    card = SimpleNamespace(
        id="card-transaction-order",
        board_id=BOARD_A,
        status=CardStatus.NOT_STARTED,
        position=0,
        policy_version=3,
    )

    class Cards:
        async def get_card(self, _card_id: str):
            return card

        async def move_card(self, *_args, **_kwargs):
            card.status = CardStatus.CANCELLED
            return card

    class UnitOfWork:
        services = SimpleNamespace(cards=Cards())

        def __init__(self, *, fail_reload: bool = False) -> None:
            self.events: list[str] = []
            self.fail_reload = fail_reload

        async def synchronize(self) -> None:
            self.events.append("synchronize")

        async def reload(self, entity: object, *, fields: tuple[str, ...] = ()) -> None:
            assert fields == ("status", "position", "policy_version")
            self.events.append("reload")
            if self.fail_reload:
                raise RuntimeError("refresh failed")
            entity.policy_version = 4

        async def commit(self) -> None:
            self.events.append("commit")

    actor = ActorContext(
        USER_ID,
        "mcp",
        board_id=BOARD_A,
        permissions=["cards:move"],
    )
    successful = UnitOfWork()
    result = await McpMoveCardUseCase().execute(
        McpMoveCardCommand(
            card.id,
            BOARD_A,
            SimpleNamespace(status=CardStatus.CANCELLED),
        ),
        actor=actor,
        uow=successful,
    )
    assert successful.events == ["synchronize", "reload", "commit"]
    assert result.card.policy_version == 4

    failing = UnitOfWork(fail_reload=True)
    with pytest.raises(RuntimeError, match="refresh failed"):
        await McpMoveCardUseCase().execute(
            McpMoveCardCommand(
                card.id,
                BOARD_A,
                SimpleNamespace(status=CardStatus.CANCELLED),
            ),
            actor=actor,
            uow=failing,
        )
    assert failing.events == ["synchronize", "reload"]


@pytest.mark.asyncio
async def test_card_reads_expose_same_subject_version(_seed) -> None:
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]

    card_payload = await _call(
        "okto_pulse_get_card",
        board_id=BOARD_A,
        card_id=card_id,
    )
    context_payload = await _call(
        "okto_pulse_get_task_context",
        board_id=BOARD_A,
        card_id=card_id,
        profile="full",
    )

    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        persisted = await db.get(Card, card_id)

    assert card_payload["subject_version"] == persisted.policy_version
    assert context_payload["card"]["subject_version"] == persisted.policy_version


@pytest.mark.asyncio
async def test_move_card_returns_committed_subject_version(_seed) -> None:
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]

    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        before = (await db.get(Card, card_id)).policy_version

    # Mirror the production adapter's before_flush subject-version bump; the
    # lightweight Core test adapter intentionally has no global listener.
    def bump_policy_version_on_flush(session, _flush_context, _instances) -> None:
        for entity in tuple(session.dirty):
            if (
                isinstance(entity, Card)
                and inspect(entity).attrs.status.history.has_changes()
            ):
                entity.policy_version += 1

    event.listen(Session, "before_flush", bump_policy_version_on_flush)
    try:
        payload = await _call(
            "okto_pulse_move_card",
            board_id=BOARD_A,
            card_id=card_id,
            status="cancelled",
            cancellation_reason="No longer required",
        )
    finally:
        event.remove(Session, "before_flush", bump_policy_version_on_flush)

    async with get_session_factory()() as db:
        persisted = await db.get(Card, card_id)

    assert payload["success"] is True
    assert payload["card"]["status"] == persisted.status.value == "cancelled"
    assert payload["card"]["subject_version"] == persisted.policy_version
    assert payload["card"]["subject_version"] == before + 1


# --- atomic activity log ----------------------------------------------------


def _assert_mcp_actor(row: ActivityLog) -> None:
    assert row.actor_type == "agent"
    assert row.actor_id == USER_ID
    assert row.actor_name == "mcp-card-test"


@pytest.mark.asyncio
async def test_create_card_writes_one_agent_activity_with_complete_details(_seed):
    spec_id, _ = _seed
    payload = await _create_card(spec_id, title="Created", priority="high")
    card_id = payload["card"]["id"]

    rows = await _activity_rows(card_id, "card_created")

    assert len(rows) == 1
    row = rows[0]
    _assert_mcp_actor(row)
    assert row.details["title"] == "Created"
    assert row.details["status"] == "not_started"
    assert row.details["priority"] == "high"
    assert row.details["traceability"] == {
        "requested": [],
        "changed": [],
        "idempotent": True,
    }


@pytest.mark.asyncio
async def test_update_card_writes_atomic_card_updated_activity(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    payload = await _call(
        "okto_pulse_update_card", board_id=BOARD_A, card_id=card_id, title="Updated"
    )
    assert payload.get("success") is True, payload
    rows = await _activity_rows(card_id, "card_updated")
    assert len(rows) == 1
    row = rows[0]
    _assert_mcp_actor(row)
    assert row.details["title"] == "Updated"
    assert row.details["changes"] == [
        {"field": "title", "old": "C", "new": "Updated"}
    ]


@pytest.mark.asyncio
async def test_delete_card_writes_atomic_card_deleted_activity(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    payload = await _call("okto_pulse_delete_card", board_id=BOARD_A, card_id=card_id)
    assert payload.get("success") is True, payload
    rows = await _activity_rows(card_id, "card_deleted")
    assert len(rows) == 1
    row = rows[0]
    _assert_mcp_actor(row)
    assert row.details == {"title": "C"}


# --- typed MCP transition envelope -----------------------------------------


@pytest.mark.asyncio
async def test_invalid_card_edge_is_not_misreported_as_dependency_block(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]

    payload = await _call(
        "okto_pulse_move_card",
        board_id=BOARD_A,
        card_id=card_id,
        status="in_progress",
    )

    assert payload["error"] == "card_transition_not_allowed"
    assert payload["blocked_by_dependencies"] is False
    assert payload["remediation"] == "move_card_to_started_first"


# --- dependency MCP envelopes -----------------------------------------------


@pytest.mark.asyncio
async def test_remove_card_dependency_missing_edge_is_non_enumerating(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    payload = await _call(
        "okto_pulse_remove_card_dependency",
        board_id=BOARD_A,
        card_id=card_id,
        depends_on_id="does-not-exist",
    )
    assert payload == {"success": False}, payload


@pytest.mark.asyncio
async def test_add_card_dependency_self_ref_returns_typed_conflict(_seed):
    spec_id, _ = _seed
    card_id = (await _create_card(spec_id))["card"]["id"]
    payload = await _call(
        "okto_pulse_add_card_dependency",
        board_id=BOARD_A,
        card_id=card_id,
        depends_on_id=card_id,
    )
    assert payload["error"] == "dependency_self_reference", payload
    assert payload["code"] == "dependency_self_reference"
    assert payload["facts"] == {
        "card_id": card_id,
        "depends_on_id": card_id,
    }


# --- create_card scenario backlink ------------------------------------------


@pytest.mark.asyncio
async def test_create_card_backlinks_scenario(_seed):
    spec_id, scen_id = _seed
    created = await _create_card(
        spec_id, title="Test card", card_type="test", test_scenario_ids=[scen_id]
    )
    card_id = created["card"]["id"]

    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        scen = next(s for s in spec.test_scenarios if s["id"] == scen_id)
    assert card_id in (scen.get("linked_task_ids") or []), (
        "create_card must bidirectionally backlink the test scenario"
    )
