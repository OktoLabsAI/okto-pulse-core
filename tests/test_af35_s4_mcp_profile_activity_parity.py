"""AF35-S4 MCP parity tests for profile, mentions and activity wrappers."""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.infra.permissions import Permissions
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ActivityLog,
    Agent,
    AgentBoard,
    Board,
    Card,
    Comment,
)
from okto_pulse.core.services import AgentService


AGENT_NAME = "AF35Bot"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _ctx(
    board_id: str,
    *,
    agent_id: str = "af35-agent",
    permissions: list[str] | None = None,
) -> SimpleNamespace:
    default_permissions = [*Permissions.DEFAULT, "board.activity_read"]
    return SimpleNamespace(
        agent_id=agent_id,
        agent_name=AGENT_NAME,
        board_id=board_id,
        permissions=permissions if permissions is not None else default_permissions,
    )


def _agent_stub(
    *,
    agent_id: str = "af35-agent",
    permissions: list[str] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=agent_id,
        name=AGENT_NAME,
        permissions=permissions if permissions is not None else Permissions.DEFAULT,
    )


async def _seed_profile_activity_fixture(db_factory) -> dict[str, str]:
    board_id = _id("board-af35-s4")
    agent_id = _id("agent-af35-s4")
    peer_agent_id = _id("agent-peer")
    card_id = _id("card-af35-s4")
    comment_id = _id("comment-af35-s4")
    activity_id = _id("activity-af35-s4")
    now = datetime.now(timezone.utc)

    key_hash = AgentService.hash_api_key(_id("api-key"))
    peer_key_hash = AgentService.hash_api_key(_id("api-key-peer"))
    async with db_factory() as db:
        db.add(Board(id=board_id, name="AF35 S4 Board", description="Board", owner_id="owner"))
        db.add(
            Agent(
                id=agent_id,
                name=AGENT_NAME,
                description="old description",
                objective="old objective",
                api_key=AgentService.credential_marker(key_hash),
                api_key_hash=key_hash,
                permissions=Permissions.DEFAULT,
                created_by="owner",
                created_at=now,
            )
        )
        db.add(
            Agent(
                id=peer_agent_id,
                name="Peer",
                description="peer",
                objective="peer objective",
                api_key=AgentService.credential_marker(peer_key_hash),
                api_key_hash=peer_key_hash,
                permissions=[Permissions.BOARD_READ],
                created_by="owner",
                created_at=now,
            )
        )
        db.add(AgentBoard(agent_id=agent_id, board_id=board_id, granted_by="owner"))
        db.add(AgentBoard(agent_id=peer_agent_id, board_id=board_id, granted_by="owner"))
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                title="Mention target",
                description="seed",
                created_by="owner",
                created_at=now,
            )
        )
        db.add(
            Comment(
                id=comment_id,
                card_id=card_id,
                content=f"Please review @{AGENT_NAME}",
                author_id="owner",
                created_at=now,
            )
        )
        db.add(
            ActivityLog(
                id=activity_id,
                board_id=board_id,
                card_id=card_id,
                action="card_updated",
                actor_type="agent",
                actor_id=agent_id,
                actor_name=AGENT_NAME,
                details={"title": "Mention target", "trigger": "test"},
                created_at=now,
            )
        )
        await db.commit()

    return {
        "board_id": board_id,
        "agent_id": agent_id,
        "peer_agent_id": peer_agent_id,
        "card_id": card_id,
        "comment_id": comment_id,
        "activity_id": activity_id,
    }


@pytest.mark.asyncio
async def test_profile_activity_success_envelopes_are_compatible(db_factory) -> None:
    ids = await _seed_profile_activity_fixture(db_factory)
    board_id = ids["board_id"]
    agent_id = ids["agent_id"]
    register_mcp_test_runtime(db_factory)

    with (
        patch.object(
            mcp_server,
            "_get_authenticated_agent",
            AsyncMock(return_value=_agent_stub(agent_id=agent_id)),
        ),
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=_ctx(board_id, agent_id=agent_id)),
        ),
    ):
        updated = json.loads(
            await mcp_server.okto_pulse_update_my_profile.fn(
                description="new description",
                objective="new objective",
            )
        )
        assert updated == {
            "success": True,
            "profile": {
                "id": agent_id,
                "name": AGENT_NAME,
                "description": "new description",
                "objective": "new objective",
            },
        }

        cleared = json.loads(
            await mcp_server.okto_pulse_update_my_profile.fn(
                description="",
            )
        )
        assert cleared["profile"]["description"] == ""
        assert cleared["profile"]["objective"] == "new objective"

        boards = json.loads(await mcp_server.okto_pulse_list_my_boards.fn())
        assert boards["agent_id"] == agent_id
        assert boards["agent_name"] == AGENT_NAME
        assert boards["boards"] == [
            {"id": board_id, "name": "AF35 S4 Board", "description": "Board"}
        ]

        mentions = json.loads(
            await mcp_server.okto_pulse_list_my_mentions.fn(board_id=board_id)
        )
        assert mentions["agent_name"] == AGENT_NAME
        assert mentions["filter"] == "unseen_only"
        assert mentions["unseen_count"] == 1
        assert mentions["mentions"][0]["type"] == "comment"
        assert mentions["mentions"][0]["item_id"] == ids["comment_id"]

        ignored = json.loads(
            await mcp_server.okto_pulse_mark_as_seen.fn(
                board_id=board_id,
                item_ids=["not-a-real-mention"],
            )
        )
        assert ignored == {
            "success": True,
            "marked_count": 0,
            "total_requested": 1,
        }
        still_unseen = json.loads(
            await mcp_server.okto_pulse_list_my_mentions.fn(board_id=board_id)
        )
        assert still_unseen["unseen_count"] == 1
        assert still_unseen["mentions"][0]["item_id"] == ids["comment_id"]

        marked = json.loads(
            await mcp_server.okto_pulse_mark_as_seen.fn(
                board_id=board_id,
                item_ids=[ids["comment_id"]],
            )
        )
        assert marked == {"success": True, "marked_count": 1, "total_requested": 1}

        unseen_after_mark = json.loads(
            await mcp_server.okto_pulse_list_my_mentions.fn(board_id=board_id)
        )
        assert unseen_after_mark["unseen_count"] == 0
        assert unseen_after_mark["mentions"] == []

        all_mentions = json.loads(
            await mcp_server.okto_pulse_list_my_mentions.fn(
                board_id=board_id,
                include_seen=True,
            )
        )
        assert all_mentions["unseen_count"] == 0
        assert all_mentions["mentions"][0]["item_id"] == ids["comment_id"]
        assert all_mentions["mentions"][0]["seen"] is True

        summary = json.loads(
            await mcp_server.okto_pulse_get_unseen_summary.fn(board_id=board_id)
        )
        assert summary["board_id"] == board_id
        assert summary["total_mentions"] == 1
        assert summary["seen_count"] == 1
        assert summary["unseen_mentions"] == 0
        assert summary["recent_activity_24h"] >= 1

        agents = json.loads(await mcp_server.okto_pulse_list_agents.fn(board_id=board_id))
        agent_ids = {row["id"] for row in agents}
        assert {agent_id, ids["peer_agent_id"]}.issubset(agent_ids)

        activity = json.loads(
            await mcp_server.okto_pulse_get_activity_log.fn(
                board_id=board_id,
                envelope=True,
                include_details=True,
            )
        )
        assert set(activity) == {"items", "next_cursor"}
        seeded_activity = next(
            item for item in activity["items"] if item["id"] == ids["activity_id"]
        )
        assert seeded_activity["details"]["title"] == "Mention target"


@pytest.mark.asyncio
async def test_update_my_profile_missing_agent_envelope_is_preserved(db_factory) -> None:
    register_mcp_test_runtime(db_factory)
    with patch.object(
        mcp_server,
        "_get_authenticated_agent",
        AsyncMock(return_value=_agent_stub(agent_id=_id("missing-agent"))),
    ):
        parsed = json.loads(
            await mcp_server.okto_pulse_update_my_profile.fn(
                description="new description",
            )
        )

    assert parsed == {"error": "Agent not found"}


@pytest.mark.asyncio
async def test_auth_and_permission_denial_envelopes_are_preserved(db_factory) -> None:
    register_mcp_test_runtime(db_factory)
    with patch.object(
        mcp_server, "_get_authenticated_agent", AsyncMock(return_value=None)
    ):
        assert json.loads(await mcp_server.okto_pulse_update_my_profile.fn()) == {
            "error": "Authentication failed"
        }
        assert json.loads(await mcp_server.okto_pulse_list_my_boards.fn()) == {
            "error": "Authentication failed"
        }

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=None)):
        cross_board = json.loads(
            await mcp_server.okto_pulse_list_my_mentions.fn(board_id="other-board")
        )
        assert cross_board == {"error": "Authentication failed or board access denied"}

        activity = json.loads(
            await mcp_server.okto_pulse_get_activity_log.fn(board_id="other-board")
        )
        assert activity == {"error": "Authentication failed or board access denied"}

    with patch.object(
        mcp_server,
        "_get_authenticated_agent",
        AsyncMock(
            return_value=_agent_stub(
                agent_id="agent-no-self-update",
                permissions=[Permissions.BOARD_READ],
            )
        ),
    ):
        denied = json.loads(await mcp_server.okto_pulse_update_my_profile.fn())
        assert denied == {"error": "Permission denied: requires 'profile.update'"}

    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=_ctx("board-no-read", permissions=[])),
    ):
            denied_agents = json.loads(
                await mcp_server.okto_pulse_list_agents.fn(board_id="board-no-read")
            )
            denial_detail = json.loads(denied_agents["error"])
            assert denial_detail["required_permission"] == "agent.entity.read"


@pytest.mark.asyncio
async def test_mark_as_seen_validation_envelopes_are_preserved(db_factory) -> None:
    register_mcp_test_runtime(db_factory)
    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=_ctx("board-validation")),
    ):
        empty = json.loads(
            await mcp_server.okto_pulse_mark_as_seen.fn(
                board_id="board-validation",
                item_ids="",
            )
        )
        assert empty == {"error": "No item_ids provided"}

        comma_only = json.loads(
            await mcp_server.okto_pulse_mark_as_seen.fn(
                board_id="board-validation",
                item_ids="a,b",
            )
        )
        assert comma_only["error"].startswith("Invalid item_ids:")
        assert "comma-separated input is rejected" in comma_only["error"]
