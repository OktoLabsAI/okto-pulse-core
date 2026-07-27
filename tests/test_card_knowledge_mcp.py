"""Card Knowledge MCP handlers.

Exercises the 5 new MCP handlers via the FastMCP tool registry:
- okto_pulse_list_knowledge (entity_type="card")
- okto_pulse_get_card_knowledge
- okto_pulse_add_card_knowledge
- okto_pulse_update_card_knowledge
- okto_pulse_delete_card_knowledge

Card Knowledge is now a read-only governed snapshot: direct add/update/delete
return a deterministic `card_resource_read_only` error while list/get remain
available for copied card context.

Each handler is fetched through `mcp.get_tool(name).fn` to bypass the
FastMCP / xml_safety decorator stack and invoke the underlying coroutine
directly. Auth is stubbed; the DB session is the real test session
factory exposed by conftest.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Spec, SpecStatus, Card, CardStatus, CardType


BOARD_ID = "card-kb-board-001"
USER_ID = "card-kb-agent-001"


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "card-kb-agent",
            "permissions": ["card.entity.update"],
        },
    )()


@pytest.fixture
async def _seed_card():
    """Create a board+spec+card baseline and return their IDs."""
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()
    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())
    async with db_factory() as db:
        if await db.get(Board, BOARD_ID) is None:
            db.add(Board(id=BOARD_ID, name="Card KB MCP", owner_id=USER_ID))
            await db.flush()
        db.add(
            Spec(
                id=spec_id,
                board_id=BOARD_ID,
                title="Card KB Spec",
                status=SpecStatus.APPROVED,
                created_by=USER_ID,
                functional_requirements=["FR1"],
                acceptance_criteria=["AC1"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=BOARD_ID,
                spec_id=spec_id,
                title="Card for KB tests",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
                knowledge_bases=[
                    {
                        "id": "cardkb_existing",
                        "title": "Copied KB",
                        "description": "brief",
                        "content": "Body here",
                        "mime_type": "text/markdown",
                        "source": f"copied_from_spec:{spec_id}:kb_source",
                        "source_kb_id": "kb_source",
                        "author_id": USER_ID,
                    }
                ],
            )
        )
        await db.commit()
    return spec_id, card_id


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest.fixture(autouse=True)
def _stub_auth():
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        yield


@pytest.mark.asyncio
async def test_list_returns_copied_card_kb(_seed_card):
    spec_id, card_id = _seed_card
    listed = await _call(
        "okto_pulse_list_knowledge",
        board_id=BOARD_ID,
        entity_type="card",
        entity_id=card_id,
    )
    assert listed.get("entity_type") == "card"
    titles = [k["title"] for k in listed["knowledge_bases"]]
    assert "Copied KB" in titles
    ids = [k["id"] for k in listed["knowledge_bases"]]
    assert "cardkb_existing" in ids


@pytest.mark.asyncio
async def test_get_returns_full_content(_seed_card):
    spec_id, card_id = _seed_card
    listed = await _call(
        "okto_pulse_list_knowledge",
        board_id=BOARD_ID,
        entity_type="card",
        entity_id=card_id,
    )
    got = await _call(
        "okto_pulse_get_card_knowledge",
        board_id=BOARD_ID,
        card_id=card_id,
        knowledge_id="cardkb_existing",
    )
    summary = next(
        item for item in listed["knowledge_bases"] if item["id"] == "cardkb_existing"
    )
    assert got.get("success") is True
    assert got["knowledge"]["title"] == "Copied KB"
    assert got["knowledge"]["content"] == "Body here"
    assert got["knowledge"]["description"] == "brief"
    assert set(got["knowledge"]) == set(summary) | {"content"}
    assert got["knowledge"]["content_hash"] == summary["content_hash"]


@pytest.mark.asyncio
async def test_direct_add_update_delete_are_read_only(_seed_card):
    spec_id, card_id = _seed_card
    add = await _call(
        "okto_pulse_add_card_knowledge",
        board_id=BOARD_ID,
        card_id=card_id,
        title="Direct",
        content="blocked",
    )
    assert add.get("error") == "card_resource_read_only"

    upd = await _call(
        "okto_pulse_update_card_knowledge",
        board_id=BOARD_ID,
        card_id=card_id,
        knowledge_id="cardkb_existing",
        title="renamed",
    )
    assert upd.get("error") == "card_resource_read_only"

    rem = await _call(
        "okto_pulse_delete_card_knowledge",
        board_id=BOARD_ID,
        card_id=card_id,
        knowledge_id="cardkb_existing",
    )
    assert rem.get("error") == "card_resource_read_only"

    listed = await _call(
        "okto_pulse_list_knowledge",
        board_id=BOARD_ID,
        entity_type="card",
        entity_id=card_id,
    )
    ids = [k["id"] for k in listed["knowledge_bases"]]
    assert "cardkb_existing" in ids


@pytest.mark.asyncio
async def test_get_404_when_kb_absent(_seed_card):
    spec_id, card_id = _seed_card
    got = await _call(
        "okto_pulse_get_card_knowledge",
        board_id=BOARD_ID,
        card_id=card_id,
        knowledge_id="kb_does_not_exist",
    )
    assert "error" in got
