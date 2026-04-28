"""TC-1 (TS1) — pytest MCP add+list+get card_knowledge round-trip.

Exercises the 5 new MCP handlers via the FastMCP tool registry:
- okto_pulse_add_card_knowledge
- okto_pulse_list_card_knowledge
- okto_pulse_get_card_knowledge
- okto_pulse_update_card_knowledge
- okto_pulse_delete_card_knowledge

Each handler is fetched through `mcp.get_tool(name).fn` to bypass the
FastMCP / xml_safety decorator stack and invoke the underlying coroutine
directly. Auth is stubbed; the DB session is the real test session
factory exposed by conftest.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Spec, SpecStatus, Card, CardStatus, CardType


BOARD_ID = "card-kb-board-001"
USER_ID = "card-kb-agent-001"


def _stub_ctx():
    return type("Ctx", (), {
        "agent_id": USER_ID,
        "agent_name": "card-kb-agent",
        "permissions": ["card.entity.update"],
    })()


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
        db.add(Spec(
            id=spec_id, board_id=BOARD_ID, title="Card KB Spec",
            status=SpecStatus.APPROVED, created_by=USER_ID,
            functional_requirements=["FR1"], acceptance_criteria=["AC1"],
            test_scenarios=[], business_rules=[], api_contracts=[],
        ))
        db.add(Card(
            id=card_id, board_id=BOARD_ID, spec_id=spec_id,
            title="Card for KB tests", status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL, created_by=USER_ID,
        ))
        await db.commit()
    return spec_id, card_id


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory
    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest.fixture(autouse=True)
def _stub_auth():
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        yield


@pytest.mark.asyncio
async def test_add_then_list_returns_the_kb(_seed_card):
    spec_id, card_id = _seed_card
    add = await _call(
        "okto_pulse_add_card_knowledge",
        board_id=BOARD_ID, card_id=card_id,
        title="Auth design", content="Use JWT with rotating secrets.",
    )
    assert add.get("success") is True, add
    kb_id = add["knowledge"]["id"]

    listed = await _call("okto_pulse_list_card_knowledge", board_id=BOARD_ID, card_id=card_id)
    assert listed.get("success") is True
    titles = [k["title"] for k in listed["knowledge"]]
    assert "Auth design" in titles
    ids = [k["id"] for k in listed["knowledge"]]
    assert kb_id in ids


@pytest.mark.asyncio
async def test_get_returns_full_content(_seed_card):
    spec_id, card_id = _seed_card
    add = await _call(
        "okto_pulse_add_card_knowledge",
        board_id=BOARD_ID, card_id=card_id,
        title="Doc", content="Body here", description="brief",
    )
    kb_id = add["knowledge"]["id"]

    got = await _call(
        "okto_pulse_get_card_knowledge",
        board_id=BOARD_ID, card_id=card_id, knowledge_id=kb_id,
    )
    assert got.get("success") is True
    assert got["knowledge"]["title"] == "Doc"
    assert got["knowledge"]["content"] == "Body here"
    assert got["knowledge"]["description"] == "brief"


@pytest.mark.asyncio
async def test_update_changes_only_provided_fields(_seed_card):
    spec_id, card_id = _seed_card
    add = await _call(
        "okto_pulse_add_card_knowledge",
        board_id=BOARD_ID, card_id=card_id,
        title="orig", content="original-body",
    )
    kb_id = add["knowledge"]["id"]

    upd = await _call(
        "okto_pulse_update_card_knowledge",
        board_id=BOARD_ID, card_id=card_id, knowledge_id=kb_id,
        title="renamed",
    )
    assert upd.get("success") is True
    assert upd["knowledge"]["title"] == "renamed"
    assert upd["knowledge"]["content"] == "original-body"  # unchanged


@pytest.mark.asyncio
async def test_delete_removes_only_target(_seed_card):
    spec_id, card_id = _seed_card
    a = await _call("okto_pulse_add_card_knowledge", board_id=BOARD_ID, card_id=card_id, title="A", content="aa")
    b = await _call("okto_pulse_add_card_knowledge", board_id=BOARD_ID, card_id=card_id, title="B", content="bb")
    a_id, b_id = a["knowledge"]["id"], b["knowledge"]["id"]

    rem = await _call(
        "okto_pulse_delete_card_knowledge",
        board_id=BOARD_ID, card_id=card_id, knowledge_id=a_id,
    )
    assert rem.get("success") is True

    listed = await _call("okto_pulse_list_card_knowledge", board_id=BOARD_ID, card_id=card_id)
    ids = [k["id"] for k in listed["knowledge"]]
    assert a_id not in ids and b_id in ids


@pytest.mark.asyncio
async def test_add_rejects_empty_title_or_content(_seed_card):
    spec_id, card_id = _seed_card
    err1 = await _call("okto_pulse_add_card_knowledge", board_id=BOARD_ID, card_id=card_id, title="", content="x")
    assert "error" in err1
    err2 = await _call("okto_pulse_add_card_knowledge", board_id=BOARD_ID, card_id=card_id, title="x", content="")
    assert "error" in err2


@pytest.mark.asyncio
async def test_get_404_when_kb_absent(_seed_card):
    spec_id, card_id = _seed_card
    got = await _call(
        "okto_pulse_get_card_knowledge",
        board_id=BOARD_ID, card_id=card_id, knowledge_id="kb_does_not_exist",
    )
    assert "error" in got
