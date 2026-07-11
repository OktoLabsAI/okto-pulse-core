from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Comment,
    Spec,
    SpecQAItem,
    SpecStatus,
)


@pytest.mark.asyncio
async def test_copy_qa_to_card_treats_choice_selection_as_answered(db_factory):
    board_id = f"board-{uuid.uuid4().hex[:8]}"
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    card_id = f"card-{uuid.uuid4().hex[:8]}"
    agent_id = "qa-copy-agent"

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Q&A copy", owner_id=agent_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec",
                status=SpecStatus.DRAFT,
                created_by=agent_id,
                functional_requirements=["FR"],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=agent_id,
            )
        )
        db.add(
            SpecQAItem(
                spec_id=spec_id,
                question="Which implementation path?",
                question_type="choice",
                choices=[
                    {"id": "opt_0", "label": "Use existing adapter"},
                    {"id": "opt_1", "label": "Create new adapter"},
                ],
                selected=["opt_0"],
                answer=None,
                asked_by=agent_id,
                answered_by=agent_id,
            )
        )
        await db.commit()

    ctx = type(
        "Ctx",
        (),
        {"agent_id": agent_id, "agent_name": agent_id, "permissions": None},
    )()
    mcp_server.register_session_factory(db_factory)
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)):
        raw = await mcp_server.okto_pulse_copy_qa_to_card.fn(
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_id,
        )

    payload = json.loads(raw)
    assert payload == {"success": True, "copied": 1}

    async with db_factory() as db:
        comments = (
            await db.execute(select(Comment).where(Comment.card_id == card_id))
        ).scalars().all()

    assert len(comments) == 1
    assert "Which implementation path?" in comments[0].content
    assert "Use existing adapter" in comments[0].content
