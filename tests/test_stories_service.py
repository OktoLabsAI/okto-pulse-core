from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import func, select

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, ConsolidationQueue, Ideation, Story, StoryStatus
from okto_pulse.core.models.schemas import (
    ScreenMockup,
    StoryConversionRequest,
    StoryCreate,
    StoryMove,
    TopicCreate,
)
from okto_pulse.core.services.analytics_service import compute_funnel
from okto_pulse.core.services.main import StoryService
from okto_pulse.core.services.traceability import build_lineage_graph


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _stub_ctx(board_id: str, actor_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": actor_id,
            "agent_name": "stories-mcp-agent",
            "board_id": board_id,
            "permissions": ["board:read", "specs:create", "specs:update"],
        },
    )()


async def _seed_board(db_factory, board_id: str, owner_id: str) -> None:
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Stories board", owner_id=owner_id))
        await db.commit()


async def _call_mcp(db_factory, tool_name: str, **kwargs) -> dict:
    mcp_server.register_session_factory(db_factory)
    tool = await mcp_server.mcp.get_tool(tool_name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest.mark.asyncio
async def test_story_conversion_propagates_mockups_to_ideation_and_lineage(db_factory):
    board_id = _id("story-board")
    actor_id = _id("agent")
    await _seed_board(db_factory, board_id, actor_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(
            board_id,
            actor_id,
            TopicCreate(name="Account onboarding", description="Entry point"),
        )
        assert topic is not None

        story = await service.create_story(
            board_id,
            actor_id,
            StoryCreate(
                topic_id=topic.id,
                title="Invite teammates",
                description="As an account owner, I want to invite teammates so we can share work.",
                actor="Account owner",
                goal="invite teammates",
                benefit="share work",
                labels=["onboarding", "collaboration"],
                screen_mockups=[
                    ScreenMockup(
                        id="story-mockup-1",
                        title="Invite form",
                        screen_type="modal",
                        html_content="<div>Invite form</div>",
                        order=0,
                    )
                ],
            ),
        )
        assert story is not None

        with pytest.raises(ValueError):
            await service.convert_stories(
                board_id,
                actor_id,
                StoryConversionRequest(story_ids=[story.id], title="Premature conversion"),
            )

        moved_story = await service.move_story(story.id, actor_id, StoryMove(status=StoryStatus.READY))
        assert moved_story is not None and moved_story.status == StoryStatus.READY

        result = await service.convert_stories(
            board_id,
            actor_id,
            StoryConversionRequest(
                story_ids=[story.id],
                title="Team invitation flow",
                mockup_ids=["story-mockup-1"],
            ),
        )
        assert result is not None
        ideation, links, propagated = result
        ideation_id = ideation.id
        story_id = story.id
        await db.commit()

    async with db_factory() as db:
        converted_story = (await db.execute(select(Story).where(Story.id == story_id))).scalar_one()
        created_ideation = (await db.execute(select(Ideation).where(Ideation.id == ideation_id))).scalar_one()

        assert converted_story.status == StoryStatus.CONVERTED
        assert len(links) == 1
        assert propagated == 1
        assert created_ideation.problem_statement is not None
        assert "Invite teammates" in created_ideation.problem_statement
        assert created_ideation.screen_mockups is not None
        assert created_ideation.screen_mockups[0]["origin_id"] == "story-mockup-1"
        assert created_ideation.screen_mockups[0]["origin_story_id"] == story_id

        funnel = await compute_funnel(db, board_id)
        assert funnel["stories"] == 1
        assert funnel["stories_converted"] == 1
        assert funnel["story_conversion_pct"] == 100.0
        assert funnel["story_ideation_links"] == 1
        assert funnel["stories_by_topic"] == [
            {"topic_id": converted_story.topic_id, "topic": "Account onboarding", "stories": 1}
        ]

        graph = await build_lineage_graph(
            db,
            board_id,
            entity_type="ideation",
            entity_id=ideation_id,
            include_artifacts=False,
        )
        assert any(node["entity_type"] == "story" and node["entity_id"] == story_id for node in graph["nodes"])
        assert any(
            edge["source"] == f"story:{story_id}"
            and edge["target"] == f"ideation:{ideation_id}"
            and edge["relationship"] == "feeds_ideation"
            for edge in graph["edges"]
        )


@pytest.mark.asyncio
async def test_story_conversion_does_not_enqueue_story_kg_nodes(db_factory):
    board_id = _id("story-board")
    actor_id = _id("agent")
    await _seed_board(db_factory, board_id, actor_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(board_id, actor_id, TopicCreate(name="Pre ideation"))
        assert topic is not None
        story = await service.create_story(
            board_id,
            actor_id,
            StoryCreate(
                topic_id=topic.id,
                title="Capture story context",
                description="As a product user, I want lightweight intake before ideation.",
            ),
        )
        assert story is not None
        await service.move_story(story.id, actor_id, StoryMove(status=StoryStatus.READY))
        await service.convert_stories(
            board_id,
            actor_id,
            StoryConversionRequest(story_ids=[story.id], title="Story intake ideation"),
        )
        await db.commit()

        queue_count = await db.scalar(
            select(func.count())
            .select_from(ConsolidationQueue)
            .where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.artifact_type == "story",
            )
        )
        assert queue_count == 0


@pytest.mark.asyncio
async def test_story_lifecycle_filters_and_archive(db_factory):
    board_id = _id("story-board")
    actor_id = _id("agent")
    await _seed_board(db_factory, board_id, actor_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(board_id, actor_id, TopicCreate(name="Billing"))
        assert topic is not None
        story = await service.create_story(
            board_id,
            actor_id,
            StoryCreate(
                topic_id=topic.id,
                title="Download invoice",
                description="As a finance user, I need invoices for accounting.",
            ),
        )
        assert story is not None

        moved = await service.move_story(story.id, actor_id, StoryMove(status=StoryStatus.TRIAGE))
        assert moved is not None
        moved = await service.move_story(story.id, actor_id, StoryMove(status=StoryStatus.READY))
        assert moved is not None
        assert moved.status == StoryStatus.READY

        with pytest.raises(ValueError):
            await service.move_story(story.id, actor_id, StoryMove(status=StoryStatus.CONVERTED))

        with pytest.raises(ValueError):
            await service.move_story(story.id, actor_id, StoryMove(status=StoryStatus.DRAFT))

        ready = await service.list_stories(board_id, status_filter="ready")
        assert [item.id for item in ready] == [story.id]

        topics = await service.list_topics(board_id)
        assert len(topics) == 1
        assert getattr(topics[0], "story_count") == 1

        archived = await service.archive_story(story.id, actor_id, archived=True)
        assert archived is not None and archived.archived is True

        visible = await service.list_stories(board_id)
        all_stories = await service.list_stories(board_id, include_archived=True)
        assert visible == []
        assert [item.id for item in all_stories] == [story.id]
        await db.commit()


@pytest.mark.asyncio
async def test_topic_name_is_unique_per_board_only(db_factory):
    owner_id = _id("agent")
    board_a = _id("story-board-a")
    board_b = _id("story-board-b")
    await _seed_board(db_factory, board_a, owner_id)
    await _seed_board(db_factory, board_b, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        assert await service.create_topic(board_a, owner_id, TopicCreate(name="Checkout")) is not None

        with pytest.raises(ValueError):
            await service.create_topic(board_a, owner_id, TopicCreate(name="checkout"))

        assert await service.create_topic(board_b, owner_id, TopicCreate(name="Checkout")) is not None
        await db.commit()


@pytest.mark.asyncio
async def test_story_rest_contract_and_mcp_tools_keep_existing_data_unbackfilled(db_factory):
    board_id = _id("story-board")
    actor_id = _id("agent")
    await _seed_board(db_factory, board_id, actor_id)

    async with db_factory() as db:
        db.add(
            Ideation(
                board_id=board_id,
                title="Existing ideation before Stories rollout",
                created_by=actor_id,
            )
        )
        await db.commit()

        funnel = await compute_funnel(db, board_id)
        assert funnel["ideations"] == 1
        assert funnel["stories"] == 0
        assert funnel["story_conversion_pct"] == 0.0

    from okto_pulse.core.api.router import api_router

    paths = {getattr(route, "path", "") for route in api_router.routes}
    assert any(path.endswith("/boards/{board_id}/stories/convert") for path in paths)
    assert any(path.endswith("/boards/{board_id}/stories/convert-to-ideation") for path in paths)
    assert any(path.endswith("/ideations/{ideation_id}/stories") for path in paths)

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id, actor_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        topic_payload = await _call_mcp(
            db_factory,
            "okto_pulse_create_topic",
            board_id=board_id,
            name="MCP intake",
            description="Created through MCP",
        )
        assert topic_payload["success"] is True
        topic_id = topic_payload["topic"]["id"]

        story_payload = await _call_mcp(
            db_factory,
            "okto_pulse_create_story",
            board_id=board_id,
            topic_id=topic_id,
            title="MCP parity Story",
            description="As an agent, I need Stories exposed through MCP.",
            labels='["mcp", "rest"]',
            status="draft",
        )
        assert story_payload["success"] is True
        story_id = story_payload["story"]["id"]
        assert story_payload["story"]["labels"] == ["mcp", "rest"]

        premature = await _call_mcp(
            db_factory,
            "okto_pulse_convert_stories_to_ideation",
            board_id=board_id,
            story_ids=[story_id],
            title="Should be rejected",
        )
        assert "Only ready Stories" in premature["error"]

        moved = await _call_mcp(
            db_factory,
            "okto_pulse_move_story",
            board_id=board_id,
            story_id=story_id,
            status="triage",
        )
        assert moved["story"]["status"] == "triage"
        moved = await _call_mcp(
            db_factory,
            "okto_pulse_move_story",
            board_id=board_id,
            story_id=story_id,
            status="ready",
        )
        assert moved["story"]["status"] == "ready"

        converted = await _call_mcp(
            db_factory,
            "okto_pulse_convert_stories_to_ideation",
            board_id=board_id,
            story_ids=json.dumps([story_id]),
            title="Converted through MCP",
        )
        assert converted["success"] is True
        assert converted["links"][0]["story_id"] == story_id

        listed = await _call_mcp(
            db_factory,
            "okto_pulse_list_stories",
            board_id=board_id,
            converted="true",
            include_archived="true",
        )
        assert listed["count"] == 1
        assert listed["stories"][0]["status"] == "converted"
