from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from okto_pulse.core.api import stories as stories_api
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    ActivityLog,
    Board,
    ConsolidationQueue,
    Ideation,
    IdeationStatus,
    Story,
    StoryIdeationLink,
    StoryStatus,
)
from okto_pulse.core.models.schemas import (
    ScreenMockup,
    StoryLinkCreate,
    StoryConversionRequest,
    StoryCreate,
    StoryMove,
    TopicCreate,
    TopicMergeRequest,
    TopicUpdate,
)
from okto_pulse.core.services.analytics_service import compute_funnel
from okto_pulse.core.services.main import InvalidTopicMergeError, StoryService, TopicNotEmptyError
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
            "permissions": [
                "board:read",
                "specs:create",
                "specs:update",
                "specs:delete",
                "topic.entity.read",
                "topic.entity.create",
                "topic.entity.edit_fields",
                "topic.entity.archive",
                "topic.entity.restore",
                "topic.entity.delete",
                "topic.entity.merge",
                "story.entity.read",
                "story.entity.create",
                "story.entity.edit_fields",
                "story.entity.label",
                "story.entity.archive",
                "story.entity.restore",
                "story.move.draft_to_triage",
                "story.move.triage_to_ready",
                "story.links.ideation",
                "story.conversion.to_ideation",
            ],
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
async def test_lineage_graph_allows_unlinked_story_root(db_factory):
    board_id = _id("story-board")
    actor_id = _id("agent")
    await _seed_board(db_factory, board_id, actor_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(board_id, actor_id, TopicCreate(name="Standalone intake"))
        assert topic is not None
        story = await service.create_story(
            board_id,
            actor_id,
            StoryCreate(
                topic_id=topic.id,
                title="Capture standalone story",
                description="As a product user, I want to register intake before ideation exists.",
            ),
        )
        assert story is not None
        story_id = story.id
        await db.commit()

    async with db_factory() as db:
        graph = await build_lineage_graph(
            db,
            board_id,
            entity_type="story",
            entity_id=story_id,
            include_artifacts=False,
        )

    assert graph["root_entity"] == {
        "type": "story",
        "id": story_id,
        "title": "Capture standalone story",
        "status": "draft",
    }
    assert graph["root_ideation"]["entity_type"] == "story"
    assert graph["resolution_path"] == [{"type": "story", "id": story_id}]
    assert graph["nodes"] == [
        {
            "id": f"story:{story_id}",
            "entity_type": "story",
            "entity_id": story_id,
            "title": "Capture standalone story",
            "label": "Capture standalone story",
            "status": "draft",
            "stage": -1,
            "summary": {"topic_id": topic.id, "mockups_count": 0},
        }
    ]
    assert graph["edges"] == []
    assert graph["summary"]["stories"] == 1
    assert graph["summary"]["ideations"] == 0
    assert graph["summary"]["nodes"] == 1
    assert graph["summary"]["edges"] == 0
    assert graph["warnings"] == [
        "Selected story is not linked to an ideation yet, so the lineage "
        "graph is rooted at the story."
    ]


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
async def test_topic_delete_blocks_active_and_archived_stories(db_factory):
    owner_id = _id("agent")
    board_id = _id("story-board")
    await _seed_board(db_factory, board_id, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(board_id, owner_id, TopicCreate(name="Resource Gate"))
        empty_topic = await service.create_topic(board_id, owner_id, TopicCreate(name="Empty Topic"))
        assert topic is not None and empty_topic is not None

        active_story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=topic.id,
                title="Active story",
                description="As a user, I want active tracking so impact is visible.",
            ),
        )
        archived_story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=topic.id,
                title="Archived story",
                description="As a user, I want archived tracking so delete remains safe.",
            ),
        )
        assert active_story is not None and archived_story is not None
        await service.archive_story(archived_story.id, owner_id, archived=True)

        with pytest.raises(TopicNotEmptyError) as exc:
            await service.delete_topic(topic.id, owner_id)
        assert exc.value.code == "topic_not_empty"
        assert exc.value.details["active_count"] == 1
        assert exc.value.details["archived_count"] == 1
        assert exc.value.details["suggested_actions"] == ["merge", "move_stories", "archive"]

        deleted = await service.delete_topic(empty_topic.id, owner_id)
        assert deleted is not None
        assert await db.get(type(empty_topic), empty_topic.id) is None
        activity = (await db.execute(
            select(ActivityLog).where(ActivityLog.board_id == board_id, ActivityLog.action == "topic_deleted")
        )).scalar_one()
        assert activity.details["topic_id"] == empty_topic.id
        await db.commit()


@pytest.mark.asyncio
async def test_topic_merge_moves_stories_preserves_links_and_archives_source(db_factory):
    owner_id = _id("agent")
    board_id = _id("story-board")
    await _seed_board(db_factory, board_id, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        source = await service.create_topic(board_id, owner_id, TopicCreate(name="Incoming"))
        target = await service.create_topic(board_id, owner_id, TopicCreate(name="Resource Gate"))
        assert source is not None and target is not None

        story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=source.id,
                title="Link preserving story",
                description="As a reviewer, I want lineage links to survive a topic merge.",
            ),
        )
        archived_story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=source.id,
                title="Archived source story",
                description="As a maintainer, I want archived stories moved too.",
            ),
        )
        assert story is not None and archived_story is not None
        ideation = Ideation(board_id=board_id, title="Topic merge ideation", created_by=owner_id)
        db.add(ideation)
        await db.flush()
        await service.link_story_to_ideation(story.id, ideation.id, owner_id)
        await service.archive_story(archived_story.id, owner_id, archived=True)

        result = await service.merge_topics(source.id, target.id, owner_id)
        assert result is not None
        assert result["moved_count"] == 2
        assert result["active_count"] == 1
        assert result["archived_count"] == 1
        assert result["source"].archived is True
        assert result["target"].archived is False
        assert getattr(result["target"], "total_associated_count") == 2

        moved_stories = list((await db.execute(
            select(Story).where(Story.id.in_([story.id, archived_story.id]))
        )).scalars().all())
        assert {item.topic_id for item in moved_stories} == {target.id}
        links = list((await db.execute(
            select(StoryIdeationLink).where(StoryIdeationLink.story_id == story.id)
        )).scalars().all())
        assert [link.ideation_id for link in links] == [ideation.id]
        activity = (await db.execute(
            select(ActivityLog).where(ActivityLog.board_id == board_id, ActivityLog.action == "topic_merged")
        )).scalar_one()
        assert activity.details["source_topic_id"] == source.id
        assert activity.details["target_topic_id"] == target.id
        assert activity.details["moved_count"] == 2
        await db.commit()


@pytest.mark.asyncio
async def test_topic_lifecycle_uses_semantic_activity_and_active_name_uniqueness(db_factory):
    owner_id = _id("agent")
    board_id = _id("story-board")
    await _seed_board(db_factory, board_id, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(board_id, owner_id, TopicCreate(name="Resource Gate"))
        assert topic is not None
        archived = await service.update_topic(topic.id, owner_id, TopicUpdate(archived=True))
        assert archived is not None and archived.archived is True

        replacement = await service.create_topic(board_id, owner_id, TopicCreate(name="Resource Gate"))
        assert replacement is not None
        assert replacement.id != topic.id
        assert topic.name.startswith("Resource Gate [archived ")

        with pytest.raises(ValueError):
            await service.update_topic(topic.id, owner_id, TopicUpdate(name="Resource Gate", archived=False))

        restored = await service.update_topic(topic.id, owner_id, TopicUpdate(archived=False))
        assert restored is not None and restored.archived is False

        actions = list((await db.execute(
            select(ActivityLog.action).where(ActivityLog.board_id == board_id)
        )).scalars().all())
        assert "topic_archived" in actions
        assert "topic_restored" in actions
        await db.commit()


@pytest.mark.asyncio
async def test_topic_merge_rejects_self_merge_and_archived_target(db_factory):
    owner_id = _id("agent")
    board_id = _id("story-board")
    await _seed_board(db_factory, board_id, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        source = await service.create_topic(board_id, owner_id, TopicCreate(name="Source"))
        target = await service.create_topic(board_id, owner_id, TopicCreate(name="Target"))
        assert source is not None and target is not None

        with pytest.raises(InvalidTopicMergeError):
            await service.merge_topics(source.id, source.id, owner_id)

        await service.update_topic(target.id, owner_id, TopicUpdate(archived=True))
        with pytest.raises(InvalidTopicMergeError):
            await service.merge_topics(source.id, target.id, owner_id)
        await db.commit()


@pytest.mark.asyncio
async def test_topic_rest_endpoints_return_contextual_delete_and_merge_payloads(db_factory):
    owner_id = _id("agent")
    board_id = _id("story-board")
    await _seed_board(db_factory, board_id, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        source = await service.create_topic(board_id, owner_id, TopicCreate(name="Source API"))
        target = await service.create_topic(board_id, owner_id, TopicCreate(name="Target API"))
        assert source is not None and target is not None
        story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=source.id,
                title="REST topic story",
                description="As a maintainer, I need REST contracts for Topic operations.",
            ),
        )
        assert story is not None
        source_id = source.id
        target_id = target.id
        await db.commit()

        with pytest.raises(HTTPException) as blocked:
            await stories_api.delete_topic(source_id, user_id=owner_id, db=db)
        assert blocked.value.status_code == 409
        assert blocked.value.detail["code"] == "topic_not_empty"
        assert blocked.value.detail["active_count"] == 1
        assert blocked.value.detail["suggested_actions"] == ["merge", "move_stories", "archive"]
        await db.rollback()

        merged = await stories_api.merge_topics(
            source_id,
            TopicMergeRequest(target_topic_id=target_id),
            user_id=owner_id,
            db=db,
        )
        assert merged["success"] is True
        assert merged["moved_count"] == 1
        assert merged["source"].archived is True
        assert getattr(merged["target"], "total_associated_count") == 1

        deleted = await stories_api.delete_topic(source_id, user_id=owner_id, db=db)
        assert deleted.success is True
        assert deleted.deleted_topic_id == source_id


@pytest.mark.asyncio
async def test_topic_rest_and_mcp_tools_enforce_granular_permissions(db_factory):
    owner_id = _id("agent")
    board_id = _id("topic-board")
    await _seed_board(db_factory, board_id, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(board_id, owner_id, TopicCreate(name="Permission source"))
        empty = await service.create_topic(board_id, owner_id, TopicCreate(name="Permission empty"))
        target = await service.create_topic(board_id, owner_id, TopicCreate(name="Permission target"))
        assert topic is not None and empty is not None and target is not None
        permission_story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=topic.id,
                title="Permission story",
                description="As an agent, I need Story MCP tools to enforce granular permissions.",
            ),
        )
        assert permission_story is not None
        permission_story_id = permission_story.id
        await db.commit()

        seen_permissions: list[str | list[str | None] | None] = []

        async def capture_permission(*args, **kwargs):
            seen_permissions.append(args[3])

        with patch.object(stories_api, "_require_permissions", side_effect=capture_permission):
            await stories_api.create_topic(board_id, TopicCreate(name="Permission created"), user_id=owner_id, db=db)
            await stories_api.list_topics(board_id, user_id=owner_id, db=db)
            await stories_api.update_topic(topic.id, TopicUpdate(name="Permission renamed"), user_id=owner_id, db=db)
            await stories_api.update_topic(topic.id, TopicUpdate(archived=True), user_id=owner_id, db=db)
            await stories_api.update_topic(topic.id, TopicUpdate(archived=False), user_id=owner_id, db=db)
            await stories_api.delete_topic(empty.id, user_id=owner_id, db=db)
            await stories_api.merge_topics(
                topic.id,
                TopicMergeRequest(target_topic_id=target.id),
                user_id=owner_id,
                db=db,
            )

        flattened = [
            permission
            for entry in seen_permissions
            for permission in (entry if isinstance(entry, list) else [entry])
            if permission
        ]
        assert "topic.entity.create" in flattened
        assert "topic.entity.read" in flattened
        assert "topic.entity.edit_fields" in flattened
        assert "topic.entity.archive" in flattened
        assert "topic.entity.restore" in flattened
        assert "topic.entity.delete" in flattened
        assert "topic.entity.merge" in flattened

    denied_ctx = _stub_ctx(board_id, owner_id)
    denied_ctx.permissions = ["board:read"]
    mcp_cases = [
        ("okto_pulse_update_topic", {"topic_id": "topic", "name": "x"}, "topic.entity.edit_fields"),
        ("okto_pulse_archive_topic", {"topic_id": "topic"}, "topic.entity.archive"),
        ("okto_pulse_restore_topic", {"topic_id": "topic"}, "topic.entity.restore"),
        ("okto_pulse_delete_topic", {"topic_id": "topic"}, "topic.entity.delete"),
        ("okto_pulse_merge_topics", {"source_topic_id": "source", "target_topic_id": "target"}, "topic.entity.merge"),
        ("okto_pulse_update_story", {"story_id": permission_story_id, "title": "x"}, "story.entity.edit_fields"),
        ("okto_pulse_archive_story", {"story_id": permission_story_id}, "story.entity.archive"),
        ("okto_pulse_restore_story", {"story_id": permission_story_id}, "story.entity.restore"),
    ]
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=denied_ctx)):
        for tool_name, kwargs, required_permission in mcp_cases:
            payload = await _call_mcp(db_factory, tool_name, board_id=board_id, **kwargs)
            assert required_permission in payload["error"]


@pytest.mark.asyncio
async def test_story_links_require_editable_ideations_and_reject_duplicates(db_factory):
    owner_id = _id("agent")
    board_id = _id("story-board")
    other_board_id = _id("other-board")
    await _seed_board(db_factory, board_id, owner_id)
    await _seed_board(db_factory, other_board_id, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(board_id, owner_id, TopicCreate(name="Operational parity"))
        assert topic is not None
        story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=topic.id,
                title="Link Story through editable Ideation selector",
                description="As a product lead, I want Story links to target editable ideations.",
            ),
        )
        assert story is not None

        editable = Ideation(
            board_id=board_id,
            title="Editable target",
            status=IdeationStatus.REVIEW,
            created_by=owner_id,
        )
        done = Ideation(
            board_id=board_id,
            title="Closed target",
            status=IdeationStatus.DONE,
            created_by=owner_id,
        )
        cancelled = Ideation(
            board_id=board_id,
            title="Cancelled target",
            status=IdeationStatus.CANCELLED,
            created_by=owner_id,
        )
        other_board = Ideation(
            board_id=other_board_id,
            title="Other board target",
            created_by=owner_id,
        )
        db.add_all([editable, done, cancelled, other_board])
        await db.flush()

        link = await service.link_story_to_ideation(story.id, editable.id, owner_id)
        assert link is not None
        assert link.ideation_id == editable.id
        converted_story = await service.get_story(story.id)
        assert converted_story is not None
        assert converted_story.status == StoryStatus.CONVERTED

        second_story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=topic.id,
                title="Second Story converging to the same Ideation",
                description="As a product lead, I want multiple Stories to feed one Ideation.",
                status=StoryStatus.READY,
            ),
        )
        assert second_story is not None
        second_link = await service.link_story_to_ideation(second_story.id, editable.id, owner_id)
        assert second_link is not None
        assert second_link.ideation_id == editable.id
        converted_second_story = await service.get_story(second_story.id)
        assert converted_second_story is not None
        assert converted_second_story.status == StoryStatus.CONVERTED

        another_editable = Ideation(
            board_id=board_id,
            title="Another editable target",
            status=IdeationStatus.REVIEW,
            created_by=owner_id,
        )
        db.add(another_editable)
        await db.flush()

        with pytest.raises(ValueError, match="already linked to another Ideation"):
            await service.link_story_to_ideation(story.id, another_editable.id, owner_id)

        with pytest.raises(ValueError, match="already linked"):
            await service.link_story_to_ideation(story.id, editable.id, owner_id)

        with pytest.raises(ValueError, match="editable Ideations"):
            await service.link_story_to_ideation(story.id, done.id, owner_id)

        with pytest.raises(ValueError, match="editable Ideations"):
            await service.link_story_to_ideation(story.id, cancelled.id, owner_id)

        assert await service.link_story_to_ideation(story.id, other_board.id, owner_id) is None

        with pytest.raises(HTTPException) as duplicate_http:
            await stories_api.link_story_to_ideation(
                story.id,
                StoryLinkCreate(ideation_id=editable.id),
                user_id=owner_id,
                db=db,
            )
        assert duplicate_http.value.status_code == 400
        assert "already linked" in duplicate_http.value.detail

        await db.commit()


@pytest.mark.asyncio
async def test_story_ideation_link_table_enforces_one_ideation_per_story(db_factory):
    board_id = _id("story-board")
    owner_id = _id("owner")
    await _seed_board(db_factory, board_id, owner_id)

    async with db_factory() as db:
        service = StoryService(db)
        topic = await service.create_topic(board_id, owner_id, TopicCreate(name="Single link constraint"))
        assert topic is not None
        story = await service.create_story(
            board_id,
            owner_id,
            StoryCreate(
                topic_id=topic.id,
                title="One Story one Ideation",
                description="As a maintainer, I need the database to enforce Story link uniqueness.",
                status=StoryStatus.READY,
            ),
        )
        first = Ideation(board_id=board_id, title="First target", status=IdeationStatus.REVIEW, created_by=owner_id)
        second = Ideation(board_id=board_id, title="Second target", status=IdeationStatus.REVIEW, created_by=owner_id)
        db.add_all([first, second])
        await db.flush()
        assert story is not None

        db.add(
            StoryIdeationLink(
                board_id=board_id,
                story_id=story.id,
                ideation_id=first.id,
                created_by=owner_id,
            )
        )
        await db.flush()
        db.add(
            StoryIdeationLink(
                board_id=board_id,
                story_id=story.id,
                ideation_id=second.id,
                created_by=owner_id,
            )
        )

        with pytest.raises(IntegrityError):
            await db.flush()
        await db.rollback()


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

        updated_story = await _call_mcp(
            db_factory,
            "okto_pulse_update_story",
            board_id=board_id,
            story_id=story_id,
            title="MCP parity Story updated",
            labels='["mcp", "updated"]',
        )
        assert updated_story["success"] is True
        assert updated_story["story"]["title"] == "MCP parity Story updated"
        assert updated_story["story"]["labels"] == ["mcp", "updated"]

        archived_story = await _call_mcp(
            db_factory,
            "okto_pulse_archive_story",
            board_id=board_id,
            story_id=story_id,
        )
        assert archived_story["success"] is True
        assert archived_story["story"]["archived"] is True

        listed_archived_story = await _call_mcp(
            db_factory,
            "okto_pulse_list_by_board",
            board_id=board_id,
            entity_type="story",
            filters={"include_archived": "true"},
        )
        assert any(item["id"] == story_id and item["archived"] is True for item in listed_archived_story["items"])

        restored_story = await _call_mcp(
            db_factory,
            "okto_pulse_restore_story",
            board_id=board_id,
            story_id=story_id,
        )
        assert restored_story["success"] is True
        assert restored_story["story"]["archived"] is False

        added_mockup = await _call_mcp(
            db_factory,
            "okto_pulse_add_screen_mockup",
            board_id=board_id,
            entity_id=story_id,
            entity_type="story",
            title="Story modal parity",
            screen_type="modal",
            html_content="<div onclick='bad()'><script>bad()</script>Story modal</div>",
        )
        assert added_mockup["success"] is True
        assert added_mockup["entity_type"] == "story"
        assert "<script>" not in added_mockup["screen"]["html_content"]
        screen_id = added_mockup["screen"]["id"]

        updated_mockup = await _call_mcp(
            db_factory,
            "okto_pulse_update_screen_mockup",
            board_id=board_id,
            entity_id=story_id,
            entity_type="story",
            screen_id=screen_id,
            title="Story modal parity updated",
        )
        assert updated_mockup["success"] is True
        assert updated_mockup["screen"]["title"] == "Story modal parity updated"

        annotation = await _call_mcp(
            db_factory,
            "okto_pulse_annotate_mockup",
            board_id=board_id,
            entity_id=story_id,
            entity_type="story",
            screen_id=screen_id,
            text="Header actions match other SDLC modals.",
        )
        assert annotation["success"] is True

        listed_mockups = await _call_mcp(
            db_factory,
            "okto_pulse_list_screen_mockups",
            board_id=board_id,
            entity_id=story_id,
            entity_type="story",
        )
        assert listed_mockups["entity_type"] == "story"
        assert listed_mockups["total"] == 1
        assert listed_mockups["screens"][0]["annotations"][0]["text"] == "Header actions match other SDLC modals."

        deleted_mockup = await _call_mcp(
            db_factory,
            "okto_pulse_delete_screen_mockup",
            board_id=board_id,
            entity_id=story_id,
            entity_type="story",
            screen_id=screen_id,
        )
        assert deleted_mockup["success"] is True

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
            "okto_pulse_list_by_board",
            board_id=board_id,
            entity_type="story",
            filters={"converted": "true", "include_archived": "true"},
        )
        assert listed["total"] == 1
        assert listed["items"][0]["status"] == "converted"

        link_topic = await _call_mcp(
            db_factory,
            "okto_pulse_create_topic",
            board_id=board_id,
            name="MCP link topic",
            description="Topic used for link parity",
        )
        assert link_topic["success"] is True
        link_story = await _call_mcp(
            db_factory,
            "okto_pulse_create_story",
            board_id=board_id,
            topic_id=link_topic["topic"]["id"],
            title="MCP link Story",
            description="As an agent, I need link semantics to mark converted.",
            status="ready",
        )
        assert link_story["success"] is True
        async with db_factory() as db:
            target = Ideation(
                board_id=board_id,
                title="MCP link target",
                status=IdeationStatus.REVIEW,
                created_by=actor_id,
            )
            db.add(target)
            await db.flush()
            target_id = target.id
            await db.commit()

        linked_story = await _call_mcp(
            db_factory,
            "okto_pulse_link_story_to_ideation",
            board_id=board_id,
            story_id=link_story["story"]["id"],
            ideation_id=target_id,
            mark_converted="false",
        )
        assert linked_story["success"] is True
        assert linked_story["story"]["status"] == "converted"
        duplicate_link = await _call_mcp(
            db_factory,
            "okto_pulse_link_story_to_ideation",
            board_id=board_id,
            story_id=link_story["story"]["id"],
            ideation_id=target_id,
        )
        assert "already linked" in duplicate_link["error"]

        target_topic = await _call_mcp(
            db_factory,
            "okto_pulse_create_topic",
            board_id=board_id,
            name="MCP merge target",
            description="Target before merge",
        )
        assert target_topic["success"] is True
        target_topic_id = target_topic["topic"]["id"]

        updated_topic = await _call_mcp(
            db_factory,
            "okto_pulse_update_topic",
            board_id=board_id,
            topic_id=target_topic_id,
            description="Updated through MCP",
        )
        assert updated_topic["success"] is True
        assert updated_topic["topic"]["description"] == "Updated through MCP"

        archived_topic = await _call_mcp(
            db_factory,
            "okto_pulse_archive_topic",
            board_id=board_id,
            topic_id=target_topic_id,
        )
        assert archived_topic["success"] is True
        assert archived_topic["topic"]["archived"] is True

        restored_topic = await _call_mcp(
            db_factory,
            "okto_pulse_restore_topic",
            board_id=board_id,
            topic_id=target_topic_id,
        )
        assert restored_topic["success"] is True
        assert restored_topic["topic"]["archived"] is False

        blocked_delete = await _call_mcp(
            db_factory,
            "okto_pulse_delete_topic",
            board_id=board_id,
            topic_id=topic_id,
        )
        assert blocked_delete["success"] is False
        assert blocked_delete["code"] == "topic_not_empty"
        assert blocked_delete["active_count"] == 1

        merged = await _call_mcp(
            db_factory,
            "okto_pulse_merge_topics",
            board_id=board_id,
            source_topic_id=topic_id,
            target_topic_id=target_topic_id,
        )
        assert merged["success"] is True
        assert merged["impact"]["moved_count"] == 1
        assert merged["source"]["archived"] is True

        deleted_source = await _call_mcp(
            db_factory,
            "okto_pulse_delete_topic",
            board_id=board_id,
            topic_id=topic_id,
        )
        assert deleted_source["success"] is True
