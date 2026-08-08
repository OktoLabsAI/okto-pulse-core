"""Two-board containment matrix for MCP mockup/copy/consolidated-list tools."""

from __future__ import annotations

import hashlib
import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import func, select

from mcp_runtime_testing import register_mcp_test_runtime
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
    McpAddScreenMockupUseCase,
    McpAnnotateMockupUseCase,
    McpCopyMockupsToCardCommand,
    McpCopyMockupsToCardUseCase,
    McpCopyQaToCardCommand,
    McpCopyQaToCardUseCase,
    McpDeleteScreenMockupUseCase,
    McpGetCardKnowledgeCommand,
    McpGetCardKnowledgeUseCase,
    McpListKnowledgeCommand,
    McpListKnowledgeUseCase,
    McpListQaCommand,
    McpListQaUseCase,
    McpListScreenMockupsUseCase,
    McpListSnapshotsCommand,
    McpListSnapshotsUseCase,
    McpScreenMockupCommand,
    McpUpdateScreenMockupUseCase,
)
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    Comment,
    Ideation,
    IdeationKnowledgeBase,
    IdeationQAItem,
    IdeationSnapshot,
    IdeationStatus,
    Refinement,
    RefinementKnowledgeBase,
    RefinementQAItem,
    RefinementSnapshot,
    RefinementStatus,
    Spec,
    SpecKnowledgeBase,
    SpecQAItem,
    SpecStatus,
    Story,
    StoryStatus,
    Topic,
)
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory


pytestmark = pytest.mark.asyncio

USER_ID = "mockup-scope-agent"
SECRET = "FOREIGN-BOARD-SECRET"


def _screen(screen_id: str, title: str) -> dict:
    return {
        "id": screen_id,
        "title": title,
        "description": None,
        "screen_type": "page",
        "html_content": f"<main>{title}</main>",
        "annotations": [],
        "order": 0,
        "design_system_ref": None,
        "design_system_evidence": None,
    }


async def test_list_mockups_defaults_to_bounded_summary_and_pages_stably() -> None:
    board_id = "mockup-summary-board"
    large_html = "<main>" + ("á" * 32_768) + "</main>"
    screens = [
        {
            "id": f"screen-{index:03d}",
            "title": f"Screen {index:03d}",
            "screen_type": "page",
            "html_content": large_html,
            "order": index,
        }
        for index in range(200)
    ]
    entity = SimpleNamespace(board_id=board_id, screen_mockups=screens)
    service = SimpleNamespace(get_spec=AsyncMock(return_value=entity))
    uow = SimpleNamespace(services=SimpleNamespace(specs=service))
    actor = ActorContext(USER_ID, "mcp", board_id=board_id)
    use_case = McpListScreenMockupsUseCase()

    summary = (
        await use_case.execute(
            McpScreenMockupCommand(
                board_id=board_id,
                entity_id="spec-summary",
                entity_type="spec",
                limit=200,
            ),
            actor=actor,
            uow=uow,
        )
    ).payload

    assert (summary["total"], summary["offset"], summary["limit"]) == (200, 0, 200)
    assert len(summary["screens"]) == 200
    assert all("html_content" not in screen for screen in summary["screens"])
    first = summary["screens"][0]
    assert first["has_html_content"] is True
    assert first["html_content_bytes"] == len(large_html.encode("utf-8"))
    assert first["html_content_sha256"] == hashlib.sha256(
        large_html.encode("utf-8")
    ).hexdigest()
    assert len(json.dumps(summary)) < 100_000

    page_command = McpScreenMockupCommand(
        board_id=board_id,
        entity_id="spec-summary",
        entity_type="spec",
        offset=17,
        limit=5,
    )
    first_page = (
        await use_case.execute(page_command, actor=actor, uow=uow)
    ).payload
    repeated_page = (
        await use_case.execute(page_command, actor=actor, uow=uow)
    ).payload
    assert first_page == repeated_page
    assert [screen["id"] for screen in first_page["screens"]] == [
        f"screen-{index:03d}" for index in range(17, 22)
    ]
    assert (first_page["total"], first_page["offset"], first_page["limit"]) == (
        200,
        17,
        5,
    )

    full = (
        await use_case.execute(
            McpScreenMockupCommand(
                board_id=board_id,
                entity_id="spec-summary",
                entity_type="spec",
                offset=17,
                limit=1,
                include_content=True,
            ),
            actor=actor,
            uow=uow,
        )
    ).payload
    assert full["screens"][0]["html_content"] == large_html
    assert full["screens"][0]["html_content_sha256"] == first_page["screens"][0][
        "html_content_sha256"
    ]


@pytest.fixture
async def scope_graph(db_factory):
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board_a": f"mock-scope-a-{suffix}",
        "board_b": f"mock-scope-b-{suffix}",
        "topic_a": f"mock-topic-a-{suffix}",
        "topic_b": f"mock-topic-b-{suffix}",
        "story_a": f"mock-story-a-{suffix}",
        "story_b": f"mock-story-b-{suffix}",
        "ideation_a": f"mock-idea-a-{suffix}",
        "ideation_b": f"mock-idea-b-{suffix}",
        "refinement_a": f"mock-refine-a-{suffix}",
        "refinement_b": f"mock-refine-b-{suffix}",
        "spec_a": f"mock-spec-a-{suffix}",
        "spec_b": f"mock-spec-b-{suffix}",
        "card_a": f"mock-card-a-{suffix}",
        "card_b": f"mock-card-b-{suffix}",
        "screen_story_a": f"screen-story-a-{suffix}",
        "screen_story_b": f"screen-story-b-{suffix}",
        "screen_idea_a": f"screen-idea-a-{suffix}",
        "screen_idea_b": f"screen-idea-b-{suffix}",
        "screen_refine_a": f"screen-refine-a-{suffix}",
        "screen_refine_b": f"screen-refine-b-{suffix}",
        "screen_spec_a": f"screen-spec-a-{suffix}",
        "screen_spec_b": f"screen-spec-b-{suffix}",
        "screen_card_b": f"screen-card-b-{suffix}",
        "card_kb_a": f"card-kb-a-{suffix}",
        "card_kb_b": f"card-kb-b-{suffix}",
    }

    async with db_factory() as db:
        db.add_all(
            [
                Board(
                    id=ids["board_a"],
                    name="Mockup scope A",
                    owner_id=USER_ID,
                    settings={"design_system_gate_mode": "off"},
                ),
                Board(
                    id=ids["board_b"],
                    name="Mockup scope B",
                    owner_id=USER_ID,
                    settings={"design_system_gate_mode": "off"},
                ),
            ]
        )
        await db.flush()

        db.add_all(
            [
                Topic(
                    id=ids["topic_a"],
                    board_id=ids["board_a"],
                    name=f"Topic A {suffix}",
                    created_by=USER_ID,
                ),
                Topic(
                    id=ids["topic_b"],
                    board_id=ids["board_b"],
                    name=f"Topic B {suffix}",
                    created_by=USER_ID,
                ),
                Ideation(
                    id=ids["ideation_a"],
                    board_id=ids["board_a"],
                    title="Local ideation",
                    status=IdeationStatus.DRAFT,
                    created_by=USER_ID,
                    screen_mockups=[
                        _screen(ids["screen_idea_a"], "Local ideation screen")
                    ],
                ),
                Ideation(
                    id=ids["ideation_b"],
                    board_id=ids["board_b"],
                    title=f"{SECRET} ideation",
                    status=IdeationStatus.DRAFT,
                    created_by=USER_ID,
                    screen_mockups=[
                        _screen(ids["screen_idea_b"], f"{SECRET} ideation screen")
                    ],
                ),
            ]
        )
        await db.flush()

        db.add_all(
            [
                Story(
                    id=ids["story_a"],
                    board_id=ids["board_a"],
                    topic_id=ids["topic_a"],
                    title="Local story",
                    description="Local story description",
                    status=StoryStatus.DRAFT,
                    created_by=USER_ID,
                    screen_mockups=[
                        _screen(ids["screen_story_a"], "Local story screen")
                    ],
                ),
                Story(
                    id=ids["story_b"],
                    board_id=ids["board_b"],
                    topic_id=ids["topic_b"],
                    title=f"{SECRET} story",
                    description=f"{SECRET} story description",
                    status=StoryStatus.DRAFT,
                    created_by=USER_ID,
                    screen_mockups=[
                        _screen(ids["screen_story_b"], f"{SECRET} story screen")
                    ],
                ),
                Refinement(
                    id=ids["refinement_a"],
                    ideation_id=ids["ideation_a"],
                    board_id=ids["board_a"],
                    title="Local refinement",
                    status=RefinementStatus.DRAFT,
                    created_by=USER_ID,
                    screen_mockups=[
                        _screen(ids["screen_refine_a"], "Local refinement screen")
                    ],
                ),
                Refinement(
                    id=ids["refinement_b"],
                    ideation_id=ids["ideation_b"],
                    board_id=ids["board_b"],
                    title=f"{SECRET} refinement",
                    status=RefinementStatus.DRAFT,
                    created_by=USER_ID,
                    screen_mockups=[
                        _screen(ids["screen_refine_b"], f"{SECRET} refinement screen")
                    ],
                ),
            ]
        )
        await db.flush()

        db.add_all(
            [
                Spec(
                    id=ids["spec_a"],
                    board_id=ids["board_a"],
                    ideation_id=ids["ideation_a"],
                    refinement_id=ids["refinement_a"],
                    title="Local spec",
                    status=SpecStatus.DRAFT,
                    created_by=USER_ID,
                    screen_mockups=[_screen(ids["screen_spec_a"], "Local spec screen")],
                ),
                Spec(
                    id=ids["spec_b"],
                    board_id=ids["board_b"],
                    ideation_id=ids["ideation_b"],
                    refinement_id=ids["refinement_b"],
                    title=f"{SECRET} spec",
                    status=SpecStatus.DRAFT,
                    created_by=USER_ID,
                    screen_mockups=[
                        _screen(ids["screen_spec_b"], f"{SECRET} spec screen")
                    ],
                ),
            ]
        )
        await db.flush()

        db.add_all(
            [
                Card(
                    id=ids["card_a"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a"],
                    title="Local card",
                    created_by=USER_ID,
                    screen_mockups=[],
                    knowledge_bases=[
                        {
                            "id": ids["card_kb_a"],
                            "title": "Local inline knowledge",
                            "content": "Local card knowledge",
                            "mime_type": "text/markdown",
                        }
                    ],
                ),
                Card(
                    id=ids["card_b"],
                    board_id=ids["board_b"],
                    spec_id=ids["spec_b"],
                    title=f"{SECRET} card",
                    created_by=USER_ID,
                    screen_mockups=[
                        _screen(ids["screen_card_b"], f"{SECRET} card screen")
                    ],
                    knowledge_bases=[
                        {
                            "id": ids["card_kb_b"],
                            "title": f"{SECRET} inline knowledge",
                            "content": f"{SECRET} card knowledge",
                            "mime_type": "text/markdown",
                        }
                    ],
                ),
            ]
        )
        await db.flush()

        db.add_all(
            [
                SpecQAItem(
                    id=f"qa-spec-a-{suffix}",
                    spec_id=ids["spec_a"],
                    question="Local spec question",
                    answer="Local spec answer",
                    asked_by=USER_ID,
                    answered_by=USER_ID,
                ),
                SpecQAItem(
                    id=f"qa-spec-b-{suffix}",
                    spec_id=ids["spec_b"],
                    question=f"{SECRET} spec question",
                    answer=f"{SECRET} spec answer",
                    asked_by=USER_ID,
                    answered_by=USER_ID,
                ),
                IdeationQAItem(
                    id=f"qa-idea-a-{suffix}",
                    ideation_id=ids["ideation_a"],
                    question="Local ideation question",
                    answer="Local ideation answer",
                    asked_by=USER_ID,
                    answered_by=USER_ID,
                ),
                IdeationQAItem(
                    id=f"qa-idea-b-{suffix}",
                    ideation_id=ids["ideation_b"],
                    question=f"{SECRET} ideation question",
                    answer=f"{SECRET} ideation answer",
                    asked_by=USER_ID,
                    answered_by=USER_ID,
                ),
                RefinementQAItem(
                    id=f"qa-refine-a-{suffix}",
                    refinement_id=ids["refinement_a"],
                    question="Local refinement question",
                    answer="Local refinement answer",
                    asked_by=USER_ID,
                    answered_by=USER_ID,
                ),
                RefinementQAItem(
                    id=f"qa-refine-b-{suffix}",
                    refinement_id=ids["refinement_b"],
                    question=f"{SECRET} refinement question",
                    answer=f"{SECRET} refinement answer",
                    asked_by=USER_ID,
                    answered_by=USER_ID,
                ),
            ]
        )
        db.add_all(
            [
                SpecKnowledgeBase(
                    id=f"kb-spec-a-{suffix}",
                    spec_id=ids["spec_a"],
                    title="Local spec knowledge",
                    content="Local spec content",
                    mime_type="text/markdown",
                    created_by=USER_ID,
                ),
                SpecKnowledgeBase(
                    id=f"kb-spec-b-{suffix}",
                    spec_id=ids["spec_b"],
                    title=f"{SECRET} spec knowledge",
                    content=f"{SECRET} spec content",
                    mime_type="text/markdown",
                    created_by=USER_ID,
                ),
                IdeationKnowledgeBase(
                    id=f"kb-idea-a-{suffix}",
                    ideation_id=ids["ideation_a"],
                    title="Local ideation knowledge",
                    content="Local ideation content",
                    mime_type="text/markdown",
                    created_by=USER_ID,
                ),
                IdeationKnowledgeBase(
                    id=f"kb-idea-b-{suffix}",
                    ideation_id=ids["ideation_b"],
                    title=f"{SECRET} ideation knowledge",
                    content=f"{SECRET} ideation content",
                    mime_type="text/markdown",
                    created_by=USER_ID,
                ),
                RefinementKnowledgeBase(
                    id=f"kb-refine-a-{suffix}",
                    refinement_id=ids["refinement_a"],
                    title="Local refinement knowledge",
                    content="Local refinement content",
                    mime_type="text/markdown",
                    created_by=USER_ID,
                ),
                RefinementKnowledgeBase(
                    id=f"kb-refine-b-{suffix}",
                    refinement_id=ids["refinement_b"],
                    title=f"{SECRET} refinement knowledge",
                    content=f"{SECRET} refinement content",
                    mime_type="text/markdown",
                    created_by=USER_ID,
                ),
            ]
        )
        db.add_all(
            [
                IdeationSnapshot(
                    id=f"snap-idea-a-{suffix}",
                    ideation_id=ids["ideation_a"],
                    version=1,
                    title="Local ideation snapshot",
                    created_by=USER_ID,
                ),
                IdeationSnapshot(
                    id=f"snap-idea-b-{suffix}",
                    ideation_id=ids["ideation_b"],
                    version=1,
                    title=f"{SECRET} ideation snapshot",
                    created_by=USER_ID,
                ),
                RefinementSnapshot(
                    id=f"snap-refine-a-{suffix}",
                    refinement_id=ids["refinement_a"],
                    version=1,
                    title="Local refinement snapshot",
                    created_by=USER_ID,
                ),
                RefinementSnapshot(
                    id=f"snap-refine-b-{suffix}",
                    refinement_id=ids["refinement_b"],
                    version=1,
                    title=f"{SECRET} refinement snapshot",
                    created_by=USER_ID,
                ),
            ]
        )
        await db.commit()

    return ids


async def _call(db_factory, tool_name: str, **kwargs) -> dict:
    board_id = kwargs["board_id"]
    ctx = type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": ["*"],
        },
    )()
    register_mcp_test_runtime(db_factory)
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)),
        patch.object(mcp_server, "check_permission", return_value=None),
        patch.object(mcp_server, "_mcp_check_permission", return_value=None),
    ):
        tool = await mcp_server.mcp.get_tool(tool_name)
        return json.loads(await tool.fn(**kwargs))


async def _graph_state(db_factory, ids: dict[str, str]) -> dict:
    async with db_factory() as db:
        entities = {}
        for model, key in (
            (Story, "story_a"),
            (Story, "story_b"),
            (Ideation, "ideation_a"),
            (Ideation, "ideation_b"),
            (Refinement, "refinement_a"),
            (Refinement, "refinement_b"),
            (Spec, "spec_a"),
            (Spec, "spec_b"),
            (Card, "card_a"),
            (Card, "card_b"),
        ):
            entity = await db.get(model, ids[key])
            entities[key] = {
                "screen_mockups": entity.screen_mockups,
                "knowledge_bases": (
                    entity.knowledge_bases if isinstance(entity, Card) else None
                ),
                "version": getattr(entity, "version", None),
            }

        comment_count = await db.scalar(
            select(func.count())
            .select_from(Comment)
            .where(Comment.card_id.in_([ids["card_a"], ids["card_b"]]))
        )
        activity_count = await db.scalar(
            select(func.count())
            .select_from(ActivityLog)
            .where(ActivityLog.board_id.in_([ids["board_a"], ids["board_b"]]))
        )
        return json.loads(
            json.dumps(
                {
                    "entities": entities,
                    "comment_count": comment_count,
                    "activity_count": activity_count,
                },
                sort_keys=True,
                default=str,
            )
        )


def _without_opaque_id(result: dict, opaque_id: str) -> dict:
    return json.loads(json.dumps(result).replace(opaque_id, "<opaque>"))


async def test_copy_tools_and_card_knowledge_contain_both_ends(db_factory, scope_graph):
    ids = scope_graph
    before = await _graph_state(db_factory, ids)

    for tool_name in (
        "okto_pulse_copy_mockups_to_card",
        "okto_pulse_copy_qa_to_card",
    ):
        foreign_source = await _call(
            db_factory,
            tool_name,
            board_id=ids["board_a"],
            spec_id=ids["spec_b"],
            card_id=ids["card_a"],
        )
        missing_source = await _call(
            db_factory,
            tool_name,
            board_id=ids["board_a"],
            spec_id=f"missing-{uuid.uuid4().hex}",
            card_id=ids["card_a"],
        )
        assert foreign_source == missing_source == {"error": "Spec not found"}

        foreign_target = await _call(
            db_factory,
            tool_name,
            board_id=ids["board_a"],
            spec_id=ids["spec_a"],
            card_id=ids["card_b"],
        )
        missing_target = await _call(
            db_factory,
            tool_name,
            board_id=ids["board_a"],
            spec_id=ids["spec_a"],
            card_id=f"missing-{uuid.uuid4().hex}",
        )
        assert foreign_target == missing_target == {"error": "Card not found"}

    foreign_knowledge = await _call(
        db_factory,
        "okto_pulse_get_card_knowledge",
        board_id=ids["board_a"],
        card_id=ids["card_b"],
        knowledge_id=ids["card_kb_b"],
    )
    missing_knowledge = await _call(
        db_factory,
        "okto_pulse_get_card_knowledge",
        board_id=ids["board_a"],
        card_id=f"missing-{uuid.uuid4().hex}",
        knowledge_id=ids["card_kb_b"],
    )
    assert foreign_knowledge == missing_knowledge == {"error": "Card not found"}
    assert SECRET not in json.dumps([foreign_source, foreign_target, foreign_knowledge])
    assert await _graph_state(db_factory, ids) == before


async def test_list_card_knowledge_is_bounded_no_content_leak(db_factory, scope_graph):
    """Regression (E2E finding: list_knowledge(card) leaked content). The card
    branch of okto_pulse_list_knowledge must return a BOUNDED projection — the same
    serializer as the spec/refinement branches (governance + metadata/IDs, NO body).
    The body is read via the single-item get_card_knowledge, never the listing."""
    ids = scope_graph
    result = await _call(
        db_factory,
        "okto_pulse_list_knowledge",
        board_id=ids["board_b"],
        entity_type="card",
        entity_id=ids["card_b"],
    )
    kbs = result["knowledge_bases"]
    assert kbs, "card knowledge listing should be non-empty"
    target = next(kb for kb in kbs if kb.get("id") == ids["card_kb_b"])

    # The knowledge BODY must not appear in a listing (the leak being fixed).
    assert "content" not in target
    assert f"{SECRET} card knowledge" not in json.dumps(result)
    # Metadata / IDs ARE preserved (the title legitimately carries SECRET — metadata,
    # not body — so it stays; only the content body is dropped).
    assert target["id"] == ids["card_kb_b"]
    assert target.get("mime_type")
    assert f"{SECRET} inline knowledge" in json.dumps(result)


async def test_screen_mockup_crud_contains_every_mutable_parent(
    db_factory, scope_graph
):
    ids = scope_graph
    before = await _graph_state(db_factory, ids)
    parents = (
        ("spec", ids["spec_b"], ids["screen_spec_b"]),
        ("ideation", ids["ideation_b"], ids["screen_idea_b"]),
        ("refinement", ids["refinement_b"], ids["screen_refine_b"]),
        ("story", ids["story_b"], ids["screen_story_b"]),
    )

    for entity_type, foreign_id, screen_id in parents:
        missing_id = f"missing-{uuid.uuid4().hex}"
        calls = (
            (
                "okto_pulse_add_screen_mockup",
                {"title": "must not persist"},
            ),
            (
                "okto_pulse_update_screen_mockup",
                {"screen_id": screen_id, "title": "must not persist"},
            ),
            (
                "okto_pulse_annotate_mockup",
                {"screen_id": screen_id, "text": "must not persist"},
            ),
            ("okto_pulse_list_screen_mockups", {}),
            (
                "okto_pulse_delete_screen_mockup",
                {"screen_id": screen_id},
            ),
        )
        for tool_name, extra in calls:
            foreign = await _call(
                db_factory,
                tool_name,
                board_id=ids["board_a"],
                entity_type=entity_type,
                entity_id=foreign_id,
                **extra,
            )
            missing = await _call(
                db_factory,
                tool_name,
                board_id=ids["board_a"],
                entity_type=entity_type,
                entity_id=missing_id,
                **extra,
            )
            assert _without_opaque_id(foreign, foreign_id) == _without_opaque_id(
                missing, missing_id
            )
            assert SECRET not in json.dumps(foreign)

    foreign_card = await _call(
        db_factory,
        "okto_pulse_list_screen_mockups",
        board_id=ids["board_a"],
        entity_type="card",
        entity_id=ids["card_b"],
    )
    missing_card_id = f"missing-{uuid.uuid4().hex}"
    missing_card = await _call(
        db_factory,
        "okto_pulse_list_screen_mockups",
        board_id=ids["board_a"],
        entity_type="card",
        entity_id=missing_card_id,
    )
    assert _without_opaque_id(foreign_card, ids["card_b"]) == _without_opaque_id(
        missing_card, missing_card_id
    )
    assert await _graph_state(db_factory, ids) == before


async def test_consolidated_lists_hide_foreign_children_like_missing_parents(
    db_factory, scope_graph
):
    ids = scope_graph
    before = await _graph_state(db_factory, ids)
    matrix = (
        ("okto_pulse_list_qa", "spec", ids["spec_b"]),
        ("okto_pulse_list_qa", "ideation", ids["ideation_b"]),
        ("okto_pulse_list_qa", "refinement", ids["refinement_b"]),
        ("okto_pulse_list_knowledge", "spec", ids["spec_b"]),
        ("okto_pulse_list_knowledge", "ideation", ids["ideation_b"]),
        ("okto_pulse_list_knowledge", "refinement", ids["refinement_b"]),
        ("okto_pulse_list_knowledge", "card", ids["card_b"]),
        ("okto_pulse_list_snapshots", "ideation", ids["ideation_b"]),
        ("okto_pulse_list_snapshots", "refinement", ids["refinement_b"]),
    )
    outputs = []
    for tool_name, entity_type, foreign_id in matrix:
        foreign = await _call(
            db_factory,
            tool_name,
            board_id=ids["board_a"],
            entity_type=entity_type,
            entity_id=foreign_id,
        )
        missing = await _call(
            db_factory,
            tool_name,
            board_id=ids["board_a"],
            entity_type=entity_type,
            entity_id=f"missing-{uuid.uuid4().hex}",
        )
        assert foreign == missing
        outputs.append(foreign)

    assert SECRET not in json.dumps(outputs)
    assert await _graph_state(db_factory, ids) == before


async def test_same_board_copy_mockup_crud_and_lists_remain_functional(
    db_factory, scope_graph
):
    ids = scope_graph

    copied = await _call(
        db_factory,
        "okto_pulse_copy_mockups_to_card",
        board_id=ids["board_a"],
        spec_id=ids["spec_a"],
        card_id=ids["card_a"],
    )
    assert copied == {
        "success": True,
        "copied": 1,
        "total_on_card": 1,
        "fallback": False,
    }
    copied_card_mockups = await _call(
        db_factory,
        "okto_pulse_list_screen_mockups",
        board_id=ids["board_a"],
        entity_type="card",
        entity_id=ids["card_a"],
        include_content=True,
    )
    assert copied_card_mockups["screens"][0]["html_content"] == (
        "<main>Local spec screen</main>"
    )

    copied_qa = await _call(
        db_factory,
        "okto_pulse_copy_qa_to_card",
        board_id=ids["board_a"],
        spec_id=ids["spec_a"],
        card_id=ids["card_a"],
    )
    assert copied_qa == {"success": True, "copied": 1}

    knowledge = await _call(
        db_factory,
        "okto_pulse_get_card_knowledge",
        board_id=ids["board_a"],
        card_id=ids["card_a"],
        knowledge_id=ids["card_kb_a"],
    )
    assert knowledge["success"] is True
    assert knowledge["knowledge"]["title"] == "Local inline knowledge"

    added = await _call(
        db_factory,
        "okto_pulse_add_screen_mockup",
        board_id=ids["board_a"],
        entity_type="story",
        entity_id=ids["story_a"],
        title="New local screen",
        html_content="<section>safe</section>",
    )
    assert added["success"] is True
    new_screen_id = added["screen"]["id"]

    listed = await _call(
        db_factory,
        "okto_pulse_list_screen_mockups",
        board_id=ids["board_a"],
        entity_type="story",
        entity_id=ids["story_a"],
    )
    assert listed["total"] == 2
    listed_screen = next(
        screen for screen in listed["screens"] if screen["id"] == new_screen_id
    )
    assert "html_content" not in listed_screen
    assert listed_screen["has_html_content"] is True
    assert listed_screen["html_content_bytes"] == len(
        "<section>safe</section>".encode("utf-8")
    )

    listed_with_content = await _call(
        db_factory,
        "okto_pulse_list_screen_mockups",
        board_id=ids["board_a"],
        entity_type="story",
        entity_id=ids["story_a"],
        include_content=True,
    )
    full_screen = next(
        screen
        for screen in listed_with_content["screens"]
        if screen["id"] == new_screen_id
    )
    assert full_screen["html_content"] == "<section>safe</section>"
    assert full_screen["html_content_sha256"] == listed_screen["html_content_sha256"]

    updated = await _call(
        db_factory,
        "okto_pulse_update_screen_mockup",
        board_id=ids["board_a"],
        entity_type="story",
        entity_id=ids["story_a"],
        screen_id=new_screen_id,
        title="Updated local screen",
    )
    assert updated["success"] is True
    assert updated["screen"]["title"] == "Updated local screen"

    annotated = await _call(
        db_factory,
        "okto_pulse_annotate_mockup",
        board_id=ids["board_a"],
        entity_type="story",
        entity_id=ids["story_a"],
        screen_id=new_screen_id,
        text="Local annotation",
    )
    assert annotated["success"] is True

    deleted = await _call(
        db_factory,
        "okto_pulse_delete_screen_mockup",
        board_id=ids["board_a"],
        entity_type="story",
        entity_id=ids["story_a"],
        screen_id=new_screen_id,
    )
    assert deleted == {"success": True, "screen_id": new_screen_id}

    list_matrix = (
        ("okto_pulse_list_qa", "spec", ids["spec_a"]),
        ("okto_pulse_list_qa", "ideation", ids["ideation_a"]),
        ("okto_pulse_list_qa", "refinement", ids["refinement_a"]),
        ("okto_pulse_list_knowledge", "spec", ids["spec_a"]),
        ("okto_pulse_list_knowledge", "ideation", ids["ideation_a"]),
        ("okto_pulse_list_knowledge", "refinement", ids["refinement_a"]),
        ("okto_pulse_list_knowledge", "card", ids["card_a"]),
        ("okto_pulse_list_snapshots", "ideation", ids["ideation_a"]),
        ("okto_pulse_list_snapshots", "refinement", ids["refinement_a"]),
    )
    for tool_name, entity_type, entity_id in list_matrix:
        result = await _call(
            db_factory,
            tool_name,
            board_id=ids["board_a"],
            entity_type=entity_type,
            entity_id=entity_id,
        )
        assert result["count"] == 1

    async with db_factory() as db:
        story = await db.get(Story, ids["story_a"])
        card = await db.get(Card, ids["card_a"])
        comments = await db.scalar(
            select(func.count())
            .select_from(Comment)
            .where(Comment.card_id == ids["card_a"])
        )
        assert [screen["id"] for screen in story.screen_mockups] == [
            ids["screen_story_a"]
        ]
        assert [screen["id"] for screen in card.screen_mockups] == [
            ids["screen_spec_a"]
        ]
        assert comments == 1


async def test_actor_command_board_mismatch_fails_closed_before_any_payload(
    db_factory, scope_graph
):
    ids = scope_graph
    before = await _graph_state(db_factory, ids)
    actor = ActorContext(USER_ID, "mcp", board_id=ids["board_a"])
    uow_factory = SQLAlchemyUnitOfWorkFactory(db_factory)

    cases = (
        (
            McpCopyMockupsToCardUseCase(),
            McpCopyMockupsToCardCommand(
                ids["board_b"], ids["spec_b"], ids["card_b"], None
            ),
            "spec",
        ),
        (
            McpCopyQaToCardUseCase(),
            McpCopyQaToCardCommand(ids["board_b"], ids["spec_b"], ids["card_b"]),
            "spec",
        ),
        (
            McpGetCardKnowledgeUseCase(),
            McpGetCardKnowledgeCommand(ids["board_b"], ids["card_b"], ids["card_kb_b"]),
            "card",
        ),
        (
            McpListQaUseCase(),
            McpListQaCommand(ids["board_b"], "spec", ids["spec_b"], {}),
            "spec",
        ),
        (
            McpListKnowledgeUseCase(),
            McpListKnowledgeCommand(ids["board_b"], "card", ids["card_b"], {}),
            "card",
        ),
        (
            McpListSnapshotsUseCase(),
            McpListSnapshotsCommand(ids["board_b"], "ideation", ids["ideation_b"]),
            "ideation",
        ),
    )
    for use_case, command, expected_entity_type in cases:
        with pytest.raises(EntityNotFoundError) as exc_info:
            async with uow_factory(actor=actor) as uow:
                await use_case.execute(command, actor=actor, uow=uow)
        assert exc_info.value.entity_type == expected_entity_type

    screen_command = McpScreenMockupCommand(
        board_id=ids["board_b"],
        entity_type="card",
        entity_id=ids["card_b"],
        screen_id=ids["screen_card_b"],
        title="must not persist",
        text="must not persist",
    )
    for use_case in (
        McpAddScreenMockupUseCase(),
        McpUpdateScreenMockupUseCase(),
        McpAnnotateMockupUseCase(),
        McpListScreenMockupsUseCase(),
        McpDeleteScreenMockupUseCase(),
    ):
        with pytest.raises(PermissionDeniedError) as exc_info:
            async with uow_factory(actor=actor) as uow:
                await use_case.execute(screen_command, actor=actor, uow=uow)
        assert json.loads(exc_info.value.message)["reason"] == "board_scope_mismatch"

    assert await _graph_state(db_factory, ids) == before
