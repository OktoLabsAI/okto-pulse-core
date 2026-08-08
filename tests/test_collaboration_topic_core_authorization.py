"""Central authorization regressions for collaboration and Topic mutations."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases import mcp_collaboration
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.card_collaboration import (
    CreateCardCommentCommand,
    CreateCardCommentUseCase,
)
from okto_pulse.core.application.use_cases.mcp_collaboration import (
    McpAddCommentCommand,
    McpAddCommentUseCase,
    McpAskQuestionCommand,
    McpAskQuestionUseCase,
    McpMergeTopicsCommand,
    McpMergeTopicsUseCase,
)
from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.mcp import server


BOARD_ID = "collaboration-auth-board"
CARD_ID = "collaboration-auth-card"


def _card_permissions(operation: str, *, allowed: bool = True) -> PermissionSet:
    namespace, leaf = operation.split(".", 1)
    assert namespace == "card"
    parts = leaf.split(".")
    permission: dict[str, object] = {}
    current = permission
    for part in parts[:-1]:
        child: dict[str, object] = {}
        current[part] = child
        current = child
    current[parts[-1]] = allowed
    permission["interact_in"] = {"in_progress": True}
    return PermissionSet({"card": permission})


@pytest.mark.asyncio
async def test_mcp_comment_orders_lookup_guard_writer_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    card = SimpleNamespace(id=CARD_ID, board_id=BOARD_ID, status="in_progress")
    comment = SimpleNamespace(
        id="comment-1",
        card_id=CARD_ID,
        content="Centralize this",
        author_id="agent-1",
        created_at=datetime.now(timezone.utc),
    )

    async def get_card(_card_id: str):
        events.append("lookup")
        return card

    async def create_comment(*_args, **_kwargs):
        events.append("writer")
        return comment

    async def log_activity(**_kwargs):
        events.append("activity")

    async def commit():
        events.append("commit")

    uow = SimpleNamespace(
        services=SimpleNamespace(
            cards=SimpleNamespace(get_card=get_card),
            comments=SimpleNamespace(create_comment=create_comment),
            boards=SimpleNamespace(_log_activity=log_activity),
        ),
        commit=commit,
    )
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id=BOARD_ID,
        permissions=_card_permissions("card.comments.create"),
    )
    real_require = mcp_collaboration.require_authorization

    async def tracked_require(actor, requirement, **kwargs):
        events.append("guard")
        assert requirement.operation == "card.comments.create"
        assert requirement.legacy_operation == "comments:create"
        assert requirement.entity == "card"
        assert requirement.state == "in_progress"
        assert kwargs["board_id"] == BOARD_ID
        return await real_require(actor, requirement, **kwargs)

    monkeypatch.setattr(mcp_collaboration, "require_authorization", tracked_require)

    result = await McpAddCommentUseCase().execute(
        McpAddCommentCommand(BOARD_ID, CARD_ID, "Centralize this"),
        actor=actor,
        uow=uow,
    )

    assert result.payload["success"] is True
    assert events == ["lookup", "guard", "writer", "activity", "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_type", "status"),
    (
        ("ideation", "evaluating"),
        ("refinement", "review"),
        ("spec", "approved"),
        ("sprint", "active"),
    ),
)
async def test_qa_ask_uses_scoped_parent_state_before_guard(
    monkeypatch: pytest.MonkeyPatch,
    target_type: str,
    status: str,
) -> None:
    events: list[str] = []
    parent = SimpleNamespace(id="parent-1", board_id=BOARD_ID, status=status)

    async def lookup(_parent_id: str):
        events.append("lookup")
        return parent

    async def writer(*_args, **_kwargs):
        events.append("writer")
        return SimpleNamespace(id="qa-1", question="Why?", asked_by="agent-1")

    services = SimpleNamespace(
        ideations=SimpleNamespace(get_ideation=lookup),
        refinements=SimpleNamespace(get_refinement=lookup),
        specs=SimpleNamespace(get_spec=lookup),
        sprints=SimpleNamespace(get_sprint=lookup),
        ideation_qa=SimpleNamespace(create_question=writer),
        refinement_qa=SimpleNamespace(create_question=writer),
        spec_qa=SimpleNamespace(create_question=writer),
        sprint_qa=SimpleNamespace(create_question=writer),
        boards=SimpleNamespace(_log_activity=AsyncMock()),
    )
    uow = SimpleNamespace(services=services, commit=AsyncMock())
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id=BOARD_ID,
        permissions=["qa:create"],
    )
    real_require = mcp_collaboration.require_authorization

    async def tracked_require(actor, requirement, **kwargs):
        events.append("guard")
        assert requirement.operation == f"{target_type}.qa.ask"
        assert requirement.legacy_operation == "qa:create"
        assert requirement.entity == target_type
        assert requirement.state == status
        assert kwargs["board_id"] == BOARD_ID
        return await real_require(actor, requirement, **kwargs)

    monkeypatch.setattr(mcp_collaboration, "require_authorization", tracked_require)

    await McpAskQuestionUseCase().execute(
        McpAskQuestionCommand(BOARD_ID, target_type, "parent-1", "Why?"),
        actor=actor,
        uow=uow,
    )

    assert events == ["lookup", "guard", "writer"]


@pytest.mark.asyncio
async def test_topic_merge_scopes_both_topics_before_guard_and_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    topics = {
        "source": SimpleNamespace(id="source", board_id=BOARD_ID),
        "target": SimpleNamespace(id="target", board_id=BOARD_ID),
    }

    async def get_topic(topic_id: str):
        events.append(f"lookup:{topic_id}")
        return topics[topic_id]

    class WriterReached(RuntimeError):
        pass

    async def merge_topics(*_args):
        events.append("writer")
        raise WriterReached

    uow = SimpleNamespace(
        services=SimpleNamespace(
            stories=SimpleNamespace(
                get_topic=get_topic,
                merge_topics=merge_topics,
            )
        )
    )
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id=BOARD_ID,
        permissions=["specs:update"],
    )
    real_require = mcp_collaboration.require_authorization

    async def tracked_require(actor, requirement, **kwargs):
        events.append("guard")
        assert requirement.operation == "topic.entity.merge"
        assert requirement.legacy_operation == "specs:update"
        assert requirement.entity is None
        assert requirement.state is None
        assert kwargs["board_id"] == BOARD_ID
        return await real_require(actor, requirement, **kwargs)

    monkeypatch.setattr(mcp_collaboration, "require_authorization", tracked_require)

    with pytest.raises(WriterReached):
        await McpMergeTopicsUseCase().execute(
            McpMergeTopicsCommand(BOARD_ID, "source", "target"),
            actor=actor,
            uow=uow,
        )

    assert events == ["lookup:source", "lookup:target", "guard", "writer"]


@pytest.mark.asyncio
async def test_rest_collaboration_denies_after_scope_before_writer() -> None:
    card = SimpleNamespace(id=CARD_ID, board_id=BOARD_ID, status="in_progress")
    writer = AsyncMock()
    uow = SimpleNamespace(
        boards=SimpleNamespace(
            get=AsyncMock(
                return_value=SimpleNamespace(
                    id=BOARD_ID,
                    owner_id="user-1",
                    realm_id=None,
                )
            )
        ),
        services=SimpleNamespace(
            cards=SimpleNamespace(get_card=AsyncMock(return_value=card)),
            shares=SimpleNamespace(),
            comments=SimpleNamespace(create_comment=writer),
        ),
        commit=AsyncMock(),
    )
    actor = ActorContext(
        "user-1",
        "rest",
        permissions=_card_permissions("card.comments.create", allowed=False),
    )

    with pytest.raises(PermissionDeniedError):
        await CreateCardCommentUseCase().execute(
            CreateCardCommentCommand(
                CARD_ID,
                SimpleNamespace(content="Denied", comment_type="text"),
            ),
            actor=actor,
            uow=uow,
        )

    uow.services.cards.get_card.assert_awaited_once_with(CARD_ID)
    uow.boards.get.assert_awaited_once_with(BOARD_ID)
    writer.assert_not_awaited()
    uow.commit.assert_not_awaited()


class _UowContext:
    def __init__(self, uow) -> None:
        self.uow = uow

    async def __aenter__(self):
        return self.uow

    async def __aexit__(self, *_args):
        return False


def _install_mcp_uow(monkeypatch: pytest.MonkeyPatch, uow) -> None:
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _UowContext(uow),
    )


def _denied_detail(raw: str) -> dict[str, object]:
    outer = json.loads(raw)
    return json.loads(outer["error"])


@pytest.mark.asyncio
async def test_mcp_boundary_projects_core_comment_denial_after_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    card = SimpleNamespace(id=CARD_ID, board_id=BOARD_ID, status="in_progress")
    writer = AsyncMock()
    uow = SimpleNamespace(
        services=SimpleNamespace(
            cards=SimpleNamespace(get_card=AsyncMock(return_value=card)),
            comments=SimpleNamespace(create_comment=writer),
        )
    )
    ctx = SimpleNamespace(
        agent_id="agent-1",
        agent_name="Denied agent",
        board_id=BOARD_ID,
        permissions=_card_permissions("card.comments.create", allowed=False),
    )
    monkeypatch.setattr(server, "_get_agent_ctx", AsyncMock(return_value=ctx))
    _install_mcp_uow(monkeypatch, uow)

    raw = await server.okto_pulse_add_comment.fn(
        board_id=BOARD_ID,
        card_id=CARD_ID,
        content="Denied",
    )

    assert _denied_detail(raw)["required_permission"] == "card.comments.create"
    uow.services.cards.get_card.assert_awaited_once_with(CARD_ID)
    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_mcp_boundary_projects_core_topic_denial_after_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topic = SimpleNamespace(id="topic-1", board_id=BOARD_ID)
    writer = AsyncMock()
    uow = SimpleNamespace(
        services=SimpleNamespace(
            stories=SimpleNamespace(
                get_topic=AsyncMock(return_value=topic),
                update_topic=writer,
            )
        )
    )
    ctx = SimpleNamespace(
        agent_id="agent-1",
        agent_name="Denied agent",
        board_id=BOARD_ID,
        permissions=PermissionSet(
            {"topic": {"entity": {"edit_fields": False}}}
        ),
    )
    monkeypatch.setattr(server, "_get_agent_ctx", AsyncMock(return_value=ctx))
    _install_mcp_uow(monkeypatch, uow)

    raw = await server.okto_pulse_update_topic.fn(
        board_id=BOARD_ID,
        topic_id="topic-1",
        name="Denied",
    )

    assert _denied_detail(raw)["required_permission"] == "topic.entity.edit_fields"
    uow.services.stories.get_topic.assert_awaited_once_with("topic-1")
    writer.assert_not_awaited()
