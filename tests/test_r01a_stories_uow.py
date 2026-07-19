"""Spec R01A REST-FU6-S2 — Stories + Topics REST on the UnitOfWork.

Every ``api/stories.py`` endpoint (topic create/list/update/delete/merge; story
create/list/get/update/move/archive/restore; story↔ideation link single + bulk;
convert-to-ideation) now routes through a transport-free use case
(``application/use_cases/stories_crud.py``) + ``get_unit_of_work``. The legacy
behavior preserved here, end-to-end through ``TestClient`` (commit really
persists across requests):

* topic lifecycle create→list→update→delete + the ``Board not found`` /
  ``Topic not found`` 404s, the not-empty 409 (``TopicNotEmptyError``) and the
  invalid-merge 400 (``InvalidTopicMergeError``)
* story lifecycle create (+ bad-topic 400, missing-board 404) → list → get
  (+ 404) → update (+ 404) → move (+ 404) → archive → restore
* story↔ideation single link (+ missing 404 "Story or Ideation not found") and
  the bulk contract endpoint, and convert-to-ideation (200 envelope)
* a direct use-case assertion (``GetStoryUseCase`` raises
  ``EntityNotFoundError``) and an AST signature guard proving every endpoint —
  and every registered route — takes ``uow`` (``get_unit_of_work``), not a raw
  ``AsyncSession``.

No agent row is seeded for the test user, so ``resolve_user_permissions`` grants
full access (the permission gate is exercised structurally; denial is out of
scope for this oracle).
"""

from __future__ import annotations

import inspect
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import stories as stories_api
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.stories import router as stories_router
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu6-s2-user"
OTHER = "r01a-fu6-s2-other"
PREFIX = "/api/v1"

_ENDPOINTS = (
    "create_topic",
    "list_topics",
    "update_topic",
    "delete_topic",
    "merge_topics",
    "create_story",
    "list_stories",
    "get_story",
    "update_story",
    "move_story",
    "archive_story",
    "restore_story",
    "link_story_to_ideation",
    "link_stories_to_ideation",
    "convert_stories",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(stories_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


def _missing() -> str:
    return f"missing-{uuid.uuid4().hex[:8]}"


async def _seed_board(owner: str = USER) -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu6s2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name="fu6s2",
                owner_id=owner,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.commit()
    return bid


async def _share_board(board_id: str, *, permission: str) -> None:
    from sqlalchemy_test_models import BoardShare

    async with get_session_factory()() as db:
        db.add(
            BoardShare(
                board_id=board_id,
                user_id=USER,
                realm_id=LOCAL_REALM_ID,
                permission=permission,
                shared_by=OTHER,
            )
        )
        await db.commit()


async def _seed_topic(
    board_id: str, name: str | None = None, *, user_id: str = USER
) -> str:
    from okto_pulse.core.models.schemas import TopicCreate
    from okto_pulse.core.services import StoryService

    async with get_session_factory()() as db:
        topic = await StoryService(db).create_topic(
            board_id, user_id, TopicCreate(name=name or f"topic-{uuid.uuid4().hex[:6]}")
        )
        await db.commit()
        return topic.id


async def _seed_story(
    board_id: str,
    topic_id: str,
    *,
    status: str = "draft",
    user_id: str = USER,
) -> str:
    from sqlalchemy_test_models import StoryStatus
    from okto_pulse.core.models.schemas import StoryCreate
    from okto_pulse.core.services import StoryService

    async with get_session_factory()() as db:
        story = await StoryService(db).create_story(
            board_id,
            user_id,
            StoryCreate(
                title=f"story-{uuid.uuid4().hex[:6]}",
                description="seeded story body",
                topic_id=topic_id,
                status=StoryStatus(status),
            ),
        )
        await db.commit()
        return story.id


async def _seed_ideation(board_id: str, *, user_id: str = USER) -> str:
    from okto_pulse.core.models.schemas import IdeationCreate
    from okto_pulse.core.services import IdeationService

    async with get_session_factory()() as db:
        ideation = await IdeationService(db).create_ideation(
            board_id,
            user_id,
            IdeationCreate(title=f"ideation-{uuid.uuid4().hex[:6]}"),
        )
        await db.commit()
        return ideation.id


# --- topics -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_topic_create_list_update_delete(client) -> None:
    bid = await _seed_board()

    created = client.post(f"{PREFIX}/boards/{bid}/topics", json={"name": "Topic A"})
    assert created.status_code == 201, created.text
    tid = created.json()["id"]

    listed = client.get(f"{PREFIX}/boards/{bid}/topics")
    assert listed.status_code == 200
    assert tid in {t["id"] for t in listed.json()}

    renamed = client.patch(f"{PREFIX}/topics/{tid}", json={"name": "Topic A2"})
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["name"] == "Topic A2"

    deleted = client.delete(f"{PREFIX}/topics/{tid}")
    assert deleted.status_code == 200, deleted.text
    assert deleted.json()["deleted_topic_id"] == tid
    # delete really committed: a second delete is a 404.
    assert client.delete(f"{PREFIX}/topics/{tid}").status_code == 404


@pytest.mark.asyncio
async def test_viewer_share_reads_stories_and_topics_but_cannot_mutate(client) -> None:
    board_id = await _seed_board(owner=OTHER)
    topic_id = await _seed_topic(board_id, name="shared topic", user_id=OTHER)
    story_id = await _seed_story(board_id, topic_id, user_id=OTHER)
    await _share_board(board_id, permission="viewer")

    assert client.get(f"{PREFIX}/boards/{board_id}/topics").status_code == 200
    assert client.get(f"{PREFIX}/boards/{board_id}/stories").status_code == 200
    assert client.get(f"{PREFIX}/stories/{story_id}").status_code == 200

    attempts = (
        client.post(f"{PREFIX}/boards/{board_id}/topics", json={"name": "blocked"}),
        client.patch(f"{PREFIX}/topics/{topic_id}", json={"name": "blocked"}),
        client.post(
            f"{PREFIX}/boards/{board_id}/stories",
            json={"title": "blocked", "description": "blocked", "topic_id": topic_id},
        ),
        client.patch(f"{PREFIX}/stories/{story_id}", json={"title": "blocked"}),
    )
    assert {response.status_code for response in attempts} == {404}

    from sqlalchemy_test_models import Story, Topic

    async with get_session_factory()() as db:
        assert (await db.get(Topic, topic_id)).name == "shared topic"
        assert (await db.get(Story, story_id)).title != "blocked"


@pytest.mark.asyncio
async def test_editor_share_can_create_and_update_story_content(client) -> None:
    board_id = await _seed_board(owner=OTHER)
    await _share_board(board_id, permission="editor")

    topic_response = client.post(
        f"{PREFIX}/boards/{board_id}/topics", json={"name": "editor topic"}
    )
    assert topic_response.status_code == 201, topic_response.text
    topic_id = topic_response.json()["id"]
    story_response = client.post(
        f"{PREFIX}/boards/{board_id}/stories",
        json={
            "title": "editor story",
            "description": "editor description",
            "topic_id": topic_id,
        },
    )
    assert story_response.status_code == 201, story_response.text
    story_id = story_response.json()["id"]

    updated = client.patch(
        f"{PREFIX}/stories/{story_id}", json={"title": "editor updated"}
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["title"] == "editor updated"


@pytest.mark.asyncio
async def test_create_topic_missing_board_404(client) -> None:
    resp = client.post(f"{PREFIX}/boards/{_missing()}/topics", json={"name": "x"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_update_topic_missing_404(client) -> None:
    resp = client.patch(f"{PREFIX}/topics/{_missing()}", json={"name": "x"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Topic not found"


@pytest.mark.asyncio
async def test_foreign_topic_mutations_match_missing_without_writer_effect(client) -> None:
    foreign_board = await _seed_board(owner=OTHER)
    foreign_topic = await _seed_topic(foreign_board, name="private", user_id=OTHER)

    foreign_update = client.patch(
        f"{PREFIX}/topics/{foreign_topic}", json={"name": "leaked"}
    )
    missing_update = client.patch(
        f"{PREFIX}/topics/{_missing()}", json={"name": "leaked"}
    )
    foreign_delete = client.delete(f"{PREFIX}/topics/{foreign_topic}")
    missing_delete = client.delete(f"{PREFIX}/topics/{_missing()}")

    assert foreign_update.content == missing_update.content
    assert foreign_delete.content == missing_delete.content
    from sqlalchemy_test_models import Topic

    async with get_session_factory()() as db:
        assert (await db.get(Topic, foreign_topic)).name == "private"


@pytest.mark.asyncio
async def test_delete_non_empty_topic_409(client) -> None:
    bid = await _seed_board()
    tid = await _seed_topic(bid)
    await _seed_story(bid, tid)
    resp = client.delete(f"{PREFIX}/topics/{tid}")
    assert resp.status_code == 409, resp.text


@pytest.mark.asyncio
async def test_merge_topics_200_and_invalid_400(client) -> None:
    bid = await _seed_board()
    source = await _seed_topic(bid, name="merge-src")
    target = await _seed_topic(bid, name="merge-dst")

    ok = client.post(f"{PREFIX}/topics/{source}/merge", json={"target_topic_id": target})
    assert ok.status_code == 200, ok.text
    body = ok.json()
    assert body["source"]["id"] == source and body["target"]["id"] == target

    # Source == target → InvalidTopicMergeError → 400.
    other = await _seed_topic(bid, name="merge-self")
    bad = client.post(f"{PREFIX}/topics/{other}/merge", json={"target_topic_id": other})
    assert bad.status_code == 400, bad.text


@pytest.mark.asyncio
async def test_merge_foreign_target_matches_missing_without_mutating_source(client) -> None:
    board_id = await _seed_board()
    source = await _seed_topic(board_id, name="source-kept")
    foreign_board = await _seed_board(owner=OTHER)
    foreign_target = await _seed_topic(
        foreign_board, name="foreign-target", user_id=OTHER
    )

    foreign = client.post(
        f"{PREFIX}/topics/{source}/merge",
        json={"target_topic_id": foreign_target},
    )
    missing = client.post(
        f"{PREFIX}/topics/{source}/merge",
        json={"target_topic_id": _missing()},
    )

    assert foreign.status_code == missing.status_code == 404
    assert foreign.content == missing.content
    from sqlalchemy_test_models import Topic

    async with get_session_factory()() as db:
        assert (await db.get(Topic, source)).name == "source-kept"


# --- stories ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_story_create_get_list(client) -> None:
    bid = await _seed_board()
    tid = await _seed_topic(bid)

    created = client.post(
        f"{PREFIX}/boards/{bid}/stories",
        json={"title": "Story 1", "description": "as a user", "topic_id": tid},
    )
    assert created.status_code == 201, created.text
    sid = created.json()["id"]

    got = client.get(f"{PREFIX}/stories/{sid}")
    assert got.status_code == 200, got.text
    assert got.json()["id"] == sid

    listed = client.get(f"{PREFIX}/boards/{bid}/stories")
    assert listed.status_code == 200
    assert sid in {s["id"] for s in listed.json()}


@pytest.mark.asyncio
async def test_create_story_bad_topic_400_and_missing_board_404(client) -> None:
    bid = await _seed_board()
    bad_topic = client.post(
        f"{PREFIX}/boards/{bid}/stories",
        json={"title": "S", "description": "d", "topic_id": _missing()},
    )
    assert bad_topic.status_code == 400, bad_topic.text

    miss_board = client.post(
        f"{PREFIX}/boards/{_missing()}/stories",
        json={"title": "S", "description": "d", "topic_id": _missing()},
    )
    assert miss_board.status_code == 404
    assert miss_board.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_get_story_missing_404(client) -> None:
    resp = client.get(f"{PREFIX}/stories/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Story not found"


@pytest.mark.asyncio
async def test_foreign_story_read_and_update_match_missing_without_mutation(client) -> None:
    foreign_board = await _seed_board(owner=OTHER)
    foreign_topic = await _seed_topic(foreign_board, user_id=OTHER)
    foreign_story = await _seed_story(
        foreign_board, foreign_topic, user_id=OTHER
    )

    foreign_get = client.get(f"{PREFIX}/stories/{foreign_story}")
    missing_get = client.get(f"{PREFIX}/stories/{_missing()}")
    foreign_update = client.patch(
        f"{PREFIX}/stories/{foreign_story}", json={"title": "leaked"}
    )
    missing_update = client.patch(
        f"{PREFIX}/stories/{_missing()}", json={"title": "leaked"}
    )

    assert foreign_get.content == missing_get.content
    assert foreign_update.content == missing_update.content
    from sqlalchemy_test_models import Story

    async with get_session_factory()() as db:
        assert (await db.get(Story, foreign_story)).title != "leaked"


@pytest.mark.asyncio
async def test_update_story_200_and_missing_404(client) -> None:
    bid = await _seed_board()
    tid = await _seed_topic(bid)
    sid = await _seed_story(bid, tid)

    ok = client.patch(f"{PREFIX}/stories/{sid}", json={"title": "renamed"})
    assert ok.status_code == 200, ok.text
    assert ok.json()["title"] == "renamed"

    miss = client.patch(f"{PREFIX}/stories/{_missing()}", json={"title": "x"})
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Story not found"


@pytest.mark.asyncio
async def test_move_story_200_and_missing_404(client) -> None:
    bid = await _seed_board()
    tid = await _seed_topic(bid)
    sid = await _seed_story(bid, tid)  # draft → triage is allowed

    ok = client.post(f"{PREFIX}/stories/{sid}/move", json={"status": "triage"})
    assert ok.status_code == 200, ok.text
    assert ok.json()["status"] == "triage"

    miss = client.post(f"{PREFIX}/stories/{_missing()}/move", json={"status": "triage"})
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Story not found"


@pytest.mark.asyncio
async def test_archive_then_restore_story(client) -> None:
    bid = await _seed_board()
    tid = await _seed_topic(bid)
    sid = await _seed_story(bid, tid)

    archived = client.delete(f"{PREFIX}/stories/{sid}")
    assert archived.status_code == 200, archived.text
    assert archived.json()["archived"] is True

    restored = client.post(f"{PREFIX}/stories/{sid}/restore")
    assert restored.status_code == 200, restored.text
    assert restored.json()["archived"] is False


# --- ideation linking + conversion ------------------------------------------


@pytest.mark.asyncio
async def test_link_story_to_ideation_200_and_missing_404(client) -> None:
    bid = await _seed_board()
    tid = await _seed_topic(bid)
    sid = await _seed_story(bid, tid)
    ideation_id = await _seed_ideation(bid)

    ok = client.post(f"{PREFIX}/stories/{sid}/ideations", json={"ideation_id": ideation_id})
    assert ok.status_code == 200, ok.text
    assert ok.json()["status"] == "converted"

    miss = client.post(
        f"{PREFIX}/stories/{_missing()}/ideations", json={"ideation_id": ideation_id}
    )
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Story or Ideation not found"


@pytest.mark.asyncio
async def test_link_stories_to_ideation_bulk(client) -> None:
    bid = await _seed_board()
    tid = await _seed_topic(bid)
    sid = await _seed_story(bid, tid)
    ideation_id = await _seed_ideation(bid)

    resp = client.post(f"{PREFIX}/ideations/{ideation_id}/stories", json={"story_ids": [sid]})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"] is True
    assert body["ideation_id"] == ideation_id
    assert body["story_ids"] == [sid]


@pytest.mark.asyncio
async def test_bulk_link_preflights_all_stories_before_any_writer(client) -> None:
    from okto_pulse.core.services import StoryService

    owned_board = await _seed_board()
    owned_topic = await _seed_topic(owned_board)
    owned_story = await _seed_story(owned_board, owned_topic)
    ideation_id = await _seed_ideation(owned_board)

    foreign_board = await _seed_board(owner=OTHER)
    foreign_topic = await _seed_topic(foreign_board, user_id=OTHER)
    foreign_story = await _seed_story(
        foreign_board, foreign_topic, user_id=OTHER
    )

    writer = AsyncMock(side_effect=AssertionError("writer must not run"))
    with patch.object(StoryService, "link_story_to_ideation", writer):
        foreign = client.post(
            f"{PREFIX}/ideations/{ideation_id}/stories",
            json={"story_ids": [owned_story, foreign_story]},
        )
        missing = client.post(
            f"{PREFIX}/ideations/{ideation_id}/stories",
            json={"story_ids": [owned_story, _missing()]},
        )

    assert foreign.status_code == missing.status_code == 404
    assert foreign.content == missing.content
    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_bulk_link_preflights_ideation_before_any_story_writer(client) -> None:
    from okto_pulse.core.services import StoryService

    owned_board = await _seed_board()
    owned_topic = await _seed_topic(owned_board)
    owned_story = await _seed_story(owned_board, owned_topic)
    foreign_board = await _seed_board(owner=OTHER)
    foreign_ideation = await _seed_ideation(foreign_board, user_id=OTHER)

    writer = AsyncMock(side_effect=AssertionError("writer must not run"))
    with patch.object(StoryService, "link_story_to_ideation", writer):
        foreign = client.post(
            f"{PREFIX}/ideations/{foreign_ideation}/stories",
            json={"story_ids": [owned_story]},
        )
        missing = client.post(
            f"{PREFIX}/ideations/{_missing()}/stories",
            json={"story_ids": [owned_story]},
        )

    assert foreign.status_code == missing.status_code == 404
    assert foreign.content == missing.content
    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_convert_stories_creates_ideation(client) -> None:
    bid = await _seed_board()
    tid = await _seed_topic(bid)
    sid = await _seed_story(bid, tid, status="ready")  # only ready/converted convert

    resp = client.post(
        f"{PREFIX}/boards/{bid}/stories/convert-to-ideation", json={"story_ids": [sid]}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"] is True
    assert body["ideation"]["id"]
    assert len(body["links"]) == 1


@pytest.mark.asyncio
async def test_convert_stories_missing_board_404(client) -> None:
    resp = client.post(
        f"{PREFIX}/boards/{_missing()}/stories/convert", json={"story_ids": [_missing()]}
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_story_use_case_raises_for_missing_story() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from okto_pulse.core.application.use_cases.stories_crud import (
        GetStoryCommand,
        GetStoryUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest", realm_id=LOCAL_REALM_ID)
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetStoryUseCase().execute(
                GetStoryCommand(_missing()), actor=actor, uow=uow
            )


def test_fu6_s2_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(stories_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_stories_router_has_no_endpoint_on_get_db() -> None:
    """FU6-S2 closes api/stories.py: no REGISTERED route endpoint may depend on
    get_db / a raw AsyncSession anymore."""
    from okto_pulse.core.infra.database import get_db as _get_db

    checked = 0
    for route in stories_router.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        checked += 1
        sig = inspect.signature(endpoint)
        assert "db" not in sig.parameters, f"{endpoint.__name__} still takes a db session"
        for param in sig.parameters.values():
            dep = getattr(param.default, "dependency", None)
            assert dep is not _get_db, f"{endpoint.__name__} still depends on get_db"
    assert checked > 0


async def _seed_agent_with_board_override(board_id: str, overrides: dict) -> None:
    """Seed an Agent owned by USER + an AgentBoard carrying restrictive
    ``permission_overrides`` for ``board_id``. With an agent present (no preset /
    flags), the resolver starts from the full registry (all True) and the board
    override AND-restricts it (ceiling model), so this proves the migrated stories
    use cases honor board-scoped overrides via services.main.resolve_user_permissions.
    """
    from sqlalchemy_test_models import Agent, AgentBoard

    aid = f"agent-fu6s2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Agent(
                id=aid,
                name="fu6s2-agent",
                api_key=f"key-{uuid.uuid4().hex}",
                api_key_hash=f"hash-{uuid.uuid4().hex}",
                created_by=USER,
            )
        )
        db.add(
            AgentBoard(
                agent_id=aid,
                board_id=board_id,
                granted_by=USER,
                permission_overrides=overrides,
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_board_permission_overrides_deny_topic_delete(client) -> None:
    """Direct oracle for the FU6-S2 permission-overrides rework: a restrictive
    ``AgentBoard.permission_overrides`` ({topic.entity.delete: False}) makes the
    MIGRATED ``delete_topic`` endpoint return 403, while the SAME actor deletes a
    topic on a board WITHOUT that override (200). Proves the board-scoped denial is
    honored end-to-end through the use case — not a global flag — exactly as the
    legacy adapter resolved ``AgentBoard.permission_overrides`` before
    ``check_permission``."""
    denied_board = await _seed_board()
    denied_topic = await _seed_topic(denied_board)
    await _seed_agent_with_board_override(
        denied_board, {"topic": {"entity": {"delete": False}}}
    )

    blocked = client.delete(f"{PREFIX}/topics/{denied_topic}")
    assert blocked.status_code == 403, blocked.text

    # the override only restricts delete: reading topics on that board still passes
    listed = client.get(f"{PREFIX}/boards/{denied_board}/topics")
    assert listed.status_code == 200, listed.text

    # board-scoped control: a board with NO override lets the SAME actor delete
    open_board = await _seed_board()
    open_topic = await _seed_topic(open_board)
    allowed = client.delete(f"{PREFIX}/topics/{open_topic}")
    assert allowed.status_code == 200, allowed.text
