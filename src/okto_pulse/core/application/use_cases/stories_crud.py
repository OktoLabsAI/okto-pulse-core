"""Story + Topic CRUD use cases (SaaS Refactor spec R01A REST-FU6-S2).

Transport-free reimplementations of every ``api/stories.py`` endpoint that still
bound ``get_db`` — the topic CRUD/merge family and the story CRUD / move /
archive / restore / ideation-link / convert family. Each use case delegates to
the EXISTING ``StoryService`` / ``BoardService`` methods so the SQL inline, the
activity logging, the lifecycle transitions and the event publishing all stay in
the service; only the transport envelope (board-ownership 404, permission gate,
not-found, mutate, commit, re-fetch) moves here.

Behavioral fidelity to the legacy endpoints:

* Board ownership is enforced by ``_ensure_board`` (``BoardService.get_board``),
  raising ``EntityNotFoundError("board")`` — the adapter maps it to the legacy
  ``"Board not found"`` 404.
* Permission gating reproduces the legacy ``_require_permissions`` exactly: it
  resolves the actor's permission set via ``services.main.resolve_user_permissions``
  and applies ``check_with_state`` when an entity + entity_status is supplied
  (the story ``interact_in.*`` gate) or ``check_permission`` otherwise, raising
  ``PermissionDeniedError`` (adapter → 403, ``json``-decoded detail) on the first
  failure. ``services.main.resolve_user_permissions`` resolves the actor's flags +
  preset AND the per-board ``AgentBoard.permission_overrides`` (ceiling model —
  a board override can only restrict), so board-scoped restrictive grants/denies
  are honored here exactly as in the legacy adapter (see the dedicated
  ``test_board_permission_overrides_deny_topic_delete`` oracle).
* ``TopicOperationError`` (a ``ValueError`` subclass: name-conflict / not-empty /
  invalid-merge) and the plain ``ValueError`` raised by the story service
  propagate UNCAUGHT so the adapter maps them to the legacy status/detail.
* Reads do not commit; writes ``commit(uow)`` after the service mutation, then
  re-fetch via ``get_story`` (so the response carries the loaded relationships)
  exactly as the legacy endpoints did.

The three topic-by-id endpoints (update / delete / merge) receive the target
topic's ``board_id`` (+ pre-mutation ``archived`` for update) from the adapter,
which performs the legacy ``db.get(Topic, …)`` transport load; no entity lookup
by ``select``/``session.get`` happens in this layer (the relational ratchet
gate).
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog


# --- shared transport-free guards (legacy _ensure_board / _require_permissions)


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


async def _ensure_board(
    services: ApplicationServiceCatalog,
    board_id: str,
    user_id: str,
    *,
    query_scope: QueryScope | None = None,
) -> None:
    """Reproduce the legacy ``_ensure_board``: raise ``EntityNotFoundError("board")``
    (adapter → 404 "Board not found") when the board is missing / not owned."""
    board = await services.boards.get_board(
        board_id,
        user_id,
        query_scope=query_scope,
    )
    if not board:
        raise EntityNotFoundError("board", board_id)


async def _require_permissions(
    services: ApplicationServiceCatalog,
    user_id: str,
    board_id: str,
    permissions: "str | list[str | None] | None",
    *,
    entity: str | None = None,
    entity_status: str | None = None,
) -> None:
    """Transport-free twin of the legacy ``_require_permissions`` guard.

    Resolves the actor's permission set via ``resolve_user_permissions`` and
    enforces each permission in order, using ``check_with_state`` when an entity
    + status is supplied (the story state gate) and ``check_permission``
    otherwise; raises ``PermissionDeniedError`` on the first failure."""
    from okto_pulse.core.services.permission_policy import PermissionSet, check_permission

    if permissions is None:
        return
    permission_set = await services.resolve_user_permissions(user_id, board_id)
    permission_names = [permissions] if isinstance(permissions, str) else list(permissions)
    for permission in permission_names:
        if not permission:
            continue
        if isinstance(permission_set, PermissionSet) and entity and entity_status:
            error = permission_set.check_with_state(permission, entity, entity_status)
        else:
            error = check_permission(permission_set, permission)
        if error:
            raise PermissionDeniedError(error)


# ===========================================================================
# Topics
# ===========================================================================


# --- create topic -----------------------------------------------------------


class CreateTopicCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class CreateTopicResult:
    __slots__ = ("topic",)

    def __init__(self, topic: Any) -> None:
        self.topic = topic


class CreateTopicUseCase:
    """Create a board-scoped Story Topic (write). Enforces board ownership +
    ``topic.entity.create``; ``TopicOperationError`` (name conflict → 409)
    propagates for the adapter; a missing board (service returns ``None``) is
    ``EntityNotFoundError("board")`` (→ 404 "Board not found")."""

    async def execute(
        self, command: CreateTopicCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateTopicResult:
        await _ensure_board(uow.services, command.board_id, actor.actor_id)
        await _require_permissions(
            uow.services, actor.actor_id, command.board_id, "topic.entity.create"
        )
        topic = await uow.services.stories.create_topic(
            command.board_id, actor.actor_id, command.data
        )
        if not topic:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return CreateTopicResult(topic)


# --- list topics ------------------------------------------------------------


class ListTopicsCommand:
    __slots__ = ("board_id", "include_archived")

    def __init__(self, board_id: str, *, include_archived: bool = False) -> None:
        self.board_id = board_id
        self.include_archived = include_archived


class ListTopicsResult:
    __slots__ = ("topics",)

    def __init__(self, topics: list[Any]) -> None:
        self.topics = topics


class ListTopicsUseCase:
    """List a board's Story Topics (read, no commit). Board ownership 404 +
    ``topic.entity.read`` gate before the service read."""

    async def execute(
        self, command: ListTopicsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListTopicsResult:
        await _ensure_board(uow.services, command.board_id, actor.actor_id)
        await _require_permissions(
            uow.services, actor.actor_id, command.board_id, "topic.entity.read"
        )
        topics = await uow.services.stories.list_topics(
            command.board_id, include_archived=command.include_archived
        )
        return ListTopicsResult(topics)


# --- update topic -----------------------------------------------------------


class UpdateTopicCommand:
    __slots__ = ("topic_id", "data")

    def __init__(self, topic_id: str, data: Any) -> None:
        self.topic_id = topic_id
        self.data = data


class UpdateTopicResult:
    __slots__ = ("topic",)

    def __init__(self, topic: Any) -> None:
        self.topic = topic


class UpdateTopicUseCase:
    """Update a Story Topic (write). The use case loads the topic itself (spec
    R01A REST-FU6-S2 rework — the adapter no longer issues ``db.get(Topic, …)``),
    deriving the board id + pre-mutation ``archived`` for the ownership gate and
    the archive/restore/edit-aware ``topic_update_permissions``. Order mirrors the
    legacy adapter exactly: topic-load → ``_ensure_board(topic.board_id)`` →
    ``_require_permissions`` → ``update_topic``. ``TopicOperationError`` (name
    conflict → 409) propagates; a missing topic is ``EntityNotFoundError("topic")``
    (→ 404 "Topic not found")."""

    async def execute(
        self, command: UpdateTopicCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateTopicResult:
        from okto_pulse.core.services.story_permissions import topic_update_permissions

        service = uow.services.stories
        topic = await service.get_topic(command.topic_id)
        if not topic:
            raise EntityNotFoundError("topic", command.topic_id)
        await _ensure_board(uow.services, topic.board_id, actor.actor_id)
        update_data = command.data.model_dump(exclude_unset=True)
        await _require_permissions(
            uow.services,
            actor.actor_id,
            topic.board_id,
            topic_update_permissions(update_data, current_archived=bool(topic.archived)),
        )
        updated = await service.update_topic(command.topic_id, actor.actor_id, command.data)
        if not updated:
            raise EntityNotFoundError("topic", command.topic_id)
        await commit(uow)
        return UpdateTopicResult(updated)


# --- delete topic -----------------------------------------------------------


class DeleteTopicCommand:
    __slots__ = ("topic_id",)

    def __init__(self, topic_id: str) -> None:
        self.topic_id = topic_id


class DeleteTopicResult:
    __slots__ = ()


class DeleteTopicUseCase:
    """Delete a Story Topic only when it has no Stories (write). The use case loads
    the topic itself (FU6-S2 rework — no adapter ORM), deriving ``board_id`` for the
    ownership + ``topic.entity.delete`` gate, mirroring the legacy order
    (topic-load → ``_ensure_board`` → ``_require_permissions`` → ``delete_topic``).
    ``TopicNotEmptyError`` (→ 409) propagates; a missing topic is
    ``EntityNotFoundError("topic")`` (→ 404 "Topic not found")."""

    async def execute(
        self, command: DeleteTopicCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteTopicResult:
        service = uow.services.stories
        topic = await service.get_topic(command.topic_id)
        if not topic:
            raise EntityNotFoundError("topic", command.topic_id)
        await _ensure_board(uow.services, topic.board_id, actor.actor_id)
        await _require_permissions(
            uow.services, actor.actor_id, topic.board_id, "topic.entity.delete"
        )
        deleted = await service.delete_topic(command.topic_id, actor.actor_id)
        if not deleted:
            raise EntityNotFoundError("topic", command.topic_id)
        await commit(uow)
        return DeleteTopicResult()


# --- merge topics -----------------------------------------------------------


class MergeTopicsCommand:
    __slots__ = ("source_topic_id", "target_topic_id")

    def __init__(self, source_topic_id: str, target_topic_id: str) -> None:
        self.source_topic_id = source_topic_id
        self.target_topic_id = target_topic_id


class MergeTopicsResult:
    __slots__ = ("result",)

    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result


class MergeTopicsUseCase:
    """Merge a source Topic into an active target Topic (write). The use case loads
    the source topic itself (FU6-S2 rework — no adapter ORM), deriving its
    ``board_id`` for the ownership + ``topic.entity.merge`` gate, mirroring the
    legacy order (source-load → ``_ensure_board`` → ``_require_permissions`` →
    ``merge_topics``). ``InvalidTopicMergeError`` (→ 400) propagates; a missing
    source/target is ``EntityNotFoundError("topic")`` (→ 404 "Topic not found").
    Returns the service's raw result dict (source/target ORM topics + counters)
    for the adapter to shape into ``TopicMergeResponse``."""

    async def execute(
        self, command: MergeTopicsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> MergeTopicsResult:
        service = uow.services.stories
        source = await service.get_topic(command.source_topic_id)
        if not source:
            raise EntityNotFoundError("topic", command.source_topic_id)
        await _ensure_board(uow.services, source.board_id, actor.actor_id)
        await _require_permissions(
            uow.services, actor.actor_id, source.board_id, "topic.entity.merge"
        )
        result = await service.merge_topics(
            command.source_topic_id, command.target_topic_id, actor.actor_id
        )
        if not result:
            raise EntityNotFoundError("topic", command.source_topic_id)
        await commit(uow)
        return MergeTopicsResult(result)


# ===========================================================================
# Stories
# ===========================================================================


# --- create story -----------------------------------------------------------


class CreateStoryCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class CreateStoryResult:
    __slots__ = ("story",)

    def __init__(self, story: Any) -> None:
        self.story = story


class CreateStoryUseCase:
    """Create a lightweight Story before Ideation (write). Board ownership +
    ``story.entity.create`` gate; ``ValueError`` (topic not in board → 400)
    propagates; a missing board (service returns ``None``) is
    ``EntityNotFoundError("board")`` (→ 404 "Board not found"). Re-fetches via
    ``get_story`` after commit so the relationships are loaded."""

    async def execute(
        self, command: CreateStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateStoryResult:
        await _ensure_board(uow.services, command.board_id, actor.actor_id)
        await _require_permissions(
            uow.services, actor.actor_id, command.board_id, "story.entity.create"
        )
        service = uow.services.stories
        story = await service.create_story(command.board_id, actor.actor_id, command.data)
        if not story:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return CreateStoryResult(await service.get_story(story.id))


# --- list stories -----------------------------------------------------------


class ListStoriesCommand:
    __slots__ = (
        "board_id",
        "status_filter",
        "topic_id",
        "search",
        "linked",
        "converted",
        "include_archived",
    )

    def __init__(
        self,
        board_id: str,
        *,
        status_filter: str | None = None,
        topic_id: str | None = None,
        search: str | None = None,
        linked: bool | None = None,
        converted: bool | None = None,
        include_archived: bool = False,
    ) -> None:
        self.board_id = board_id
        self.status_filter = status_filter
        self.topic_id = topic_id
        self.search = search
        self.linked = linked
        self.converted = converted
        self.include_archived = include_archived


class ListStoriesResult:
    __slots__ = ("stories",)

    def __init__(self, stories: list[Any]) -> None:
        self.stories = stories


class ListStoriesUseCase:
    """List a board's Stories (read, no commit). Board ownership +
    ``story.entity.read`` gate; ``ValueError`` (bad status filter → 400)
    propagates."""

    async def execute(
        self, command: ListStoriesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListStoriesResult:
        await _ensure_board(uow.services, command.board_id, actor.actor_id)
        await _require_permissions(
            uow.services, actor.actor_id, command.board_id, "story.entity.read"
        )
        stories = await uow.services.stories.list_stories(
            command.board_id,
            status_filter=command.status_filter,
            topic_id=command.topic_id,
            search=command.search,
            linked=command.linked,
            converted=command.converted,
            include_archived=command.include_archived,
        )
        return ListStoriesResult(stories)


# --- get story --------------------------------------------------------------


class GetStoryCommand:
    __slots__ = ("story_id",)

    def __init__(self, story_id: str) -> None:
        self.story_id = story_id


class GetStoryResult:
    __slots__ = ("story",)

    def __init__(self, story: Any) -> None:
        self.story = story


class GetStoryUseCase:
    """Get a Story with Topic + Ideation links (read, no commit). A missing story
    is ``EntityNotFoundError("story")`` (→ 404 "Story not found"); board ownership
    + ``story.entity.read`` gate follow, exactly as the legacy endpoint."""

    async def execute(
        self, command: GetStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetStoryResult:
        story = await uow.services.stories.get_story(command.story_id)
        if not story:
            raise EntityNotFoundError("story", command.story_id)
        await _ensure_board(uow.services, story.board_id, actor.actor_id)
        await _require_permissions(
            uow.services, actor.actor_id, story.board_id, "story.entity.read"
        )
        return GetStoryResult(story)


# --- update story -----------------------------------------------------------


class UpdateStoryCommand:
    __slots__ = ("story_id", "data")

    def __init__(self, story_id: str, data: Any) -> None:
        self.story_id = story_id
        self.data = data


class UpdateStoryResult:
    __slots__ = ("story",)

    def __init__(self, story: Any) -> None:
        self.story = story


class UpdateStoryUseCase:
    """Update a Story (write). A missing story is ``EntityNotFoundError("story")``;
    board ownership + the field-aware ``story_update_permissions`` gate with the
    ``story`` ``interact_in`` state apply; ``ValueError`` (archived / topic → 400)
    propagates. Re-fetches via ``get_story`` after commit."""

    async def execute(
        self, command: UpdateStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateStoryResult:
        from okto_pulse.core.services.story_permissions import (
            story_state,
            story_update_permissions,
        )

        service = uow.services.stories
        existing = await service.get_story(command.story_id)
        if not existing:
            raise EntityNotFoundError("story", command.story_id)
        await _ensure_board(uow.services, existing.board_id, actor.actor_id)
        update_data = command.data.model_dump(exclude_unset=True)
        await _require_permissions(
            uow.services,
            actor.actor_id,
            existing.board_id,
            story_update_permissions(update_data),
            entity="story",
            entity_status=story_state(existing.status, archived=bool(existing.archived)),
        )
        story = await service.update_story(command.story_id, actor.actor_id, command.data)
        if not story:
            raise EntityNotFoundError("story", command.story_id)
        await commit(uow)
        return UpdateStoryResult(await service.get_story(command.story_id))


# --- move story -------------------------------------------------------------


class MoveStoryCommand:
    __slots__ = ("story_id", "data")

    def __init__(self, story_id: str, data: Any) -> None:
        self.story_id = story_id
        self.data = data


class MoveStoryResult:
    __slots__ = ("story",)

    def __init__(self, story: Any) -> None:
        self.story = story


class MoveStoryUseCase:
    """Change a Story's lifecycle status (write). A missing story is
    ``EntityNotFoundError("story")``; board ownership + the transition-specific
    ``story_move_permission`` gate with the ``story`` ``interact_in`` state apply;
    ``ValueError`` (illegal transition / archived → 400) propagates. Re-fetches via
    ``get_story`` after commit."""

    async def execute(
        self, command: MoveStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> MoveStoryResult:
        from okto_pulse.core.services.story_permissions import (
            story_move_permission,
            story_state,
        )

        service = uow.services.stories
        existing = await service.get_story(command.story_id)
        if not existing:
            raise EntityNotFoundError("story", command.story_id)
        await _ensure_board(uow.services, existing.board_id, actor.actor_id)
        await _require_permissions(
            uow.services,
            actor.actor_id,
            existing.board_id,
            story_move_permission(existing.status, command.data.status),
            entity="story",
            entity_status=story_state(existing.status, archived=bool(existing.archived)),
        )
        story = await service.move_story(command.story_id, actor.actor_id, command.data)
        if not story:
            raise EntityNotFoundError("story", command.story_id)
        await commit(uow)
        return MoveStoryResult(await service.get_story(command.story_id))


# --- archive / restore story ------------------------------------------------


class ArchiveStoryCommand:
    __slots__ = ("story_id", "archived")

    def __init__(self, story_id: str, *, archived: bool) -> None:
        self.story_id = story_id
        self.archived = archived


class ArchiveStoryResult:
    __slots__ = ("story",)

    def __init__(self, story: Any) -> None:
        self.story = story


class ArchiveStoryUseCase:
    """Archive (``archived=True``) or restore (``archived=False``) a Story (write).
    A missing story is ``EntityNotFoundError("story")``; board ownership + the
    ``story.entity.archive`` / ``story.entity.restore`` gate (selected by the flag)
    with the ``story`` ``interact_in`` state apply. Re-fetches via ``get_story``
    after commit — drives both the DELETE (archive) and the /restore endpoints."""

    async def execute(
        self, command: ArchiveStoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ArchiveStoryResult:
        from okto_pulse.core.services.story_permissions import story_state

        service = uow.services.stories
        existing = await service.get_story(command.story_id)
        if not existing:
            raise EntityNotFoundError("story", command.story_id)
        await _ensure_board(uow.services, existing.board_id, actor.actor_id)
        permission = "story.entity.archive" if command.archived else "story.entity.restore"
        await _require_permissions(
            uow.services,
            actor.actor_id,
            existing.board_id,
            permission,
            entity="story",
            entity_status=story_state(existing.status, archived=bool(existing.archived)),
        )
        story = await service.archive_story(
            command.story_id, actor.actor_id, archived=command.archived
        )
        if not story:
            raise EntityNotFoundError("story", command.story_id)
        await commit(uow)
        return ArchiveStoryResult(await service.get_story(command.story_id))


# --- link story to ideation -------------------------------------------------


class LinkStoryToIdeationCommand:
    __slots__ = ("story_id", "ideation_id")

    def __init__(self, story_id: str, ideation_id: str) -> None:
        self.story_id = story_id
        self.ideation_id = ideation_id


class LinkStoryToIdeationResult:
    __slots__ = ("story",)

    def __init__(self, story: Any) -> None:
        self.story = story


class LinkStoryToIdeationUseCase:
    """Link a Story to an existing Ideation (write). A missing story OR a ``None``
    link result is ``EntityNotFoundError("story_or_ideation")`` (→ 404 "Story or
    Ideation not found"); board ownership + ``story.links.ideation`` gate with the
    ``story`` ``interact_in`` state apply; ``ValueError`` (non-editable ideation /
    already linked → 400) propagates. Re-fetches via ``get_story`` after commit."""

    async def execute(
        self, command: LinkStoryToIdeationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> LinkStoryToIdeationResult:
        from okto_pulse.core.services.story_permissions import story_state

        service = uow.services.stories
        story = await service.get_story(command.story_id)
        if not story:
            raise EntityNotFoundError("story_or_ideation", command.story_id)
        await _ensure_board(uow.services, story.board_id, actor.actor_id)
        await _require_permissions(
            uow.services,
            actor.actor_id,
            story.board_id,
            "story.links.ideation",
            entity="story",
            entity_status=story_state(story.status, archived=bool(story.archived)),
        )
        link = await service.link_story_to_ideation(
            command.story_id, command.ideation_id, actor.actor_id
        )
        if not link:
            raise EntityNotFoundError("story_or_ideation", command.story_id)
        await commit(uow)
        return LinkStoryToIdeationResult(await service.get_story(command.story_id))


# --- link many stories to ideation (contract-compatible) --------------------


class LinkStoriesToIdeationCommand:
    __slots__ = ("ideation_id", "story_ids")

    def __init__(self, ideation_id: str, story_ids: list[str]) -> None:
        self.ideation_id = ideation_id
        self.story_ids = story_ids


class LinkStoriesToIdeationResult:
    __slots__ = ("ideation_id", "story_ids")

    def __init__(self, ideation_id: str, story_ids: list[str]) -> None:
        self.ideation_id = ideation_id
        self.story_ids = story_ids


class LinkStoriesToIdeationUseCase:
    """Contract-compatible bulk link of Stories to an existing Ideation (write).
    Iterates the ``story_ids`` in order, applying the SAME per-story gate as the
    single-link endpoint (missing story / ``None`` link →
    ``EntityNotFoundError("story_or_ideation")``; board ownership;
    ``story.links.ideation`` with the ``story`` state; ``ValueError`` → 400). A
    single commit at the end, exactly as the legacy endpoint."""

    async def execute(
        self, command: LinkStoriesToIdeationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> LinkStoriesToIdeationResult:
        from okto_pulse.core.services.story_permissions import story_state

        service = uow.services.stories
        linked_story_ids: list[str] = []
        for story_id in command.story_ids:
            story = await service.get_story(story_id)
            if not story:
                raise EntityNotFoundError("story_or_ideation", story_id)
            await _ensure_board(uow.services, story.board_id, actor.actor_id)
            await _require_permissions(
                uow.services,
                actor.actor_id,
                story.board_id,
                "story.links.ideation",
                entity="story",
                entity_status=story_state(story.status, archived=bool(story.archived)),
            )
            link = await service.link_story_to_ideation(
                story_id, command.ideation_id, actor.actor_id
            )
            if not link:
                raise EntityNotFoundError("story_or_ideation", story_id)
            linked_story_ids.append(story_id)
        await commit(uow)
        return LinkStoriesToIdeationResult(command.ideation_id, linked_story_ids)


# --- convert stories to ideation --------------------------------------------


class ConvertStoriesCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class ConvertStoriesResult:
    __slots__ = ("ideation", "links", "propagated_mockups")

    def __init__(self, ideation: Any, links: list[Any], propagated_mockups: int) -> None:
        self.ideation = ideation
        self.links = links
        self.propagated_mockups = propagated_mockups


class ConvertStoriesUseCase:
    """Create or link an Ideation from selected Stories (write). Board ownership +
    the board-level ``story.conversion.to_ideation`` gate, then a per-story
    ``story.conversion.to_ideation`` check (with the ``story`` ``interact_in``
    state) for every selected story that exists in this board — exactly as the
    legacy endpoint, BEFORE the conversion. ``ValueError`` (not-found / not-ready
    → 400) propagates; a missing board (service returns ``None``) is
    ``EntityNotFoundError("board")``. Returns the unpacked
    ``(ideation, links, propagated)`` for the adapter envelope."""

    async def execute(
        self, command: ConvertStoriesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ConvertStoriesResult:
        from okto_pulse.core.services.story_permissions import story_state

        query_scope = _query_scope_for_actor(actor, board_id=command.board_id)
        await _ensure_board(
            uow.services,
            command.board_id,
            actor.actor_id,
            query_scope=query_scope,
        )
        await _require_permissions(
            uow.services, actor.actor_id, command.board_id, "story.conversion.to_ideation"
        )
        service = uow.services.stories
        for story_id in command.data.story_ids:
            story = await service.get_story(story_id)
            if story and story.board_id == command.board_id:
                await _require_permissions(
                    uow.services,
                    actor.actor_id,
                    command.board_id,
                    "story.conversion.to_ideation",
                    entity="story",
                    entity_status=story_state(story.status, archived=bool(story.archived)),
                )
        result = await service.convert_stories(
            command.board_id,
            actor.actor_id,
            command.data,
            query_scope=query_scope,
        )
        if not result:
            raise EntityNotFoundError("board", command.board_id)
        ideation, links, propagated = result
        await commit(uow)
        return ConvertStoriesResult(ideation, links, propagated)
