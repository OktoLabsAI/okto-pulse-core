"""Discovery REST use cases (SaaS Refactor spec R01A REST-FU7-S4).

Transport-free reimplementations of the five read/execute ``api/discovery.py``
endpoints that the legacy router drove directly off the request session — the
intent catalog, board selector options, saved searches, per-user search history
and intent execution. The inline SQL moves to ``DiscoveryCatalogReader`` and the
selector board/spec read policy moves to ``DiscoverySelectorRestAccessPolicy``
(both in ``services/discovery_catalog_reader``); each use case only assembles the
transport envelope (lookup -> validation / not-found -> delegate) so this layer
never touches ``select``/``AsyncSession``/ORM models (the relational ratchet
gate).

Reads do NOT commit. ``ExecuteDiscoveryIntentUseCase`` mirrors the legacy
endpoint EXACTLY — it did not commit either (any persistence inside
``execute_intent`` is owned by that service), and its
``DiscoverySelectorExecutionError`` / ``ValueError`` propagate uncaught for the
adapter to map (status_code+code / 400). Missing-or-inactive intent →
``EntityNotFoundError("intent")``; missing ``board_id`` →
``CommandValidationError``. The selector use case re-raises
``DiscoverySelectorAccessDenied`` for a denied board read so the adapter maps the
SAME 403 + ``selector_access_denied`` reason whether the denial is the explicit
pre-check or surfaced from inside the catalog — exactly as before.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    session_of,
)


# --- list active intents (read) ---------------------------------------------


class ListDiscoveryIntentsCommand:
    __slots__ = ()


class ListDiscoveryIntentsResult:
    __slots__ = ("intents",)

    def __init__(self, intents: list[Any]) -> None:
        self.intents = intents


class ListDiscoveryIntentsUseCase:
    """Return the active catalog of user-facing Discovery intents (read, no
    commit)."""

    async def execute(
        self, command: ListDiscoveryIntentsCommand, *, actor: ActorContext, uow: Any
    ) -> ListDiscoveryIntentsResult:
        from okto_pulse.core.services.discovery_catalog_reader import (
            DiscoveryCatalogReader,
        )

        intents = await DiscoveryCatalogReader(session_of(uow)).list_active_intents()
        return ListDiscoveryIntentsResult(intents)


# --- selector options (read) ------------------------------------------------


class ListDiscoverySelectorOptionsCommand:
    __slots__ = (
        "board_id",
        "selector_kind",
        "spec_id",
        "child_type",
        "status_filter",
        "q",
        "limit",
        "offset",
        "include_superseded",
    )

    def __init__(
        self,
        board_id: str,
        *,
        selector_kind: str = "spec",
        spec_id: str | None = None,
        child_type: str | None = None,
        status_filter: str | None = "active",
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
        include_superseded: bool = False,
    ) -> None:
        self.board_id = board_id
        self.selector_kind = selector_kind
        self.spec_id = spec_id
        self.child_type = child_type
        self.status_filter = status_filter
        self.q = q
        self.limit = limit
        self.offset = offset
        self.include_superseded = include_superseded


class ListDiscoverySelectorOptionsResult:
    __slots__ = ("payload", "cache_status")

    def __init__(self, payload: dict[str, Any], cache_status: str) -> None:
        self.payload = payload
        self.cache_status = cache_status


class ListDiscoverySelectorOptionsUseCase:
    """Return metadata-only Discovery selector options for a board (read, no
    commit). Performs the EXPLICIT board read check before catalog projection so
    a forbidden response never reveals whether requested specs/children exist —
    on denial it raises ``DiscoverySelectorAccessDenied`` (adapter → 403, reason
    ``selector_access_denied``), unifying the pre-check with the catalog's own
    access denial exactly as the legacy endpoint observed it. The catalog's
    ``DiscoverySelectorInvalidRequest`` / ``DiscoverySelectorSpecNotFound`` /
    ``DiscoverySelectorUnsafeProjection`` propagate for the adapter to map
    (400/404/500). ``cache_status`` is surfaced for the adapter's metric."""

    async def execute(
        self,
        command: ListDiscoverySelectorOptionsCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> ListDiscoverySelectorOptionsResult:
        from okto_pulse.core.services.discovery_catalog_reader import (
            DiscoverySelectorRestAccessPolicy,
        )
        from okto_pulse.core.services.discovery_selector_catalog import (
            DiscoverySelectorAccessDenied,
            DiscoverySelectorCatalog,
            get_default_discovery_selector_cache,
        )

        session = session_of(uow)
        policy = DiscoverySelectorRestAccessPolicy()
        if not await policy.can_read_board(session, actor.actor_id, command.board_id):
            raise DiscoverySelectorAccessDenied("selector_access_denied")

        catalog = DiscoverySelectorCatalog(
            policy,
            cache=get_default_discovery_selector_cache(),
        )
        result = await catalog.list_options(
            session,
            board_id=command.board_id,
            selector_kind=command.selector_kind,  # type: ignore[arg-type]
            identity=actor.actor_id,
            spec_id=command.spec_id,
            child_type=command.child_type,
            status=command.status_filter,
            q=command.q,
            limit=command.limit,
            offset=command.offset,
            include_superseded=command.include_superseded,
        )
        return ListDiscoverySelectorOptionsResult(result.to_dict(), result.cache_status)


# --- saved searches (read) --------------------------------------------------


class ListDiscoverySavedSearchesCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class ListDiscoverySavedSearchesResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class ListDiscoverySavedSearchesUseCase:
    """Return the saved searches for a board, newest first (read, no commit).
    Mirrors the legacy endpoint: no existence/permission check beyond auth — an
    unknown board simply yields an empty list."""

    async def execute(
        self,
        command: ListDiscoverySavedSearchesCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> ListDiscoverySavedSearchesResult:
        from okto_pulse.core.services.discovery_catalog_reader import (
            DiscoveryCatalogReader,
        )

        items = await DiscoveryCatalogReader(session_of(uow)).list_saved_searches(
            command.board_id
        )
        return ListDiscoverySavedSearchesResult(items)


# --- search history (read) --------------------------------------------------


class ListDiscoverySearchHistoryCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class ListDiscoverySearchHistoryResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class ListDiscoverySearchHistoryUseCase:
    """Return the current user's last 50 search entries on a board (read, no
    commit). The user is the actor; the board comes from the path. Mirrors the
    legacy endpoint exactly (no existence check)."""

    async def execute(
        self,
        command: ListDiscoverySearchHistoryCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> ListDiscoverySearchHistoryResult:
        from okto_pulse.core.services.discovery_catalog_reader import (
            DiscoveryCatalogReader,
        )

        items = await DiscoveryCatalogReader(session_of(uow)).list_search_history(
            command.board_id, actor.actor_id
        )
        return ListDiscoverySearchHistoryResult(items)


# --- execute intent (delegates to discovery_executor) -----------------------


class ExecuteDiscoveryIntentCommand:
    __slots__ = ("intent_id", "board_id", "params")

    def __init__(
        self, intent_id: str, board_id: str | None, params: dict[str, Any] | None = None
    ) -> None:
        self.intent_id = intent_id
        self.board_id = board_id
        self.params = params


class ExecuteDiscoveryIntentResult:
    __slots__ = ("payload",)

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class ExecuteDiscoveryIntentUseCase:
    """Execute the real tool bound to an intent and return a normalized payload
    (no commit — exactly as the legacy endpoint, whose persistence is owned by
    ``execute_intent``). Missing ``board_id`` → ``CommandValidationError`` (→ 400
    "board_id is required"); an unknown or inactive intent →
    ``EntityNotFoundError("intent")`` (→ 404 "Intent not found"). The
    ``DiscoverySelectorExecutionError`` (carrying ``status_code``/``code``) and
    ``ValueError`` (missing param / unknown binding → 400) raised by
    ``execute_intent`` propagate for the adapter to map. ``intent_id`` and
    ``intent_name`` are appended to the result, exactly as before."""

    async def execute(
        self,
        command: ExecuteDiscoveryIntentCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> ExecuteDiscoveryIntentResult:
        from okto_pulse.core.services.discovery_catalog_reader import (
            DiscoveryCatalogReader,
        )
        from okto_pulse.core.services.discovery_executor import execute_intent

        if not command.board_id:
            raise CommandValidationError("board_id is required")

        session = session_of(uow)
        intent = await DiscoveryCatalogReader(session).get_intent(command.intent_id)
        if intent is None or not intent.active:
            raise EntityNotFoundError("intent", command.intent_id)

        result = await execute_intent(
            session, actor.actor_id, command.board_id, intent, command.params or {}
        )
        result["intent_id"] = intent.id
        result["intent_name"] = intent.name
        return ExecuteDiscoveryIntentResult(result)
