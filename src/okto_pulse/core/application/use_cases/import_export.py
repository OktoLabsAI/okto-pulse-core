"""Import/export use cases for the admin catalog families (v0.3.0 ITEM 19).

Transport-free use cases behind the REST endpoints
``GET .../export`` / ``POST .../import`` for the four exportable families:

* **guidelines** — the actor's global catalog + (optionally) a board's inline
  guidelines, scope preserved;
* **design_systems** — the global Design System catalog (or a single entry,
  any scope);
* **presets** — permission presets visible to the actor (built-ins + own
  custom presets);
* **board_config** — DefaultBoardConfiguration versions of a scope, the
  active one marked with ``is_active``.

Canonical envelope (schema_version "1")::

    {"schema_version": "1", "kind": "<family>", "exported_at": "<iso8601>",
     "items": [...]}

Item fields mirror the existing detail responses MINUS the server-generated
fields (ids, owner, timestamps, sequence versions) so the import can recreate
every item through the NORMAL creation path (service/gateway used by the
existing POST endpoints — no parallel write path).

Conflict policy (documented per family on each import use case):

* guidelines — natural key = title (case-insensitive) within the partition
  (actor's global catalog, or the target board's inline set) →
  ``skipped`` with a duplicate reason instead of duplicating;
* design_systems — natural key = title within the catalog partition
  ``(scope, board_id)`` → skipped duplicate;
* presets — natural key = preset name among the presets visible to the actor
  (built-ins + own custom) → skipped duplicate;
* board_config — versions are an append-only history with no natural key:
  every item becomes a NEW version; the item marked active is activated.

All-or-nothing: an invalid item raises :class:`ImportItemError`; the adapter
maps it to a 400 WITHOUT committing, so the request mutates nothing.
``dry_run`` executes the exact same path and then rolls the session back
(the REST ``get_db`` dependency commits at request end, so a plain
"skip commit" would still leak the staged writes).
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from pydantic import ValidationError

from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)

ENVELOPE_SCHEMA_VERSION = "1"

KIND_GUIDELINES = "guidelines"
KIND_DESIGN_SYSTEMS = "design_systems"
KIND_PRESETS = "presets"
KIND_BOARD_CONFIG = "board_config"

# Export reads are unpaginated by design (admin backup surface); the ceiling
# only guards against a pathological catalog.
_EXPORT_LIST_LIMIT = 10_000


# ---------------------------------------------------------------------------
# Envelope helpers (shared by the four families)
# ---------------------------------------------------------------------------


class EnvelopeError(Exception):
    """The import body is not a valid schema_version-1 envelope for the family."""


class ImportItemError(Exception):
    """A single item failed creation-path validation (all-or-nothing per request)."""

    def __init__(self, index: int, detail: Any) -> None:
        self.index = index
        self.detail = detail
        super().__init__(str(detail))


def build_envelope(kind: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "kind": kind,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }


def parse_import_envelope(raw: Any, *, kind: str) -> list[dict[str, Any]]:
    """Validate the canonical envelope and return its raw items."""
    if not isinstance(raw, dict):
        raise EnvelopeError("Import body must be a JSON object (envelope).")
    schema_version = raw.get("schema_version")
    if str(schema_version) != ENVELOPE_SCHEMA_VERSION:
        raise EnvelopeError(
            f"Unsupported schema_version '{schema_version}' "
            f"(expected '{ENVELOPE_SCHEMA_VERSION}')."
        )
    if raw.get("kind") != kind:
        raise EnvelopeError(
            f"Envelope kind '{raw.get('kind')}' does not match this endpoint "
            f"(expected '{kind}')."
        )
    items = raw.get("items")
    if not isinstance(items, list):
        raise EnvelopeError("Envelope 'items' must be a list.")
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise EnvelopeError(f"items[{index}] must be a JSON object.")
    return items


def validate_items(
    items: list[dict[str, Any]], validator: Callable[[dict[str, Any]], Any]
) -> tuple[list[Any], list[dict[str, Any]]]:
    """Run every raw item through the family's EXISTING creation validator.

    Returns ``(parsed, errors)`` where ``errors`` is ``[{index, detail}, ...]``
    — the adapter turns a non-empty error list into a 400 before any mutation.
    """
    parsed: list[Any] = []
    errors: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        try:
            parsed.append(validator(item))
        except ValidationError as exc:
            errors.append({"index": index, "detail": exc.errors(include_url=False)})
        except ImportItemError as exc:
            errors.append({"index": exc.index if exc.index >= 0 else index, "detail": exc.detail})
    return parsed, errors


@dataclass
class ImportResult:
    created: int = 0
    skipped: list[dict[str, Any]] = field(default_factory=list)

    def payload(self, *, dry_run: bool) -> dict[str, Any]:
        return {
            "created": self.created,
            "skipped": self.skipped,
            "errors": [],
            "dry_run": dry_run,
        }


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


async def _finalize(uow: PulseUnitOfWork, *, dry_run: bool) -> None:
    """Commit a real import; roll a dry-run back explicitly (the request-scoped
    ``get_db`` session commits at request end, so staged writes must not survive)."""
    if dry_run:
        await uow.rollback()
    else:
        await commit(uow)


def _norm_key(value: str | None) -> str:
    return (value or "").strip().lower()


# ===========================================================================
# Guidelines
# ===========================================================================


def _guideline_export_item(
    *, title: Any, content: Any, tags: Any, scope: Any, board_id: Any
) -> dict[str, Any]:
    """GuidelineResponse fields minus id / owner_id / created_at / updated_at."""
    return {
        "title": title,
        "content": content,
        "tags": list(tags) if tags else None,
        "scope": scope,
        "board_id": board_id,
    }


@dataclass(frozen=True)
class ExportGuidelinesCommand:
    board_id: str | None = None


class ExportGuidelinesUseCase:
    """Export the actor's global guidelines + (if ``board_id``) the board's
    inline guidelines, scope preserved (read, no commit)."""

    async def execute(
        self, command: ExportGuidelinesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> dict[str, Any]:

        service = uow.services.guidelines
        global_scope = _query_scope_for_actor(actor)
        globals_ = await service.list_guidelines(
            actor.actor_id,
            offset=0,
            limit=_EXPORT_LIST_LIMIT,
            query_scope=global_scope,
        )
        items = [
            _guideline_export_item(
                title=g.title,
                content=g.content,
                tags=g.tags,
                scope=g.scope,
                board_id=g.board_id,
            )
            for g in globals_
        ]
        if command.board_id:
            board_scope = _query_scope_for_actor(actor, board_id=command.board_id)
            board = await uow.services.boards.get_board(
                command.board_id, actor.actor_id, query_scope=board_scope
            )
            if not board:
                raise EntityNotFoundError("board", command.board_id)
            entries = await service.get_board_guidelines(
                command.board_id, surface="menu_board", query_scope=board_scope
            )
            for entry in entries:
                guideline = entry.get("guideline") or {}
                if guideline.get("scope") == "inline":
                    items.append(
                        _guideline_export_item(
                            title=guideline.get("title"),
                            content=guideline.get("content"),
                            tags=guideline.get("tags"),
                            scope="inline",
                            board_id=command.board_id,
                        )
                    )
        return build_envelope(KIND_GUIDELINES, items)


@dataclass(frozen=True)
class ImportGuidelinesCommand:
    items: list[Any]  # GuidelineCreate-shaped (title/content/tags/scope/board_id)
    board_id: str | None = None  # optional target-board remap for inline items
    dry_run: bool = False


class ImportGuidelinesUseCase:
    """Recreate guidelines through ``GuidelineService.create_guideline``.

    Conflict policy — natural key = title (case-insensitive) within the
    partition: an identical global title in the actor's catalog, or an
    identical inline title on the target board, is reported as skipped
    (``duplicate_*_title``) instead of duplicated. Inline items require a
    board: the request-level ``board_id`` override wins over the item's own
    ``board_id`` (so an export of board A can be imported into board B).
    """

    async def execute(
        self, command: ImportGuidelinesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ImportResult:
        from okto_pulse.core.services.application_schemas import GuidelineCreate

        service = uow.services.guidelines
        result = ImportResult()

        global_scope = _query_scope_for_actor(actor)
        existing_globals = {
            _norm_key(g.title)
            for g in await service.list_guidelines(
                actor.actor_id,
                offset=0,
                limit=_EXPORT_LIST_LIMIT,
                query_scope=global_scope,
            )
        }
        inline_titles: dict[str, set[str]] = {}

        async def _inline_titles_for(board_id: str) -> set[str]:
            if board_id not in inline_titles:
                board_scope = _query_scope_for_actor(actor, board_id=board_id)
                board = await uow.services.boards.get_board(
                    board_id, actor.actor_id, query_scope=board_scope
                )
                if not board:
                    raise EntityNotFoundError("board", board_id)
                entries = await service.get_board_guidelines(
                    board_id, surface="menu_board", query_scope=board_scope
                )
                inline_titles[board_id] = {
                    _norm_key((entry.get("guideline") or {}).get("title"))
                    for entry in entries
                    if (entry.get("guideline") or {}).get("scope") == "inline"
                }
            return inline_titles[board_id]

        for index, item in enumerate(command.items):
            scope = getattr(item, "scope", None) or "global"
            title_key = _norm_key(getattr(item, "title", None))
            if scope == "global":
                if title_key in existing_globals:
                    result.skipped.append(
                        {
                            "index": index,
                            "title": item.title,
                            "reason": "duplicate_global_title",
                        }
                    )
                    continue
                await service.create_guideline(
                    actor.actor_id,
                    GuidelineCreate(
                        title=item.title,
                        content=item.content,
                        tags=item.tags,
                        scope="global",
                        board_id=None,
                    ),
                    query_scope=global_scope,
                )
                existing_globals.add(title_key)
                result.created += 1
            elif scope == "inline":
                target_board = command.board_id or getattr(item, "board_id", None)
                if not target_board:
                    raise ImportItemError(
                        index,
                        "Inline guideline requires a board_id "
                        "(on the item or as the ?board_id= query parameter).",
                    )
                try:
                    titles = await _inline_titles_for(target_board)
                except EntityNotFoundError:
                    raise ImportItemError(index, f"Board '{target_board}' not found.")
                if title_key in titles:
                    result.skipped.append(
                        {
                            "index": index,
                            "title": item.title,
                            "reason": "duplicate_inline_title",
                        }
                    )
                    continue
                await service.create_guideline(
                    actor.actor_id,
                    GuidelineCreate(
                        title=item.title,
                        content=item.content,
                        tags=item.tags,
                        scope="inline",
                        board_id=target_board,
                    ),
                    query_scope=_query_scope_for_actor(actor, board_id=target_board),
                )
                titles.add(title_key)
                result.created += 1
            else:
                raise ImportItemError(index, f"Unsupported guideline scope '{scope}'.")

        await _finalize(uow, dry_run=command.dry_run)
        return result


# ===========================================================================
# Design Systems
# ===========================================================================


def _design_system_export_item(serialized: dict[str, Any]) -> dict[str, Any]:
    """serialize_design_system fields minus id / version / owner_id / timestamps."""
    return {
        "title": serialized["title"],
        "scope": serialized["scope"],
        "board_id": serialized["board_id"],
        "payload": serialized["payload"],
        "status": serialized["status"],
    }


@dataclass(frozen=True)
class ExportDesignSystemsCommand:
    design_system_id: str | None = None


class ExportDesignSystemsUseCase:
    """Export the global Design System catalog, or a single entry by id (any
    scope). Raises ``DesignSystemError`` (404) for an unknown id — the adapter
    maps it exactly like the existing GET detail endpoint."""

    async def execute(
        self, command: ExportDesignSystemsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> dict[str, Any]:
        from okto_pulse.core.services.design_system import (
            serialize_design_system,
        )

        service = uow.services.design_systems
        if command.design_system_id:
            serialized = [
                serialize_design_system(
                    await service.require_design_system(command.design_system_id)
                )
            ]
        else:
            serialized = [
                serialize_design_system(item)
                for item in await service.list_catalog(scope="global")
            ]
        return build_envelope(
            KIND_DESIGN_SYSTEMS,
            [_design_system_export_item(item) for item in serialized],
        )


@dataclass(frozen=True)
class ImportDesignSystemsCommand:
    items: list[dict[str, Any]]  # CreateDesignSystemRequest-shaped dicts
    dry_run: bool = False


class ImportDesignSystemsUseCase:
    """Recreate Design Systems through ``DesignSystemService.create_design_system``
    (version restarts at 1, exactly like the normal POST).

    Conflict policy — natural key = title (case-insensitive) within the catalog
    partition ``(scope, board_id)``: a matching title is reported as skipped
    (``duplicate_title``). Service-level validation failures (invalid scope /
    status / missing title / inline without board) raise ``ImportItemError``.
    """

    async def execute(
        self, command: ImportDesignSystemsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ImportResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemError,
        )

        service = uow.services.design_systems
        result = ImportResult()
        existing: dict[tuple[str, str | None], set[str]] = {}

        async def _titles_for(scope: str, board_id: str | None) -> set[str]:
            partition = (scope, board_id)
            if partition not in existing:
                catalog = await service.list_catalog(scope=scope, board_id=board_id)
                existing[partition] = {_norm_key(item.title) for item in catalog}
            return existing[partition]

        for index, item in enumerate(command.items):
            scope = item.get("scope") or "global"
            if scope not in ("global", "inline"):
                raise ImportItemError(index, f"Unsupported design system scope '{scope}'.")
            board_id = item.get("board_id") if scope == "inline" else None
            title_key = _norm_key(item.get("title"))
            titles = await _titles_for(scope, board_id)
            if title_key and title_key in titles:
                result.skipped.append(
                    {"index": index, "title": item.get("title"), "reason": "duplicate_title"}
                )
                continue
            try:
                await service.create_design_system(
                    actor.actor_id,
                    title=item.get("title") or "",
                    scope=scope,
                    board_id=item.get("board_id"),
                    payload=item.get("payload"),
                    status=item.get("status") or "active",
                )
            except DesignSystemError as exc:
                raise ImportItemError(index, exc.to_dict()) from exc
            titles.add(title_key)
            result.created += 1

        await _finalize(uow, dry_run=command.dry_run)
        return result


# ===========================================================================
# Permission presets
# ===========================================================================


class ExportPresetsCommand:
    __slots__ = ()


class ExportPresetsUseCase:
    """Export every preset visible to the actor (built-ins + own custom).
    ``is_builtin`` is informational — the import path always creates custom
    presets (built-in-ness is not re-creatable via the creation path)."""

    async def execute(
        self, command: ExportPresetsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> dict[str, Any]:
        gateway = uow.services.permission_presets
        presets = await gateway.list_presets(user_id=actor.actor_id)
        items = [
            {
                "name": preset.name,
                "description": preset.description,
                "flags": preset.flags,
                "is_builtin": preset.is_builtin,
            }
            for preset in presets
        ]
        return build_envelope(KIND_PRESETS, items)


@dataclass(frozen=True)
class ImportPresetsCommand:
    items: list[Any]  # PresetCreate-shaped (name/description/flags)
    dry_run: bool = False


class ImportPresetsUseCase:
    """Recreate presets through the permission-preset gateway's ``create_preset``
    (same path as POST /presets — always a NEW custom preset owned by the actor).

    Conflict policy — natural key = preset name (case-insensitive) among the
    presets the actor can already see (built-ins + own custom): matching names
    are reported as skipped (``duplicate_name``).
    """

    async def execute(
        self, command: ImportPresetsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ImportResult:
        gateway = uow.services.permission_presets
        result = ImportResult()
        existing = {
            _norm_key(preset.name)
            for preset in await gateway.list_presets(user_id=actor.actor_id)
        }
        for index, item in enumerate(command.items):
            name_key = _norm_key(getattr(item, "name", None))
            if name_key in existing:
                result.skipped.append(
                    {"index": index, "name": item.name, "reason": "duplicate_name"}
                )
                continue
            await gateway.create_preset(
                user_id=actor.actor_id,
                name=item.name,
                description=item.description,
                flags=item.flags,
            )
            existing.add(name_key)
            result.created += 1

        await _finalize(uow, dry_run=command.dry_run)
        return result


# ===========================================================================
# Default board configuration (board_config)
# ===========================================================================


@dataclass(frozen=True)
class ExportBoardConfigCommand:
    scope: str = "global"


class ExportBoardConfigUseCase:
    """Export every DefaultBoardConfiguration version of a scope, oldest first,
    with the active one marked ``is_active`` (read, no commit). Item fields =
    the version-list serialization minus id / version / status / created_by /
    timestamps (all server-managed)."""

    async def execute(
        self, command: ExportBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> dict[str, Any]:

        data = await uow.services.default_board_config.list_versions(
            scope=command.scope
        )
        versions = sorted(
            data.get("versions") or [], key=lambda item: item.get("version") or 0
        )
        items = [
            {
                "scope": version.get("scope"),
                "settings_payload": dict(version.get("settings_payload") or {}),
                "guideline_default_refs": list(version.get("guideline_default_refs") or []),
                "design_system_default_ref": version.get("design_system_default_ref"),
                "is_active": bool(version.get("is_active")),
            }
            for version in versions
        ]
        return build_envelope(KIND_BOARD_CONFIG, items)


@dataclass(frozen=True)
class ImportBoardConfigCommand:
    items: list[dict[str, Any]]  # create_version kwargs (activate already mapped)
    dry_run: bool = False


class ImportBoardConfigUseCase:
    """Recreate template versions through
    ``DefaultBoardConfigApiService.create_version`` (the normal creation path,
    including BoardSettings validation and guideline/design-system default-ref
    validation on activate).

    Conflict policy — NO natural key: DefaultBoardConfiguration versions are an
    append-only history, so every item is created as a NEW version (fresh ids,
    next sequence numbers). The item exported with ``is_active`` is imported
    with ``activate=True`` and becomes the active template (single-active is
    enforced by the service). Validation failures raise ``ImportItemError``.
    """

    async def execute(
        self, command: ImportBoardConfigCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ImportResult:
        from okto_pulse.core.services.default_board_configuration import (
            DefaultBoardConfigurationError,
        )

        service = uow.services.default_board_config
        result = ImportResult()
        query_scope = _query_scope_for_actor(actor)
        for index, item in enumerate(command.items):
            try:
                await service.create_version(
                    actor=actor.actor_id,
                    settings_payload=item.get("settings_payload"),
                    scope=item.get("scope") or "global",
                    guideline_default_refs=item.get("guideline_default_refs") or None,
                    design_system_default_ref=item.get("design_system_default_ref"),
                    activate=bool(item.get("activate")),
                    query_scope=query_scope,
                )
            except DefaultBoardConfigurationError as exc:
                raise ImportItemError(index, exc.to_dict()) from exc
            result.created += 1

        await _finalize(uow, dry_run=command.dry_run)
        return result
