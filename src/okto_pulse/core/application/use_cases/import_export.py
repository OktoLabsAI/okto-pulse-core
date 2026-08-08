"""Import/export use cases for the admin catalog families (v0.3.0 ITEM 19).

Transport-free use cases behind the REST endpoints
``GET .../export`` / ``POST .../import`` for the three generic catalog families:

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

Guidelines no longer use this schema-v1 surface.  The deprecated command
classes below fail closed with ``guideline_export_v3_required`` after
authorization; callers must use ``guideline_import_export`` so semantic
metrics and governed binding history cannot be silently discarded.

Conflict policy (documented per supported family on each import use case):

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
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_any_authority,
    require_authorization,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    REVISIONS_CREATE,
    REVISIONS_READ,
    require_policy_governance_capabilities,
)
from okto_pulse.core.services.default_board_configuration import (
    guideline_ref_diff_has_changes,
)

ENVELOPE_SCHEMA_VERSION = "1"

KIND_DESIGN_SYSTEMS = "design_systems"
KIND_PRESETS = "presets"
KIND_BOARD_CONFIG = "board_config"


_BOARD_WRITE_SHARE_PERMISSIONS = {"editor", "admin"}


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


class GuidelineExportV3Required(ValueError):
    """The lossy schema-v1 guideline catalog surface has been retired."""

    def __init__(self) -> None:
        super().__init__("guideline_export_v3_required")


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
    updated: int = 0
    replaced: int = 0
    skipped: list[dict[str, Any]] = field(default_factory=list)

    def payload(self, *, dry_run: bool) -> dict[str, Any]:
        payload = {
            "created": self.created,
            "skipped": self.skipped,
            "errors": [],
            "dry_run": dry_run,
        }
        if self.updated:
            payload["updated"] = self.updated
        if self.replaced:
            payload["replaced"] = self.replaced
        return payload


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


def _shared_board_query_scope(actor: ActorContext, board_id: str) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(
        target_board_id=board_id,
        allowed_board_ids={board_id},
        require_ownership=False,
    )


async def _require_board_access(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    write: bool,
) -> QueryScope:
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=(
            _BOARD_WRITE_SHARE_PERMISSIONS if write else None
        ),
    )
    if board is None:
        raise EntityNotFoundError("board", board_id)
    return _shared_board_query_scope(actor, board_id)


async def _finalize(uow: PulseUnitOfWork, *, dry_run: bool) -> None:
    """Commit a real import; roll a dry-run back explicitly (the request-scoped
    ``get_db`` session commits at request end, so staged writes must not survive)."""
    if dry_run:
        await uow.rollback()
    else:
        await commit(uow)


def _norm_key(value: str | None) -> str:
    return (value or "").strip().lower()


def _item_value(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


# ===========================================================================
# Retired generic guideline surface
# ===========================================================================


@dataclass(frozen=True)
class ExportGuidelinesCommand:
    board_id: str | None = None


class ExportGuidelinesUseCase:
    """Deprecated fail-closed shim for the governed lossless V3 exporter."""

    async def execute(
        self, command: ExportGuidelinesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> dict[str, Any]:
        del command, uow
        require_policy_governance_capabilities(actor, REVISIONS_READ)
        raise GuidelineExportV3Required()


@dataclass(frozen=True)
class ImportGuidelinesCommand:
    items: list[Any]  # GuidelineCreate-shaped (title/content/tags/scope/board_id)
    board_id: str | None = None  # optional target-board remap for inline items
    dry_run: bool = False


class ImportGuidelinesUseCase:
    """Deprecated fail-closed shim for the governed atomic V3 importer."""

    async def execute(
        self, command: ImportGuidelinesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ImportResult:
        # Repeat authorization before the fail-closed response so callers
        # cannot use deprecation behavior to probe a protected capability.
        require_policy_governance_capabilities(actor, REVISIONS_CREATE)
        del command, uow
        raise GuidelineExportV3Required()


# ===========================================================================
# Design Systems
# ===========================================================================


def _design_system_export_item(serialized: dict[str, Any]) -> dict[str, Any]:
    """Portable Design System identity plus its complete domain payload."""
    return {
        "id": serialized["id"],
        "version": serialized["version"],
        "title": serialized["title"],
        "scope": serialized["scope"],
        "board_id": serialized["board_id"],
        "payload": serialized["payload"],
        "status": serialized["status"],
    }


@dataclass(frozen=True)
class ExportDesignSystemsCommand:
    design_system_id: str | None = None
    board_id: str | None = None


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
            board_authorized = bool(command.board_id)
            if board_authorized:
                try:
                    await _require_board_access(
                        uow,
                        command.board_id or "",
                        actor,
                        write=False,
                    )
                except EntityNotFoundError:
                    from okto_pulse.core.services.design_system import (
                        DesignSystemError,
                    )

                    raise DesignSystemError(
                        "design_system_not_found",
                        f"Design System '{command.design_system_id}' was not found.",
                        404,
                        {"design_system_id": command.design_system_id},
                    ) from None
            await require_authorization(
                actor,
                PermissionRequirement(
                    "design_system.export",
                    legacy_operation="spec.architecture.read",
                ),
                uow=uow,
                board_id=command.board_id,
            )
            serialized = [
                serialize_design_system(
                    await service.require_authorized_design_system(
                        command.design_system_id,
                        actor.actor_id,
                        board_id=command.board_id,
                        board_access_authorized=board_authorized,
                    )
                )
            ]
        else:
            await require_authorization(
                actor,
                PermissionRequirement(
                    "design_system.export",
                    legacy_operation="spec.architecture.read",
                ),
                uow=uow,
            )
            serialized = [
                serialize_design_system(item)
                for item in await service.list_catalog(
                    scope="global",
                    owner_id=actor.actor_id,
                )
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
    """Import complete Design Systems into the GLOBAL catalog.

    Matching stable ids create a new version. New ids are preserved. Imported
    ``scope`` and ``board_id`` values never create an inline Design System.
    """

    async def execute(
        self, command: ImportDesignSystemsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ImportResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemError,
        )

        service = uow.services.design_systems
        preflight_existing: list[Any | None] = []
        for index, item in enumerate(command.items):
            design_system_id = item.get("id")
            existing = (
                await service.get_design_system(design_system_id)
                if design_system_id
                else None
            )
            preflight_existing.append(existing)
            if existing is not None and existing.owner_id != actor.actor_id:
                error = DesignSystemError(
                    "design_system_not_found",
                    f"Design System '{design_system_id}' was not found.",
                    404,
                    {"design_system_id": design_system_id},
                )
                raise ImportItemError(index, error.to_dict()) from error

        await require_authorization(
            actor,
            PermissionRequirement(
                "design_system.import",
                legacy_operation="spec.architecture.import",
            ),
            uow=uow,
        )
        result = ImportResult()
        imported_by_id: dict[str, Any] = {}
        for index, item in enumerate(command.items):
            design_system_id = item.get("id")
            try:
                existing = (
                    imported_by_id.get(design_system_id)
                    if design_system_id
                    else None
                )
                if design_system_id and existing is None:
                    existing = preflight_existing[index]
                if existing is not None:
                    if existing.owner_id != actor.actor_id:
                        raise DesignSystemError(
                            "design_system_not_found",
                            f"Design System '{design_system_id}' was not found.",
                            404,
                            {"design_system_id": design_system_id},
                        )
                    await service.update_design_system(
                        design_system_id,
                        actor.actor_id,
                        allow_owned_global_without_link=True,
                        title=item.get("title") or "",
                        payload=item.get("payload"),
                        status=item.get("status") or "active",
                        force_version_bump=True,
                        normalize_global=True,
                        replace_fields=True,
                    )
                    result.updated += 1
                else:
                    created = await service.create_design_system(
                        actor.actor_id,
                        title=item.get("title") or "",
                        scope="global",
                        board_id=None,
                        payload=item.get("payload"),
                        status=item.get("status") or "active",
                        design_system_id=design_system_id,
                        version=int(item.get("version") or 1),
                    )
                    if design_system_id:
                        imported_by_id[design_system_id] = created
                    result.created += 1
            except DesignSystemError as exc:
                raise ImportItemError(index, exc.to_dict()) from exc

        await _finalize(uow, dry_run=command.dry_run)
        return result


# ===========================================================================
# Permission presets
# ===========================================================================


@dataclass(frozen=True)
class ExportPresetsCommand:
    preset_id: str | None = None


class ExportPresetsUseCase:
    """Export every preset visible to the actor (built-ins + own custom).
    ``is_builtin`` is informational — the import path always creates custom
    presets (built-in-ness is not re-creatable via the creation path)."""

    async def execute(
        self, command: ExportPresetsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> dict[str, Any]:
        await require_authorization(
            actor,
            PermissionRequirement(
                "permission_preset.export",
                legacy_operation="board.read",
            ),
            uow=uow,
        )
        gateway = uow.services.permission_presets
        presets = await gateway.list_presets(user_id=actor.actor_id)
        if command.preset_id:
            presets = [preset for preset in presets if preset.id == command.preset_id]
            if not presets:
                raise EntityNotFoundError("preset", command.preset_id)
        items = [
            {
                "id": preset.id,
                "name": preset.name,
                "description": preset.description,
                "flags": preset.flags,
                "is_builtin": preset.is_builtin,
                "base_preset_id": preset.base_preset_id,
            }
            for preset in presets
        ]
        return build_envelope(KIND_PRESETS, items)


@dataclass(frozen=True)
class ImportPresetsCommand:
    items: list[Any]  # PresetCreate-shaped (name/description/flags)
    dry_run: bool = False
    replace_existing: bool = False


class ImportPresetsUseCase:
    """Import presets with explicit confirmation for same-id replacement.

    Built-ins remain immutable. Legacy id-less envelopes retain name-based
    duplicate protection and create a fresh custom preset otherwise.
    """

    async def execute(
        self, command: ImportPresetsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ImportResult:
        gateway = uow.services.permission_presets
        visible = await gateway.list_presets(user_id=actor.actor_id)
        existing_by_id = {preset.id: preset for preset in visible}
        existing_names = {_norm_key(preset.name) for preset in visible}
        for index, item in enumerate(command.items):
            preset_id = _item_value(item, "id")
            existing = existing_by_id.get(preset_id) if preset_id else None
            if preset_id and existing is None and await gateway.get_preset(preset_id=preset_id):
                raise ImportItemError(
                    index,
                    {
                        "code": "preset_id_conflict",
                        "message": "The preset id already belongs to another catalog.",
                        "id": preset_id,
                    },
                )

        await require_authorization(
            actor,
            PermissionRequirement(
                "permission_preset.import",
                legacy_operation="profile.update",
            ),
            uow=uow,
        )
        result = ImportResult()
        for index, item in enumerate(command.items):
            preset_id = _item_value(item, "id")
            name = _item_value(item, "name")
            name_key = _norm_key(name)
            existing = existing_by_id.get(preset_id) if preset_id else None
            if existing is not None:
                if existing.is_builtin:
                    result.skipped.append(
                        {
                            "index": index,
                            "id": preset_id,
                            "name": name,
                            "reason": "builtin_not_replaceable",
                        }
                    )
                    continue
                if not command.replace_existing:
                    result.skipped.append(
                        {
                            "index": index,
                            "id": preset_id,
                            "name": name,
                            "reason": "replacement_requires_confirmation",
                        }
                    )
                    continue
                await gateway.update_preset(
                    preset_id=preset_id,
                    user_id=actor.actor_id,
                    name=name,
                    description=_item_value(item, "description"),
                    flags=_item_value(item, "flags"),
                    replace=True,
                )
                result.replaced += 1
                continue
            if not preset_id and name_key in existing_names:
                result.skipped.append(
                    {"index": index, "name": name, "reason": "duplicate_name"}
                )
                continue
            created = await gateway.create_preset(
                user_id=actor.actor_id,
                name=name,
                description=_item_value(item, "description", "") or "",
                flags=_item_value(item, "flags"),
                preset_id=preset_id,
            )
            if preset_id:
                existing_by_id[preset_id] = created
            existing_names.add(name_key)
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
        await require_authorization(
            actor,
            PermissionRequirement(
                "default_board_config.export",
                legacy_operation="board.read",
            ),
            uow=uow,
        )
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
                "spec_checklist_mode": version.get("spec_checklist_mode"),
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
        # Keep the import path aligned with every other global template writer.
        # This check deliberately precedes both validation reads and the first
        # staged create so denied callers have zero downstream mutations.
        from okto_pulse.core.services.default_board_configuration import (
            DefaultBoardConfigurationError,
        )

        await require_any_authority(
            actor,
            PermissionRequirement(
                "default_board_config.import",
                legacy_operation="spec.entity.edit_fields",
            ),
            roles=("admin", "operator"),
            uow=uow,
        )
        service = uow.services.default_board_config
        result = ImportResult()
        query_scope = _query_scope_for_actor(actor)
        # The envelope is atomic. Preflight every item's ref delta before the
        # first staged create so a later governed item cannot leave earlier
        # versions pending in adapters that expose eager writes.
        for item in command.items:
            diff = await service.preview_create_guideline_ref_diff(
                scope=str(item.get("scope") or "global"),
                guideline_default_refs=item.get("guideline_default_refs") or None,
                compatibility_import=True,
            )
            if guideline_ref_diff_has_changes(diff):
                await require_authorization(
                    actor,
                    PermissionRequirement(
                        "default_board_config.guidelines.edit",
                        legacy_operation="guidelines.adoption.manage",
                    ),
                    uow=uow,
                )
                break
        for index, item in enumerate(command.items):
            try:
                await service.create_version(
                    actor=actor.actor_id,
                    settings_payload=item.get("settings_payload"),
                    scope=item.get("scope") or "global",
                    guideline_default_refs=item.get("guideline_default_refs") or None,
                    design_system_default_ref=item.get("design_system_default_ref"),
                    spec_checklist_mode=item.get("spec_checklist_mode"),
                    activate=bool(item.get("activate")),
                    query_scope=query_scope,
                    compatibility_import=True,
                )
            except DefaultBoardConfigurationError as exc:
                raise ImportItemError(index, exc.to_dict()) from exc
            result.created += 1

        await _finalize(uow, dry_run=command.dry_run)
        return result
