"""Administrative orchestration for DefaultBoardConfiguration (spec 9df814bc /
card 7da43521, FR7).

``DefaultBoardConfigApiService`` is the single orchestrator shared by REST
(api/default_board_config.py) and the MCP twin tools so both surfaces return the
SAME structured payload + reason codes. It wraps the validated
``DefaultBoardConfigurationService`` (cards #1-#4); it never introduces a parallel
default/snapshot mechanism (TR10). Every failure is a structured
``DefaultBoardConfigurationError`` — never a raw exception leak (TR8).
"""

from __future__ import annotations

from typing import Any


from okto_pulse.core.application.scope import QueryScope
from okto_pulse.core.domain.configuration_presence import (
    project_configuration_presence,
)
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.services.amendment_revision_api import reject_bypass_fields
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)

__all__ = ["DefaultBoardConfigApiService", "reject_bypass_fields"]


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    iso = getattr(value, "isoformat", None)
    return iso() if callable(iso) else str(value)


class DefaultBoardConfigApiService:
    """Validate + orchestrate admin operations on default board configuration."""

    def __init__(self, db: object) -> None:
        self._db = db
        self._svc = DefaultBoardConfigurationService(db)

    # -- reads -------------------------------------------------------------

    async def get_active(self, *, scope: str = "global") -> dict[str, Any]:
        template = await self._svc.resolve_active(scope)
        if template is None:
            projection = project_configuration_presence(
                baseline_available=False,
                comparable=False,
            )
            return {
                "scope": scope,
                "presence": projection.state,
                "baseline_available": projection.baseline_available,
                "comparable": projection.comparable,
                "active": None,
            }
        raw_components = (
            template.settings_payload,
            template.guideline_default_refs,
            template.design_system_default_ref,
            template.spec_checklist_mode,
        )
        if all(component is None for component in raw_components):
            raw_configuration: Any = None
        elif not any(bool(component) for component in raw_components):
            raw_configuration = {}
        else:
            raw_configuration = {
                "settings_payload": template.settings_payload,
                "guideline_default_refs": template.guideline_default_refs,
                "design_system_default_ref": template.design_system_default_ref,
                "spec_checklist_mode": template.spec_checklist_mode,
            }
        projection = project_configuration_presence(raw_configuration)
        serialized = self._serialize(template)
        return {
            "scope": scope,
            "presence": projection.state,
            "baseline_available": projection.baseline_available,
            "comparable": projection.comparable,
            "active": serialized,
        }

    async def list_versions(self, *, scope: str = "global") -> dict[str, Any]:
        versions = await self._svc.list_versions(scope)
        active = next((t for t in versions if t.is_active), None)
        return {
            "scope": scope,
            "active_id": active.id if active else None,
            "versions": [self._serialize(t) for t in versions],
        }

    async def preview_create_guideline_ref_diff(
        self,
        *,
        scope: str,
        guideline_default_refs: list[Any] | None,
        compatibility_import: bool = False,
    ) -> dict[str, list[str]]:
        return await self._svc.preview_create_guideline_ref_diff(
            scope=scope,
            guideline_default_refs=guideline_default_refs,
            compatibility_import=compatibility_import,
        )

    async def preview_activate_guideline_ref_diff(
        self,
        *,
        template_id: str,
    ) -> dict[str, list[str]]:
        return await self._svc.preview_activate_guideline_ref_diff(
            template_id=template_id,
        )

    async def preview_deactivate_guideline_ref_diff(
        self,
        *,
        template_id: str,
    ) -> dict[str, list[str]]:
        return await self._svc.preview_deactivate_guideline_ref_diff(
            template_id=template_id,
        )

    async def get_board_diff(
        self,
        *,
        board_id: str,
        uow: object | None = None,
    ) -> dict[str, Any]:
        # R01C IMP3 drain: existence get-by-id via the edition-owned repository port
        # (R01B FR3 ``resolve_unit_of_work_factory().wrap`` seam) instead of the ORM
        # import. No owner/permission predicate here (access is enforced at the REST
        # layer); the ``board is None`` → 404 mapping is preserved exactly.
        # Application use cases already own an authenticated request UoW. Reuse
        # it when supplied: wrapping the same session again loses the actor at
        # this Core-only seam and conflicts with Community's fail-closed
        # semantic-subject actor binding. Direct service/MCP compatibility
        # callers may still omit it and use the legacy wrapping path.
        resolved_uow = uow or resolve_unit_of_work_factory().wrap(self._db)
        board = await resolved_uow.boards.get(board_id)
        if board is None:
            raise DefaultBoardConfigurationError(
                "board_not_found",
                f"Board '{board_id}' was not found or is not accessible.",
                404,
            )
        data = await self._svc.diff_board_config(board)

        # Checklist governance is persisted in its own versioned binding, not in
        # Board.settings. Enrich the canonical diff here so REST, MCP and future
        # adapters all report a local mode override identically.
        snapshot = getattr(board, "default_config_snapshot", None)
        checklist_snapshot = (
            snapshot.get("spec_checklist")
            if isinstance(snapshot, dict)
            else None
        )
        applied_mode = (
            checklist_snapshot.get("mode")
            if isinstance(checklist_snapshot, dict)
            else None
        )
        if isinstance(applied_mode, str):
            from okto_pulse.core.services.checklist import (
                ChecklistNotFoundError,
                ChecklistService,
            )

            try:
                binding = await ChecklistService().get_binding(
                    board_id=board_id,
                    persistence=resolved_uow.services.checklists,
                )
                current_mode = binding.mode.value
            except ChecklistNotFoundError:
                current_mode = "off"
            if current_mode != applied_mode:
                fields = list(data.get("fields") or [])
                fields.append(
                    {
                        "field": "spec_checklist_mode",
                        "template_value": applied_mode,
                        "current_value": current_mode,
                        "state": "overridden",
                    }
                )
                data["fields"] = fields
        return data

    # -- writes (admin) ----------------------------------------------------

    async def create_version(
        self,
        *,
        actor: str,
        settings_payload: dict[str, Any] | None = None,
        scope: str = "global",
        guideline_default_refs: list[Any] | None = None,
        design_system_default_ref: dict[str, Any] | None = None,
        spec_checklist_mode: str | None = None,
        activate: bool = False,
        query_scope: QueryScope | None = None,
        compatibility_import: bool = False,
    ) -> dict[str, Any]:
        template = await self._svc.create_version(
            settings_payload=settings_payload,
            actor=actor,
            scope=scope,
            guideline_default_refs=guideline_default_refs,
            design_system_default_ref=design_system_default_ref,
            spec_checklist_mode=spec_checklist_mode,
            activate=activate,
            query_scope=query_scope,
            compatibility_import=compatibility_import,
        )
        return self._serialize(template)

    async def activate_version(
        self,
        *,
        template_id: str,
        actor: str,
        query_scope: QueryScope | None = None,
    ) -> dict[str, Any]:
        template = await self._svc.activate_version(
            template_id,
            actor,
            query_scope=query_scope,
        )
        return self._serialize(template)

    async def deactivate_version(self, *, template_id: str, actor: str) -> dict[str, Any]:
        template = await self._svc.deactivate_version(template_id, actor)
        return self._serialize(template)

    # -- guideline defaults (spec 8a2fad91) --------------------------------

    async def update_template_guidelines(
        self,
        *,
        template_id: str,
        guideline_default_refs: list[Any] | None,
        actor: str,
        query_scope: QueryScope | None = None,
    ) -> dict[str, Any]:
        """Update a template's guideline_default_refs. Returns the EFFECTIVE template
        (a new version for an active template — Q1=B copy-on-write — or the mutated
        draft). Errors surface as structured DefaultBoardConfigurationError (TR6)."""
        template = await self._svc.update_guideline_default_refs(
            template_id,
            guideline_default_refs,
            actor,
            query_scope=query_scope,
        )
        return self._serialize(template)

    async def list_default_candidates(
        self,
        *,
        scope: str = "global",
        template_id: str | None = None,
        actor: str | None = None,
        query_scope: QueryScope | None = None,
    ) -> dict[str, Any]:
        return await self._svc.list_default_candidates(
            scope=scope,
            template_id=template_id,
            actor=actor,
            query_scope=query_scope,
        )

    # -- design system default (spec 3a006f65) -----------------------------

    async def set_template_design_system(
        self,
        *,
        template_id: str,
        design_system_id: str,
        actor: str,
        version: int | None = None,
        snapshot: dict[str, Any] | None = None,
        gate_mode: str = "off",
    ) -> dict[str, Any]:
        """Set the Design System default + canonical gate mode on a template. Active
        template => copy-on-write new version (Q1=B/Q3). Errors surface as structured
        DefaultBoardConfigurationError (TR6/no synthetic refs)."""
        template = await self._svc.update_template_design_system(
            template_id,
            design_system_id=design_system_id,
            actor=actor,
            version=version,
            snapshot=snapshot,
            gate_mode=gate_mode,
        )
        return self._serialize(template)

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _serialize(template) -> dict[str, Any]:
        raw_settings = template.settings_payload
        settings_projection = project_configuration_presence(raw_settings)
        return {
            "id": template.id,
            "version": template.version,
            "status": template.status,
            "is_active": template.is_active,
            "scope": template.scope,
            "settings_payload": (
                None if raw_settings is None else dict(raw_settings)
            ),
            "settings_presence": settings_projection.state,
            "settings_baseline_available": settings_projection.baseline_available,
            "settings_comparable": settings_projection.comparable,
            "guideline_default_refs": list(template.guideline_default_refs or []),
            "design_system_default_ref": template.design_system_default_ref,
            "spec_checklist_mode": (
                DefaultBoardConfigurationService._validate_spec_checklist_mode(
                    template.spec_checklist_mode
                )
            ),
            "created_by": template.created_by,
            "created_at": _iso(getattr(template, "created_at", None)),
            "updated_at": _iso(getattr(template, "updated_at", None)),
        }
