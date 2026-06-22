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

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import Board
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

    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self._svc = DefaultBoardConfigurationService(db)

    # -- reads -------------------------------------------------------------

    async def get_active(self, *, scope: str = "global") -> dict[str, Any]:
        template = await self._svc.resolve_active(scope)
        return {"scope": scope, "active": self._serialize(template) if template else None}

    async def list_versions(self, *, scope: str = "global") -> dict[str, Any]:
        versions = await self._svc.list_versions(scope)
        active = next((t for t in versions if t.is_active), None)
        return {
            "scope": scope,
            "active_id": active.id if active else None,
            "versions": [self._serialize(t) for t in versions],
        }

    async def get_board_diff(self, *, board_id: str) -> dict[str, Any]:
        board = await self._db.get(Board, board_id)
        if board is None:
            raise DefaultBoardConfigurationError(
                "board_not_found",
                f"Board '{board_id}' was not found or is not accessible.",
                404,
            )
        return await self._svc.diff_board_config(board)

    # -- writes (admin) ----------------------------------------------------

    async def create_version(
        self,
        *,
        actor: str,
        settings_payload: dict[str, Any] | None = None,
        scope: str = "global",
        guideline_default_refs: list[Any] | None = None,
        design_system_default_ref: dict[str, Any] | None = None,
        activate: bool = False,
    ) -> dict[str, Any]:
        template = await self._svc.create_version(
            settings_payload=settings_payload,
            actor=actor,
            scope=scope,
            guideline_default_refs=guideline_default_refs,
            design_system_default_ref=design_system_default_ref,
            activate=activate,
        )
        await self._db.refresh(template)  # load server_default created_at/updated_at (no MissingGreenlet)
        return self._serialize(template)

    async def activate_version(self, *, template_id: str, actor: str) -> dict[str, Any]:
        template = await self._svc.activate_version(template_id, actor)
        await self._db.refresh(template)  # load onupdate updated_at (no MissingGreenlet)
        return self._serialize(template)

    async def deactivate_version(self, *, template_id: str, actor: str) -> dict[str, Any]:
        template = await self._svc.deactivate_version(template_id, actor)
        await self._db.refresh(template)
        return self._serialize(template)

    # -- guideline defaults (spec 8a2fad91) --------------------------------

    async def update_template_guidelines(
        self,
        *,
        template_id: str,
        guideline_default_refs: list[Any] | None,
        actor: str,
    ) -> dict[str, Any]:
        """Update a template's guideline_default_refs. Returns the EFFECTIVE template
        (a new version for an active template — Q1=B copy-on-write — or the mutated
        draft). Errors surface as structured DefaultBoardConfigurationError (TR6)."""
        template = await self._svc.update_guideline_default_refs(
            template_id, guideline_default_refs, actor
        )
        await self._db.refresh(template)
        return self._serialize(template)

    async def list_default_candidates(
        self, *, scope: str = "global", template_id: str | None = None
    ) -> dict[str, Any]:
        return await self._svc.list_default_candidates(scope=scope, template_id=template_id)

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
        await self._db.refresh(template)
        return self._serialize(template)

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _serialize(template) -> dict[str, Any]:
        return {
            "id": template.id,
            "version": template.version,
            "status": template.status,
            "is_active": template.is_active,
            "scope": template.scope,
            "settings_payload": dict(template.settings_payload or {}),
            "guideline_default_refs": list(template.guideline_default_refs or []),
            "design_system_default_ref": template.design_system_default_ref,
            "created_by": template.created_by,
            "created_at": _iso(getattr(template, "created_at", None)),
            "updated_at": _iso(getattr(template, "updated_at", None)),
        }
