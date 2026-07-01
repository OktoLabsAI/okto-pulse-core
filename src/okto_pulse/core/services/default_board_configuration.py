"""DefaultBoardConfiguration provider (spec 9df814bc / card d86f4f96).

``DefaultBoardConfigurationService`` is the SINGLE provider of the active default
board template. It resolves the active template, builds the effective board
settings + snapshot metadata at board-creation time, and owns the template
lifecycle (create/activate/deactivate) with a dedicated global audit trail.

Design invariants:
* snapshot metadata is returned for ``Board.default_config_snapshot`` (OUTSIDE
  ``Board.settings``); future template changes never mutate existing boards (TR5);
* ``BoardCreate.settings`` is a PARTIAL override merged over the template via
  ``BoardGovernanceService.merge_settings_patch`` (TR3);
* no active template → effective settings fall back to the supplied override (or
  ``BoardSettings()`` default), with NO snapshot and NO error (AC11);
* there is at most ONE active template per scope, enforced inside the caller's
  transaction (AC-single-active);
* Guidelines (card #3) and Design System (card #4) consume this provider as
  adapters — this card stores their refs but never materializes them, so there is
  no parallel default/snapshot mechanism (TR10).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import (
    BoardGuideline,
    DefaultBoardConfiguration,
    DefaultBoardConfigurationAudit,
    DesignSystem,
    Guideline,
)
from okto_pulse.core.models.schemas import BoardSettings
from okto_pulse.core.services.board_governance import (
    LEGACY_ABSENT_SETTING_KEYS,
    BoardGovernanceService,
)

# Stable global template audit event types (FR9).
EVENT_CREATED = "default_board_configuration_created"
EVENT_ACTIVATED = "default_board_configuration_activated"
EVENT_DEACTIVATED = "default_board_configuration_deactivated"

# Guideline-default admin audit event types (spec 8a2fad91 FR6).
EVENT_GUIDELINE_ADDED = "default_guideline_added"
EVENT_GUIDELINE_REMOVED = "default_guideline_removed"
EVENT_GUIDELINE_REORDERED = "default_guideline_reordered"
EVENT_GUIDELINE_REJECTED = "default_guideline_rejected"
# Materialization events (spec 8a2fad91 FR6, card 2803c136): applied_to_board is a
# committed DB audit row (success path); materialization_failed is a structured log
# (the failing path rolls back, so a same-transaction audit row cannot survive — and
# a separate-transaction write would block on SQLite's single writer while the board
# transaction is pending).
EVENT_GUIDELINE_APPLIED_TO_BOARD = "default_guideline_applied_to_board"
EVENT_MATERIALIZATION_FAILED = "default_guideline_materialization_failed"
# Design System default admin/materialization events (spec 3a006f65 / card 96f76a5f).
EVENT_DESIGN_SYSTEM_DEFAULT_SET = "design_system_default_set"
EVENT_DESIGN_SYSTEM_APPLIED_TO_BOARD = "design_system_applied_to_board"

# Stable board-scoped event names (logged in ActivityLog by BoardService).
BOARD_EVENT_APPLIED = "default_board_configuration_applied"
BOARD_EVENT_FALLBACK = "board_created_without_default_config"

_DEFAULT_SCOPE = "global"
_ALLOWED_STATUSES = ("draft", "active", "inactive")

logger = logging.getLogger(__name__)


@dataclass  # NOT frozen: an Exception must stay mutable (Python sets __traceback__ on
# propagation; a frozen dataclass raises FrozenInstanceError -> 500). See DesignSystemError.
class DefaultBoardConfigurationError(Exception):
    """Structured error for REST/MCP (card #5) — never a raw exception leak."""

    code: str
    message: str
    status_code: int = 400
    details: dict[str, Any] = None  # type: ignore[assignment]

    def to_dict(self) -> dict[str, Any]:
        out = {
            "error": self.code,
            "code": self.code,
            "message": self.message,
            "status_code": self.status_code,
        }
        if self.details:
            out["details"] = self.details
        return out


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ref_id(ref: Any) -> str | None:
    return ref.get("guideline_id") if isinstance(ref, dict) else None


def _diff_guideline_refs(
    old: list[Any], new: list[Any]
) -> dict[str, list[str]]:
    """Classify a guideline_default_refs change into added / removed / reordered
    guideline_ids (spec 8a2fad91 FR6 — unambiguous audit/diff). ``reordered``
    captures a guideline kept in the set whose priority OR relative position
    changed."""
    old_by_id = {_ref_id(r): r for r in old if _ref_id(r)}
    new_by_id = {_ref_id(r): r for r in new if _ref_id(r)}
    added = [gid for gid in new_by_id if gid not in old_by_id]
    removed = [gid for gid in old_by_id if gid not in new_by_id]
    reordered: list[str] = []
    for gid, ref in new_by_id.items():
        if gid in old_by_id:
            old_prio = int(old_by_id[gid].get("priority", 0) or 0)
            new_prio = int(ref.get("priority", 0) or 0)
            if old_prio != new_prio:
                reordered.append(gid)
    kept_old_order = [gid for gid in old_by_id if gid in new_by_id]
    kept_new_order = [gid for gid in new_by_id if gid in old_by_id]
    if kept_old_order != kept_new_order:
        for gid in kept_new_order:
            if gid not in reordered:
                reordered.append(gid)
    return {"added": added, "removed": removed, "reordered": reordered}


class DefaultBoardConfigurationService:
    """Single provider + lifecycle owner for default board configuration templates."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # -- read / apply ------------------------------------------------------

    async def resolve_active(self, scope: str = _DEFAULT_SCOPE) -> DefaultBoardConfiguration | None:
        """The single active template for ``scope`` (None if none is active)."""
        result = await self.db.execute(
            select(DefaultBoardConfiguration)
            .where(
                DefaultBoardConfiguration.scope == scope,
                DefaultBoardConfiguration.is_active.is_(True),
            )
            .order_by(DefaultBoardConfiguration.version.desc())
        )
        return result.scalars().first()

    async def build_snapshot_for_create(
        self,
        *,
        settings_override: BoardSettings | dict[str, Any] | None = None,
        applied_by: str,
        scope: str = _DEFAULT_SCOPE,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """Compute (effective_settings, snapshot_meta) for a new board.

        With an active template: effective = template payload + partial override;
        snapshot_meta records the applied template + override summary.
        Without one: effective = normalized override (or BoardSettings() default)
        and snapshot_meta is ``None`` (graceful fallback, AC11)."""
        template = await self.resolve_active(scope)
        if template is None:
            return BoardGovernanceService.normalize_settings(settings_override), None

        effective = BoardGovernanceService.merge_settings_patch(
            template.settings_payload, settings_override
        )
        snapshot_meta = {
            "template_id": template.id,
            "template_version": template.version,
            "scope": template.scope,
            "applied_at": _utcnow_iso(),
            "applied_by": applied_by,
            "override_summary": self._override_fields(settings_override),
        }
        # FR6: the Design System default is an effective record inside the snapshot
        # (no parallel store / no rich entity — TR7).
        if template.design_system_default_ref:
            snapshot_meta["design_system"] = template.design_system_default_ref
        return effective, snapshot_meta

    @staticmethod
    def _override_fields(
        settings_override: BoardSettings | dict[str, Any] | None,
    ) -> dict[str, Any]:
        """The fields the override explicitly changes (partial, not full defaults)."""
        if settings_override is None:
            return {}
        if isinstance(settings_override, BoardSettings):
            return settings_override.model_dump(mode="json", exclude_unset=True)
        return dict(settings_override)

    async def describe_board_config(self, board) -> dict[str, Any]:
        """READ-ONLY diff/state of a board's applied snapshot vs the currently
        active template (card 7da43521 exposes this via API/MCP/UI). It NEVER
        mutates the board and NEVER backfills a legacy board (TR4/TR5):

        * a board with no snapshot is reported as ``legacy_no_snapshot`` as-is;
        * a board with a snapshot reports its ORIGINAL applied template version
          alongside the currently active one, so template changes stay
          forward-only (no live inheritance).
        """
        snapshot = board.default_config_snapshot
        if not snapshot:
            return {"state": "legacy_no_snapshot", "board_id": board.id}
        scope = snapshot.get("scope", _DEFAULT_SCOPE)
        active = await self.resolve_active(scope)
        applied_version = snapshot.get("template_version")
        active_version = active.version if active is not None else None
        return {
            "state": "applied",
            "board_id": board.id,
            "scope": scope,
            "applied_template_id": snapshot.get("template_id"),
            "applied_template_version": applied_version,
            "active_template_id": active.id if active is not None else None,
            "active_template_version": active_version,
            "is_outdated": active_version is not None and active_version != applied_version,
            "override_summary": snapshot.get("override_summary", {}),
        }

    async def diff_board_config(self, board) -> dict[str, Any]:
        """READ-ONLY field-level diff between the template snapshot APPLIED to the
        board and the board's CURRENT settings (FR7). ``template_value`` is the value
        the APPLIED template version prescribed (templates are append-only/immutable,
        so the applied version is recoverable by id) — NOT the currently active
        template. Reports ``snapshot_state`` plus applied-vs-active version metadata
        in separate fields. Never mutates the board / never backfills (TR4/TR5)."""
        snapshot = board.default_config_snapshot
        if not snapshot:
            return {
                "board_id": board.id,
                "snapshot_state": "legacy_no_snapshot",
                "applied_template_id": None,
                "applied_template_version": None,
                "active_template_id": None,
                "active_template_version": None,
                "is_outdated": False,
                "fields": [],
            }
        scope = snapshot.get("scope", _DEFAULT_SCOPE)
        active = await self.resolve_active(scope)
        applied_template = await self.db.get(
            DefaultBoardConfiguration, snapshot.get("template_id")
        )
        template_settings = dict(applied_template.settings_payload or {}) if applied_template else {}
        current_settings = dict(board.settings or {})
        applied_version = snapshot.get("template_version")
        active_version = active.version if active is not None else None
        fields = []
        for field in sorted(set(template_settings) | set(current_settings)):
            template_value = template_settings.get(field)
            current_value = current_settings.get(field)
            if template_value != current_value:
                fields.append({
                    "field": field,
                    "template_value": template_value,
                    "current_value": current_value,
                    "state": "overridden",
                })
        return {
            "board_id": board.id,
            "snapshot_state": "applied",
            "applied_template_id": snapshot.get("template_id"),
            "applied_template_version": applied_version,
            "active_template_id": active.id if active is not None else None,
            "active_template_version": active_version,
            "is_outdated": active_version is not None and active_version != applied_version,
            "fields": fields,
        }

    async def materialize_default_guidelines(
        self, board_id: str, *, scope: str = _DEFAULT_SCOPE, actor: str = "system"
    ) -> list[BoardGuideline]:
        """Adapter (FR3): resolve the ACTIVE template's GLOBAL guideline defaults and
        DELEGATE link creation to ``GuidelineService.apply_default_guidelines`` (the
        guideline-domain writer — and the ts_a48e70ee failure-injection point), in the
        caller's transaction. Returns ``[]`` when no template/refs (graceful — never an
        error). Re-validates global-only fail-closed (defense). On success emits a
        committed ``default_guideline_applied_to_board`` audit row carrying
        actor/template_version/board_id (FR6). The umbrella stays the single source of
        truth; there is no parallel store/snapshot of guideline defaults."""
        template = await self.resolve_active(scope)
        if template is None:
            return []
        refs = template.guideline_default_refs or []
        if not refs:
            return []
        await self._validate_guideline_default_refs(refs)

        # Local import avoids the main <-> default_board_configuration import cycle.
        from okto_pulse.core.services.main import GuidelineService

        created = await GuidelineService(self.db).apply_default_guidelines(
            board_id,
            refs,
            template_id=template.id,
            template_version=template.version,
            actor=actor,
        )
        if created:
            self._audit(
                template,
                EVENT_GUIDELINE_APPLIED_TO_BOARD,
                actor,
                {"board_id": board_id, "guideline_ids": [c.guideline_id for c in created]},
            )
        return created

    async def materialize_design_system_default(
        self, board_id: str, *, scope: str = _DEFAULT_SCOPE, actor: str = "system"
    ) -> dict[str, Any] | None:
        """Adapter (FR4 / card 96f76a5f): apply the active template's Design System
        default to a NEW board in the caller's transaction. Re-validates the ref
        fail-closed (real global active — the failable design-system adapter), then
        materializes the REAL effective association (``BoardDesignSystem`` via
        ``DesignSystemService``) so the board's effective Design System is queryable from
        persisted state. The snapshot in ``Board.default_config_snapshot['design_system']``
        stays as provenance metadata (no parallel store, TR7). Returns the applied ref
        (or ``None`` when no template/ref). Forward-only: this only runs at board
        creation, so a later default change never touches existing boards."""
        template = await self.resolve_active(scope)
        if template is None or not template.design_system_default_ref:
            return None
        ref = template.design_system_default_ref
        await self._validate_design_system_default_ref(ref)

        # Local import avoids any import cycle; materializes the REAL board link.
        from okto_pulse.core.services.design_system import DesignSystemService

        link = await DesignSystemService(self.db).link_design_system_to_board(
            board_id, ref["design_system_id"]
        )
        self._audit(
            template,
            EVENT_DESIGN_SYSTEM_APPLIED_TO_BOARD,
            actor,
            {
                "board_id": board_id,
                "design_system_id": ref["design_system_id"],
                "design_system_version": link.design_system_version,
            },
        )
        return ref

    async def apply_default_config_to_board(
        self, board_id: str, *, scope: str = _DEFAULT_SCOPE, actor: str = "system"
    ) -> None:
        """Umbrella orchestration (TR2/TR3/TR7): run EVERY default adapter (guidelines +
        design system) for a new board in the caller's transaction. ANY adapter failure
        (structured or unexpected) aborts with a single structured
        ``default_materialization_failed`` so the whole ``create_board`` reverts — no
        partial board/link/snapshot (AC4). The failure is recorded as a STRUCTURED LOG
        carrying actor/template_id/template_version/board_id (it must survive the
        rollback, which a same-transaction audit row could not; a separate-transaction
        write would block on SQLite's single writer while the board txn is pending) —
        the success path's applied_to_board audit is a committed DB row. No active
        template -> graceful no-op. Single umbrella entry point; no parallel path."""
        template = await self.resolve_active(scope)
        try:
            await self.materialize_default_guidelines(board_id, scope=scope, actor=actor)
            await self.materialize_design_system_default(board_id, scope=scope, actor=actor)
        except Exception as exc:
            is_structured = isinstance(exc, DefaultBoardConfigurationError)
            cause = exc.code if is_structured else type(exc).__name__
            detail = exc.message if is_structured else str(exc)
            logger.warning(
                "default_guideline_materialization_failed board=%s scope=%s "
                "template_id=%s template_version=%s actor=%s cause=%s detail=%s",
                board_id, scope, getattr(template, "id", None),
                getattr(template, "version", None), actor, cause, detail,
                extra={
                    "event": EVENT_MATERIALIZATION_FAILED,
                    "board_id": board_id,
                    "template_id": getattr(template, "id", None),
                    "template_version": getattr(template, "version", None),
                    "actor": actor,
                    "cause": cause,
                },
            )
            raise DefaultBoardConfigurationError(
                "default_materialization_failed",
                "Applying the default board configuration adapters failed; board "
                "creation was aborted to avoid a partially-configured board.",
                422,
                {
                    "cause": cause,
                    "detail": detail,
                    "board_id": board_id,
                    "template_id": getattr(template, "id", None),
                    "template_version": getattr(template, "version", None),
                    "actor": actor,
                },
            ) from exc

    # -- lifecycle ---------------------------------------------------------

    async def create_version(
        self,
        *,
        settings_payload: BoardSettings | dict[str, Any] | None,
        actor: str,
        scope: str = _DEFAULT_SCOPE,
        guideline_default_refs: list[Any] | None = None,
        design_system_default_ref: dict[str, Any] | None = None,
        activate: bool = False,
    ) -> DefaultBoardConfiguration:
        """Create a new draft template version (validated as BoardSettings, TR1).
        ``activate=True`` immediately activates it (single-active enforced)."""
        validated_payload = self._validate_settings(settings_payload)
        active = await self.resolve_active(scope)
        if isinstance(settings_payload, dict) and active is not None:
            active_payload = active.settings_payload or {}
            for key in LEGACY_ABSENT_SETTING_KEYS:
                if key not in settings_payload and key not in active_payload:
                    validated_payload.pop(key, None)
        result = await self.db.execute(
            select(func.max(DefaultBoardConfiguration.version)).where(
                DefaultBoardConfiguration.scope == scope
            )
        )
        next_version = int(result.scalar() or 0) + 1
        template = DefaultBoardConfiguration(
            version=next_version,
            status="draft",
            is_active=False,
            scope=scope,
            settings_payload=validated_payload,
            guideline_default_refs=list(guideline_default_refs) if guideline_default_refs else None,
            design_system_default_ref=design_system_default_ref,
            created_by=actor,
        )
        self.db.add(template)
        await self.db.flush()
        self._audit(template, EVENT_CREATED, actor)
        if activate:
            template = await self.activate_version(template.id, actor)
        return template

    async def activate_version(self, template_id: str, actor: str) -> DefaultBoardConfiguration:
        """Activate a template; deactivate every other active template in the same
        scope IN THE SAME TRANSACTION so there is never more than one active."""
        template = await self._require(template_id)
        # FR5/TR6: a template whose guideline defaults are inline / non-global /
        # missing MUST NOT activate (fail-closed before it can reach any board).
        await self._validate_guideline_default_refs(template.guideline_default_refs)
        # FR6: the Design System default ref (if any) must satisfy the minimal contract.
        await self._validate_design_system_default_ref(template.design_system_default_ref)
        result = await self.db.execute(
            select(DefaultBoardConfiguration).where(
                DefaultBoardConfiguration.scope == template.scope,
                DefaultBoardConfiguration.is_active.is_(True),
                DefaultBoardConfiguration.id != template.id,
            )
        )
        for other in result.scalars():
            other.is_active = False
            other.status = "inactive"
            self._audit(
                other, EVENT_DEACTIVATED, actor,
                {"reason": "superseded", "superseded_by": template.id},
            )
        template.is_active = True
        template.status = "active"
        await self.db.flush()
        self._audit(template, EVENT_ACTIVATED, actor)
        return template

    async def deactivate_version(self, template_id: str, actor: str) -> DefaultBoardConfiguration:
        """Deactivate a template (no active template remains unless another exists)."""
        template = await self._require(template_id)
        template.is_active = False
        template.status = "inactive"
        await self.db.flush()
        self._audit(template, EVENT_DEACTIVATED, actor)
        return template

    async def list_versions(self, scope: str = _DEFAULT_SCOPE) -> list[DefaultBoardConfiguration]:
        result = await self.db.execute(
            select(DefaultBoardConfiguration)
            .where(DefaultBoardConfiguration.scope == scope)
            .order_by(DefaultBoardConfiguration.version.desc())
        )
        return list(result.scalars())

    # -- guideline-default administration (spec 8a2fad91) ------------------

    async def update_guideline_default_refs(
        self, template_id: str, refs: list[Any] | None, actor: str
    ) -> DefaultBoardConfiguration:
        """Admin mutation of a template's guideline_default_refs (FR1; single owner
        TR1 — never a ``Guideline.is_default`` flag). Rejects inline/missing/non-global
        refs fail-closed BEFORE any persistence (FR2/TR6). A DRAFT template mutates
        in-place; an ACTIVE template is COPY-ON-WRITE: a new version is derived from
        the active config with the new refs and activated, so the prior version stays
        intact and reconstituible by id/version (Q1=B). Audits added/removed/reordered
        (FR6). Each stored ref is normalized to ``{guideline_id, priority,
        guideline_version}`` so the default carries an auditable guideline version (AC1)."""
        template = await self._require(template_id)
        # FR2/TR6: fail closed BEFORE persisting. The rejection rolls back with the
        # caller's transaction, so it is recorded as a structured log, not a doomed
        # audit row; the structured error is the API-level rejection signal.
        try:
            await self._validate_guideline_default_refs(refs)
        except DefaultBoardConfigurationError as exc:
            logger.warning(
                "default_guideline_rejected template_id=%s reason=%s actor=%s",
                template_id,
                exc.code,
                actor,
            )
            raise
        normalized = await self._normalize_guideline_refs(refs)
        old_refs = list(template.guideline_default_refs or [])
        diff = _diff_guideline_refs(old_refs, normalized)

        if template.is_active:
            # Q1=B: copy-on-write — never mutate an active template's content in place.
            new_template = await self.create_version(
                settings_payload=dict(template.settings_payload or {}),
                actor=actor,
                scope=template.scope,
                guideline_default_refs=normalized,
                design_system_default_ref=template.design_system_default_ref,
                activate=True,
            )
            self._audit_guideline_diff(
                new_template, actor, diff, {"source_version": template.version}
            )
            return new_template

        # Draft (or inactive) template: mutate refs in place.
        template.guideline_default_refs = normalized or None
        await self.db.flush()
        self._audit_guideline_diff(template, actor, diff, None)
        return template

    async def list_default_candidates(
        self, *, scope: str = _DEFAULT_SCOPE, template_id: str | None = None
    ) -> dict[str, Any]:
        """List GLOBAL catalog guidelines with derived eligibility + current default
        status (FR1/AC7/BR br_8f801aa4 — state is resolved FROM the umbrella template,
        never from a ``Guideline`` flag). Uses the ACTIVE template by default; an
        explicit ``template_id`` inspects a specific (e.g. draft) version."""
        if template_id is not None:
            template = await self._require(template_id)
        else:
            template = await self.resolve_active(scope)
        refs = (template.guideline_default_refs or []) if template else []
        ref_by_id = {_ref_id(r): r for r in refs if _ref_id(r)}
        result = await self.db.execute(
            select(Guideline)
            .where(Guideline.scope == _DEFAULT_SCOPE, Guideline.board_id.is_(None))
            .order_by(Guideline.title)
        )
        candidates: list[dict[str, Any]] = []
        for g in result.scalars():
            ref = ref_by_id.get(g.id)
            candidates.append(
                {
                    "guideline_id": g.id,
                    "title": g.title,
                    "scope": g.scope,
                    "guideline_version": g.version,
                    "eligible": True,  # a global catalog guideline is always eligible
                    "is_default": ref is not None,
                    "priority": int(ref.get("priority", 0) or 0) if ref else None,
                }
            )
        return {
            "scope": scope,
            "template_id": template.id if template else None,
            "template_version": template.version if template else None,
            "candidates": candidates,
        }

    async def _normalize_guideline_refs(
        self, refs: list[Any] | None
    ) -> list[dict[str, Any]]:
        """Normalize each (already-validated) ref to ``{guideline_id, priority,
        guideline_version}``, capturing the catalog guideline's current version so the
        default is auditable (AC1)."""
        normalized: list[dict[str, Any]] = []
        for ref in refs or []:
            guideline_id = ref["guideline_id"]
            guideline = await self.db.get(Guideline, guideline_id)
            normalized.append(
                {
                    "guideline_id": guideline_id,
                    "priority": int(ref.get("priority", 0) or 0),
                    "guideline_version": getattr(guideline, "version", None),
                }
            )
        return normalized

    def _audit_guideline_diff(
        self,
        template: DefaultBoardConfiguration,
        actor: str,
        diff: dict[str, list[str]],
        extra: dict[str, Any] | None,
    ) -> None:
        """Emit one audit row per change category that actually occurred (FR6)."""
        base = dict(extra or {})
        for event, key in (
            (EVENT_GUIDELINE_ADDED, "added"),
            (EVENT_GUIDELINE_REMOVED, "removed"),
            (EVENT_GUIDELINE_REORDERED, "reordered"),
        ):
            if diff.get(key):
                self._audit(template, event, actor, {**base, "guideline_ids": diff[key]})

    # -- design-system default administration (spec 3a006f65 / card 96f76a5f) --

    async def update_template_design_system(
        self,
        template_id: str,
        *,
        design_system_id: str,
        actor: str,
        version: int | None = None,
        snapshot: dict[str, Any] | None = None,
        gate_mode: str = "off",
    ) -> DefaultBoardConfiguration:
        """Set the Design System default on a template (FR3): the IDENTITY goes to
        ``design_system_default_ref`` and the CANONICAL gate mode to
        ``settings_payload.design_system_gate_mode`` in the SAME mutation (Q1=B — the
        gate_mode mirrored inside the ref is never an authoritative second source). The
        ref is rejected fail-closed when it is not a real, global, active DesignSystem
        (no synthetic refs — card #1 layer kept intact). An ACTIVE template is
        copy-on-write (new version + activate); a draft mutates in place. Forward-only +
        audited."""
        template = await self._require(template_id)
        gate_mode = gate_mode or "off"
        if gate_mode not in ("off", "advisory", "blocking"):
            raise DefaultBoardConfigurationError(
                "design_system_gate_mode_invalid",
                f"design_system_gate_mode '{gate_mode}' is invalid (off|advisory|blocking).",
                422,
                {"gate_mode": gate_mode},
            )
        # The ref carries identity; the gate_mode inside it is a derived mirror of the
        # canonical settings_payload.design_system_gate_mode.
        ref: dict[str, Any] = {"design_system_id": design_system_id, "gate_mode": gate_mode}
        if version is not None:
            ref["version"] = version
        if snapshot is not None:
            ref["snapshot"] = snapshot
        # Fail closed BEFORE persisting: a real, global, active DesignSystem only.
        await self._validate_design_system_default_ref(ref)
        # Canonical gate mode -> settings_payload (validated as BoardSettings).
        merged_settings = dict(template.settings_payload or {})
        merged_settings["design_system_gate_mode"] = gate_mode
        merged_settings = self._validate_settings(merged_settings)

        if template.is_active:
            # Q1=B / Q3: copy-on-write — never mutate an active template's content.
            new_template = await self.create_version(
                settings_payload=merged_settings,
                actor=actor,
                scope=template.scope,
                guideline_default_refs=template.guideline_default_refs,
                design_system_default_ref=ref,
                activate=True,
            )
            self._audit(
                new_template, EVENT_DESIGN_SYSTEM_DEFAULT_SET, actor,
                {
                    "source_version": template.version,
                    "design_system_id": design_system_id,
                    "gate_mode": gate_mode,
                },
            )
            return new_template

        # Draft (or inactive) template: mutate in place.
        template.design_system_default_ref = ref
        template.settings_payload = merged_settings
        await self.db.flush()
        self._audit(
            template, EVENT_DESIGN_SYSTEM_DEFAULT_SET, actor,
            {"design_system_id": design_system_id, "gate_mode": gate_mode},
        )
        return template

    # -- internals ---------------------------------------------------------

    def _validate_settings(
        self, settings_payload: BoardSettings | dict[str, Any] | None
    ) -> dict[str, Any]:
        try:
            return BoardGovernanceService.normalize_settings(settings_payload)
        except ValidationError as exc:
            raise DefaultBoardConfigurationError(
                "invalid_settings_payload",
                "settings_payload is not a valid BoardSettings.",
                422,
                {"errors": exc.errors(include_url=False)},
            ) from exc

    async def _validate_guideline_default_refs(self, refs: list[Any] | None) -> None:
        """FR5/TR6/BR br_512d374b: every guideline default MUST reference an EXISTING
        GLOBAL catalog guideline. Inline (no guideline_id), missing, or
        board-scoped/non-global refs are rejected fail-closed BEFORE activate/apply."""
        for ref in refs or []:
            guideline_id = ref.get("guideline_id") if isinstance(ref, dict) else None
            if not guideline_id:
                raise DefaultBoardConfigurationError(
                    "default_guideline_inline_not_allowed",
                    "Inline guideline defaults are not allowed; reference a global "
                    "catalog guideline by guideline_id.",
                    422,
                    {"ref": ref},
                )
            guideline = await self.db.get(Guideline, guideline_id)
            if guideline is None:
                raise DefaultBoardConfigurationError(
                    "default_guideline_not_found",
                    f"Guideline '{guideline_id}' was not found.",
                    422,
                    {"guideline_id": guideline_id},
                )
            if guideline.scope != _DEFAULT_SCOPE or guideline.board_id is not None:
                raise DefaultBoardConfigurationError(
                    "default_guideline_not_global",
                    f"Guideline '{guideline_id}' is not a global catalog guideline "
                    "(scope must be 'global' and not board-scoped).",
                    422,
                    {"guideline_id": guideline_id},
                )

    async def _validate_design_system_default_ref(self, ref: dict[str, Any] | None) -> None:
        """FR6/TR7 + spec 3a006f65 (card 1392f59d): Design System default contract.
        A present ref MUST be a dict with a non-empty design_system_id and a gate_mode
        in the allowed set; version (int|null) and snapshot (dict|null) are optional.
        The design_system_id MUST refer to an EXISTING, GLOBAL, ACTIVE DesignSystem —
        inline/board-scoped/missing/non-active refs are rejected fail-closed so a
        synthetic or inline default can never be activated (ts_1054bf42, no synthetic
        refs)."""
        if ref is None:
            return
        if not isinstance(ref, dict) or not ref.get("design_system_id"):
            raise DefaultBoardConfigurationError(
                "design_system_default_invalid",
                "Design System default must be an object with a non-empty design_system_id.",
                422,
                {"ref": ref},
            )
        gate_mode = ref.get("gate_mode")
        if gate_mode not in (None, "off", "advisory", "blocking"):
            raise DefaultBoardConfigurationError(
                "design_system_default_invalid",
                f"Design System default gate_mode '{gate_mode}' is invalid "
                "(allowed: off, advisory, blocking, null).",
                422,
                {"gate_mode": gate_mode},
            )
        version = ref.get("version")
        if version is not None and (isinstance(version, bool) or not isinstance(version, int)):
            raise DefaultBoardConfigurationError(
                "design_system_default_invalid",
                "Design System default version must be an integer or null.",
                422,
                {"version": version},
            )
        # Entity check: the default MUST reference a real, GLOBAL, ACTIVE DesignSystem.
        design_system_id = ref["design_system_id"]
        design_system = await self.db.get(DesignSystem, design_system_id)
        if design_system is None:
            raise DefaultBoardConfigurationError(
                "design_system_not_found",
                f"Design System '{design_system_id}' was not found.",
                422,
                {"design_system_id": design_system_id},
            )
        if design_system.scope != "global" or design_system.board_id is not None:
            raise DefaultBoardConfigurationError(
                "design_system_default_not_global",
                f"Design System '{design_system_id}' is not a global catalog entry; "
                "inline/board-scoped systems cannot be a global default.",
                422,
                {"design_system_id": design_system_id},
            )
        if design_system.status != "active":
            raise DefaultBoardConfigurationError(
                "design_system_default_not_active",
                f"Design System '{design_system_id}' is not active (status="
                f"{design_system.status!r}).",
                422,
                {"design_system_id": design_system_id, "status": design_system.status},
            )

    async def _require(self, template_id: str) -> DefaultBoardConfiguration:
        template = await self.db.get(DefaultBoardConfiguration, template_id)
        if template is None:
            raise DefaultBoardConfigurationError(
                "template_not_found",
                f"DefaultBoardConfiguration '{template_id}' was not found.",
                404,
            )
        return template

    def _audit(
        self,
        template: DefaultBoardConfiguration,
        event_type: str,
        actor: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Append a GLOBAL template audit row (FR9) — reconstituible by query."""
        self.db.add(
            DefaultBoardConfigurationAudit(
                template_id=template.id,
                template_version=template.version,
                event_type=event_type,
                actor_id=actor,
                scope=template.scope,
                payload=payload,
            )
        )


__all__ = [
    "DefaultBoardConfigurationService",
    "DefaultBoardConfigurationError",
    "EVENT_CREATED",
    "EVENT_ACTIVATED",
    "EVENT_DEACTIVATED",
    "BOARD_EVENT_APPLIED",
    "BOARD_EVENT_FALLBACK",
]
