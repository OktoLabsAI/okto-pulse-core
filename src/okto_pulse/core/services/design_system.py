"""Design System catalog + board-link service (spec 3a006f65 / card 1392f59d).

``DesignSystemService`` owns the persisted DesignSystem lifecycle (global catalog +
board-inline artifacts, versioned), the singular board<->design-system effective
link, and the board's effective Design System read. Every failure is a structured
``DesignSystemError`` (never a raw exception leak) so REST + the MCP twins share the
same payload + reason codes.

Scope of THIS card: the entity + catalog + board link + effective read. The default
application from the umbrella + ``design_system_gate_mode`` (card #2) and the
``MockupDesignSystemGate`` (card #3) consume this — they are NOT implemented here.
There is no parallel default/snapshot store: a Design System default lives only in
the ``DefaultBoardConfiguration`` template ref.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import uuid

from okto_pulse.core.ports.design_system import (
    BoardDesignSystemRecord,
    DesignSystemGateAuditRecord,
    DesignSystemRecord,
    get_design_system_store,
)

_VALID_STATUSES = ("active", "draft", "archived")
_VALID_SCOPES = ("global", "inline")

# MockupDesignSystemGate reason codes (spec 3a006f65 / card 0192f58d, FR8).
GATE_REQUIRED = "design_system_required"
GATE_NOT_FOUND = "design_system_not_found"
GATE_VERSION_MISMATCH = "design_system_version_mismatch"
GATE_EVIDENCE_MISSING = "design_system_evidence_missing"

# Mockup fields whose change re-gates an existing mockup (delta-only; an unchanged
# mockup is legacy-safe and skipped). Editing the consumed identity/evidence/visual
# content is exactly when Design System compliance must be (re)checked.
_GATE_RELEVANT_FIELDS = ("design_system_ref", "design_system_evidence", "html_content")

__all__ = [
    "DesignSystemService",
    "DesignSystemError",
    "MockupDesignSystemGate",
    "normalize_design_system_ref",
    "gate_entity_screen_mockups",
    "GATE_REQUIRED",
    "GATE_NOT_FOUND",
    "GATE_VERSION_MISMATCH",
    "GATE_EVIDENCE_MISSING",
]


# NOT frozen: this subclasses Exception, and Python mutates exceptions during
# propagation (sets __traceback__/__cause__/__context__). A frozen dataclass raises
# FrozenInstanceError from its generated __setattr__ when the exception unwinds through
# an AsyncExitStack (e.g. a FastAPI `Depends` with yield), turning the intended 422 into
# a 500. Regression: test_design_system_error_propagation.py.
@dataclass
class DesignSystemError(Exception):
    """Structured error for REST/MCP — never a raw exception leak."""

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


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    iso = getattr(value, "isoformat", None)
    return iso() if callable(iso) else str(value)


def serialize_design_system(ds: DesignSystemRecord) -> dict[str, Any]:
    return {
        "id": ds.id,
        "scope": ds.scope,
        "board_id": ds.board_id,
        "title": ds.title,
        "payload": dict(ds.payload) if isinstance(ds.payload, dict) else ds.payload,
        "version": ds.version,
        "status": ds.status,
        "owner_id": ds.owner_id,
        "created_at": _iso(getattr(ds, "created_at", None)),
        "updated_at": _iso(getattr(ds, "updated_at", None)),
    }


class DesignSystemService:
    """Catalog + board-link service for Design Systems."""

    def __init__(self, db: Any) -> None:
        self.db = db

    # -- catalog lifecycle -------------------------------------------------

    async def create_design_system(
        self,
        owner_id: str,
        *,
        title: str,
        scope: str = "global",
        board_id: str | None = None,
        payload: dict[str, Any] | None = None,
        status: str = "active",
    ) -> DesignSystemRecord:
        """Create a global catalog entry or a board-inline Design System (FR1). Inline
        REQUIRES board_id (AC2); a global one is never board-scoped. version starts 1."""
        scope = scope or "global"
        if scope not in _VALID_SCOPES:
            raise DesignSystemError(
                "design_system_invalid_scope",
                f"scope '{scope}' is invalid (allowed: global, inline).",
                422,
                {"scope": scope},
            )
        if scope == "inline":
            if not board_id:
                raise DesignSystemError(
                    "design_system_inline_requires_board",
                    "Inline Design Systems require a board_id (AC2).",
                    422,
                )
        else:  # global is never board-scoped
            board_id = None
        if status not in _VALID_STATUSES:
            raise DesignSystemError(
                "design_system_invalid_status",
                f"status '{status}' is invalid (allowed: {', '.join(_VALID_STATUSES)}).",
                422,
                {"status": status},
            )
        if not (title or "").strip():
            raise DesignSystemError(
                "design_system_invalid_title", "A Design System title is required.", 422
            )
        now = datetime.now(timezone.utc)
        ds = DesignSystemRecord(
            id=str(uuid.uuid4()),
            scope=scope,
            board_id=board_id,
            title=title,
            payload=payload,
            version=1,
            status=status,
            owner_id=owner_id,
            created_at=now,
            updated_at=now,
        )
        return await get_design_system_store().create(self.db, ds)

    async def list_catalog(
        self, *, scope: str = "global", board_id: str | None = None
    ) -> list[DesignSystemRecord]:
        """List the GLOBAL catalog (default) or the inline systems of a board."""
        return list(
            await get_design_system_store().list_catalog(
                self.db,
                scope=scope,
                board_id=board_id,
            )
        )

    async def get_design_system(
        self, design_system_id: str
    ) -> DesignSystemRecord | None:
        return await get_design_system_store().get(
            self.db,
            design_system_id=design_system_id,
        )

    async def require_design_system(self, design_system_id: str) -> DesignSystemRecord:
        ds = await self.get_design_system(design_system_id)
        if ds is None:
            raise DesignSystemError(
                "design_system_not_found",
                f"Design System '{design_system_id}' was not found.",
                404,
                {"design_system_id": design_system_id},
            )
        return ds

    async def update_design_system(
        self,
        design_system_id: str,
        owner_id: str,
        *,
        title: str | None = None,
        payload: dict[str, Any] | None = None,
        status: str | None = None,
    ) -> DesignSystemRecord:
        """Update title/payload/status. A title or payload change bumps ``version``
        (including inline, so the mockup gate has a stable version to compare)."""
        ds = await self.require_design_system(design_system_id)
        bump = False
        if title is not None and title != ds.title:
            if not title.strip():
                raise DesignSystemError(
                    "design_system_invalid_title", "A Design System title is required.", 422
                )
            ds.title = title
            bump = True
        if payload is not None and payload != ds.payload:
            ds.payload = payload
            bump = True
        if status is not None:
            if status not in _VALID_STATUSES:
                raise DesignSystemError(
                    "design_system_invalid_status",
                    f"status '{status}' is invalid (allowed: {', '.join(_VALID_STATUSES)}).",
                    422,
                    {"status": status},
                )
            ds.status = status
        if bump:
            ds.version = (ds.version or 1) + 1
        return await get_design_system_store().save(self.db, ds)

    async def delete_design_system(self, design_system_id: str, owner_id: str) -> bool:
        return await get_design_system_store().delete(
            self.db,
            design_system_id=design_system_id,
        )

    # -- board link (singular effective per board) -------------------------

    async def link_design_system_to_board(
        self, board_id: str, design_system_id: str
    ) -> BoardDesignSystemRecord:
        """Set the board's single effective Design System (FR2). Upserts the singular
        ``BoardDesignSystem`` row and captures the current design_system_version. An
        inline Design System can only be linked to its OWN board."""
        ds = await self.require_design_system(design_system_id)
        if ds.scope == "inline" and ds.board_id != board_id:
            raise DesignSystemError(
                "design_system_inline_other_board",
                "An inline Design System can only be linked to its own board.",
                422,
                {"design_system_id": design_system_id, "board_id": board_id},
            )
        return await get_design_system_store().upsert_board_link(
            self.db,
            board_id=board_id,
            design_system_id=design_system_id,
            design_system_version=ds.version,
        )

    async def unlink_design_system_from_board(self, board_id: str) -> bool:
        return await get_design_system_store().delete_board_link(
            self.db,
            board_id=board_id,
        )

    async def get_board_effective_design_system(self, board_id: str) -> dict[str, Any] | None:
        """Resolve the board's EFFECTIVE Design System from real persisted state: an
        explicit ``BoardDesignSystem`` link takes precedence, else the umbrella default
        snapshot on ``Board.default_config_snapshot['design_system']``. Returns ``None``
        when the board has no effective Design System. Never fabricates a ref."""
        store = get_design_system_store()
        link = await store.get_board_link(self.db, board_id=board_id)
        if link is not None:
            ds = await store.get(
                self.db,
                design_system_id=link.design_system_id,
            )
            return {
                "source": "board_link",
                "design_system_id": link.design_system_id,
                "version": link.design_system_version,
                "title": ds.title if ds else None,
                "status": ds.status if ds else None,
                "scope": ds.scope if ds else None,
                "exists": ds is not None,
            }
        snapshot = await store.get_board_snapshot(self.db, board_id=board_id)
        if snapshot:
            return {
                "source": "default_snapshot",
                "design_system_id": snapshot.get("design_system_id"),
                "version": snapshot.get("version"),
                "gate_mode": snapshot.get("gate_mode"),
            }
        return None


# ---------------------------------------------------------------------------
# MockupDesignSystemGate (spec 3a006f65 / card 0192f58d)
# ---------------------------------------------------------------------------


def normalize_design_system_ref(
    design_system_ref: Any, design_system_version: Any = None
) -> dict[str, Any] | None:
    """Normalize the external mockup contract (``design_system_ref`` as an id string OR
    an object, plus an optional ``design_system_version``) into the stored shape
    ``{design_system_id, version}`` (FR6 / card decision Q2). Returns ``None`` when no
    ref is supplied."""
    if design_system_ref in (None, "", {}):
        return None
    if isinstance(design_system_ref, dict):
        ds_id = design_system_ref.get("design_system_id") or design_system_ref.get("id")
        version = design_system_ref.get("version", design_system_version)
    else:
        ds_id = str(design_system_ref)
        version = design_system_version
    if not ds_id:
        return None
    out: dict[str, Any] = {"design_system_id": ds_id}
    if version is not None:
        out["version"] = version
    return out


def _as_dict(screen: Any) -> dict[str, Any]:
    if hasattr(screen, "model_dump"):
        return screen.model_dump()
    return dict(screen) if isinstance(screen, dict) else {}


class MockupDesignSystemGate:
    """Mockup submission gate (NOT a Resource Gate type). On ScreenMockup create/update
    it resolves the board's REAL effective Design System (persisted ``BoardDesignSystem``
    /snapshot — never the client payload) and the CANONICAL
    ``BoardSettings.design_system_gate_mode`` (never the ref/snapshot mirror), then:

    * off / board with NO Design System configured -> ``not_applicable`` (never blocks,
      even under blocking — AC8);
    * Design System configured but BROKEN (dangling link / deleted-invisible DS) ->
      ``design_system_not_found`` in blocking (advisory: persist + warning);
    * blocking with a real effective DS -> reject BEFORE persistence on missing ref
      (``design_system_required``), synthetic/non-existent/mismatched-identity ref
      (``design_system_not_found``), version mismatch (``design_system_version_mismatch``)
      or missing evidence (``design_system_evidence_missing``);
    * advisory -> persist + structured warning + a queryable ``DesignSystemGateAudit`` row.

    Delta-only: the service-layer path only evaluates new or gate-relevant-changed
    mockups so flipping to blocking never re-rejects untouched legacy mockups. A screen
    already gated in the same transaction (db.info marker) is skipped to avoid
    double-gating across the MCP tool + the service-layer guard."""

    def __init__(self, db: Any) -> None:
        self.db = db
        self._svc = DesignSystemService(db)

    # -- resolution --------------------------------------------------------

    async def _gate_mode(self, board_id: str) -> str:
        settings = await get_design_system_store().get_board_settings(
            self.db,
            board_id=board_id,
        )
        return settings.get("design_system_gate_mode", "off") or "off"

    async def _effective(self, board_id: str) -> tuple[str, dict[str, Any] | None]:
        """Return (kind, effective) where kind is 'none' (no DS configured),
        'dangling' (configured but the DS row is missing/invisible), or 'real'."""
        effective = await self._svc.get_board_effective_design_system(board_id)
        if effective is None:
            return "none", None
        ds_id = effective.get("design_system_id")
        if not ds_id:
            return "dangling", effective
        if effective.get("source") == "board_link":
            real = bool(effective.get("exists"))
        else:  # default_snapshot — confirm the snapshot id resolves to a real row.
            real = (
                await get_design_system_store().get(
                    self.db,
                    design_system_id=str(ds_id),
                )
            ) is not None
        return ("real" if real else "dangling"), effective

    # -- marking (anti double-gate within a transaction) -------------------

    def _marked_ids(self) -> set:
        return get_design_system_store().marked_screen_ids(self.db)

    def _mark(self, screen: dict[str, Any]) -> None:
        sid = screen.get("id")
        if sid:
            self._marked_ids().add(sid)

    # -- core evaluation ---------------------------------------------------

    async def evaluate_screen(
        self,
        board_id: str,
        screen: dict[str, Any],
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
        mark: bool = True,
    ) -> dict[str, Any]:
        """Evaluate ONE mockup. Returns an outcome dict. Raises ``DesignSystemError``
        (pre-persistence) in blocking on violation; writes an audit row + returns a
        warning outcome in advisory."""
        screen = _as_dict(screen)
        gate_mode = await self._gate_mode(board_id)
        if gate_mode == "off":
            return {"mode": "off", "outcome": "not_applicable"}

        kind, effective = await self._effective(board_id)
        if kind == "none":
            # AC8: a board with NO Design System configured never blocks — even blocking.
            return {
                "mode": gate_mode,
                "outcome": "not_applicable",
                "reason": "no_effective_design_system",
            }

        expected_id = effective.get("design_system_id") if effective else None
        expected_version = effective.get("version") if effective else None
        reason = await self._violation_reason(kind, expected_id, expected_version, screen)

        if reason is None:
            if mark:
                self._mark(screen)
            return {
                "mode": gate_mode,
                "outcome": "pass",
                "expected_design_system_id": expected_id,
                "expected_design_system_version": expected_version,
            }

        if gate_mode == "blocking":
            # Raise BEFORE any persistence/version bump. The caller's transaction
            # discards on exit-without-commit, so nothing is durably written.
            raise DesignSystemError(reason, _GATE_MESSAGES[reason], 422, {
                "board_id": board_id,
                "mockup_id": screen.get("id"),
                "expected_design_system_id": expected_id,
                "expected_design_system_version": expected_version,
                "provided_ref": screen.get("design_system_ref"),
            })

        # advisory: persist the mockup, record a queryable warning audit row.
        get_design_system_store().add_gate_audit(
            self.db,
            DesignSystemGateAuditRecord(
                board_id=board_id,
                entity_type=entity_type,
                entity_id=entity_id,
                mockup_id=screen.get("id"),
                mode="advisory",
                outcome="warning",
                reason=reason,
                expected_design_system_id=expected_id,
                expected_design_system_version=expected_version,
                provided_ref=screen.get("design_system_ref"),
            )
        )
        if mark:
            self._mark(screen)
        return {
            "mode": "advisory",
            "outcome": "warning",
            "reason": reason,
            "expected_design_system_id": expected_id,
            "expected_design_system_version": expected_version,
        }

    async def _violation_reason(
        self,
        kind: str,
        expected_id: str | None,
        expected_version: Any,
        screen: dict[str, Any],
    ) -> str | None:
        """Return the reason code for a non-compliant screen, or ``None`` if compliant.
        Identity is resolved from the REAL effective DS; the payload ref is only a
        cross-check (kill#2 — never the source of identity)."""
        if kind == "dangling":
            # Configured but broken (dangling link / deleted-invisible DS): fail-closed.
            return GATE_NOT_FOUND
        ref = screen.get("design_system_ref") or None
        provided_id = ref.get("design_system_id") if isinstance(ref, dict) else None
        provided_version = ref.get("version") if isinstance(ref, dict) else None
        if not ref or not provided_id:
            return GATE_REQUIRED
        provided_ds = await get_design_system_store().get(
            self.db,
            design_system_id=str(provided_id),
        )
        if provided_ds is None or provided_id != expected_id:
            # synthetic / non-existent / not the board's real effective identity.
            return GATE_NOT_FOUND
        if expected_version is not None and provided_version != expected_version:
            return GATE_VERSION_MISMATCH
        evidence = screen.get("design_system_evidence")
        if evidence in (None, "", {}, []):
            return GATE_EVIDENCE_MISSING
        return None

    # -- delta (service-layer / bulk path) ---------------------------------

    async def gate_delta(
        self,
        board_id: str,
        old_screens: list[Any] | None,
        new_screens: list[Any] | None,
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Gate only NEW or gate-relevant-CHANGED mockups (delta-only). Unchanged
        existing mockups and screens already gated in this transaction are skipped.
        Raises on the first blocking violation (pre-persistence)."""
        old_by_id = {
            s.get("id"): s for s in (_as_dict(x) for x in (old_screens or [])) if s.get("id")
        }
        marked = self._marked_ids()
        outcomes: list[dict[str, Any]] = []
        for raw in new_screens or []:
            screen = _as_dict(raw)
            sid = screen.get("id")
            if sid and sid in marked:
                continue  # already gated in this transaction (MCP tool path)
            old = old_by_id.get(sid) if sid else None
            if old is not None and not self._gate_relevant_changed(old, screen):
                continue  # unchanged existing mockup -> legacy-safe
            outcomes.append(
                await self.evaluate_screen(
                    board_id, screen, entity_type=entity_type, entity_id=entity_id, mark=True
                )
            )
        return outcomes

    @staticmethod
    def _gate_relevant_changed(old: dict[str, Any], new: dict[str, Any]) -> bool:
        return any(old.get(k) != new.get(k) for k in _GATE_RELEVANT_FIELDS)


_GATE_MESSAGES = {
    GATE_REQUIRED: "This board enforces a Design System: the mockup must reference the "
    "board's effective Design System (design_system_ref).",
    GATE_NOT_FOUND: "The Design System reference does not resolve to the board's real "
    "effective Design System (synthetic, non-existent, or dangling).",
    GATE_VERSION_MISMATCH: "The mockup's Design System version does not match the board's "
    "effective Design System version.",
    GATE_EVIDENCE_MISSING: "The mockup is missing the required Design System consumption "
    "evidence.",
}


async def gate_entity_screen_mockups(
    db: Any,
    entity: Any,
    new_screens: list[Any] | None,
    *,
    entity_type: str | None = None,
) -> list[dict[str, Any]]:
    """Service-layer entry point: gate a bulk screen_mockups update on an entity that
    carries a ``board_id`` (Spec/Ideation/Refinement/Card/Story). Resolves board_id off
    the entity and runs the delta-only gate against the entity's CURRENT mockups. Raises
    ``DesignSystemError`` (blocking) before the caller persists; returns advisory
    outcomes. No-op when the entity has no board_id."""
    board_id = getattr(entity, "board_id", None)
    if not board_id:
        return []
    old_screens = list(getattr(entity, "screen_mockups", None) or [])
    return await MockupDesignSystemGate(db).gate_delta(
        board_id,
        old_screens,
        new_screens,
        entity_type=entity_type,
        entity_id=getattr(entity, "id", None),
    )
