"""Bug-scoped orchestration for AmendmentHotfixRevision create/list/get/associate
(spec be089cd3 / card 4e7e1143). Shared by REST (api/amendment_revisions.py) and
the MCP twin tools (ir_54ceb69b) so both surfaces return the SAME structured
payload + reason codes + mutation restrictions.

Fail-closed + never a bypass (FR5): there is NO skip_gate/override_gate path here;
the REST request models forbid extra fields and the MCP tools have no such args.
Every mutation is audit-backed (delegated to AmendmentRevisionService) and every
failure is a structured ``AmendmentRevisionApiError`` (code/message/status_code +
to_dict) — never a silent no-op (TR4).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
)
from okto_pulse.core.domain.enums import CardType, SpecStatus
from okto_pulse.core.models.db import Card, Spec
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.bug_regression_preview import (
    BugRegressionScenarioPreviewError,
    BugRegressionScenarioPreviewService,
)

#: Spec statuses that are ALWAYS content-locked (immutable) — a Path B amendment
#: always attaches here. ``in_progress`` is handled separately by the
#: content-lock-aware predicate below (it may be locked or still editable).
_DONE_OR_LOCKED_SPEC_STATUSES = frozenset({SpecStatus.VALIDATED, SpecStatus.DONE})


def _path_b_eligible(spec: Spec) -> bool:
    """Content-lock-aware Path B eligibility (spec 62cf2d36, fr_0d2f84a1/fr_58b6aa0b).

    A bug's original spec admits a Path B amendment when it is either:

    - ``validated``/``done`` (always content-locked / immutable), OR
    - ``in_progress`` AND currently content-locked — its ``current_validation_id``
      points to a validation with ``outcome='success'`` (a validated spec moved to
      in_progress for execution).

    ``in_progress`` WITHOUT an active passed validation is still editable, so the
    spec should be edited directly; a ``failed``/``stale``/``superseded`` validation
    is NOT a lock. This is the exact dual of
    ``services.main.spec_is_content_locked`` so the content-lock gate and Path B can
    never contradict — there is no spec that is content-locked (cannot be edited)
    yet Path-B-ineligible (the catch-22 this fix removes).
    """
    if spec.status in _DONE_OR_LOCKED_SPEC_STATUSES:
        return True
    if spec.status != SpecStatus.IN_PROGRESS:
        return False
    # Deferred import: services.main is a heavy module; importing it lazily here
    # avoids a module-load cycle (main imports the amendment service, not this API).
    from okto_pulse.core.services.main import spec_is_content_locked

    return spec_is_content_locked(spec)

#: Bypass-intent field names rejected fail-closed on any write surface (FR5).
BYPASS_FIELD_NAMES = frozenset(
    {"skip_gate", "override_gate", "bypass", "force", "skip", "ignore_gate"}
)


@dataclass  # NOT frozen: an Exception must stay mutable (Python sets __traceback__ on
# propagation; a frozen dataclass raises FrozenInstanceError -> 500). See DesignSystemError.
class AmendmentRevisionApiError(Exception):
    """Structured error for REST + MCP (mirrors BugRegressionScenarioPreviewError).

    The payload is enough for an agent to know the next safe action without
    parsing raw exception text (AC3)."""

    code: str
    message: str
    status_code: int = 400
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "message": self.message,
            "status_code": self.status_code,
            **({"details": self.details} if self.details else {}),
        }


def reject_bypass_fields(payload: dict[str, Any] | None) -> None:
    """FR5: explicitly refuse any gate-bypass intent on a write surface."""
    for key in payload or {}:
        if str(key).lower() in BYPASS_FIELD_NAMES:
            raise AmendmentRevisionApiError(
                code="gate_bypass_not_allowed",
                message=(
                    f"Field '{key}' is not allowed: MCP/API only REMEDIATE the bug "
                    "regression gate, they never skip or override it."
                ),
                status_code=400,
            )


class AmendmentRevisionApiService:
    """Validate + orchestrate amendment-revision operations for a bug."""

    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self._store = AmendmentRevisionService(db)

    async def create(
        self,
        *,
        board_id: str,
        bug_id: str,
        author: str,
        original_spec_id: str | None = None,
        initial_status: str | None = None,
        origin_task_ids: list[str] | None = None,
        affected_task_ids: list[str] | None = None,
        revision_spec_id: str | None = None,
        regression_scenario_ids: list[str] | None = None,
        regression_test_task_ids: list[str] | None = None,
        automated_regression_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        bug = await self._require_bug(board_id, bug_id)

        # original_spec_id binds to the bug's spec (FR2). Default to it; reject a
        # mismatching explicit value (never reparent across specs).
        resolved_spec_id = original_spec_id or bug.spec_id
        if resolved_spec_id != bug.spec_id:
            raise AmendmentRevisionApiError(
                "bug_spec_mismatch",
                f"original_spec_id '{resolved_spec_id}' does not match the bug's "
                f"spec '{bug.spec_id}'. A Path B amendment binds to the bug's own spec.",
                status_code=422,
            )

        spec = await self._db.get(Spec, resolved_spec_id)
        if spec is None or spec.board_id != board_id:
            raise AmendmentRevisionApiError(
                "original_spec_not_found",
                f"Original spec '{resolved_spec_id}' was not found on this board.",
                status_code=404,
            )
        if not _path_b_eligible(spec):
            status_label = getattr(spec.status, "value", spec.status)
            editable_hint = (
                " It is in_progress but NOT content-locked, so it is still editable — "
                "edit the spec directly instead of opening a Path B amendment."
                if spec.status == SpecStatus.IN_PROGRESS
                else ""
            )
            raise AmendmentRevisionApiError(
                "original_spec_not_done_or_locked",
                f"Original spec '{resolved_spec_id}' is '{status_label}' and not "
                "content-locked. Path B amendments attach to a done/validated spec, "
                "or to an in_progress spec that is still content-locked by an active "
                f"passed validation (current_validation_id -> outcome=success).{editable_hint}",
                status_code=409,
                details={"spec_status": str(status_label), "content_locked": False},
            )

        # initial_status: draft-only. Never let a create mint approved/done
        # (that would skip lifecycle gates).
        if initial_status is not None and str(initial_status).lower() != AmendmentRevisionStatus.DRAFT.value:
            raise AmendmentRevisionApiError(
                "invalid_initial_status",
                f"initial_status '{initial_status}' is not allowed: a new amendment "
                "must start as 'draft' (status changes go through the lifecycle).",
                status_code=422,
            )

        amendment = await self._store.create(
            board_id=board_id,
            original_spec_id=resolved_spec_id,
            origin_bug_id=bug_id,
            author=author,
            origin_task_ids=origin_task_ids,
            affected_task_ids=affected_task_ids,
            revision_spec_id=revision_spec_id,
            regression_scenario_ids=regression_scenario_ids,
            regression_test_task_ids=regression_test_task_ids,
            automated_regression_refs=automated_regression_refs,
            # NEVER accept coverage_confirmation here — the reserved key is
            # writable only by the validator-only confirm_amendment_coverage
            # writer (non-forgeable). create() also strips it defensively.
            validation_metadata=None,
        )
        # Load server-side columns (created_at/updated_at) inside the async
        # context before serializing — avoids a lazy MissingGreenlet.
        await self._db.refresh(amendment)
        return self._serialize_revision(amendment)

    async def get(self, *, board_id: str, bug_id: str, amendment_id: str) -> dict[str, Any]:
        await self._require_bug(board_id, bug_id)
        amendment = await self._require_scoped_amendment(board_id, bug_id, amendment_id)
        return self._serialize_revision(amendment)

    async def list_for_bug(self, *, board_id: str, bug_id: str) -> dict[str, Any]:
        bug = await self._require_bug(board_id, bug_id)
        amendments = await self._store.list_for_bug(
            board_id=board_id,
            original_spec_id=bug.spec_id,
            origin_bug_id=bug_id,
        )
        revisions = [self._serialize_revision(a) for a in amendments]
        candidate_scenarios = sorted(
            {sid for a in amendments for sid in (a.regression_scenario_ids or [])}
        )
        return {
            "board_id": board_id,
            "bug_id": bug_id,
            "original_spec_id": bug.spec_id,
            "revisions": revisions,
            # FR2: bug-level Path B resolution payload (lineage_state, missing_links,
            # safe_next_actions, rejected/eligible artifacts, coverage_state). Same
            # source as the gate/preview (parity).
            "path_b_resolution": await self._path_b_resolution(
                board_id, bug_id, candidate_scenarios
            ),
        }

    async def associate(
        self,
        *,
        board_id: str,
        bug_id: str,
        amendment_id: str,
        actor: str,
        regression_test_task_ids: list[str] | None = None,
        regression_scenario_ids: list[str] | None = None,
        automated_regression_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        await self._require_bug(board_id, bug_id)
        await self._require_scoped_amendment(board_id, bug_id, amendment_id)
        if not any(
            (regression_test_task_ids, regression_scenario_ids, automated_regression_refs)
        ):
            raise AmendmentRevisionApiError(
                "no_artifacts_to_associate",
                "Provide at least one of regression_test_task_ids, "
                "regression_scenario_ids or automated_regression_refs.",
                status_code=422,
            )
        amendment = await self._store.associate_artifacts(
            amendment_id,
            regression_test_task_ids=regression_test_task_ids,
            regression_scenario_ids=regression_scenario_ids,
            automated_regression_refs=automated_regression_refs,
            actor=actor,
        )
        await self._db.refresh(amendment)  # load onupdate updated_at (no MissingGreenlet)
        return self._serialize_revision(amendment)

    async def transition_lifecycle(
        self,
        *,
        board_id: str,
        bug_id: str,
        amendment_id: str,
        actor: str,
        status: str | None = None,
        lineage_state: str | None = None,
    ) -> dict[str, Any]:
        """Transition an amendment's lifecycle (status and/or lineage_state) for a
        bug — the agent-facing step that lets a created/associated amendment reach
        ``approved``/``done`` + complete lineage (card 14ddfab0). Fail-closed:

        * unknown status/lineage are rejected (``invalid_amendment_status`` /
          ``invalid_lineage_state``);
        * ``lineage_state=complete`` needs declared regression artifacts + the bug's
          authoritative origin-task membership (``incomplete_lineage_artifacts``);
        * promotion to a non-blocking status (``approved``/``done``) needs lineage
          already complete (``cannot_promote_incomplete_lineage``);
        * a ``cancelled``/``superseded`` revision is terminal and can NOT be
          promoted back to ``approved``/``done`` (``terminal_amendment_revision``) —
          open a new revision instead;
        * it NEVER writes coverage (there is no coverage param); the bug stays
          ``coverage_pending`` until the validator runs
          ``confirm_amendment_coverage``.
        """
        if status is None and lineage_state is None:
            raise AmendmentRevisionApiError(
                "no_lifecycle_change",
                "Provide status and/or lineage_state to transition.",
                422,
            )
        bug = await self._require_bug(board_id, bug_id)
        amendment = await self._require_scoped_amendment(board_id, bug_id, amendment_id)

        new_status = self._coerce_status_or_error(status) if status is not None else None
        new_lineage = (
            self._coerce_lineage_or_error(lineage_state) if lineage_state is not None else None
        )

        promoted = {AmendmentRevisionStatus.APPROVED.value, AmendmentRevisionStatus.DONE.value}
        terminal = {AmendmentRevisionStatus.CANCELLED.value, AmendmentRevisionStatus.SUPERSEDED.value}
        current_status_val = getattr(amendment.status, "value", amendment.status)

        # Q4: a terminal revision can never be resurrected to approved/done.
        if current_status_val in terminal and new_status is not None and new_status.value in promoted:
            raise AmendmentRevisionApiError(
                "terminal_amendment_revision",
                f"Amendment '{amendment_id}' is '{current_status_val}' (terminal) and cannot be "
                "promoted to approved/done. Create a new amendment revision instead.",
                409,
            )

        effective_lineage_val = (
            new_lineage.value
            if new_lineage is not None
            else getattr(amendment.lineage_state, "value", amendment.lineage_state)
        )

        # Q2: lineage_state=complete requires sufficient declared artifacts + membership.
        if new_lineage is AmendmentLineageState.COMPLETE and not self._has_lineage_artifacts(
            amendment, bug
        ):
            raise AmendmentRevisionApiError(
                "incomplete_lineage_artifacts",
                "lineage_state=complete requires at least one regression_scenario_id, one "
                "regression_test_task_id, and the bug's authoritative origin task in "
                "origin_task_ids/affected_task_ids.",
                409,
            )

        # Q2: promotion to a non-blocking status requires complete lineage.
        if (
            new_status is not None
            and new_status.value in promoted
            and effective_lineage_val != AmendmentLineageState.COMPLETE.value
        ):
            raise AmendmentRevisionApiError(
                "cannot_promote_incomplete_lineage",
                f"Cannot set status='{new_status.value}' while lineage_state is not complete. "
                "Complete the lineage first.",
                409,
            )

        # Apply lineage before status so a combined call lands consistently. Coverage
        # is NEVER touched here — it stays validator-only via confirm_amendment_coverage.
        if new_lineage is not None:
            amendment = await self._store.set_lineage_state(amendment_id, new_lineage, actor)
        if new_status is not None:
            amendment = await self._store.set_status(amendment_id, new_status, actor)
        await self._db.refresh(amendment)
        return self._serialize_revision(amendment)

    # -- internals ---------------------------------------------------------

    def _coerce_status_or_error(self, value: str) -> AmendmentRevisionStatus:
        try:
            return AmendmentRevisionStatus(str(value))
        except ValueError:
            raise AmendmentRevisionApiError(
                "invalid_amendment_status",
                f"Unknown amendment status '{value}'. Allowed: "
                f"{[s.value for s in AmendmentRevisionStatus]}.",
                422,
            ) from None

    def _coerce_lineage_or_error(self, value: str) -> AmendmentLineageState:
        try:
            return AmendmentLineageState(str(value))
        except ValueError:
            raise AmendmentRevisionApiError(
                "invalid_lineage_state",
                f"Unknown lineage_state '{value}'. Allowed: "
                f"{[s.value for s in AmendmentLineageState]}.",
                422,
            ) from None

    @staticmethod
    def _has_lineage_artifacts(amendment, bug) -> bool:
        membership = set(amendment.origin_task_ids or []) | set(amendment.affected_task_ids or [])
        return (
            bool(amendment.regression_scenario_ids)
            and bool(amendment.regression_test_task_ids)
            and bug.origin_task_id in membership
        )

    async def _require_bug(self, board_id: str, bug_id: str) -> Card:
        bug = await self._db.get(Card, bug_id)
        if bug is None or bug.board_id != board_id:
            raise AmendmentRevisionApiError(
                "bug_not_found", f"Bug '{bug_id}' was not found on this board.", 404
            )
        if getattr(bug.card_type, "value", bug.card_type) != CardType.BUG.value:
            raise AmendmentRevisionApiError(
                "not_bug_card", f"Card '{bug_id}' is not a bug card.", 400
            )
        if not bug.spec_id:
            raise AmendmentRevisionApiError(
                "bug_spec_missing", f"Bug '{bug_id}' is not linked to a spec.", 422
            )
        return bug

    async def _require_scoped_amendment(self, board_id: str, bug_id: str, amendment_id: str):
        amendment = await self._store.get(amendment_id)
        if amendment is None:
            raise AmendmentRevisionApiError(
                "amendment_not_found",
                f"Amendment revision '{amendment_id}' was not found.",
                404,
            )
        # scope check — NEVER leak a foreign amendment as success (no reparenting).
        if amendment.board_id != board_id or amendment.origin_bug_id != bug_id:
            raise AmendmentRevisionApiError(
                "amendment_bug_mismatch",
                f"Amendment '{amendment_id}' does not belong to bug '{bug_id}' on this board.",
                409,
            )
        return amendment

    async def _path_b_resolution(
        self, board_id: str, bug_id: str, candidate_scenario_ids: list[str]
    ) -> dict[str, Any]:
        try:
            payload = await BugRegressionScenarioPreviewService(self._db).resolve(
                board_id=board_id,
                bug_id=bug_id,
                candidate_scenario_ids=candidate_scenario_ids or None,
            )
        except BugRegressionScenarioPreviewError as exc:
            # structured, not an exception leak (AC3): surface why the bug-level
            # resolution could not be computed.
            return {"available": False, **exc.to_dict()}
        return {
            "available": True,
            "coverage_state": payload.get("coverage_state"),
            "coverage_pending_scenarios": payload.get("coverage_pending_scenarios"),
            "missing_links": payload.get("missing_links"),
            "safe_next_actions": payload.get("safe_next_actions"),
            "next_action": payload.get("next_action"),
            "eligible_regression_artifacts": payload.get("eligible_regression_artifacts"),
            "rejected_regression_artifacts": payload.get("rejected_regression_artifacts"),
            "rejected_scenarios": payload.get("rejected_scenarios"),
            "amendment_revision_id": payload.get("amendment_revision_id"),
        }

    def _serialize_revision(self, amendment) -> dict[str, Any]:
        verdict = AmendmentRevisionService.eligibility(amendment)
        return {
            "id": amendment.id,
            "board_id": amendment.board_id,
            "original_spec_id": amendment.original_spec_id,
            "origin_bug_id": amendment.origin_bug_id,
            "revision_spec_id": amendment.revision_spec_id,
            "status": getattr(amendment.status, "value", amendment.status),
            "lineage_state": getattr(amendment.lineage_state, "value", amendment.lineage_state),
            "origin_task_ids": list(amendment.origin_task_ids or []),
            "affected_task_ids": list(amendment.affected_task_ids or []),
            "regression_scenario_ids": list(amendment.regression_scenario_ids or []),
            "regression_test_task_ids": list(amendment.regression_test_task_ids or []),
            "automated_regression_refs": list(amendment.automated_regression_refs or []),
            "eligibility": {
                "lineage_eligible": verdict.lineage_eligible,
                "canonicalization_candidate": verdict.canonicalization_candidate,
                "blocked": verdict.blocked,
                "reason_code": verdict.reason_code,
            },
            "created_at": _iso(getattr(amendment, "created_at", None)),
            "updated_at": _iso(getattr(amendment, "updated_at", None)),
        }


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    iso = getattr(value, "isoformat", None)
    return iso() if callable(iso) else str(value)


__all__ = [
    "AmendmentRevisionApiService",
    "AmendmentRevisionApiError",
    "reject_bypass_fields",
    "BYPASS_FIELD_NAMES",
]
