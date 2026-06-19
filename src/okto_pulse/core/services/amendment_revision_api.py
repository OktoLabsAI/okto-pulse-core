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

from okto_pulse.core.domain.amendment_eligibility import AmendmentRevisionStatus
from okto_pulse.core.models.db import Card, CardType, Spec, SpecStatus
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.bug_regression_preview import (
    BugRegressionScenarioPreviewError,
    BugRegressionScenarioPreviewService,
)

#: Path B amendments only attach to a done/locked (validated) original spec — an
#: in_progress/draft spec is still editable, so it needs no amendment.
_DONE_OR_LOCKED_SPEC_STATUSES = frozenset({SpecStatus.VALIDATED, SpecStatus.DONE})

#: Bypass-intent field names rejected fail-closed on any write surface (FR5).
BYPASS_FIELD_NAMES = frozenset(
    {"skip_gate", "override_gate", "bypass", "force", "skip", "ignore_gate"}
)


@dataclass(frozen=True)
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
        if spec.status not in _DONE_OR_LOCKED_SPEC_STATUSES:
            raise AmendmentRevisionApiError(
                "original_spec_not_done_or_locked",
                f"Original spec '{resolved_spec_id}' is '{getattr(spec.status, 'value', spec.status)}'. "
                "Path B amendments only attach to a done/validated (locked) spec; "
                "edit the spec directly while it is still in progress.",
                status_code=409,
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

    # -- internals ---------------------------------------------------------

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
