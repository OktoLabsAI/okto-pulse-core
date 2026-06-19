"""Path B AmendmentHotfixRevision store/service (spec 7ea1e4be, card f5dca74a).

I/O layer: persistence + audit for the amendment lineage artifact. The pure
``status x lineage_state`` eligibility predicate lives in
``core.domain.amendment_eligibility`` (imported here, never the reverse), so the
bug-regression resolver/gate can read eligibility without this DB service. This
service NEVER mutates the original spec (AC1) — it only writes amendment rows
and emits audit entries on create/status/lineage changes (TR5).
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.domain.amendment_eligibility import (
    COVERAGE_CONFIRMATION_KEY,
    AmendmentEligibility,
    AmendmentLineageState,
    AmendmentRevisionStatus,
    evaluate_amendment_eligibility,
)

from okto_pulse.core.models.db import ActivityLog, AmendmentHotfixRevision

logger = logging.getLogger("okto_pulse.services.amendment_revision")


class AmendmentRevisionError(ValueError):
    """Raised on an invalid amendment write (unknown status/lineage, not found).

    Fail-closed: an unknown status or lineage value is never silently coerced to
    a valid one (spec 7ea1e4be — unknown states are blocking)."""


def _coerce_status(value: AmendmentRevisionStatus | str) -> AmendmentRevisionStatus:
    if isinstance(value, AmendmentRevisionStatus):
        return value
    try:
        return AmendmentRevisionStatus(value)
    except (ValueError, TypeError) as exc:
        raise AmendmentRevisionError(f"unknown_amendment_status: {value!r}") from exc


def _coerce_lineage(value: AmendmentLineageState | str) -> AmendmentLineageState:
    if isinstance(value, AmendmentLineageState):
        return value
    try:
        return AmendmentLineageState(value)
    except (ValueError, TypeError) as exc:
        raise AmendmentRevisionError(f"unknown_lineage_state: {value!r}") from exc


class AmendmentRevisionService:
    """Create/read/update Path B amendment revisions with audit (TR5)."""

    def __init__(self, db) -> None:
        self.db = db

    async def create(
        self,
        *,
        board_id: str,
        original_spec_id: str,
        origin_bug_id: str,
        author: str,
        origin_task_ids: list[str] | None = None,
        affected_task_ids: list[str] | None = None,
        revision_spec_id: str | None = None,
        regression_scenario_ids: list[str] | None = None,
        regression_test_task_ids: list[str] | None = None,
        automated_regression_refs: list[str] | None = None,
        validation_metadata: dict | None = None,
    ) -> AmendmentHotfixRevision:
        """Persist a new amendment in DRAFT/INCOMPLETE. Never touches the
        original spec (AC1)."""
        # NON-FORGEABLE (G2 / card c9cf9781): the coverage attestation reserved
        # key may ONLY be written by ``CardService.confirm_amendment_coverage``.
        # A generic create can never inject it (would forge validator coverage),
        # so strip it here regardless of caller input.
        safe_metadata = dict(validation_metadata) if validation_metadata else None
        if safe_metadata and COVERAGE_CONFIRMATION_KEY in safe_metadata:
            safe_metadata.pop(COVERAGE_CONFIRMATION_KEY, None)
            logger.warning(
                "amendment_revision.create.stripped_reserved_key key=%s "
                "(coverage_confirmation is writable only via confirm_amendment_coverage)",
                COVERAGE_CONFIRMATION_KEY,
            )
        amendment = AmendmentHotfixRevision(
            board_id=board_id,
            original_spec_id=original_spec_id,
            origin_bug_id=origin_bug_id,
            origin_task_ids=list(origin_task_ids or []),
            affected_task_ids=list(affected_task_ids or []),
            revision_spec_id=revision_spec_id,
            regression_scenario_ids=list(regression_scenario_ids or []),
            regression_test_task_ids=list(regression_test_task_ids or []),
            automated_regression_refs=list(automated_regression_refs or []),
            status=AmendmentRevisionStatus.DRAFT,
            lineage_state=AmendmentLineageState.INCOMPLETE,
            validation_metadata=safe_metadata,
            created_by=author,
        )
        self.db.add(amendment)
        await self.db.flush()
        self._audit(
            amendment,
            "amendment_revision_created",
            author,
            {"original_spec_id": original_spec_id, "origin_bug_id": origin_bug_id},
        )
        return amendment

    async def get(self, amendment_id: str) -> AmendmentHotfixRevision | None:
        return await self.db.get(AmendmentHotfixRevision, amendment_id)

    async def list_for_bug(
        self,
        *,
        board_id: str,
        original_spec_id: str,
        origin_bug_id: str,
    ) -> list[AmendmentHotfixRevision]:
        """Read-only: amendments formally linked to this bug+spec on this board.

        Used by the bug-regression gate/preview to build pure Path B lineage
        facts (spec f5a7cae7 / card ead17e4d). Never mutates anything; the
        ``board_id``/``original_spec_id``/``origin_bug_id`` filter mirrors the
        formal-link requirement (FR2)."""
        result = await self.db.execute(
            select(AmendmentHotfixRevision).where(
                AmendmentHotfixRevision.board_id == board_id,
                AmendmentHotfixRevision.original_spec_id == original_spec_id,
                AmendmentHotfixRevision.origin_bug_id == origin_bug_id,
            )
        )
        return list(result.scalars())

    async def _require(self, amendment_id: str) -> AmendmentHotfixRevision:
        amendment = await self.get(amendment_id)
        if amendment is None:
            raise AmendmentRevisionError(f"amendment_not_found: {amendment_id}")
        return amendment

    async def set_status(
        self, amendment_id: str, new_status: AmendmentRevisionStatus | str, actor: str
    ) -> AmendmentHotfixRevision:
        amendment = await self._require(amendment_id)
        status = _coerce_status(new_status)  # fail-closed on unknown
        old = amendment.status
        amendment.status = status
        await self.db.flush()
        self._audit(
            amendment,
            "amendment_revision_status_changed",
            actor,
            {"old_status": getattr(old, "value", old), "new_status": status.value},
        )
        return amendment

    async def set_lineage_state(
        self, amendment_id: str, lineage_state: AmendmentLineageState | str, actor: str
    ) -> AmendmentHotfixRevision:
        amendment = await self._require(amendment_id)
        lineage = _coerce_lineage(lineage_state)  # fail-closed on unknown
        old = amendment.lineage_state
        amendment.lineage_state = lineage
        await self.db.flush()
        self._audit(
            amendment,
            "amendment_revision_lineage_changed",
            actor,
            {
                "old_lineage_state": getattr(old, "value", old),
                "new_lineage_state": lineage.value,
            },
        )
        return amendment

    async def set_coverage_confirmation(
        self,
        amendment_id: str,
        *,
        confirmation: dict[str, Any],
        actor: str,
    ) -> AmendmentHotfixRevision:
        """Persist the validator coverage attestation under the reserved key (G2).

        This is the ONLY writer of ``validation_metadata.coverage_confirmation``.
        The caller (``CardService.confirm_amendment_coverage``) MUST have already
        authorized the validator role and validated the artifact binding; this
        method only persists + audits (non-forgeability is enforced by the caller
        + the reserved-key strip in ``create``)."""
        amendment = await self._require(amendment_id)
        metadata = dict(amendment.validation_metadata or {})
        metadata[COVERAGE_CONFIRMATION_KEY] = dict(confirmation)
        amendment.validation_metadata = metadata
        flag_modified(amendment, "validation_metadata")
        await self.db.flush()
        self._audit(
            amendment,
            "amendment_coverage_confirmed",
            actor,
            {
                "regression_test_task_id": confirmation.get("regression_test_task_id"),
                "regression_scenario_id": confirmation.get("regression_scenario_id"),
            },
        )
        return amendment

    async def associate_artifacts(
        self,
        amendment_id: str,
        *,
        regression_test_task_ids: list[str] | None = None,
        regression_scenario_ids: list[str] | None = None,
        automated_regression_refs: list[str] | None = None,
        actor: str,
    ) -> AmendmentHotfixRevision:
        """Additively associate regression artifacts/evidence to an existing
        amendment (spec be089cd3 / card 4e7e1143). NEVER reparents
        origin_bug_id/original_spec_id (caller enforces bug/spec scope). Audit-
        backed, order-preserving de-dup, no silent no-op."""
        amendment = await self._require(amendment_id)

        def _merge(existing: list[str] | None, incoming: list[str] | None) -> list[str]:
            out = list(existing or [])
            seen = set(out)
            for value in incoming or []:
                text = str(value)
                if text not in seen:
                    seen.add(text)
                    out.append(text)
            return out

        amendment.regression_test_task_ids = _merge(
            amendment.regression_test_task_ids, regression_test_task_ids
        )
        amendment.regression_scenario_ids = _merge(
            amendment.regression_scenario_ids, regression_scenario_ids
        )
        amendment.automated_regression_refs = _merge(
            amendment.automated_regression_refs, automated_regression_refs
        )
        for column in (
            "regression_test_task_ids",
            "regression_scenario_ids",
            "automated_regression_refs",
        ):
            flag_modified(amendment, column)
        await self.db.flush()
        self._audit(
            amendment,
            "amendment_revision_artifacts_associated",
            actor,
            {
                "regression_test_task_ids": list(regression_test_task_ids or []),
                "regression_scenario_ids": list(regression_scenario_ids or []),
                "automated_regression_refs": list(automated_regression_refs or []),
            },
        )
        return amendment

    @staticmethod
    def eligibility(amendment: AmendmentHotfixRevision) -> AmendmentEligibility:
        """Deterministic status x lineage verdict (delegates to the pure policy).

        ``lineage_eligible`` is NOT closure — final bug closure also needs the
        shared predicate + coverage_confirmed from later cards.
        """
        return evaluate_amendment_eligibility(amendment.status, amendment.lineage_state)

    def _audit(
        self,
        amendment: AmendmentHotfixRevision,
        action: str,
        actor: str,
        extra: dict[str, Any],
    ) -> None:
        self.db.add(
            ActivityLog(
                board_id=amendment.board_id,
                card_id=amendment.origin_bug_id,
                action=action,
                actor_type="agent",
                actor_id=actor,
                actor_name=actor,
                details={"amendment_id": amendment.id, **extra},
            )
        )


__all__ = ["AmendmentRevisionService", "AmendmentRevisionError"]
