"""Core amendment/hotfix revision rules over an edition-owned store."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.domain.amendment_eligibility import (
    COVERAGE_CONFIRMATION_KEY,
    AmendmentEligibility,
    AmendmentLineageState,
    AmendmentRevisionStatus,
    evaluate_amendment_eligibility,
)
from okto_pulse.core.ports.amendment_revision import (
    AmendmentAuditRecord,
    AmendmentRevisionRecord,
    get_amendment_revision_store,
)

logger = logging.getLogger("okto_pulse.services.amendment_revision")


class AmendmentRevisionError(ValueError):
    """Invalid amendment write or unknown lifecycle value."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code or message.partition(":")[0]


TERMINAL_AMENDMENT_REVISION_STATUSES = frozenset(
    {
        AmendmentRevisionStatus.CANCELLED.value,
        AmendmentRevisionStatus.SUPERSEDED.value,
    }
)


def amendment_revision_is_terminal(
    status: AmendmentRevisionStatus | str,
) -> bool:
    """Return whether ``status`` makes a revision permanently immutable."""
    return getattr(status, "value", status) in TERMINAL_AMENDMENT_REVISION_STATUSES


def _require_mutable(
    amendment: AmendmentRevisionRecord,
    *,
    mutation: str,
) -> None:
    current_status = getattr(amendment.status, "value", amendment.status)
    if amendment_revision_is_terminal(current_status):
        raise AmendmentRevisionError(
            (
                "terminal_amendment_revision: "
                f"amendment '{amendment.id}' is '{current_status}' and cannot "
                f"be mutated via {mutation}; create a new amendment revision"
            ),
            code="terminal_amendment_revision",
        )


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
    def __init__(self, db: object) -> None:
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
    ) -> AmendmentRevisionRecord:
        safe_metadata = dict(validation_metadata) if validation_metadata else None
        if safe_metadata and COVERAGE_CONFIRMATION_KEY in safe_metadata:
            safe_metadata.pop(COVERAGE_CONFIRMATION_KEY, None)
            logger.warning(
                "amendment_revision.create.stripped_reserved_key key=%s",
                COVERAGE_CONFIRMATION_KEY,
            )
        record = AmendmentRevisionRecord(
            board_id=board_id,
            original_spec_id=original_spec_id,
            origin_bug_id=origin_bug_id,
            origin_task_ids=list(origin_task_ids or []),
            affected_task_ids=list(affected_task_ids or []),
            revision_spec_id=revision_spec_id,
            regression_scenario_ids=list(regression_scenario_ids or []),
            regression_test_task_ids=list(regression_test_task_ids or []),
            automated_regression_refs=list(automated_regression_refs or []),
            validation_metadata=safe_metadata,
            created_by=author,
        )
        return await self._save(
            record,
            "amendment_revision_created",
            author,
            {
                "original_spec_id": original_spec_id,
                "origin_bug_id": origin_bug_id,
            },
        )

    async def get(self, amendment_id: str) -> AmendmentRevisionRecord | None:
        return await get_amendment_revision_store().get(
            self.db,
            amendment_id=amendment_id,
        )

    async def list_for_bug(
        self,
        *,
        board_id: str,
        original_spec_id: str,
        origin_bug_id: str,
    ) -> list[AmendmentRevisionRecord]:
        rows = await get_amendment_revision_store().list_for_bug(
            self.db,
            board_id=board_id,
            original_spec_id=original_spec_id,
            origin_bug_id=origin_bug_id,
        )
        return list(rows)

    async def _require(self, amendment_id: str) -> AmendmentRevisionRecord:
        amendment = await self.get(amendment_id)
        if amendment is None:
            raise AmendmentRevisionError(f"amendment_not_found: {amendment_id}")
        return amendment

    async def set_status(
        self,
        amendment_id: str,
        new_status: AmendmentRevisionStatus | str,
        actor: str,
    ) -> AmendmentRevisionRecord:
        amendment = await self._require(amendment_id)
        status = _coerce_status(new_status)
        old = amendment.status
        if amendment_revision_is_terminal(old):
            # Retrying the exact terminal transition is the only operation allowed
            # after cancellation/supersession.  It is a true no-op: no timestamp
            # bump, persistence call, or duplicate audit record.
            if getattr(old, "value", old) == status.value:
                return amendment
            _require_mutable(
                amendment,
                mutation=f"set_status(target_status='{status.value}')",
            )
        amendment.status = status
        amendment.updated_at = datetime.now(timezone.utc)
        return await self._save(
            amendment,
            "amendment_revision_status_changed",
            actor,
            {"old_status": getattr(old, "value", old), "new_status": status.value},
        )

    async def set_lineage_state(
        self,
        amendment_id: str,
        lineage_state: AmendmentLineageState | str,
        actor: str,
    ) -> AmendmentRevisionRecord:
        amendment = await self._require(amendment_id)
        _require_mutable(amendment, mutation="set_lineage_state")
        lineage = _coerce_lineage(lineage_state)
        old = amendment.lineage_state
        amendment.lineage_state = lineage
        amendment.updated_at = datetime.now(timezone.utc)
        return await self._save(
            amendment,
            "amendment_revision_lineage_changed",
            actor,
            {
                "old_lineage_state": getattr(old, "value", old),
                "new_lineage_state": lineage.value,
            },
        )

    async def set_coverage_confirmation(
        self,
        amendment_id: str,
        *,
        confirmation: dict[str, Any],
        actor: str,
    ) -> AmendmentRevisionRecord:
        amendment = await self._require(amendment_id)
        _require_mutable(amendment, mutation="set_coverage_confirmation")
        metadata = dict(amendment.validation_metadata or {})
        metadata[COVERAGE_CONFIRMATION_KEY] = dict(confirmation)
        amendment.validation_metadata = metadata
        amendment.updated_at = datetime.now(timezone.utc)
        return await self._save(
            amendment,
            "amendment_coverage_confirmed",
            actor,
            {
                "regression_test_task_id": confirmation.get(
                    "regression_test_task_id"
                ),
                "regression_scenario_id": confirmation.get(
                    "regression_scenario_id"
                ),
            },
        )

    async def associate_artifacts(
        self,
        amendment_id: str,
        *,
        regression_test_task_ids: list[str] | None = None,
        regression_scenario_ids: list[str] | None = None,
        automated_regression_refs: list[str] | None = None,
        actor: str,
    ) -> AmendmentRevisionRecord:
        amendment = await self._require(amendment_id)
        _require_mutable(amendment, mutation="associate_artifacts")

        def merge(existing: list[str], incoming: list[str] | None) -> list[str]:
            result = list(existing)
            seen = set(result)
            for value in incoming or []:
                text = str(value)
                if text not in seen:
                    seen.add(text)
                    result.append(text)
            return result

        amendment.regression_test_task_ids = merge(
            amendment.regression_test_task_ids, regression_test_task_ids
        )
        amendment.regression_scenario_ids = merge(
            amendment.regression_scenario_ids, regression_scenario_ids
        )
        amendment.automated_regression_refs = merge(
            amendment.automated_regression_refs, automated_regression_refs
        )
        amendment.updated_at = datetime.now(timezone.utc)
        return await self._save(
            amendment,
            "amendment_revision_artifacts_associated",
            actor,
            {
                "regression_test_task_ids": list(regression_test_task_ids or []),
                "regression_scenario_ids": list(regression_scenario_ids or []),
                "automated_regression_refs": list(automated_regression_refs or []),
            },
        )

    @staticmethod
    def eligibility(amendment: AmendmentRevisionRecord) -> AmendmentEligibility:
        return evaluate_amendment_eligibility(
            amendment.status,
            amendment.lineage_state,
        )

    async def _save(
        self,
        amendment: AmendmentRevisionRecord,
        action: str,
        actor: str,
        details: dict[str, Any],
    ) -> AmendmentRevisionRecord:
        return await get_amendment_revision_store().save(
            self.db,
            amendment,
            audit=AmendmentAuditRecord(
                action=action,
                actor=actor,
                details={"amendment_id": amendment.id, **details},
            ),
        )


__all__ = [
    "AmendmentRevisionService",
    "AmendmentRevisionError",
    "TERMINAL_AMENDMENT_REVISION_STATUSES",
    "amendment_revision_is_terminal",
]
