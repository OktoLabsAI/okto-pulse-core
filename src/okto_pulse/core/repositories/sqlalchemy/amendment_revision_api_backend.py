"""SQLAlchemy-backed backend for AmendmentRevisionApiService."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
)
from okto_pulse.core.models.db import Card, Spec
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.bug_regression_preview import (
    BugRegressionScenarioPreviewError,
    BugRegressionScenarioPreviewService,
)


class SQLAlchemyAmendmentRevisionApiBackend:
    """Transitional relational backend for the Path B amendment API service."""

    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self._store = AmendmentRevisionService(db)

    async def get_bug(self, board_id: str, bug_id: str) -> Card | None:
        return await self._db.get(Card, bug_id)

    async def get_spec(self, board_id: str, spec_id: str) -> Spec | None:
        return await self._db.get(Spec, spec_id)

    async def create_amendment(
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
    ) -> Any:
        return await self._store.create(
            board_id=board_id,
            original_spec_id=original_spec_id,
            origin_bug_id=origin_bug_id,
            author=author,
            origin_task_ids=origin_task_ids,
            affected_task_ids=affected_task_ids,
            revision_spec_id=revision_spec_id,
            regression_scenario_ids=regression_scenario_ids,
            regression_test_task_ids=regression_test_task_ids,
            automated_regression_refs=automated_regression_refs,
            validation_metadata=None,
        )

    async def get_amendment(self, amendment_id: str) -> Any | None:
        return await self._store.get(amendment_id)

    async def list_amendments_for_bug(
        self,
        *,
        board_id: str,
        original_spec_id: str,
        origin_bug_id: str,
    ) -> list[Any]:
        return await self._store.list_for_bug(
            board_id=board_id,
            original_spec_id=original_spec_id,
            origin_bug_id=origin_bug_id,
        )

    async def associate_artifacts(
        self,
        amendment_id: str,
        *,
        regression_test_task_ids: list[str] | None = None,
        regression_scenario_ids: list[str] | None = None,
        automated_regression_refs: list[str] | None = None,
        actor: str,
    ) -> Any:
        return await self._store.associate_artifacts(
            amendment_id,
            regression_test_task_ids=regression_test_task_ids,
            regression_scenario_ids=regression_scenario_ids,
            automated_regression_refs=automated_regression_refs,
            actor=actor,
        )

    async def set_lineage_state(
        self,
        amendment_id: str,
        lineage_state: AmendmentLineageState,
        actor: str,
    ) -> Any:
        return await self._store.set_lineage_state(amendment_id, lineage_state, actor)

    async def set_status(
        self,
        amendment_id: str,
        new_status: AmendmentRevisionStatus,
        actor: str,
    ) -> Any:
        return await self._store.set_status(amendment_id, new_status, actor)

    async def refresh(self, entity: Any) -> None:
        await self._db.refresh(entity)

    async def path_b_resolution(
        self,
        *,
        board_id: str,
        bug_id: str,
        candidate_scenario_ids: list[str],
    ) -> dict[str, Any]:
        try:
            payload = await BugRegressionScenarioPreviewService(self._db).resolve(
                board_id=board_id,
                bug_id=bug_id,
                candidate_scenario_ids=candidate_scenario_ids or None,
            )
        except BugRegressionScenarioPreviewError as exc:
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

    def eligibility(self, amendment: Any) -> Any:
        return AmendmentRevisionService.eligibility(amendment)


__all__ = ["SQLAlchemyAmendmentRevisionApiBackend"]
