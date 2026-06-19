"""DB-backed preview surface for bug regression scenario reuse.

The eligibility resolver remains pure. This service is the thin adapter that
loads cards/specs, classifies cross-spec references, and returns the canonical
preview payload used by REST and MCP.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import Card, CardType, Spec
from okto_pulse.core.services.amendment_revision import AmendmentRevisionService
from okto_pulse.core.services.bug_regression_observability import (
    observe_bug_regression_resolution,
    observe_bug_workflow_remediation,
)
from okto_pulse.core.services.bug_regression_scenarios import (
    AmendmentLineageFact,
    BugRegressionScenarioEligibilityResolver,
    BugRegressionScenarioEligibilityResult,
)
from okto_pulse.core.services.bug_workflow_remediation import (
    BugWorkflowRemediationMessageBuilder,
    serialize_bug_workflow_remediation,
)


@dataclass(frozen=True)
class BugRegressionScenarioPreviewError(Exception):
    """Typed preview error for REST/MCP callers."""

    code: str
    message: str
    status_code: int = 400

    def to_dict(self) -> dict[str, object]:
        return {
            "error": self.code,
            "message": self.message,
            "status_code": self.status_code,
        }


class BugRegressionScenarioPreviewService:
    """Resolve candidate regression scenarios for a bug without side effects."""

    def __init__(
        self,
        db: AsyncSession,
        resolver: BugRegressionScenarioEligibilityResolver | None = None,
        remediation_builder: BugWorkflowRemediationMessageBuilder | None = None,
    ) -> None:
        self._db = db
        self._resolver = resolver or BugRegressionScenarioEligibilityResolver()
        self._remediation_builder = (
            remediation_builder or BugWorkflowRemediationMessageBuilder()
        )

    async def resolve(
        self,
        *,
        board_id: str,
        bug_id: str,
        affected_task_ids: Sequence[str] | None = None,
        candidate_scenario_ids: Sequence[str] | None = None,
        surface: str = "preview",
    ) -> dict[str, object]:
        """Return eligible/rejected scenario candidates for a bug card.

        ``affected_task_ids`` is intentionally request-scoped. It gives the
        preview enough context for multi-task incidents without adding a
        persisted Card field.
        """

        started = time.perf_counter()
        bug_card = await self._load_card(bug_id, board_id, "bug_not_found")
        if self._card_type_value(bug_card) != CardType.BUG.value:
            raise BugRegressionScenarioPreviewError(
                code="not_bug_card",
                message="Card is not a bug card",
                status_code=400,
            )
        if not bug_card.spec_id:
            raise BugRegressionScenarioPreviewError(
                code="spec_not_found",
                message="Bug card is not linked to a spec",
                status_code=404,
            )

        spec = await self._db.get(Spec, bug_card.spec_id)
        if not spec or spec.board_id != board_id:
            raise BugRegressionScenarioPreviewError(
                code="spec_not_found",
                message="Bug spec was not found on this board",
                status_code=404,
            )

        origin_task = None
        if bug_card.origin_task_id:
            origin_task = await self._db.get(Card, bug_card.origin_task_id)
            if not origin_task or origin_task.board_id != board_id:
                raise BugRegressionScenarioPreviewError(
                    code="origin_task_missing",
                    message="Bug origin_task_id does not resolve to a card on this board",
                    status_code=422,
                )
            if origin_task.spec_id != bug_card.spec_id:
                raise BugRegressionScenarioPreviewError(
                    code="origin_task_missing",
                    message="Bug origin_task_id points to a card outside the bug spec",
                    status_code=422,
                )

        affected_tasks = await self._load_affected_tasks(
            board_id=board_id,
            spec_id=bug_card.spec_id,
            task_ids=affected_task_ids or (),
        )
        candidate_spec_ids_by_scenario_id = await self._candidate_spec_ids_by_scenario_id(
            board_id=board_id,
            current_spec_id=spec.id,
            candidate_scenario_ids=candidate_scenario_ids or (),
        )

        # ADJ-D: the preview MUST use the same Path B facts + predicate as the
        # gate so its verdict can never diverge from the enforced decision. G2
        # (c9cf9781): coverage is derived from the persisted, artifact-bound
        # attestation on each amendment fact — identical source as the gate.
        amendment_rows = await AmendmentRevisionService(self._db).list_for_bug(
            board_id=board_id,
            original_spec_id=spec.id,
            origin_bug_id=bug_card.id,
        )
        amendment_facts = [AmendmentLineageFact.from_row(row) for row in amendment_rows]

        result = self._resolver.resolve(
            bug_card=bug_card,
            spec=spec,
            origin_task=origin_task,
            affected_tasks=affected_tasks,
            candidate_scenario_ids=candidate_scenario_ids,
            candidate_spec_ids_by_scenario_id=candidate_spec_ids_by_scenario_id,
            amendment_facts=amendment_facts,
        )
        observe_bug_regression_resolution(
            board_id=board_id,
            result=result,
            duration_ms=(time.perf_counter() - started) * 1000,
        )
        remediation = self._remediation_builder.build_from_eligibility(result)
        observe_bug_workflow_remediation(
            board_id=board_id,
            spec_id=result.spec_id,
            message=remediation,
            surface=surface,
            outcome="preview",
        )
        return self._serialize_result(
            result,
            origin_task_id=origin_task.id if origin_task else None,
            affected_task_ids=[task.id for task in affected_tasks],
            remediation_builder=self._remediation_builder,
            remediation=remediation,
        )

    async def _load_card(self, card_id: str, board_id: str, code: str) -> Card:
        card = await self._db.get(Card, card_id)
        if not card or card.board_id != board_id:
            raise BugRegressionScenarioPreviewError(
                code=code,
                message="Card was not found on this board",
                status_code=404,
            )
        return card

    async def _load_affected_tasks(
        self,
        *,
        board_id: str,
        spec_id: str,
        task_ids: Sequence[str],
    ) -> list[Card]:
        affected: list[Card] = []
        for task_id in self._ordered_unique(task_ids):
            task = await self._db.get(Card, task_id)
            if not task or task.board_id != board_id:
                raise BugRegressionScenarioPreviewError(
                    code="affected_task_not_found",
                    message=f"Affected task '{task_id}' was not found on this board",
                    status_code=404,
                )
            if task.spec_id != spec_id:
                raise BugRegressionScenarioPreviewError(
                    code="affected_task_wrong_spec",
                    message=f"Affected task '{task_id}' does not belong to the bug spec",
                    status_code=422,
                )
            affected.append(task)
        return affected

    async def _candidate_spec_ids_by_scenario_id(
        self,
        *,
        board_id: str,
        current_spec_id: str,
        candidate_scenario_ids: Sequence[str],
    ) -> dict[str, str]:
        if not candidate_scenario_ids:
            return {}

        candidate_set = set(self._ordered_unique(candidate_scenario_ids))
        stmt = select(Spec.id, Spec.test_scenarios).where(Spec.board_id == board_id)
        rows = (await self._db.execute(stmt)).all()
        mapping: dict[str, str] = {}
        for spec_id, scenarios in rows:
            for scenario in scenarios or []:
                if not isinstance(scenario, dict):
                    continue
                scenario_id = scenario.get("id")
                if scenario_id is None:
                    continue
                scenario_id_str = str(scenario_id)
                if scenario_id_str in candidate_set:
                    mapping.setdefault(scenario_id_str, str(spec_id))

        # The pure resolver only needs foreign-spec mapping for scenarios not
        # present on the current spec. Keeping current-spec IDs is harmless, but
        # removing them keeps the serialized diagnostics focused.
        return {
            scenario_id: spec_id
            for scenario_id, spec_id in mapping.items()
            if spec_id != current_spec_id
        }

    @staticmethod
    def _serialize_result(
        result: BugRegressionScenarioEligibilityResult,
        *,
        origin_task_id: str | None,
        affected_task_ids: list[str],
        remediation_builder: BugWorkflowRemediationMessageBuilder,
        remediation=None,
    ) -> dict[str, object]:
        remediation = remediation or remediation_builder.build_from_eligibility(result)
        return {
            "bug_id": result.bug_id,
            "spec_id": result.spec_id,
            "origin_task_id": origin_task_id,
            "affected_task_ids": affected_task_ids,
            "eligible_scenarios": [
                {
                    "scenario_id": scenario.scenario_id,
                    "title": scenario.title,
                    "reason": scenario.reason.value,
                    "source_task_id": scenario.source_task_id,
                }
                for scenario in result.eligible_scenarios
            ],
            "rejected_scenarios": [
                {
                    "scenario_id": scenario.scenario_id,
                    "reason": scenario.reason.value,
                    "detail": scenario.detail,
                }
                for scenario in result.rejected_scenarios
            ],
            "next_action": result.next_action.value,
            "semantic_gap_required": result.semantic_gap_required,
            "spec_mutation_required": result.spec_mutation_required,
            # Path B / TR2 payload extensions.
            "coverage_state": result.coverage_state.value,
            "coverage_pending_scenarios": list(result.coverage_pending_scenarios),
            "amendment_revision_id": result.amendment_revision_id,
            "amendment_status": result.amendment_status,
            "lineage_state": result.lineage_state,
            "missing_links": list(result.missing_links),
            "eligible_regression_artifacts": list(result.eligible_regression_artifacts),
            "rejected_regression_artifacts": list(result.rejected_regression_artifacts),
            "safe_next_actions": list(result.safe_next_actions),
            "remediation": serialize_bug_workflow_remediation(remediation),
        }

    @staticmethod
    def _ordered_unique(values: Sequence[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values:
            value_str = str(value)
            if value_str not in seen:
                seen.add(value_str)
                ordered.append(value_str)
        return ordered

    @staticmethod
    def _card_type_value(card: Card) -> str:
        value = getattr(card, "card_type", None)
        return value.value if hasattr(value, "value") else str(value or "")
