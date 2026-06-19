"""Bug regression scenario eligibility resolution.

This module is intentionally pure: it classifies already-loaded spec/card
objects and never mutates the canonical spec or writes audit records.

Path A (spec 7ea1e4be / card ead17e4d): same-spec regression lineage — a
candidate scenario is eligible when it is linked to the bug origin task or an
explicit affected task. This is the unchanged legacy behavior.

Path B: a CROSS-SPEC scenario may become eligible ONLY when a formal
``AmendmentHotfixRevision`` backs it. The amendment is supplied as a pure
``AmendmentLineageFact`` by the caller (the resolver never touches the DB).
Path B is a NARROW ADDITIVE allow layered on top of the existing cross-spec
reject — it never relaxes Path A and is fail-closed:

* the amendment must formally link the SAME board, ``original_spec_id ==
  bug.spec_id`` and ``origin_bug_id == bug.id`` (else ``missing_amendment_revision``);
* the amendment status must be non-blocking (else ``blocked_amendment_status``);
* the amendment lineage_state must be ``complete`` (else ``incomplete_amendment_lineage``);
* the scenario must be DECLARED in the amendment regression artifacts (else
  ``missing_regression_artifact``);
* ADJ-A: membership is authoritative from the BUG. The amendment task_ids are a
  normalized CLAIM; only their intersection with the bug's authoritative task
  set counts. A fabricated/foreign task_id does NOT grant eligibility — if no
  compatible membership remains, the scenario is rejected
  ``unrelated_cross_spec_scenario`` (fail-closed).

ADJ-B: lineage eligibility is NOT coverage. ``coverage_confirmed`` is a seam for
the validator-confirmed coverage signal delivered by card c9cf9781; no
production caller sets it true. Without confirmed coverage a fully lineage-
eligible Path B scenario returns ``coverage_pending`` and never closure-ready.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping, Sequence

from okto_pulse.core.domain.amendment_eligibility import amendment_status_is_blocking
from okto_pulse.core.models.db import Card, Spec


class BugRegressionEligibilityReason(str, Enum):
    """Why a scenario is eligible for a bug regression test card."""

    ORIGIN_TASK_DIRECT = "origin_task_direct"
    ORIGIN_TASK_LINKED_SCENARIO = "origin_task_linked_scenario"
    AFFECTED_TASK_DIRECT = "affected_task_direct"
    AFFECTED_TASK_LINKED_SCENARIO = "affected_task_linked_scenario"
    # Path B: cross-spec scenario backed by a formal amendment lineage.
    PATH_B_AMENDMENT_LINEAGE = "path_b_amendment_lineage"


class BugRegressionRejectionReason(str, Enum):
    """Why a candidate scenario cannot satisfy the bug regression gate."""

    SCENARIO_NOT_FOUND = "scenario_not_found"
    CROSS_SPEC_SCENARIO = "cross_spec_scenario"
    DELETED_SCENARIO = "deleted_scenario"
    UNRELATED_SCENARIO = "unrelated_scenario"
    # Path B fail-closed reason vocabulary (FR4 / TR3 / P5). Bounded + stable.
    MISSING_AMENDMENT_REVISION = "missing_amendment_revision"
    INCOMPLETE_AMENDMENT_LINEAGE = "incomplete_amendment_lineage"
    BLOCKED_AMENDMENT_STATUS = "blocked_amendment_status"
    UNRELATED_CROSS_SPEC_SCENARIO = "unrelated_cross_spec_scenario"
    MISSING_REGRESSION_ARTIFACT = "missing_regression_artifact"


class BugRegressionCoverageState(str, Enum):
    """Separate lineage eligibility from validator-confirmed coverage (G2).

    ``coverage_pending`` means the Path B lineage is eligible but closure still
    needs validator-confirmed coverage (delivered by card c9cf9781).
    """

    NOT_APPLICABLE = "not_applicable"
    COVERAGE_PENDING = "coverage_pending"
    PATH_B_READY = "path_b_ready"


class BugRegressionNextAction(str, Enum):
    """Deterministic next action for the caller."""

    CREATE_REGRESSION_TEST_CARD = "create_regression_test_card"
    ESCALATE_SEMANTIC_GAP = "escalate_semantic_gap"
    CONFIRM_VALIDATOR_COVERAGE = "confirm_validator_coverage"


class BugRegressionGateDecision(str, Enum):
    """Bug linked-test-task gate decision."""

    ALLOW = "allow"
    BLOCK_UNRELATED_SCENARIO = "block_unrelated_scenario"
    BLOCK_SEMANTIC_GAP = "block_semantic_gap"
    BLOCK_COVERAGE_PENDING = "block_coverage_pending"


#: Reserved key inside ``AmendmentHotfixRevision.validation_metadata`` that
#: holds the validator coverage attestation (G2 / card c9cf9781). NON-FORGEABLE:
#: only the validator-only writer ``CardService.confirm_amendment_coverage`` may
#: persist it; every other write path (e.g. amendment create) MUST strip it so a
#: generic/non-validator route can never inject a signal the gate honors.
COVERAGE_CONFIRMATION_KEY = "coverage_confirmation"


@dataclass(frozen=True)
class CoverageConfirmationFact:
    """Pure projection of the persisted validator coverage attestation (G2).

    The gate/preview NEVER trust a passed-in ``coverage_confirmed`` bool (that
    would be forgeable by the caller). Coverage is derived ONLY from this
    persisted, artifact-bound attestation written by the validator-only writer."""

    validator_id: str
    amendment_revision_id: str
    regression_test_task_id: str
    regression_scenario_id: str
    evidence_ref: str
    confirmed_at: str = ""

    @classmethod
    def from_metadata(cls, raw: object) -> "CoverageConfirmationFact | None":
        if not isinstance(raw, dict):
            return None
        return cls(
            validator_id=str(raw.get("validator_id") or ""),
            amendment_revision_id=str(raw.get("amendment_revision_id") or ""),
            regression_test_task_id=str(raw.get("regression_test_task_id") or ""),
            regression_scenario_id=str(raw.get("regression_scenario_id") or ""),
            evidence_ref=str(raw.get("evidence_ref") or ""),
            confirmed_at=str(raw.get("confirmed_at") or ""),
        )


@dataclass(frozen=True)
class AmendmentLineageFact:
    """Pure projection of an ``AmendmentHotfixRevision`` for Path B evaluation.

    Loaded and supplied by the caller (gate/preview) so the resolver stays pure
    and DB-free. The task id lists are the amendment's CLAIM — authoritative
    membership is still the bug's task set (ADJ-A)."""

    amendment_revision_id: str
    board_id: str
    original_spec_id: str
    origin_bug_id: str
    status: str
    lineage_state: str
    origin_task_ids: tuple[str, ...] = ()
    affected_task_ids: tuple[str, ...] = ()
    regression_scenario_ids: tuple[str, ...] = ()
    regression_test_task_ids: tuple[str, ...] = ()
    automated_regression_refs: tuple[str, ...] = ()
    coverage_confirmation: CoverageConfirmationFact | None = None

    @classmethod
    def from_row(cls, amendment: object) -> "AmendmentLineageFact":
        """Build the pure fact from an ``AmendmentHotfixRevision`` row (duck-typed
        so this module stays DB-free). The single conversion both the gate and
        the preview use, so Path B facts are identical on both surfaces (ADJ-D)."""

        def _strs(values: object) -> tuple[str, ...]:
            return tuple(str(v) for v in (values or []))

        def _enum_str(value: object) -> str:
            return str(getattr(value, "value", value) or "")

        metadata = getattr(amendment, "validation_metadata", None)
        confirmation = None
        if isinstance(metadata, dict):
            confirmation = CoverageConfirmationFact.from_metadata(
                metadata.get(COVERAGE_CONFIRMATION_KEY)
            )

        return cls(
            amendment_revision_id=str(getattr(amendment, "id", "")),
            board_id=str(getattr(amendment, "board_id", "") or ""),
            original_spec_id=str(getattr(amendment, "original_spec_id", "") or ""),
            origin_bug_id=str(getattr(amendment, "origin_bug_id", "") or ""),
            status=_enum_str(getattr(amendment, "status", "")),
            lineage_state=_enum_str(getattr(amendment, "lineage_state", "")),
            origin_task_ids=_strs(getattr(amendment, "origin_task_ids", None)),
            affected_task_ids=_strs(getattr(amendment, "affected_task_ids", None)),
            regression_scenario_ids=_strs(getattr(amendment, "regression_scenario_ids", None)),
            regression_test_task_ids=_strs(getattr(amendment, "regression_test_task_ids", None)),
            automated_regression_refs=_strs(getattr(amendment, "automated_regression_refs", None)),
            coverage_confirmation=confirmation,
        )


class _PathBOutcome(str, Enum):
    READY = "ready"
    COVERAGE_PENDING = "coverage_pending"
    REJECT = "reject"


@dataclass(frozen=True)
class PathBScenarioEvaluation:
    """Per cross-spec scenario Path B verdict."""

    outcome: _PathBOutcome
    coverage_state: BugRegressionCoverageState
    reject_reason: BugRegressionRejectionReason | None = None
    amendment_revision_id: str | None = None
    amendment_status: str | None = None
    lineage_state: str | None = None
    source_task_id: str | None = None
    missing_links: tuple[str, ...] = ()
    detail: str | None = None


@dataclass(frozen=True)
class EligibleBugRegressionScenario:
    """Eligible scenario candidate with lineage proof."""

    scenario_id: str
    title: str | None
    reason: BugRegressionEligibilityReason
    source_task_id: str


@dataclass(frozen=True)
class RejectedBugRegressionScenario:
    """Rejected scenario candidate with bounded reason vocabulary."""

    scenario_id: str
    reason: BugRegressionRejectionReason
    detail: str | None = None


@dataclass(frozen=True)
class BugRegressionScenarioEligibilityResult:
    """Complete resolver outcome."""

    bug_id: str
    spec_id: str
    eligible_scenarios: tuple[EligibleBugRegressionScenario, ...]
    rejected_scenarios: tuple[RejectedBugRegressionScenario, ...]
    semantic_gap_required: bool
    spec_mutation_required: bool
    next_action: BugRegressionNextAction
    # Path B / TR2 payload extensions (additive; default to a Path-A-only view).
    coverage_state: BugRegressionCoverageState = BugRegressionCoverageState.NOT_APPLICABLE
    coverage_pending_scenarios: tuple[str, ...] = ()
    amendment_revision_id: str | None = None
    amendment_status: str | None = None
    lineage_state: str | None = None
    missing_links: tuple[str, ...] = ()
    eligible_regression_artifacts: tuple[str, ...] = ()
    rejected_regression_artifacts: tuple[str, ...] = ()
    safe_next_actions: tuple[str, ...] = ()


def evaluate_path_b_for_scenario(
    *,
    scenario_id: str,
    bug_id: str,
    spec_id: str,
    board_id: str,
    bug_authoritative_task_ids: frozenset[str],
    amendment_facts: Sequence[AmendmentLineageFact],
    coverage_confirmed: bool,
) -> PathBScenarioEvaluation:
    """Pure, fail-closed Path B predicate for a single cross-spec scenario.

    Returns the furthest-progressed outcome across all linked amendments so the
    reason code is the most actionable one. A missing/blocking/incomplete/
    unrelated amendment never grants eligibility.
    """

    linked = [
        fact
        for fact in amendment_facts
        if fact.board_id == board_id
        and fact.original_spec_id == spec_id
        and fact.origin_bug_id == bug_id
    ]
    if not linked:
        return PathBScenarioEvaluation(
            outcome=_PathBOutcome.REJECT,
            coverage_state=BugRegressionCoverageState.NOT_APPLICABLE,
            reject_reason=BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION,
            missing_links=("amendment_revision",),
        )

    best_rank = -1
    best = PathBScenarioEvaluation(
        outcome=_PathBOutcome.REJECT,
        coverage_state=BugRegressionCoverageState.NOT_APPLICABLE,
        reject_reason=BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION,
        missing_links=("amendment_revision",),
    )
    for fact in sorted(linked, key=lambda f: f.amendment_revision_id):
        rank, evaluation = _evaluate_single_amendment(
            fact=fact,
            scenario_id=scenario_id,
            bug_authoritative_task_ids=bug_authoritative_task_ids,
            coverage_confirmed=coverage_confirmed,
        )
        if rank > best_rank:
            best_rank, best = rank, evaluation
    return best


def _evaluate_single_amendment(
    *,
    fact: AmendmentLineageFact,
    scenario_id: str,
    bug_authoritative_task_ids: frozenset[str],
    coverage_confirmed: bool,
) -> tuple[int, PathBScenarioEvaluation]:
    common = {
        "amendment_revision_id": fact.amendment_revision_id,
        "amendment_status": fact.status,
        "lineage_state": fact.lineage_state,
    }

    # 1. status must be non-blocking (draft/review/cancelled/superseded/unknown
    #    are all blocking — reuses the card #1 pure policy, fail-closed).
    if amendment_status_is_blocking(fact.status):
        return 1, PathBScenarioEvaluation(
            outcome=_PathBOutcome.REJECT,
            coverage_state=BugRegressionCoverageState.NOT_APPLICABLE,
            reject_reason=BugRegressionRejectionReason.BLOCKED_AMENDMENT_STATUS,
            missing_links=("non_blocking_status",),
            **common,
        )

    # 2. lineage_state must be complete.
    if str(fact.lineage_state or "").strip().lower() != "complete":
        return 2, PathBScenarioEvaluation(
            outcome=_PathBOutcome.REJECT,
            coverage_state=BugRegressionCoverageState.NOT_APPLICABLE,
            reject_reason=BugRegressionRejectionReason.INCOMPLETE_AMENDMENT_LINEAGE,
            missing_links=("complete_lineage",),
            **common,
        )

    # 3. the scenario must be DECLARED as a regression artifact by the amendment.
    if scenario_id not in set(fact.regression_scenario_ids):
        return 3, PathBScenarioEvaluation(
            outcome=_PathBOutcome.REJECT,
            coverage_state=BugRegressionCoverageState.NOT_APPLICABLE,
            reject_reason=BugRegressionRejectionReason.MISSING_REGRESSION_ARTIFACT,
            missing_links=("regression_artifact",),
            detail=scenario_id,
            **common,
        )

    # 4. ADJ-A: membership is authoritative from the BUG. The amendment task_ids
    #    are a claim; only the intersection with the bug's authoritative set
    #    counts. Fabricated/foreign task ids never grant eligibility.
    claim = set(fact.origin_task_ids) | set(fact.affected_task_ids)
    compatible = sorted(claim & set(bug_authoritative_task_ids))
    if not compatible:
        return 4, PathBScenarioEvaluation(
            outcome=_PathBOutcome.REJECT,
            coverage_state=BugRegressionCoverageState.NOT_APPLICABLE,
            reject_reason=BugRegressionRejectionReason.UNRELATED_CROSS_SPEC_SCENARIO,
            missing_links=("authoritative_task_membership",),
            **common,
        )

    # 5. Fully lineage-eligible. Coverage (ADJ-B) decides ready vs pending.
    if coverage_confirmed:
        return 5, PathBScenarioEvaluation(
            outcome=_PathBOutcome.READY,
            coverage_state=BugRegressionCoverageState.PATH_B_READY,
            source_task_id=compatible[0],
            **common,
        )
    return 5, PathBScenarioEvaluation(
        outcome=_PathBOutcome.COVERAGE_PENDING,
        coverage_state=BugRegressionCoverageState.COVERAGE_PENDING,
        source_task_id=compatible[0],
        missing_links=("validator_coverage",),
        **common,
    )


@dataclass(frozen=True)
class BugRegressionGateValidationResult:
    """Eligibility-aware result for the bug regression test gate."""

    allowed: bool
    decision: BugRegressionGateDecision
    eligibility: BugRegressionScenarioEligibilityResult


class BugRegressionScenarioEligibilityResolver:
    """Resolve reusable regression scenarios from explicit task lineage only."""

    def resolve(
        self,
        *,
        bug_card: Card,
        spec: Spec,
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None = None,
        candidate_scenario_ids: Sequence[str] | None = None,
        candidate_spec_ids_by_scenario_id: Mapping[str, str] | None = None,
        amendment_facts: Sequence[AmendmentLineageFact] | None = None,
        coverage_confirmed: bool = False,
    ) -> BugRegressionScenarioEligibilityResult:
        """Classify candidate scenarios without database access or side effects.

        When ``amendment_facts`` is ``None`` the resolver runs in legacy
        Path-A-only mode and a cross-spec scenario is always rejected
        ``cross_spec_scenario`` (unchanged). When it is provided (even empty)
        the shared Path A/Path B predicate is active and a cross-spec scenario
        is fail-closed unless a formal amendment backs it.
        """

        scenarios_by_id = {
            str(scenario.get("id")): scenario
            for scenario in (spec.test_scenarios or [])
            if isinstance(scenario, dict) and scenario.get("id") is not None
        }
        candidate_ids = self._ordered_unique(
            candidate_scenario_ids
            if candidate_scenario_ids is not None
            else self._lineage_candidate_ids(spec, origin_task, affected_tasks)
        )

        # ADJ-A: the bug's authoritative task set is its resolved lineage cards
        # (origin task + explicit affected tasks). The amendment claim is
        # validated against THIS, never trusted on its own.
        bug_authoritative_task_ids = self._bug_authoritative_task_ids(
            origin_task, affected_tasks
        )

        lineage_reasons = self._lineage_reasons(spec, origin_task, affected_tasks)
        eligible: list[EligibleBugRegressionScenario] = []
        rejected: list[RejectedBugRegressionScenario] = []
        coverage_pending: list[str] = []
        path_b_evaluations: list[PathBScenarioEvaluation] = []

        for scenario_id in candidate_ids:
            scenario = scenarios_by_id.get(scenario_id)
            if scenario is None:
                mapped_spec_id = (candidate_spec_ids_by_scenario_id or {}).get(scenario_id)
                if mapped_spec_id and mapped_spec_id != spec.id:
                    if amendment_facts is None:
                        # Legacy Path-A-only mode (no Path B context supplied).
                        rejected.append(
                            RejectedBugRegressionScenario(
                                scenario_id=scenario_id,
                                reason=BugRegressionRejectionReason.CROSS_SPEC_SCENARIO,
                                detail=mapped_spec_id,
                            )
                        )
                        continue
                    evaluation = evaluate_path_b_for_scenario(
                        scenario_id=scenario_id,
                        bug_id=bug_card.id,
                        spec_id=spec.id,
                        board_id=spec.board_id,
                        bug_authoritative_task_ids=bug_authoritative_task_ids,
                        amendment_facts=amendment_facts,
                        coverage_confirmed=coverage_confirmed,
                    )
                    path_b_evaluations.append(evaluation)
                    if evaluation.outcome is _PathBOutcome.READY:
                        eligible.append(
                            EligibleBugRegressionScenario(
                                scenario_id=scenario_id,
                                title=scenario_title_or_none(scenario),
                                reason=BugRegressionEligibilityReason.PATH_B_AMENDMENT_LINEAGE,
                                source_task_id=evaluation.source_task_id or "",
                            )
                        )
                    elif evaluation.outcome is _PathBOutcome.COVERAGE_PENDING:
                        coverage_pending.append(scenario_id)
                    else:
                        rejected.append(
                            RejectedBugRegressionScenario(
                                scenario_id=scenario_id,
                                reason=evaluation.reject_reason
                                or BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION,
                                detail=evaluation.detail or mapped_spec_id,
                            )
                        )
                else:
                    rejected.append(
                        RejectedBugRegressionScenario(
                            scenario_id=scenario_id,
                            reason=BugRegressionRejectionReason.SCENARIO_NOT_FOUND,
                        )
                    )
                continue

            if str(scenario.get("status") or "").lower() == "deleted":
                rejected.append(
                    RejectedBugRegressionScenario(
                        scenario_id=scenario_id,
                        reason=BugRegressionRejectionReason.DELETED_SCENARIO,
                    )
                )
                continue

            reason_and_task = lineage_reasons.get(scenario_id)
            if reason_and_task:
                reason, task_id = reason_and_task
                eligible.append(
                    EligibleBugRegressionScenario(
                        scenario_id=scenario_id,
                        title=scenario.get("title"),
                        reason=reason,
                        source_task_id=task_id,
                    )
                )
                continue

            rejected.append(
                RejectedBugRegressionScenario(
                    scenario_id=scenario_id,
                    reason=BugRegressionRejectionReason.UNRELATED_SCENARIO,
                )
            )

        return self._build_result(
            bug_card=bug_card,
            spec=spec,
            eligible=eligible,
            rejected=rejected,
            coverage_pending=coverage_pending,
            path_b_evaluations=path_b_evaluations,
        )

    def _build_result(
        self,
        *,
        bug_card: Card,
        spec: Spec,
        eligible: list[EligibleBugRegressionScenario],
        rejected: list[RejectedBugRegressionScenario],
        coverage_pending: list[str],
        path_b_evaluations: list[PathBScenarioEvaluation],
    ) -> BugRegressionScenarioEligibilityResult:
        # coverage_pending lineage IS eligible — it is not a semantic gap. Only a
        # truly empty result (no eligible, no pending Path B lineage) requires a
        # new canonical artifact / spec mutation.
        semantic_gap_required = not eligible and not coverage_pending

        if coverage_pending and not eligible:
            next_action = BugRegressionNextAction.CONFIRM_VALIDATOR_COVERAGE
        elif semantic_gap_required:
            next_action = BugRegressionNextAction.ESCALATE_SEMANTIC_GAP
        else:
            next_action = BugRegressionNextAction.CREATE_REGRESSION_TEST_CARD

        # Coverage state + governing amendment metadata (TR2). Prefer the
        # furthest-progressed Path B evaluation for diagnostics.
        coverage_state = BugRegressionCoverageState.NOT_APPLICABLE
        governing: PathBScenarioEvaluation | None = None
        for evaluation in path_b_evaluations:
            if evaluation.coverage_state is BugRegressionCoverageState.PATH_B_READY:
                coverage_state = BugRegressionCoverageState.PATH_B_READY
                governing = evaluation
                break
        if governing is None:
            for evaluation in path_b_evaluations:
                if evaluation.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING:
                    coverage_state = BugRegressionCoverageState.COVERAGE_PENDING
                    governing = evaluation
                    break
        if governing is None and path_b_evaluations:
            governing = path_b_evaluations[0]

        eligible_artifacts = tuple(item.scenario_id for item in eligible)
        rejected_artifacts = tuple(item.scenario_id for item in rejected)
        safe_next_actions = (next_action.value,)

        return BugRegressionScenarioEligibilityResult(
            bug_id=bug_card.id,
            spec_id=spec.id,
            eligible_scenarios=tuple(eligible),
            rejected_scenarios=tuple(rejected),
            semantic_gap_required=semantic_gap_required,
            spec_mutation_required=semantic_gap_required,
            next_action=next_action,
            coverage_state=coverage_state,
            coverage_pending_scenarios=tuple(coverage_pending),
            amendment_revision_id=governing.amendment_revision_id if governing else None,
            amendment_status=governing.amendment_status if governing else None,
            lineage_state=governing.lineage_state if governing else None,
            missing_links=governing.missing_links if governing else (),
            eligible_regression_artifacts=eligible_artifacts,
            rejected_regression_artifacts=rejected_artifacts,
            safe_next_actions=safe_next_actions,
        )

    @staticmethod
    def _bug_authoritative_task_ids(
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> frozenset[str]:
        ids: set[str] = set()
        if origin_task is not None:
            ids.add(str(origin_task.id))
        for task in affected_tasks or []:
            ids.add(str(task.id))
        return frozenset(ids)

    @staticmethod
    def _ordered_unique(values: Sequence[str] | None) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values or []:
            scenario_id = str(value)
            if scenario_id not in seen:
                seen.add(scenario_id)
                ordered.append(scenario_id)
        return ordered

    def _lineage_candidate_ids(
        self,
        spec: Spec,
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> list[str]:
        candidates: list[str] = []
        for task in self._tasks_in_priority_order(origin_task, affected_tasks):
            candidates.extend(task.test_scenario_ids or [])
            candidates.extend(self._scenario_ids_linked_to_task(spec, task))
        return candidates

    def _lineage_reasons(
        self,
        spec: Spec,
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> dict[str, tuple[BugRegressionEligibilityReason, str]]:
        reasons: dict[str, tuple[BugRegressionEligibilityReason, str]] = {}

        if origin_task:
            for scenario_id in origin_task.test_scenario_ids or []:
                reasons.setdefault(
                    str(scenario_id),
                    (
                        BugRegressionEligibilityReason.ORIGIN_TASK_DIRECT,
                        origin_task.id,
                    ),
                )

        for task in affected_tasks or []:
            for scenario_id in task.test_scenario_ids or []:
                reasons.setdefault(
                    str(scenario_id),
                    (
                        BugRegressionEligibilityReason.AFFECTED_TASK_DIRECT,
                        task.id,
                    ),
                )

        # linked_task_ids on the canonical scenario are secondary proof. Direct
        # card linkage wins when both are present.
        for task, reason in self._linked_task_reason_order(origin_task, affected_tasks):
            for scenario_id in self._scenario_ids_linked_to_task(spec, task):
                reasons.setdefault(str(scenario_id), (reason, task.id))

        return reasons

    @staticmethod
    def _tasks_in_priority_order(
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> list[Card]:
        tasks: list[Card] = []
        if origin_task:
            tasks.append(origin_task)
        tasks.extend(affected_tasks or [])
        return tasks

    @staticmethod
    def _linked_task_reason_order(
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None,
    ) -> list[tuple[Card, BugRegressionEligibilityReason]]:
        tasks: list[tuple[Card, BugRegressionEligibilityReason]] = []
        if origin_task:
            tasks.append(
                (
                    origin_task,
                    BugRegressionEligibilityReason.ORIGIN_TASK_LINKED_SCENARIO,
                )
            )
        for task in affected_tasks or []:
            tasks.append(
                (
                    task,
                    BugRegressionEligibilityReason.AFFECTED_TASK_LINKED_SCENARIO,
                )
            )
        return tasks

    @staticmethod
    def _scenario_ids_linked_to_task(spec: Spec, task: Card) -> list[str]:
        scenario_ids: list[str] = []
        for scenario in spec.test_scenarios or []:
            if not isinstance(scenario, dict):
                continue
            if task.id in (scenario.get("linked_task_ids") or []):
                scenario_id = scenario.get("id")
                if scenario_id is not None:
                    scenario_ids.append(str(scenario_id))
        return scenario_ids


def scenario_title_or_none(scenario: object) -> str | None:
    if isinstance(scenario, dict):
        title = scenario.get("title")
        return str(title) if title is not None else None
    return None


class BugRegressionGateValidator:
    """Validate linked bug test tasks against scenario lineage eligibility."""

    def __init__(
        self,
        resolver: BugRegressionScenarioEligibilityResolver | None = None,
    ) -> None:
        self._resolver = resolver or BugRegressionScenarioEligibilityResolver()

    def validate_linked_test_tasks(
        self,
        *,
        bug_card: Card,
        linked_test_tasks: Sequence[Card],
        spec: Spec,
        origin_task: Card | None,
        affected_tasks: Sequence[Card] | None = None,
        candidate_spec_ids_by_scenario_id: Mapping[str, str] | None = None,
        amendment_facts: Sequence[AmendmentLineageFact] | None = None,
        coverage_confirmed: bool = False,
    ) -> BugRegressionGateValidationResult:
        candidate_scenario_ids = self._ordered_unique(
            scenario_id
            for task in linked_test_tasks
            for scenario_id in (task.test_scenario_ids or [])
        )
        eligibility = self._resolver.resolve(
            bug_card=bug_card,
            spec=spec,
            origin_task=origin_task,
            affected_tasks=affected_tasks,
            candidate_scenario_ids=candidate_scenario_ids,
            candidate_spec_ids_by_scenario_id=candidate_spec_ids_by_scenario_id,
            amendment_facts=amendment_facts,
            coverage_confirmed=coverage_confirmed,
        )

        # ALLOW only on a clean eligible set: Path A lineage OR a Path B amendment
        # that is path_b_ready (coverage confirmed). A coverage_pending Path B
        # scenario is lineage eligible but NOT closure-ready -> block (ADJ-B).
        if (
            eligibility.eligible_scenarios
            and not eligibility.rejected_scenarios
            and eligibility.coverage_state is not BugRegressionCoverageState.COVERAGE_PENDING
            and not eligibility.coverage_pending_scenarios
        ):
            return BugRegressionGateValidationResult(
                allowed=True,
                decision=BugRegressionGateDecision.ALLOW,
                eligibility=eligibility,
            )

        if (
            not eligibility.rejected_scenarios
            and eligibility.coverage_pending_scenarios
        ):
            decision = BugRegressionGateDecision.BLOCK_COVERAGE_PENDING
        elif eligibility.semantic_gap_required:
            decision = BugRegressionGateDecision.BLOCK_SEMANTIC_GAP
        else:
            decision = BugRegressionGateDecision.BLOCK_UNRELATED_SCENARIO

        return BugRegressionGateValidationResult(
            allowed=False,
            decision=decision,
            eligibility=eligibility,
        )

    @staticmethod
    def _ordered_unique(values: Iterable[object]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values or []:
            scenario_id = str(value)
            if scenario_id not in seen:
                seen.add(scenario_id)
                ordered.append(scenario_id)
        return ordered
