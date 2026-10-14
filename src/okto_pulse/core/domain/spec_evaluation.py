"""Edition currentness for native decomposition reviews.

An approval never supersedes a rejection inside the same edition.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class SpecEvaluationInput(BaseModel):
    """One authored decomposition review, shared by REST, MCP and composition."""

    model_config = ConfigDict(extra="forbid", strict=True)

    breakdown_completeness: int = Field(ge=0, le=100)
    breakdown_justification: str = Field(min_length=10)
    granularity: int = Field(ge=0, le=100)
    granularity_justification: str = Field(min_length=10)
    dependency_coherence: int = Field(ge=0, le=100)
    dependency_justification: str = Field(min_length=10)
    test_coverage_quality: int = Field(ge=0, le=100)
    test_coverage_justification: str = Field(min_length=10)
    overall_score: int = Field(ge=0, le=100)
    overall_justification: str = Field(min_length=10)
    recommendation: Literal["approve", "request_changes", "reject"]


@dataclass(frozen=True, slots=True)
class SpecDecompositionVerdict:
    """Native decomposition judgment, independent from Spec quality scores."""

    required: bool
    threshold: int
    current_ids: tuple[str, ...] = ()
    approval_ids: tuple[str, ...] = ()
    rejection_ids: tuple[str, ...] = ()
    average_approval_score: float | None = None
    blocker: Literal["rejected", "approval_missing", "score_below_threshold"] | None = None
    rejection_names: tuple[str, ...] = ()

    @property
    def allowed(self) -> bool:
        return self.blocker is None

    def require_allowed(self, *, target: str = "in_progress") -> None:
        if self.blocker == "rejected":
            raise ValueError(
                f"Cannot move spec to '{target}': {len(self.rejection_ids)} evaluation(s) "
                f"with 'reject' recommendation exist (by: {', '.join(self.rejection_names)}). "
                "Reopen the Spec to Draft, correct its plan and obtain "
                "new evaluations for the new edition."
            )
        if self.blocker == "approval_missing":
            raise ValueError(
                f"Cannot move spec to '{target}': no evaluation with "
                "'approve' recommendation found. At least one approval is required. "
                "Submit an evaluation via okto_pulse_submit_spec_evaluation."
            )
        if self.blocker == "score_below_threshold":
            raise ValueError(
                f"Cannot move spec to '{target}': average approval score "
                f"({self.average_approval_score:.0f}) is below threshold ({self.threshold}). "
                "Submit additional evaluations with higher scores or lower the threshold."
            )


def evaluate_spec_decomposition(
    evaluations: list | tuple, *, edition: int, required: bool, threshold: int,
) -> SpecDecompositionVerdict:
    """Share the existing gate across promotion, readiness and start.

    Only Current approvals contribute to the native average. A Current reject
    cannot be superseded by another approval; request_changes is not a new veto.
    A disabled gate does not reinterpret historical records or manufacture approval.
    """
    if not required:
        return SpecDecompositionVerdict(required=False, threshold=threshold)
    current = [row for row in evaluations if spec_evaluation_is_current(row, edition)]
    approvals = [row for row in current if row.get("recommendation") == "approve"]
    rejections = [row for row in current if row.get("recommendation") == "reject"]
    average = (sum(row.get("overall_score", 0) for row in approvals) / len(approvals)
               if approvals else None)
    blocker = ("rejected" if rejections else "approval_missing" if not approvals
               else "score_below_threshold" if average < threshold else None)
    return SpecDecompositionVerdict(
        required=True, threshold=threshold,
        current_ids=tuple(row["id"] for row in current),
        approval_ids=tuple(row["id"] for row in approvals),
        rejection_ids=tuple(row["id"] for row in rejections),
        average_approval_score=average, blocker=blocker,
        rejection_names=tuple(row.get("evaluator_name", row.get("evaluator_id", "?"))
                              for row in rejections),
    )


def _recorded_edition(evaluation: Mapping) -> int:
    recorded = evaluation.get('spec_edition')
    if type(recorded) is not int or recorded < 1:
        raise ValueError('spec_evaluation_edition_required')
    return recorded


def spec_evaluation_is_current(evaluation: Mapping, edition: int) -> bool:
    recorded = _recorded_edition(evaluation)
    if evaluation.get('stale'):
        return False
    return recorded == edition


def previous_spec_evaluations(evaluations: list, *, reopened_in_edition: int) -> list:
    """Change lifecycle metadata only; retain every authored historical field."""
    for item in evaluations:
        _recorded_edition(item)
    return [
        {**item, 'stale': True, 'stale_reason': 'spec_reopened',
         'stale_in_edition': reopened_in_edition}
        if not item.get('stale') else dict(item)
        for item in evaluations
    ]


def project_spec_evaluation(evaluation: Mapping, edition: int) -> dict:
    current = spec_evaluation_is_current(evaluation, edition)
    return {**evaluation, 'is_current': current,
            'lifecycle_state': 'current' if current else 'previous'}
