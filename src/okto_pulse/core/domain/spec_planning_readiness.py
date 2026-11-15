"""Planning admission is a conjunction of native judgments, never a score."""
from dataclasses import dataclass
from typing import Literal


PlanningState = Literal["ready", "blocked", "unavailable"]
PlanningMode = Literal["off", "advisory", "blocking"]
PLANNING_COMPONENTS = (
    "spec_validation", "decomposition", "requirement_lint", "curated_checklist",
    "policy_compliance", "acceptance_coverage", "test_card_coverage",
    "business_coverage", "technical_coverage", "contract_coverage",
    "integration_coverage", "observability_coverage", "task_requirement_links",
    "decision_verification", "code_evidence", "architecture", "resource_coverage",
)


@dataclass(frozen=True, slots=True)
class SpecPlanningComponent:
    key: str
    state: PlanningState
    mode: PlanningMode | None
    applicable: bool | None
    native_result: str | None = None
    result_ids: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    message: str | None = None
    next_action: str | None = None

    def __post_init__(self) -> None:
        if self.key not in PLANNING_COMPONENTS:
            raise ValueError("spec_planning_component_unknown")
        if self.state not in {"ready", "blocked", "unavailable"}:
            raise ValueError("spec_planning_component_state_invalid")
        if self.mode not in {None, "off", "advisory", "blocking"}:
            raise ValueError("spec_planning_component_mode_invalid")
        if self.applicable is not None and type(self.applicable) is not bool:
            raise ValueError("spec_planning_component_applicability_invalid")
        if self.state == "ready" and (self.mode is None or self.applicable is None):
            raise ValueError("spec_planning_component_authority_missing")
        if self.state == "blocked" and (self.mode != "blocking" or self.applicable is not True):
            raise ValueError("spec_planning_component_block_invalid")

    @classmethod
    def unreadable(cls, key: str) -> "SpecPlanningComponent":
        return cls(key, "unavailable", None, None, reason_codes=("read_access_required",))

    def to_dict(self) -> dict[str, object]:
        return {
            "key": self.key, "state": self.state, "mode": self.mode,
            "applicable": self.applicable, "native_result": self.native_result,
            "result_ids": list(self.result_ids), "reason_codes": list(self.reason_codes),
            "message": self.message, "next_action": self.next_action,
        }


@dataclass(frozen=True, slots=True)
class SpecPlanningReadiness:
    """Satisfaction of planning gates, not lifecycle or actor authorization."""
    components: tuple[SpecPlanningComponent, ...]

    def __post_init__(self) -> None:
        if tuple(item.key for item in self.components) != PLANNING_COMPONENTS:
            raise ValueError("spec_planning_component_census_incomplete")

    @property
    def state(self) -> PlanningState:
        if any(item.state == "unavailable" for item in self.components):
            return "unavailable"
        if any(item.state == "blocked" for item in self.components):
            return "blocked"
        return "ready"

    def to_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "blocked_actions": [] if self.state == "ready" else ["promote_to_validated"],
            "components": [item.to_dict() for item in self.components],
        }


ValidationVerdict = Literal[
    "validated", "validated_with_exception", "ready_to_conclude",
    "not_validated", "unavailable", "not_required",
]


@dataclass(frozen=True, slots=True)
class SpecValidationSummary:
    """Read-only presentation of native judgments; never transition authority.

    Null counts mean the source could not establish that population. The caller
    owns the authorized, consistent snapshot, including effective exceptions.
    Technical receipts and individual assessments stay in the detailed view.
    """

    edition: int
    verdict: ValidationVerdict
    planning_state: PlanningState
    reason_code: str
    blockers_count: int | None
    exception_count: int | None
    next_action: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": "validation-summary/v1",
            "edition": self.edition,
            "verdict": self.verdict,
            "planning_state": self.planning_state,
            "reason_code": self.reason_code,
            "blockers_count": self.blockers_count,
            "exception_count": self.exception_count,
            "next_action": self.next_action,
        }


def build_spec_validation_summary(
    *, planning: SpecPlanningReadiness, edition: int, subject_status: str,
    current_validation_edition: int | None,
    current_validation_outcome: Literal["success", "failed"] | None,
    exception_count: int | None, can_conclude: bool = False,
) -> SpecValidationSummary:
    """Summarize the complete planning census without recomputing any gate.

    A successful quality result alone cannot produce a positive verdict. A
    completed lifecycle also needs current, readable planning facts. This pure
    projection neither clears a historical result nor reopens a completed Spec.
    ``can_conclude`` must come from the caller's native authorization decision.
    """
    if type(edition) is not int or edition < 1:
        raise ValueError("validation_summary_edition_invalid")
    if current_validation_edition is not None and (
        type(current_validation_edition) is not int or current_validation_edition < 1
    ):
        raise ValueError("validation_summary_result_edition_invalid")
    if current_validation_outcome not in {None, "success", "failed"}:
        raise ValueError("validation_summary_outcome_invalid")
    if exception_count is not None and (type(exception_count) is not int or exception_count < 0):
        raise ValueError("validation_summary_exception_count_invalid")
    if type(can_conclude) is not bool:
        raise ValueError("validation_summary_authority_invalid")
    if subject_status not in {"draft", "review", "approved", "validated", "in_progress", "done", "cancelled"}:
        raise ValueError("validation_summary_status_invalid")

    state = planning.state
    blockers = tuple(item for item in planning.components if item.state == "blocked")

    def result(verdict: ValidationVerdict, reason: str, action: str | None = None):
        return SpecValidationSummary(
            edition, verdict, state, reason,
            None if state == "unavailable" else len(blockers), exception_count, action,
        )

    if state == "unavailable":
        return result("unavailable", "planning_unavailable")
    if subject_status in {"draft", "review", "cancelled"}:
        return result("not_validated", "validation_stage_not_reached")
    if blockers:
        first = blockers[0]
        return result("not_validated", next(iter(first.reason_codes), "planning_blocked"), first.next_action)
    quality = planning.components[0]
    if quality.mode == "off":
        return result("not_required", "validation_not_required")
    if current_validation_edition != edition or current_validation_outcome != "success":
        return result("not_validated", "current_validation_required")
    if subject_status == "approved":
        return result("ready_to_conclude", "planning_ready", "submit_spec_validation" if can_conclude else None)
    if exception_count is None:
        return result("unavailable", "validation_exceptions_unavailable")
    return result(
        "validated_with_exception" if exception_count else "validated",
        "validation_completed_with_exception" if exception_count else "validation_completed",
    )
