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
