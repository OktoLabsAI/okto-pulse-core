"""One complete relational plan for qualification, admission and transitions."""

from dataclasses import dataclass
from okto_pulse.core.domain.delivery_inventory import COLLECTIONS
from okto_pulse.core.domain.requirement_verification_resolution import (
    resolve_requirement_verification,
)
from okto_pulse.core.domain.verification_plan import resolve_verification_plan
from okto_pulse.core.domain.effective_delivery_inventory import (
    EffectiveDeliveryInventory,
    resolve_effective_delivery_inventory,
)


@dataclass(frozen=True, slots=True)
class SpecExecutionPlan:
    qualification: dict
    inventory: EffectiveDeliveryInventory

    @property
    def complete(self):
        return (
            self.inventory.complete
            and self.qualification.get("method_plan_complete") is True
            and self.qualification.get("verification_work_complete") is True
        )


def resolve_spec_execution_plan(*, spec, cards, admitted_methods):
    qualification = resolve_requirement_verification(
        spec_id=spec.id,
        collections={field: getattr(spec, field, None) for _, field in COLLECTIONS},
    )
    qualification = resolve_verification_plan(
        qualification=qualification,
        board_id=spec.board_id,
        spec_id=spec.id,
        scenarios=getattr(spec, "test_scenarios", None),
        criteria=getattr(spec, "acceptance_criteria", None),
        cards=cards,
        admitted_methods=admitted_methods,
    )
    return SpecExecutionPlan(
        qualification,
        resolve_effective_delivery_inventory(
            spec=spec, cards=cards, qualification=qualification
        ),
    )


def card_verification_plan(plan: SpecExecutionPlan | None, card_id: str) -> dict:
    """Select planned tests by the Card's actual contribution, never all links.

    This is a read projection of the existing plan. It creates no ownership,
    inheritance, test result or proof credit. A Test Card also sees the scenarios
    explicitly allocated to itself. Legacy contracts remain unidentified here.
    """
    if plan is None or not plan.inventory.population_complete:
        return {"complete": False, "status": "legacy_or_unavailable", "items": []}
    criteria = set()
    for row in plan.inventory.card_obligations(card_id):
        for contribution in row.contributions:
            if contribution.card_id == card_id:
                criteria.update(contribution.criterion_ids)
        if row.family == "ac" and card_id in row.declared_card_ids:
            criteria.add(row.binding.obligation_ref.removeprefix("ac:"))
    scenarios = {}
    for requirement in plan.qualification["requirements"]:
        for path in requirement["criteria_paths"]:
            for scenario in path.get("scenario_plans", []):
                if path["criterion_id"] not in criteria and card_id not in scenario["test_card_ids"]:
                    continue
                item = scenarios.setdefault(scenario["scenario_id"], {
                    "scenario_id": scenario["scenario_id"], "method": scenario["method"],
                    "criterion_ids": set(), "test_card_ids": set(), "blockers": set(),
                })
                item["criterion_ids"].add(path["criterion_id"])
                item["test_card_ids"].update(scenario["test_card_ids"])
                item["blockers"].update(scenario["blockers"] + scenario["work_blockers"])
    return {"complete": plan.qualification.get("planning_population_complete") is True,
        "status": "resolved", "items": [
            {**item, **{key: sorted(item[key]) for key in ("criterion_ids", "test_card_ids", "blockers")}}
            for _, item in sorted(scenarios.items())]}
