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
