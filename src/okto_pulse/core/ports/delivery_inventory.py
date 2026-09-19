"""Public policy seam shared by delivery adapters and application gates.

The edition supplies relational facts; Core owns obligation selection and
semantic hashes. No adapter may reproduce this policy or import private
delivery services. The default is pure domain policy, with no persistence,
runtime provider, cache or registration side effect.
"""

from typing import Protocol
from okto_pulse.core.domain.effective_delivery_inventory import EffectiveDeliveryInventory

from okto_pulse.core.domain.delivery_evidence import DeliveryObligation
from okto_pulse.core.domain.delivery_inventory import DefaultDeliveryInventoryPolicy
from okto_pulse.core.domain.implementation_responsibility import ImplementationResponsibilityPlan


class DeliveryInventoryPolicy(Protocol):
    def effective_inventory(self, *, spec: object, cards: list[dict], qualification: dict) -> EffectiveDeliveryInventory: ...

    def spec_obligations(self, spec: object) -> tuple[DeliveryObligation, ...]: ...

    def card_obligations(
        self, spec: object, card: object
    ) -> tuple[DeliveryObligation, ...]: ...

    def payload_digest(self, value: object) -> str: ...

    def resolve_implementation_responsibility(
        self, *, board_id: str, spec_id: str, collections: dict,
        cards: list[dict], qualification: dict,
    ) -> ImplementationResponsibilityPlan: ...


def default_delivery_inventory_policy() -> DeliveryInventoryPolicy:
    """Return the canonical policy without loading any concrete adapter."""
    return DefaultDeliveryInventoryPolicy()
