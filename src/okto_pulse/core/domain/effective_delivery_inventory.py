"""Complete prospective inventory; legacy credit remains under its adopted contract.

Qualified responsibility and the remaining relational obligations share this
resolver. Missing allocation is visible, never removed to obtain readiness.
"""

from dataclasses import dataclass
from collections.abc import Mapping

from okto_pulse.core.domain.delivery_evidence import DeliveryBinding
from okto_pulse.core.domain.delivery_inventory import (
    COLLECTIONS,
    delivery_digest,
    delivery_inventory,
)
from okto_pulse.core.domain.criterion_verification import (
    VERIFICATION_REQUIREMENT_FIELDS,
)
from okto_pulse.core.domain.requirement_verification import (
    requirement_verification_digest,
)
from okto_pulse.core.domain.implementation_responsibility import (
    ImplementationResponsibilityPlan,
    RequirementContribution,
    resolve_implementation_responsibility,
)


@dataclass(frozen=True, slots=True)
class EffectiveDeliveryObligation:
    binding: DeliveryBinding
    family: str
    contributions: tuple[RequirementContribution, ...]
    blockers: tuple[str, ...]
    declared_card_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EffectiveDeliveryInventory:
    rows: tuple[EffectiveDeliveryObligation, ...]
    responsibilities: ImplementationResponsibilityPlan
    population_complete: bool
    issues: tuple[str, ...]
    contract_version: str = "effective-delivery-inventory/v1"

    @property
    def complete(self):
        return (
            self.population_complete
            and bool(self.rows)
            and not self.issues
            and all(not row.blockers for row in self.rows)
        )

    @property
    def snapshot_sha256(self):
        return delivery_digest(
            {
                "contract_version": self.contract_version,
                "rows": [
                    {
                        "ref": row.binding.obligation_ref,
                        "definition": row.binding.semantic_sha256,
                        "contributions": sorted(
                            fact.scope_sha256 for fact in row.contributions
                        ),
                        "blockers": row.blockers,
                        "declared_card_ids": row.declared_card_ids,
                    }
                    for row in self.rows
                ],
                "population_complete": self.population_complete,
                "issues": self.issues,
            }
        )

    def card_obligations(self, card_id):
        if not self.population_complete:
            raise ValueError("delivery_inventory_population_unavailable")
        return tuple(
            row
            for row in self.rows
            if card_id in row.declared_card_ids
            or any(fact.card_id == card_id for fact in row.contributions)
        )


def resolve_effective_delivery_inventory(*, spec, cards, qualification):
    collections = {field: getattr(spec, field, None) for _, field in COLLECTIONS}
    responsibilities = resolve_implementation_responsibility(
        board_id=spec.board_id,
        spec_id=spec.id,
        collections=collections,
        cards=cards,
        qualification=qualification,
    )
    complete = responsibilities.population_complete
    issues = set(responsibilities.issues)
    # A missing collection is not an empty population. Bound the entire input,
    # including supplementary collections not visited by qualification.
    for _, field in COLLECTIONS:
        values = getattr(spec, field, None)
        if not isinstance(values, (list, tuple)) or len(values) > 5000:
            complete = False
            issues.add("delivery_inventory_population_unavailable")
    if not complete:
        return EffectiveDeliveryInventory(
            (), responsibilities, False, tuple(sorted(issues))
        )
    if sum(len(values) for values in collections.values()) > 5000:
        return EffectiveDeliveryInventory(
            (), responsibilities, False, ("delivery_inventory_resolution_limit",)
        )
    try:
        legacy = delivery_inventory(spec)
    except (ValueError, TypeError):
        return EffectiveDeliveryInventory(
            (), responsibilities, False, ("delivery_inventory_population_invalid",)
        )
    if len(legacy) > 5000:
        return EffectiveDeliveryInventory(
            (), responsibilities, False, ("delivery_inventory_resolution_limit",)
        )
    by_field = {field: kind for kind, field in VERIFICATION_REQUIREMENT_FIELDS.items()}
    by_prefix = {
        prefix: by_field[field] for prefix, field in COLLECTIONS if field in by_field
    }
    qualified = {
        (row.requirement_type, row.requirement_id): row for row in responsibilities.rows
    }
    eligible = {
        card["id"]: card
        for card in cards
        if isinstance(card, Mapping)
        and card.get("board_id") == spec.board_id
        and card.get("spec_id") == spec.id
        and card.get("card_type") in ("normal", "bug")
        and card.get("archived") is False
        and card.get("status") != "cancelled"
    }
    raw = {
        f"{prefix}:{value.get('id') or f'index-{index}'}": value
        for prefix, field in COLLECTIONS
        for index, value in enumerate(collections[field])
        if isinstance(value, Mapping)
    }
    allocated = set()
    rows = []

    def direct(binding, card_id, *, origin="direct", scope="whole_requirement"):
        return RequirementContribution(
            card_id,
            origin,
            scope,
            (),
            None,
            (),
            delivery_digest(
                {
                    "contract_version": "effective-delivery-contribution/v1",
                    "board_id": spec.board_id,
                    "spec_id": spec.id,
                    "card_id": card_id,
                    "obligation_ref": binding.obligation_ref,
                    "definition_sha256": binding.semantic_sha256,
                    "origin": origin,
                    "scope": scope,
                }
            ),
        )

    for obligation in legacy:
        ref = obligation.binding.obligation_ref
        prefix, identity = ref.split(":", 1)
        facts = ()
        binding = obligation.binding
        blockers = set()
        links = raw.get(ref, {}).get("linked_task_ids") or []
        if not isinstance(links, list) or any(
            not isinstance(link, str) for link in links
        ):
            links = []
            blockers.add("delivery_inventory_links_invalid")
        allocated.update(link for link in links if link in eligible)
        if prefix in by_prefix:
            responsibility = qualified.get((by_prefix[prefix], identity))
            if responsibility is None:
                blockers.add("delivery_inventory_qualification_unavailable")
            else:
                facts = responsibility.contributions
                blockers.update(responsibility.blockers)
                allocated.update(fact.card_id for fact in facts)
                try:
                    binding = DeliveryBinding(
                        ref,
                        requirement_verification_digest(
                            spec.id, by_prefix[prefix], raw[ref]
                        ),
                    )
                except ValueError:
                    blockers.add("delivery_inventory_qualification_unavailable")
        else:
            # Direct links remain explicit ownership. Several owners without a
            # declared split stay pending; do not synthesize whole ownership.
            if len(links) == 1 and links[0] in eligible:
                facts = (direct(obligation.binding, links[0]),)
            elif links:
                blockers.add("delivery_inventory_allocation_unresolved")
        if not facts:
            blockers.add("delivery_inventory_work_unassigned")
        rows.append(
            EffectiveDeliveryObligation(
                binding,
                prefix,
                facts,
                tuple(sorted(blockers)),
                tuple(sorted(set(links) & set(eligible))),
            )
        )
    for card_id in sorted(set(eligible) - allocated):
        card = eligible[card_id]
        # F11 was reproduced against the legacy title-only digest. This is a
        # prospective definition: it does not rewrite any sealed legacy binding.
        normative = {
            key: card.get(key)
            for key in ("title", "description", "details", "card_type")
        }
        binding = DeliveryBinding(
            f"card:{card_id}",
            delivery_digest(
                {"contract_version": "card-delivery-scope/v2", **normative}
            ),
        )
        blockers = (
            ()
            if all(key in card for key in ("title", "description", "details"))
            else ("delivery_inventory_card_scope_unavailable",)
        )
        rows.append(
            EffectiveDeliveryObligation(
                binding,
                "card",
                (direct(binding, card_id, origin="card_scope", scope="whole_card"),),
                blockers,
            )
        )
    if len(rows) > 5000:
        return EffectiveDeliveryInventory(
            (), responsibilities, False, ("delivery_inventory_resolution_limit",)
        )
    return EffectiveDeliveryInventory(
        tuple(sorted(rows, key=lambda row: row.binding.obligation_ref)),
        responsibilities,
        True,
        tuple(sorted(issues)),
    )
