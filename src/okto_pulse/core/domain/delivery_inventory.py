"""Canonical delivery inventory policy, independent of relational mechanisms.

Extraction from services preserves v0.3.4 bindings and digests unchanged.
Fallback/verification evolution is a separate, explicitly versioned concern.
"""

import hashlib
import json

from okto_pulse.core.domain.delivery_evidence import DeliveryBinding, DeliveryObligation

COLLECTIONS = (
    ("fr", "functional_requirements"),
    ("tr", "technical_requirements"),
    ("br", "business_rules"),
    ("ac", "acceptance_criteria"),
    ("api", "api_contracts"),
    ("ir", "integration_requirements"),
    ("or", "observability_requirements"),
    ("decision", "decisions"),
)
# Operational links and state do not change the obligation being implemented.
_OPERATIONAL = {"linked_task_ids", "status", "created_at", "updated_at"}


def delivery_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


def _collection_obligations(
    prefix: str,
    values: list,
    *,
    linked_card_id: str | None = None,
) -> list[DeliveryObligation]:
    """Obligations of one spec collection, optionally filtered by card links.

    Digests are identical between the spec and card inventories by
    construction: both paths run the same semantic extraction, so the
    migration verdict-equivalence gate compares like with like. String items
    carry no ``linked_task_ids`` and are therefore spec-inventory only.
    """
    result: list[DeliveryObligation] = []
    for index, value in enumerate(values):
        if isinstance(value, dict):
            if value.get("status") in {
                "cancelled",
                "superseded",
                "deprecated",
                "revoked",
            }:
                continue
            if linked_card_id is not None and linked_card_id not in (
                value.get("linked_task_ids") or []
            ):
                continue
            identity = str(value.get("id") or f"index-{index}")
            semantic = {k: v for k, v in value.items() if k not in _OPERATIONAL}
            title = str(
                value.get("title")
                or value.get("text")
                or value.get("description")
                or value.get("rule")
                or identity
            )
        elif isinstance(value, str) and value.strip():
            if linked_card_id is not None:
                continue
            identity, semantic, title = f"index-{index}", value, value
        else:
            raise ValueError("delivery_obligation_inventory_invalid")
        result.append(
            DeliveryObligation(
                DeliveryBinding(f"{prefix}:{identity}", delivery_digest(semantic)),
                title,
            )
        )
    return result


def delivery_inventory(spec: object) -> tuple[DeliveryObligation, ...]:
    result = []
    for prefix, collection in COLLECTIONS:
        values = getattr(spec, collection, None) or []
        if not isinstance(values, list):
            raise ValueError("delivery_obligation_inventory_invalid")
        result.extend(_collection_obligations(prefix, values))
    # A spec without structured obligations still needs an explicit scope decision;
    # it is never silently counted as 100% delivered.
    if not result:
        semantic = {
            name: getattr(spec, name, None)
            for name in ("title", "description", "context")
        }
        result.append(
            DeliveryObligation(
                DeliveryBinding(f"spec:{spec.id}", delivery_digest(semantic)),
                str(spec.title),
            )
        )
    if len({item.binding.obligation_ref for item in result}) != len(result):
        raise ValueError("delivery_obligations_ambiguous")
    return tuple(result)


def card_delivery_inventory(spec: object, card: object) -> tuple[DeliveryObligation, ...]:
    """Obligations of one card, derived from its links in the spec collections.

    The join key is ``linked_task_ids`` on each structured entity (the same
    refs ``delivery_inventory`` produces), so a card sees exactly the
    obligations it is linked to. A card without links receives exactly the
    fallback obligation ``card:<card_id>`` — coverage is never vacuously
    satisfied (BR: nunca vacuamente satisfeito).
    """
    card_id = str(card.id)
    result: list[DeliveryObligation] = []
    for prefix, collection in COLLECTIONS:
        values = getattr(spec, collection, None) or []
        if not isinstance(values, list):
            raise ValueError("delivery_obligation_inventory_invalid")
        result.extend(_collection_obligations(prefix, values, linked_card_id=card_id))
    if not result:
        semantic = {"title": str(card.title)}
        result.append(
            DeliveryObligation(
                DeliveryBinding(f"card:{card_id}", delivery_digest(semantic)),
                str(card.title),
            )
        )
    if len({item.binding.obligation_ref for item in result}) != len(result):
        raise ValueError("delivery_obligations_ambiguous")
    return tuple(result)


class DefaultDeliveryInventoryPolicy:
    """One domain implementation for adapters, projections and lifecycle gates."""

    def spec_obligations(self, spec: object) -> tuple[DeliveryObligation, ...]:
        return delivery_inventory(spec)

    def card_obligations(
        self, spec: object, card: object
    ) -> tuple[DeliveryObligation, ...]:
        return card_delivery_inventory(spec, card)

    def payload_digest(self, value: object) -> str:
        return delivery_digest(value)

    def effective_inventory(self, *, spec, cards, qualification):
        from okto_pulse.core.domain.effective_delivery_inventory import resolve_effective_delivery_inventory
        return resolve_effective_delivery_inventory(spec=spec, cards=cards, qualification=qualification)

    def resolve_implementation_responsibility(self, *, board_id, spec_id, collections, cards, qualification):
        from okto_pulse.core.domain.implementation_responsibility import resolve_implementation_responsibility
        return resolve_implementation_responsibility(
            board_id=board_id, spec_id=spec_id, collections=collections,
            cards=cards, qualification=qualification,
        )
