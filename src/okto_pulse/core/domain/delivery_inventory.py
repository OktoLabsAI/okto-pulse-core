"""Canonical delivery inventory policy, independent of relational mechanisms.

Structured obligations retain authored identities. Current execution planning
owns Card allocation and the complete normative Card scope.
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
) -> list[DeliveryObligation]:
    """Read structured obligations without synthesizing identities."""
    result: list[DeliveryObligation] = []
    for value in values:
        if not isinstance(value, dict):
            raise ValueError("delivery_obligation_inventory_invalid")
        identity = value.get("id")
        if not isinstance(identity, str) or not identity.strip():
            raise ValueError("delivery_obligation_inventory_invalid")
        if value.get("status") in {
            "cancelled", "superseded", "deprecated", "revoked",
        }:
            continue
        excluded = _OPERATIONAL | ({"notes", "locale"} if prefix == "ac" else set())
        semantic = {k: v for k, v in value.items() if k not in excluded}
        title = str(
            value.get("title")
            or value.get("text")
            or value.get("description")
            or value.get("rule")
            or identity
        )
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
        values = getattr(spec, collection, None)
        if values is None:
            values = []
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



class DefaultDeliveryInventoryPolicy:
    """One domain implementation for adapters, projections and lifecycle gates."""

    def execution_plan(self, *, spec, cards, admitted_methods):
        from okto_pulse.core.domain.execution_plan import resolve_spec_execution_plan
        return resolve_spec_execution_plan(spec=spec, cards=cards, admitted_methods=admitted_methods)

    def effective_coverage(self, *, inventory, snapshot, implementations, tests, admitted_methods):
        from okto_pulse.core.domain.effective_delivery_coverage import evaluate_effective_delivery_coverage
        return evaluate_effective_delivery_coverage(inventory=inventory, snapshot=snapshot,
            implementations=implementations, tests=tests, admitted_methods=admitted_methods)

    def spec_obligations(self, spec: object) -> tuple[DeliveryObligation, ...]:
        return delivery_inventory(spec)

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
