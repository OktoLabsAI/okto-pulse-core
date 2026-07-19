"""Atomic, idempotent card-to-spec traceability mutation.

Creation and explicit ``link_task`` orchestration call this same function.  It
validates every requested target before mutating either side, eliminating the
historical create-then-link partial state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from okto_pulse.core.services.persistence_mutation import (
    mark_mutable_field_modified,
)


TRACEABILITY_TARGET_FIELDS: Mapping[str, str] = {
    "scenario": "test_scenarios",
    "test_scenario": "test_scenarios",
    "fr": "functional_requirements",
    "functional_requirement": "functional_requirements",
    "rule": "business_rules",
    "business_rule": "business_rules",
}


@dataclass(frozen=True, slots=True)
class TraceabilityLinkResult:
    requested: tuple[tuple[str, str], ...]
    changed: tuple[tuple[str, str], ...]

    @property
    def idempotent(self) -> bool:
        return not self.changed

    def to_dict(self) -> dict[str, object]:
        return {
            "requested": [
                {"target_type": target_type, "target_id": target_id}
                for target_type, target_id in self.requested
            ],
            "changed": [
                {"target_type": target_type, "target_id": target_id}
                for target_type, target_id in self.changed
            ],
            "idempotent": self.idempotent,
        }


class TraceabilityTargetNotFoundError(ValueError):
    def __init__(self, target_type: str, target_id: str) -> None:
        self.target_type = target_type
        self.target_id = target_id
        super().__init__(
            f"Traceability target '{target_id}' ({target_type}) was not found in the spec."
        )


class TraceabilityScenarioLimitError(ValueError):
    """Internal policy signal translated to the public CardOperationError."""

    def __init__(self, provided_count: int, max_scenarios_per_card: int) -> None:
        self.provided_count = provided_count
        self.max_scenarios_per_card = max_scenarios_per_card
        super().__init__(
            f"Cannot link {provided_count} scenarios; board limit is "
            f"{max_scenarios_per_card}."
        )


def _canonical_type(target_type: str) -> str:
    normalized = (target_type or "").strip().lower()
    field_name = TRACEABILITY_TARGET_FIELDS.get(normalized)
    if field_name is None:
        raise ValueError(
            f"Unsupported traceability target_type '{target_type}'. Expected one of: "
            f"{', '.join(sorted(TRACEABILITY_TARGET_FIELDS))}"
        )
    if field_name == "test_scenarios":
        return "scenario"
    if field_name == "functional_requirements":
        return "fr"
    return "rule"


def link_card_traceability(
    *,
    spec: object,
    card: object,
    targets: Sequence[tuple[str, str]],
    max_scenarios_per_card: int | None = None,
) -> TraceabilityLinkResult:
    """Validate all targets, then apply both sides as one in-memory mutation."""

    canonical = tuple(
        dict.fromkeys(
            (_canonical_type(target_type), str(target_id).strip())
            for target_type, target_id in targets
            if str(target_id).strip()
        )
    )
    if not canonical:
        return TraceabilityLinkResult((), ())

    spec_id = str(getattr(spec, "id", "") or "")
    card_spec_id = str(getattr(card, "spec_id", "") or "")
    if spec_id != card_spec_id:
        raise ValueError(
            f"Card belongs to spec '{card_spec_id}', not requested spec '{spec_id}'."
        )

    staged: dict[str, list[dict[str, Any]]] = {}
    locations: dict[tuple[str, str], tuple[str, int]] = {}
    for target_type, target_id in canonical:
        field_name = TRACEABILITY_TARGET_FIELDS[target_type]
        if field_name not in staged:
            source = getattr(spec, field_name, None) or []
            staged[field_name] = [
                dict(item) if isinstance(item, Mapping) else {"legacy_value": item}
                for item in source
            ]
        index = next(
            (
                idx
                for idx, item in enumerate(staged[field_name])
                if str(item.get("id") or "") == target_id
            ),
            None,
        )
        if index is None:
            raise TraceabilityTargetNotFoundError(target_type, target_id)
        locations[(target_type, target_id)] = (field_name, index)

    card_id = str(getattr(card, "id", "") or "")
    changed: list[tuple[str, str]] = []
    for target in canonical:
        field_name, index = locations[target]
        item = staged[field_name][index]
        linked_ids = list(item.get("linked_task_ids") or [])
        if card_id not in linked_ids:
            linked_ids.append(card_id)
            item["linked_task_ids"] = linked_ids
            changed.append(target)

    scenario_ids = [target_id for target_type, target_id in canonical if target_type == "scenario"]
    current_scenarios = list(getattr(card, "test_scenario_ids", None) or [])
    merged_scenarios = current_scenarios + [
        scenario_id for scenario_id in scenario_ids if scenario_id not in current_scenarios
    ]
    if (
        max_scenarios_per_card is not None
        and len(merged_scenarios) > max_scenarios_per_card
    ):
        raise TraceabilityScenarioLimitError(
            len(merged_scenarios), max_scenarios_per_card
        )

    # No mutations occur above this point. Apply staged fields only after the
    # complete target set has passed validation.
    changed_fields = {
        TRACEABILITY_TARGET_FIELDS[target_type]
        for target_type, _ in changed
    }
    for field_name in changed_fields:
        setattr(spec, field_name, staged[field_name])
        mark_mutable_field_modified(spec, field_name)
    if merged_scenarios != current_scenarios:
        card.test_scenario_ids = merged_scenarios
        mark_mutable_field_modified(card, "test_scenario_ids")

    return TraceabilityLinkResult(canonical, tuple(changed))


__all__ = [
    "TRACEABILITY_TARGET_FIELDS",
    "TraceabilityLinkResult",
    "TraceabilityScenarioLimitError",
    "TraceabilityTargetNotFoundError",
    "link_card_traceability",
]
