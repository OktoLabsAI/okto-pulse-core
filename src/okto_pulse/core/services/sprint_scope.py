"""Canonical sprint scope resolution and completion evidence matrix.

The resolver is transport/persistence neutral. MCP context, close gates,
analytics and KG projections can consume the same deterministic scope instead
of reimplementing explicit-id plus linked-card union logic.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from okto_pulse.core.services.test_scenario_lifecycle import (
    scenario_has_required_evidence,
)


_SPEC_COLLECTIONS = (
    "functional_requirements",
    "acceptance_criteria",
    "test_scenarios",
    "business_rules",
    "technical_requirements",
    "api_contracts",
    "integration_requirements",
    "observability_requirements",
    "decisions",
)


def _value(obj: object, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _enum_value(value: object) -> str:
    return str(getattr(value, "value", value or ""))


def _item_id(item: object) -> str:
    return str(_value(item, "id", "") or "")


def _linked_task_ids(item: object) -> tuple[str, ...]:
    values = _value(item, "linked_task_ids", ()) or ()
    return tuple(str(value) for value in values if value)


@dataclass(frozen=True, slots=True)
class SprintScope:
    sprint_id: str
    sprint_version: int
    spec_id: str
    spec_version: int
    card_ids: tuple[str, ...]
    items: Mapping[str, tuple[object, ...]]
    provenance: Mapping[str, Mapping[str, tuple[str, ...]]] = field(default_factory=dict)

    def ids(self, collection: str) -> tuple[str, ...]:
        return tuple(_item_id(item) for item in self.items.get(collection, ()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "sprint_id": self.sprint_id,
            "sprint_version": self.sprint_version,
            "spec_id": self.spec_id,
            "spec_version": self.spec_version,
            "card_ids": list(self.card_ids),
            "items": {name: list(values) for name, values in self.items.items()},
            "provenance": {
                name: {item_id: list(sources) for item_id, sources in rows.items()}
                for name, rows in self.provenance.items()
            },
        }


@dataclass(frozen=True, slots=True)
class SprintCompletionBlocker:
    code: str
    collection: str
    item_id: str
    detail: str
    remediation: str

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "collection": self.collection,
            "item_id": self.item_id,
            "detail": self.detail,
            "remediation": self.remediation,
        }


class SprintScopeResolver:
    """Resolve and cache scope by semantic versions and card assignment set."""

    _cache: "OrderedDict[tuple[object, ...], SprintScope]" = OrderedDict()
    _max_cache_entries = 256

    @classmethod
    def clear_cache(cls) -> None:
        cls._cache.clear()

    @classmethod
    def resolve(
        cls,
        *,
        sprint: object,
        spec: object,
        cards: Sequence[object] | None = None,
    ) -> SprintScope:
        cards = tuple(cards if cards is not None else (_value(sprint, "cards", ()) or ()))
        card_fingerprint = tuple(
            sorted(
                (
                    str(_value(card, "id", "")),
                    int(_value(card, "version", 0) or 0),
                    _enum_value(_value(card, "status")),
                )
                for card in cards
            )
        )
        key = (
            str(_value(sprint, "id", "")),
            int(_value(sprint, "version", 0) or 0),
            str(_value(spec, "id", "")),
            int(_value(spec, "version", 0) or 0),
            card_fingerprint,
        )
        cached = cls._cache.get(key)
        if cached is not None:
            cls._cache.move_to_end(key)
            return cached

        card_ids = tuple(sorted(card_id for card_id, _, _ in card_fingerprint if card_id))
        assigned = set(card_ids)
        card_test_ids = {
            str(scenario_id)
            for card in cards
            if _enum_value(_value(card, "card_type")) == "test"
            for scenario_id in (_value(card, "test_scenario_ids", ()) or ())
            if scenario_id
        }
        explicit_by_collection = {
            "test_scenarios": {
                str(value) for value in (_value(sprint, "test_scenario_ids", ()) or ())
            },
            "business_rules": {
                str(value) for value in (_value(sprint, "business_rule_ids", ()) or ())
            },
        }
        resolved: dict[str, tuple[object, ...]] = {}
        provenance: dict[str, dict[str, tuple[str, ...]]] = {}
        for collection in _SPEC_COLLECTIONS:
            selected: list[object] = []
            item_sources: dict[str, tuple[str, ...]] = {}
            explicit_ids = explicit_by_collection.get(collection, set())
            for item in _value(spec, collection, ()) or ():
                if not isinstance(item, Mapping):
                    continue
                item_id = _item_id(item)
                linked = assigned.intersection(_linked_task_ids(item))
                sources: list[str] = []
                if item_id and item_id in explicit_ids:
                    sources.append("explicit_sprint_scope")
                if linked:
                    sources.append("assigned_card_link")
                if collection == "test_scenarios" and item_id in card_test_ids:
                    sources.append("assigned_test_card_scope")
                if sources:
                    selected.append(item)
                    item_sources[item_id] = tuple(sources)
            resolved[collection] = tuple(selected)
            provenance[collection] = item_sources

        scope = SprintScope(
            sprint_id=str(_value(sprint, "id", "")),
            sprint_version=int(_value(sprint, "version", 0) or 0),
            spec_id=str(_value(spec, "id", "")),
            spec_version=int(_value(spec, "version", 0) or 0),
            card_ids=card_ids,
            items=resolved,
            provenance=provenance,
        )
        cls._cache[key] = scope
        cls._cache.move_to_end(key)
        while len(cls._cache) > cls._max_cache_entries:
            cls._cache.popitem(last=False)
        return scope


def completion_blockers(
    scope: SprintScope,
    *,
    skip_test_coverage: bool = False,
    skip_test_evidence: bool = False,
    skip_rules_coverage: bool = False,
    evidence_validator: Callable[[dict[str, Any]], bool] | None = None,
) -> tuple[SprintCompletionBlocker, ...]:
    """Evaluate proportional evidence for every active scoped gate.

    Test scenarios require a terminal successful state. Replayable evidence is
    proportional to executable scope: it is mandatory when an assigned test
    card claims the scenario, while legacy explicit sprint scope keeps the
    historical status-only contract. Explicitly-scoped business rules require
    an assigned-card backlink.
    """

    blockers: list[SprintCompletionBlocker] = []
    if not skip_test_coverage:
        for scenario in scope.items.get("test_scenarios", ()):
            scenario_id = _item_id(scenario)
            status = str(_value(scenario, "status", "draft") or "draft")
            if status not in {"passed", "automated"}:
                blockers.append(
                    SprintCompletionBlocker(
                        "sprint_test_not_successful",
                        "test_scenarios",
                        scenario_id,
                        f"Scoped test scenario '{scenario_id}' is '{status}'.",
                        "execute_and_record_test_scenario",
                    )
                )
                continue
            if not skip_test_evidence:
                sources = scope.provenance.get("test_scenarios", {}).get(
                    scenario_id, ()
                )
                requires_evidence = "assigned_test_card_scope" in sources
                candidate = dict(scenario)
                evidence = candidate.get("evidence") or candidate.get("latest_evidence")
                claims_v2 = bool(
                    isinstance(evidence, dict)
                    and (
                        evidence.get("manifest_ref") is not None
                        or evidence.get("execution_attestation") is not None
                        or evidence.get("execution_receipt") is not None
                    )
                )
                if evidence_validator is not None:
                    evidence_ok = evidence_validator(candidate)
                elif claims_v2:
                    # A persistence-neutral consumer has no ledger authority.
                    # Evidence V2 therefore fails closed unless its caller
                    # supplies an authenticated edition-backed validator.
                    evidence_ok = False
                else:
                    evidence_ok = scenario_has_required_evidence(candidate)
                if requires_evidence and not evidence_ok:
                    blockers.append(
                        SprintCompletionBlocker(
                            "sprint_test_evidence_missing",
                            "test_scenarios",
                            scenario_id,
                            f"Scoped test scenario '{scenario_id}' has no structured evidence.",
                            "attach_replayable_test_evidence",
                        )
                    )

    if not skip_rules_coverage:
        for rule in scope.items.get("business_rules", ()):
            rule_id = _item_id(rule)
            if not set(_linked_task_ids(rule)).intersection(scope.card_ids):
                blockers.append(
                    SprintCompletionBlocker(
                        "sprint_business_rule_uncovered",
                        "business_rules",
                        rule_id,
                        f"Explicitly scoped business rule '{rule_id}' has no assigned-card link.",
                        "link_sprint_card_to_business_rule",
                    )
                )
    return tuple(blockers)


__all__ = [
    "SprintCompletionBlocker",
    "SprintScope",
    "SprintScopeResolver",
    "completion_blockers",
]
