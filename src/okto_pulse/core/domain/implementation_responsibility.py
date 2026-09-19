"""Typed declared contribution facts, shared by inventory and planning consumers.

No execution credit is inferred. Explicit Card links take precedence. A BR with
no direct allocation may reuse an unambiguous FR contribution; shared criteria
with several possible owners require an explicit allocation, never fan-out.
"""

import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass

from okto_pulse.core.domain.criterion_verification import (
    VERIFICATION_REQUIREMENT_FIELDS,
)
from okto_pulse.core.domain.implementation_plan import RequirementImplementationPlan
from okto_pulse.core.domain.requirement_verification import (
    requirement_verification_digest,
)


@dataclass(frozen=True, slots=True)
class ContributionSource:
    requirement_type: str
    requirement_id: str
    scope_sha256: str


@dataclass(frozen=True, slots=True)
class RequirementContribution:
    card_id: str
    origin: str
    scope: str
    criterion_ids: tuple[str, ...]
    summary: str | None
    sources: tuple[ContributionSource, ...]
    scope_sha256: str


@dataclass(frozen=True, slots=True)
class RequirementResponsibility:
    requirement_type: str
    requirement_id: str
    contributions: tuple[RequirementContribution, ...]
    blockers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ImplementationResponsibilityPlan:
    rows: tuple[RequirementResponsibility, ...]
    population_complete: bool
    issues: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return (
            self.population_complete
            and bool(self.rows)
            and not self.issues
            and all(not row.blockers for row in self.rows)
        )


def _digest(value):
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


def resolve_implementation_responsibility(
    *, board_id, spec_id, collections, cards, qualification
):
    """Use current same-scope facts. Limits/absence/ambiguity never mean zero work."""
    issues = set()
    complete = qualification["population_complete"]
    if not qualification["criteria_resolution_complete"]:
        issues.add("implementation_qualification_unresolved")
    if not isinstance(cards, (list, tuple)) or len(cards) > 5000:
        cards = []
        complete = False
        issues.add("implementation_card_population_unavailable")
    card_counts = Counter(
        card.get("id")
        for card in cards
        if isinstance(card, Mapping) and isinstance(card.get("id"), str)
    )
    eligible = {}
    for card in cards:
        if (
            not isinstance(card, Mapping)
            or not isinstance(card.get("id"), str)
            or card_counts[card["id"]] != 1
        ):
            complete = False
            issues.add("implementation_card_identity_invalid")
            continue
        if card.get("board_id") != board_id or card.get("spec_id") != spec_id:
            complete = False
            issues.add("implementation_card_scope_invalid")
            continue
        if (
            not card["id"].strip()
            or len(card["id"]) > 255
            or type(card.get("archived")) is not bool
            or card.get("card_type") not in ("normal", "bug", "test")
            or card.get("status")
            not in (
                "not_started",
                "started",
                "in_progress",
                "validation",
                "rejected",
                "on_hold",
                "done",
                "cancelled",
            )
        ):
            complete = False
            issues.add("implementation_card_state_invalid")
            continue
        if (
            card.get("card_type") in ("normal", "bug")
            and card.get("archived") is False
            and card.get("status")
            in (
                "not_started",
                "started",
                "in_progress",
                "validation",
                "rejected",
                "on_hold",
                "done",
            )
        ):
            eligible[card["id"]] = card
    raw_rows = {}
    for kind, field in VERIFICATION_REQUIREMENT_FIELDS.items():
        values = collections.get(field)
        if not isinstance(values, (list, tuple)) or len(values) > 5000:
            complete = False
            issues.add("implementation_requirement_population_unavailable")
            continue
        for value in values:
            if isinstance(value, Mapping) and isinstance(value.get("id"), str):
                raw_rows[(kind, value["id"])] = value
    criterion_values = collections.get("acceptance_criteria")
    if not isinstance(criterion_values, (list, tuple)) or len(criterion_values) > 5000:
        criterion_values = []
        complete = False
        issues.add("implementation_criterion_population_unavailable")
    criteria = {
        value["id"]: value
        for value in criterion_values
        if isinstance(value, Mapping) and isinstance(value.get("id"), str)
    }
    rows = {}
    row_criteria = {}
    facts = {}
    blockers = {}
    expansions = 0

    def bind(key, card_id, origin, scope, ids, summary, sources=()):
        nonlocal expansions, complete
        ids = tuple(sorted(ids))
        expansions += max(1, len(ids))
        if expansions > 4096:
            complete = False
            issues.add("implementation_responsibility_resolution_limit")
            return None
        definitions = [
            {
                k: v
                for k, v in criteria.get(identity, {}).items()
                if k
                not in {
                    "linked_task_ids",
                    "notes",
                    "created_at",
                    "updated_at",
                    "status",
                }
            }
            for identity in ids
        ]
        digest = _digest(
            {
                "contract_version": "implementation-contribution/v1",
                "board_id": board_id,
                "spec_id": spec_id,
                "requirement_type": key[0],
                "requirement_id": key[1],
                "definition_sha256": requirement_verification_digest(
                    spec_id, key[0], raw_rows[key]
                ),
                "card_id": card_id,
                "origin": origin,
                "scope": scope,
                "criteria": definitions,
                "summary": summary,
                "sources": [asdict(source) for source in sources],
            }
        )
        return RequirementContribution(
            card_id, origin, scope, ids, summary, tuple(sources), digest
        )

    for row in qualification["requirements"]:
        key = row["requirement_type"], row["requirement_id"]
        rows[key] = row
        row_criteria[key] = {path["criterion_id"] for path in row["criteria_paths"]}
        facts[key] = []
        blockers[key] = set()
        if not row["qualification_resolved"]:
            blockers[key].add("implementation_qualification_unresolved")
        raw = raw_rows.get(key, {})
        links = raw.get("linked_task_ids") or []
        if not isinstance(links, list) or any(
            not isinstance(value, str) or not value.strip() for value in links
        ):
            blockers[key].add("implementation_canonical_links_invalid")
            continue
        try:
            plan = (
                RequirementImplementationPlan.model_validate(raw["implementation_plan"])
                if raw.get("implementation_plan") is not None
                else None
            )
        except ValueError:
            blockers[key].add("implementation_plan_invalid")
            continue
        if plan is None:
            if key[0] != "business_rule" or links:
                blockers[key].add("implementation_plan_required")
            continue
        allocated = {item.card_id for item in plan.contributions}
        if allocated != set(links):
            blockers[key].add("implementation_contribution_links_changed")
        for contribution in plan.contributions:
            if (
                contribution.card_id not in eligible
                or contribution.card_id not in links
            ):
                blockers[key].add("implementation_contribution_card_unavailable")
                continue
            selected = (
                row_criteria[key]
                if contribution.scope == "whole_requirement"
                else set(contribution.criterion_ids)
            )
            if not selected.issubset(row_criteria[key]):
                blockers[key].add("implementation_contribution_criterion_unresolved")
                continue
            fact = bind(
                key,
                contribution.card_id,
                "direct",
                contribution.scope,
                selected,
                contribution.summary,
            )
            if fact:
                facts[key].append(fact)
        covered = set().union(*(set(fact.criterion_ids) for fact in facts[key]))
        if not row_criteria[key].issubset(covered):
            blockers[key].add("implementation_contribution_scope_uncovered")

    inheritance_checks = 0
    for key in rows:
        raw = raw_rows.get(key, {})
        if (
            key[0] != "business_rule"
            or raw.get("linked_task_ids")
            or raw.get("implementation_plan") is not None
            or blockers[key]
        ):
            continue
        related = raw.get("linked_requirements") or []
        if (
            not isinstance(related, list)
            or not related
            or len(related) > 100
            or any(
                not isinstance(value, str)
                or ("functional_requirement", value) not in rows
                for value in related
            )
        ):
            blockers[key].add("implementation_inheritance_source_unresolved")
            continue
        sources = [("functional_requirement", value) for value in sorted(set(related))]
        selected_by_card = {}
        for criterion_id in sorted(row_criteria[key]):
            candidates = {}
            for source in sources:
                inheritance_checks += 1
                if inheritance_checks > 8192:
                    complete = False
                    issues.add("implementation_responsibility_resolution_limit")
                    break
                if blockers[source]:
                    continue
                for fact in facts[source]:
                    if criterion_id in fact.criterion_ids or (
                        len(sources) == 1
                        and len(facts[source]) == 1
                        and fact.scope == "whole_requirement"
                    ):
                        candidates.setdefault(fact.card_id, set()).add(
                            ContributionSource(source[0], source[1], fact.scope_sha256)
                        )
            if inheritance_checks > 8192:
                blockers[key].add("implementation_responsibility_resolution_limit")
                break
            if len(candidates) != 1:
                blockers[key].add("implementation_inheritance_allocation_ambiguous")
                continue
            card_id, provenance = next(iter(candidates.items()))
            selected_ids, source_facts = selected_by_card.setdefault(
                card_id, (set(), set())
            )
            selected_ids.add(criterion_id)
            source_facts.update(provenance)
        for card_id, (selected, source_facts) in sorted(selected_by_card.items()):
            fact = bind(
                key,
                card_id,
                "inherited",
                "selected_criteria",
                selected,
                None,
                sorted(
                    source_facts,
                    key=lambda source: (
                        source.requirement_type,
                        source.requirement_id,
                        source.scope_sha256,
                    ),
                ),
            )
            if fact:
                facts[key].append(fact)
        if not facts[key]:
            blockers[key].add("implementation_plan_required")
    return ImplementationResponsibilityPlan(
        tuple(
            RequirementResponsibility(
                key[0], key[1], tuple(facts[key]), tuple(sorted(blockers[key]))
            )
            for key in sorted(rows)
        ),
        complete,
        tuple(sorted(issues)),
    )
