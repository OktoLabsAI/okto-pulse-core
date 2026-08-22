"""Build policy/resource readiness from public validation and resource facts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
)
from okto_pulse.core.ports.policy_resource_readiness import (
    PolicyAuthority,
    PolicyReadinessFact,
    PolicyReadinessState,
    ResourceL1Fact,
    ResourceL1State,
    ResourceType,
)
from okto_pulse.core.services.policy_resource_readiness import (
    PolicyResourceReadinessService,
)


def _value(value: object) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _validation(spec: object) -> Mapping[str, object] | None:
    rows = getattr(spec, "validations", None)
    if not isinstance(rows, list):
        return None
    current_id = getattr(spec, "current_validation_id", None)
    selected = [
        item
        for item in rows
        if isinstance(item, Mapping)
        and (
            (
                current_id
                and (
                    item.get("id") == current_id
                    or item.get("validation_id") == current_id
                )
            )
            or (not current_id and item.get("is_current") is True)
        )
    ]
    return selected[0] if len(selected) == 1 else None


def _policy(spec: object) -> PolicyReadinessFact:
    item = _validation(spec)
    authority_ref = f"spec:{spec.id}:policy:validation"
    if item is None:
        return PolicyReadinessFact(
            "spec_validation",
            PolicyReadinessState.BLOCKING_PENDING,
            PolicyAuthority.BLOCKING,
            authority_ref,
        )
    identity = item.get("id") or item.get("validation_id")
    edition = item.get("edition") or item.get("validation_edition")
    evidence_ref = f"spec:{spec.id}:validation:{identity}"
    if edition != int(getattr(spec, "edition", 1)):
        return PolicyReadinessFact(
            "spec_validation",
            PolicyReadinessState.STALE,
            PolicyAuthority.BLOCKING,
            authority_ref,
            evidence_ref,
            currentness_reason="previous_edition",
        )
    state = (
        PolicyReadinessState.NATIVE_PASS
        if _value(item.get("outcome")) == "success"
        else PolicyReadinessState.BLOCKING_FAILED
    )
    return PolicyReadinessFact(
        "spec_validation",
        state,
        PolicyAuthority.BLOCKING,
        authority_ref,
        evidence_ref,
    )


def _resource_payload(card: object, resource_type: ResourceType) -> list[object]:
    field = {
        ResourceType.MOCKUP: "screen_mockups",
        ResourceType.KNOWLEDGE_BASE: "knowledge_bases",
    }.get(resource_type)
    if field is None:
        return []
    raw = getattr(card, field, None)
    return raw if isinstance(raw, list) else []


def build_policy_resource_readiness_projection(
    *,
    query: AnalyticsFoundationQuery,
    as_of: datetime,
    specs: Iterable[object],
    cards: Iterable[object],
    architecture_designs: Iterable[object],
    spec_knowledge_bases: Iterable[object],
    not_applicable: Iterable[object],
):
    spec_rows = tuple(specs)
    card_rows = tuple(cards)
    architecture_rows = tuple(architecture_designs)
    kb_rows = tuple(spec_knowledge_bases)
    na_rows = tuple(not_applicable)
    result = []
    for spec in sorted(spec_rows, key=lambda item: str(item.id)):
        spec_id = str(spec.id)
        scoped_cards = tuple(card for card in card_rows if card.spec_id == spec_id)
        active_cards = tuple(
            card
            for card in scoped_cards
            if getattr(card, "archived", False) is not True
            and _value(getattr(card, "status", None)) != "cancelled"
        )
        historical_cards = tuple(
            card for card in scoped_cards if card not in active_cards
        )
        na_types = {
            _value(row.resource_type)
            for row in na_rows
            if row.entity_type == "spec"
            and row.entity_id == spec_id
            and row.active is True
        }
        l1 = []
        l2 = []
        for resource_type in ResourceType:
            authority_ref = f"spec:{spec_id}:resource:{resource_type.value}"
            evidence_by_card: dict[str, tuple[str, ...]] = {}
            if resource_type is ResourceType.ARCHITECTURE:
                for row in architecture_rows:
                    if row.card_id and any(
                        card.id == row.card_id for card in scoped_cards
                    ):
                        evidence_by_card.setdefault(str(row.card_id), tuple())
                        evidence_by_card[str(row.card_id)] += (
                            f"architecture:{row.id}",
                        )
                spec_refs = tuple(
                    f"architecture:{row.id}"
                    for row in architecture_rows
                    if row.spec_id == spec_id
                )
            elif resource_type is ResourceType.KNOWLEDGE_BASE:
                for card in scoped_cards:
                    if _resource_payload(card, resource_type):
                        evidence_by_card[str(card.id)] = (
                            f"card:{card.id}:knowledge_bases",
                        )
                spec_refs = tuple(
                    f"spec-knowledge:{row.id}"
                    for row in kb_rows
                    if row.spec_id == spec_id
                )
            else:
                for card in scoped_cards:
                    if _resource_payload(card, resource_type):
                        evidence_by_card[str(card.id)] = (
                            f"card:{card.id}:screen_mockups",
                        )
                spec_refs = ()
            card_refs = tuple(ref for refs in evidence_by_card.values() for ref in refs)
            na_aliases = {
                resource_type.value,
                "architecture_design"
                if resource_type is ResourceType.ARCHITECTURE
                else resource_type.value,
            }
            if na_types.intersection(na_aliases):
                l1.append(
                    ResourceL1Fact(
                        resource_type,
                        ResourceL1State.NOT_APPLICABLE,
                        PolicyAuthority.BLOCKING,
                        authority_ref,
                        currentness_reason="not_required",
                    )
                )
            elif spec_refs or card_refs:
                l1.append(
                    ResourceL1Fact(
                        resource_type,
                        ResourceL1State.PROVIDED,
                        PolicyAuthority.BLOCKING,
                        authority_ref,
                        (spec_refs + card_refs)[0],
                    )
                )
            else:
                l1.append(
                    ResourceL1Fact(
                        resource_type,
                        ResourceL1State.MISSING,
                        PolicyAuthority.BLOCKING,
                        authority_ref,
                    )
                )
            active_ids = tuple(sorted(str(card.id) for card in active_cards))
            covered_ids = tuple(sorted(set(active_ids).intersection(evidence_by_card)))
            historical_ids = tuple(
                sorted(
                    str(card.id)
                    for card in historical_cards
                    if str(card.id) in evidence_by_card
                )
            )
            evidence_refs = tuple(
                sorted(
                    ref for card_id in covered_ids for ref in evidence_by_card[card_id]
                )
            )
            l2.append(
                PolicyResourceReadinessService.resource_l2_from_authority(
                    resource_type=resource_type,
                    eligible_card_ids=active_ids,
                    covered_eligible_card_ids=covered_ids,
                    cancelled_or_archived_card_ids=historical_ids,
                    evidence_refs=evidence_refs,
                )
            )
        result.append(
            PolicyResourceReadinessService.row(
                spec_id=spec_id,
                edition=int(getattr(spec, "edition", 1)),
                policies=(_policy(spec),),
                resources_l1=tuple(l1),
                resources_l2=tuple(l2),
            )
        )
    return PolicyResourceReadinessService.projection(
        query=query,
        as_of=as_of,
        population_scope=AnalyticsPopulationScope(query.actor_scope_ref, len(result)),
        exclusions=AnalyticsExclusionSummary(),
        specs=tuple(result),
    )


__all__ = ["build_policy_resource_readiness_projection"]
