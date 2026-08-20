"""Build canonical coverage facts from the public relational read model."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceSpecRelationType,
    CodeEvidenceType,
    CodeTraceabilityLifecycleStatus,
)
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
)
from okto_pulse.core.ports.coverage_traceability import (
    CodeEvidenceMatrix,
    CodeEvidenceMatrixState,
    CoverageAuthorityState,
    CoverageCurrentness,
    CoverageDeliveryState,
    CoverageEvidenceFact,
    CoverageObligationFact,
    CoverageObligationIdentity,
    CoverageObligationType,
    CoverageSkipMetadata,
    CoverageSkipState,
    CoverageTraceabilityProjection,
)
from okto_pulse.core.services.coverage_traceability import (
    CoverageTraceabilityService,
)


_COLLECTIONS: tuple[tuple[CoverageObligationType, str], ...] = (
    (CoverageObligationType.ACCEPTANCE_CRITERION, "acceptance_criteria"),
    (CoverageObligationType.FUNCTIONAL_REQUIREMENT, "functional_requirements"),
    (CoverageObligationType.TEST_SCENARIO, "test_scenarios"),
    (CoverageObligationType.BUSINESS_RULE, "business_rules"),
    (CoverageObligationType.API_CONTRACT, "api_contracts"),
    (CoverageObligationType.TECHNICAL_REQUIREMENT, "technical_requirements"),
    (CoverageObligationType.DECISION, "decisions"),
    (CoverageObligationType.INTEGRATION_REQUIREMENT, "integration_requirements"),
    (
        CoverageObligationType.OBSERVABILITY_REQUIREMENT,
        "observability_requirements",
    ),
)

_SKIP_FIELDS = {
    CoverageObligationType.ACCEPTANCE_CRITERION: "skip_test_coverage",
    CoverageObligationType.FUNCTIONAL_REQUIREMENT: "skip_rules_coverage",
    CoverageObligationType.TEST_SCENARIO: "skip_test_coverage",
    CoverageObligationType.BUSINESS_RULE: "skip_rules_coverage",
    CoverageObligationType.API_CONTRACT: "skip_contract_coverage",
    CoverageObligationType.TECHNICAL_REQUIREMENT: "skip_trs_coverage",
    CoverageObligationType.DECISION: "skip_decisions_coverage",
    CoverageObligationType.INTEGRATION_REQUIREMENT: "skip_ir_coverage",
    CoverageObligationType.OBSERVABILITY_REQUIREMENT: "skip_or_coverage",
}


def _enum_value(value: object) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


def _structured(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _linked_ids(item: Mapping[str, Any]) -> set[str]:
    raw = item.get("linked_task_ids") or ()
    if not isinstance(raw, list):
        return set()
    return {str(value) for value in raw if isinstance(value, str) and value}


def _derived_links(spec: object) -> dict[tuple[CoverageObligationType, int], set[str]]:
    from okto_pulse.core.services.analytics_service import (
        resolve_linked_criteria_to_indices,
        resolve_linked_fr_indices,
    )

    acceptance = list(_structured(getattr(spec, "acceptance_criteria", None)))
    requirements = list(_structured(getattr(spec, "functional_requirements", None)))
    links: dict[tuple[CoverageObligationType, int], set[str]] = {}
    for scenario in _structured(getattr(spec, "test_scenarios", None)):
        task_ids = _linked_ids(scenario)
        for index in resolve_linked_criteria_to_indices(
            scenario.get("linked_criteria"), acceptance
        ):
            links.setdefault(
                (CoverageObligationType.ACCEPTANCE_CRITERION, index), set()
            ).update(task_ids)
    for rule in _structured(getattr(spec, "business_rules", None)):
        task_ids = _linked_ids(rule)
        for index in resolve_linked_fr_indices(
            rule.get("linked_requirements") or (), requirements
        ):
            links.setdefault(
                (CoverageObligationType.FUNCTIONAL_REQUIREMENT, index), set()
            ).update(task_ids)
    return links


def _skip(
    spec: object, obligation_type: CoverageObligationType
) -> CoverageSkipMetadata:
    field = _SKIP_FIELDS[obligation_type]
    if getattr(spec, field, False) is not True:
        return CoverageSkipMetadata()
    return CoverageSkipMetadata(
        state=CoverageSkipState.SKIPPED,
        authority_ref=f"spec:{spec.id}:{field}",
        reason_code="governed_skip_enabled",
        currentness=CoverageCurrentness.CURRENT,
    )


def _delivery_state(card: object) -> CoverageDeliveryState:
    if getattr(card, "archived", False) is True:
        return CoverageDeliveryState.ARCHIVED
    if _enum_value(getattr(card, "status", None)) == "cancelled":
        return CoverageDeliveryState.CANCELLED
    return CoverageDeliveryState.ACTIVE


def _evidence_digest(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _evidence(
    *,
    card: object,
    identity: CoverageObligationIdentity,
    applicable: bool,
) -> CoverageEvidenceFact:
    card_id = str(card.id)
    card_type = _enum_value(getattr(card, "card_type", None))
    relation = (
        CodeEvidenceSpecRelationType.TESTS
        if identity.obligation_type
        in {
            CoverageObligationType.ACCEPTANCE_CRITERION,
            CoverageObligationType.TEST_SCENARIO,
        }
        or card_type == "test"
        else CodeEvidenceSpecRelationType.IMPLEMENTS
    )
    currentness = (
        CoverageCurrentness.CURRENT if applicable else CoverageCurrentness.STALE
    )
    digest_payload = {
        "card_id": card_id,
        "card_type": card_type,
        "card_version": int(getattr(card, "policy_version", 1)),
        "obligation": identity.canonical_dict(),
        "relation": relation.value,
    }
    return CoverageEvidenceFact(
        evidence_id=(
            f"coverage:{identity.spec_id}:{identity.obligation_type.value}:"
            f"{identity.obligation_id}:{card_id}"
        ),
        evidence_type=(
            CodeEvidenceType.TEST if card_type == "test" else CodeEvidenceType.BEHAVIOR
        ),
        source_ref=f"card:{card_id}",
        obligation=identity,
        relation_type=relation,
        evidence_content_sha256=_evidence_digest(digest_payload),
        parent_card_id=card_id,
        delivery_state=_delivery_state(card),
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        currentness=currentness,
        currentness_reason=None if applicable else "obligation_not_active",
        authority_ref=f"card:{card_id}:policy:{digest_payload['card_version']}",
    )


def build_coverage_traceability_projection(
    *,
    query: AnalyticsFoundationQuery,
    as_of: datetime,
    specs: Iterable[object],
    cards: Iterable[object],
) -> CoverageTraceabilityProjection:
    """Project current structured obligations; unavailable authority stays explicit."""
    spec_rows = tuple(specs)
    card_rows = tuple(cards)
    cards_by_spec: dict[str, dict[str, object]] = {}
    for card in card_rows:
        cards_by_spec.setdefault(str(getattr(card, "spec_id", "")), {})[
            str(card.id)
        ] = card

    obligations: list[CoverageObligationFact] = []
    evidence: list[CoverageEvidenceFact] = []
    for spec in spec_rows:
        spec_id = str(spec.id)
        edition = int(getattr(spec, "edition", 1))
        derived = _derived_links(spec)
        scoped_cards = cards_by_spec.get(spec_id, {})
        for obligation_type, field in _COLLECTIONS:
            collection = _structured(getattr(spec, field, None))
            for index, item in enumerate(collection):
                raw_id = item.get("id")
                structured_id = isinstance(raw_id, str) and bool(raw_id.strip())
                obligation_id = (
                    raw_id.strip() if structured_id else f"legacy-index:{index}"
                )
                identity = CoverageObligationIdentity(
                    spec_id=spec_id,
                    obligation_type=obligation_type,
                    obligation_id=obligation_id,
                    edition=edition,
                    currentness=CoverageCurrentness.CURRENT,
                )
                applicable = str(item.get("status", "active")).lower() == "active"
                authority_state = (
                    CoverageAuthorityState.AVAILABLE
                    if structured_id
                    else CoverageAuthorityState.UNAVAILABLE
                )
                obligations.append(
                    CoverageObligationFact(
                        identity=identity,
                        applicable=applicable,
                        authority_state=authority_state,
                        authority_ref=(
                            f"spec:{spec_id}:edition:{edition}:{field}:{obligation_id}"
                            if structured_id
                            else None
                        ),
                        authority_reason=(
                            None if structured_id else "structured_identity_missing"
                        ),
                        skip=(
                            _skip(spec, obligation_type)
                            if structured_id and applicable
                            else CoverageSkipMetadata()
                        ),
                    )
                )
                if not structured_id:
                    continue
                task_ids = set(_linked_ids(item))
                task_ids.update(derived.get((obligation_type, index), set()))
                for card_id in sorted(task_ids):
                    card = scoped_cards.get(card_id)
                    if card is not None:
                        evidence.append(
                            _evidence(
                                card=card,
                                identity=identity,
                                applicable=applicable,
                            )
                        )

    population = AnalyticsPopulationScope(
        query.actor_scope_ref,
        len(obligations),
    )
    evidence_population = AnalyticsPopulationScope(
        query.actor_scope_ref,
        len(evidence),
    )
    no_exclusions = AnalyticsExclusionSummary()
    return CoverageTraceabilityService.projection(
        query=query,
        as_of=as_of,
        population_scope=population,
        exclusions=no_exclusions,
        evidence_population_scope=evidence_population,
        evidence_exclusions=no_exclusions,
        obligations=tuple(obligations),
        evidence=tuple(evidence),
        code_evidence=CodeEvidenceMatrix(
            state=CodeEvidenceMatrixState.UNAVAILABLE,
            reason="code_evidence_authority_unavailable",
        ),
    )


__all__ = ["build_coverage_traceability_projection"]
