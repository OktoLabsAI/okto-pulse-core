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
    CodeTraceabilityContext,
    CodeTraceabilityLifecycleStatus,
)
from okto_pulse.core.models.schemas import BoardSettings
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
)
from okto_pulse.core.ports.coverage_traceability import (
    CodeEvidenceMatrix,
    CodeEvidenceMatrixState,
    CodeEvidenceExecutionFact,
    CodeEvidenceOverlapFact,
    CodeEvidenceResolutionFact,
    CodeEvidenceTargetFact,
    CodeEvidenceWaiverFact,
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

_GLOBAL_SKIP_FIELDS = {
    CoverageObligationType.ACCEPTANCE_CRITERION: "skip_test_coverage_global",
    CoverageObligationType.FUNCTIONAL_REQUIREMENT: "skip_rules_coverage_global",
    CoverageObligationType.TEST_SCENARIO: "skip_test_coverage_global",
    CoverageObligationType.BUSINESS_RULE: "skip_rules_coverage_global",
    CoverageObligationType.API_CONTRACT: "skip_contract_coverage_global",
    CoverageObligationType.TECHNICAL_REQUIREMENT: "skip_trs_coverage_global",
    CoverageObligationType.DECISION: "skip_decisions_coverage_global",
    CoverageObligationType.INTEGRATION_REQUIREMENT: "skip_ir_coverage_global",
    CoverageObligationType.OBSERVABILITY_REQUIREMENT: "skip_or_coverage_global",
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
    board_id: str,
    board_settings: BoardSettings,
    spec: object,
    obligation_type: CoverageObligationType,
) -> CoverageSkipMetadata:
    global_field = _GLOBAL_SKIP_FIELDS[obligation_type]
    if getattr(board_settings, global_field) is True:
        return CoverageSkipMetadata(
            state=CoverageSkipState.SKIPPED,
            authority_ref=f"board:{board_id}:settings:{global_field}",
            reason_code="global_skip_enabled",
            currentness=CoverageCurrentness.CURRENT,
        )
    spec_field = _SKIP_FIELDS[obligation_type]
    if getattr(spec, spec_field, False) is True:
        return CoverageSkipMetadata(
            state=CoverageSkipState.SKIPPED,
            authority_ref=(
                f"spec:{spec.id}:edition:{getattr(spec, 'edition', 1)}:{spec_field}"
            ),
            reason_code="spec_skip_enabled",
            currentness=CoverageCurrentness.CURRENT,
        )
    return CoverageSkipMetadata()


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


def _code_evidence_matrix(
    *,
    specs: tuple[object, ...],
    cards: tuple[object, ...],
    contexts: tuple[CodeTraceabilityContext, ...] | None,
) -> CodeEvidenceMatrix:
    if contexts is None:
        return CoverageTraceabilityService.code_evidence_matrix(
            authority_state=CodeEvidenceMatrixState.UNAVAILABLE,
            reason="code_evidence_authority_unavailable",
        )

    expected_spec_ids = {str(spec.id) for spec in specs}
    observed_spec_ids = {context.subject_id for context in contexts}
    authority_reason: str | None = None
    if observed_spec_ids != expected_spec_ids:
        authority_reason = "code_evidence_spec_authority_incomplete"
    relevant_collections = {
        "targets",
        "resolutions",
        "executions",
        "overlaps",
        "waivers",
    }
    if any(
        item.collection in relevant_collections
        for context in contexts
        for item in context.omitted_content_manifest
    ):
        authority_reason = "code_evidence_projection_truncated"

    cards_by_id = {str(card.id): card for card in cards}
    spec_versions = {
        str(spec.id): int(getattr(spec, "version", 1)) for spec in specs
    }
    target_domains: dict[str, object] = {}
    resolution_domains: dict[str, object] = {}
    execution_domains: dict[str, object] = {}
    overlap_domains: dict[str, object] = {}
    waiver_domains: dict[str, object] = {}

    def collect(target: dict[str, object], identity: str, value: object) -> None:
        nonlocal authority_reason
        previous = target.get(identity)
        if previous is not None and previous != value:
            authority_reason = "code_evidence_authority_inconsistent"
            return
        target[identity] = value

    for context in contexts:
        for item in context.targets:
            collect(target_domains, item.id, item)
        for item in context.resolutions:
            collect(resolution_domains, item.id, item)
        for item in context.executions:
            collect(execution_domains, item.id, item)
        for item in context.overlaps:
            overlap_id = "overlap:" + _evidence_digest(
                {
                    "target_a_id": item.target_a_id,
                    "target_b_id": item.target_b_id,
                    "resolution_a_id": item.resolution_a_id,
                    "resolution_b_id": item.resolution_b_id,
                }
            )
            collect(overlap_domains, overlap_id, item)
        for item in context.waivers:
            collect(waiver_domains, item.id, item)

    if any(
        str(getattr(target, "card_id", "")) not in cards_by_id
        for target in target_domains.values()
    ):
        authority_reason = "code_evidence_card_authority_missing"

    target_facts: dict[str, CodeEvidenceTargetFact] = {}
    for target_id, target in target_domains.items():
        card = cards_by_id.get(str(getattr(target, "card_id", "")))
        if card is None:
            continue
        delivery_state = _delivery_state(card)
        lifecycle = target.lifecycle_status
        currentness = CoverageCurrentness.CURRENT
        currentness_reason: str | None = None
        if lifecycle is not CodeTraceabilityLifecycleStatus.ACTIVE:
            currentness = CoverageCurrentness.PREVIOUS
            currentness_reason = "target_lifecycle_inactive"
        elif delivery_state is not CoverageDeliveryState.ACTIVE:
            currentness = CoverageCurrentness.PREVIOUS
            currentness_reason = "parent_card_inactive"
        else:
            spec_id = str(getattr(card, "spec_id", ""))
            current_spec_version = spec_versions.get(spec_id)
            if current_spec_version is None:
                currentness = CoverageCurrentness.STALE
                currentness_reason = "spec_authority_missing"
                authority_reason = authority_reason or "code_evidence_spec_authority_missing"
            elif target.source_spec_version != current_spec_version:
                currentness = CoverageCurrentness.STALE
                currentness_reason = "prior_spec_version"
        target_facts[target_id] = CodeEvidenceTargetFact(
            target_id=target.id,
            card_id=target.card_id,
            source_ref=target.source_ref,
            revision=target.revision,
            lifecycle_status=lifecycle,
            delivery_state=delivery_state,
            currentness=currentness,
            currentness_reason=currentness_reason,
            current_resolution_id=target.current_resolution_id,
        )

    resolution_facts: list[CodeEvidenceResolutionFact] = []
    for resolution in resolution_domains.values():
        target = target_domains.get(resolution.target_id)
        target_fact = target_facts.get(resolution.target_id)
        card = (
            cards_by_id.get(str(getattr(target, "card_id", "")))
            if target is not None
            else None
        )
        currentness = CoverageCurrentness.PREVIOUS
        reason = "prior_resolution"
        if target is None or target_fact is None or card is None:
            reason = "target_authority_missing"
            authority_reason = authority_reason or "code_evidence_target_authority_missing"
        elif target_fact.currentness is not CoverageCurrentness.CURRENT:
            reason = "target_not_current"
        elif target.current_resolution_id != resolution.id:
            reason = "prior_resolution"
        elif target.revision != resolution.target_revision:
            currentness = CoverageCurrentness.STALE
            reason = "prior_target_revision"
        elif int(getattr(card, "policy_version", 1)) != resolution.subject_version:
            currentness = CoverageCurrentness.STALE
            reason = "prior_card_version"
        else:
            currentness = CoverageCurrentness.CURRENT
            reason = None
        resolution_facts.append(
            CodeEvidenceResolutionFact(
                resolution_id=resolution.id,
                target_id=resolution.target_id,
                target_revision=resolution.target_revision,
                state=resolution.state,
                currentness=currentness,
                currentness_reason=reason,
                authority_ref=(
                    f"resolution:{resolution.id}:receipt:"
                    f"{resolution.investigation_receipt_id}"
                ),
            )
        )

    execution_facts: list[CodeEvidenceExecutionFact] = []
    for execution in execution_domains.values():
        target_fact = target_facts.get(execution.target_id)
        currentness = CoverageCurrentness.PREVIOUS
        reason = "prior_target_revision"
        if target_fact is None:
            reason = "target_authority_missing"
            authority_reason = authority_reason or "code_evidence_target_authority_missing"
        elif target_fact.currentness is not CoverageCurrentness.CURRENT:
            reason = "target_not_current"
        elif target_fact.revision == execution.target_revision:
            currentness = CoverageCurrentness.CURRENT
            reason = None
        execution_facts.append(
            CodeEvidenceExecutionFact(
                execution_id=execution.id,
                target_id=execution.target_id,
                target_revision=execution.target_revision,
                disposition=execution.disposition,
                currentness=currentness,
                currentness_reason=reason,
                authority_ref=(
                    f"execution:{execution.id}:receipt:"
                    f"{execution.result_investigation_receipt_id}"
                ),
            )
        )

    current_resolutions = {
        item.resolution_id: item
        for item in resolution_facts
        if item.currentness is CoverageCurrentness.CURRENT
    }
    overlap_facts: list[CodeEvidenceOverlapFact] = []
    for overlap_id, overlap in overlap_domains.items():
        target_a = target_facts.get(overlap.target_a_id)
        target_b = target_facts.get(overlap.target_b_id)
        current = (
            target_a is not None
            and target_b is not None
            and target_a.currentness is CoverageCurrentness.CURRENT
            and target_b.currentness is CoverageCurrentness.CURRENT
            and target_a.current_resolution_id == overlap.resolution_a_id
            and target_b.current_resolution_id == overlap.resolution_b_id
            and overlap.resolution_a_id in current_resolutions
            and overlap.resolution_b_id in current_resolutions
        )
        acknowledgement = getattr(overlap, "acknowledgement", None)
        overlap_facts.append(
            CodeEvidenceOverlapFact(
                overlap_id=overlap_id,
                target_a_id=overlap.target_a_id,
                target_b_id=overlap.target_b_id,
                resolution_a_id=overlap.resolution_a_id,
                resolution_b_id=overlap.resolution_b_id,
                severity=overlap.severity,
                disposition=(
                    acknowledgement.disposition if acknowledgement is not None else None
                ),
                currentness=(
                    CoverageCurrentness.CURRENT
                    if current
                    else CoverageCurrentness.PREVIOUS
                ),
                currentness_reason=None if current else "prior_overlap_snapshot",
            )
        )

    waiver_facts = tuple(
        CodeEvidenceWaiverFact(
            waiver_id=waiver.id,
            entity_type=waiver.entity_type,
            entity_id=waiver.entity_id,
            scope=waiver.scope,
            reason_code=waiver.reason_code,
            active=waiver.active,
            currentness=(
                CoverageCurrentness.CURRENT
                if waiver.active
                else CoverageCurrentness.PREVIOUS
            ),
            currentness_reason=None if waiver.active else "waiver_cleared",
            authority_ref=f"waiver:{waiver.id}",
        )
        for waiver in waiver_domains.values()
    )
    authority_state = (
        CodeEvidenceMatrixState.AVAILABLE
        if authority_reason is None
        else (
            CodeEvidenceMatrixState.INCONSISTENT
            if authority_reason == "code_evidence_authority_inconsistent"
            else CodeEvidenceMatrixState.UNAVAILABLE
        )
    )
    return CoverageTraceabilityService.code_evidence_matrix(
        authority_state=authority_state,
        targets=tuple(target_facts.values()),
        resolutions=tuple(resolution_facts),
        executions=tuple(execution_facts),
        overlaps=tuple(overlap_facts),
        waivers=waiver_facts,
        reason=authority_reason,
    )


def build_coverage_traceability_projection(
    *,
    query: AnalyticsFoundationQuery,
    as_of: datetime,
    board: object | None = None,
    specs: Iterable[object],
    cards: Iterable[object],
    code_traceability_contexts: Iterable[CodeTraceabilityContext] | None = None,
) -> CoverageTraceabilityProjection:
    """Project current structured obligations; unavailable authority stays explicit."""
    spec_rows = tuple(specs)
    card_rows = tuple(cards)
    board_settings = BoardSettings.model_validate(
        (getattr(board, "settings", None) if board is not None else None) or {}
    )
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
                            _skip(query.board_id, board_settings, spec, obligation_type)
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
    code_evidence = _code_evidence_matrix(
        specs=spec_rows,
        cards=card_rows,
        contexts=(
            None
            if code_traceability_contexts is None
            else tuple(code_traceability_contexts)
        ),
    )
    matrix_rows = sum(
        len(items)
        for items in (
            code_evidence.targets,
            code_evidence.resolutions,
            code_evidence.executions,
            code_evidence.overlaps,
            code_evidence.waivers,
        )
    )
    evidence_population = AnalyticsPopulationScope(
        query.actor_scope_ref,
        len(evidence) + matrix_rows,
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
        code_evidence=code_evidence,
    )


__all__ = ["build_coverage_traceability_projection"]
