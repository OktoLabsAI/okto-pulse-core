from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusion,
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsUtcWindow,
)
from okto_pulse.core.ports.spec_readiness import (
    SpecReadinessCheck,
    SpecReadinessCheckKind,
    SpecReadinessEvidenceState,
    SpecReadinessRow,
    SpecValidationAttemptFacts,
    SpecValidationMeasures,
    SpecValidationReadiness,
)
from okto_pulse.core.services.analytics_foundation import AnalyticsEvidenceBinding
from okto_pulse.core.services.spec_readiness import (
    GovernedValidationEpisode,
    SpecReadinessService,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _query() -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        "board-1",
        "actor:user-1",
        AnalyticsUtcWindow(NOW - timedelta(days=30), NOW + timedelta(seconds=1)),
        (AnalyticsFilterClause("edition", "eq", 7),),
        NOW,
    )


def _attempts() -> SpecValidationAttemptFacts:
    return SpecValidationAttemptFacts(2, False, 1)


def _measures(*, legacy: int | None = None) -> SpecValidationMeasures:
    return SpecValidationMeasures(80, 81, 82, 83, 12, legacy)


def _current(*, ready: bool = True) -> SpecValidationReadiness:
    return SpecValidationReadiness(
        state=SpecReadinessEvidenceState.CURRENT,
        validation_id="validation-7",
        authority_ref="authority-7",
        evidence_edition=7,
        measures=_measures(legacy=99),
        attempts=_attempts(),
        lifecycle_ready=ready,
    )


def _missing() -> SpecValidationReadiness:
    return SpecValidationReadiness(
        state=SpecReadinessEvidenceState.MISSING,
        measures=SpecValidationMeasures(),
        attempts=SpecValidationAttemptFacts(0, None, 0),
    )


def test_five_canonical_measures_remain_distinct_from_legacy_completeness() -> None:
    measures = _measures(legacy=99)

    assert measures.canonical_complete is True
    assert measures.canonical_present_count == 5
    assert measures.canonical_dict() == {
        "confidence": 80,
        "clarity": 81,
        "assertiveness": 82,
        "decidability": 83,
        "ambiguity": 12,
        "legacy_completeness": 99,
    }


def test_legacy_completeness_never_fills_a_missing_canonical_measure() -> None:
    measures = SpecValidationMeasures(confidence=80, legacy_completeness=95)

    assert measures.canonical_present_count == 1
    assert measures.clarity is None
    with pytest.raises(
        ValueError, match="spec_readiness_incomplete_validation_cannot_be_ready"
    ):
        SpecValidationReadiness(
            state=SpecReadinessEvidenceState.CURRENT,
            validation_id="validation-legacy",
            authority_ref="authority-legacy",
            evidence_edition=7,
            measures=measures,
            attempts=SpecValidationAttemptFacts(1, True, 0),
            lifecycle_ready=True,
        )


def test_attempt_facts_use_distinct_ordered_episodes_of_selected_edition() -> None:
    episodes = (
        GovernedValidationEpisode("e1", "spec-1", 6, "a1", True, NOW),
        GovernedValidationEpisode(
            "e2", "spec-1", 7, "a2", False, NOW + timedelta(seconds=1)
        ),
        GovernedValidationEpisode(
            "e3", "spec-1", 7, "a3", True, NOW + timedelta(seconds=2)
        ),
        GovernedValidationEpisode(
            "e4", "spec-2", 7, "a4", True, NOW + timedelta(seconds=3)
        ),
    )

    assert SpecReadinessService.attempt_facts(
        episodes, spec_id="spec-1", edition=7
    ) == SpecValidationAttemptFacts(2, False, 1)


def test_attempt_facts_reject_out_of_order_or_duplicate_episode_ledger() -> None:
    first = GovernedValidationEpisode("e1", "spec-1", 7, "a1", True, NOW)
    second = GovernedValidationEpisode(
        "e2", "spec-1", 7, "a2", True, NOW + timedelta(seconds=1)
    )
    with pytest.raises(ValueError, match="spec_readiness_episodes_out_of_order"):
        SpecReadinessService.attempt_facts((second, first), spec_id="spec-1", edition=7)
    with pytest.raises(ValueError, match="spec_readiness_episode_duplicate"):
        SpecReadinessService.attempt_facts((first, first), spec_id="spec-1", edition=7)


def test_currentness_is_bound_to_edition_and_authority_not_mutable_status() -> None:
    expected = AnalyticsEvidenceBinding("spec-1", "edition-7", "validation", "a7")
    same_edition_after_status_change = AnalyticsEvidenceBinding(
        "spec-1", "edition-7", "validation", "a7"
    )
    previous = AnalyticsEvidenceBinding("spec-1", "edition-6", "validation", "a6")

    assert (
        SpecReadinessService.classify_currentness(
            expected=expected, evidence=same_edition_after_status_change
        )
        is SpecReadinessEvidenceState.CURRENT
    )
    assert (
        SpecReadinessService.classify_currentness(
            expected=expected,
            evidence=previous,
            accepted_previous_authority_refs=frozenset({"a6"}),
        )
        is SpecReadinessEvidenceState.PREVIOUS
    )


def test_missing_validation_is_pending_even_when_other_evidence_might_exist() -> None:
    row = SpecReadinessRow(
        spec_id="spec-1",
        edition=7,
        validation=_missing(),
        checklist=(),
        requirement_lint=(),
        spec_pending_validation=True,
    )

    assert row.validation.state is SpecReadinessEvidenceState.MISSING
    assert row.spec_pending_validation is True


def test_current_validation_uses_lifecycle_authority_for_pending_state() -> None:
    ready = SpecReadinessRow("spec-1", 7, _current(ready=True), (), (), False)
    blocked = SpecReadinessRow("spec-2", 7, _current(ready=False), (), (), True)

    assert ready.spec_pending_validation is False
    assert blocked.spec_pending_validation is True


def test_restricted_validation_is_not_zero_filled_or_marked_missing() -> None:
    restricted = SpecValidationReadiness(
        state=SpecReadinessEvidenceState.RESTRICTED,
        measures=SpecValidationMeasures(),
        attempts=SpecValidationAttemptFacts(0, None, 0),
    )
    row = SpecReadinessRow("spec-1", 7, restricted, (), (), None)

    assert row.spec_pending_validation is None
    assert row.validation.canonical_dict()["state"] == "restricted"


def test_check_identity_severity_evidence_and_edition_are_preserved() -> None:
    checklist = SpecReadinessCheck(
        SpecReadinessCheckKind.CURATED_CHECKLIST,
        "check.acceptance_criteria",
        "high",
        SpecReadinessEvidenceState.CURRENT,
        "receipt-check-7",
        7,
    )
    lint = SpecReadinessCheck(
        SpecReadinessCheckKind.REQUIREMENT_LINT,
        "lint.atomic_subject",
        "medium",
        SpecReadinessEvidenceState.PREVIOUS,
        "receipt-lint-6",
        6,
    )
    row = SpecReadinessRow("spec-1", 7, _current(), (checklist,), (lint,), False)

    assert row.checklist[0].canonical_dict()["evidence_ref"] == "receipt-check-7"
    assert row.requirement_lint[0].canonical_dict()["evidence_edition"] == 6


def test_current_check_from_another_edition_is_rejected() -> None:
    wrong = SpecReadinessCheck(
        SpecReadinessCheckKind.REQUIREMENT_LINT,
        "lint.atomic_subject",
        "high",
        SpecReadinessEvidenceState.CURRENT,
        "receipt-lint-6",
        6,
    )

    with pytest.raises(
        ValueError, match="spec_readiness_current_check_edition_mismatch"
    ):
        SpecReadinessRow("spec-1", 7, _current(), (), (wrong,), False)


def test_projection_binds_query_population_exclusions_and_as_of() -> None:
    query = _query()
    row = SpecReadinessRow("spec-1", 7, _current(), (), (), False)
    population = AnalyticsPopulationScope("actor:user-1", 1, 2)
    exclusions = AnalyticsExclusionSummary(
        restricted_count=2,
        excluded_count=2,
        reasons=(AnalyticsExclusion("permission_denied", 2),),
    )

    projection = SpecReadinessService.projection(
        query=query,
        as_of=NOW,
        population_scope=population,
        exclusions=exclusions,
        specs=(row,),
    )
    payload = projection.canonical_dict()

    assert projection.query_fingerprint == query.fingerprint
    assert payload["foundation_version"] == "3"
    assert payload["specs"][0]["validation"]["attempts"] == 2
    assert payload["exclusions"]["restricted_count"] == 2


def test_projection_rejects_population_that_does_not_equal_accessible_rows() -> None:
    with pytest.raises(ValueError, match="spec_readiness_population_count_mismatch"):
        SpecReadinessService.projection(
            query=_query(),
            as_of=NOW,
            population_scope=AnalyticsPopulationScope("actor:user-1", 2, 0),
            exclusions=AnalyticsExclusionSummary(),
            specs=(SpecReadinessRow("spec-1", 7, _current(), (), (), False),),
        )
