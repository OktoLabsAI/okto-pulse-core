"""Pure authority-preserving semantics for Spec readiness analytics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsEvidenceCurrentness,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
)
from okto_pulse.core.ports.spec_readiness import (
    SPEC_READINESS_CONTRACT_VERSION,
    SpecReadinessEvidenceState,
    SpecReadinessProjection,
    SpecReadinessRow,
    SpecValidationAttemptFacts,
)
from okto_pulse.core.services.analytics_foundation import (
    AnalyticsEvidenceBinding,
    classify_evidence_currentness,
)


@dataclass(frozen=True, slots=True)
class GovernedValidationEpisode:
    episode_id: str
    spec_id: str
    edition: int
    authority_ref: str
    accepted: bool
    occurred_at: datetime

    def __post_init__(self) -> None:
        for field in ("episode_id", "spec_id", "authority_ref"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"spec_readiness_episode_{field}_required")
            object.__setattr__(self, field, value.strip())
        if (
            isinstance(self.edition, bool)
            or not isinstance(self.edition, int)
            or self.edition < 1
        ):
            raise ValueError("spec_readiness_episode_edition_invalid")
        if not isinstance(self.accepted, bool):
            raise ValueError("spec_readiness_episode_outcome_invalid")
        from okto_pulse.core.ports.analytics_foundation import require_utc_datetime

        object.__setattr__(
            self,
            "occurred_at",
            require_utc_datetime(self.occurred_at, field="validation_episode_at"),
        )


def readiness_state_from_currentness(
    currentness: AnalyticsEvidenceCurrentness,
) -> SpecReadinessEvidenceState:
    if not isinstance(currentness, AnalyticsEvidenceCurrentness):
        raise ValueError("spec_readiness_currentness_invalid")
    return SpecReadinessEvidenceState(currentness.value)


class SpecReadinessService:
    """Compose readiness facts without scanning or reinterpreting authorities."""

    @staticmethod
    def classify_currentness(
        *,
        expected: AnalyticsEvidenceBinding,
        evidence: AnalyticsEvidenceBinding | None,
        accepted_previous_authority_refs: frozenset[str] = frozenset(),
    ) -> SpecReadinessEvidenceState:
        return readiness_state_from_currentness(
            classify_evidence_currentness(
                expected=expected,
                evidence=evidence,
                accepted_previous_authority_refs=accepted_previous_authority_refs,
            )
        )

    @staticmethod
    def attempt_facts(
        episodes: tuple[GovernedValidationEpisode, ...],
        *,
        spec_id: str,
        edition: int,
    ) -> SpecValidationAttemptFacts:
        if not isinstance(episodes, tuple) or any(
            not isinstance(item, GovernedValidationEpisode) for item in episodes
        ):
            raise ValueError("spec_readiness_episodes_invalid")
        if tuple(sorted(episodes, key=lambda item: item.occurred_at)) != episodes:
            raise ValueError("spec_readiness_episodes_out_of_order")
        identities = tuple(item.episode_id for item in episodes)
        if len(set(identities)) != len(identities):
            raise ValueError("spec_readiness_episode_duplicate")
        selected = tuple(
            item
            for item in episodes
            if item.spec_id == spec_id and item.edition == edition
        )
        if not selected:
            return SpecValidationAttemptFacts(0, None, 0)
        return SpecValidationAttemptFacts(
            attempts=len(selected),
            first_pass=selected[0].accepted,
            revalidation_count=len(selected) - 1,
        )

    @staticmethod
    def projection(
        *,
        query: AnalyticsFoundationQuery,
        as_of: datetime,
        population_scope: AnalyticsPopulationScope,
        exclusions: AnalyticsExclusionSummary,
        specs: tuple[SpecReadinessRow, ...],
        next_cursor: str | None = None,
    ) -> SpecReadinessProjection:
        return SpecReadinessProjection(
            contract_version=SPEC_READINESS_CONTRACT_VERSION,
            foundation_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            query_fingerprint=query.fingerprint,
            filters=query.filters,
            as_of=as_of,
            population_scope=population_scope,
            exclusions=exclusions,
            specs=specs,
            next_cursor=next_cursor,
        )


__all__ = [
    "GovernedValidationEpisode",
    "SpecReadinessService",
    "readiness_state_from_currentness",
]
