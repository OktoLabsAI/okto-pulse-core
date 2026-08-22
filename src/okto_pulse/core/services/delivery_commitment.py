"""Sprint activation-baseline writes and read-only delivery commitment."""

from __future__ import annotations

from datetime import datetime

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    require_utc_datetime,
)
from okto_pulse.core.ports.delivery_commitment import (
    DELIVERY_COMMITMENT_CONTRACT_VERSION,
    DeliveryCommitmentProjection,
    SprintCommitmentSlice,
)
from okto_pulse.core.ports.sprint_activation_baseline import (
    SprintActivationBaseline,
    SprintActivationMember,
    SprintCommitmentState,
    get_sprint_activation_baseline_store,
)


class SprintActivationBaselineConflictError(RuntimeError):
    pass


class DeliveryCommitmentService:
    """Own immutable commitment authority; never reconstruct legacy baselines."""

    @staticmethod
    def build_activation_baseline(
        *,
        board_id: str,
        sprint_id: str,
        spec_id: str,
        sprint_version: int,
        activated_at: datetime,
        activated_by: str,
        members: tuple[SprintActivationMember, ...],
    ) -> SprintActivationBaseline:
        return SprintActivationBaseline(
            board_id=board_id,
            sprint_id=sprint_id,
            spec_id=spec_id,
            sprint_version=sprint_version,
            activated_at=activated_at,
            activated_by=activated_by,
            members=tuple(sorted(members)),
        )

    @staticmethod
    async def persist_activation_baseline(
        context: object, baseline: SprintActivationBaseline
    ) -> SprintActivationBaseline:
        store = get_sprint_activation_baseline_store()
        existing = await store.get(
            context, board_id=baseline.board_id, sprint_id=baseline.sprint_id
        )
        if existing is not None:
            # Reopening a Sprint to Active preserves the original commitment.
            # Immutable authority is first-write-wins; current membership is a
            # delta and must never rewrite the activation baseline.
            return existing
        persisted = await store.save_if_absent(context, baseline)
        if persisted != baseline:
            raise SprintActivationBaselineConflictError(
                "sprint_activation_baseline_conflict"
            )
        return persisted

    @staticmethod
    def commitment_slice(
        *,
        sprint_id: str,
        baseline: SprintActivationBaseline | None,
        current_members: tuple[SprintActivationMember, ...],
    ) -> SprintCommitmentSlice:
        if baseline is None:
            return SprintCommitmentSlice(
                sprint_id=sprint_id,
                state=SprintCommitmentState.UNAVAILABLE_LEGACY,
                baseline_ref=None,
                activated_at=None,
                original_member_count=None,
                current_member_count=None,
                added_count=None,
                removed_count=None,
                unavailable_reason="activation_baseline_not_persisted",
            )
        if baseline.sprint_id != sprint_id:
            raise ValueError("delivery_commitment_baseline_sprint_mismatch")
        if not isinstance(current_members, tuple) or any(
            not isinstance(item, SprintActivationMember) for item in current_members
        ):
            raise ValueError("delivery_commitment_current_members_invalid")
        current = {item.card_id for item in current_members}
        original = {item.card_id for item in baseline.members}
        return SprintCommitmentSlice(
            sprint_id=sprint_id,
            state=SprintCommitmentState.AVAILABLE,
            baseline_ref=baseline.baseline_ref,
            activated_at=baseline.activated_at,
            original_member_count=len(original),
            current_member_count=len(current),
            added_count=len(current - original),
            removed_count=len(original - current),
        )

    @staticmethod
    def projection(
        *,
        query: AnalyticsFoundationQuery,
        as_of: datetime,
        population_scope: AnalyticsPopulationScope,
        exclusions: AnalyticsExclusionSummary,
        baselines: dict[str, SprintActivationBaseline | None],
        current_members: dict[str, tuple[SprintActivationMember, ...]],
        next_cursor: str | None = None,
    ) -> DeliveryCommitmentProjection:
        observed_at = require_utc_datetime(
            as_of, field="delivery_commitment_projection_as_of"
        )
        if query.as_of is not None and query.as_of != observed_at:
            raise ValueError("delivery_commitment_as_of_mismatch")
        if population_scope.scope_ref != query.actor_scope_ref:
            raise ValueError("delivery_commitment_population_scope_mismatch")
        if not isinstance(baselines, dict) or not isinstance(current_members, dict):
            raise ValueError("delivery_commitment_sources_invalid")
        if set(baselines) != set(current_members):
            raise ValueError("delivery_commitment_source_scope_mismatch")
        sprints = tuple(
            DeliveryCommitmentService.commitment_slice(
                sprint_id=sprint_id,
                baseline=baselines[sprint_id],
                current_members=current_members[sprint_id],
            )
            for sprint_id in sorted(baselines)
        )
        return DeliveryCommitmentProjection(
            contract_version=DELIVERY_COMMITMENT_CONTRACT_VERSION,
            foundation_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            query_fingerprint=query.fingerprint,
            filters=query.filters,
            as_of=observed_at,
            population_scope=population_scope,
            exclusions=exclusions,
            sprints=sprints,
            next_cursor=next_cursor,
        )


__all__ = [
    "DeliveryCommitmentService",
    "SprintActivationBaselineConflictError",
]
