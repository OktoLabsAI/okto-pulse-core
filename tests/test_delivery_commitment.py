from datetime import UTC, datetime, timedelta

import pytest

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsUtcWindow,
)
from okto_pulse.core.ports.sprint_activation_baseline import (
    SprintActivationBaseline,
    SprintActivationMember,
    SprintCommitmentState,
    register_sprint_activation_baseline_store,
    reset_sprint_activation_baseline_store_for_tests,
)
from okto_pulse.core.services.delivery_commitment import (
    DeliveryCommitmentService,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _member(card_id: str, version: int = 1) -> SprintActivationMember:
    return SprintActivationMember(card_id, "task", version)


def _baseline(*members: SprintActivationMember) -> SprintActivationBaseline:
    return DeliveryCommitmentService.build_activation_baseline(
        board_id="board-1",
        sprint_id="sprint-1",
        spec_id="spec-1",
        sprint_version=4,
        activated_at=NOW,
        activated_by="user-1",
        members=tuple(members) or (_member("card-1"),),
    )


def _query() -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:user-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=7), NOW + timedelta(seconds=1)),
        as_of=NOW,
    )


class _Store:
    def __init__(self, existing=None, *, fail=False):
        self.existing = existing
        self.fail = fail
        self.writes = 0

    async def get(self, context, *, board_id, sprint_id):
        return self.existing

    async def save_if_absent(self, context, baseline):
        self.writes += 1
        if self.fail:
            raise RuntimeError("injected_baseline_write_failure")
        if self.existing is None:
            self.existing = baseline
        return self.existing


@pytest.fixture(autouse=True)
def _reset_store():
    reset_sprint_activation_baseline_store_for_tests()
    yield
    reset_sprint_activation_baseline_store_for_tests()


def test_baseline_ref_is_immutable_ordered_content_digest():
    first = _baseline(_member("card-1"), _member("card-2", 2))
    same = _baseline(_member("card-1"), _member("card-2", 2))

    assert first.baseline_ref == same.baseline_ref
    assert first.member_count == 2
    assert first.canonical_dict()["members"][1]["card_id"] == "card-2"


def test_baseline_members_are_sorted_and_duplicates_fail_closed():
    baseline = DeliveryCommitmentService.build_activation_baseline(
        board_id="board-1",
        sprint_id="sprint-1",
        spec_id="spec-1",
        sprint_version=4,
        activated_at=NOW,
        activated_by="user-1",
        members=(_member("card-2"), _member("card-1")),
    )
    assert tuple(item.card_id for item in baseline.members) == ("card-1", "card-2")

    with pytest.raises(ValueError, match="member_duplicate"):
        _baseline(_member("card-1"), _member("card-1", 2))


@pytest.mark.asyncio
async def test_atomic_store_is_idempotent_for_exact_activation_retry():
    baseline = _baseline()
    store = _Store()
    register_sprint_activation_baseline_store(store)

    assert (
        await DeliveryCommitmentService.persist_activation_baseline(object(), baseline)
        == baseline
    )
    assert (
        await DeliveryCommitmentService.persist_activation_baseline(object(), baseline)
        == baseline
    )
    assert store.writes == 1


@pytest.mark.asyncio
async def test_reactivation_never_overwrites_first_baseline():
    existing = _baseline()
    store = _Store(existing)
    register_sprint_activation_baseline_store(store)
    conflict = DeliveryCommitmentService.build_activation_baseline(
        board_id="board-1",
        sprint_id="sprint-1",
        spec_id="spec-1",
        sprint_version=4,
        activated_at=NOW + timedelta(seconds=1),
        activated_by="user-1",
        members=(_member("card-1"),),
    )

    assert (
        await DeliveryCommitmentService.persist_activation_baseline(object(), conflict)
        == existing
    )
    assert store.writes == 0
    assert store.existing == existing


@pytest.mark.asyncio
async def test_baseline_write_failure_is_not_hidden():
    store = _Store(fail=True)
    register_sprint_activation_baseline_store(store)

    with pytest.raises(RuntimeError, match="injected_baseline_write_failure"):
        await DeliveryCommitmentService.persist_activation_baseline(
            object(), _baseline()
        )


def test_legacy_sprint_never_infers_commitment_from_current_membership():
    commitment = DeliveryCommitmentService.commitment_slice(
        sprint_id="sprint-legacy",
        baseline=None,
        current_members=(_member("current-card"),),
    )

    assert commitment.state is SprintCommitmentState.UNAVAILABLE_LEGACY
    assert commitment.baseline_ref is None
    assert commitment.original_member_count is None
    assert commitment.current_member_count is None
    payload = commitment.canonical_dict()
    assert "original_member_count" not in payload
    assert "current_member_count" not in payload


def test_available_commitment_reports_explicit_deltas_from_baseline():
    baseline = _baseline(_member("card-1"), _member("card-2"))

    commitment = DeliveryCommitmentService.commitment_slice(
        sprint_id="sprint-1",
        baseline=baseline,
        current_members=(_member("card-2"), _member("card-3")),
    )

    assert commitment.state is SprintCommitmentState.AVAILABLE
    assert commitment.original_member_count == 2
    assert commitment.current_member_count == 2
    assert commitment.added_count == 1
    assert commitment.removed_count == 1


def test_projection_reconciles_actor_population_and_transport_payload():
    result = DeliveryCommitmentService.projection(
        query=_query(),
        as_of=NOW,
        population_scope=AnalyticsPopulationScope("actor:user-1", 2),
        exclusions=AnalyticsExclusionSummary(),
        baselines={"sprint-1": _baseline(), "sprint-legacy": None},
        current_members={
            "sprint-1": (_member("card-1"),),
            "sprint-legacy": (_member("current-card"),),
        },
    )

    assert tuple(item.sprint_id for item in result.sprints) == (
        "sprint-1",
        "sprint-legacy",
    )
    assert result.query_fingerprint == _query().fingerprint
    assert result.canonical_dict()["sprints"][1]["state"] == "unavailable_legacy"


def test_projection_source_scope_mismatch_fails_closed():
    with pytest.raises(ValueError, match="source_scope_mismatch"):
        DeliveryCommitmentService.projection(
            query=_query(),
            as_of=NOW,
            population_scope=AnalyticsPopulationScope("actor:user-1", 1),
            exclusions=AnalyticsExclusionSummary(),
            baselines={"sprint-1": _baseline()},
            current_members={},
        )
