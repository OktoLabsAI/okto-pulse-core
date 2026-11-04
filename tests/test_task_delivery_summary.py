"""User verdict precedence, currentness and action authority."""

from copy import deepcopy
from dataclasses import replace

import pytest

from okto_pulse.core.models.delivery_readiness import DeliveryMetric
from okto_pulse.core.models.delivery_summary import TaskRequirementSummary
from okto_pulse.core.services.delivery_readiness import EvaluatedDeliveryReadiness
from okto_pulse.core.services.persisted_delivery_readiness import build_readiness_generation
from okto_pulse.core.services.task_delivery_summary import build_task_delivery_summary, present_task_delivery_summary
from test_delivery_obligation_assessment import snapshot
from test_delivery_readiness_access import batch


READ = frozenset({"spec.entity.read", "card.entity.read", "code_traceability.evidence.read"})
WRITE = READ | {"card.conclusion.write", "card.move.in_progress_to_done", "card.move.in_progress_to_validation"}
REQUIRED = TaskRequirementSummary(decision="required", current=True)


def context():
    value = EvaluatedDeliveryReadiness(batch(snapshot()), actor_id="reader").context("ui")
    return value.model_copy(update={"lifecycle": value.lifecycle.model_copy(update={
        "check_status": "allowed", "other_blocker_count": 0, "qualification_is_current": True})})


def metric(state):
    satisfied = int(state == "satisfied")
    unknown = int(state == "unknown")
    return DeliveryMetric(unit="obligation", status="unknown" if unknown else "known",
        percent=None if unknown else satisfied * 100, total=1, applicable=1,
        satisfied=satisfied, not_applicable=0, waived=0, unresolved=1 - satisfied,
        unknown=unknown, counts_by_state={state: 1}, reason_codes=(), known_counts=None)


def summary(value=None, **kwargs):
    return build_task_delivery_summary(value or context(), **dict(
        requirement=REQUIRED, population_known=True, evaluation_complete=True) | kwargs)


def present(value, **kwargs):
    return present_task_delivery_summary(value.model_dump(mode="json") if value else None,
        **dict(board_id="board", spec_id="spec", card_id="ui", spec_edition=2, card_version=1,
               status="in_progress", freshness="current", actor_kind="user", allowed_operations=WRITE) | kwargs)


@pytest.mark.parametrize("state", ["updating_or_unknown", "optional_or_waived", "verification_failed",
    "historical_done", "implementation_pending", "verification_pending", "completion_pending", "delivered", "ready"])
def test_nine_states_have_unambiguous_verdicts_without_mutating_facts(state):
    value = context()
    options = {}
    if state == "updating_or_unknown":
        options["population_known"] = False
    elif state == "optional_or_waived":
        options["requirement"] = TaskRequirementSummary(decision="waived", current=True)
    elif state == "verification_failed":
        value = value.model_copy(update={"verification": metric("failed")})
    elif state == "historical_done":
        value = value.model_copy(update={"lifecycle": value.lifecycle.model_copy(update={
            "stored_status": "done", "qualification_is_current": False})})
    elif state == "implementation_pending":
        value = value.model_copy(update={"implementation": metric("missing")})
    elif state == "verification_pending":
        value = value.model_copy(update={"verification": metric("missing")})
    elif state == "completion_pending":
        value = value.model_copy(update={"lifecycle": value.lifecycle.model_copy(update={
            "check_status": "blocked", "other_blocker_count": 1})})
    elif state == "delivered":
        value = value.model_copy(update={"lifecycle": value.lifecycle.model_copy(update={"stored_status": "done"})})
    before = value.model_dump()
    result = summary(value, **options)
    assert result.state == state
    assert result.reason and result.reason_code and result.next_action is None
    assert result.card_id == "ui" and result.card_version == 1
    assert value.model_dump() == before


def test_unknown_precedes_waiver_failure_and_done_and_never_reports_empty_success():
    value = context()
    value = value.model_copy(update={"verification": metric("failed"),
        "lifecycle": value.lifecycle.model_copy(update={"stored_status": "done"})})
    for completeness in ({"population_known": False}, {"evaluation_complete": False}):
        result = summary(value, requirement=TaskRequirementSummary(decision="waived", current=True), **completeness)
        assert result.state == "updating_or_unknown" and result.pending_count is None
    assert summary(value, requirement=None).state == "updating_or_unknown"
    assert summary(value, requirement=TaskRequirementSummary(decision="waived", current=False)).state == "updating_or_unknown"


def test_optional_precedes_failure_and_failure_precedes_historical_done():
    value = context()
    value = value.model_copy(update={"verification": metric("failed"),
        "lifecycle": value.lifecycle.model_copy(update={"stored_status": "done", "qualification_is_current": False})})
    assert summary(value).state == "verification_failed"
    assert summary(value, requirement=TaskRequirementSummary(decision="optional", current=True)).state == "optional_or_waived"
    value = value.model_copy(update={"implementation": metric("missing"), "verification": metric("satisfied")})
    assert summary(value).state == "historical_done"


def test_unknown_metrics_and_unevaluated_completion_cannot_be_ready():
    value = context()
    assert summary(value.model_copy(update={"verification": metric("unknown")})).state == "updating_or_unknown"
    result = summary(value.model_copy(update={"lifecycle": value.lifecycle.model_copy(update={
        "check_status": "not_evaluated", "qualification_is_current": None, "other_blocker_count": None})}))
    assert result.state == "completion_pending" and result.reason_code == "completion_not_evaluated"
    assert result.pending_count is None


@pytest.mark.parametrize("status,reason", [("not_started", "not_started"), ("on_hold", "paused"), ("cancelled", "cancelled")])
def test_lifecycle_wording_never_claims_unstarted_work_is_in_progress(status, reason):
    value = context()
    value = value.model_copy(update={"implementation": metric("missing"),
        "lifecycle": value.lifecycle.model_copy(update={"stored_status": status})})
    assert summary(value).reason_code == reason
    if status == "cancelled":
        value = value.model_copy(update={"implementation": metric("satisfied")})
        assert summary(value).state == "completion_pending"


@pytest.mark.parametrize("changes", [
    {"freshness": "updating"}, {"freshness": "pending_initial"}, {"freshness": "unavailable"},
    {"spec_edition": 3}, {"card_version": 2}, {"status": "cancelled"},
    {"board_id": "other"}, {"spec_id": "other"}, {"card_id": "other"},
    {"allowed_operations": READ - {"card.entity.read"}},
])
def test_reader_never_reuses_success_across_changed_scope_freshness_or_acl(changes):
    result = present(summary(), **changes)
    assert result.state == "updating_or_unknown" and result.pending_count is None
    assert not result.population_known and not result.evaluation_complete


def test_actions_are_recomputed_for_human_permission_and_never_copied_from_storage():
    value = summary()
    assert present(value).next_action.kind == "conclude"
    assert present(value, actor_kind="agent").next_action.kind == "open_details"
    assert present(value, allowed_operations=READ).next_action.kind == "open_details"
    assert present(value, allowed_operations=frozenset()).next_action is None
    assert present(None).state == "updating_or_unknown"


def test_generation_contains_same_canonical_task_verdict_and_incremental_scope():
    source = batch(snapshot())
    generation = build_readiness_generation(source, captured_revision=1)
    row = next(item for item in generation.summaries if item.card_id == "ui")
    payload = row.payload()
    assert payload["task_delivery"]["contract_version"] == "task-delivery-summary/v1"
    assert payload["task_delivery"]["snapshot_id"] == payload["context"]["snapshot_id"]
    assert payload["task_delivery"]["next_action"] is None
    # Reused rows must acquire the new snapshot identity along with their context.
    changed = replace(row, context={**row.context, "snapshot_id": "b" * 64})
    assert changed.payload()["task_delivery"]["snapshot_id"] == "b" * 64
    assert row.payload() == deepcopy(payload)
