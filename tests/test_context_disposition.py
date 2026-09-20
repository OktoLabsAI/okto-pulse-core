import pytest
from pydantic import ValidationError

from okto_pulse.core.ports.context_disposition import ContextDisposition, ContextDispositionPlan, ContextTarget, require_context_target_scope


def decision(**changes):
    return ContextDisposition(**{"candidate_sha256": "a" * 64, "action": "retain_history", "rationale": "Administrative history", **changes})


def test_plan_roundtrip_is_closed_and_does_not_claim_authenticated_approval():
    plan = ContextDispositionPlan(migration_id="cutover", decision_reference="operator-input:1", decisions=(decision(),))
    assert ContextDispositionPlan.model_validate_json(plan.model_dump_json()) == plan
    with pytest.raises(ValidationError):
        ContextDispositionPlan(migration_id="cutover", decision_reference="operator-input:1", decisions=(), approved=True)


@pytest.mark.parametrize("changes", [{"action": "approve"}, {"action": "bind_context"}, {"rationale": " "},
    {"candidate_sha256": "G" * 64}, {"targets": (ContextTarget(kind="spec", identity="spec"),)}])
def test_invalid_or_implicit_dispositions_fail(changes):
    with pytest.raises(ValidationError):
        decision(**changes)


def test_target_and_candidate_deduplication_are_explicit():
    target = ContextTarget(kind="card", identity="card")
    with pytest.raises(ValidationError):
        decision(action="bind_context", targets=(target, target))
    with pytest.raises(ValidationError):
        ContextDispositionPlan(migration_id="cutover", decision_reference="input", decisions=(decision(), decision()))
    with pytest.raises(ValidationError):
        ContextTarget(kind="sprint", identity="old")
    with pytest.raises(ValueError, match="scope_mismatch"):
        require_context_target_scope(origin_board="a", target_board="b")
    require_context_target_scope(origin_board="a", target_board="a")
