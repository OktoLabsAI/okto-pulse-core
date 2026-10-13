"""P01 closed output, immutable results and shared proof/lifecycle predicates."""

import ast
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.delivery_assessment import (
    AssessmentCompleteness, FactualReason, ReadinessReason, UnitPhaseAssessment, UnitState,
)
from okto_pulse.core.domain.delivery_completeness import calculate_delivery_completeness
from okto_pulse.core.domain.effective_delivery_coverage import evaluate_effective_snapshot
from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.models.delivery_readiness import (
    DeliveryMetric, DeliveryReadiness, ReadinessPinpoint, StateCounts,
)
from test_delivery_completeness import snapshot, with_verifications


def metric(*, satisfied=6, missing=2, waived=0, not_applicable=0, unknown=0, status="known"):
    counts = StateCounts(satisfied=satisfied, missing=missing, waived=waived,
        not_applicable=not_applicable, unknown=unknown)
    applicable = satisfied + missing + waived + unknown
    return dict(unit="obligation", status=status,
        percent=100 * satisfied // applicable if applicable and status == "known" else None,
        total=applicable + not_applicable, applicable=applicable, satisfied=satisfied,
        not_applicable=not_applicable, waived=waived, unresolved=missing + waived + unknown,
        unknown=unknown, counts_by_state=counts.model_dump(), reason_codes=[], known_counts=None)


@pytest.mark.parametrize("damage", ["percent", "total", "applicable", "unresolved", "satisfied", "waived", "unknown", "not_applicable"])
def test_P01_incoherent_metric_is_rejected(damage):
    value = metric()
    value[damage] += 1
    with pytest.raises(ValidationError):
        DeliveryMetric.model_validate(value)


@pytest.mark.parametrize("bad", [-1, True, 2.0, "2"])
def test_P01_counts_are_nonnegative_strict_integers(bad):
    with pytest.raises(ValidationError):
        StateCounts(satisfied=bad)


def test_P01_unknown_population_preserves_null_instead_of_zero():
    value = metric(status="unknown")
    value.update(total=None, applicable=None, unresolved=None,
        known_counts=value["counts_by_state"])
    parsed = DeliveryMetric.model_validate(value)
    decoded = parsed.model_dump(mode="json")
    assert all(key in decoded and decoded[key] is None for key in ("total", "applicable", "unresolved", "percent"))
    assert decoded["known_counts"]["satisfied"] == 6
    with pytest.raises(ValidationError):
        DeliveryMetric.model_validate({**value, "percent": 0})


def test_P01_na_and_known_population_unknown_evaluation_are_distinct():
    na = DeliveryMetric.model_validate(metric(satisfied=0, missing=0, not_applicable=2, status="not_applicable"))
    unavailable = DeliveryMetric.model_validate(metric(satisfied=6, missing=0, unknown=2, status="unknown"))
    assert na.percent is unavailable.percent is None
    assert na.applicable == 0 and unavailable.applicable == 8
    assert na.status != unavailable.status
    with pytest.raises(ValidationError):
        DeliveryMetric.model_validate(metric(satisfied=6, missing=0, unknown=2))


def test_P01_floor_and_waiver_do_not_manufacture_100():
    assert DeliveryMetric.model_validate(metric(satisfied=999, missing=1)).percent == 99
    assert DeliveryMetric.model_validate(metric(satisfied=6, missing=0, waived=2)).percent == 75
    assert DeliveryMetric.model_validate(metric(satisfied=6, missing=0, not_applicable=2)).percent == 100


def test_P01_models_reject_unknown_fields_and_mutation():
    with pytest.raises(ValidationError):
        DeliveryMetric.model_validate({**metric(), "agent_score": 100})
    parsed = DeliveryMetric.model_validate(metric())
    with pytest.raises(ValidationError):
        parsed.percent = 100
    with pytest.raises(ValidationError):
        parsed.counts_by_state.missing = 0
    completeness = AssessmentCompleteness(True, False, True, False)
    with pytest.raises(FrozenInstanceError):
        completeness.scope_resolved = True
    assert completeness.population_known and completeness.evaluation_complete
    assert not completeness.scope_resolved and not completeness.page_complete


def test_P01_reason_vocabulary_is_closed_and_satisfaction_requires_proof():
    assert len(ReadinessReason) == 26
    with pytest.raises(ValueError):
        ReadinessReason("unmapped_native_error")
    with pytest.raises(ValueError, match="satisfied_without_proof"):
        UnitPhaseAssessment(UnitState.SATISFIED, UnitState.MISSING, False, True)
    with pytest.raises(ValueError, match="waiver_without_effective_exception"):
        UnitPhaseAssessment(UnitState.WAIVED, UnitState.MISSING, False, True)
    reason = FactualReason(ReadinessReason.IMPLEMENTATION_MISSING,
        native_reason_codes=("delivery_implementation_missing",))
    with pytest.raises(FrozenInstanceError):
        reason.code = ReadinessReason.VERIFICATION_FAILED


def test_P01_shared_evaluator_separates_proof_from_lifecycle_without_rewriting_facts():
    value = with_verifications(snapshot(1, 1))
    before = repr(value)
    proof = evaluate_effective_snapshot(value, require_lifecycle=False)
    lifecycle = evaluate_effective_snapshot(value)
    assert proof.allowed and proof.rows[0].implementation_satisfied and proof.rows[0].test_satisfied
    assert not lifecycle.allowed
    assert calculate_delivery_completeness(value, "ui").percent == 80
    assert repr(value) == before
    assert value.tests[0].card_status == value.implementations[0].card_status == CardStatus.IN_PROGRESS


@pytest.mark.parametrize("state", [CardStatus.NOT_STARTED, CardStatus.CANCELLED, CardStatus.REJECTED])
def test_P01_proof_projection_does_not_admit_ineligible_card_states(state):
    value = with_verifications(snapshot(1, 1))
    scoped = value.effective_context.tests[0]
    scoped = replace(scoped, fact=replace(scoped.fact, card_status=state))
    value = replace(value, tests=(scoped.fact,), effective_context=replace(value.effective_context, tests=(scoped,)))
    result = evaluate_effective_snapshot(value, require_lifecycle=False)
    assert result.rows[0].implementation_satisfied
    assert not result.rows[0].test_satisfied


def test_P01_domain_foundation_has_no_infrastructure_imports():
    import okto_pulse.core.domain.delivery_assessment as domain
    tree = ast.parse(Path(domain.__file__).read_text(encoding="utf-8"))
    modules = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    assert set(modules) <= {"dataclasses", "enum", "okto_pulse.core.domain.delivery_evidence"}


def pinpoint(**changes):
    return dict(kind="entity", entity_type="acceptance_criterion", entity_id="ac-ports",
        source_ref=None, revision=None, relative_path=None, qualified_symbol=None,
        line_start=None, line_end=None, target_id=None, execution_id=None, record_id=None,
        observation_id=None, precision="entity_only", origin="native_entity", **changes)


def test_P01_pinpoint_allows_entity_only_and_requires_versioned_evidence_for_lines():
    entity = ReadinessPinpoint.model_validate(pinpoint())
    assert entity.revision is entity.line_start is entity.relative_path is None
    span = entity.model_dump() | dict(kind="span", precision="exact_span", source_ref="repo:product",
        revision="a" * 40, relative_path="src/ports.py", line_start=2, line_end=4,
        execution_id="execution-1", origin="execution_receipt")
    assert ReadinessPinpoint.model_validate(span).revision == "a" * 40
    for key in ("revision", "source_ref", "relative_path", "execution_id", "line_start", "line_end"):
        with pytest.raises(ValidationError):
            ReadinessPinpoint.model_validate(span | {key: None})
    for path in ("C:\\private\\ports.py", "/private/ports.py", "../ports.py"):
        with pytest.raises(ValidationError):
            ReadinessPinpoint.model_validate(span | dict(relative_path=path))


def test_P01_response_roundtrip_keeps_nulls_and_rejects_schema_drift():
    unknown = metric(satisfied=0, missing=0, unknown=1, status="unknown")
    data = dict(scope=dict(board_id="b", spec_id="s", card_id=None, card_type=None,
        spec_edition=1, spec_version=1, card_version=None), snapshot_id="a" * 64,
        evaluated_at=datetime.now(timezone.utc), scope_complete=False, evaluation_complete=False,
        implementation=unknown, verification=unknown, delivery_state="unknown", proof_complete=None,
        delivery_gate=dict(status="unknown", mode="blocking", exception_applied=False, exception_refs=[]),
        lifecycle=dict(stored_status="in_progress", check_status="not_evaluated",
            other_blocker_count=None, qualification_is_current=None),
        summary=dict(message_code="assessment_unavailable", message="Assessment unavailable",
            unresolved_units=None, root_causes=0, lifecycle_pending=None, exceptions=0),
        issues=[], next_actions=[], page=dict(total=0, returned=0, has_more=False,
            next_cursor=None, response_truncated=False, phase_filter="all"))
    value = DeliveryReadiness.model_validate(data)
    restored = DeliveryReadiness.model_validate_json(value.model_dump_json())
    assert restored == value
    output = value.model_dump(mode="json")
    assert output["proof_complete"] is output["implementation"]["percent"] is None
    assert output["contract_version"] == "delivery-readiness/v1" and output["metrics_version"] == 2
    for changes in (dict(metrics_version=1), dict(delivery_state="looks_done"),
                    dict(evaluated_at=datetime.now()), dict(mutation_reauthorization_required=False)):
        with pytest.raises(ValidationError):
            DeliveryReadiness.model_validate(data | changes)
