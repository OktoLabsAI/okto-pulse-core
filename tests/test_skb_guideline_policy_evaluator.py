"""SK-B B06 acceptance tests for deterministic policy-evaluator/v1."""

from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import process_time_ns

import pytest

from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    GuidelineEnforcement,
    GuidelinePolicyContractError,
    GuidelinePredicate,
    GuidelineRevision,
    GuidelineRule,
    GuidelineRuleOperator,
    PolicyComplianceState,
    PolicyComplianceReasonCode,
    PolicyEntityType,
    PolicyEvaluationInput,
    PolicyEvaluationOutcome,
    PolicySubjectRef,
    PolicySubjectSnapshot,
    PolicyWaiver,
    PolicyWaiverAuthorization,
    PolicyWaiverEventType,
    PolicyWaiverExpireReasonCode,
    PolicyWaiverStatus,
)
from okto_pulse.core.domain.guideline_policy_evaluator import (
    POLICY_EVALUATOR_VERSION,
    POLICY_RULESET_VERSION,
    PolicyEvaluatorError,
    PolicyOperationalErrorCode,
    build_policy_evaluation_error_result_v1,
    build_policy_evaluation_input_v1,
    evaluate_policy,
    evaluate_policy_predicate,
    policy_binding_head_digest_v1,
    policy_evaluation_input_digest_v1,
    policy_set_digest_v1,
)
from okto_pulse.core.domain.guideline_predicate_catalog import (
    GUIDELINE_PREDICATE_CATALOG_VERSION,
    GuidelinePredicateCatalogError,
    PolicyPredicateOperator,
)


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)
EVALUATED_AT = NOW + timedelta(hours=1)
CONTENT_DIGEST = "a" * 64
EVALUATOR_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "domain"
    / "guideline_policy_evaluator.py"
)
_MISSING = object()


def _predicate(
    operator: str,
    fact: str,
    *,
    value: object = _MISSING,
    values: object = _MISSING,
) -> GuidelinePredicate:
    parameters: list[tuple[str, object]] = [("fact", fact)]
    if value is not _MISSING:
        parameters.append(("value", value))
    if values is not _MISSING:
        parameters.append(("values", values))
    return GuidelinePredicate(
        predicate_code=operator,
        parameters=parameters,
    )


def _rule(
    index: int,
    *,
    predicates: tuple[GuidelinePredicate, ...] | None = None,
    targets: tuple[PolicyEntityType, ...] = (PolicyEntityType.SPEC,),
    enforcement: GuidelineEnforcement = GuidelineEnforcement.ADVISORY,
    operator: GuidelineRuleOperator = GuidelineRuleOperator.ALL,
    waivable: bool = True,
) -> GuidelineRule:
    return GuidelineRule(
        rule_id=f"rule-{index:03}",
        code=f"policy.rule_{index:03}",
        title=f"Policy rule {index}",
        description=f"Deterministic policy rule {index}.",
        target_entity_types=targets,
        predicates=predicates or (_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=enforcement,
        operator=operator,
        waivable=waivable,
    )


def _revision(
    index: int,
    rules: tuple[GuidelineRule, ...],
) -> GuidelineRevision:
    return GuidelineRevision(
        revision_id=f"revision-{index:03}",
        guideline_id=f"guideline-{index:03}",
        revision_number=1,
        semantic_version="1.0.0",
        title=f"Guideline {index}",
        content=f"Executable guideline {index}.",
        content_digest=f"{index + 1:064x}",
        rules=rules,
        created_by="author-1",
        created_at=NOW,
    )


def _binding(
    revision: GuidelineRevision,
    index: int,
    *,
    priority: int = 0,
    default_enforcement: GuidelineEnforcement = (GuidelineEnforcement.ADVISORY),
) -> BoardGuidelineBinding:
    return BoardGuidelineBinding(
        binding_id=f"binding-{index:03}",
        board_id="board-1",
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        semantic_version=revision.semantic_version,
        revision_digest=revision.content_digest,
        priority=priority,
        binding_revision=1,
        adopted_by="owner-1",
        adopted_at=NOW,
        default_enforcement=default_enforcement,
    )


def _snapshot(
    *,
    target: PolicyEntityType = PolicyEntityType.SPEC,
    attributes: tuple[tuple[str, object], ...] = (("resource_gate_ready", True),),
    subject_id: str | None = None,
) -> PolicySubjectSnapshot:
    return PolicySubjectSnapshot(
        subject=PolicySubjectRef(
            board_id="board-1",
            entity_type=target,
            subject_id=subject_id or f"{target.value}-1",
            subject_version=3,
        ),
        content_digest=CONTENT_DIGEST,
        captured_at=NOW,
        attributes=attributes,
    )


def _evaluation_input(
    *,
    snapshot: PolicySubjectSnapshot,
    revisions: tuple[GuidelineRevision, ...],
    bindings: tuple[BoardGuidelineBinding, ...],
):
    return build_policy_evaluation_input_v1(
        evaluation_id="evaluation-1",
        subject_snapshot=snapshot,
        bindings=bindings,
        revisions=revisions,
        requested_by="requester-1",
        requested_at=NOW,
        idempotency_key="evaluate:subject:v3",
    )


def _single_policy(
    rules: tuple[GuidelineRule, ...],
    *,
    snapshot: PolicySubjectSnapshot | None = None,
    default_enforcement: GuidelineEnforcement = (GuidelineEnforcement.ADVISORY),
):
    revision = _revision(1, rules)
    binding = _binding(
        revision,
        1,
        default_enforcement=default_enforcement,
    )
    actual_snapshot = snapshot or _snapshot()
    evaluation_input = _evaluation_input(
        snapshot=actual_snapshot,
        revisions=(revision,),
        bindings=(binding,),
    )
    return evaluation_input, (revision,)


def _approved_waiver(
    *,
    rule: GuidelineRule,
    subject: PolicySubjectRef,
    waiver_id: str = "waiver-1",
    guideline_id: str = "guideline-001",
    revision_id: str = "revision-001",
    requested_at: datetime = NOW,
    reviewed_at: datetime = NOW + timedelta(minutes=5),
    expires_at: datetime = NOW + timedelta(days=1),
) -> PolicyWaiver:
    return PolicyWaiver(
        waiver_id=waiver_id,
        board_id=subject.board_id,
        finding_id=f"finding-{waiver_id}",
        receipt_id=f"receipt-{waiver_id}",
        guideline_id=guideline_id,
        revision_id=revision_id,
        rule_id=rule.rule_id,
        subject=subject,
        status=PolicyWaiverStatus.APPROVED,
        justification="Bounded exception while remediating the finding.",
        evidence_refs=("ticket://waiver-review",),
        requested_by="requester-1",
        requested_at=requested_at,
        waiver_revision=2,
        expires_at=expires_at,
        last_event_id=f"event-{waiver_id}-2",
        last_event_type=PolicyWaiverEventType.APPROVE,
        last_event_at=reviewed_at,
        reviewed_by="reviewer-1",
        reviewed_at=reviewed_at,
        review_reason="Independently reviewed exception.",
    )


def _authorization(
    waiver: PolicyWaiver,
    evaluation_input: PolicyEvaluationInput,
    *,
    resolved_at: datetime = EVALUATED_AT,
) -> PolicyWaiverAuthorization:
    return PolicyWaiverAuthorization(
        waiver=waiver,
        subject_content_digest=(evaluation_input.subject_snapshot.content_digest),
        input_digest=evaluation_input.input_digest,
        policy_set_digest=evaluation_input.policy_set_digest,
        binding_head_digest=evaluation_input.binding_head_digest,
        catalog_version=evaluation_input.catalog_version,
        ruleset_version=evaluation_input.ruleset_version,
        resolved_at=resolved_at,
    )


def test_evaluator_contract_is_pure_versioned_and_has_no_hidden_clock() -> None:
    assert POLICY_EVALUATOR_VERSION == "policy-evaluator/v1"
    assert POLICY_RULESET_VERSION == "policy-ruleset/v1"

    source = EVALUATOR_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(EVALUATOR_PATH))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    assert not any(
        module == forbidden or module.startswith(f"{forbidden}.")
        for module in modules
        for forbidden in (
            "fastapi",
            "httpx",
            "openai",
            "pydantic",
            "requests",
            "sqlalchemy",
            "subprocess",
            "time",
            "okto_pulse.community",
            "okto_pulse.core.infra",
        )
    )
    lowered = source.lower()
    for forbidden in (
        ".now(",
        ".utcnow(",
        "datetime.today(",
        "time.time(",
        "eval(",
        "exec(",
        "compile(",
        "__import__(",
    ):
        assert forbidden not in lowered


@pytest.mark.parametrize(
    ("operator", "fact", "value", "values", "facts", "expected"),
    (
        ("exists", "labels", _MISSING, _MISSING, (("labels", ("api",)),), True),
        ("not_exists", "labels", _MISSING, _MISSING, (), True),
        (
            "eq",
            "resource_gate_ready",
            True,
            _MISSING,
            (("resource_gate_ready", True),),
            True,
        ),
        (
            "ne",
            "resource_gate_ready",
            False,
            _MISSING,
            (("resource_gate_ready", True),),
            True,
        ),
        (
            "in",
            "status",
            _MISSING,
            ("review", "approved"),
            (("status", "review"),),
            True,
        ),
        (
            "not_in",
            "status",
            _MISSING,
            ("draft", "approved"),
            (("status", "review"),),
            True,
        ),
        ("gt", "coverage_percent", 90, _MISSING, (("coverage_percent", 95),), True),
        ("gte", "coverage_percent", 95, _MISSING, (("coverage_percent", 95),), True),
        ("lt", "coverage_percent", 95, _MISSING, (("coverage_percent", 90),), True),
        ("lte", "coverage_percent", 90, _MISSING, (("coverage_percent", 90),), True),
        ("count_eq", "labels", 2, _MISSING, (("labels", ("api", "security")),), True),
        ("count_ne", "labels", 1, _MISSING, (("labels", ("api", "security")),), True),
        ("count_gt", "labels", 1, _MISSING, (("labels", ("api", "security")),), True),
        ("count_gte", "labels", 2, _MISSING, (("labels", ("api", "security")),), True),
        ("count_lt", "labels", 3, _MISSING, (("labels", ("api", "security")),), True),
        ("count_lte", "labels", 2, _MISSING, (("labels", ("api", "security")),), True),
        (
            "contains",
            "labels",
            "security",
            _MISSING,
            (("labels", ("api", "security")),),
            True,
        ),
        (
            "not_contains",
            "labels",
            "ops",
            _MISSING,
            (("labels", ("api", "security")),),
            True,
        ),
    ),
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_every_closed_operator_has_deterministic_semantics(
    operator,
    fact,
    value,
    values,
    facts,
    expected,
) -> None:
    predicate = _predicate(
        operator,
        fact,
        value=value,
        values=values,
    )

    result = evaluate_policy_predicate(
        predicate,
        target_entity_type=PolicyEntityType.SPEC,
        facts=facts,
    )

    assert result.operator is PolicyPredicateOperator(operator)
    assert result.matched is expected
    assert len(result.predicate_digest) == 64


@pytest.mark.parametrize("target", tuple(PolicyEntityType))
def test_every_target_uses_the_same_typed_common_fact_contract(target) -> None:
    result = evaluate_policy_predicate(
        _predicate("eq", "resource_gate_ready", value=True),
        target_entity_type=target,
        facts=(("resource_gate_ready", True),),
    )

    assert result.fact_present is True
    assert result.matched is True


@pytest.mark.parametrize(
    ("operator", "expected"),
    (
        ("exists", False),
        ("not_exists", True),
        ("eq", False),
        ("ne", False),
        ("in", False),
        ("not_in", False),
        ("gt", False),
        ("gte", False),
        ("lt", False),
        ("lte", False),
        ("count_eq", False),
        ("count_ne", False),
        ("count_gt", False),
        ("count_gte", False),
        ("count_lt", False),
        ("count_lte", False),
        ("contains", False),
        ("not_contains", False),
    ),
)
def test_missing_fact_semantics_are_closed(operator, expected) -> None:
    if operator in {"exists", "not_exists"}:
        predicate = _predicate(operator, "labels")
    elif operator in {"in", "not_in"}:
        predicate = _predicate(operator, "status", values=("approved",))
    elif operator.startswith("count_"):
        predicate = _predicate(operator, "labels", value=1)
    elif operator in {"contains", "not_contains"}:
        predicate = _predicate(operator, "labels", value="security")
    elif operator in {"gt", "gte", "lt", "lte"}:
        predicate = _predicate(operator, "coverage_percent", value=90)
    else:
        predicate = _predicate(operator, "resource_gate_ready", value=True)

    result = evaluate_policy_predicate(
        predicate,
        target_entity_type=PolicyEntityType.SPEC,
        facts=(),
    )

    assert result.fact_present is False
    assert result.matched is expected


def test_all_and_any_materialize_every_predicate_without_short_circuit() -> None:
    all_rule = _rule(
        1,
        predicates=(
            _predicate("eq", "resource_gate_ready", value=False),
            _predicate("exists", "status"),
        ),
        operator=GuidelineRuleOperator.ALL,
    )
    any_rule = _rule(
        2,
        predicates=(
            _predicate("eq", "resource_gate_ready", value=True),
            _predicate("exists", "labels"),
        ),
        operator=GuidelineRuleOperator.ANY,
    )
    evaluation_input, revisions = _single_policy(
        (any_rule, all_rule),
        snapshot=_snapshot(
            attributes=(
                ("resource_gate_ready", True),
                ("status", "review"),
            )
        ),
    )

    output = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )

    assert tuple(item.rule_id for item in output.rule_evaluations) == (
        all_rule.rule_id,
        any_rule.rule_id,
    )
    assert tuple(len(item.predicate_results) for item in output.rule_evaluations) == (
        2,
        2,
    )
    assert tuple(item.outcome for item in output.rule_evaluations) == (
        PolicyEvaluationOutcome.FAIL,
        PolicyEvaluationOutcome.PASS,
    )


def test_all_rules_are_evaluated_and_advisory_is_distinct_from_blocking() -> None:
    advisory = _rule(
        1,
        predicates=(_predicate("gte", "coverage_percent", value=90),),
    )
    blocking = _rule(
        2,
        predicates=(_predicate("contains", "labels", value="security"),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    passing = _rule(3)
    evaluation_input, revisions = _single_policy(
        (passing, blocking, advisory),
        snapshot=_snapshot(
            attributes=(
                ("coverage_percent", 60),
                ("labels", ("api",)),
                ("resource_gate_ready", True),
            )
        ),
    )

    output = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )

    assert tuple(item.rule_id for item in output.rule_evaluations) == (
        advisory.rule_id,
        blocking.rule_id,
        passing.rule_id,
    )
    assert output.counts.total_rule_count == 3
    assert output.counts.evaluated_rule_count == 3
    assert output.counts.passed_rule_count == 1
    assert output.counts.failed_rule_count == 2
    assert output.counts.advisory_failure_count == 1
    assert output.counts.blocking_failure_count == 1
    assert output.counts.unwaived_blocking_failure_count == 1
    assert output.result.receipt.outcome is PolicyEvaluationOutcome.FAIL
    assert output.result.receipt.state is PolicyComplianceState.BLOCKED
    assert len(output.result.receipt.findings) == 2


def test_binding_default_never_overrides_explicit_rule_enforcement() -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
        default_enforcement=GuidelineEnforcement.BLOCKING,
    )

    output = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )

    assert output.rule_evaluations[0].enforcement is GuidelineEnforcement.ADVISORY
    assert output.result.receipt.state is PolicyComplianceState.READY


def test_exact_effective_waiver_preserves_finding_and_changes_gate_state() -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
    )
    waiver = _approved_waiver(
        rule=rule,
        subject=evaluation_input.subject_snapshot.subject,
    )

    output = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        waivers=(_authorization(waiver, evaluation_input),),
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )

    assert output.counts.failed_rule_count == 1
    assert output.counts.blocking_failure_count == 1
    assert output.counts.waived_failure_count == 1
    assert output.counts.unwaived_blocking_failure_count == 0
    assert output.result.receipt.outcome is PolicyEvaluationOutcome.FAIL
    assert output.result.receipt.state is PolicyComplianceState.READY_WITH_WAIVERS
    assert output.result.receipt.findings[0].waiver_id == waiver.waiver_id
    assert output.result.receipt.findings[0].blocking is False


def test_raw_waiver_head_is_audit_state_not_evaluator_authorization() -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
    )
    waiver = _approved_waiver(
        rule=rule,
        subject=evaluation_input.subject_snapshot.subject,
    )

    with pytest.raises(
        PolicyEvaluatorError,
        match="policy_evaluator_waivers_invalid",
    ):
        evaluate_policy(
            evaluation_input,
            revisions=revisions,
            waivers=(waiver,),  # type: ignore[arg-type]
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("subject_content_digest", "b" * 64),
        ("input_digest", "c" * 64),
        ("policy_set_digest", "d" * 64),
        ("binding_head_digest", "e" * 64),
        ("catalog_version", "catalog/stale"),
        ("ruleset_version", "ruleset/stale"),
        ("resolved_at", EVALUATED_AT - timedelta(microseconds=1)),
        ("resolved_at", EVALUATED_AT + timedelta(microseconds=1)),
    ),
)
def test_stale_waiver_authorization_fails_closed_on_every_fence(
    field_name: str,
    value: str | datetime,
) -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
    )
    waiver = _approved_waiver(
        rule=rule,
        subject=evaluation_input.subject_snapshot.subject,
    )
    stale = replace(
        _authorization(waiver, evaluation_input),
        **{field_name: value},
    )

    with pytest.raises(
        PolicyEvaluatorError,
        match="policy_evaluator_waiver_authorization_stale",
    ):
        evaluate_policy(
            evaluation_input,
            revisions=revisions,
            waivers=(stale,),
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )


def test_non_effective_waivers_never_suppress_a_blocking_finding() -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
    )
    subject = evaluation_input.subject_snapshot.subject
    requested = PolicyWaiver(
        waiver_id="waiver-requested",
        board_id=subject.board_id,
        finding_id="finding-requested",
        receipt_id="receipt-requested",
        guideline_id="guideline-001",
        revision_id="revision-001",
        rule_id=rule.rule_id,
        subject=subject,
        status=PolicyWaiverStatus.REQUESTED,
        justification="Awaiting review.",
        evidence_refs=("ticket://waiver-requested",),
        requested_by="requester-1",
        requested_at=NOW,
        waiver_revision=1,
        expires_at=NOW + timedelta(days=1),
        last_event_id="event-waiver-requested-1",
        last_event_type=PolicyWaiverEventType.REQUEST,
        last_event_at=NOW,
    )
    rejected = replace(
        _approved_waiver(
            rule=rule,
            subject=subject,
            waiver_id="waiver-rejected",
        ),
        status=PolicyWaiverStatus.REJECTED,
        last_event_id="event-waiver-rejected-3",
        last_event_type=PolicyWaiverEventType.REJECT,
        last_event_at=NOW + timedelta(minutes=6),
    )
    expired = replace(
        _approved_waiver(
            rule=rule,
            subject=subject,
            waiver_id="waiver-expired",
            expires_at=EVALUATED_AT,
        ),
        status=PolicyWaiverStatus.EXPIRED,
        last_event_id="event-waiver-expired-3",
        last_event_type=PolicyWaiverEventType.EXPIRE,
        last_event_at=EVALUATED_AT,
        expire_reason_code=(PolicyWaiverExpireReasonCode.SCHEDULED_EXPIRY),
    )
    future_review = _approved_waiver(
        rule=rule,
        subject=subject,
        waiver_id="waiver-future",
        reviewed_at=EVALUATED_AT + timedelta(minutes=1),
    )
    wrong_subject = _approved_waiver(
        rule=rule,
        subject=replace(subject, subject_id="spec-other"),
        waiver_id="waiver-other-subject",
    )

    for non_effective in (requested, rejected, expired, future_review):
        with pytest.raises(
            GuidelinePolicyContractError,
            match="policy_waiver_authorization_head_not_effective",
        ):
            _authorization(non_effective, evaluation_input)

    output = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        waivers=(_authorization(wrong_subject, evaluation_input),),
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )

    assert output.counts.waived_failure_count == 0
    assert output.counts.unwaived_blocking_failure_count == 1
    assert output.result.receipt.state is PolicyComplianceState.BLOCKED
    assert output.result.receipt.findings[0].waiver_id is None


def test_waived_blocking_and_open_advisory_remain_ready_with_waivers() -> None:
    blocking = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    advisory = _rule(
        2,
        predicates=(_predicate("gte", "coverage_percent", value=90),),
    )
    evaluation_input, revisions = _single_policy(
        (advisory, blocking),
        snapshot=_snapshot(
            attributes=(
                ("coverage_percent", 50),
                ("resource_gate_ready", False),
            )
        ),
    )
    waiver = _approved_waiver(
        rule=blocking,
        subject=evaluation_input.subject_snapshot.subject,
    )

    output = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        waivers=(_authorization(waiver, evaluation_input),),
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )

    assert output.counts.failed_rule_count == 2
    assert output.counts.advisory_failure_count == 1
    assert output.counts.blocking_failure_count == 1
    assert output.counts.waived_failure_count == 1
    assert output.counts.unwaived_blocking_failure_count == 0
    assert output.result.receipt.state is PolicyComplianceState.READY_WITH_WAIVERS
    assert len(output.result.receipt.findings) == 2


def test_effective_waiver_for_non_waivable_rule_fails_closed() -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
        waivable=False,
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
    )
    waiver = _approved_waiver(
        rule=rule,
        subject=evaluation_input.subject_snapshot.subject,
    )

    with pytest.raises(
        PolicyEvaluatorError,
        match="policy_evaluator_non_waivable_rule",
    ):
        evaluate_policy(
            evaluation_input,
            revisions=revisions,
            waivers=(_authorization(waiver, evaluation_input),),
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )


def test_multiple_effective_waivers_for_one_rule_fail_closed() -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
    )
    first = _approved_waiver(
        rule=rule,
        subject=evaluation_input.subject_snapshot.subject,
        waiver_id="waiver-a",
    )
    second = replace(first, waiver_id="waiver-b")

    with pytest.raises(
        PolicyEvaluatorError,
        match="policy_evaluator_multiple_effective_waivers",
    ):
        evaluate_policy(
            evaluation_input,
            revisions=revisions,
            waivers=(
                _authorization(second, evaluation_input),
                _authorization(first, evaluation_input),
            ),
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )


def test_input_rule_fact_and_waiver_permutations_produce_identical_output() -> None:
    first_rule = _rule(
        1,
        predicates=(
            _predicate("contains", "labels", value="security"),
            _predicate("gte", "coverage_percent", value=90),
        ),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    second_rule = _rule(2)
    first_revision = _revision(1, (second_rule, first_rule))
    second_revision = _revision(
        2,
        (
            _rule(
                3,
                predicates=(_predicate("eq", "validation_state", value="current"),),
            ),
        ),
    )
    first_binding = _binding(first_revision, 1, priority=5)
    second_binding = _binding(second_revision, 2, priority=1)
    snapshot_a = _snapshot(
        attributes=(
            ("labels", ("api",)),
            ("coverage_percent", 80),
            ("validation_state", "provider_specific_current"),
            ("resource_gate_ready", True),
        )
    )
    snapshot_b = _snapshot(
        attributes=(
            ("resource_gate_ready", True),
            ("validation_state", "provider_specific_current"),
            ("coverage_percent", 80),
            ("labels", ("api",)),
        )
    )
    input_a = _evaluation_input(
        snapshot=snapshot_a,
        revisions=(first_revision, second_revision),
        bindings=(first_binding, second_binding),
    )
    input_b = _evaluation_input(
        snapshot=snapshot_b,
        revisions=(second_revision, first_revision),
        bindings=(second_binding, first_binding),
    )
    waiver_a = _approved_waiver(
        rule=first_rule,
        subject=snapshot_a.subject,
    )
    irrelevant = replace(
        waiver_a,
        waiver_id="waiver-irrelevant",
        rule_id=second_rule.rule_id,
    )

    output_a = evaluate_policy(
        input_a,
        revisions=(first_revision, second_revision),
        waivers=(
            _authorization(waiver_a, input_a),
            _authorization(irrelevant, input_a),
        ),
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )
    output_b = evaluate_policy(
        input_b,
        revisions=(second_revision, first_revision),
        waivers=(
            _authorization(irrelevant, input_b),
            _authorization(waiver_a, input_b),
        ),
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )

    assert input_a == input_b
    assert output_a == output_b
    assert output_a.evaluation_digest == output_b.evaluation_digest
    assert output_a.result.receipt.receipt_id == output_b.result.receipt.receipt_id
    assert output_a.result.receipt.findings == output_b.result.receipt.findings


def test_execution_identity_separates_history_but_exact_replay_is_stable() -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
    )
    first = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )
    replay = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )
    later_input = replace(evaluation_input, evaluation_id="evaluation-2")
    later = evaluate_policy(
        later_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT + timedelta(minutes=1),
        evaluated_by="evaluator-2",
    )

    assert replay == first
    assert later_input.input_digest == evaluation_input.input_digest
    assert later.evaluation_digest != first.evaluation_digest
    assert later.result.receipt.receipt_id != first.result.receipt.receipt_id
    assert (
        later.result.receipt.findings[0].finding_id
        != first.result.receipt.findings[0].finding_id
    )


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    (
        ("evaluation_id", "evaluation-other"),
        ("evaluated_at", EVALUATED_AT + timedelta(seconds=1)),
        ("evaluated_by", "evaluator-other"),
    ),
)
def test_each_explicit_execution_identity_component_changes_receipt_identity(
    changed_field,
    changed_value,
) -> None:
    rule = _rule(
        1,
        predicates=(_predicate("eq", "resource_gate_ready", value=True),),
        enforcement=GuidelineEnforcement.BLOCKING,
    )
    evaluation_input, revisions = _single_policy(
        (rule,),
        snapshot=_snapshot(attributes=(("resource_gate_ready", False),)),
    )
    base = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )
    changed_input = (
        replace(evaluation_input, evaluation_id=changed_value)
        if changed_field == "evaluation_id"
        else evaluation_input
    )
    changed = evaluate_policy(
        changed_input,
        revisions=revisions,
        evaluated_at=(
            changed_value if changed_field == "evaluated_at" else EVALUATED_AT
        ),
        evaluated_by=(
            changed_value if changed_field == "evaluated_by" else "evaluator-1"
        ),
    )

    assert changed.result.receipt.receipt_id != base.result.receipt.receipt_id


@pytest.mark.parametrize(
    ("field_name", "replacement_value", "code"),
    (
        ("catalog_version", "policy/v999", "policy_evaluator_catalog_unknown"),
        ("ruleset_version", "rules/v999", "policy_evaluator_ruleset_unknown"),
        ("evaluator_version", "evaluator/v999", "policy_evaluator_version_unknown"),
        ("binding_head_digest", "b" * 64, "policy_evaluator_binding_digest_mismatch"),
        ("policy_set_digest", "c" * 64, "policy_evaluator_policy_set_digest_mismatch"),
        ("input_digest", "d" * 64, "policy_evaluator_input_digest_mismatch"),
    ),
)
def test_unknown_versions_and_tampered_digests_fail_closed(
    field_name,
    replacement_value,
    code,
) -> None:
    evaluation_input, revisions = _single_policy((_rule(1),))
    tampered = replace(
        evaluation_input,
        **{field_name: replacement_value},
    )

    with pytest.raises(PolicyEvaluatorError) as raised:
        evaluate_policy(
            tampered,
            revisions=revisions,
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )
    assert raised.value.code == code


def test_exact_bound_revision_set_digest_and_version_are_required() -> None:
    evaluation_input, revisions = _single_policy((_rule(1),))
    revision = revisions[0]
    extra = _revision(2, (_rule(2),))

    for invalid_revisions in ((), (revision, extra)):
        with pytest.raises(
            PolicyEvaluatorError,
            match="policy_evaluator_revision_set_mismatch",
        ):
            evaluate_policy(
                evaluation_input,
                revisions=invalid_revisions,
                evaluated_at=EVALUATED_AT,
                evaluated_by="evaluator-1",
            )

    wrong_digest = replace(revision, content_digest="e" * 64)
    with pytest.raises(
        PolicyEvaluatorError,
        match="policy_evaluator_revision_digest_mismatch",
    ):
        evaluate_policy(
            evaluation_input,
            revisions=(wrong_digest,),
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )

    wrong_version = replace(revision, semantic_version="1.1.0")
    with pytest.raises(
        PolicyEvaluatorError,
        match="policy_evaluator_revision_version_mismatch",
    ):
        evaluate_policy(
            evaluation_input,
            revisions=(wrong_version,),
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )


def test_unknown_facts_and_operators_fail_closed() -> None:
    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_fact_unknown",
    ):
        evaluate_policy_predicate(
            _predicate("eq", "unknown_fact", value=True),
            target_entity_type=PolicyEntityType.SPEC,
            facts=(),
        )
    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_operator_unknown",
    ):
        evaluate_policy_predicate(
            _predicate("regex", "status", value="review"),
            target_entity_type=PolicyEntityType.SPEC,
            facts=(("status", "review"),),
        )


def test_spec_validation_state_remains_a_typed_server_owned_snapshot_value() -> None:
    result = evaluate_policy_predicate(
        _predicate(
            "eq",
            "validation_state",
            value="provider_specific_current",
        ),
        target_entity_type=PolicyEntityType.SPEC,
        facts=(("validation_state", "provider_specific_current"),),
    )

    assert result.matched is True


def test_non_applicable_rules_are_counted_without_findings() -> None:
    card_rule = _rule(
        1,
        targets=(PolicyEntityType.CARD,),
    )
    evaluation_input, revisions = _single_policy((card_rule,))

    output = evaluate_policy(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
    )

    assert output.rule_evaluations == ()
    assert output.counts.total_rule_count == 1
    assert output.counts.evaluated_rule_count == 0
    assert output.counts.not_applicable_rule_count == 1
    assert output.result.receipt.outcome is PolicyEvaluationOutcome.NOT_APPLICABLE
    assert output.result.receipt.state is PolicyComplianceState.NOT_APPLICABLE
    assert output.result.receipt.rule_count == 0
    assert output.result.receipt.reason_codes == (
        PolicyComplianceReasonCode.NO_APPLICABLE_RULES,
    )
    assert output.result.receipt.findings == ()


@pytest.mark.parametrize(
    ("enforcement", "expected_state", "expected_reason"),
    (
        (
            GuidelineEnforcement.BLOCKING,
            PolicyComplianceState.BLOCKED,
            PolicyComplianceReasonCode.POLICY_EVALUATION_UNAVAILABLE,
        ),
        (
            GuidelineEnforcement.ADVISORY,
            PolicyComplianceState.READY,
            PolicyComplianceReasonCode.POLICY_EVALUATION_DEGRADED,
        ),
    ),
)
def test_operational_error_factory_is_deterministic_and_honest(
    enforcement: GuidelineEnforcement,
    expected_state: PolicyComplianceState,
    expected_reason: PolicyComplianceReasonCode,
) -> None:
    evaluation_input, revisions = _single_policy((_rule(1, enforcement=enforcement),))

    first = build_policy_evaluation_error_result_v1(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
        operational_error_code=(
            PolicyOperationalErrorCode.PREDICATE_RUNTIME_UNAVAILABLE
        ),
    )
    second = build_policy_evaluation_error_result_v1(
        evaluation_input,
        revisions=revisions,
        evaluated_at=EVALUATED_AT,
        evaluated_by="evaluator-1",
        operational_error_code=(
            PolicyOperationalErrorCode.PREDICATE_RUNTIME_UNAVAILABLE
        ),
    )

    assert first == second
    assert first.counts.error_rule_count == 1
    assert first.counts.passed_rule_count == 0
    assert first.counts.failed_rule_count == 0
    assert len(first.rule_evaluations) == 1
    assert first.rule_evaluations[0].outcome is PolicyEvaluationOutcome.ERROR
    receipt = first.result.receipt
    assert receipt.state is expected_state
    assert receipt.outcome is PolicyEvaluationOutcome.ERROR
    assert receipt.reason_codes == (expected_reason,)
    assert receipt.error_rule_count == 1
    assert len(receipt.findings) == 1
    assert receipt.findings[0].outcome is PolicyEvaluationOutcome.ERROR

    with pytest.raises(
        PolicyEvaluatorError,
        match="policy_evaluator_input_digest_mismatch",
    ):
        build_policy_evaluation_error_result_v1(
            replace(evaluation_input, input_digest="f" * 64),
            revisions=revisions,
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
            operational_error_code=(
                PolicyOperationalErrorCode.PREDICATE_RUNTIME_UNAVAILABLE
            ),
        )

    for forbidden_code in (
        "guideline_policy_subject_conflict",
        "guideline_policy_cas_conflict",
        "policy_evaluator_input_digest_mismatch",
    ):
        with pytest.raises(
            PolicyEvaluatorError,
            match="policy_evaluator_operational_error_code_invalid",
        ):
            build_policy_evaluation_error_result_v1(
                evaluation_input,
                revisions=revisions,
                evaluated_at=EVALUATED_AT,
                evaluated_by="evaluator-1",
                operational_error_code=forbidden_code,  # type: ignore[arg-type]
            )


def test_digest_helpers_are_order_independent_and_version_fenced() -> None:
    first = _revision(1, (_rule(2), _rule(1)))
    second = _revision(2, (_rule(3),))
    first_binding = _binding(first, 1, priority=9)
    second_binding = _binding(second, 2, priority=1)
    snapshot = _snapshot(
        attributes=(
            ("labels", ("security", "api")),
            ("resource_gate_ready", True),
        )
    )

    assert policy_binding_head_digest_v1(
        (first_binding, second_binding)
    ) == policy_binding_head_digest_v1((second_binding, first_binding))
    assert policy_set_digest_v1(
        (first_binding, second_binding),
        (first, second),
    ) == policy_set_digest_v1(
        (second_binding, first_binding),
        (second, first),
    )
    priority_swapped = (
        replace(first_binding, priority=1),
        replace(second_binding, priority=9),
    )
    assert policy_set_digest_v1(
        (first_binding, second_binding),
        (first, second),
    ) == policy_set_digest_v1(priority_swapped, (first, second))
    assert policy_binding_head_digest_v1(
        (first_binding, second_binding)
    ) != policy_binding_head_digest_v1(priority_swapped)
    policy_digest = policy_set_digest_v1(
        (first_binding, second_binding),
        (first, second),
    )
    binding_digest = policy_binding_head_digest_v1((first_binding, second_binding))
    assert (
        len(
            policy_evaluation_input_digest_v1(
                subject_snapshot=snapshot,
                policy_set_digest=policy_digest,
                binding_head_digest=binding_digest,
            )
        )
        == 64
    )
    with pytest.raises(
        PolicyEvaluatorError,
        match="policy_evaluator_catalog_unknown",
    ):
        policy_evaluation_input_digest_v1(
            subject_snapshot=snapshot,
            policy_set_digest=policy_digest,
            binding_head_digest=binding_digest,
            catalog_version=f"{GUIDELINE_PREDICATE_CATALOG_VERSION}-unknown",
        )


def test_p95_for_two_hundred_rules_is_below_one_hundred_milliseconds() -> None:
    rules = tuple(_rule(index) for index in range(1, 201))
    evaluation_input, revisions = _single_policy((rules))

    for _ in range(3):
        output = evaluate_policy(
            evaluation_input,
            revisions=revisions,
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )
        assert output.counts.evaluated_rule_count == 200

    samples_ms: list[float] = []
    for _ in range(20):
        started = process_time_ns()
        output = evaluate_policy(
            evaluation_input,
            revisions=revisions,
            evaluated_at=EVALUATED_AT,
            evaluated_by="evaluator-1",
        )
        samples_ms.append((process_time_ns() - started) / 1_000_000)
        assert output.counts.evaluated_rule_count == 200

    p95_ms = sorted(samples_ms)[18]
    assert p95_ms <= 100, (
        f"policy-evaluator/v1 p95 was {p95_ms:.3f}ms for 200 rules; "
        f"samples={samples_ms!r}"
    )
