"""Pure deterministic evaluator for executable guideline policy.

``policy-evaluator/v1`` evaluates every applicable rule without short-circuit,
uses no clock, network, model, cache, or mutable global state, and emits stable
rule traces, findings, counts, IDs, and digests.  Callers must supply the
evaluation timestamp explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
from types import MappingProxyType
from typing import Final, Mapping

from okto_pulse.core.domain.guideline_policy import (
    AdoptedGuidelineRevisionRef,
    BoardGuidelineBinding,
    GuidelineEnforcement,
    GuidelinePredicate,
    GuidelineRevision,
    GuidelineRule,
    GuidelineRuleOperator,
    PolicyComplianceFinding,
    PolicyComplianceReasonCode,
    PolicyComplianceReceipt,
    PolicyComplianceRuleResult,
    PolicyComplianceState,
    PolicyCurrentness,
    PolicyEntityType,
    PolicyEvaluationInput,
    PolicyEvaluationOutcome,
    PolicyEvaluationResult,
    PolicyParameterValue,
    PolicySubjectSnapshot,
    PolicyWaiver,
    PolicyWaiverAuthorization,
)
from okto_pulse.core.domain.guideline_predicate_catalog import (
    GUIDELINE_PREDICATE_CATALOG_DIGEST,
    GUIDELINE_PREDICATE_CATALOG_VERSION,
    GuidelinePredicateCatalogError,
    PolicyOperatorFamily,
    PolicyPredicateOperator,
    require_policy_fact,
    validate_guideline_predicate,
    validate_policy_fact_value,
)
from okto_pulse.core.domain.quality_canonicalization import (
    canonical_json_bytes,
    canonical_sha256,
)


POLICY_EVALUATOR_VERSION: Final = "policy-evaluator/v1"
POLICY_RULESET_VERSION: Final = "policy-ruleset/v1"


class PolicyEvaluatorError(ValueError):
    """An input violates the deterministic policy-evaluator/v1 contract."""

    def __init__(
        self,
        code: str,
        *,
        subject_id: str | None = None,
        guideline_id: str | None = None,
        revision_id: str | None = None,
        rule_id: str | None = None,
    ) -> None:
        self.code = code
        self.subject_id = subject_id
        self.guideline_id = guideline_id
        self.revision_id = revision_id
        self.rule_id = rule_id
        super().__init__(code)


class PolicyOperationalErrorCode(StrEnum):
    """Closed operational failures eligible for honest ERROR evidence."""

    FACT_SNAPSHOT_UNAVAILABLE = "fact_snapshot_unavailable"
    PREDICATE_RUNTIME_UNAVAILABLE = "predicate_runtime_unavailable"
    EVALUATOR_RUNTIME_UNAVAILABLE = "evaluator_runtime_unavailable"


@dataclass(frozen=True, slots=True)
class PolicyPredicateEvaluation:
    predicate_digest: str
    fact_code: str
    operator: PolicyPredicateOperator
    fact_present: bool
    matched: bool


@dataclass(frozen=True, slots=True)
class PolicyRuleEvaluation:
    binding_id: str
    guideline_id: str
    revision_id: str
    rule_id: str
    rule_code: str
    enforcement: GuidelineEnforcement
    outcome: PolicyEvaluationOutcome
    predicate_results: tuple[PolicyPredicateEvaluation, ...]
    waiver_id: str | None = None

    @property
    def failed(self) -> bool:
        return self.outcome is PolicyEvaluationOutcome.FAIL

    @property
    def errored(self) -> bool:
        return self.outcome is PolicyEvaluationOutcome.ERROR

    @property
    def blocking(self) -> bool:
        return (
            (self.failed or self.errored)
            and self.enforcement is GuidelineEnforcement.BLOCKING
            and self.waiver_id is None
        )


@dataclass(frozen=True, slots=True)
class PolicyEvaluationCounts:
    total_rule_count: int
    evaluated_rule_count: int
    not_applicable_rule_count: int
    passed_rule_count: int
    failed_rule_count: int
    advisory_failure_count: int
    blocking_failure_count: int
    waived_failure_count: int
    unwaived_blocking_failure_count: int
    error_rule_count: int = 0
    advisory_error_count: int = 0
    blocking_error_count: int = 0

    def __post_init__(self) -> None:
        for field_name in (
            "total_rule_count",
            "evaluated_rule_count",
            "not_applicable_rule_count",
            "passed_rule_count",
            "failed_rule_count",
            "advisory_failure_count",
            "blocking_failure_count",
            "waived_failure_count",
            "unwaived_blocking_failure_count",
            "error_rule_count",
            "advisory_error_count",
            "blocking_error_count",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise PolicyEvaluatorError("policy_evaluator_counts_invalid")
        if (
            self.total_rule_count
            != self.evaluated_rule_count + self.not_applicable_rule_count
            or self.evaluated_rule_count
            != (self.passed_rule_count + self.failed_rule_count + self.error_rule_count)
            or self.failed_rule_count
            != self.advisory_failure_count + self.blocking_failure_count
            or self.error_rule_count
            != self.advisory_error_count + self.blocking_error_count
            or self.unwaived_blocking_failure_count > self.blocking_failure_count
            or self.waived_failure_count > self.failed_rule_count
        ):
            raise PolicyEvaluatorError("policy_evaluator_counts_inconsistent")


@dataclass(frozen=True, slots=True)
class PolicyEvaluatorOutput:
    result: PolicyEvaluationResult
    evaluation_digest: str
    counts: PolicyEvaluationCounts
    rule_evaluations: tuple[PolicyRuleEvaluation, ...]


@dataclass(frozen=True, slots=True)
class _PreparedPredicate:
    predicate: GuidelinePredicate
    manifest: Mapping[str, object]
    canonical_bytes: bytes
    digest: str


@dataclass(frozen=True, slots=True)
class _PreparedRule:
    rule: GuidelineRule
    predicates: tuple[_PreparedPredicate, ...]
    manifest: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _PreparedRevision:
    revision: GuidelineRevision
    rules: tuple[_PreparedRule, ...]
    manifest: Mapping[str, object]


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise PolicyEvaluatorError(code)
    return value.astimezone(timezone.utc)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PolicyEvaluatorError(code)
    return value.strip()


def _binding_manifest(binding: BoardGuidelineBinding) -> Mapping[str, object]:
    return MappingProxyType(
        {
            "binding_id": binding.binding_id,
            "binding_revision": binding.binding_revision,
            "board_id": binding.board_id,
            "guideline_id": binding.guideline_id,
            "revision_id": binding.revision_id,
            "semantic_version": binding.semantic_version,
            "revision_digest": binding.revision_digest,
            "priority": binding.priority,
            "default_enforcement": binding.default_enforcement.value,
        }
    )


def _predicate_manifest(predicate: GuidelinePredicate) -> Mapping[str, object]:
    return MappingProxyType(
        {
            "predicate_code": predicate.predicate_code,
            "parameters": predicate.parameters,
        }
    )


def _prepare_predicate(
    predicate: GuidelinePredicate,
    *,
    target_entity_types: tuple[PolicyEntityType, ...],
) -> _PreparedPredicate:
    canonical = validate_guideline_predicate(
        predicate,
        target_entity_types=target_entity_types,
    )
    manifest = _predicate_manifest(canonical)
    canonical_bytes = canonical_json_bytes(manifest)
    return _PreparedPredicate(
        predicate=canonical,
        manifest=manifest,
        canonical_bytes=canonical_bytes,
        digest=sha256(canonical_bytes).hexdigest(),
    )


def _prepare_rule(rule: GuidelineRule) -> _PreparedRule:
    prepared_predicates = tuple(
        _prepare_predicate(
            predicate,
            target_entity_types=rule.target_entity_types,
        )
        for predicate in rule.predicates
    )
    if len({item.canonical_bytes for item in prepared_predicates}) != len(
        prepared_predicates
    ):
        raise GuidelinePredicateCatalogError("policy_predicate_duplicate")
    prepared_predicates = tuple(
        sorted(
            prepared_predicates,
            key=lambda item: item.canonical_bytes,
        )
    )
    canonical_rule = replace(
        rule,
        predicates=tuple(item.predicate for item in prepared_predicates),
    )
    manifest = MappingProxyType(
        {
            "rule_id": canonical_rule.rule_id,
            "code": canonical_rule.code,
            "title": canonical_rule.title,
            "description": canonical_rule.description,
            "target_entity_types": tuple(
                target.value for target in canonical_rule.target_entity_types
            ),
            "enforcement": canonical_rule.enforcement.value,
            "operator": canonical_rule.operator.value,
            "waivable": canonical_rule.waivable,
            "policy_class": canonical_rule.policy_class,
            "predicates": tuple(item.manifest for item in prepared_predicates),
        }
    )
    return _PreparedRule(
        rule=canonical_rule,
        predicates=prepared_predicates,
        manifest=manifest,
    )


def _prepare_revision(revision: GuidelineRevision) -> _PreparedRevision:
    if not isinstance(revision, GuidelineRevision):
        raise PolicyEvaluatorError("policy_evaluator_revisions_invalid")
    rules = tuple(
        sorted(
            (_prepare_rule(rule) for rule in revision.rules),
            key=lambda item: item.rule.code,
        )
    )
    canonical_revision = replace(
        revision,
        rules=tuple(item.rule for item in rules),
    )
    manifest = MappingProxyType(
        {
            "revision_id": canonical_revision.revision_id,
            "guideline_id": canonical_revision.guideline_id,
            "revision_number": canonical_revision.revision_number,
            "semantic_version": canonical_revision.semantic_version,
            "content_digest": canonical_revision.content_digest,
            "rules": tuple(item.manifest for item in rules),
        }
    )
    return _PreparedRevision(
        revision=canonical_revision,
        rules=rules,
        manifest=manifest,
    )


def _required_bindings(
    value: object,
) -> tuple[BoardGuidelineBinding, ...]:
    if not isinstance(value, tuple | list) or any(
        not isinstance(binding, BoardGuidelineBinding) for binding in value
    ):
        raise PolicyEvaluatorError("policy_evaluator_bindings_invalid")
    bindings = tuple(value)
    if len({binding.binding_id for binding in bindings}) != len(bindings) or len(
        {binding.guideline_id for binding in bindings}
    ) != len(bindings):
        raise PolicyEvaluatorError("policy_evaluator_bindings_duplicate")
    return tuple(
        sorted(
            bindings,
            key=lambda binding: (
                binding.priority,
                binding.binding_id,
            ),
        )
    )


def policy_binding_head_digest_v1(
    bindings: tuple[BoardGuidelineBinding, ...],
) -> str:
    """Digest the exact adopted board binding heads."""

    canonical_bindings = _required_bindings(bindings)
    return canonical_sha256(
        {
            "contract": "policy-binding-head/v1",
            "bindings": tuple(
                _binding_manifest(binding) for binding in canonical_bindings
            ),
        }
    )


def _resolve_bound_revisions(
    bindings: tuple[BoardGuidelineBinding, ...],
    revisions: object,
) -> tuple[tuple[BoardGuidelineBinding, _PreparedRevision], ...]:
    if not isinstance(revisions, tuple | list) or any(
        not isinstance(revision, GuidelineRevision) for revision in revisions
    ):
        raise PolicyEvaluatorError("policy_evaluator_revisions_invalid")
    revisions_tuple = tuple(revisions)
    identities = tuple(
        (revision.guideline_id, revision.revision_id) for revision in revisions_tuple
    )
    if len(set(identities)) != len(identities):
        raise PolicyEvaluatorError("policy_evaluator_revisions_duplicate")
    revision_by_identity = {
        identity: _prepare_revision(revision)
        for identity, revision in zip(
            identities,
            revisions_tuple,
            strict=True,
        )
    }
    expected_identities = {
        (binding.guideline_id, binding.revision_id) for binding in bindings
    }
    if set(revision_by_identity) != expected_identities:
        raise PolicyEvaluatorError("policy_evaluator_revision_set_mismatch")
    resolved: list[tuple[BoardGuidelineBinding, _PreparedRevision]] = []
    for binding in bindings:
        prepared = revision_by_identity[(binding.guideline_id, binding.revision_id)]
        revision = prepared.revision
        if binding.revision_digest != revision.content_digest:
            raise PolicyEvaluatorError(
                "policy_evaluator_revision_digest_mismatch",
                guideline_id=binding.guideline_id,
                revision_id=binding.revision_id,
            )
        if binding.semantic_version != revision.semantic_version:
            raise PolicyEvaluatorError(
                "policy_evaluator_revision_version_mismatch",
                guideline_id=binding.guideline_id,
                revision_id=binding.revision_id,
            )
        resolved.append((binding, prepared))
    return tuple(resolved)


def policy_set_digest_v1(
    bindings: tuple[BoardGuidelineBinding, ...],
    revisions: tuple[GuidelineRevision, ...],
) -> str:
    """Digest exact adopted revisions and their canonical executable rules."""

    canonical_bindings = _required_bindings(bindings)
    resolved = _resolve_bound_revisions(canonical_bindings, revisions)
    return _policy_set_digest_from_resolved(resolved)


def _policy_set_digest_from_resolved(
    resolved: tuple[
        tuple[BoardGuidelineBinding, _PreparedRevision],
        ...,
    ],
) -> str:
    semantic_order = sorted(
        resolved,
        key=lambda item: (
            item[0].guideline_id,
            item[1].revision.revision_id,
            item[1].revision.content_digest,
        ),
    )
    return canonical_sha256(
        {
            "contract": "policy-set/v1",
            "adopted": tuple(
                {
                    "guideline_id": binding.guideline_id,
                    "revision": prepared.manifest,
                }
                for binding, prepared in semantic_order
            ),
        }
    )


def _canonical_subject_facts(
    snapshot: PolicySubjectSnapshot,
) -> tuple[tuple[str, PolicyParameterValue], ...]:
    if not isinstance(snapshot, PolicySubjectSnapshot):
        raise PolicyEvaluatorError("policy_evaluator_subject_snapshot_invalid")
    facts: list[tuple[str, PolicyParameterValue]] = []
    for fact_code, raw_value in snapshot.attributes:
        definition = require_policy_fact(
            snapshot.subject.entity_type,
            fact_code,
        )
        facts.append(
            (
                fact_code,
                validate_policy_fact_value(definition, raw_value),
            )
        )
    return tuple(sorted(facts, key=lambda item: item[0]))


def policy_evaluation_input_digest_v1(
    *,
    subject_snapshot: PolicySubjectSnapshot,
    policy_set_digest: str,
    binding_head_digest: str,
    catalog_version: str = GUIDELINE_PREDICATE_CATALOG_VERSION,
    ruleset_version: str = POLICY_RULESET_VERSION,
    evaluator_version: str = POLICY_EVALUATOR_VERSION,
) -> str:
    """Compute the content identity consumed by policy-evaluator/v1."""

    if catalog_version != GUIDELINE_PREDICATE_CATALOG_VERSION:
        raise PolicyEvaluatorError("policy_evaluator_catalog_unknown")
    if ruleset_version != POLICY_RULESET_VERSION:
        raise PolicyEvaluatorError("policy_evaluator_ruleset_unknown")
    if evaluator_version != POLICY_EVALUATOR_VERSION:
        raise PolicyEvaluatorError("policy_evaluator_version_unknown")
    facts = _canonical_subject_facts(subject_snapshot)
    return canonical_sha256(
        {
            "contract": "policy-evaluation-input/v1",
            "subject": {
                "board_id": subject_snapshot.subject.board_id,
                "entity_type": subject_snapshot.subject.entity_type.value,
                "subject_id": subject_snapshot.subject.subject_id,
                "subject_version": subject_snapshot.subject.subject_version,
                "content_digest": subject_snapshot.content_digest,
                "facts": facts,
            },
            "policy_set_digest": policy_set_digest,
            "binding_head_digest": binding_head_digest,
            "catalog_version": catalog_version,
            "catalog_digest": GUIDELINE_PREDICATE_CATALOG_DIGEST,
            "ruleset_version": ruleset_version,
            "evaluator_version": evaluator_version,
        }
    )


def build_policy_evaluation_input_v1(
    *,
    evaluation_id: str,
    subject_snapshot: PolicySubjectSnapshot,
    bindings: tuple[BoardGuidelineBinding, ...],
    revisions: tuple[GuidelineRevision, ...],
    requested_by: str,
    requested_at: datetime,
    idempotency_key: str,
) -> PolicyEvaluationInput:
    """Build a correctly fenced B01 input for this evaluator."""

    canonical_bindings = _required_bindings(bindings)
    binding_head_digest = policy_binding_head_digest_v1(canonical_bindings)
    policy_set_digest = policy_set_digest_v1(
        canonical_bindings,
        revisions,
    )
    input_digest = policy_evaluation_input_digest_v1(
        subject_snapshot=subject_snapshot,
        policy_set_digest=policy_set_digest,
        binding_head_digest=binding_head_digest,
    )
    return PolicyEvaluationInput(
        evaluation_id=evaluation_id,
        subject_snapshot=subject_snapshot,
        bindings=canonical_bindings,
        input_digest=input_digest,
        policy_set_digest=policy_set_digest,
        binding_head_digest=binding_head_digest,
        catalog_version=GUIDELINE_PREDICATE_CATALOG_VERSION,
        ruleset_version=POLICY_RULESET_VERSION,
        evaluator_version=POLICY_EVALUATOR_VERSION,
        requested_by=requested_by,
        requested_at=requested_at,
        idempotency_key=idempotency_key,
    )


_OPERATOR_FAMILY: Final[Mapping[PolicyPredicateOperator, PolicyOperatorFamily]] = (
    MappingProxyType(
        {
            PolicyPredicateOperator.EXISTS: PolicyOperatorFamily.PRESENCE,
            PolicyPredicateOperator.NOT_EXISTS: PolicyOperatorFamily.PRESENCE,
            PolicyPredicateOperator.EQ: PolicyOperatorFamily.EQUALITY,
            PolicyPredicateOperator.NE: PolicyOperatorFamily.EQUALITY,
            PolicyPredicateOperator.IN: PolicyOperatorFamily.MEMBERSHIP,
            PolicyPredicateOperator.NOT_IN: PolicyOperatorFamily.MEMBERSHIP,
            PolicyPredicateOperator.GT: PolicyOperatorFamily.NUMERIC_COMPARISON,
            PolicyPredicateOperator.GTE: PolicyOperatorFamily.NUMERIC_COMPARISON,
            PolicyPredicateOperator.LT: PolicyOperatorFamily.NUMERIC_COMPARISON,
            PolicyPredicateOperator.LTE: PolicyOperatorFamily.NUMERIC_COMPARISON,
            PolicyPredicateOperator.COUNT_EQ: PolicyOperatorFamily.COUNT,
            PolicyPredicateOperator.COUNT_NE: PolicyOperatorFamily.COUNT,
            PolicyPredicateOperator.COUNT_GT: PolicyOperatorFamily.COUNT,
            PolicyPredicateOperator.COUNT_GTE: PolicyOperatorFamily.COUNT,
            PolicyPredicateOperator.COUNT_LT: PolicyOperatorFamily.COUNT,
            PolicyPredicateOperator.COUNT_LTE: PolicyOperatorFamily.COUNT,
            PolicyPredicateOperator.CONTAINS: PolicyOperatorFamily.CONTAINS,
            PolicyPredicateOperator.NOT_CONTAINS: PolicyOperatorFamily.CONTAINS,
        }
    )
)


def _compare(
    operator: PolicyPredicateOperator,
    actual: object,
    expected: object,
) -> bool:
    if operator is PolicyPredicateOperator.EQ:
        return actual == expected
    if operator is PolicyPredicateOperator.NE:
        return actual != expected
    if operator is PolicyPredicateOperator.IN:
        return actual in expected
    if operator is PolicyPredicateOperator.NOT_IN:
        return actual not in expected
    if operator is PolicyPredicateOperator.GT:
        return actual > expected
    if operator is PolicyPredicateOperator.GTE:
        return actual >= expected
    if operator is PolicyPredicateOperator.LT:
        return actual < expected
    if operator is PolicyPredicateOperator.LTE:
        return actual <= expected
    if operator is PolicyPredicateOperator.COUNT_EQ:
        return len(actual) == expected
    if operator is PolicyPredicateOperator.COUNT_NE:
        return len(actual) != expected
    if operator is PolicyPredicateOperator.COUNT_GT:
        return len(actual) > expected
    if operator is PolicyPredicateOperator.COUNT_GTE:
        return len(actual) >= expected
    if operator is PolicyPredicateOperator.COUNT_LT:
        return len(actual) < expected
    if operator is PolicyPredicateOperator.COUNT_LTE:
        return len(actual) <= expected
    if operator is PolicyPredicateOperator.CONTAINS:
        return expected in actual
    if operator is PolicyPredicateOperator.NOT_CONTAINS:
        return expected not in actual
    raise PolicyEvaluatorError("policy_evaluator_operator_unknown")


def _evaluate_prepared_predicate(
    prepared: _PreparedPredicate,
    facts: Mapping[str, PolicyParameterValue],
) -> PolicyPredicateEvaluation:
    predicate = prepared.predicate
    parameters = dict(predicate.parameters)
    fact_code = parameters["fact"]
    if not isinstance(fact_code, str):  # catalog validation guarantees this
        raise PolicyEvaluatorError("policy_evaluator_fact_invalid")
    operator = PolicyPredicateOperator(predicate.predicate_code)
    present = fact_code in facts
    if operator is PolicyPredicateOperator.EXISTS:
        matched = present
    elif operator is PolicyPredicateOperator.NOT_EXISTS:
        matched = not present
    elif not present:
        matched = False
    else:
        family = _OPERATOR_FAMILY[operator]
        parameter_name = (
            "values" if family is PolicyOperatorFamily.MEMBERSHIP else "value"
        )
        matched = _compare(
            operator,
            facts[fact_code],
            parameters[parameter_name],
        )
    return PolicyPredicateEvaluation(
        predicate_digest=prepared.digest,
        fact_code=fact_code,
        operator=operator,
        fact_present=present,
        matched=matched,
    )


def evaluate_policy_predicate(
    predicate: GuidelinePredicate,
    *,
    target_entity_type: PolicyEntityType,
    facts: tuple[tuple[str, PolicyParameterValue], ...],
) -> PolicyPredicateEvaluation:
    """Evaluate one policy/v1 predicate against typed server-owned facts."""

    if not isinstance(target_entity_type, PolicyEntityType):
        raise PolicyEvaluatorError("policy_evaluator_target_invalid")
    if not isinstance(facts, tuple | list):
        raise PolicyEvaluatorError("policy_evaluator_facts_invalid")
    fact_map: dict[str, PolicyParameterValue] = {}
    for fact_code, raw_value in facts:
        if fact_code in fact_map:
            raise PolicyEvaluatorError("policy_evaluator_fact_duplicate")
        definition = require_policy_fact(target_entity_type, fact_code)
        fact_map[fact_code] = validate_policy_fact_value(
            definition,
            raw_value,
        )
    prepared = _prepare_predicate(
        predicate,
        target_entity_types=(target_entity_type,),
    )
    return _evaluate_prepared_predicate(
        prepared,
        MappingProxyType(fact_map),
    )


def _required_waivers(
    value: object,
) -> tuple[PolicyWaiverAuthorization, ...]:
    if not isinstance(value, tuple | list) or any(
        not isinstance(authorization, PolicyWaiverAuthorization)
        for authorization in value
    ):
        raise PolicyEvaluatorError("policy_evaluator_waivers_invalid")
    authorizations = tuple(value)
    if len({authorization.waiver.waiver_id for authorization in authorizations}) != len(
        authorizations
    ):
        raise PolicyEvaluatorError("policy_evaluator_waivers_duplicate")
    return tuple(
        sorted(
            authorizations,
            key=lambda authorization: (
                authorization.waiver.guideline_id,
                authorization.waiver.revision_id,
                authorization.waiver.rule_id,
                authorization.waiver.waiver_id,
            ),
        )
    )


def _effective_waiver(
    *,
    waivers: tuple[PolicyWaiverAuthorization, ...],
    evaluation_input: PolicyEvaluationInput,
    guideline_id: str,
    revision_id: str,
    rule: GuidelineRule,
    evaluated_at: datetime,
) -> PolicyWaiver | None:
    scoped = tuple(
        authorization
        for authorization in waivers
        if authorization.waiver.subject == evaluation_input.subject_snapshot.subject
        and authorization.waiver.guideline_id == guideline_id
        and authorization.waiver.revision_id == revision_id
        and authorization.waiver.rule_id == rule.rule_id
    )
    if any(
        authorization.resolved_at != evaluated_at
        or not authorization.matches(evaluation_input)
        for authorization in scoped
    ):
        raise PolicyEvaluatorError(
            "policy_evaluator_waiver_authorization_stale",
            guideline_id=guideline_id,
            revision_id=revision_id,
            rule_id=rule.rule_id,
        )
    effective = tuple(
        authorization.waiver
        for authorization in scoped
        if authorization.waiver.is_effective_at(evaluated_at)
    )
    if effective and not rule.waivable:
        raise PolicyEvaluatorError(
            "policy_evaluator_non_waivable_rule",
            guideline_id=guideline_id,
            revision_id=revision_id,
            rule_id=rule.rule_id,
        )
    if len(effective) > 1:
        raise PolicyEvaluatorError(
            "policy_evaluator_multiple_effective_waivers",
            guideline_id=guideline_id,
            revision_id=revision_id,
            rule_id=rule.rule_id,
        )
    return effective[0] if effective else None


def _rule_evaluation_manifest(
    evaluation: PolicyRuleEvaluation,
) -> Mapping[str, object]:
    return MappingProxyType(
        {
            "binding_id": evaluation.binding_id,
            "guideline_id": evaluation.guideline_id,
            "revision_id": evaluation.revision_id,
            "rule_id": evaluation.rule_id,
            "rule_code": evaluation.rule_code,
            "enforcement": evaluation.enforcement.value,
            "outcome": evaluation.outcome.value,
            "waiver_id": evaluation.waiver_id,
            "predicates": tuple(
                {
                    "predicate_digest": result.predicate_digest,
                    "fact_code": result.fact_code,
                    "operator": result.operator.value,
                    "fact_present": result.fact_present,
                    "matched": result.matched,
                }
                for result in evaluation.predicate_results
            ),
        }
    )


def _evaluation_counts(
    *,
    total_rule_count: int,
    evaluations: tuple[PolicyRuleEvaluation, ...],
) -> PolicyEvaluationCounts:
    passed = sum(
        evaluation.outcome is PolicyEvaluationOutcome.PASS for evaluation in evaluations
    )
    failed = sum(evaluation.failed for evaluation in evaluations)
    errors = sum(evaluation.errored for evaluation in evaluations)
    advisory = sum(
        evaluation.failed and evaluation.enforcement is GuidelineEnforcement.ADVISORY
        for evaluation in evaluations
    )
    blocking = failed - advisory
    advisory_errors = sum(
        evaluation.errored and evaluation.enforcement is GuidelineEnforcement.ADVISORY
        for evaluation in evaluations
    )
    blocking_errors = errors - advisory_errors
    waived = sum(evaluation.waiver_id is not None for evaluation in evaluations)
    unwaived_blocking = sum(
        evaluation.failed
        and evaluation.enforcement is GuidelineEnforcement.BLOCKING
        and evaluation.waiver_id is None
        for evaluation in evaluations
    )
    return PolicyEvaluationCounts(
        total_rule_count=total_rule_count,
        evaluated_rule_count=len(evaluations),
        not_applicable_rule_count=total_rule_count - len(evaluations),
        passed_rule_count=passed,
        failed_rule_count=failed,
        advisory_failure_count=advisory,
        blocking_failure_count=blocking,
        waived_failure_count=waived,
        unwaived_blocking_failure_count=unwaived_blocking,
        error_rule_count=errors,
        advisory_error_count=advisory_errors,
        blocking_error_count=blocking_errors,
    )


def _validated_policy_bundle(
    evaluation_input: PolicyEvaluationInput,
    revisions: tuple[GuidelineRevision, ...],
) -> tuple[
    tuple[BoardGuidelineBinding, ...],
    tuple[tuple[BoardGuidelineBinding, _PreparedRevision], ...],
]:
    """Validate immutable bundle/fences before any outcome is materialized."""

    if not isinstance(evaluation_input, PolicyEvaluationInput):
        raise PolicyEvaluatorError("policy_evaluator_input_invalid")
    if evaluation_input.catalog_version != GUIDELINE_PREDICATE_CATALOG_VERSION:
        raise PolicyEvaluatorError("policy_evaluator_catalog_unknown")
    if evaluation_input.ruleset_version != POLICY_RULESET_VERSION:
        raise PolicyEvaluatorError("policy_evaluator_ruleset_unknown")
    if evaluation_input.evaluator_version != POLICY_EVALUATOR_VERSION:
        raise PolicyEvaluatorError("policy_evaluator_version_unknown")
    bindings = _required_bindings(evaluation_input.bindings)
    resolved = _resolve_bound_revisions(bindings, revisions)
    expected_binding_digest = policy_binding_head_digest_v1(bindings)
    if evaluation_input.binding_head_digest != expected_binding_digest:
        raise PolicyEvaluatorError("policy_evaluator_binding_digest_mismatch")
    expected_policy_set_digest = _policy_set_digest_from_resolved(resolved)
    if evaluation_input.policy_set_digest != expected_policy_set_digest:
        raise PolicyEvaluatorError("policy_evaluator_policy_set_digest_mismatch")
    expected_input_digest = policy_evaluation_input_digest_v1(
        subject_snapshot=evaluation_input.subject_snapshot,
        policy_set_digest=expected_policy_set_digest,
        binding_head_digest=expected_binding_digest,
        catalog_version=evaluation_input.catalog_version,
        ruleset_version=evaluation_input.ruleset_version,
        evaluator_version=evaluation_input.evaluator_version,
    )
    if evaluation_input.input_digest != expected_input_digest:
        raise PolicyEvaluatorError("policy_evaluator_input_digest_mismatch")
    return bindings, resolved


def _applicable_prepared_rules(
    resolved: tuple[
        tuple[BoardGuidelineBinding, _PreparedRevision],
        ...,
    ],
    subject_type: PolicyEntityType,
) -> tuple[tuple[BoardGuidelineBinding, _PreparedRevision, _PreparedRule], ...]:
    applicable = [
        (binding, prepared_revision, prepared_rule)
        for binding, prepared_revision in resolved
        for prepared_rule in prepared_revision.rules
        if prepared_rule.rule.applies_to(subject_type)
    ]
    applicable.sort(
        key=lambda item: (
            item[0].priority,
            item[0].binding_id,
            item[2].rule.code,
            item[2].rule.rule_id,
        )
    )
    return tuple(applicable)


def evaluate_policy(
    evaluation_input: PolicyEvaluationInput,
    *,
    revisions: tuple[GuidelineRevision, ...],
    waivers: tuple[PolicyWaiverAuthorization, ...] = (),
    evaluated_at: datetime,
    evaluated_by: str,
) -> PolicyEvaluatorOutput:
    """Evaluate every applicable adopted rule and return a deterministic receipt."""

    if not isinstance(evaluation_input, PolicyEvaluationInput):
        raise PolicyEvaluatorError("policy_evaluator_input_invalid")
    evaluated_at = _aware_utc(
        evaluated_at,
        "policy_evaluator_evaluated_at_invalid",
    )
    evaluated_by = _required_text(
        evaluated_by,
        "policy_evaluator_evaluated_by_required",
    )
    if evaluation_input.requested_at > evaluated_at:
        raise PolicyEvaluatorError("policy_evaluator_requested_at_future")
    bindings, resolved = _validated_policy_bundle(evaluation_input, revisions)

    facts = MappingProxyType(
        dict(_canonical_subject_facts(evaluation_input.subject_snapshot))
    )
    canonical_waivers = _required_waivers(waivers)
    subject_type = evaluation_input.subject_snapshot.subject.entity_type
    total_rule_count = sum(len(prepared.rules) for _, prepared in resolved)
    applicable = _applicable_prepared_rules(resolved, subject_type)

    rule_evaluations: list[PolicyRuleEvaluation] = []
    for binding, prepared_revision, prepared_rule in applicable:
        revision = prepared_revision.revision
        canonical_rule = prepared_rule.rule
        # Materialize every result before ALL/ANY reduction: no short-circuit.
        predicate_results = tuple(
            _evaluate_prepared_predicate(predicate, facts)
            for predicate in prepared_rule.predicates
        )
        if canonical_rule.operator is GuidelineRuleOperator.ALL:
            matched = all(result.matched for result in predicate_results)
        else:
            matched = any(result.matched for result in predicate_results)
        outcome = (
            PolicyEvaluationOutcome.PASS if matched else PolicyEvaluationOutcome.FAIL
        )
        waiver = (
            None
            if matched
            else _effective_waiver(
                waivers=canonical_waivers,
                evaluation_input=evaluation_input,
                guideline_id=revision.guideline_id,
                revision_id=revision.revision_id,
                rule=canonical_rule,
                evaluated_at=evaluated_at,
            )
        )
        rule_evaluations.append(
            PolicyRuleEvaluation(
                binding_id=binding.binding_id,
                guideline_id=revision.guideline_id,
                revision_id=revision.revision_id,
                rule_id=canonical_rule.rule_id,
                rule_code=canonical_rule.code,
                enforcement=canonical_rule.enforcement,
                outcome=outcome,
                predicate_results=predicate_results,
                waiver_id=waiver.waiver_id if waiver is not None else None,
            )
        )
    evaluations = tuple(rule_evaluations)
    counts = _evaluation_counts(
        total_rule_count=total_rule_count,
        evaluations=evaluations,
    )
    evaluation_digest = canonical_sha256(
        {
            "contract": POLICY_EVALUATOR_VERSION,
            "evaluation_id": evaluation_input.evaluation_id,
            "input_digest": evaluation_input.input_digest,
            "catalog_digest": GUIDELINE_PREDICATE_CATALOG_DIGEST,
            "evaluated_at": evaluated_at.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "evaluated_by": evaluated_by,
            "rules": tuple(
                _rule_evaluation_manifest(evaluation) for evaluation in evaluations
            ),
            "counts": {
                field_name: getattr(counts, field_name)
                for field_name in counts.__dataclass_fields__
            },
        }
    )
    receipt_id = f"pcr_{evaluation_digest[:32]}"
    findings: list[PolicyComplianceFinding] = []
    for evaluation in evaluations:
        if not evaluation.failed:
            continue
        finding_digest = canonical_sha256(
            {
                "contract": "policy-compliance-finding/v1",
                "receipt_id": receipt_id,
                "guideline_id": evaluation.guideline_id,
                "revision_id": evaluation.revision_id,
                "rule_id": evaluation.rule_id,
            }
        )
        findings.append(
            PolicyComplianceFinding(
                finding_id=f"pcf_{finding_digest[:32]}",
                receipt_id=receipt_id,
                subject=evaluation_input.subject_snapshot.subject,
                guideline_id=evaluation.guideline_id,
                revision_id=evaluation.revision_id,
                rule_id=evaluation.rule_id,
                outcome=PolicyEvaluationOutcome.FAIL,
                enforcement=evaluation.enforcement,
                message=f"Policy rule {evaluation.rule_code} failed.",
                created_at=evaluated_at,
                evidence_refs=tuple(
                    (
                        f"predicate:{predicate.predicate_digest}:"
                        f"{'pass' if predicate.matched else 'fail'}"
                    )
                    for predicate in evaluation.predicate_results
                ),
                waiver_id=evaluation.waiver_id,
            )
        )
    if not evaluations:
        receipt_outcome = PolicyEvaluationOutcome.NOT_APPLICABLE
        state = PolicyComplianceState.NOT_APPLICABLE
    else:
        receipt_outcome = (
            PolicyEvaluationOutcome.FAIL
            if counts.failed_rule_count
            else PolicyEvaluationOutcome.PASS
        )
        if counts.unwaived_blocking_failure_count:
            state = PolicyComplianceState.BLOCKED
        elif counts.waived_failure_count:
            state = PolicyComplianceState.READY_WITH_WAIVERS
        else:
            state = PolicyComplianceState.READY
    receipt = PolicyComplianceReceipt(
        receipt_id=receipt_id,
        subject=evaluation_input.subject_snapshot.subject,
        subject_content_digest=evaluation_input.subject_snapshot.content_digest,
        input_digest=evaluation_input.input_digest,
        policy_set_digest=evaluation_input.policy_set_digest,
        binding_head_digest=evaluation_input.binding_head_digest,
        catalog_version=evaluation_input.catalog_version,
        ruleset_version=evaluation_input.ruleset_version,
        adopted_revisions=tuple(
            AdoptedGuidelineRevisionRef.from_binding(binding) for binding in bindings
        ),
        outcome=receipt_outcome,
        state=state,
        currentness=PolicyCurrentness.CURRENT,
        findings=tuple(findings),
        evaluator_version=POLICY_EVALUATOR_VERSION,
        evaluated_by=evaluated_by,
        evaluated_at=evaluated_at,
        rule_results=tuple(
            PolicyComplianceRuleResult(
                guideline_id=evaluation.guideline_id,
                revision_id=evaluation.revision_id,
                rule_id=evaluation.rule_id,
                outcome=evaluation.outcome,
                enforcement=evaluation.enforcement,
                waiver_id=evaluation.waiver_id,
            )
            for evaluation in evaluations
        ),
        reason_codes=(
            (PolicyComplianceReasonCode.NO_APPLICABLE_RULES,) if not evaluations else ()
        ),
    )
    return PolicyEvaluatorOutput(
        result=PolicyEvaluationResult(
            evaluation_id=evaluation_input.evaluation_id,
            input_digest=evaluation_input.input_digest,
            receipt=receipt,
        ),
        evaluation_digest=evaluation_digest,
        counts=counts,
        rule_evaluations=evaluations,
    )


def build_policy_evaluation_error_result_v1(
    evaluation_input: PolicyEvaluationInput,
    *,
    revisions: tuple[GuidelineRevision, ...],
    evaluated_at: datetime,
    evaluated_by: str,
    operational_error_code: PolicyOperationalErrorCode,
) -> PolicyEvaluatorOutput:
    """Materialize an honest deterministic receipt for an operational outage.

    The immutable bundle and every digest fence are validated first.  Callers
    must invoke this factory only for a typed operational evaluation failure;
    contract, stale-bundle and CAS failures propagate and must never be
    converted into fabricated evidence.
    """

    if not isinstance(evaluation_input, PolicyEvaluationInput):
        raise PolicyEvaluatorError("policy_evaluator_input_invalid")
    evaluated_at = _aware_utc(
        evaluated_at,
        "policy_evaluator_evaluated_at_invalid",
    )
    evaluated_by = _required_text(
        evaluated_by,
        "policy_evaluator_evaluated_by_required",
    )
    if not isinstance(operational_error_code, PolicyOperationalErrorCode):
        raise PolicyEvaluatorError("policy_evaluator_operational_error_code_invalid")
    if evaluation_input.requested_at > evaluated_at:
        raise PolicyEvaluatorError("policy_evaluator_requested_at_future")
    bindings, resolved = _validated_policy_bundle(evaluation_input, revisions)
    applicable = _applicable_prepared_rules(
        resolved,
        evaluation_input.subject_snapshot.subject.entity_type,
    )
    evaluations = tuple(
        PolicyRuleEvaluation(
            binding_id=binding.binding_id,
            guideline_id=prepared_revision.revision.guideline_id,
            revision_id=prepared_revision.revision.revision_id,
            rule_id=prepared_rule.rule.rule_id,
            rule_code=prepared_rule.rule.code,
            enforcement=prepared_rule.rule.enforcement,
            outcome=PolicyEvaluationOutcome.ERROR,
            predicate_results=(),
        )
        for binding, prepared_revision, prepared_rule in applicable
    )
    total_rule_count = sum(
        len(prepared_revision.rules) for _, prepared_revision in resolved
    )
    counts = _evaluation_counts(
        total_rule_count=total_rule_count,
        evaluations=evaluations,
    )
    error_code_digest = canonical_sha256(
        {
            "contract": "policy-evaluation-operational-error/v1",
            "error_code": operational_error_code.value,
        }
    )
    evaluation_digest = canonical_sha256(
        {
            "contract": POLICY_EVALUATOR_VERSION,
            "mode": "operational_error",
            "evaluation_id": evaluation_input.evaluation_id,
            "input_digest": evaluation_input.input_digest,
            "error_code_digest": error_code_digest,
            "evaluated_at": evaluated_at.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "evaluated_by": evaluated_by,
            "rules": tuple(
                {
                    "guideline_id": evaluation.guideline_id,
                    "revision_id": evaluation.revision_id,
                    "rule_id": evaluation.rule_id,
                    "enforcement": evaluation.enforcement.value,
                    "outcome": evaluation.outcome.value,
                }
                for evaluation in evaluations
            ),
            "counts": {
                field_name: getattr(counts, field_name)
                for field_name in counts.__dataclass_fields__
            },
        }
    )
    receipt_id = f"pcr_{evaluation_digest[:32]}"
    findings = tuple(
        PolicyComplianceFinding(
            finding_id=(
                "pcf_"
                + canonical_sha256(
                    {
                        "contract": "policy-compliance-finding/v1",
                        "receipt_id": receipt_id,
                        "guideline_id": evaluation.guideline_id,
                        "revision_id": evaluation.revision_id,
                        "rule_id": evaluation.rule_id,
                        "outcome": PolicyEvaluationOutcome.ERROR.value,
                    }
                )[:32]
            ),
            receipt_id=receipt_id,
            subject=evaluation_input.subject_snapshot.subject,
            guideline_id=evaluation.guideline_id,
            revision_id=evaluation.revision_id,
            rule_id=evaluation.rule_id,
            outcome=PolicyEvaluationOutcome.ERROR,
            enforcement=evaluation.enforcement,
            message=f"Policy rule {evaluation.rule_code} could not be evaluated.",
            created_at=evaluated_at,
            evidence_refs=(f"evaluation_error:{error_code_digest}",),
        )
        for evaluation in evaluations
    )
    blocking_error = any(evaluation.blocking for evaluation in evaluations)
    if not evaluations:
        receipt_outcome = PolicyEvaluationOutcome.NOT_APPLICABLE
        state = PolicyComplianceState.NOT_APPLICABLE
        reason_codes = (PolicyComplianceReasonCode.NO_APPLICABLE_RULES,)
    elif blocking_error:
        receipt_outcome = PolicyEvaluationOutcome.ERROR
        state = PolicyComplianceState.BLOCKED
        reason_codes = (PolicyComplianceReasonCode.POLICY_EVALUATION_UNAVAILABLE,)
    else:
        receipt_outcome = PolicyEvaluationOutcome.ERROR
        state = PolicyComplianceState.READY
        reason_codes = (PolicyComplianceReasonCode.POLICY_EVALUATION_DEGRADED,)
    receipt = PolicyComplianceReceipt(
        receipt_id=receipt_id,
        subject=evaluation_input.subject_snapshot.subject,
        subject_content_digest=evaluation_input.subject_snapshot.content_digest,
        input_digest=evaluation_input.input_digest,
        policy_set_digest=evaluation_input.policy_set_digest,
        binding_head_digest=evaluation_input.binding_head_digest,
        catalog_version=evaluation_input.catalog_version,
        ruleset_version=evaluation_input.ruleset_version,
        adopted_revisions=tuple(
            AdoptedGuidelineRevisionRef.from_binding(binding) for binding in bindings
        ),
        outcome=receipt_outcome,
        state=state,
        currentness=PolicyCurrentness.CURRENT,
        findings=findings,
        evaluator_version=POLICY_EVALUATOR_VERSION,
        evaluated_by=evaluated_by,
        evaluated_at=evaluated_at,
        rule_results=tuple(
            PolicyComplianceRuleResult(
                guideline_id=evaluation.guideline_id,
                revision_id=evaluation.revision_id,
                rule_id=evaluation.rule_id,
                outcome=PolicyEvaluationOutcome.ERROR,
                enforcement=evaluation.enforcement,
            )
            for evaluation in evaluations
        ),
        reason_codes=reason_codes,
    )
    return PolicyEvaluatorOutput(
        result=PolicyEvaluationResult(
            evaluation_id=evaluation_input.evaluation_id,
            input_digest=evaluation_input.input_digest,
            receipt=receipt,
        ),
        evaluation_digest=evaluation_digest,
        counts=counts,
        rule_evaluations=evaluations,
    )


__all__ = [
    "POLICY_EVALUATOR_VERSION",
    "POLICY_RULESET_VERSION",
    "PolicyEvaluationCounts",
    "PolicyEvaluatorError",
    "PolicyEvaluatorOutput",
    "PolicyOperationalErrorCode",
    "PolicyPredicateEvaluation",
    "PolicyRuleEvaluation",
    "build_policy_evaluation_error_result_v1",
    "build_policy_evaluation_input_v1",
    "evaluate_policy",
    "evaluate_policy_predicate",
    "policy_binding_head_digest_v1",
    "policy_evaluation_input_digest_v1",
    "policy_set_digest_v1",
]
