"""Closed deterministic predicate catalog for executable guidelines.

``policy/v1`` is a pure domain contract.  It exposes only named server-owned
facts and typed operators; it is deliberately not an expression language.
Unknown targets, facts, operators, parameters, and fact/operator combinations
fail before a guideline revision reaches persistence.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType
from typing import Final, Mapping

from okto_pulse.core.domain.enums import (
    CardPriority,
    CardStatus,
    CardType,
    IdeationComplexity,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintStatus,
)
from okto_pulse.core.domain.guideline_policy import (
    GuidelinePolicyContractError,
    GuidelinePredicate,
    GuidelineRevision,
    GuidelineRule,
    PolicyEntityType,
    PolicyParameterValue,
    PolicyScalar,
)
from okto_pulse.core.domain.quality_canonicalization import (
    canonical_json_bytes,
    canonical_sha256,
)
from okto_pulse.core.domain.test_scenarios import VALID_SCENARIO_TYPES


GUIDELINE_PREDICATE_CATALOG_VERSION: Final = "policy/v1"


class GuidelinePredicateCatalogError(ValueError):
    """A predicate is outside the closed policy/v1 language."""

    def __init__(
        self,
        code: str,
        *,
        target: object | None = None,
        fact: object | None = None,
        operator: object | None = None,
    ) -> None:
        self.code = code
        self.target = target
        self.fact = fact
        self.operator = operator
        super().__init__(code)


class PolicyFactValueType(str, Enum):
    BOOLEAN = "boolean"
    ENUM = "enum"
    INTEGER = "integer"
    NUMBER = "number"
    STRING_SET = "string_set"


class PolicyOperatorFamily(str, Enum):
    PRESENCE = "presence"
    EQUALITY = "equality"
    MEMBERSHIP = "membership"
    NUMERIC_COMPARISON = "numeric_comparison"
    COUNT = "count"
    CONTAINS = "contains"


class PolicyPredicateOperator(str, Enum):
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    EQ = "eq"
    NE = "ne"
    IN = "in"
    NOT_IN = "not_in"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    COUNT_EQ = "count_eq"
    COUNT_NE = "count_ne"
    COUNT_GT = "count_gt"
    COUNT_GTE = "count_gte"
    COUNT_LT = "count_lt"
    COUNT_LTE = "count_lte"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"


@dataclass(frozen=True, slots=True)
class PolicyOperatorDefinition:
    operator: PolicyPredicateOperator
    family: PolicyOperatorFamily
    parameter_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PolicyFactDefinition:
    target_entity_type: PolicyEntityType
    fact_code: str
    value_type: PolicyFactValueType
    allowed_operators: tuple[PolicyPredicateOperator, ...]
    allowed_values: tuple[str, ...] = ()
    minimum: float | None = None
    maximum: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.target_entity_type, PolicyEntityType):
            raise GuidelinePredicateCatalogError("policy_fact_target_invalid")
        if (
            not isinstance(self.fact_code, str)
            or not self.fact_code
            or self.fact_code != self.fact_code.strip()
        ):
            raise GuidelinePredicateCatalogError("policy_fact_code_invalid")
        if not isinstance(self.value_type, PolicyFactValueType):
            raise GuidelinePredicateCatalogError("policy_fact_type_invalid")
        if (
            not isinstance(self.allowed_operators, tuple)
            or not self.allowed_operators
            or any(
                not isinstance(operator, PolicyPredicateOperator)
                for operator in self.allowed_operators
            )
            or len(set(self.allowed_operators)) != len(self.allowed_operators)
        ):
            raise GuidelinePredicateCatalogError("policy_fact_operators_invalid")
        if (
            not isinstance(self.allowed_values, tuple)
            or any(
                not isinstance(value, str) or not value or value != value.strip()
                for value in self.allowed_values
            )
            or len(set(self.allowed_values)) != len(self.allowed_values)
        ):
            raise GuidelinePredicateCatalogError("policy_fact_allowed_values_invalid")
        if self.allowed_values and self.value_type is not PolicyFactValueType.ENUM:
            raise GuidelinePredicateCatalogError(
                "policy_fact_allowed_values_type_mismatch"
            )
        for bound in (self.minimum, self.maximum):
            if bound is not None and (
                isinstance(bound, bool)
                or not isinstance(bound, int | float)
                or not math.isfinite(float(bound))
            ):
                raise GuidelinePredicateCatalogError(
                    "policy_fact_numeric_bound_invalid"
                )
        if (
            self.minimum is not None
            and self.maximum is not None
            and self.minimum > self.maximum
        ):
            raise GuidelinePredicateCatalogError("policy_fact_numeric_bound_invalid")


_PRESENCE_OPERATORS = (
    PolicyPredicateOperator.EXISTS,
    PolicyPredicateOperator.NOT_EXISTS,
)
_EQUALITY_OPERATORS = (
    PolicyPredicateOperator.EQ,
    PolicyPredicateOperator.NE,
)
_MEMBERSHIP_OPERATORS = (
    PolicyPredicateOperator.IN,
    PolicyPredicateOperator.NOT_IN,
)
_NUMERIC_OPERATORS = (
    PolicyPredicateOperator.GT,
    PolicyPredicateOperator.GTE,
    PolicyPredicateOperator.LT,
    PolicyPredicateOperator.LTE,
)
_COUNT_OPERATORS = (
    PolicyPredicateOperator.COUNT_EQ,
    PolicyPredicateOperator.COUNT_NE,
    PolicyPredicateOperator.COUNT_GT,
    PolicyPredicateOperator.COUNT_GTE,
    PolicyPredicateOperator.COUNT_LT,
    PolicyPredicateOperator.COUNT_LTE,
)
_CONTAINS_OPERATORS = (
    PolicyPredicateOperator.CONTAINS,
    PolicyPredicateOperator.NOT_CONTAINS,
)

POLICY_OPERATOR_CATALOG_V1: Final[tuple[PolicyOperatorDefinition, ...]] = (
    PolicyOperatorDefinition(
        PolicyPredicateOperator.EXISTS,
        PolicyOperatorFamily.PRESENCE,
        ("fact",),
    ),
    PolicyOperatorDefinition(
        PolicyPredicateOperator.NOT_EXISTS,
        PolicyOperatorFamily.PRESENCE,
        ("fact",),
    ),
    PolicyOperatorDefinition(
        PolicyPredicateOperator.EQ,
        PolicyOperatorFamily.EQUALITY,
        ("fact", "value"),
    ),
    PolicyOperatorDefinition(
        PolicyPredicateOperator.NE,
        PolicyOperatorFamily.EQUALITY,
        ("fact", "value"),
    ),
    PolicyOperatorDefinition(
        PolicyPredicateOperator.IN,
        PolicyOperatorFamily.MEMBERSHIP,
        ("fact", "values"),
    ),
    PolicyOperatorDefinition(
        PolicyPredicateOperator.NOT_IN,
        PolicyOperatorFamily.MEMBERSHIP,
        ("fact", "values"),
    ),
    *(
        PolicyOperatorDefinition(
            operator,
            PolicyOperatorFamily.NUMERIC_COMPARISON,
            ("fact", "value"),
        )
        for operator in _NUMERIC_OPERATORS
    ),
    *(
        PolicyOperatorDefinition(
            operator,
            PolicyOperatorFamily.COUNT,
            ("fact", "value"),
        )
        for operator in _COUNT_OPERATORS
    ),
    PolicyOperatorDefinition(
        PolicyPredicateOperator.CONTAINS,
        PolicyOperatorFamily.CONTAINS,
        ("fact", "value"),
    ),
    PolicyOperatorDefinition(
        PolicyPredicateOperator.NOT_CONTAINS,
        PolicyOperatorFamily.CONTAINS,
        ("fact", "value"),
    ),
)
_OPERATOR_BY_CODE: Final[Mapping[str, PolicyOperatorDefinition]] = MappingProxyType(
    {definition.operator.value: definition for definition in POLICY_OPERATOR_CATALOG_V1}
)


def _enum_values(enum_type: type[Enum]) -> tuple[str, ...]:
    return tuple(item.value for item in enum_type)


_STATUS_VALUES: Final[Mapping[PolicyEntityType, tuple[str, ...]]] = MappingProxyType(
    {
        PolicyEntityType.IDEATION: _enum_values(IdeationStatus),
        PolicyEntityType.REFINEMENT: _enum_values(RefinementStatus),
        PolicyEntityType.SPEC: _enum_values(SpecStatus),
        PolicyEntityType.SPRINT: _enum_values(SprintStatus),
        PolicyEntityType.CARD: _enum_values(CardStatus),
        PolicyEntityType.TEST_SCENARIO: (
            "draft",
            "ready",
            "automated",
            "passed",
            "failed",
        ),
    }
)


def _fact(
    target: PolicyEntityType,
    code: str,
    value_type: PolicyFactValueType,
    *,
    allowed_values: tuple[str, ...] = (),
    minimum: float | None = None,
    maximum: float | None = None,
) -> PolicyFactDefinition:
    if value_type is PolicyFactValueType.BOOLEAN:
        operators = _PRESENCE_OPERATORS + _EQUALITY_OPERATORS
    elif value_type is PolicyFactValueType.ENUM:
        operators = _PRESENCE_OPERATORS + _EQUALITY_OPERATORS + _MEMBERSHIP_OPERATORS
    elif value_type in {
        PolicyFactValueType.INTEGER,
        PolicyFactValueType.NUMBER,
    }:
        operators = (
            _PRESENCE_OPERATORS
            + _EQUALITY_OPERATORS
            + _MEMBERSHIP_OPERATORS
            + _NUMERIC_OPERATORS
        )
    elif value_type is PolicyFactValueType.STRING_SET:
        operators = _PRESENCE_OPERATORS + _COUNT_OPERATORS + _CONTAINS_OPERATORS
    else:  # pragma: no cover - closed enum exhaustiveness
        raise GuidelinePredicateCatalogError("policy_fact_type_invalid")
    return PolicyFactDefinition(
        target_entity_type=target,
        fact_code=code,
        value_type=value_type,
        allowed_operators=operators,
        allowed_values=allowed_values,
        minimum=minimum,
        maximum=maximum,
    )


def _common_facts(target: PolicyEntityType) -> tuple[PolicyFactDefinition, ...]:
    return (
        _fact(
            target,
            "status",
            PolicyFactValueType.ENUM,
            allowed_values=_STATUS_VALUES[target],
        ),
        _fact(target, "labels", PolicyFactValueType.STRING_SET),
        _fact(target, "resource_gate_ready", PolicyFactValueType.BOOLEAN),
    )


_TARGET_SPECIFIC_FACTS: Final[
    Mapping[PolicyEntityType, tuple[PolicyFactDefinition, ...]]
] = MappingProxyType(
    {
        PolicyEntityType.IDEATION: (
            _fact(
                PolicyEntityType.IDEATION,
                "complexity",
                PolicyFactValueType.ENUM,
                allowed_values=_enum_values(IdeationComplexity),
            ),
            _fact(
                PolicyEntityType.IDEATION,
                "qa_open_count",
                PolicyFactValueType.INTEGER,
                minimum=0,
            ),
            _fact(
                PolicyEntityType.IDEATION,
                "ambiguity_score",
                PolicyFactValueType.NUMBER,
                minimum=1,
                maximum=5,
            ),
        ),
        PolicyEntityType.REFINEMENT: (
            _fact(
                PolicyEntityType.REFINEMENT,
                "research_open_count",
                PolicyFactValueType.INTEGER,
                minimum=0,
            ),
            _fact(
                PolicyEntityType.REFINEMENT,
                "research_resolved_count",
                PolicyFactValueType.INTEGER,
                minimum=0,
            ),
            _fact(
                PolicyEntityType.REFINEMENT,
                "ambiguity_score",
                PolicyFactValueType.NUMBER,
                minimum=1,
                maximum=5,
            ),
        ),
        PolicyEntityType.SPEC: (
            *(
                _fact(
                    PolicyEntityType.SPEC,
                    code,
                    PolicyFactValueType.INTEGER,
                    minimum=0,
                )
                for code in ("fr_count", "ac_count", "tr_count", "test_scenario_count")
            ),
            _fact(
                PolicyEntityType.SPEC,
                "coverage_percent",
                PolicyFactValueType.NUMBER,
                minimum=0,
                maximum=100,
            ),
            _fact(
                PolicyEntityType.SPEC,
                "validation_state",
                PolicyFactValueType.ENUM,
            ),
        ),
        PolicyEntityType.SPRINT: tuple(
            _fact(
                PolicyEntityType.SPRINT,
                code,
                PolicyFactValueType.INTEGER,
                minimum=0,
            )
            for code in (
                "card_count",
                "open_card_count",
                "test_scenario_count",
                "passed_scenario_count",
            )
        ),
        PolicyEntityType.CARD: (
            _fact(
                PolicyEntityType.CARD,
                "card_type",
                PolicyFactValueType.ENUM,
                allowed_values=_enum_values(CardType),
            ),
            _fact(
                PolicyEntityType.CARD,
                "priority",
                PolicyFactValueType.ENUM,
                allowed_values=_enum_values(CardPriority),
            ),
            *(
                _fact(
                    PolicyEntityType.CARD,
                    code,
                    PolicyFactValueType.INTEGER,
                    minimum=0,
                )
                for code in (
                    "dependency_open_count",
                    "test_scenario_count",
                    "evidence_count",
                )
            ),
        ),
        PolicyEntityType.TEST_SCENARIO: (
            _fact(
                PolicyEntityType.TEST_SCENARIO,
                "scenario_type",
                PolicyFactValueType.ENUM,
                allowed_values=VALID_SCENARIO_TYPES,
            ),
            _fact(
                PolicyEntityType.TEST_SCENARIO,
                "linked_test_card_count",
                PolicyFactValueType.INTEGER,
                minimum=0,
            ),
            _fact(
                PolicyEntityType.TEST_SCENARIO,
                "evidence_count",
                PolicyFactValueType.INTEGER,
                minimum=0,
            ),
        ),
    }
)

POLICY_FACT_CATALOG_V1: Final[tuple[PolicyFactDefinition, ...]] = tuple(
    fact
    for target in PolicyEntityType
    for fact in (*_common_facts(target), *_TARGET_SPECIFIC_FACTS[target])
)
_FACT_BY_TARGET_AND_CODE: Final[
    Mapping[tuple[PolicyEntityType, str], PolicyFactDefinition]
] = MappingProxyType(
    {(fact.target_entity_type, fact.fact_code): fact for fact in POLICY_FACT_CATALOG_V1}
)


def _operator_manifest(
    definition: PolicyOperatorDefinition,
) -> Mapping[str, object]:
    return MappingProxyType(
        {
            "operator": definition.operator.value,
            "family": definition.family.value,
            "parameter_names": definition.parameter_names,
        }
    )


def _fact_manifest(definition: PolicyFactDefinition) -> Mapping[str, object]:
    return MappingProxyType(
        {
            "target_entity_type": definition.target_entity_type.value,
            "fact_code": definition.fact_code,
            "value_type": definition.value_type.value,
            "allowed_operators": tuple(
                operator.value for operator in definition.allowed_operators
            ),
            "allowed_values": definition.allowed_values,
            "minimum": definition.minimum,
            "maximum": definition.maximum,
        }
    )


GUIDELINE_PREDICATE_CATALOG_MANIFEST_V1: Final[Mapping[str, object]] = MappingProxyType(
    {
        "version": GUIDELINE_PREDICATE_CATALOG_VERSION,
        "target_entity_types": tuple(target.value for target in PolicyEntityType),
        "operators": tuple(
            _operator_manifest(definition) for definition in POLICY_OPERATOR_CATALOG_V1
        ),
        "facts": tuple(
            _fact_manifest(definition) for definition in POLICY_FACT_CATALOG_V1
        ),
        "validation_state_values": "server_owned_open_string",
        "forbidden_languages": (
            "regex",
            "sql",
            "jmespath",
            "shell",
            "script",
            "attribute_path",
        ),
    }
)
GUIDELINE_PREDICATE_CATALOG_DIGEST: Final = canonical_sha256(
    GUIDELINE_PREDICATE_CATALOG_MANIFEST_V1
)


def require_policy_fact(
    target_entity_type: PolicyEntityType,
    fact_code: str,
) -> PolicyFactDefinition:
    if not isinstance(target_entity_type, PolicyEntityType):
        raise GuidelinePredicateCatalogError(
            "policy_target_invalid",
            target=target_entity_type,
        )
    if (
        not isinstance(fact_code, str)
        or not fact_code
        or fact_code != fact_code.strip()
    ):
        raise GuidelinePredicateCatalogError(
            "policy_fact_invalid",
            target=target_entity_type,
            fact=fact_code,
        )
    try:
        return _FACT_BY_TARGET_AND_CODE[(target_entity_type, fact_code)]
    except KeyError as exc:
        raise GuidelinePredicateCatalogError(
            "policy_fact_unknown",
            target=target_entity_type,
            fact=fact_code,
        ) from exc


def _normalize_scalar_for_fact(
    definition: PolicyFactDefinition,
    value: object,
) -> PolicyScalar:
    value_type = definition.value_type
    if value_type is PolicyFactValueType.BOOLEAN:
        if not isinstance(value, bool):
            raise GuidelinePredicateCatalogError(
                "policy_value_type_mismatch",
                target=definition.target_entity_type,
                fact=definition.fact_code,
            )
        return value
    if value_type is PolicyFactValueType.ENUM:
        if not isinstance(value, str) or not value or value != value.strip():
            raise GuidelinePredicateCatalogError(
                "policy_value_type_mismatch",
                target=definition.target_entity_type,
                fact=definition.fact_code,
            )
        if definition.allowed_values and value not in definition.allowed_values:
            raise GuidelinePredicateCatalogError(
                "policy_value_unknown",
                target=definition.target_entity_type,
                fact=definition.fact_code,
            )
        return value
    if value_type is PolicyFactValueType.INTEGER:
        if not isinstance(value, int) or isinstance(value, bool):
            raise GuidelinePredicateCatalogError(
                "policy_value_type_mismatch",
                target=definition.target_entity_type,
                fact=definition.fact_code,
            )
        normalized_number: int | float = value
    elif value_type is PolicyFactValueType.NUMBER:
        if (
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or not math.isfinite(float(value))
        ):
            raise GuidelinePredicateCatalogError(
                "policy_value_type_mismatch",
                target=definition.target_entity_type,
                fact=definition.fact_code,
            )
        normalized_number = float(value)
    else:
        raise GuidelinePredicateCatalogError(
            "policy_scalar_value_for_collection_fact",
            target=definition.target_entity_type,
            fact=definition.fact_code,
        )
    if (definition.minimum is not None and normalized_number < definition.minimum) or (
        definition.maximum is not None and normalized_number > definition.maximum
    ):
        raise GuidelinePredicateCatalogError(
            "policy_value_out_of_range",
            target=definition.target_entity_type,
            fact=definition.fact_code,
        )
    return normalized_number


def validate_policy_fact_value(
    definition: PolicyFactDefinition,
    value: object,
) -> PolicyParameterValue:
    """Validate one server-owned snapshot fact against its catalog type."""

    if not isinstance(definition, PolicyFactDefinition):
        raise GuidelinePredicateCatalogError("policy_fact_definition_invalid")
    if definition.value_type is not PolicyFactValueType.STRING_SET:
        return _normalize_scalar_for_fact(definition, value)
    if not isinstance(value, tuple | list):
        raise GuidelinePredicateCatalogError(
            "policy_value_type_mismatch",
            target=definition.target_entity_type,
            fact=definition.fact_code,
        )
    values: dict[bytes, str] = {}
    for item in value:
        if not isinstance(item, str) or not item or item != item.strip():
            raise GuidelinePredicateCatalogError(
                "policy_value_type_mismatch",
                target=definition.target_entity_type,
                fact=definition.fact_code,
            )
        values[canonical_json_bytes(item)] = item
    return tuple(values[key] for key in sorted(values))


def _canonical_membership_values(
    definitions: tuple[PolicyFactDefinition, ...],
    raw_values: object,
) -> tuple[PolicyScalar, ...]:
    if not isinstance(raw_values, tuple | list) or not raw_values:
        raise GuidelinePredicateCatalogError("policy_membership_values_invalid")
    canonical: dict[bytes, PolicyScalar] = {}
    for raw_value in raw_values:
        if isinstance(raw_value, tuple | list | dict | set):
            raise GuidelinePredicateCatalogError("policy_membership_values_invalid")
        normalized_by_target = tuple(
            _normalize_scalar_for_fact(definition, raw_value)
            for definition in definitions
        )
        if len({canonical_json_bytes(value) for value in normalized_by_target}) != 1:
            raise GuidelinePredicateCatalogError("policy_cross_target_value_mismatch")
        normalized = normalized_by_target[0]
        canonical[canonical_json_bytes(normalized)] = normalized
    return tuple(canonical[key] for key in sorted(canonical))


def _required_target_tuple(value: object) -> tuple[PolicyEntityType, ...]:
    if (
        not isinstance(value, tuple | list)
        or not value
        or any(not isinstance(target, PolicyEntityType) for target in value)
    ):
        raise GuidelinePredicateCatalogError("policy_target_invalid")
    targets = tuple(value)
    if len(set(targets)) != len(targets):
        raise GuidelinePredicateCatalogError("policy_target_duplicate")
    return tuple(sorted(targets, key=lambda target: target.value))


def validate_guideline_predicate(
    predicate: GuidelinePredicate,
    *,
    target_entity_types: tuple[PolicyEntityType, ...],
) -> GuidelinePredicate:
    """Return the canonical policy/v1 predicate or fail closed."""

    if not isinstance(predicate, GuidelinePredicate):
        raise GuidelinePredicateCatalogError("policy_predicate_invalid")
    targets = _required_target_tuple(target_entity_types)
    try:
        operator_definition = _OPERATOR_BY_CODE[predicate.predicate_code]
    except KeyError as exc:
        raise GuidelinePredicateCatalogError(
            "policy_operator_unknown",
            operator=predicate.predicate_code,
        ) from exc
    parameters = dict(predicate.parameters)
    expected_names = set(operator_definition.parameter_names)
    if set(parameters) != expected_names:
        raise GuidelinePredicateCatalogError(
            "policy_operator_parameters_invalid",
            operator=operator_definition.operator,
        )
    raw_fact = parameters["fact"]
    if not isinstance(raw_fact, str):
        raise GuidelinePredicateCatalogError("policy_fact_invalid", fact=raw_fact)
    definitions = tuple(require_policy_fact(target, raw_fact) for target in targets)
    operator = operator_definition.operator
    for definition in definitions:
        if operator not in definition.allowed_operators:
            raise GuidelinePredicateCatalogError(
                "policy_fact_operator_incompatible",
                target=definition.target_entity_type,
                fact=definition.fact_code,
                operator=operator,
            )

    canonical_parameters: list[tuple[str, PolicyParameterValue]] = [("fact", raw_fact)]
    if operator_definition.family is PolicyOperatorFamily.MEMBERSHIP:
        canonical_parameters.append(
            (
                "values",
                _canonical_membership_values(
                    definitions,
                    parameters["values"],
                ),
            )
        )
    elif operator_definition.family is PolicyOperatorFamily.COUNT:
        raw_value = parameters["value"]
        if (
            not isinstance(raw_value, int)
            or isinstance(raw_value, bool)
            or raw_value < 0
        ):
            raise GuidelinePredicateCatalogError(
                "policy_count_value_invalid",
                fact=raw_fact,
                operator=operator,
            )
        canonical_parameters.append(("value", raw_value))
    elif operator_definition.family is PolicyOperatorFamily.CONTAINS:
        raw_value = parameters["value"]
        if (
            not isinstance(raw_value, str)
            or not raw_value
            or raw_value != raw_value.strip()
        ):
            raise GuidelinePredicateCatalogError(
                "policy_contains_value_invalid",
                fact=raw_fact,
                operator=operator,
            )
        canonical_parameters.append(("value", raw_value))
    elif operator_definition.family is not PolicyOperatorFamily.PRESENCE:
        normalized_by_target = tuple(
            _normalize_scalar_for_fact(definition, parameters["value"])
            for definition in definitions
        )
        if len({canonical_json_bytes(value) for value in normalized_by_target}) != 1:
            raise GuidelinePredicateCatalogError(
                "policy_cross_target_value_mismatch",
                fact=raw_fact,
                operator=operator,
            )
        canonical_parameters.append(("value", normalized_by_target[0]))
    try:
        return GuidelinePredicate(
            predicate_code=operator.value,
            parameters=tuple(canonical_parameters),
        )
    except GuidelinePolicyContractError as exc:  # defensive contract mapping
        raise GuidelinePredicateCatalogError(
            "policy_predicate_canonicalization_failed",
            operator=operator,
        ) from exc


def validate_guideline_rule(rule: GuidelineRule) -> GuidelineRule:
    """Validate and canonicalize all predicates for one executable rule."""

    if not isinstance(rule, GuidelineRule):
        raise GuidelinePredicateCatalogError("policy_rule_invalid")
    predicates = tuple(
        validate_guideline_predicate(
            predicate,
            target_entity_types=rule.target_entity_types,
        )
        for predicate in rule.predicates
    )
    keys = tuple(
        canonical_json_bytes(
            {
                "predicate_code": predicate.predicate_code,
                "parameters": predicate.parameters,
            }
        )
        for predicate in predicates
    )
    if len(set(keys)) != len(keys):
        raise GuidelinePredicateCatalogError("policy_predicate_duplicate")
    return replace(rule, predicates=predicates)


def validate_guideline_revision_for_persistence(
    revision: GuidelineRevision,
) -> GuidelineRevision:
    """Final policy/v1 fail-closed boundary before a persistence port call."""

    if not isinstance(revision, GuidelineRevision):
        raise GuidelinePredicateCatalogError("policy_revision_invalid")
    return replace(
        revision,
        rules=tuple(validate_guideline_rule(rule) for rule in revision.rules),
    )


__all__ = [
    "GUIDELINE_PREDICATE_CATALOG_DIGEST",
    "GUIDELINE_PREDICATE_CATALOG_MANIFEST_V1",
    "GUIDELINE_PREDICATE_CATALOG_VERSION",
    "POLICY_FACT_CATALOG_V1",
    "POLICY_OPERATOR_CATALOG_V1",
    "GuidelinePredicateCatalogError",
    "PolicyFactDefinition",
    "PolicyFactValueType",
    "PolicyOperatorDefinition",
    "PolicyOperatorFamily",
    "PolicyPredicateOperator",
    "require_policy_fact",
    "validate_guideline_predicate",
    "validate_guideline_revision_for_persistence",
    "validate_guideline_rule",
    "validate_policy_fact_value",
]
