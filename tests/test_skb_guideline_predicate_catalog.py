"""SK-B B05 acceptance tests for the closed policy/v1 predicate catalog."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path

import pytest

from okto_pulse.core.domain.guideline_policy import (
    GuidelinePolicyContractError,
    GuidelinePredicate,
    GuidelineRevision,
    GuidelineRule,
    PolicyEntityType,
)
from okto_pulse.core.domain.guideline_predicate_catalog import (
    GUIDELINE_PREDICATE_CATALOG_DIGEST,
    GUIDELINE_PREDICATE_CATALOG_MANIFEST_V1,
    GUIDELINE_PREDICATE_CATALOG_VERSION,
    POLICY_FACT_CATALOG_V1,
    POLICY_OPERATOR_CATALOG_V1,
    GuidelinePredicateCatalogError,
    PolicyOperatorFamily,
    PolicyPredicateOperator,
    require_policy_fact,
    validate_guideline_predicate,
    validate_guideline_revision_for_persistence,
    validate_guideline_rule,
    validate_policy_fact_value,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)
CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "domain"
    / "guideline_predicate_catalog.py"
)
_MISSING = object()

COMMON_FACTS = {"status", "labels", "resource_gate_ready"}
SPECIFIC_FACTS = {
    PolicyEntityType.IDEATION: {
        "complexity",
        "qa_open_count",
        "ambiguity_score",
    },
    PolicyEntityType.REFINEMENT: {
        "research_open_count",
        "research_resolved_count",
        "ambiguity_score",
    },
    PolicyEntityType.SPEC: {
        "fr_count",
        "ac_count",
        "tr_count",
        "test_scenario_count",
        "coverage_percent",
        "validation_state",
    },
    PolicyEntityType.SPRINT: {
        "card_count",
        "open_card_count",
        "test_scenario_count",
        "passed_scenario_count",
    },
    PolicyEntityType.CARD: {
        "card_type",
        "priority",
        "dependency_open_count",
        "test_scenario_count",
        "evidence_count",
    },
    PolicyEntityType.TEST_SCENARIO: {
        "scenario_type",
        "status",
        "linked_test_card_count",
        "evidence_count",
    },
}


def _predicate(
    operator: str,
    fact: str,
    *,
    value: object = _MISSING,
    values: object = _MISSING,
    extra: tuple[tuple[str, object], ...] = (),
) -> GuidelinePredicate:
    parameters: list[tuple[str, object]] = [("fact", fact)]
    if value is not _MISSING:
        parameters.append(("value", value))
    if values is not _MISSING:
        parameters.append(("values", values))
    parameters.extend(extra)
    return GuidelinePredicate(operator, parameters)


def _rule(
    predicate: GuidelinePredicate,
    *,
    targets: tuple[PolicyEntityType, ...] = (PolicyEntityType.SPEC,),
) -> GuidelineRule:
    return GuidelineRule(
        rule_id="rule-1",
        code="policy.rule_1",
        title="Policy rule",
        description="A deterministic policy rule.",
        target_entity_types=targets,
        predicates=(predicate,),
    )


def _revision(rule: GuidelineRule) -> GuidelineRevision:
    return GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Executable guideline",
        content="Deterministic policy.",
        content_digest="a" * 64,
        rules=(rule,),
        created_by="agent-1",
        created_at=NOW,
    )


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_policy_v1_manifest_and_digest_are_stable_and_deep_read_only() -> None:
    assert GUIDELINE_PREDICATE_CATALOG_VERSION == "policy/v1"
    assert len(GUIDELINE_PREDICATE_CATALOG_DIGEST) == 64
    assert GUIDELINE_PREDICATE_CATALOG_DIGEST == canonical_sha256(
        GUIDELINE_PREDICATE_CATALOG_MANIFEST_V1
    )
    with pytest.raises(TypeError):
        GUIDELINE_PREDICATE_CATALOG_MANIFEST_V1["version"] = "changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        GUIDELINE_PREDICATE_CATALOG_MANIFEST_V1["facts"][0]["fact_code"] = "x"  # type: ignore[index]


def test_catalog_contains_exact_common_and_per_target_fact_matrix() -> None:
    by_target = {
        target: {
            definition.fact_code
            for definition in POLICY_FACT_CATALOG_V1
            if definition.target_entity_type is target
        }
        for target in PolicyEntityType
    }
    expected = {
        target: COMMON_FACTS | specific for target, specific in SPECIFIC_FACTS.items()
    }

    assert by_target == expected
    assert len(POLICY_FACT_CATALOG_V1) == 42
    assert len(
        {
            (definition.target_entity_type, definition.fact_code)
            for definition in POLICY_FACT_CATALOG_V1
        }
    ) == len(POLICY_FACT_CATALOG_V1)


def test_operator_catalog_is_closed_to_six_families() -> None:
    assert {definition.family for definition in POLICY_OPERATOR_CATALOG_V1} == {
        PolicyOperatorFamily.PRESENCE,
        PolicyOperatorFamily.EQUALITY,
        PolicyOperatorFamily.MEMBERSHIP,
        PolicyOperatorFamily.NUMERIC_COMPARISON,
        PolicyOperatorFamily.COUNT,
        PolicyOperatorFamily.CONTAINS,
    }
    assert {definition.operator for definition in POLICY_OPERATOR_CATALOG_V1} == set(
        PolicyPredicateOperator
    )


@pytest.mark.parametrize(
    ("target", "predicate", "expected_parameters"),
    [
        (
            PolicyEntityType.SPEC,
            _predicate("exists", "validation_state"),
            (("fact", "validation_state"),),
        ),
        (
            PolicyEntityType.SPEC,
            _predicate("eq", "resource_gate_ready", value=True),
            (("fact", "resource_gate_ready"), ("value", True)),
        ),
        (
            PolicyEntityType.SPEC,
            _predicate("gte", "coverage_percent", value=90),
            (("fact", "coverage_percent"), ("value", 90.0)),
        ),
        (
            PolicyEntityType.SPEC,
            _predicate("count_gte", "labels", value=2),
            (("fact", "labels"), ("value", 2)),
        ),
        (
            PolicyEntityType.SPEC,
            _predicate("contains", "labels", value="security"),
            (("fact", "labels"), ("value", "security")),
        ),
    ],
    ids=(
        "presence",
        "equality",
        "numeric-comparison",
        "count",
        "contains",
    ),
)
def test_valid_operator_families_canonicalize(
    target,
    predicate,
    expected_parameters,
) -> None:
    result = validate_guideline_predicate(
        predicate,
        target_entity_types=(target,),
    )

    assert result.parameters == expected_parameters


def test_membership_is_distinct_from_contains_and_order_independent() -> None:
    first = validate_guideline_predicate(
        _predicate(
            "in",
            "status",
            values=["validated", "approved", "validated"],
        ),
        target_entity_types=(PolicyEntityType.SPEC,),
    )
    second = validate_guideline_predicate(
        _predicate(
            "in",
            "status",
            values=("approved", "validated"),
        ),
        target_entity_types=(PolicyEntityType.SPEC,),
    )

    assert dict(first.parameters)["values"] == ("approved", "validated")
    assert first == second
    assert canonical_sha256(first.parameters) == canonical_sha256(second.parameters)

    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_fact_operator_incompatible",
    ):
        validate_guideline_predicate(
            _predicate("contains", "status", value="approved"),
            target_entity_types=(PolicyEntityType.SPEC,),
        )


def test_flat_parameter_collections_extend_b01_without_mutability() -> None:
    raw_values = ["review", "approved"]
    predicate = _predicate("in", "status", values=raw_values)
    raw_values.append("done")

    assert dict(predicate.parameters)["values"] == ("review", "approved")

    for invalid in ([], [["review"]], [{"status": "review"}]):
        with pytest.raises(GuidelinePolicyContractError):
            _predicate("in", "status", values=invalid)


def test_cross_target_rules_require_the_fact_and_value_to_be_valid_for_every_target() -> (
    None
):
    common = validate_guideline_predicate(
        _predicate("eq", "resource_gate_ready", value=True),
        target_entity_types=(
            PolicyEntityType.IDEATION,
            PolicyEntityType.REFINEMENT,
        ),
    )
    assert common.predicate_code == "eq"

    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_fact_unknown",
    ):
        validate_guideline_predicate(
            _predicate("gte", "qa_open_count", value=0),
            target_entity_types=(
                PolicyEntityType.IDEATION,
                PolicyEntityType.REFINEMENT,
            ),
        )

    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_value_unknown",
    ):
        validate_guideline_predicate(
            _predicate("eq", "status", value="evaluating"),
            target_entity_types=(
                PolicyEntityType.IDEATION,
                PolicyEntityType.REFINEMENT,
            ),
        )


@pytest.mark.parametrize(
    "operator",
    (
        "regex",
        "sql",
        "jmespath",
        "shell",
        "script",
        "python",
        "attribute",
        "status.__class__",
    ),
)
def test_expression_languages_and_unknown_operators_are_rejected(operator) -> None:
    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_operator_unknown",
    ):
        validate_guideline_predicate(
            _predicate(operator, "status", value="approved"),
            target_entity_types=(PolicyEntityType.SPEC,),
        )


@pytest.mark.parametrize(
    ("predicate", "code"),
    [
        (
            _predicate("eq", "unknown_fact", value="value"),
            "policy_fact_unknown",
        ),
        (
            _predicate("eq", "status.__class__", value="approved"),
            "policy_fact_unknown",
        ),
        (
            _predicate("eq", "status", value="approved", extra=(("sql", "x"),)),
            "policy_operator_parameters_invalid",
        ),
        (
            _predicate("gte", "resource_gate_ready", value=1),
            "policy_fact_operator_incompatible",
        ),
        (
            _predicate("count_gte", "status", value=1),
            "policy_fact_operator_incompatible",
        ),
        (
            _predicate("contains", "coverage_percent", value="90"),
            "policy_fact_operator_incompatible",
        ),
        (
            _predicate("gte", "coverage_percent", value=True),
            "policy_value_type_mismatch",
        ),
        (
            _predicate("eq", "fr_count", value=True),
            "policy_value_type_mismatch",
        ),
    ],
    ids=(
        "unknown-fact",
        "attribute-path-fact",
        "unknown-parameter",
        "numeric-on-boolean",
        "count-on-enum",
        "contains-on-number",
        "bool-as-number",
        "bool-as-integer",
    ),
)
def test_unknown_and_type_incompatible_combinations_fail_closed(
    predicate,
    code,
) -> None:
    with pytest.raises(GuidelinePredicateCatalogError) as raised:
        validate_guideline_predicate(
            predicate,
            target_entity_types=(PolicyEntityType.SPEC,),
        )
    assert raised.value.code == code


def test_all_and_untyped_targets_are_rejected_before_fact_resolution() -> None:
    for invalid_targets in (("all",), ("spec",), (), (PolicyEntityType.SPEC, "all")):
        with pytest.raises(
            GuidelinePredicateCatalogError,
            match="policy_target_invalid",
        ):
            validate_guideline_predicate(
                _predicate("eq", "status", value="approved"),
                target_entity_types=invalid_targets,  # type: ignore[arg-type]
            )


def test_validation_state_name_and_operators_are_closed_but_values_remain_server_owned() -> (
    None
):
    predicate = validate_guideline_predicate(
        _predicate("eq", "validation_state", value="provider_specific_current"),
        target_entity_types=(PolicyEntityType.SPEC,),
    )
    assert dict(predicate.parameters)["value"] == "provider_specific_current"

    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_value_type_mismatch",
    ):
        validate_guideline_predicate(
            _predicate("eq", "validation_state", value=1),
            target_entity_types=(PolicyEntityType.SPEC,),
        )


def test_snapshot_fact_values_are_typed_and_bool_is_never_numeric() -> None:
    labels = require_policy_fact(PolicyEntityType.SPEC, "labels")
    count = require_policy_fact(PolicyEntityType.SPEC, "fr_count")
    score = require_policy_fact(PolicyEntityType.SPEC, "coverage_percent")

    assert validate_policy_fact_value(labels, ["security", "security", "api"]) == (
        "api",
        "security",
    )
    assert validate_policy_fact_value(count, 0) == 0
    assert validate_policy_fact_value(score, 92) == 92.0
    for definition in (count, score):
        with pytest.raises(
            GuidelinePredicateCatalogError,
            match="policy_value_type_mismatch",
        ):
            validate_policy_fact_value(definition, True)


def test_rule_and_revision_boundary_canonicalize_before_persistence() -> None:
    rule = _rule(
        _predicate(
            "in",
            "status",
            values=("validated", "approved", "validated"),
        )
    )
    revision = _revision(rule)

    prepared = validate_guideline_revision_for_persistence(revision)
    assert dict(prepared.rules[0].predicates[0].parameters)["values"] == (
        "approved",
        "validated",
    )

    invalid = _revision(_rule(_predicate("sql", "status", value="1=1")))
    persisted: list[GuidelineRevision] = []
    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_operator_unknown",
    ):
        persisted.append(validate_guideline_revision_for_persistence(invalid))
    assert persisted == []


def test_duplicate_canonical_predicates_are_rejected() -> None:
    first = _predicate("in", "status", values=("approved", "review"))
    second = _predicate("in", "status", values=("review", "approved", "review"))
    rule = GuidelineRule(
        rule_id="rule-1",
        code="policy.rule_1",
        title="Policy rule",
        description="No duplicate predicates.",
        target_entity_types=(PolicyEntityType.SPEC,),
        predicates=(first, second),
    )

    with pytest.raises(
        GuidelinePredicateCatalogError,
        match="policy_predicate_duplicate",
    ):
        validate_guideline_rule(rule)


def test_catalog_module_has_no_transport_persistence_or_dynamic_language_imports() -> (
    None
):
    imports = _imported_modules(CATALOG_PATH)
    assert not any(
        module == forbidden or module.startswith(f"{forbidden}.")
        for module in imports
        for forbidden in (
            "fastapi",
            "pydantic",
            "sqlalchemy",
            "okto_pulse.community",
            "okto_pulse.core.infra",
            "re",
            "subprocess",
        )
    )
    source = CATALOG_PATH.read_text(encoding="utf-8").lower()
    for forbidden in ("eval(", "exec(", "compile(", "__import__(", "getattr("):
        assert forbidden not in source
