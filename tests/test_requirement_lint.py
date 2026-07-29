from __future__ import annotations

from dataclasses import asdict

import pytest

from okto_pulse.core.domain.quality_assessment import (
    AssessmentScaleKind,
    FindingAnchorType,
    FindingSeverity,
    ScoreDirection,
)
from okto_pulse.core.domain.quality_canonicalization import (
    canonical_json_bytes,
    ruleset_digest_v1,
)
from okto_pulse.core.domain.requirement_lint import (
    REQUIREMENT_LINT_RULESET_DIGEST,
    REQUIREMENT_LINT_RULESET_MANIFEST_V1,
    REQUIREMENT_LINT_RULESET_VERSION,
    REQUIREMENT_LINT_RULE_CATALOG_V1,
    REQUIREMENT_LINT_TECHNICAL_LEXICON_DIGEST,
    RequirementEntityType,
    RequirementLintChild,
    RequirementLintContext,
    RequirementLintContractError,
    RequirementLocale,
    detect_requirement_signals,
    lint_requirements,
    requirement_lint_children_from_spec_payload,
)

INPUT_DIGEST = "a" * 64


def _context(
    locale: RequirementLocale = RequirementLocale.EN,
) -> RequirementLintContext:
    return RequirementLintContext(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=7,
        input_digest=INPUT_DIGEST,
        locale=locale,
    )


def _child(
    entity_type: RequirementEntityType,
    child_id: str,
    text: str,
    *,
    status: str = "active",
    locale: RequirementLocale | None = None,
) -> RequirementLintChild:
    return RequirementLintChild(
        entity_type=entity_type,
        child_id=child_id,
        text=text,
        status=status,
        locale=locale,
    )


def _rule_codes(result) -> set[str]:
    return {finding.rule_code for finding in result.findings}


def test_ruleset_catalog_version_and_digests_are_deterministic() -> None:
    assert REQUIREMENT_LINT_RULESET_VERSION == "requirement_lint_ruleset_v1"
    assert REQUIREMENT_LINT_RULESET_DIGEST == ruleset_digest_v1(
        REQUIREMENT_LINT_RULESET_VERSION,
        REQUIREMENT_LINT_RULESET_MANIFEST_V1,
    )
    assert len(REQUIREMENT_LINT_RULESET_DIGEST) == 64
    assert len(REQUIREMENT_LINT_TECHNICAL_LEXICON_DIGEST) == 64
    assert tuple(rule.code for rule in REQUIREMENT_LINT_RULE_CATALOG_V1) == tuple(
        sorted(rule.code for rule in REQUIREMENT_LINT_RULE_CATALOG_V1)
    )

    fr_ac_rules = {
        rule.code
        for rule in REQUIREMENT_LINT_RULE_CATALOG_V1
        if set(rule.entity_types)
        & {RequirementEntityType.FR, RequirementEntityType.AC}
    }
    assert not any("technology_agnostic" in code for code in fr_ac_rules)
    assert not any("technical_coupling" in code for code in fr_ac_rules)


@pytest.mark.parametrize(
    ("locale", "text", "signal"),
    [
        (RequirementLocale.EN, "Responds within 100 ms.", "number_with_unit"),
        (RequirementLocale.EN, "Latency must be <= 100.", "comparator"),
        (
            RequirementLocale.EN,
            "Given a user, When they save, Then the API returns status 200.",
            "gwt_en",
        ),
        (
            RequirementLocale.PT,
            "Dado um usuário, Quando salvar, Então retorna o status ativo.",
            "dado_quando_entao_pt",
        ),
        (RequirementLocale.EN, "If the account exists, continue.", "condition"),
        (RequirementLocale.PT, "Se a conta existir, continue.", "condition"),
        (RequirementLocale.EN, "The API returns the result.", "observable_result"),
        (RequirementLocale.PT, "A API retorna o resultado.", "observable_result"),
        (RequirementLocale.EN, "The state is active.", "state"),
        (RequirementLocale.PT, "O estado fica ativo.", "state"),
        (RequirementLocale.EN, "Return an error on failure.", "error"),
        (RequirementLocale.PT, "Retorna erro em caso de falha.", "error"),
        (RequirementLocale.UNKNOWN, "Persist as PostgreSQL JSON.", "technical_term"),
    ],
    ids=(
        "en-number-unit",
        "en-comparator",
        "en-gwt",
        "pt-dado-quando-entao",
        "en-condition",
        "pt-condition",
        "en-observable-result",
        "pt-observable-result",
        "en-state",
        "pt-state",
        "en-error",
        "pt-error",
        "unknown-technical-term",
    ),
)
def test_positive_detectors(
    locale: RequirementLocale,
    text: str,
    signal: str,
) -> None:
    detected = detect_requirement_signals(text, locale=locale)
    assert getattr(detected, signal) is True


@pytest.mark.parametrize(
    ("locale", "text", "expected"),
    [
        (RequirementLocale.PT, "O limite permanece a definir.", True),
        (RequirementLocale.PT, "Todo pedido pendente deve ser processado.", False),
        (RequirementLocale.EN, "The limit is to be defined.", True),
        (RequirementLocale.EN, "Every pending request must be processed.", False),
        (RequirementLocale.UNKNOWN, "O limite permanece a definir.", False),
        (RequirementLocale.UNKNOWN, "The limit is to be defined.", False),
    ],
    ids=(
        "pt-localized-placeholder",
        "pt-legitimate-todo-pendente",
        "en-localized-placeholder",
        "en-legitimate-pending",
        "unknown-ignores-pt-placeholder",
        "unknown-ignores-en-placeholder",
    ),
)
def test_localized_placeholder_matrix(
    locale: RequirementLocale,
    text: str,
    expected: bool,
) -> None:
    detected = detect_requirement_signals(text, locale=locale)
    assert detected.localized_placeholder is expected


@pytest.mark.parametrize(
    ("locale", "text", "expected"),
    [
        (RequirementLocale.PT, "A resposta chega em até 100 ms.", True),
        (RequirementLocale.PT, "A resposta chega em até o fechamento.", False),
        (RequirementLocale.EN, "The response arrives within 100 ms.", True),
        (
            RequirementLocale.EN,
            "The response arrives within the maintenance window.",
            False,
        ),
        (RequirementLocale.UNKNOWN, "The response is <= 100.", True),
        (RequirementLocale.UNKNOWN, "The response arrives within 100 ms.", False),
    ],
    ids=(
        "pt-bounded-comparator",
        "pt-isolated-lexical-comparator",
        "en-bounded-comparator",
        "en-isolated-lexical-comparator",
        "unknown-symbolic-comparator",
        "unknown-ignores-localized-comparator",
    ),
)
def test_comparator_matrix_requires_a_bound_for_lexical_terms(
    locale: RequirementLocale,
    text: str,
    expected: bool,
) -> None:
    detected = detect_requirement_signals(text, locale=locale)
    assert detected.comparator is expected


@pytest.mark.parametrize(
    ("locale", "text", "expected"),
    [
        (RequirementLocale.PT, "A latência é de 100 milissegundos.", True),
        (RequirementLocale.PT, "A disponibilidade é de 99,9%.", True),
        (RequirementLocale.PT, "A capacidade é de 100 bananas.", False),
        (RequirementLocale.EN, "Latency is 100 milliseconds.", True),
        (RequirementLocale.EN, "Availability is 99.9%.", True),
        (RequirementLocale.EN, "Capacity is 100 bananas.", False),
        (RequirementLocale.UNKNOWN, "Latency is 100 milliseconds.", True),
        (RequirementLocale.UNKNOWN, "Capacity is 100 bananas.", False),
    ],
    ids=(
        "pt-number-unit",
        "pt-decimal-percent",
        "pt-number-without-unit",
        "en-number-unit",
        "en-decimal-percent",
        "en-number-without-unit",
        "unknown-neutral-number-unit",
        "unknown-number-without-unit",
    ),
)
def test_number_with_unit_matrix(
    locale: RequirementLocale,
    text: str,
    expected: bool,
) -> None:
    detected = detect_requirement_signals(text, locale=locale)
    assert detected.number_with_unit is expected


@pytest.mark.parametrize(
    ("locale", "text", "expected"),
    [
        (RequirementLocale.PT, "O fluxo deve ser rápido.", True),
        (RequirementLocale.PT, "O fluxo deve retornar status ativo.", False),
        (RequirementLocale.EN, "O fluxo deve ser rápido.", False),
        (RequirementLocale.UNKNOWN, "O fluxo deve ser rápido.", False),
    ],
    ids=(
        "pt-vague-term",
        "pt-concrete-language",
        "en-ignores-pt-vague-term",
        "unknown-ignores-pt-vague-term",
    ),
)
def test_portuguese_vague_term_matrix(
    locale: RequirementLocale,
    text: str,
    expected: bool,
) -> None:
    detected = detect_requirement_signals(text, locale=locale)
    assert detected.vague_term is expected


def test_todo_marker_is_case_sensitive_and_portuguese_todo_is_prose() -> None:
    marker = detect_requirement_signals(
        "TODO: definir o limite.",
        locale=RequirementLocale.PT,
    )
    ordinary_prose = detect_requirement_signals(
        "Todo pedido pendente deve retornar o status pendente.",
        locale=RequirementLocale.PT,
    )

    assert marker.neutral_placeholder is True
    assert ordinary_prose.neutral_placeholder is False
    assert ordinary_prose.localized_placeholder is False
    assert ordinary_prose.state is True

    result = lint_requirements(
        [
            _child(
                RequirementEntityType.FR,
                "fr_marker",
                "TODO: definir o limite.",
                locale=RequirementLocale.PT,
            ),
            _child(
                RequirementEntityType.FR,
                "fr_ordinary_prose",
                "Todo pedido pendente deve retornar o status pendente.",
                locale=RequirementLocale.PT,
            ),
        ],
        context=_context(RequirementLocale.PT),
    )
    assert [
        finding.rule_code
        for finding in result.findings
        if finding.anchor.anchor_ref == "fr_marker"
    ] == ["requirement_neutral_placeholder"]
    assert not any(
        finding.anchor.anchor_ref == "fr_ordinary_prose"
        for finding in result.findings
    )


def test_pending_is_a_state_not_a_localized_placeholder() -> None:
    english = detect_requirement_signals(
        "Every pending request must return pending status.",
        locale=RequirementLocale.EN,
    )
    portuguese = detect_requirement_signals(
        "Todo pedido pendente deve retornar status pendente.",
        locale=RequirementLocale.PT,
    )

    assert english.state is True
    assert english.localized_placeholder is False
    assert portuguese.state is True
    assert portuguese.localized_placeholder is False

    result = lint_requirements(
        [
            _child(
                RequirementEntityType.FR,
                "fr_pending_en",
                "Every pending request must return pending status.",
                locale=RequirementLocale.EN,
            ),
            _child(
                RequirementEntityType.FR,
                "fr_pendente_pt",
                "Todo pedido pendente deve retornar status pendente.",
                locale=RequirementLocale.PT,
            ),
        ],
        context=_context(),
    )
    assert result.findings == ()


def test_well_formed_pt_and_en_requirements_have_no_findings() -> None:
    en = lint_requirements(
        [
            _child(
                RequirementEntityType.FR,
                "fr_login",
                "The service returns an error state within 100 ms if login fails.",
            ),
            _child(
                RequirementEntityType.AC,
                "ac_login",
                (
                    "Given an invalid password, When login is requested, "
                    "Then the API returns error status 401 within 100 ms."
                ),
            ),
            _child(
                RequirementEntityType.TR,
                "tr_api",
                "The REST API uses HTTPS and JSON.",
            ),
        ],
        context=_context(RequirementLocale.EN),
    )
    pt = lint_requirements(
        [
            _child(
                RequirementEntityType.AC,
                "ac_login_pt",
                (
                    "Dado login inválido, Quando autenticar, Então retorna "
                    "status de erro 401 em no máximo 100 ms."
                ),
            ),
            _child(
                RequirementEntityType.TR,
                "tr_api_pt",
                "A API usa HTTPS e JSON.",
            ),
        ],
        context=_context(RequirementLocale.PT),
    )

    assert en.findings == ()
    assert pt.findings == ()


def test_negative_rules_are_explainable_and_do_not_police_fr_technology() -> None:
    result = lint_requirements(
        [
            _child(RequirementEntityType.FR, "fr_vague", "The flow must be easy."),
            _child(RequirementEntityType.AC, "ac_weak", "The operation succeeds."),
            _child(RequirementEntityType.TR, "tr_weak", "It must be robust."),
            _child(
                RequirementEntityType.FR,
                "fr_tech",
                "Implement the workflow with PostgreSQL.",
            ),
        ],
        context=_context(),
    )

    codes = _rule_codes(result)
    assert "fr_vague_without_observable_outcome" in codes
    assert "ac_condition_missing" in codes
    assert "ac_observable_result_missing" in codes
    assert "ac_verifiable_signal_missing" in codes
    assert "tr_technical_restriction_missing" in codes
    assert not any(
        finding.anchor.anchor_ref == "fr_tech"
        for finding in result.findings
    )
    assert all(finding.blocking_eligible is False for finding in result.findings)
    assert all(finding.deterministic is True for finding in result.findings)


def test_child_locale_overrides_spec_locale() -> None:
    portuguese_child = _child(
        RequirementEntityType.AC,
        "ac_child_pt",
        (
            "Dado login inválido, Quando autenticar, Então retorna "
            "status de erro 401 em no máximo 100 ms."
        ),
        locale=RequirementLocale.PT,
    )
    inherited_english_child = _child(
        RequirementEntityType.AC,
        "ac_child_pt",
        portuguese_child.text,
    )

    overridden = lint_requirements(
        [portuguese_child],
        context=_context(RequirementLocale.EN),
    )
    inherited = lint_requirements(
        [inherited_english_child],
        context=_context(RequirementLocale.EN),
    )

    assert overridden.findings == ()
    assert {
        finding.rule_code for finding in inherited.findings
    } == {
        "ac_condition_missing",
        "ac_observable_result_missing",
    }


def test_unknown_child_locale_overrides_spec_and_stays_neutral_only() -> None:
    result = lint_requirements(
        [
            _child(
                RequirementEntityType.AC,
                "ac_child_unknown",
                "Given X When Y Then returns status within 100 ms.",
                locale=RequirementLocale.UNKNOWN,
            )
        ],
        context=_context(RequirementLocale.EN),
    )

    assert result.findings == ()
    assert result.evaluated_rule_count == 3
    assert "ac_condition_missing" not in _rule_codes(result)
    assert "ac_observable_result_missing" not in _rule_codes(result)


def test_locale_is_explicit_and_unknown_uses_only_neutral_detectors() -> None:
    portuguese = (
        "Dado um usuário, Quando salvar, Então retorna estado de erro "
        "em no máximo 100 ms usando JSON."
    )
    as_en = detect_requirement_signals(portuguese, locale=RequirementLocale.EN)
    as_unknown = detect_requirement_signals(
        portuguese,
        locale=RequirementLocale.UNKNOWN,
    )

    assert as_en.dado_quando_entao_pt is False
    assert as_unknown.gwt_en is False
    assert as_unknown.dado_quando_entao_pt is False
    assert as_unknown.condition is False
    assert as_unknown.observable_result is False
    assert as_unknown.state is False
    assert as_unknown.error is False
    assert as_unknown.number_with_unit is True
    assert as_unknown.technical_term is True

    unknown_result = lint_requirements(
        [
            _child(
                RequirementEntityType.AC,
                "ac_unknown",
                "Given X When Y Then returns status within 100 ms.",
            )
        ],
        context=_context(RequirementLocale.UNKNOWN),
    )
    assert "ac_condition_missing" not in _rule_codes(unknown_result)
    assert "ac_observable_result_missing" not in _rule_codes(unknown_result)
    assert unknown_result.findings == ()

    with pytest.raises(
        RequirementLintContractError,
        match="requirement_lint_locale_invalid",
    ):
        _context("en")  # type: ignore[arg-type]


def test_tr_catalog_is_separate_from_fr_and_ac() -> None:
    plain_text = "The component must remain reliable."
    result = lint_requirements(
        [
            _child(RequirementEntityType.FR, "fr_plain", plain_text),
            _child(RequirementEntityType.TR, "tr_plain", plain_text),
            _child(
                RequirementEntityType.TR,
                "tr_bound",
                "The endpoint latency must be <= 250 ms.",
            ),
        ],
        context=_context(),
    )

    tr_findings = [
        finding
        for finding in result.findings
        if finding.anchor.anchor_ref == "tr_plain"
    ]
    assert [finding.rule_code for finding in tr_findings] == [
        "tr_technical_restriction_missing"
    ]
    assert not any(
        finding.rule_code == "tr_technical_restriction_missing"
        and finding.anchor.anchor_ref != "tr_plain"
        for finding in result.findings
    )


def test_fr_and_ac_never_emit_technology_coupling_findings() -> None:
    result = lint_requirements(
        [
            _child(
                RequirementEntityType.FR,
                "fr_postgresql",
                (
                    "If PostgreSQL is available, the service returns active "
                    "status within 100 ms."
                ),
            ),
            _child(
                RequirementEntityType.AC,
                "ac_postgresql",
                (
                    "Given PostgreSQL is available, When data is saved, "
                    "Then the service returns active status within 100 ms."
                ),
            ),
        ],
        context=_context(RequirementLocale.EN),
    )
    technical_rules = tuple(
        rule
        for rule in REQUIREMENT_LINT_RULE_CATALOG_V1
        if "technical" in rule.code or "technology" in rule.code
    )

    assert result.findings == ()
    assert technical_rules
    assert all(
        rule.entity_types == (RequirementEntityType.TR,)
        for rule in technical_rules
    )


def test_inactive_children_are_ignored_and_empty_scale_remains_valid() -> None:
    inactive = _child(
        RequirementEntityType.AC,
        "ac_inactive",
        "",
        status="deprecated",
        locale=RequirementLocale.UNKNOWN,
    )
    result = lint_requirements(
        [inactive],
        context=_context(),
    )
    baseline = lint_requirements([], context=_context())

    assert result == baseline
    assert result.active_child_count == 0
    assert result.findings == ()
    assert result.proposed_questions == ()
    assert result.evaluated_rule_count == 1
    assert result.score == 0.0
    assert result.scale.kind is AssessmentScaleKind.FINDING_COUNT
    assert result.scale.minimum == 0.0
    assert result.scale.maximum == 1.0
    assert result.scale.direction is ScoreDirection.LOWER_BETTER


def test_inactive_children_do_not_change_active_counts_or_rule_counts() -> None:
    active = _child(
        RequirementEntityType.TR,
        "tr_active",
        "The REST API uses HTTPS and JSON.",
    )
    active_only = lint_requirements([active], context=_context())
    with_inactive = lint_requirements(
        [
            active,
            _child(
                RequirementEntityType.AC,
                "ac_inactive",
                "",
                status="deprecated",
                locale=RequirementLocale.UNKNOWN,
            )
        ],
        context=_context(),
    )

    assert with_inactive == active_only
    assert with_inactive.active_child_count == 1
    assert with_inactive.evaluated_rule_count == active_only.evaluated_rule_count


def test_findings_have_stable_structured_child_anchors_and_order() -> None:
    children = [
        _child(RequirementEntityType.AC, "ac_z", ""),
        _child(RequirementEntityType.TR, "tr_a", "TBD"),
    ]
    first = lint_requirements(children, context=_context())
    second = lint_requirements(list(reversed(children)), context=_context())

    assert first == second
    assert [finding.finding_key for finding in first.findings] == [
        finding.finding_key for finding in second.findings
    ]
    assert all(
        finding.anchor.anchor_type is FindingAnchorType.STRUCTURED_CHILD
        for finding in first.findings
    )
    assert all(
        finding.anchor.anchor_ref in {"ac_z", "tr_a"}
        for finding in first.findings
    )
    assert all(
        finding.anchor.input_digest == INPUT_DIGEST
        and finding.anchor.subject_id == "spec-1"
        and finding.anchor.subject_version == 7
        and len(finding.anchor.excerpt_hash or "") == 64
        for finding in first.findings
    )


def test_question_budget_uses_severity_confidence_and_stable_key_order() -> None:
    result = lint_requirements(
        [
            _child(RequirementEntityType.AC, "ac_c", ""),
            _child(RequirementEntityType.AC, "ac_a", ""),
            _child(RequirementEntityType.AC, "ac_b", ""),
        ],
        context=_context(),
    )
    severity_rank = {
        FindingSeverity.INFO: 0,
        FindingSeverity.LOW: 1,
        FindingSeverity.MEDIUM: 2,
        FindingSeverity.HIGH: 3,
        FindingSeverity.CRITICAL: 4,
    }
    expected_order = sorted(
        result.findings,
        key=lambda finding: (
            -severity_rank[finding.severity],
            -finding.confidence,
            finding.finding_key,
        ),
    )

    assert list(result.findings) == expected_order
    assert len(result.proposed_questions) == 5
    assert [
        question.finding_keys[0] for question in result.proposed_questions
    ] == [finding.finding_key for finding in result.findings[:5]]


def test_scale_is_finding_count_bounded_by_evaluated_rules() -> None:
    result = lint_requirements(
        [
            _child(RequirementEntityType.AC, "ac_empty", ""),
            _child(RequirementEntityType.TR, "tr_empty", ""),
        ],
        context=_context(),
    )

    assert result.score == float(len(result.findings))
    assert result.scale.minimum == 0.0
    assert result.scale.maximum == float(result.evaluated_rule_count)
    assert 1 <= result.evaluated_rule_count
    assert result.score <= result.scale.maximum


def test_empty_input_has_a_deterministic_scale_baseline() -> None:
    first = lint_requirements([], context=_context())
    second = lint_requirements((), context=_context())

    assert first == second
    assert canonical_json_bytes(asdict(first)) == canonical_json_bytes(
        asdict(second)
    )
    assert first.active_child_count == 0
    assert first.evaluated_rule_count == 1
    assert first.findings == ()
    assert first.proposed_questions == ()
    assert first.score == first.scale.validate_score(0)
    assert first.scale.kind is AssessmentScaleKind.FINDING_COUNT
    assert first.scale.minimum == 0.0
    assert first.scale.maximum == 1.0
    assert first.scale.direction is ScoreDirection.LOWER_BETTER


def test_repeated_evaluation_is_byte_identical_after_unicode_normalization() -> None:
    composed = _child(
        RequirementEntityType.AC,
        "ac_unicode",
        "Given café, When saved, Then returns status within 100 ms.\r\n",
    )
    decomposed = _child(
        RequirementEntityType.AC,
        "ac_unicode",
        "Given cafe\u0301, When saved, Then returns status within 100 ms.\n",
    )

    first = lint_requirements([composed], context=_context())
    second = lint_requirements([decomposed], context=_context())

    assert canonical_json_bytes(asdict(first)) == canonical_json_bytes(
        asdict(second)
    )


def test_duplicate_or_mutable_child_identity_fails_closed() -> None:
    with pytest.raises(
        RequirementLintContractError,
        match="requirement_lint_child_id_invalid",
    ):
        _child(RequirementEntityType.FR, "functional_requirements[0]", "Text")

    child = _child(RequirementEntityType.FR, "fr_same", "Text")
    with pytest.raises(
        RequirementLintContractError,
        match="requirement_lint_duplicate_child_id",
    ):
        lint_requirements([child, child], context=_context())


def test_spec_payload_mapper_has_one_route_independent_child_order() -> None:
    children = requirement_lint_children_from_spec_payload(
        {
            "functional_requirements": [
                {
                    "id": "fr_one",
                    "text": "O sistema retorna status.",
                    "status": "active",
                    "locale": "pt",
                }
            ],
            "acceptance_criteria": [
                {
                    "id": "ac_one",
                    "text": "Given input, When saved, Then returns status.",
                    "status": "active",
                    "locale": "en",
                }
            ],
            "technical_requirements": [
                {
                    "id": "tr_one",
                    "text": "Use HTTP within 100 ms.",
                    "status": "active",
                }
            ],
        }
    )

    assert [(child.entity_type, child.child_id) for child in children] == [
        (RequirementEntityType.FR, "fr_one"),
        (RequirementEntityType.AC, "ac_one"),
        (RequirementEntityType.TR, "tr_one"),
    ]
    assert [child.locale for child in children] == [
        RequirementLocale.PT,
        RequirementLocale.EN,
        None,
    ]


def test_spec_payload_mapper_rejects_an_explicit_unknown_locale_literal() -> None:
    with pytest.raises(
        RequirementLintContractError,
        match="requirement_lint_locale_invalid",
    ):
        requirement_lint_children_from_spec_payload(
            {
                "functional_requirements": [
                    {
                        "id": "fr_one",
                        "text": "Text",
                        "status": "active",
                        "locale": "pt-BR",
                    }
                ]
            }
        )
