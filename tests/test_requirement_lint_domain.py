
def test_builtin_lexicons_cover_es_de_fr() -> None:
    """The five built-in languages detect linguistic signals; Gherkin
    condition patterns (Dado/Cuando/Entonces, Angenommen/Wenn/Dann,
    Etant donne/Quand/Alors) drive condition and observable result."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    cases = {
        RequirementLocale.ES: (
            "Cuando el usuario guarda, el sistema devuelve un error.",
            "Dado X cuando Y entonces Z",
        ),
        RequirementLocale.DE: (
            "Wenn der Benutzer speichert, liefert das System einen Fehler.",
            "Angenommen X wenn Y dann Z",
        ),
        RequirementLocale.FR: (
            "Lorsque l'utilisateur enregistre, le systeme retourne une erreur.",
            "Etant donne X quand Y alors Z",
        ),
    }
    for locale, (prose, gherkin) in cases.items():
        prose_signals = detect_requirement_signals(prose, locale=locale)
        assert prose_signals.condition, locale
        assert prose_signals.observable_result, locale
        assert prose_signals.error, locale
        assert prose_signals.verifiable_signal, locale

        gherkin_signals = detect_requirement_signals(gherkin, locale=locale)
        assert gherkin_signals.condition, locale
        assert gherkin_signals.observable_result, locale

    # The unknown profile remains neutral-only: the same prose yields no
    # linguistic signal without a declared locale.
    neutral = detect_requirement_signals(
        "Cuando el usuario guarda, el sistema devuelve un error.",
        locale=RequirementLocale.UNKNOWN,
    )
    assert not neutral.condition
    assert not neutral.observable_result
    assert not neutral.error


def test_unicode_comparators_count_as_symbolic() -> None:
    """S1: ≥/≤/≠ followed by a digit are comparators under EVERY profile,
    including the neutral UNKNOWN one — they are symbols, not language."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    for text in ("aceita ≥1 item", "limite ≤ 200 registros", "count ≠ 0"):
        signals = detect_requirement_signals(
            text, locale=RequirementLocale.UNKNOWN
        )
        assert signals.comparator, text
        assert signals.verifiable_signal, text
    bare = detect_requirement_signals(
        "o valor deve ser ≥ ao esperado", locale=RequirementLocale.UNKNOWN
    )
    assert not bare.comparator  # symbol without an adjacent digit stays inert


def test_pt_error_inflections_and_compounds() -> None:
    """S2: whole-word matching no longer misses inflections ('rejeitado')
    nor hyphenated compounds ('não-conforme')."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    for text in (
        "o move é rejeitado com remediation",
        "bloco corrompido diretamente no banco",
        "payload não-conforme ao schema",
        "entrada não conforme é descartada",
    ):
        signals = detect_requirement_signals(text, locale=RequirementLocale.PT)
        assert signals.error, text
        assert signals.verifiable_signal, text


def test_zero_cardinality_counts_as_comparator() -> None:
    """S4: 'NENHUM payload contém X' is a verifiable count-zero bound; the
    tokens are locale lexicon entries, so the neutral profile stays inert."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    pt = detect_requirement_signals(
        "nenhum payload de summary contém o campo",
        locale=RequirementLocale.PT,
    )
    assert pt.comparator
    en = detect_requirement_signals(
        "none of the summary payloads carry the field",
        locale=RequirementLocale.EN,
    )
    assert en.comparator
    pt_zero = detect_requirement_signals(
        "a seção abre com zero linhas", locale=RequirementLocale.PT
    )
    assert pt_zero.comparator
    neutral = detect_requirement_signals(
        "nenhum payload de summary contém o campo",
        locale=RequirementLocale.UNKNOWN,
    )
    assert not neutral.comparator


def test_strong_equality_and_bound_exact() -> None:
    """S5/S6: byte-level equality carries its own oracle; 'exatamente' only
    counts when it binds a number or enumerable article — the bare adverb
    stays vague (dilution guard)."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    assert detect_requirement_signals(
        "o bloco publicado permanece byte-idêntico no oneOf",
        locale=RequirementLocale.PT,
    ).comparator
    assert detect_requirement_signals(
        "the published block stays byte-identical",
        locale=RequirementLocale.EN,
    ).comparator
    assert detect_requirement_signals(
        "selects contendo exatamente os enums do FR-2",
        locale=RequirementLocale.PT,
    ).comparator
    assert not detect_requirement_signals(
        "deve se comportar exatamente como esperado",
        locale=RequirementLocale.PT,
    ).comparator


def test_ui_state_tokens() -> None:
    """S3: concrete UI states count under their locale."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    assert detect_requirement_signals(
        "a seção renderiza colapsada", locale=RequirementLocale.PT
    ).state
    assert detect_requirement_signals(
        "the block renders read-only", locale=RequirementLocale.EN
    ).state
    assert detect_requirement_signals(
        "the card is set to done", locale=RequirementLocale.EN
    ).state


def test_code_artifacts_are_neutral_technical_terms() -> None:
    """S7: identifiers, file names and key=value literals are code, not
    prose — they satisfy the technical restriction under every profile."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    for text in (
        "impact_evidence entra em CardMove fora do bloco",
        "segue o padrão de reviewer_separation.py",
        "modelos de INPUT fechados com extra='forbid'",
        "o json_schema_extra publicado permanece estável",
        "valores fora do enum 'off' são rejeitados",
    ):
        signals = detect_requirement_signals(
            text, locale=RequirementLocale.UNKNOWN
        )
        assert signals.technical_term, text
        assert signals.technical_restriction, text
    prose = detect_requirement_signals(
        "o sistema deve ser rápido e confiável",
        locale=RequirementLocale.UNKNOWN,
    )
    assert not prose.technical_term


def test_error_class_identifiers_signal_error_neutrally() -> None:
    """S7: CamelCase *Error/*Exception classes are error contracts in any
    language."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    signals = detect_requirement_signals(
        "levanta CardOperationError com remediation e facts",
        locale=RequirementLocale.UNKNOWN,
    )
    assert signals.error
    assert signals.verifiable_signal


def test_dilution_guards_stay_unverifiable() -> None:
    """Panel-vetoed generic verbs must NOT count as verifiable signals:
    'então a operação sucede' stays flagged-worthy."""

    from okto_pulse.core.domain.requirement_lint import (
        RequirementLocale,
        detect_requirement_signals,
    )

    for text in (
        "quando o usuário clica, então a operação sucede",
        "o sistema deve sempre funcionar",
        "então a página é renderizada corretamente",
        "então o sistema registra a operação",
    ):
        signals = detect_requirement_signals(text, locale=RequirementLocale.PT)
        assert not signals.verifiable_signal, text


def test_manifest_seals_new_signal_families() -> None:
    """The ruleset manifest must seal every new pattern and lexicon family
    so receipts stale when they change."""

    from okto_pulse.core.domain.requirement_lint import (
        REQUIREMENT_LINT_RULESET_MANIFEST_V1 as manifest,
    )

    for key in (
        "code_artifact_patterns",
        "error_class_pattern",
        "pt_zero_cardinality",
        "en_zero_cardinality",
        "pt_strong_equality",
        "en_strong_equality",
        "pt_bound_exact_pattern",
        "en_bound_exact_pattern",
    ):
        assert key in manifest, key
    assert "≥" in manifest["symbolic_comparator_pattern"]


def test_proposed_questions_carry_anchored_id_and_requirement_text() -> None:
    """A human answering in the dashboard sees only the question text: it must
    contain the flagged item id in parentheses AND the requirement text; long
    texts truncate deterministically at the sealed cap."""

    from okto_pulse.core.domain.requirement_lint import (
        QUESTION_ANCHOR_EXCERPT_CAP,
        REQUIREMENT_LINT_RULESET_MANIFEST_V1,
        RequirementEntityType,
        RequirementLintChild,
        RequirementLintContext,
        RequirementLocale,
        lint_requirements,
    )

    text = "AC-X: O sistema deve funcionar de forma adequada e performática."
    context = RequirementLintContext(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=1,
        input_digest="0" * 64,
        locale=RequirementLocale.UNKNOWN,
        locales=(RequirementLocale.PT,),
    )
    result = lint_requirements(
        [
            RequirementLintChild(
                entity_type=RequirementEntityType.AC,
                child_id="ac_anchor1",
                text=text,
            )
        ],
        context=context,
    )
    assert result.proposed_questions, "expected at least one question"
    question = result.proposed_questions[0].question
    assert "(ac_anchor1)" in question
    assert 'Requisito: "' in question
    assert text in question

    long_text = "AC-Y: " + ("critério vago " * 40)
    long_result = lint_requirements(
        [
            RequirementLintChild(
                entity_type=RequirementEntityType.AC,
                child_id="ac_anchor2",
                text=long_text,
            )
        ],
        context=context,
    )
    long_question = long_result.proposed_questions[0].question
    assert "…" in long_question
    start = long_question.index('Requisito: "') + len('Requisito: "')
    excerpt = long_question[start : long_question.rindex('"')]
    assert len(excerpt) <= QUESTION_ANCHOR_EXCERPT_CAP

    manifest = REQUIREMENT_LINT_RULESET_MANIFEST_V1
    assert manifest["question_anchor_format"] == (
        "child-id-parenthesized+excerpt/v1"
    )
    assert manifest["question_anchor_excerpt_cap"] == QUESTION_ANCHOR_EXCERPT_CAP
