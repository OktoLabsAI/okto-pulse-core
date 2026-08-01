
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
