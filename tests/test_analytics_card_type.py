"""Regressão: classificação de cards em analytics usa enum CardType.

Bug original: ``_is_normal_card/_is_test_card/_is_bug_card`` comparavam
``str(card.card_type).endswith("normal|test|bug")``. Como
``card.card_type`` vem do SQLAlchemy como enum Python, ``str(ct)``
retorna ``"CardType.NORMAL"`` (maiúsculo) — ``.endswith("normal")``
era sempre False. Resultado: zerava ``total_cards_impl/test/bug``,
``task_validation_gate.total_submitted``, ``velocity[].test/bug`` e
propagava divergência com ``validation_bounce``.

Spec `f1b29a1d` — contrato rígido: compara enum por identidade.
"""

from __future__ import annotations

from types import SimpleNamespace

from okto_pulse.core.api.analytics import (
    _is_bug_card as api_is_bug_card,
    _is_normal_card as api_is_normal_card,
    _is_test_card as api_is_test_card,
)
from okto_pulse.core.models.db import CardType
from okto_pulse.core.services.analytics_service import (
    _is_bug_card as svc_is_bug_card,
    _is_normal_card as svc_is_normal_card,
    _is_test_card as svc_is_test_card,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _card(card_type):
    """Simula um Card carregado do SQLAlchemy com o enum real."""
    return SimpleNamespace(card_type=card_type)


# ===========================================================================
# api/analytics.py predicados
# ===========================================================================


def test_api_is_normal_card_true_for_enum_normal():
    assert api_is_normal_card(_card(CardType.NORMAL)) is True


def test_api_is_normal_card_false_for_enum_test_or_bug():
    assert api_is_normal_card(_card(CardType.TEST)) is False
    assert api_is_normal_card(_card(CardType.BUG)) is False


def test_api_is_test_card_true_for_enum_test():
    assert api_is_test_card(_card(CardType.TEST)) is True


def test_api_is_test_card_false_for_other_types():
    assert api_is_test_card(_card(CardType.NORMAL)) is False
    assert api_is_test_card(_card(CardType.BUG)) is False


def test_api_is_bug_card_true_for_enum_bug():
    assert api_is_bug_card(_card(CardType.BUG)) is True


def test_api_is_bug_card_false_for_other_types():
    assert api_is_bug_card(_card(CardType.NORMAL)) is False
    assert api_is_bug_card(_card(CardType.TEST)) is False


# Regressão explícita do bug original — o str do enum começa com
# "CardType." e é maiúsculo. A função legada usava
# str(ct).endswith("normal") que retornava False.
def test_api_regression_str_of_enum_is_uppercase():
    ct = CardType.NORMAL
    assert str(ct) == "CardType.NORMAL"
    assert not str(ct).endswith("normal"), (
        "Confirmação do bug original: str(enum) é maiúsculo"
    )
    # Mas o predicado novo funciona porque usa identidade:
    assert api_is_normal_card(_card(ct)) is True


def test_api_none_card_type_is_not_normal():
    """Contrato rígido: card_type None não é NORMAL. Sem fallback."""
    assert api_is_normal_card(_card(None)) is False
    assert api_is_test_card(_card(None)) is False
    assert api_is_bug_card(_card(None)) is False


# ===========================================================================
# services/analytics_service.py predicados (duplicados — mesmo contrato)
# ===========================================================================


def test_svc_is_normal_card_true_for_enum_normal():
    assert svc_is_normal_card(_card(CardType.NORMAL)) is True


def test_svc_is_test_card_true_for_enum_test():
    assert svc_is_test_card(_card(CardType.TEST)) is True


def test_svc_is_bug_card_true_for_enum_bug():
    assert svc_is_bug_card(_card(CardType.BUG)) is True


def test_svc_none_card_type_is_not_normal():
    assert svc_is_normal_card(_card(None)) is False
    assert svc_is_test_card(_card(None)) is False
    assert svc_is_bug_card(_card(None)) is False


# ===========================================================================
# Totals em mock dataset
# ===========================================================================


def test_totals_on_mixed_set_match_expected_enum_counts():
    cards = (
        [_card(CardType.NORMAL) for _ in range(130)]
        + [_card(CardType.TEST) for _ in range(212)]
        + [_card(CardType.BUG) for _ in range(3)]
    )
    impl = sum(1 for c in cards if api_is_normal_card(c))
    tests = sum(1 for c in cards if api_is_test_card(c))
    bugs = sum(1 for c in cards if api_is_bug_card(c))
    assert impl == 130
    assert tests == 212
    assert bugs == 3
    assert impl + tests + bugs == 345


# ===========================================================================
# Auditoria — analytics.py e analytics_service.py não contêm mais
# o padrão str(ct).endswith()
# ===========================================================================


def test_audit_no_endswith_pattern_in_analytics_sources():
    import re
    from pathlib import Path

    root = Path(__file__).parent.parent / "src" / "okto_pulse" / "core"
    files = [
        root / "api" / "analytics.py",
        root / "services" / "analytics_service.py",
    ]
    for f in files:
        content = f.read_text(encoding="utf-8")
        matches = re.findall(r'str\(ct\)\.endswith', content)
        assert matches == [], (
            f"{f.name} ainda contém str(ct).endswith — o refactor "
            f"não está completo."
        )


def test_audit_card_type_enum_referenced_in_predicates():
    from pathlib import Path

    root = Path(__file__).parent.parent / "src" / "okto_pulse" / "core"
    for f in [root / "api" / "analytics.py", root / "services" / "analytics_service.py"]:
        content = f.read_text(encoding="utf-8")
        assert "CardType.NORMAL" in content
        assert "CardType.TEST" in content
        assert "CardType.BUG" in content
