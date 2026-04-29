"""Regression tests for spec 233eaad3 — cancelled-card filter no
spec_coverage_summary + extensão do shape em _coverage_row_for_spec.

Cobre cards C5a/C5b/C5c (unit tests) e C5d (integration tests).

Princípios SSOT (Single Source of Truth) verificados:
  - cards cancelled descobrem suas linkagens (TS/BR/Contract/TR/Decision)
  - status restore reverte o filtro
  - revoked/superseded decisions continuam fora do denominator (regression)
  - shape de _coverage_row_for_spec preserva fields legados + 4 novos

AC e FR coverage NÃO devem mudar (são estruturais via TS.linked_criteria
e BR.linked_requirements).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.services.analytics_service import (
    _coverage_row_for_spec,
    spec_coverage_summary,
)


# ---------------------------------------------------------------------------
# Helpers — fabricam Spec-like e Card-like objects via SimpleNamespace para
# evitar setup ORM completo. spec_coverage_summary é uma função pura.
# ---------------------------------------------------------------------------


def _make_spec(
    *,
    acs=None,
    frs=None,
    test_scenarios=None,
    business_rules=None,
    api_contracts=None,
    technical_requirements=None,
    decisions=None,
):
    return SimpleNamespace(
        id="spec-test",
        title="Spec Test",
        acceptance_criteria=acs or [],
        functional_requirements=frs or [],
        test_scenarios=test_scenarios or [],
        business_rules=business_rules or [],
        api_contracts=api_contracts or [],
        technical_requirements=technical_requirements or [],
        decisions=decisions or [],
        skip_test_coverage=False,
        skip_rules_coverage=False,
        skip_decisions_coverage=False,
    )


def _card(card_id: str, status: str):
    """Fabrica um card-like com .id e .status (string ou enum-like)."""
    return SimpleNamespace(
        id=card_id,
        status=SimpleNamespace(value=status),
        spec_id="spec-test",
        archived=False,
        card_type=SimpleNamespace(value="normal"),
    )


# ---------------------------------------------------------------------------
# C5a — Backward compat e TS drop
# ---------------------------------------------------------------------------


class TestBackwardCompat:
    def test_no_cards_kwarg_returns_baseline_shape(self):
        """spec_coverage_summary(spec) sem cards deve retornar shape histórico."""
        spec = _make_spec(
            test_scenarios=[{"linked_task_ids": ["c1"]}, {}],
            business_rules=[{"linked_task_ids": ["c1"]}],
            api_contracts=[{"linked_task_ids": []}],
            technical_requirements=[{"id": "tr1", "text": "x", "linked_task_ids": ["c1"]}],
            decisions=[{"id": "d1", "status": "active", "linked_task_ids": ["c1"]}],
        )
        cov = spec_coverage_summary(spec)
        assert cov["scenarios_linked"] == 1
        assert cov["brs_linked"] == 1
        assert cov["contracts_linked"] == 0
        assert cov["trs_linked"] == 1
        assert cov["decisions_linked"] == 1
        assert cov["decisions_uncovered_ids"] == []

    def test_cards_with_no_cancellations_equals_no_cards_path(self):
        """cards param contendo apenas in_progress/done/started cards
        produz mesmos números que sem cards (set difference vazio)."""
        spec = _make_spec(
            test_scenarios=[{"linked_task_ids": ["c1"]}],
            business_rules=[{"linked_task_ids": ["c1"]}],
            decisions=[{"id": "d1", "status": "active", "linked_task_ids": ["c1"]}],
        )
        baseline = spec_coverage_summary(spec)
        with_cards = spec_coverage_summary(
            spec,
            cards=[_card("c1", "in_progress"), _card("c2", "done")],
        )
        for k in (
            "scenarios_linked",
            "brs_linked",
            "contracts_linked",
            "trs_linked",
            "decisions_linked",
            "decisions_uncovered_ids",
        ):
            assert with_cards[k] == baseline[k], f"diverged on key {k}"


class TestTSDrop:
    def test_ts_drop_when_only_card_cancelled(self):
        spec = _make_spec(test_scenarios=[{"linked_task_ids": ["c_a"]}])
        cov = spec_coverage_summary(spec, cards=[_card("c_a", "cancelled")])
        assert cov["scenarios_linked"] == 0
        assert cov["scenario_task_linkage_pct"] == 0.0


# ---------------------------------------------------------------------------
# C5b — BR/Contract/TR drop
# ---------------------------------------------------------------------------


class TestBRDrop:
    def test_br_drop_when_only_card_cancelled(self):
        spec = _make_spec(business_rules=[{"linked_task_ids": ["c_b"]}])
        cov = spec_coverage_summary(spec, cards=[_card("c_b", "cancelled")])
        assert cov["brs_linked"] == 0
        assert cov["br_task_linkage_pct"] == 0.0


class TestContractDrop:
    def test_contract_drop_when_only_card_cancelled(self):
        spec = _make_spec(api_contracts=[{"linked_task_ids": ["c_c"]}])
        cov = spec_coverage_summary(spec, cards=[_card("c_c", "cancelled")])
        assert cov["contracts_linked"] == 0
        assert cov["contract_task_linkage_pct"] == 0.0


class TestTRDrop:
    def test_tr_drop_when_only_card_cancelled(self):
        spec = _make_spec(
            technical_requirements=[
                {"id": "tr1", "text": "x", "linked_task_ids": ["c_d"]}
            ]
        )
        cov = spec_coverage_summary(spec, cards=[_card("c_d", "cancelled")])
        assert cov["trs_linked"] == 0
        assert cov["tr_task_linkage_pct"] == 0.0


# ---------------------------------------------------------------------------
# C5c — Decision drop, restore e revoked regression
# ---------------------------------------------------------------------------


class TestDecisionDropAndUncoveredIds:
    def test_decision_drop_includes_uncovered_id(self):
        spec = _make_spec(
            decisions=[
                {"id": "dec_x", "status": "active", "linked_task_ids": ["c_e"]}
            ]
        )
        cov = spec_coverage_summary(spec, cards=[_card("c_e", "cancelled")])
        assert cov["decisions_linked"] == 0
        assert cov["decisions_coverage_pct"] == 0.0
        assert cov["decisions_uncovered_ids"] == ["dec_x"]


class TestStatusRestore:
    def test_restore_brings_back_coverage(self):
        spec = _make_spec(business_rules=[{"linked_task_ids": ["c_f"]}])
        # First: card cancelled — coverage drop
        cov_before = spec_coverage_summary(
            spec, cards=[_card("c_f", "cancelled")]
        )
        assert cov_before["brs_linked"] == 0
        # Then: card restored to in_progress — coverage returns
        cov_after = spec_coverage_summary(
            spec, cards=[_card("c_f", "in_progress")]
        )
        assert cov_after["brs_linked"] == 1
        assert cov_after["br_task_linkage_pct"] == 100.0


class TestRevokedSupersededRegression:
    def test_revoked_and_superseded_decisions_excluded_from_total(self):
        """Filtro pré-existente de active_decisions deve permanecer
        funcionando mesmo após introdução do cancelled-card filter."""
        spec = _make_spec(
            decisions=[
                {"id": "dec_a", "status": "active", "linked_task_ids": ["c"]},
                {"id": "dec_b", "status": "revoked", "linked_task_ids": ["c"]},
                {"id": "dec_c", "status": "superseded", "linked_task_ids": ["c"]},
            ]
        )
        cov = spec_coverage_summary(spec, cards=[_card("c", "in_progress")])
        # Apenas dec_a entra no denominator
        assert cov["decisions_total"] == 1
        assert cov["decisions_linked"] == 1


# ---------------------------------------------------------------------------
# C5d — Integration tests: _coverage_row_for_spec shape extension
# (covers TS for board_spec_analytics e board_coverage indirectly via
# spec_coverage_summary, plus shape preservation contract.)
# ---------------------------------------------------------------------------


class TestCoverageRowForSpecExtension:
    def test_extended_shape_includes_4_new_fields(self):
        spec = _make_spec(
            acs=["AC1"],
            frs=["FR1"],
            test_scenarios=[{"linked_criteria": [0], "linked_task_ids": ["c"]}],
            decisions=[
                {"id": "d1", "status": "active", "linked_task_ids": ["c"]}
            ],
            technical_requirements=[
                {"id": "tr1", "text": "tr", "linked_task_ids": ["c"]}
            ],
        )
        row = _coverage_row_for_spec(spec, cards=[_card("c", "in_progress")])
        # Legacy fields preserved
        for legacy_key in (
            "spec_id",
            "title",
            "total_ac",
            "covered_ac",
            "total_scenarios",
            "scenario_status_counts",
            "business_rules_count",
            "api_contracts_count",
            "fr_with_rules_pct",
            "fr_with_contracts_pct",
        ):
            assert legacy_key in row, f"legacy field {legacy_key} missing"
        # 4 new fields present
        for new_key in (
            "decisions_coverage_pct",
            "decisions_total",
            "tr_task_linkage_pct",
            "trs_total",
        ):
            assert new_key in row, f"new field {new_key} missing"
        assert row["decisions_total"] == 1
        assert row["trs_total"] == 1

    def test_cancelled_card_propagates_to_extended_shape(self):
        spec = _make_spec(
            decisions=[
                {"id": "d1", "status": "active", "linked_task_ids": ["c"]}
            ],
            technical_requirements=[
                {"id": "tr1", "text": "tr", "linked_task_ids": ["c"]}
            ],
        )
        row = _coverage_row_for_spec(spec, cards=[_card("c", "cancelled")])
        assert row["decisions_coverage_pct"] == 0.0
        assert row["tr_task_linkage_pct"] == 0.0


class TestACFRCoverageUnaffected:
    """AC e FR coverage devem permanecer estáveis — são estruturais
    via TS.linked_criteria e BR.linked_requirements."""

    def test_ac_coverage_unchanged_when_card_cancelled(self):
        spec = _make_spec(
            acs=["AC1"],
            test_scenarios=[
                {"linked_criteria": [0], "linked_task_ids": ["c"]}
            ],
        )
        baseline = spec_coverage_summary(spec)
        with_cancelled = spec_coverage_summary(
            spec, cards=[_card("c", "cancelled")]
        )
        assert with_cancelled["ac_coverage_pct"] == baseline["ac_coverage_pct"]
        assert with_cancelled["ac_covered"] == baseline["ac_covered"]

    def test_fr_coverage_unchanged_when_card_cancelled(self):
        spec = _make_spec(
            frs=["FR1"],
            business_rules=[
                {"linked_requirements": [0], "linked_task_ids": ["c"]}
            ],
        )
        baseline = spec_coverage_summary(spec)
        with_cancelled = spec_coverage_summary(
            spec, cards=[_card("c", "cancelled")]
        )
        assert with_cancelled["fr_coverage_pct"] == baseline["fr_coverage_pct"]
        assert with_cancelled["fr_covered"] == baseline["fr_covered"]
