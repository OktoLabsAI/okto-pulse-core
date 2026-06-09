"""
Tests for spec 8fdb20be (Refinement B of ideation 41425528 — F13).

A per-requirement not_applicable + justification on IR / OR / ApiContract, honored
in the spec-validation coverage so a single requirement can be waived WITHOUT
flipping the coarse per-spec skip. Additive: the per-spec skip is preserved.

Coverage is exercised via the pure spec_coverage_summary (SimpleNamespace spec +
dict items), mirroring tests/test_spec_coverage_cancelled_filter.py. Schema-level
checks (enum + justification validator) use the Pydantic models directly. AC5 is
load-bearing via negative-wiring (the same item, active, still counts).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import get_args

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.schemas import (
    ApiContract,
    DecisionStatus,
    IntegrationRequirement,
    ObservabilityRequirement,
)
from okto_pulse.core.services.analytics_service import spec_coverage_summary

CORE_DIR = Path(__file__).parent.parent / "src" / "okto_pulse" / "core"
SCHEMAS_PY = CORE_DIR / "models" / "schemas.py"
ANALYTICS_PY = CORE_DIR / "services" / "analytics_service.py"
COVERAGE_CALC_PY = CORE_DIR / "services" / "coverage_calculator.py"


def _spec(*, irs=None, ors=None, contracts=None, skip_ir=False, skip_or=False):
    return SimpleNamespace(
        id="spec-na",
        title="Spec N/A",
        acceptance_criteria=[],
        functional_requirements=[],
        test_scenarios=[],
        business_rules=[],
        api_contracts=contracts or [],
        technical_requirements=[],
        decisions=[],
        integration_requirements=irs or [],
        observability_requirements=ors or [],
        skip_test_coverage=False,
        skip_rules_coverage=False,
        skip_decisions_coverage=False,
        skip_ir_coverage=skip_ir,
        skip_or_coverage=skip_or,
    )


# ---------------------------------------------------------------------------
# AC1 — the status accepts not_applicable for IR/OR/contract, not Decision
# ---------------------------------------------------------------------------


def test_ac1_status_accepts_not_applicable_for_ir_or_contract_not_decision() -> None:
    IntegrationRequirement(id="ir", title="t", status="not_applicable", notes="waived")
    ObservabilityRequirement(id="or", title="t", status="not_applicable", notes="waived")
    ApiContract(id="c", method="GET", path="/x", status="not_applicable", notes="waived")
    # DecisionStatus is a separate alias and must NOT gain the value.
    assert "not_applicable" not in get_args(DecisionStatus)


# ---------------------------------------------------------------------------
# AC2 — not_applicable items excluded from the coverage denominator
# ---------------------------------------------------------------------------


def test_ac2_not_applicable_excluded_from_denominator() -> None:
    spec = _spec(
        irs=[
            {"id": "ir_na", "status": "not_applicable", "linked_task_ids": []},
            {"id": "ir_a", "status": "active", "linked_task_ids": ["card1"]},
        ],
        ors=[{"id": "or_na", "status": "not_applicable", "linked_task_ids": []}],
        contracts=[
            {"id": "c_na", "status": "not_applicable", "linked_task_ids": []},
            {"id": "c_a", "status": "active", "linked_task_ids": ["card1"]},
        ],
    )
    cov = spec_coverage_summary(spec)
    assert cov["irs_total"] == 1  # only ir_a
    assert "ir_na" not in (cov["irs_uncovered_ids"] or [])
    assert cov["ors_total"] == 0  # the only OR is not_applicable
    assert cov["contracts_total"] == 1  # only c_a


# ---------------------------------------------------------------------------
# AC3 — a not_applicable waiver requires a justification
# ---------------------------------------------------------------------------


def test_ac3_not_applicable_requires_justification() -> None:
    for model, kwargs in (
        (IntegrationRequirement, {"id": "ir", "title": "t"}),
        (ObservabilityRequirement, {"id": "or", "title": "t"}),
        (ApiContract, {"id": "c", "method": "GET", "path": "/x"}),
    ):
        with pytest.raises(ValidationError):
            model(status="not_applicable", **kwargs)  # no notes
        # with a justification it is accepted
        model(status="not_applicable", notes="justified waiver", **kwargs)


# ---------------------------------------------------------------------------
# AC4 — the supersede/revoke / update path tolerates a not_applicable item
# ---------------------------------------------------------------------------


def test_ac4_not_applicable_is_coverage_exclusion_not_lifecycle() -> None:
    ir = IntegrationRequirement(id="ir", title="t", status="not_applicable", notes="w")
    # The status is preserved (not coerced to a lifecycle state); the item stays visible.
    assert ir.status == "not_applicable"
    dumped = ir.model_dump()
    assert dumped["status"] == "not_applicable"
    # Coverage tolerates the value (no error) and excludes it.
    spec = _spec(irs=[dumped])
    cov = spec_coverage_summary(spec)
    assert cov["irs_total"] == 0


# ---------------------------------------------------------------------------
# AC5 — per-spec skip preserved + the exclusion is load-bearing (negative-wiring)
# ---------------------------------------------------------------------------


def test_ac5_skip_preserved_and_exclusion_load_bearing() -> None:
    # Real: a not_applicable IR is excluded from the denominator.
    spec_na = _spec(irs=[{"id": "ir_na", "status": "not_applicable", "linked_task_ids": []}])
    assert spec_coverage_summary(spec_na)["irs_total"] == 0
    # Negative-wiring: the SAME item, active, DOES count — so the exclusion is real,
    # not an always-zero denominator.
    spec_active = _spec(irs=[{"id": "ir_a", "status": "active", "linked_task_ids": []}])
    assert spec_coverage_summary(spec_active)["irs_total"] == 1
    # The per-spec skip flag is preserved end-to-end in the payload.
    spec_skip = _spec(
        irs=[{"id": "ir_a", "status": "active", "linked_task_ids": []}], skip_ir=True
    )
    assert spec_coverage_summary(spec_skip)["skip_ir_coverage"] is True


# ---------------------------------------------------------------------------
# AC6 — DecisionStatus + skip logic + irs/ors filters byte-unchanged
# ---------------------------------------------------------------------------


def test_ac6_machinery_unchanged() -> None:
    schemas = SCHEMAS_PY.read_text(encoding="utf-8")
    # DecisionStatus did NOT gain not_applicable.
    assert 'DecisionStatus = Literal["active", "superseded", "revoked"]' in schemas

    analytics = ANALYTICS_PY.read_text(encoding="utf-8")
    # irs/ors filters unchanged (still status=="active"); contracts now mirror them.
    assert 'active_irs = [' in analytics
    assert 'active_ors = [' in analytics
    assert 'active_contracts = [' in analytics
    assert 'ir for ir in _irs' in analytics

    coverage_calc = COVERAGE_CALC_PY.read_text(encoding="utf-8")
    # The per-spec skip dimension wiring is intact.
    assert "skip_ir_coverage" in coverage_calc
    assert "skip_or_coverage" in coverage_calc
