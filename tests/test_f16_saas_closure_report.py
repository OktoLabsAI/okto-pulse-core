from __future__ import annotations

import json
from pathlib import Path

from okto_pulse.core.application.boundary.saas_closure_report import (
    SaaSClosureReport,
    TransitionalBudget,
    audit_core_import_ownership,
    build_transitional_budgets,
    render_saas_closure_readme,
    validate_saas_closure_readmes,
)


def _synthetic_core(tmp_path: Path, source: str) -> Path:
    repo = tmp_path / "core-repo"
    package = repo / "src" / "okto_pulse" / "core"
    package.mkdir(parents=True)
    (package / "sample.py").write_text(source, encoding="utf-8")
    return repo


def _clean_report() -> SaaSClosureReport:
    budgets = tuple(
        TransitionalBudget(
            key=key,
            current=0,
            limit=0,
            implementation_card_id="card",
            removal_criterion="already removed",
        )
        for key in ("imports", "singletons")
    )
    return SaaSClosureReport(
        version="test",
        rows=(),
        budgets=budgets,
        findings=(),
        evidence={
            "core_import_rows": 1,
            "community_to_core_import_rows": 2,
            "dependency_rows": 3,
        },
    )


def test_f16_injected_community_import_is_blocking(tmp_path: Path) -> None:
    repo = _synthetic_core(
        tmp_path,
        "from okto_pulse.community.adapters import storage\n",
    )
    rows = audit_core_import_ownership(repo, dependency_ledger=())
    assert len(rows) == 1
    assert rows[0].classification == "edition_implementation_reach_in"
    assert rows[0].severity == "blocking"


def test_f16_injected_undeclared_dependency_is_blocking(tmp_path: Path) -> None:
    repo = _synthetic_core(tmp_path, "import undeclared_runtime\n")
    rows = audit_core_import_ownership(repo, dependency_ledger=())
    assert len(rows) == 1
    assert rows[0].classification == "unowned_external_import"
    assert rows[0].target_owner == "unowned"
    assert rows[0].severity == "blocking"


def test_f16_every_ownership_row_is_fail_closed_and_located(tmp_path: Path) -> None:
    repo = _synthetic_core(tmp_path, "import json\nfrom . import sibling\n")
    rows = audit_core_import_ownership(repo, dependency_ledger=())
    assert len(rows) == 2
    assert all(row.target_owner and row.classification for row in rows)
    assert all(row.severity in {"info", "warning", "blocking"} for row in rows)
    assert all(":" in row.location for row in rows)


def test_f16_private_reach_in_growth_has_zero_budget() -> None:
    budgets = build_transitional_budgets(
        community_import_report={"occurrence_count": 1},
        community_provenance_report={"bridge_count": 0},
        af35_residue_count=0,
    )
    budget = next(item for item in budgets if item.key == "community_private_reach_ins")
    assert budget.current == 1
    assert budget.limit == 0
    assert budget.ok is False


def test_f16_all_active_repository_budgets_are_zero() -> None:
    budgets = build_transitional_budgets(
        community_import_report={"occurrence_count": 0},
        community_provenance_report={"bridge_count": 0},
        af35_residue_count=0,
    )
    assert budgets
    assert all(item.current == item.limit == 0 for item in budgets)
    assert all(item.ok for item in budgets)


def test_f16_readme_projection_is_exact_and_drift_fails() -> None:
    report = _clean_report()
    expected = render_saas_closure_readme(report)
    assert (
        validate_saas_closure_readmes(
            report,
            core_readme=expected,
            community_readme=expected,
        )
        == ()
    )

    findings = validate_saas_closure_readmes(
        report,
        core_readme=expected.replace("| 1 |", "| 99 |"),
        community_readme=expected,
    )
    assert [item.code for item in findings] == ["readme_closure_matrix_mismatch"]


def test_f16_json_report_is_deterministic_and_machine_readable() -> None:
    report = _clean_report()
    first = report.to_json()
    second = report.to_json()
    assert first == second
    assert json.loads(first)["ok"] is True
