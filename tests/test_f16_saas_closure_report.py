from __future__ import annotations

import json
from pathlib import Path

import pytest

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


@pytest.mark.parametrize("source", [
    "from importlib.metadata import version\n",
    "import importlib.metadata as metadata\n",
    "from importlib import metadata\n",
])
@pytest.mark.parametrize("surface", ["ports", "infra", "services", "domain", "application/use_cases"])
def test_f16_metadata_discovery_in_runtime_policy_is_blocking(tmp_path, source, surface):
    repo = _synthetic_core(tmp_path, "")
    package = repo / "src/okto_pulse/core" / surface
    package.mkdir(parents=True)
    (package / "version.py").write_text(source, encoding="utf-8")
    rows = audit_core_import_ownership(repo)
    assert len(rows) == 1
    assert rows[0].classification == "edition_metadata_discovery_in_core"
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


@pytest.mark.timeout(300)
def test_f16_zero_compatibility_budget_cannot_hide_backend_surface_violation(tmp_path, monkeypatch):
    from okto_pulse.core.application.boundary.graph_runtime_surface_gate import (
        GraphRuntimeSurfaceGate, GraphRuntimeSurfaceGateInput,
    )
    from okto_pulse.core.application.boundary.saas_closure_report import build_saas_closure_report

    injected = _synthetic_core(tmp_path, 'def lookup(row):\n    return row["kuzu_node_id"]\n')
    observed = GraphRuntimeSurfaceGate().run(GraphRuntimeSurfaceGateInput(source_root=injected / "src"))
    assert observed.status == "blocking"
    assert observed.evidence["compatibility_ledger"] == []
    monkeypatch.setattr(GraphRuntimeSurfaceGate, "run", lambda self, data: observed)
    core = Path(__file__).resolve().parents[1]
    # Also runs in Core's isolated-wheel CI, where no edition checkout exists.
    # Other missing ownership findings are expected in this negative fixture;
    # the assertions specifically require propagation of the graph violation.
    community = tmp_path / "community-metadata"
    community.mkdir()
    (community / "pyproject.toml").write_text('[project]\nname="okto-pulse"\nversion="0.3.4"\ndependencies=[]\n', encoding="utf-8")
    (community / "uv.lock").write_text('version=1\n[[package]]\nname="okto-pulse"\nversion="0.3.4"\n', encoding="utf-8")
    report = build_saas_closure_report(core_repo=core, community_repo=community,
        community_import_report={}, community_provenance_report={})
    assert not report.ok
    assert any(item.code == "graph_runtime_surface_not_terminal" for item in report.findings)
    assert any(item.code == "graph_runtime_surface_violation" and "sample.py" in item.location
        for item in report.findings)
    budget = next(item for item in report.budgets if item.key == "graph_runtime_compatibility")
    assert budget.current == budget.limit == 0
