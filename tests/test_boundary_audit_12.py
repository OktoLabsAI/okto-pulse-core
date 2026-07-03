"""Spec #12 (card 19f982ec) — core-pure boundary audit suite.

Focused impl tests for the four gates + GateReport contract. The dedicated test
cards (23866493 / 7b095a87 / 8fc9f361 / 5b68dd36 / e65b107e / 49165d15) own the
full scenario matrix; these lock the implementation behaviour.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from okto_pulse.core.application.boundary import (
    DependencyAuditGate,
    DependencyAuditGateInput,
    GateException,
    GateReport,
    ImportBoundaryGate,
    ImportBoundaryGateInput,
    ImportSideEffectSmokeGate,
    ImportSideEffectSmokeInput,
    LayerResolver,
    PackageManifestGate,
    PackageManifestGateInput,
    enforce_exception_policy,
)

PURE_LAYERS = {"domain", "application", "ports", "future_target"}

AF11_ELIMINATED_APPLICATION_IMPORTS = (
    ("okto_pulse/core/application/use_cases/cognitive_readiness.py", 29, "okto_pulse.core.kg.cognitive_readiness", "kg"),
    ("okto_pulse/core/application/use_cases/cognitive_readiness.py", 30, "okto_pulse.core.kg.rebuild_audit", "kg"),
    ("okto_pulse/core/application/use_cases/cognitive_readiness.py", 82, "okto_pulse.core.kg.bug_cognitive_closure", "kg"),
    ("okto_pulse/core/application/use_cases/cognitive_readiness.py", 146, "okto_pulse.core.kg.cognitive_action_center", "kg"),
    ("okto_pulse/core/application/use_cases/consolidation.py", 44, "okto_pulse.core.kg.primitives", "kg"),
    ("okto_pulse/core/application/use_cases/consolidation.py", 72, "okto_pulse.core.kg.primitives", "kg"),
    ("okto_pulse/core/application/use_cases/consolidation.py", 105, "okto_pulse.core.kg.commit_coordinator", "kg"),
    ("okto_pulse/core/application/use_cases/consolidation.py", 106, "okto_pulse.core.kg.primitives", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 60, "okto_pulse.core.kg.dashboard_readers", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 107, "okto_pulse.core.kg.dashboard_readers", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 108, "okto_pulse.core.kg.kg_service", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 148, "okto_pulse.core.kg.governance", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 181, "okto_pulse.core.kg.governance", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 213, "okto_pulse.core.kg.governance", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 245, "okto_pulse.core.kg.governance", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 284, "okto_pulse.core.kg.dashboard_readers", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 317, "okto_pulse.core.kg.dashboard_readers", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 357, "okto_pulse.core.kg.governance", "kg"),
    ("okto_pulse/core/application/use_cases/kg_routes_crud.py", 410, "okto_pulse.core.kg.governance", "kg"),
    ("okto_pulse/core/application/use_cases/list_stale_canonical_parity.py", 15, "okto_pulse.core.kg.stale_canonical_parity", "kg"),
    ("okto_pulse/core/application/use_cases/mcp_kg_crud.py", 127, "okto_pulse.core.kg.canonical_partition_integrity", "kg"),
    ("okto_pulse/core/application/use_cases/mcp_kg_crud.py", 176, "okto_pulse.core.kg.global_discovery.layer_parity", "kg"),
    ("okto_pulse/core/application/use_cases/architecture_crud.py", 46, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/guidelines_crud.py", 43, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_card_crud.py", 490, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 368, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 472, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 571, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 753, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 907, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 1091, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 1213, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 1350, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 1526, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 1696, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_spec_crud.py", 2006, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/spec_crud.py", 23, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/spec_crud.py", 603, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/spec_crud.py", 678, "okto_pulse.core.models.schemas", "schemas"),
    ("okto_pulse/core/application/use_cases/mcp_ideation_crud.py", 594, "okto_pulse.core.infra.permissions", "permissions"),
    ("okto_pulse/core/application/use_cases/mcp_ideation_crud.py", 651, "okto_pulse.core.infra.permissions", "permissions"),
    ("okto_pulse/core/application/use_cases/mcp_ideation_crud.py", 719, "okto_pulse.core.infra.permissions", "permissions"),
    ("okto_pulse/core/application/use_cases/spec_crud.py", 350, "okto_pulse.core.infra.permissions", "permissions"),
    ("okto_pulse/core/application/use_cases/spec_crud.py", 790, "okto_pulse.core.infra.permissions", "permissions"),
    ("okto_pulse/core/application/use_cases/stories_crud.py", 80, "okto_pulse.core.infra.permissions", "permissions"),
    ("okto_pulse/core/application/use_cases/card_crud.py", 575, "sqlalchemy.orm.attributes", "sqlalchemy"),
    ("okto_pulse/core/application/use_cases/card_crud.py", 656, "sqlalchemy.orm.attributes", "sqlalchemy"),
    ("okto_pulse/core/application/use_cases/ideations_crud.py", 281, "sqlalchemy.orm.attributes", "sqlalchemy"),
    ("okto_pulse/core/application/use_cases/mcp_card_crud.py", 94, "sqlalchemy.orm.attributes", "sqlalchemy"),
    ("okto_pulse/core/application/use_cases/mcp_ideation_crud.py", 559, "sqlalchemy.orm.attributes", "sqlalchemy"),
)


@pytest.mark.parametrize(
    "path,expected",
    [
        ("src/okto_pulse/core/domain/x.py", "domain"),
        ("src/okto_pulse/core/application/use_cases/y.py", "application"),
        ("okto_pulse/core/app.py", "composition"),
        ("okto_pulse/core/services/main.py", "legacy_application_transitional_debt"),
        ("okto_pulse/core/commands/x.py", "legacy_application_transitional_debt"),
        ("okto_pulse/core/inbound/adapter.py", "inbound"),
        ("okto_pulse/core/api/router.py", "inbound"),
        ("okto_pulse/core/mcp/server.py", "inbound"),
        ("okto_pulse/core/repositories/board_repo.py", "outbound"),
        ("okto_pulse/core/infra/database.py", "outbound"),
        ("okto_pulse/core/kg/schema.py", "outbound"),
        ("okto_pulse/core/telemetry/store.py", "outbound"),
        ("okto_pulse/core/models/db.py", "outbound"),
        ("okto_pulse/core/events/bus.py", "event_runtime_messaging"),
        ("okto_pulse/core/ports/foo.py", "ports"),
        ("okto_pulse/tools/x.py", "packaging_ops_edition"),
        ("pyproject.toml", "packaging_ops_edition"),
        ("okto_pulse/weird/z.py", "unclassified"),
    ],
)
def test_layer_resolver_current_tree_v2(path: str, expected: str) -> None:
    assert LayerResolver().resolve(path).layer == expected


def test_layer_resolver_current_core_ports_is_not_future_governance() -> None:
    resolution = LayerResolver().resolve("okto_pulse/core/ports/foo.py")
    assert resolution.layer == "ports"
    assert resolution.requires_governance is False


def test_import_boundary_pure_layers_have_no_blocking_on_real_tree() -> None:
    report = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="blocking"))
    pure_blocking = [
        v
        for v in report.evidence["violations"]
        if v["status"] == "blocking" and v["layer"] in PURE_LAYERS
    ]
    assert pure_blocking == [], pure_blocking


def test_import_boundary_bootstrap_is_non_blocking_but_blocking_mode_enforces() -> None:
    boot = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="bootstrap"))
    enforced = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="blocking"))
    # bootstrap records known adapter debt as baseline/xfail_advisory, never blocking
    assert boot.status in ("passed", "baseline", "xfail_advisory")
    assert all(v["status"] != "blocking" for v in boot.evidence["violations"])
    # blocking mode enforces the same adapter coupling as blocking
    assert enforced.status == "blocking"
    assert any(v["status"] == "blocking" for v in enforced.evidence["violations"])


def test_af11_application_import_ratchet_real_tree_stays_zero() -> None:
    report = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="bootstrap"))
    blocking = [
        v
        for v in report.evidence["violations"]
        if v["status"] == "blocking" and v["layer"] in PURE_LAYERS
    ]
    assert report.observed_value == 0
    assert blocking == []

    eliminated = {(file, imported, category) for file, _line, imported, category in AF11_ELIMINATED_APPLICATION_IMPORTS}
    reintroduced = [
        {
            "file": v["file"],
            "line": v["line"],
            "imported": v["imported"],
            "category": v["category"],
            "rule": v["rule"],
        }
        for v in report.evidence["violations"]
        if (v["file"], v["imported"], v["category"]) in eliminated
    ]
    assert reintroduced == []


def test_af11_application_import_ratchet_negative_fixture_reports_categories(tmp_path: Path) -> None:
    use_cases = tmp_path / "okto_pulse" / "core" / "application" / "use_cases"
    use_cases.mkdir(parents=True)
    rogue = use_cases / "ratchet_regression.py"
    rogue.write_text(
        "from okto_pulse.core.kg.governance import start_historical_consolidation\n"
        "from okto_pulse.core.models.schemas import SpecUpdate\n"
        "from okto_pulse.core.infra.permissions import check_permission\n"
        "from sqlalchemy.orm.attributes import flag_modified\n",
        encoding="utf-8",
    )

    report = ImportBoundaryGate().run(
        ImportBoundaryGateInput(source_root=tmp_path, mode="bootstrap")
    )
    assert report.status == "blocking"
    assert report.observed_value == 4

    by_import = {v["imported"]: v for v in report.evidence["violations"]}
    expected = {
        "okto_pulse.core.kg.governance": ("kg", "forbidden_target_layer:outbound"),
        "okto_pulse.core.models.schemas": ("schemas", "forbidden_target_layer:outbound"),
        "okto_pulse.core.infra.permissions": ("permissions", "forbidden_target_layer:outbound"),
        "sqlalchemy.orm.attributes": ("sqlalchemy", "forbidden_import_root"),
    }
    assert set(expected) <= set(by_import)
    for imported, (category, rule) in expected.items():
        violation = by_import[imported]
        assert violation["file"] == "okto_pulse/core/application/use_cases/ratchet_regression.py"
        assert violation["line"] > 0
        assert violation["layer"] == "application"
        assert violation["status"] == "blocking"
        assert violation["category"] == category
        assert violation["rule"] == rule


def test_package_manifest_tools_loc_baseline_passes() -> None:
    report = PackageManifestGate().run(PackageManifestGateInput())
    assert report.evidence["tools_loc_expected"] == 146
    assert report.evidence["tools_loc_observed"] == 146
    assert report.status == "passed"
    assert report.evidence["force_includes"], "force-includes must be enumerated"


def test_package_manifest_loc_drift_blocks() -> None:
    report = PackageManifestGate().run(PackageManifestGateInput(tools_loc_baseline=999))
    assert report.status == "blocking"
    assert report.evidence["error"] == "tools_loc_drift"
    assert report.observed_value == 146
    assert report.expected_value == 999


def test_package_manifest_loc_drift_against_canonical_146_blocks(tmp_path) -> None:
    # ts_53684ed4: observed tools/ LOC != canonical 146 (no rebaseline/exception)
    # blocks as tools_loc_drift, preserving observed_value, expected_value=108,
    # owner, promotion_criteria and remediation_hint. Never the stale 85.
    tools = tmp_path / "okto_pulse" / "tools"
    tools.mkdir(parents=True)
    (tools / "drifted.py").write_text(
        "\n".join(f"x{i} = {i}" for i in range(50)), encoding="utf-8"
    )  # 50 lines, != 108
    repo_root = Path(__file__).resolve().parents[1]
    report = PackageManifestGate().run(
        PackageManifestGateInput(
            source_root=tmp_path,
            pyproject_path=repo_root / "pyproject.toml",
        )
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "tools_loc_drift"
    assert report.observed_value == 50
    assert report.expected_value == 146
    assert report.evidence["tools_loc_expected"] == 146
    assert report.evidence["tools_loc_expected"] != 85  # stale baseline is an error
    assert report.owner == "okto-pulse-core/architecture"
    assert report.promotion_criteria and report.remediation_hint


def test_dependency_audit_missing_policy_blocks() -> None:
    report = DependencyAuditGate().run(DependencyAuditGateInput())
    assert report.status == "blocking"
    assert report.evidence["error"] == "dependency_policy_missing"


def test_dependency_audit_forbidden_match_blocks() -> None:
    report = DependencyAuditGate().run(
        DependencyAuditGateInput(forbidden_dependencies=["fastapi"])
    )
    assert report.status == "blocking"
    assert "fastapi" in report.evidence["forbidden_matches"]


def test_gate_report_expired_exception_is_rejected() -> None:
    expired = GateException(
        owner="arch", justification="known debt", deadline="2020-01-01",
        promotion_criteria="migrate", status="active",
    )
    report = GateReport(gate_id="g", subject="s", status="baseline", exception=expired)
    out = enforce_exception_policy(report, today=date(2026, 6, 24))
    assert out.status == "reject"
    assert out.evidence["expired_exception"]["reason"] == "expired_exception"
    # owner/deadline/promotion_criteria preserved
    assert out.owner == "arch"
    assert out.exception is not None and out.exception.deadline == "2020-01-01"


def test_gate_report_status_expired_exception_rejects_with_full_governance() -> None:
    # ts_8dc314aa: exception.status=expired (independent of a future deadline) must
    # produce reject, preserving owner, deadline, promotion_criteria and
    # remediation_hint in the report.
    exc = GateException(
        owner="arch", justification="known debt", deadline="2099-01-01",
        promotion_criteria="migrate behind a port", status="expired",
    )
    report = GateReport(
        gate_id="import_boundary", subject="legacy debt", status="baseline",
        owner="arch", exception=exc, promotion_criteria="migrate behind a port",
        remediation_hint="depend on a port",
    )
    out = enforce_exception_policy(report, today=date(2026, 6, 24))
    assert out.status == "reject"
    assert out.evidence["expired_exception"]["reason"] == "expired_exception"
    assert out.owner == "arch"
    assert out.exception is exc
    assert out.exception.deadline == "2099-01-01"
    assert out.promotion_criteria == "migrate behind a port"
    assert out.remediation_hint  # preserved


def test_gate_report_active_exception_is_honoured() -> None:
    active = GateException(
        owner="arch", justification="known debt", deadline="2099-01-01",
        promotion_criteria="migrate",
    )
    report = GateReport(gate_id="g", subject="s", status="baseline", exception=active)
    out = enforce_exception_policy(report, today=date(2026, 6, 24))
    assert out.status == "baseline"


def test_side_effect_smoke_pure_module_starts_no_runtime() -> None:
    report = ImportSideEffectSmokeGate().run(
        ImportSideEffectSmokeInput(
            modules=[
                "okto_pulse.core.application.boundary.report",
                "okto_pulse.core.application.purity_gate",
            ],
            forbidden_side_effects=["ladybug", "apscheduler.schedulers"],
        )
    )
    assert report.status == "passed", report.evidence
    assert report.evidence["side_effects_detected"] == {}


# --------------------------------------------------------------------------- #
# rework (codex validation msg_72f4417): RECORD reading + 4-gate CLI
# --------------------------------------------------------------------------- #
def test_package_manifest_missing_record_blocks_not_silently_passes(tmp_path) -> None:
    report = PackageManifestGate().run(
        PackageManifestGateInput(wheel_record_path=tmp_path / "MISSING_RECORD")
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "manifest_not_found"
    assert "wheel_files" not in report.evidence or report.evidence.get("wheel_files") is None


def test_package_manifest_reads_record_and_reports_wheel_files(tmp_path) -> None:
    record = tmp_path / "RECORD"
    record.write_text(
        "okto_pulse/__init__.py,sha256=abc,12\n"
        "okto_pulse/core/app.py,sha256=def,3400\n"
        "okto_pulse_core-0.3.0.dist-info/METADATA,sha256=ghi,900\n",
        encoding="utf-8",
    )
    report = PackageManifestGate().run(PackageManifestGateInput(wheel_record_path=record))
    assert report.status == "passed", report.evidence
    assert report.evidence["wheel_record_audited"] is True
    assert "okto_pulse/core/app.py" in report.evidence["wheel_files"]


def test_package_manifest_unexpected_runtime_file_in_record_blocks(tmp_path) -> None:
    record = tmp_path / "RECORD"
    record.write_text(
        "okto_pulse/__init__.py,sha256=abc,12\n"
        "rogue_runtime.py,sha256=zzz,50\n",
        encoding="utf-8",
    )
    report = PackageManifestGate().run(PackageManifestGateInput(wheel_record_path=record))
    assert report.status == "blocking"
    assert report.evidence["error"] == "unexpected_runtime_file"
    assert "rogue_runtime.py" in report.observed_value


def test_dependency_audit_bootstrap_records_forbidden_as_baseline() -> None:
    # The current monolith declares fastapi/sqlalchemy; in bootstrap those are
    # transitional debt (baseline), not a hard block.
    report = DependencyAuditGate().run(
        DependencyAuditGateInput(
            forbidden_dependencies=["fastapi", "sqlalchemy"], mode="bootstrap"
        )
    )
    assert report.status == "baseline"
    assert report.evidence["error"] == "forbidden_dependency"
    assert report.promotion_criteria
    blocking = DependencyAuditGate().run(
        DependencyAuditGateInput(forbidden_dependencies=["fastapi"], mode="blocking")
    )
    assert blocking.status == "blocking"


def test_cli_audit_orchestrates_all_four_gates() -> None:
    from okto_pulse.core.application.boundary import cli

    reports = cli._run_audit(
        source_root=None,
        mode="bootstrap",
        wheel_record=None,
        dependency_policy=None,
        side_effect_modules=["okto_pulse.core.application.boundary.report"],
    )
    gate_ids = {r.gate_id for r in reports}
    assert gate_ids == {
        "import_boundary",
        "package_manifest",
        "dependency_audit",
        "import_side_effect_smoke",
    }


def test_cli_main_emits_json_with_four_gate_ids(capsys) -> None:
    import json as _json

    from okto_pulse.core.application.boundary import cli

    rc = cli.main(
        ["audit", "--format", "json", "--side-effect-module",
         "okto_pulse.core.application.boundary.report"]
    )
    out = _json.loads(capsys.readouterr().out)
    assert {r["gate_id"] for r in out} == {
        "import_boundary",
        "package_manifest",
        "dependency_audit",
        "import_side_effect_smoke",
    }
    assert rc in (0, 1)


def test_import_boundary_skips_type_checking_guarded_imports() -> None:
    import ast

    from okto_pulse.core.application.boundary.gates import ImportBoundaryGate
    from okto_pulse.core.application.boundary.import_matrix import rule_for

    source = (
        "from __future__ import annotations\n"
        "from typing import TYPE_CHECKING\n"
        "import fastapi\n"
        "if TYPE_CHECKING:\n"
        "    from sqlalchemy.ext.asyncio import AsyncSession\n"
    )
    tree = ast.parse(source)
    gate = ImportBoundaryGate()
    violations = gate._scan_file("okto_pulse/core/ports/x.py", "future_target", tree, rule_for("future_target"))
    imported = {v.imported for v in violations}
    assert "fastapi" in imported  # runtime import flagged
    assert not any("sqlalchemy" in i for i in imported)  # TYPE_CHECKING import skipped


def test_import_boundary_prohibited_import_reports_full_evidence() -> None:
    # ts_9c161104 negative: a prohibited import is reported with resolved layer,
    # file, import, owner, severity, remediation_hint and promotion_criteria.
    import ast

    from okto_pulse.core.application.boundary.gates import ImportBoundaryGate
    from okto_pulse.core.application.boundary.import_matrix import rule_for

    gate = ImportBoundaryGate()

    # legacy/transitional debt layer -> baseline WITH owner + promotion_criteria
    legacy = gate._scan_file(
        "okto_pulse/core/services/main.py",
        "legacy_application_transitional_debt",
        ast.parse("import fastapi\n"),
        rule_for("legacy_application_transitional_debt"),
    )
    assert legacy, "prohibited framework import in legacy layer must be reported"
    v = legacy[0]
    assert v.layer == "legacy_application_transitional_debt"
    assert v.file == "okto_pulse/core/services/main.py"
    assert "fastapi" in v.imported
    assert v.status == "baseline"
    assert v.owner and v.promotion_criteria and v.remediation_hint

    # pure layer -> blocking (must be clean immediately), with file/layer/import/hint
    pure = gate._scan_file(
        "okto_pulse/core/domain/x.py",
        "domain",
        ast.parse("import sqlalchemy\n"),
        rule_for("domain"),
    )
    assert pure and pure[0].status == "blocking"
    assert pure[0].layer == "domain"
    assert "sqlalchemy" in pure[0].imported
    assert pure[0].remediation_hint


def test_package_manifest_force_includes_and_baseline_not_stale_85() -> None:
    # ts_713873bb: package_manifest reports pyproject force-includes and the
    # canonical tools baseline of 146 LOC (never the stale 85).
    report = PackageManifestGate().run(PackageManifestGateInput())
    assert report.status == "passed"
    assert report.evidence["tools_loc_expected"] == 146
    assert report.evidence["tools_loc_expected"] != 85
    assert report.evidence["tools_loc_observed"] == 146
    # force-includes from pyproject [tool.hatch.build.targets.wheel.force-include]
    fi = report.evidence["force_includes"]
    assert fi and any("agent_instructions.md" in entry for entry in fi)
    assert report.evidence["tools_classification"] == "ops/edition"


def test_bootstrap_report_separates_baseline_and_xfail_from_blocking_with_governance() -> None:
    # ts_dc8191bf: in bootstrap the report separates baseline + xfail_advisory from
    # blocking, preserving owner/promotion_criteria/remediation_hint per item.
    report = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="bootstrap"))
    statuses = {v["status"] for v in report.evidence["violations"]}
    # known adapter/legacy debt is recorded as baseline, NEVER blocking in bootstrap
    assert "blocking" not in statuses
    assert "baseline" in statuses
    # unclassified package roots surface as xfail_advisory at the report level
    assert report.status in ("baseline", "xfail_advisory")
    assert report.evidence["unclassified_layers"]
    # every baseline finding keeps owner + promotion_criteria + remediation_hint
    for v in report.evidence["violations"]:
        if v["status"] == "baseline":
            assert v["owner"], v
            assert v["promotion_criteria"], v
            assert v["remediation_hint"], v


def test_bootstrap_baseline_exception_preserves_owner_and_promotion() -> None:
    # ts_dc8191bf: a baseline item with an ACTIVE exception keeps owner, exception,
    # promotion_criteria and remediation_hint (not promoted away).
    exc = GateException(
        owner="arch", justification="known decoupling debt", deadline="2099-01-01",
        promotion_criteria="migrate behind a port",
    )
    report = GateReport(
        gate_id="import_boundary", subject="legacy debt", status="baseline",
        owner="arch", exception=exc, promotion_criteria="migrate behind a port",
        remediation_hint="depend on a port",
    )
    out = enforce_exception_policy(report, today=date(2026, 6, 24))
    assert out.status == "baseline"  # active exception is honoured, stays baseline
    assert out.exception is exc
    assert out.owner == "arch"
    assert out.promotion_criteria == "migrate behind a port"
    assert out.remediation_hint == "depend on a port"


def test_dependency_audit_forbidden_dependency_reports_full_evidence() -> None:
    # ts_5cab7f4f: a forbidden runtime dependency blocks with dependency, owner,
    # severity and the offending matches; a missing catalog fails closed.
    report = DependencyAuditGate().run(
        DependencyAuditGateInput(forbidden_dependencies=["fastapi", "sqlalchemy"], mode="blocking")
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "forbidden_dependency"
    assert {"fastapi", "sqlalchemy"} <= set(report.evidence["forbidden_matches"])
    assert report.owner == "okto-pulse-core/architecture"
    assert report.severity in ("high", "critical")
    assert report.observed_value and report.remediation_hint
    # no policy at all -> dependency_policy_missing (never a silent approval)
    missing = DependencyAuditGate().run(DependencyAuditGateInput())
    assert missing.status == "blocking" and missing.evidence["error"] == "dependency_policy_missing"


def test_side_effect_smoke_detects_runtime_init_on_import(tmp_path) -> None:
    # ts_38016393: importing a module that starts a runtime thread (scheduler/worker
    # proxy) is detected as a side effect, never passed silently.
    mod = tmp_path / "sidefx_runtime.py"
    mod.write_text(
        "import threading\n"
        "_evt = threading.Event()\n"
        "threading.Thread(target=_evt.wait, name='rogue-worker', daemon=True).start()\n",
        encoding="utf-8",
    )
    report = ImportSideEffectSmokeGate().run(
        ImportSideEffectSmokeInput(
            modules=["sidefx_runtime"], extra_sys_path=(str(tmp_path),)
        )
    )
    assert report.status == "blocking", report.evidence
    assert report.evidence["error"] == "import_side_effect"
    assert "thread:rogue-worker" in report.evidence["side_effects_detected"]["sidefx_runtime"]


def test_side_effect_smoke_known_debt_is_baseline_not_silent(tmp_path) -> None:
    # ts_38016393: known accepted debt is recorded as baseline with owner +
    # promotion_criteria, NEVER a silent pass and never blocking.
    mod = tmp_path / "sidefx_known.py"
    mod.write_text(
        "import threading\n"
        "_evt = threading.Event()\n"
        "threading.Thread(target=_evt.wait, name='known-worker', daemon=True).start()\n",
        encoding="utf-8",
    )
    report = ImportSideEffectSmokeGate().run(
        ImportSideEffectSmokeInput(
            modules=["sidefx_known"],
            extra_sys_path=(str(tmp_path),),
            known_side_effect_debt=("sidefx_known",),
        )
    )
    assert report.status == "baseline", report.evidence
    assert report.owner == "okto-pulse-core/architecture"
    assert report.promotion_criteria
    assert "sidefx_known" in report.evidence["baseline_side_effect_debt"]
