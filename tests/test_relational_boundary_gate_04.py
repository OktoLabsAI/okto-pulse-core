"""Spec #04 card b37786d9 — RelationalBoundaryGate (tr_4c0d19ed / or_9940a072).

Proves the gate (1) passes on the real migrated use cases (no direct relational
coupling), (2) flags every relational symbol with the correct surface bucket,
(3) covers get_db_for_mcp explicitly (the MCP surface, the validator's key
finding), (4) produces the by-surface metric, and (5) reports the core-wide
baseline as non-blocking transitional debt.
"""

from __future__ import annotations

from okto_pulse.core.repositories import (
    METRIC_RELATIONAL_BOUNDARY_VIOLATIONS,
    RELATIONAL_BASELINE,
    RELATIONAL_BASELINE_R01B,
    RELATIONAL_COVERAGE_BASELINE,
    observe_relational_boundary_violations,
    relational_baseline_report,
    relational_coverage_drift,
    run_relational_boundary_gate,
)

_LEAKY_USE_CASE = (
    "from sqlalchemy.ext.asyncio import AsyncSession\n"
    "from sqlalchemy import select\n"
    "from fastapi import Depends\n"
    "from okto_pulse.core.infra.database import get_db, get_db_for_mcp\n"
    "from okto_pulse.core.models.db import Board\n"
    "\n"
    "async def handler(db: AsyncSession = Depends(get_db)):\n"
    "    rows = select(Board)\n"
    "    return rows\n"
    "\n"
    "async def mcp_handler():\n"
    "    async with get_db_for_mcp() as db:\n"
    "        return db\n"
)


def test_gate_clean_on_real_migrated_use_cases():
    report = run_relational_boundary_gate()
    assert report.ok, report.as_dict()
    assert report.scanned_files >= 4
    assert report.violations_by_surface == {}


def test_gate_flags_every_relational_symbol_with_surface(tmp_path):
    (tmp_path / "leaky_use_case.py").write_text(_LEAKY_USE_CASE, encoding="utf-8")
    report = run_relational_boundary_gate(tmp_path)
    assert report.ok is False
    symbols = {v.symbol for v in report.violations}
    assert "AsyncSession" in symbols
    assert "select" in symbols
    assert "get_db" in symbols
    assert "get_db_for_mcp" in symbols
    assert any(s.startswith("Depends(get_db)") for s in symbols)
    assert any(s == "orm:Board" for s in symbols)
    # Surface buckets: REST (Depends(get_db)/get_db), MCP (get_db_for_mcp),
    # service (AsyncSession/select/ORM).
    assert report.violations_by_surface.get("rest", 0) >= 1
    assert report.violations_by_surface.get("mcp", 0) >= 1
    assert report.violations_by_surface.get("service", 0) >= 1
    for v in report.violations:
        assert v.severity == "blocking"
        assert v.remediation_hint
        assert v.file and v.line >= 1


def test_gate_covers_get_db_for_mcp_explicitly(tmp_path):
    # The validator's key finding: the MCP surface must not escape the strangler.
    (tmp_path / "mcp_only.py").write_text(
        "from okto_pulse.core.infra.database import get_db_for_mcp\n"
        "async def tool():\n"
        "    async with get_db_for_mcp() as db:\n"
        "        return db\n",
        encoding="utf-8",
    )
    report = run_relational_boundary_gate(tmp_path)
    assert report.ok is False
    mcp_violations = [v for v in report.violations if v.surface == "mcp"]
    assert mcp_violations
    assert any(v.symbol == "get_db_for_mcp" for v in mcp_violations)


def test_observe_metric_buckets_by_surface(tmp_path):
    (tmp_path / "leaky_use_case.py").write_text(_LEAKY_USE_CASE, encoding="utf-8")
    report = run_relational_boundary_gate(tmp_path)
    metric = observe_relational_boundary_violations(report)
    assert metric["metric"] == METRIC_RELATIONAL_BOUNDARY_VIOLATIONS
    assert metric["metric"] == "okto_pulse_relational_boundary_violations_total"
    assert set(metric["by_surface"]) == {"rest", "mcp", "service"}
    assert metric["by_surface"]["mcp"] >= 1
    assert metric["blocking_total"] == len(report.violations)


def test_baseline_report_is_non_blocking_debt():
    report = relational_baseline_report()
    assert report["blocking"] is False
    assert report["classification"] == "transitional_debt_out_of_scope"
    assert report["owner"] and report["promotion_criteria"]
    # Documented baseline keys (ac_cddd871d).
    assert set(report["documented_baseline"]) == {
        "depends_get_db",
        "get_db_for_mcp",
        "async_session",
        "orm_base_classes",
        "migrate_refs",
    }
    assert report["documented_baseline"] == RELATIONAL_BASELINE
    # Live recount present. Post the R01A REST+MCP CRUD strangler + the R01B
    # ownership inversion, the core-wide Depends(get_db)/get_db_for_mcp/
    # AsyncSession counts SHRANK from the spec #04 documented baseline
    # (268/225/554) to the R01B drawn-down baseline — a NEW measurement taken
    # post-TR5/R01B that reconciles the stale >=200/>=200/>=500 floors which
    # predated the strangler. The report stays non-blocking debt.
    live = report["live_counts"]
    assert live["orm_base_classes"] == 59
    assert report["r01b_baseline"] == RELATIONAL_BASELINE_R01B
    for key in ("depends_get_db", "get_db_for_mcp", "async_session"):
        # the frozen drawn-down baseline matches the live recount...
        assert live[key] == RELATIONAL_BASELINE_R01B[key]
        # ...and is strictly below the spec #04 documented baseline (it shrank).
        assert live[key] < RELATIONAL_BASELINE[key]


def test_gate_catches_orm_module_alias_bypasses(tmp_path):
    # Adversarial: the ORM module reached via aliases — the reported blocker class.
    (tmp_path / "via_import_as.py").write_text(
        "import okto_pulse.core.models.db as orm\n"
        "def f():\n    return orm.Board\n",
        encoding="utf-8",
    )
    (tmp_path / "via_from_import_sub.py").write_text(
        "from okto_pulse.core.models import db\n"
        "def f():\n    return db.Spec\n",
        encoding="utf-8",
    )
    (tmp_path / "via_no_alias_chain.py").write_text(
        "import okto_pulse.core.models.db\n"
        "def f():\n    return okto_pulse.core.models.db.Card\n",
        encoding="utf-8",
    )
    report = run_relational_boundary_gate(tmp_path)
    assert report.ok is False
    symbols = {v.symbol for v in report.violations}
    assert "orm:Board" in symbols
    assert "orm:Spec" in symbols
    assert "orm:Card" in symbols
    assert all(
        v.surface == "service" for v in report.violations if v.symbol.startswith("orm:")
    )


def test_gate_catches_aliased_relational_module_bypasses(tmp_path):
    # Same bypass class for the other modules — must not regress on those either.
    (tmp_path / "sa_alias.py").write_text(
        "import sqlalchemy as sa\n"
        "def f():\n    return sa.select(1)\n",
        encoding="utf-8",
    )
    (tmp_path / "dbmod_alias.py").write_text(
        "import okto_pulse.core.infra.database as dbmod\n"
        "async def f():\n    async with dbmod.get_db_for_mcp() as s:\n        return s\n",
        encoding="utf-8",
    )
    report = run_relational_boundary_gate(tmp_path)
    assert report.ok is False
    surfaces = {(v.symbol, v.surface) for v in report.violations}
    assert ("select", "service") in surfaces
    assert ("get_db_for_mcp", "mcp") in surfaces


# ---------------------------------------------------------------------------
# AC5 (ac_28f50f9d, R01B IMP2) — frozen relational coverage baseline.
# ---------------------------------------------------------------------------

# The exact frozen snapshot shape the drift checker is fed in the unit tests
# below (the 3 aggregates + the per-surface breakdown of classified call-sites).
_SNAP = {
    "relational_imports": 371,
    "relational_symbols": 851,
    "classified_call_sites": 1429,
    "by_surface": {"rest": 163, "service": 1149, "mcp": 117},
}


def test_relational_coverage_frozen_snapshot_clears_floors_and_no_drift():
    # The real core tree must (1) clear the AC5 "ao menos" floors and (2) match
    # the frozen R01B live snapshot exactly — zero drift.
    report = relational_baseline_report()
    assert report["coverage_floor"] == RELATIONAL_COVERAGE_BASELINE
    cov = report["coverage_counts"]
    for aggregate, floor in RELATIONAL_COVERAGE_BASELINE.items():
        assert cov[aggregate] >= floor, (aggregate, cov[aggregate], floor)
    assert report["coverage_floor_ok"] is True
    snap = report["coverage_snapshot_r01b"]
    for aggregate in ("relational_imports", "relational_symbols", "classified_call_sites"):
        assert cov[aggregate] == snap[aggregate], (aggregate, cov[aggregate], snap[aggregate])
    assert cov["by_surface"] == snap["by_surface"]
    drift = report["coverage_drift"]
    assert drift["ok"] is True, drift
    assert drift["undeclared_drift"] == []
    assert drift["floor_violations"] == []


def test_relational_coverage_undeclared_drift_fails():
    # (a) a DROP with no declaration is undeclared drift — even while the
    #     aggregate is still ABOVE its floor (800 >= 605).
    live_drop = {
        **_SNAP,
        "relational_symbols": _SNAP["relational_symbols"] - 41,
        "by_surface": dict(_SNAP["by_surface"]),
    }
    verdict = relational_coverage_drift(live_drop, snapshot=_SNAP)
    assert verdict["ok"] is False
    assert verdict["floor_violations"] == []  # still above the 605 floor
    assert any(
        d["counter"] == "relational_symbols" and d["delta"] == -41
        for d in verdict["undeclared_drift"]
    )

    # (b) a RISE is NEW coupling — a removal can never raise a count — so it is
    #     always undeclared drift, even when a declaration is (wrongly) supplied.
    live_rise = {
        **_SNAP,
        "classified_call_sites": _SNAP["classified_call_sites"] + 10,
        "by_surface": {
            **_SNAP["by_surface"],
            "service": _SNAP["by_surface"]["service"] + 10,
        },
    }
    verdict2 = relational_coverage_drift(
        live_rise,
        snapshot=_SNAP,
        declared_removals={"classified_call_sites": {"count": 999, "task": "R01C-x"}},
    )
    assert verdict2["ok"] is False
    assert any(
        d["counter"] == "classified_call_sites" and d["delta"] == 10
        for d in verdict2["undeclared_drift"]
    )


def test_relational_coverage_declared_r01_drawdown_passes():
    # An R01C strangle removes 100 service call-sites: a DECLARED drawdown that
    # still clears the floor (1322 >= 276) passes the gate.
    live = {
        **_SNAP,
        "classified_call_sites": _SNAP["classified_call_sites"] - 100,
        "by_surface": {
            **_SNAP["by_surface"],
            "service": _SNAP["by_surface"]["service"] - 100,
        },
    }
    declared = {
        "classified_call_sites": {"count": 100, "task": "R01C-strangle-service"},
        "callsites_service": {"count": 100, "task": "R01C-strangle-service"},
    }
    verdict = relational_coverage_drift(live, snapshot=_SNAP, declared_removals=declared)
    assert verdict["ok"] is True, verdict
    assert verdict["undeclared_drift"] == []
    assert {d["counter"] for d in verdict["declared_removals"]} == {
        "classified_call_sites",
        "callsites_service",
    }
    assert verdict["floor_violations"] == []

    # A declaration that does NOT cover the full shortfall is still undeclared.
    short = {
        "classified_call_sites": {"count": 10, "task": "R01C-strangle-service"},
        "callsites_service": {"count": 10, "task": "R01C-strangle-service"},
    }
    assert relational_coverage_drift(live, snapshot=_SNAP, declared_removals=short)["ok"] is False

    # A non-R01 task does not authorise the drawdown.
    wrong = {
        "classified_call_sites": {"count": 100, "task": "FOO-1"},
        "callsites_service": {"count": 100, "task": "FOO-1"},
    }
    assert relational_coverage_drift(live, snapshot=_SNAP, declared_removals=wrong)["ok"] is False


def test_relational_coverage_below_floor_fails_even_when_declared():
    # The floor is a HARD minimum: dropping an aggregate below it fails the gate
    # regardless of a structurally-valid R01 declaration covering the snapshot
    # drift (Codex AC5 ruling: "exigir live >= floors" is independent).
    live = {
        "relational_imports": 384,
        "relational_symbols": 600,  # below the 605 floor
        "classified_call_sites": 1422,
        "by_surface": dict(_SNAP["by_surface"]),
    }
    declared = {"relational_symbols": {"count": 241, "task": "R01C-x"}}
    verdict = relational_coverage_drift(live, snapshot=_SNAP, declared_removals=declared)
    assert verdict["ok"] is False
    assert any(fv["aggregate"] == "relational_symbols" for fv in verdict["floor_violations"])
