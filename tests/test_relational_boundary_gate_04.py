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
    observe_relational_boundary_violations,
    relational_baseline_report,
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
    # Live recount present and in the right ballpark (baseline is debt, not a
    # blind target — exact drift is expected and allowed).
    live = report["live_counts"]
    assert live["orm_base_classes"] == 59
    assert live["depends_get_db"] >= 200
    assert live["get_db_for_mcp"] >= 200
    assert live["async_session"] >= 500


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
