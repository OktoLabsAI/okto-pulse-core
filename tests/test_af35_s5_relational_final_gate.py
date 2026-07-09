"""AF35-S5 final relational ownership gate."""

from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.af35_s3_rest_residual_manifest import (
    AF35S3RestManifestEntry,
    CLASS_MIGRATED_CLEAN_TARGET,
    PATTERN_PRODUCTIVE_REST_RESIDUE,
)
from okto_pulse.core.application.boundary.af35_s5_relational_final_gate import (
    AF35S5RelationalLedgerRow,
    CLASS_MIGRATED_CLEAN,
    CLASS_TEMPORARY_EXCEPTION,
    CLASS_UOW_SEAM,
    PATTERN_MCP_DIRECT_GET_DB_FOR_MCP,
    PATTERN_USE_CASE_DIRECT_RELATIONAL,
    build_af35_s5_relational_final_report,
    run_af35_s5_relational_final_gate,
    validate_af35_s5_relational_rows,
)

CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def test_af35_s5_final_report_passes_current_core_tree() -> None:
    report = run_af35_s5_relational_final_gate(CORE_ROOT)
    payload = report.as_dict()

    assert report.ok, payload
    assert payload["fail_closed"] is True
    assert payload["source_root"].endswith("src/okto_pulse/core")
    assert payload["unowned_findings"] == []
    assert payload["stale_ledger_entries"] == []
    assert payload["metadata_errors"] == []
    assert payload["upstream_errors"] == []

    assert report.counts_by_classification[CLASS_TEMPORARY_EXCEPTION] == 190
    assert report.counts_by_classification[CLASS_UOW_SEAM] == 4
    assert report.counts_by_pattern[PATTERN_MCP_DIRECT_GET_DB_FOR_MCP] == 39
    assert report.counts_by_pattern[PATTERN_USE_CASE_DIRECT_RELATIONAL] == 0

    gate_report = report.as_gate_report()
    assert gate_report.status == "passed"
    assert gate_report.observed_value == 0


def test_af35_s5_blocks_new_async_session_in_use_case(tmp_path: Path) -> None:
    use_cases = tmp_path / "application" / "use_cases"
    use_cases.mkdir(parents=True)
    (use_cases / "leaky.py").write_text(
        "from sqlalchemy.ext.asyncio import AsyncSession\n"
        "async def run(db: AsyncSession):\n"
        "    return db\n",
        encoding="utf-8",
    )

    report = run_af35_s5_relational_final_gate(
        tmp_path,
        use_cases_root=use_cases,
        include_s2=False,
        include_s3=False,
        include_mcp=False,
    )

    assert report.ok is False
    assert report.stale_ledger_entries == ()
    assert report.metadata_errors == ()
    assert any(
        finding.pattern == PATTERN_USE_CASE_DIRECT_RELATIONAL
        and finding.symbol == "AsyncSession"
        and finding.classification == "unowned"
        for finding in report.unowned_findings
    )
    assert report.as_gate_report().status == "blocking"


def test_af35_s5_blocks_reintroduced_rest_db_dependency(tmp_path: Path) -> None:
    api = tmp_path / "api"
    api.mkdir()
    (api / "clean.py").write_text(
        "from fastapi import Depends\n"
        "from okto_pulse.core.infra.database import get_db\n"
        "def route(db=Depends(get_db)):\n"
        "    return {'ok': True}\n",
        encoding="utf-8",
    )
    manifest = (
        AF35S3RestManifestEntry(
            file="api/clean.py",
            pattern=PATTERN_PRODUCTIVE_REST_RESIDUE,
            classification=CLASS_MIGRATED_CLEAN_TARGET,
            allowed_count=0,
        ),
    )

    report = run_af35_s5_relational_final_gate(
        tmp_path,
        rest_manifest=manifest,
        rest_target_files=("api/clean.py",),
        include_s2=False,
        include_mcp=False,
        include_use_cases=False,
    )

    assert report.ok is False
    assert any(
        finding.surface == "rest"
        and finding.file == "api/clean.py"
        and finding.classification == "unowned"
        for finding in report.unowned_findings
    )


def test_af35_s5_blocks_unledgered_mcp_direct_session_opener() -> None:
    mcp_source = (
        "async def okto_pulse_new_tool():\n"
        "    async with get_db_for_mcp() as db:\n"
        "        return db\n"
    )

    report = run_af35_s5_relational_final_gate(
        Path("."),
        mcp_source=mcp_source,
        mcp_ledger=(),
        include_s2=False,
        include_s3=False,
        include_use_cases=False,
    )

    assert report.ok is False
    assert report.metadata_errors == ()
    assert report.stale_ledger_entries == ()
    assert len(report.unowned_findings) == 1
    blocker = report.unowned_findings[0]
    assert blocker.file == "mcp/server.py::okto_pulse_new_tool"
    assert blocker.pattern == PATTERN_MCP_DIRECT_GET_DB_FOR_MCP
    assert blocker.symbol == "get_db_for_mcp"


def test_af35_s5_rejects_missing_metadata_for_non_clean_rows() -> None:
    row = AF35S5RelationalLedgerRow(
        source="test",
        surface="rest",
        file="api/deferred.py",
        pattern="depends_get_db",
        classification=CLASS_TEMPORARY_EXCEPTION,
        allowed_count=1,
        observed_count=1,
    )

    errors, stale = validate_af35_s5_relational_rows((row,))

    assert stale == ()
    assert errors == (
        "S5 ledger row api/deferred.py:depends_get_db missing evidence_ref",
        "S5 ledger row api/deferred.py:depends_get_db missing owner",
        "S5 ledger row api/deferred.py:depends_get_db missing public_surface",
        "S5 ledger row api/deferred.py:depends_get_db missing rationale",
        "S5 ledger row api/deferred.py:depends_get_db missing removal_criterion",
    )


def test_af35_s5_reports_stale_ledger_allowance() -> None:
    row = AF35S5RelationalLedgerRow(
        source="test",
        surface="mcp",
        file="mcp/server.py::old_tool",
        pattern=PATTERN_MCP_DIRECT_GET_DB_FOR_MCP,
        classification=CLASS_UOW_SEAM,
        allowed_count=2,
        observed_count=1,
        owner="af35-s5/test",
        rationale="Synthetic stale allowance.",
        public_surface="MCP tools/test",
        removal_criterion="Remove stale test row.",
        evidence_ref="test fixture",
    )

    report = build_af35_s5_relational_final_report(
        source_root="test",
        rows=(row,),
    )

    assert report.ok is False
    assert report.unowned_findings == ()
    assert report.metadata_errors == ()
    assert len(report.stale_ledger_entries) == 1
    assert report.stale_ledger_entries[0].file == "mcp/server.py::old_tool"
    assert report.as_gate_report().status == "blocking"


def test_af35_s5_rejects_unknown_classification() -> None:
    row = AF35S5RelationalLedgerRow(
        source="test",
        surface="rest",
        file="api/a.py",
        pattern="x",
        classification="not_in_taxonomy",
        allowed_count=0,
        observed_count=0,
    )

    errors, _stale = validate_af35_s5_relational_rows((row,))

    assert errors == ("unknown S5 classification for api/a.py:x: not_in_taxonomy",)
    assert CLASS_MIGRATED_CLEAN != "not_in_taxonomy"
