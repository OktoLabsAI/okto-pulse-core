from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.af35_s2_relational_residue_gate import (
    LEDGER_STATUS_UNLEDGERED,
    run_af35_s2_relational_residue_gate,
    validate_af35_s2_residue_ledger,
)


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _write(root: Path, rel: str, source: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_af35_s2_real_core_tree_green_with_explicit_ledger() -> None:
    report = run_af35_s2_relational_residue_gate(core_root=CORE_ROOT)

    assert validate_af35_s2_residue_ledger() == ()
    assert report.ok, report.as_dict()
    assert report.unledgered_findings == ()

    files_with_residue = {finding.file for finding in report.findings}
    assert "kg/governance.py" in files_with_residue
    assert "kg/workers/consolidation.py" in files_with_residue
    for migrated_file in {
        "kg/dashboard_readers.py",
        "kg/health.py",
        "kg/cognitive_readiness.py",
        "kg/cognitive_action_center.py",
        "kg/workers/commit_events.py",
        "kg/workers/dead_letter.py",
    }:
        assert migrated_file not in files_with_residue

    gate_report = report.as_gate_report()
    assert gate_report.status == "passed"
    assert gate_report.observed_value == 0


def test_af35_s2_new_residue_in_clean_reader_fails_closed(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "kg/health.py",
        "from sqlalchemy import select\n"
        "async def read(db):\n"
        "    return await db.execute(select(object))\n",
    )

    report = run_af35_s2_relational_residue_gate(core_root=tmp_path)

    assert report.ok is False
    unledgered = [finding.as_dict() for finding in report.unledgered_findings]
    assert any(
        finding["file"] == "kg/health.py"
        and finding["pattern"] == "select_import"
        and finding["ledger_status"] == LEDGER_STATUS_UNLEDGERED
        for finding in unledgered
    )
    assert any(
        finding["file"] == "kg/health.py"
        and finding["pattern"] == "session_execute_call"
        and finding["symbol"] == "db.execute"
        and finding["line"] == 3
        and finding["ledger_status"] == LEDGER_STATUS_UNLEDGERED
        for finding in unledgered
    )

    gate_report = report.as_gate_report()
    assert gate_report.status == "blocking"
    assert gate_report.observed_value == len(report.unledgered_findings)


def test_af35_s2_extra_residue_in_ledgered_file_exceeds_allowed_count(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "kg/governance.py",
        "def drift():\n" + "".join("    select(object)\n" for _ in range(19)),
    )

    report = run_af35_s2_relational_residue_gate(core_root=tmp_path)

    select_overflow = [
        finding
        for finding in report.unledgered_findings
        if finding.file == "kg/governance.py" and finding.pattern == "select_call"
    ]
    assert len(select_overflow) == 1
    assert select_overflow[0].occurrence_index == 19
    assert select_overflow[0].allowed_count == 18
    assert select_overflow[0].ledger_status == LEDGER_STATUS_UNLEDGERED


def test_af35_s2_alias_bypasses_are_detected(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "kg/cognitive_action_center.py",
        "import sqlalchemy as sa\n"
        "import sqlalchemy.ext.asyncio as sql_asyncio\n"
        "from sqlalchemy.orm import attributes as orm_attributes\n"
        "async def read(db: sql_asyncio.AsyncSession):\n"
        "    orm_attributes.flag_modified(object(), 'settings')\n"
        "    return await db.execute(sa.select(object))\n",
    )

    report = run_af35_s2_relational_residue_gate(core_root=tmp_path)
    patterns = {finding.pattern for finding in report.unledgered_findings}

    assert report.ok is False
    assert {
        "async_session_annotation",
        "flag_modified_call",
        "select_call",
        "session_execute_call",
    } <= patterns
