"""R15A — boundary evidence stays data and fails closed."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import okto_pulse.core.application.boundary.community_boundary_evidence as r15a
from okto_pulse.core.application.boundary.community_boundary_evidence import (
    BoundaryEvidenceCheck,
    CommunityBoundaryEvidence,
    CommunityBoundaryEvidenceGate,
    CommunityBoundaryEvidenceGateInput,
    CommunityReferenceLedgerEntry,
    discover_community_references,
    run_phased_relational_hardening,
    validate_community_boundary_evidence,
    validate_community_reference_ledger,
)

SRC_ROOT = Path(r15a.__file__).resolve().parents[4]


def _valid_payload(now: datetime) -> dict:
    return {
        "schema_version": "1",
        "producer": "okto-pulse-community",
        "edition": "community",
        "generated_at": now.isoformat(),
        "max_age_seconds": 3600,
        "core_commit": "core-sha",
        "community_commit": "community-sha",
        "artifact_hash": "sha256:abc",
        "ledger_path": "reports/community-boundary-ledger.json",
        "checks": [
            {
                "name": "classification-ledger",
                "surface": "boundary",
                "status": "passed",
                "details": {"rows": 1},
            },
            {
                "name": "readiness-evidence",
                "surface": "readiness",
                "status": "passed",
                "details": {},
            },
        ],
    }


def test_ts_ad32f387_core_contract_imports_without_community_runtime(tmp_path):
    code = (
        "import sys\n"
        "from datetime import datetime, timezone\n"
        "import okto_pulse.core.application.boundary.community_boundary_evidence as m\n"
        "payload = {\n"
        "  'schema_version': '1', 'producer': 'okto-pulse-community',\n"
        "  'edition': 'community', 'generated_at': '2026-07-01T00:00:00+00:00',\n"
        "  'max_age_seconds': 3600, 'core_commit': 'c', 'community_commit': 'k',\n"
        "  'artifact_hash': 'h', 'ledger_path': 'ledger.json',\n"
        "  'checks': [{'name': 'string-ref', 'surface': 'boundary', 'status': 'passed'}],\n"
        "}\n"
        "clock = datetime(2026, 7, 1, 0, 10, tzinfo=timezone.utc)\n"
        "assert m.validate_community_boundary_evidence(payload, now=clock).ok\n"
        "leaked = [n for n in sys.modules if n == 'okto_pulse.community' or "
        "n.startswith('okto_pulse.community.')]\n"
        "assert leaked == [], leaked\n"
        "print('NO_COMMUNITY_IMPORT')\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=90,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "NO_COMMUNITY_IMPORT" in proc.stdout


def test_ts_c55f8007_and_ts_7f010e63_evidence_fails_closed():
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    report = validate_community_boundary_evidence(
        _valid_payload(now),
        now=now + timedelta(minutes=10),
        expected_core_commit="core-sha",
        expected_community_commit="community-sha",
        expected_artifact_hash="sha256:abc",
        required_surfaces=("boundary", "readiness"),
    )
    assert report.ok, report.as_dict()

    stale = _valid_payload(now - timedelta(hours=2))
    stale_report = validate_community_boundary_evidence(stale, now=now)
    assert not stale_report.ok
    assert {f.code for f in stale_report.findings} == {r15a.DIAG_EVIDENCE_STALE}

    malformed = _valid_payload(now)
    del malformed["schema_version"]
    malformed_report = validate_community_boundary_evidence(malformed, now=now)
    assert not malformed_report.ok
    assert r15a.DIAG_EVIDENCE_MALFORMED in {f.code for f in malformed_report.findings}

    failing = _valid_payload(now)
    failing["checks"][0]["status"] = "failed"
    failing_report = validate_community_boundary_evidence(failing, now=now)
    assert not failing_report.ok
    assert r15a.DIAG_EVIDENCE_FAILING in {f.code for f in failing_report.findings}

    mismatch = validate_community_boundary_evidence(
        _valid_payload(now),
        now=now,
        expected_artifact_hash="sha256:different",
    )
    assert not mismatch.ok
    assert r15a.DIAG_EVIDENCE_MISMATCH in {f.code for f in mismatch.findings}

    gate = CommunityBoundaryEvidenceGate().run(
        CommunityBoundaryEvidenceGateInput(
            payload=_valid_payload(now),
            now=now,
            expected_core_commit="core-sha",
            required_surfaces=("boundary", "readiness"),
        )
    )
    assert gate.status == "passed"
    bad_gate = CommunityBoundaryEvidenceGate().run(
        CommunityBoundaryEvidenceGateInput(payload=None, now=now)
    )
    assert bad_gate.status == "blocking"
    assert bad_gate.evidence["findings"][0]["code"] == r15a.DIAG_EVIDENCE_MISSING


def test_ts_5f17a0c9_ledger_completeness_blocks_missing_community_reference(tmp_path):
    src = tmp_path / "boundary_fixture.py"
    src.write_text(
        "EVIDENCE_REF = 'okto_pulse.community.adapters.boundary_evidence'\n",
        encoding="utf-8",
    )
    discovered = discover_community_references([src])
    assert len(discovered) == 1
    assert discovered[0].kind == "evidence_schema"

    missing = validate_community_reference_ledger(discovered=discovered, ledger_entries=())
    assert not missing.ok
    assert missing.findings[0].code == r15a.DIAG_LEDGER_MISSING_REFERENCE

    today = datetime(2026, 7, 1, tzinfo=timezone.utc)
    complete_entry = CommunityReferenceLedgerEntry(
        file=discovered[0].file,
        line=discovered[0].line,
        reference=discovered[0].reference,
        kind=discovered[0].kind,
        owner="okto-pulse-community/boundary-evidence",
        valid_until="2026-07-02T00:00:00+00:00",
        action="consume_as_data",
    )
    complete = validate_community_reference_ledger(
        discovered=discovered,
        ledger_entries=(complete_entry,),
        today=today,
    )
    assert complete.ok, complete.as_dict()

    no_owner = CommunityReferenceLedgerEntry(
        file=discovered[0].file,
        line=discovered[0].line,
        reference=discovered[0].reference,
        kind=discovered[0].kind,
    )
    owner_report = validate_community_reference_ledger(
        discovered=discovered,
        ledger_entries=(no_owner,),
        today=today,
    )
    assert not owner_report.ok
    assert owner_report.findings[0].code == r15a.DIAG_LEDGER_MISSING_OWNER_VALIDITY


def test_ts_ad32f387_runtime_import_in_ledger_is_blocking():
    entry = CommunityReferenceLedgerEntry(
        file="core/application/boundary/rogue.py",
        line=1,
        reference="okto_pulse.community.adapters.boundary_evidence",
        kind="runtime_import",
        owner="nobody",
        valid_until="2026-07-02T00:00:00+00:00",
    )
    report = validate_community_reference_ledger(
        discovered=(entry,),
        ledger_entries=(entry,),
        today=datetime(2026, 7, 1, tzinfo=timezone.utc),
    )
    assert not report.ok
    assert report.findings[0].code == r15a.DIAG_LEDGER_RUNTIME_IMPORT


def test_ts_c50c7ad3_phased_hardening_waits_for_r01_dependencies(tmp_path):
    rogue = tmp_path / "rogue.py"
    rogue.write_text(
        "from sqlalchemy.ext.asyncio import AsyncSession\n"
        "def f(session: AsyncSession): return session\n",
        encoding="utf-8",
    )

    before_r01 = run_phased_relational_hardening(
        root=tmp_path,
        completed_dependencies=("R01A", "R01B"),
    )
    assert not before_r01.ok
    assert before_r01.dependencies_missing == ("R01C",)
    assert before_r01.findings[0].code == r15a.DIAG_RELATIONAL_DEPENDENCIES_PENDING
    assert before_r01.relational_report is None

    after_r01 = run_phased_relational_hardening(
        root=tmp_path,
        completed_dependencies=("R01A", "R01B", "R01C"),
    )
    assert not after_r01.ok
    assert after_r01.dependencies_missing == ()
    assert after_r01.findings[0].code == r15a.DIAG_RELATIONAL_VIOLATION

    clean = tmp_path / "clean"
    clean.mkdir()
    (clean / "port_only.py").write_text("def f(port): return port\n", encoding="utf-8")
    clean_report = run_phased_relational_hardening(
        root=clean,
        completed_dependencies=("R01A", "R01B", "R01C"),
    )
    assert clean_report.ok, clean_report.as_dict()


def test_evidence_projection_is_stable_json_shape():
    ev = CommunityBoundaryEvidence(
        schema_version="1",
        producer="okto-pulse-community",
        edition="community",
        generated_at="2026-07-01T00:00:00+00:00",
        max_age_seconds=60,
        expires_at=None,
        core_commit="c",
        community_commit="k",
        artifact_hash="h",
        ledger_path="ledger.json",
        checks=(
            BoundaryEvidenceCheck(
                name="x",
                surface="boundary",
                status="passed",
                details={"ok": True},
            ),
        ),
    )
    assert ev.as_dict()["checks"] == [
        {"name": "x", "surface": "boundary", "status": "passed", "details": {"ok": True}}
    ]
