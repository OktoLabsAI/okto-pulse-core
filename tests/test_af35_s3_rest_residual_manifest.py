"""AF35-S3 C1 REST residual manifest and conformance gate.

The gate is narrower than the core-wide relational baseline: it only governs the
REST ``core/api`` residue that S3 will migrate or intentionally defer before the
final S5 ledger.
"""

from __future__ import annotations

from okto_pulse.core.application.boundary.af35_s3_rest_residual_manifest import (
    AF35S3RestManifestEntry,
    AF35_S3_REST_MANIFEST_SCHEMA_VERSION,
    AF35_S3_REST_RESIDUAL_TAXONOMY,
    CLASS_ALLOWED_UOW_SEAM_API_DEPS,
    CLASS_COMMENT_OR_DOCSTRING,
    CLASS_DEFERRED_WITH_OWNER,
    CLASS_MIGRATED_CLEAN_TARGET,
    CLASS_NON_REST_FALSE_POSITIVE,
    CLASS_TEST_ONLY,
    PATTERN_DEPENDS_GET_DB,
    PATTERN_GET_DB_IMPORT,
    PATTERN_PRODUCTIVE_REST_RESIDUE,
    STATUS_EXCEEDED_MANIFEST_COUNT,
    STATUS_UNCLASSIFIED,
    build_af35_s3_rest_manifest,
    run_af35_s3_rest_residual_gate,
    validate_af35_s3_rest_manifest,
)


def _deferred_entry(file: str, pattern: str, allowed_count: int) -> AF35S3RestManifestEntry:
    return AF35S3RestManifestEntry(
        file=file,
        pattern=pattern,
        classification=CLASS_DEFERRED_WITH_OWNER,
        allowed_count=allowed_count,
        owner="af35-s3/test-owner",
        rationale="Synthetic deferred test residue.",
        evidence_ref="test fixture",
        retirement_criterion="Remove when the fixture is migrated.",
    )


def test_real_rest_manifest_classifies_current_productive_residue() -> None:
    report = run_af35_s3_rest_residual_gate()
    payload = report.as_dict()

    assert report.ok, payload
    assert report.scanned_files == 48
    assert len(report.findings) == 4
    assert payload["by_classification"] == {
        CLASS_ALLOWED_UOW_SEAM_API_DEPS: 4,
    }
    assert payload["by_pattern"] == {
        "async_session_annotation": 1,
        "async_session_import": 1,
        "depends_get_db": 1,
        "get_db_import": 1,
    }
    assert report.as_gate_report().status == "passed"


def test_manifest_has_closed_taxonomy_and_required_metadata() -> None:
    assert AF35_S3_REST_RESIDUAL_TAXONOMY == {
        CLASS_MIGRATED_CLEAN_TARGET,
        CLASS_DEFERRED_WITH_OWNER,
        CLASS_ALLOWED_UOW_SEAM_API_DEPS,
        CLASS_TEST_ONLY,
        CLASS_COMMENT_OR_DOCSTRING,
        CLASS_NON_REST_FALSE_POSITIVE,
    }
    manifest = build_af35_s3_rest_manifest()
    assert validate_af35_s3_rest_manifest(manifest) == ()
    assert len({(entry.file, entry.pattern) for entry in manifest}) == len(manifest)

    for entry in manifest:
        assert entry.file.startswith("api/")
        assert entry.classification in AF35_S3_REST_RESIDUAL_TAXONOMY
        if entry.classification == CLASS_MIGRATED_CLEAN_TARGET:
            assert entry.pattern == PATTERN_PRODUCTIVE_REST_RESIDUE
            assert entry.allowed_count == 0
        else:
            assert entry.owner
            assert entry.rationale
            assert entry.evidence_ref
            assert entry.retirement_criterion

    seam_files = {
        entry.file
        for entry in manifest
        if entry.classification == CLASS_ALLOWED_UOW_SEAM_API_DEPS
    }
    assert seam_files == {"api/deps.py"}


def test_s5_rows_are_machine_consumable_and_exact_for_non_clean_entries() -> None:
    report = run_af35_s3_rest_residual_gate()
    rows = report.s5_ledger_rows()

    assert rows
    assert all(row["schema_version"] == AF35_S3_REST_MANIFEST_SCHEMA_VERSION for row in rows)
    non_clean = [
        row for row in rows if row["classification"] != CLASS_MIGRATED_CLEAN_TARGET
    ]
    assert non_clean
    for row in non_clean:
        assert row["owner"]
        assert row["rationale"]
        assert row["evidence_ref"]
        assert row["retirement_criterion"]
        assert row["observed_count"] == row["allowed_count"]


def test_s5_rows_reject_missing_non_clean_metadata() -> None:
    manifest = (
        AF35S3RestManifestEntry(
            file="api/deferred.py",
            pattern=PATTERN_DEPENDS_GET_DB,
            classification=CLASS_DEFERRED_WITH_OWNER,
            allowed_count=1,
        ),
    )

    assert validate_af35_s3_rest_manifest(manifest) == (
        "manifest entry api/deferred.py:depends_get_db missing owner",
        "manifest entry api/deferred.py:depends_get_db missing rationale",
        "manifest entry api/deferred.py:depends_get_db missing evidence_ref",
        "manifest entry api/deferred.py:depends_get_db missing retirement_criterion",
    )


def test_migrated_clean_target_blocks_reintroduced_rest_coupling(tmp_path) -> None:
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

    report = run_af35_s3_rest_residual_gate(
        tmp_path, manifest=manifest, target_files=("api/clean.py",)
    )

    assert report.ok is False
    assert {finding.status for finding in report.blocking_findings} == {
        STATUS_EXCEEDED_MANIFEST_COUNT
    }
    assert {finding.classification for finding in report.blocking_findings} == {
        CLASS_MIGRATED_CLEAN_TARGET
    }


def test_unmanifested_rest_residue_fails_closed(tmp_path) -> None:
    api = tmp_path / "api"
    api.mkdir()
    (api / "unknown.py").write_text(
        "from fastapi import Depends\n"
        "from okto_pulse.core.infra.database import get_db\n"
        "def route(db=Depends(get_db)):\n"
        "    return {'ok': True}\n",
        encoding="utf-8",
    )

    report = run_af35_s3_rest_residual_gate(
        tmp_path, manifest=(), target_files=("api/unknown.py",)
    )

    assert report.ok is False
    assert "REST api file missing from S3 manifest: api/unknown.py" in report.manifest_errors
    assert {finding.status for finding in report.blocking_findings} == {
        STATUS_UNCLASSIFIED
    }


def test_count_gate_catches_extra_occurrence_beyond_manifest(tmp_path) -> None:
    api = tmp_path / "api"
    api.mkdir()
    (api / "deferred.py").write_text(
        "from fastapi import Depends\n"
        "from okto_pulse.core.infra.database import get_db\n"
        "def first(db=Depends(get_db)):\n"
        "    return db\n"
        "def second(db=Depends(get_db)):\n"
        "    return db\n",
        encoding="utf-8",
    )
    manifest = (
        _deferred_entry("api/deferred.py", PATTERN_GET_DB_IMPORT, 1),
        _deferred_entry("api/deferred.py", PATTERN_DEPENDS_GET_DB, 1),
    )

    report = run_af35_s3_rest_residual_gate(
        tmp_path, manifest=manifest, target_files=("api/deferred.py",)
    )

    assert report.ok is False
    assert len(report.blocking_findings) == 1
    blocker = report.blocking_findings[0]
    assert blocker.pattern == PATTERN_DEPENDS_GET_DB
    assert blocker.status == STATUS_EXCEEDED_MANIFEST_COUNT
    assert blocker.allowed_count == 1
    assert blocker.occurrence_index == 2


def test_stale_allowance_for_removed_residue_is_blocking(tmp_path) -> None:
    api = tmp_path / "api"
    api.mkdir()
    (api / "deferred.py").write_text(
        "from fastapi import Depends\n"
        "from okto_pulse.core.infra.database import get_db\n"
        "def only(db=Depends(get_db)):\n"
        "    return db\n",
        encoding="utf-8",
    )
    manifest = (
        _deferred_entry("api/deferred.py", PATTERN_GET_DB_IMPORT, 1),
        _deferred_entry("api/deferred.py", PATTERN_DEPENDS_GET_DB, 2),
    )

    report = run_af35_s3_rest_residual_gate(
        tmp_path, manifest=manifest, target_files=("api/deferred.py",)
    )

    assert report.ok is False
    assert report.blocking_findings == ()
    assert report.manifest_errors == (
        "stale manifest allowance for api/deferred.py:depends_get_db: "
        "allowed=2 observed=1",
    )
