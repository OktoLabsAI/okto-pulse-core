"""R02 REPLAN-IMP2 — storage-bypass gate (FR4/AC4) + StorageProvider ledger (FR3).

Pure (no DB / no HTTP) teeth for:
  - AC4 (ts_dc951024): the gate FAILS when ``FileResponse(path=...)`` is
    reintroduced in a core API endpoint; the StorageProvider variant passes.
  - AC4 (ts_ff4e1d84): the gate FAILS on direct ``open()`` / ``pathlib`` IO /
    ``shutil`` copy in a core API endpoint (incl. aliased imports).
  - FR3 (ts_a44f0d14): the StorageProvider readiness ledger stays not-ready when
    any of upload / download_stream / delete (or the bypass gate) is unproven,
    citing the missing op; complete evidence makes the port ready.
"""

from __future__ import annotations

import textwrap

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    STORAGE_PROVIDER_ORACLE_OPS,
    StorageProviderReadinessEvidence,
    evaluate_storage_provider_readiness,
)
from okto_pulse.core.application.boundary.storage_bypass_gate import (
    KIND_FILE_RESPONSE,
    KIND_OPEN,
    KIND_PATHLIB_IO,
    KIND_SHUTIL_IO,
    STORAGE_BYPASS_ALLOWLIST,
    STORAGE_BYPASS_OCCURRENCE_ALLOWLIST,
    run_storage_bypass_gate,
    storage_bypass_allowlist_only_shrinks,
    storage_bypass_occurrence_allowlist_only_shrinks,
)


def _write(tmp_path, name: str, body: str):
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return p


# --------------------------------------------------------------------------- AC4
def test_gate_green_on_real_core_api():
    """The real core/api tree has no filesystem bypass after IMP1 (the gate's
    steady-state must be green, else the teeth would always fire)."""
    report = run_storage_bypass_gate()
    assert report.ok, [v.as_dict() for v in report.violations]
    assert report.violations == []
    assert report.scanned_files > 0


def test_gate_flags_reintroduced_fileresponse_path(tmp_path):  # ts_dc951024
    _write(
        tmp_path,
        "attachments.py",
        """
        from starlette.responses import FileResponse

        def download_attachment(attachment):
            return FileResponse(path=attachment.path)
        """,
    )
    report = run_storage_bypass_gate(tmp_path)
    assert not report.ok
    assert KIND_FILE_RESPONSE in {v.kind for v in report.violations}
    fr = next(v for v in report.violations if v.kind == KIND_FILE_RESPONSE)
    assert fr.symbol == "FileResponse"
    assert fr.file.endswith("attachments.py") and fr.line > 0


def test_gate_flags_open_pathlib_shutil(tmp_path):  # ts_ff4e1d84
    _write(
        tmp_path,
        "resource.py",
        """
        import shutil
        from pathlib import Path

        def serve_resource(src, dst):
            raw = open(src, "rb").read()
            blob = Path(src).read_bytes()
            shutil.copyfile(src, dst)
            return raw, blob
        """,
    )
    report = run_storage_bypass_gate(tmp_path)
    assert not report.ok
    kinds = {v.kind for v in report.violations}
    assert {KIND_OPEN, KIND_PATHLIB_IO, KIND_SHUTIL_IO} <= kinds


def test_gate_flags_aliased_shutil_and_pathlib(tmp_path):
    """Alias bypass must not hide the touch-point."""
    _write(
        tmp_path,
        "aliased.py",
        """
        import shutil as sh
        from pathlib import Path as P

        def serve(src, dst):
            sh.move(src, dst)
            return P(src).read_text()
        """,
    )
    report = run_storage_bypass_gate(tmp_path)
    kinds = {v.kind for v in report.violations}
    assert KIND_SHUTIL_IO in kinds
    assert KIND_PATHLIB_IO in kinds


def test_gate_flags_aliased_fileresponse(tmp_path):  # ts_dc951024 (alias bypass)
    """`from starlette.responses import FileResponse as FR; FR(path=...)` must NOT
    sneak past the gate — alias resolution catches the renamed import."""
    _write(
        tmp_path,
        "attachments_aliased.py",
        """
        from starlette.responses import FileResponse as FR

        def download(attachment):
            return FR(path=attachment.path)
        """,
    )
    report = run_storage_bypass_gate(tmp_path)
    assert not report.ok
    assert KIND_FILE_RESPONSE in {v.kind for v in report.violations}
    fr = next(v for v in report.violations if v.kind == KIND_FILE_RESPONSE)
    assert fr.symbol == "FileResponse"  # canonicalised, even though source said FR


def test_gate_passes_storageprovider_variant(tmp_path):
    """The clean variant that delegates to the StorageProvider has no bypass."""
    _write(
        tmp_path,
        "attachments_clean.py",
        """
        from okto_pulse.core.infra.storage import get_storage_provider

        async def download(attachment):
            storage = get_storage_provider()
            return storage.open_stream(attachment.path)
        """,
    )
    report = run_storage_bypass_gate(tmp_path)
    assert report.ok and report.violations == []


def test_gate_allowlist_ratchet_only_shrinks():
    assert storage_bypass_allowlist_only_shrinks(STORAGE_BYPASS_ALLOWLIST, STORAGE_BYPASS_ALLOWLIST)
    # a NEW allowlisted file (loosening) is not an honest ratchet
    assert not storage_bypass_allowlist_only_shrinks(
        STORAGE_BYPASS_ALLOWLIST, {"src/x.py": "reason"}
    )


def test_occurrence_allowlist_survives_line_shift(tmp_path):
    root = _write(
        tmp_path,
        "src/okto_pulse/core/mcp/server.py",
        """
        def _load_instructions():
            # Line shifts and comments are diagnostics only.
            # The semantic call below remains the same authorized occurrence.
            for candidate in []:
                if candidate.exists():
                    return candidate.read_text(encoding="utf-8")
        """,
    ).parents[1]

    report = run_storage_bypass_gate(root)

    assert report.ok, [v.as_dict() for v in report.violations]
    occurrence = next(o for o in report.occurrences if o.enclosing_qualname == "_load_instructions")
    assert occurrence.line != 167
    assert occurrence.key() in STORAGE_BYPASS_OCCURRENCE_ALLOWLIST


def test_occurrence_allowlist_rejects_semantic_call_change(tmp_path):
    root = _write(
        tmp_path,
        "src/okto_pulse/core/mcp/server.py",
        """
        def _load_instructions():
            for candidate in []:
                if candidate.exists():
                    return candidate.read_text(encoding="latin-1")
        """,
    ).parents[1]

    report = run_storage_bypass_gate(root)

    assert not report.ok
    violation = next(v for v in report.violations if v.enclosing_qualname == "_load_instructions")
    assert violation.file == "src/okto_pulse/core/mcp/server.py"
    assert violation.line > 0
    assert violation.key() not in STORAGE_BYPASS_OCCURRENCE_ALLOWLIST


def test_occurrence_fingerprint_distinguishes_identical_calls_by_scope(tmp_path):
    root = _write(
        tmp_path,
        "src/okto_pulse/core/mcp/server.py",
        """
        def _load_instructions():
            for candidate in []:
                if candidate.exists():
                    return candidate.read_text(encoding="utf-8")

        def _load_resource_file(relative_path):
            candidate = relative_path
            if candidate.exists():
                return candidate.read_text(encoding="utf-8")
            return ""
        """,
    ).parents[1]

    report = run_storage_bypass_gate(root)

    assert report.ok, [v.as_dict() for v in report.violations]
    by_scope = {o.enclosing_qualname: o for o in report.occurrences}
    assert {
        "_load_instructions",
        "_load_resource_file",
    } <= set(by_scope)
    assert by_scope["_load_instructions"].normalized_call_ast == (
        by_scope["_load_resource_file"].normalized_call_ast
    )
    assert by_scope["_load_instructions"].fingerprint != by_scope["_load_resource_file"].fingerprint


def test_occurrence_allowlist_ratchet_only_shrinks():
    current = STORAGE_BYPASS_OCCURRENCE_ALLOWLIST
    assert storage_bypass_occurrence_allowlist_only_shrinks(current, current)

    first_key, first_value = next(iter(current.items()))
    assert storage_bypass_occurrence_allowlist_only_shrinks(
        current, {first_key: dict(first_value)}
    )

    widened = {
        **current,
        "new-fingerprint": {
            "owner": "core",
            "reason": "new bypass",
            "removal_criteria": "remove later",
        },
    }
    assert not storage_bypass_occurrence_allowlist_only_shrinks(current, widened)

    changed = dict(current)
    changed[first_key] = {**first_value, "reason": "changed"}
    assert not storage_bypass_occurrence_allowlist_only_shrinks(current, changed)


# --------------------------------------------------------------------------- FR3
def test_ledger_not_ready_when_any_oracle_op_missing():  # ts_a44f0d14
    for omitted in STORAGE_PROVIDER_ORACLE_OPS:
        kwargs = {op: True for op in STORAGE_PROVIDER_ORACLE_OPS}
        kwargs["bypass_gate"] = True
        kwargs[omitted] = None
        verdict = evaluate_storage_provider_readiness(
            StorageProviderReadinessEvidence(**kwargs)
        )
        assert not verdict.is_ready
        assert omitted in verdict.missing_evidence
        assert verdict.status == "blocked"


def test_ledger_not_ready_when_oracle_op_failed():
    verdict = evaluate_storage_provider_readiness(
        StorageProviderReadinessEvidence(
            upload=True, download_stream=False, delete=True, bypass_gate=True
        )
    )
    assert not verdict.is_ready
    assert "download_stream" in verdict.failed_evidence


def test_ledger_not_ready_without_bypass_gate():
    verdict = evaluate_storage_provider_readiness(
        StorageProviderReadinessEvidence(
            upload=True, download_stream=True, delete=True, bypass_gate=None
        )
    )
    assert not verdict.is_ready
    assert "bypass_gate" in verdict.missing_evidence


def test_ledger_ready_only_with_full_oracle_and_gate():
    verdict = evaluate_storage_provider_readiness(
        StorageProviderReadinessEvidence(
            upload=True, download_stream=True, delete=True, bypass_gate=True
        )
    )
    assert verdict.is_ready
    assert verdict.status == "ready"
    assert verdict.missing_evidence == () and verdict.failed_evidence == ()


def test_generic_evaluate_adapter_readiness_storage_entry_blocks_until_complete():  # ts_a44f0d14
    """The generic ledger evaluator on the StorageProvider INVENTORY ENTRY is also
    fail-closed (the path the scenario names, ``evaluate_adapter_readiness`` for the
    filesystem_storage_provider entry): an omitted oracle keeps it not-ready and
    cites the missing field; complete evidence makes the port ready."""
    from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
        AdapterEvidence,
        build_adapter_inventory,
        evaluate_adapter_readiness,
    )

    entry = next(
        e for e in build_adapter_inventory() if e.adapter_key == "filesystem_storage_provider"
    )
    incomplete = evaluate_adapter_readiness(
        entry,
        AdapterEvidence(
            port_closed=True,
            community_registered=True,
            oracle_passed=None,  # the download/upload/delete oracle is unproven
            import_audit_passed=True,
            dependency_audit_passed=True,
            register_before_remove_passed=True,
        ),
    )
    assert not incomplete.is_ready
    assert "oracle_passed" in incomplete.missing_evidence

    complete = evaluate_adapter_readiness(
        entry,
        AdapterEvidence(
            port_closed=True,
            community_registered=True,
            oracle_passed=True,
            import_audit_passed=True,
            dependency_audit_passed=True,
            register_before_remove_passed=True,
        ),
    )
    assert complete.is_ready and complete.status == "ready"
