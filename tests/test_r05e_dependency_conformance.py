"""R05-E — dependency cleanup + fail-closed conformance auditor.

Scenario mapping (spec d9d30831, card 0c066d12):

  ts_c2f39229 (TS-R05E-01) — asyncpg removed from the core default (manifest +
                lock + source).
  ts_3a5d3275 (TS-R05E-02) — the gate fails closed for an unledgered dependency /
                import AND for a re-introduced `removed` token (negative).
  ts_681ca6d4 (TS-R05E-03) — the ledger covers every remaining temporary
                exception with owner/axis/criterion/oracle.
  ts_7068fa1e (TS-R05E-04) — the auditor ignores non-executable text, files
                outside src/okto_pulse/core, and transitive-only lock entries (no
                false positive).

AF-05 extends the same harness: aiofiles is removed from the core runtime
manifest/lock/wheel metadata and fails closed if reintroduced on any runtime
surface.

Plus: ledger purity / isolation, and the actionable report (OR-R05E-01).
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

import okto_pulse.core.application.boundary.dependency_conformance as _conf_mod
import okto_pulse.core.application.boundary.dependency_ledger as _ledger_mod
from okto_pulse.core.application.boundary.dependency_conformance import (
    GOVERNED_TECHNICAL_TOKENS,
    audit_dependency_conformance,
    render_report,
)
from okto_pulse.core.application.boundary.dependency_ledger import (
    CANONICAL_AF40_CARRY_FORWARD_TOKENS,
    CANONICAL_AF40_DEPENDENCY_TOKENS,
    CANONICAL_TEMPORARY_EXCEPTION_TOKENS,
    REQUIRED_ENTRY_FIELDS,
    build_dependency_ledger,
    normalize_token,
)
from repository_checkout_testing import community_repo_for

CONF_PY = Path(_conf_mod.__file__)
LEDGER_PY = Path(_ledger_mod.__file__)
SRC_ROOT = CONF_PY.resolve().parents[4]  # .../src
REPO_ROOT = SRC_ROOT.parent
COMMUNITY_ROOT = community_repo_for(REPO_ROOT)


# --------------------------------------------------------------------------- #
# synthetic-repo helper
# --------------------------------------------------------------------------- #
def _synthetic_audit(
    tmp_path: Path,
    *,
    pyproject_text: str | None = None,
    lock_text: str | None = None,
    core_files: dict[str, str] | None = None,
    outside_files: dict[str, str] | None = None,
    governed=None,
    ledger=None,
):
    repo = tmp_path
    pyproject = repo / "pyproject.toml"
    if pyproject_text is not None:
        pyproject.write_text(pyproject_text, encoding="utf-8")
    lock = repo / "uv.lock"
    if lock_text is not None:
        lock.write_text(lock_text, encoding="utf-8")
    src = repo / "src"
    core = src / "okto_pulse" / "core"
    core.mkdir(parents=True, exist_ok=True)
    (core / "__init__.py").write_text("", encoding="utf-8")
    for name, content in (core_files or {}).items():
        (core / name).write_text(content, encoding="utf-8")
    for rel, content in (outside_files or {}).items():
        target = repo / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return audit_dependency_conformance(
        repo_root=repo,
        source_root=src,
        pyproject_path=pyproject if pyproject_text is not None else repo / "missing.toml",
        lock_path=lock if lock_text is not None else repo / "missing.lock",
        audit_wheel=False,
        governed_tokens=governed,
        ledger=ledger,
    )


# ===========================================================================
# ts_c2f39229 — asyncpg removed from the core default.
# ===========================================================================
def test_ts_c2f39229_asyncpg_removed_from_core_default():
    import tomllib

    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    deps = pyproject["project"]["dependencies"]
    names = {
        d.split("[")[0].split(">")[0].split("<")[0].split("=")[0].strip().lower()
        for d in deps
    }
    assert "asyncpg" not in names, "asyncpg still declared as a core direct dependency"

    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    core = next(p for p in lock["package"] if p["name"] == "okto-pulse-core")
    lock_dep_names = {d["name"] for d in core.get("dependencies", [])}
    assert "asyncpg" not in lock_dep_names, "uv.lock still lists asyncpg as a core direct dep"

    # The auditor confirms removal and raises no violation about asyncpg.
    report = audit_dependency_conformance(audit_wheel=False)
    assert "asyncpg" in report.removed_dependencies
    assert all(v.token != "asyncpg" for v in report.violations)


def test_af05_aiofiles_removed_from_core_runtime_surfaces():
    import tomllib

    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    deps = pyproject["project"]["dependencies"]
    names = {
        d.split("[")[0].split(">")[0].split("<")[0].split("=")[0].strip().lower()
        for d in deps
    }
    assert "aiofiles" not in names, "aiofiles still declared as a core direct dependency"

    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    core = next(p for p in lock["package"] if p["name"] == "okto-pulse-core")
    lock_dep_names = {d["name"] for d in core.get("dependencies", [])}
    assert "aiofiles" not in lock_dep_names, "uv.lock still lists aiofiles as a core direct dep"

    report = audit_dependency_conformance(audit_wheel=False)
    assert "aiofiles" in report.removed_dependencies
    assert all(v.token != "aiofiles" for v in report.violations)


# ===========================================================================
# ts_3a5d3275 — fail-closed for unledgered dep/import + removed re-introduction.
# ===========================================================================
def test_ts_3a5d3275_fail_closed_on_unledgered_dependency_and_import(tmp_path):
    governed = GOVERNED_TECHNICAL_TOKENS | {normalize_token("redis")}
    pyproject_text = (
        "[project]\n"
        'name = "synthetic"\n'
        'version = "0"\n'
        'dependencies = ["redis>=5.0"]\n'
    )
    report = _synthetic_audit(
        tmp_path,
        pyproject_text=pyproject_text,
        core_files={"cache.py": "import redis\n\nX = redis\n"},
        governed=governed,
    )
    assert report.ok is False
    codes = {v.diagnostic_code for v in report.violations}
    assert "unledgered_dependency" in codes
    assert "unledgered_import" in codes
    # the diagnostics carry origin + location for remediation.
    src_v = next(v for v in report.violations if v.diagnostic_code == "unledgered_import")
    assert src_v.origin == "runtime_import"
    assert "cache.py" in src_v.location


def test_ts_3a5d3275_fail_closed_on_reintroduced_removed_token(tmp_path):
    # asyncpg is ledgered as `removed`; re-adding it as a dep or import is a
    # deterministic fail-closed violation under the REAL ledger.
    pyproject_text = (
        "[project]\n"
        'name = "synthetic"\n'
        'version = "0"\n'
        'dependencies = ["asyncpg>=0.29"]\n'
    )
    report = _synthetic_audit(
        tmp_path,
        pyproject_text=pyproject_text,
        core_files={"db.py": "import asyncpg\n"},
    )
    assert report.ok is False
    codes = {v.diagnostic_code for v in report.violations}
    assert "removed_dependency_present" in codes
    assert "removed_import_present" in codes


def test_ts_3a5d3275_incomplete_ledger_entry_fails_closed(tmp_path):
    # A ledger entry missing a required field makes the ledger itself non-conformant.
    broken = list(build_dependency_ledger())
    broken[0] = replace(broken[0], validation_oracle="   ")  # blank required field
    report = _synthetic_audit(tmp_path, pyproject_text='[project]\nname="x"\nversion="0"\n', ledger=tuple(broken))
    assert report.ledger_integrity_ok is False
    assert report.ok is False
    assert any(v.diagnostic_code == "ledger_entry_incomplete" for v in report.violations)


def test_ts_3a5d3275_explicit_wheel_metadata_with_removed_token_fails_closed(tmp_path):
    # An EXPLICIT dist-info METADATA artifact is AUTHORITATIVE (unlike a possibly
    # stale editable importlib.metadata): a `removed` token (asyncpg) in its
    # Requires-Dist must fail CLOSED as `removed_dependency_present`/blocking, not
    # be tolerated as a `stale_removed_in_wheel` warning. The real repo manifest +
    # lock + source are clean, so the wheel finding is the sole violation.
    meta = tmp_path / "METADATA"
    meta.write_text(
        "Metadata-Version: 2.1\n"
        "Name: okto-pulse-core\n"
        "Version: 0.3.0\n"
        "Requires-Dist: asyncpg>=0.29\n",
        encoding="utf-8",
    )
    report = audit_dependency_conformance(wheel_metadata_path=meta, audit_wheel=True)

    assert "wheel" in report.surfaces_audited
    wheel_removed = [
        v
        for v in report.violations
        if v.surface == "wheel"
        and v.token == "asyncpg"
        and v.diagnostic_code == "removed_dependency_present"
    ]
    assert wheel_removed, [
        f.as_dict() for f in report.findings if f.surface == "wheel"
    ]
    assert wheel_removed[0].severity == "blocking"
    assert report.ok is False
    # The explicit-metadata path must NOT downgrade to a stale warning.
    assert all(w.diagnostic_code != "stale_removed_in_wheel" for w in report.warnings)


def test_af05_aiofiles_reintroduction_fails_closed_on_all_runtime_surfaces(tmp_path):
    pyproject_text = (
        "[project]\n"
        'name = "synthetic"\n'
        'version = "0"\n'
        'dependencies = ["aiofiles>=23.2"]\n'
    )
    lock_text = (
        "version = 1\n"
        'requires-python = ">=3.11"\n\n'
        "[[package]]\n"
        'name = "okto-pulse-core"\n'
        'version = "0.3.0"\n'
        "dependencies = [\n"
        '    { name = "aiofiles" },\n'
        "]\n"
    )
    metadata = tmp_path / "okto_pulse_core-0.dist-info" / "METADATA"
    metadata.parent.mkdir()
    metadata.write_text(
        "Metadata-Version: 2.1\n"
        "Name: okto-pulse-core\n"
        "Version: 0\n"
        "Requires-Dist: aiofiles>=23.2\n",
        encoding="utf-8",
    )

    repo = tmp_path
    pyproject = repo / "pyproject.toml"
    pyproject.write_text(pyproject_text, encoding="utf-8")
    lock = repo / "uv.lock"
    lock.write_text(lock_text, encoding="utf-8")
    src = repo / "src"
    core = src / "okto_pulse" / "core"
    core.mkdir(parents=True)
    (core / "__init__.py").write_text("", encoding="utf-8")
    (core / "files.py").write_text("import aiofiles\n", encoding="utf-8")

    report = audit_dependency_conformance(
        repo_root=repo,
        source_root=src,
        pyproject_path=pyproject,
        lock_path=lock,
        wheel_metadata_path=metadata,
        audit_wheel=True,
    )

    assert report.ok is False
    hits = [
        v
        for v in report.violations
        if v.token == "aiofiles"
        and v.diagnostic_code
        in {"removed_dependency_present", "removed_import_present"}
    ]
    assert {v.surface for v in hits} == {"manifest", "lock", "source", "wheel"}
    assert all(v.classification == "removed" for v in hits)


# ===========================================================================
# ts_681ca6d4 — terminal ledger has no temporary exceptions.
# ===========================================================================
def test_ts_681ca6d4_ledger_covers_temporary_exceptions():
    ledger = build_dependency_ledger()
    index = {normalize_token(e.token): e for e in ledger}
    for e in ledger:
        for alias in e.aliases:
            index[normalize_token(alias)] = e

    assert CANONICAL_AF40_CARRY_FORWARD_TOKENS == CANONICAL_TEMPORARY_EXCEPTION_TOKENS

    for token in CANONICAL_TEMPORARY_EXCEPTION_TOKENS:
        entry = index.get(normalize_token(token))
        assert entry is not None, f"temporary exception '{token}' is not ledgered"
        for fld in REQUIRED_ENTRY_FIELDS:
            value = getattr(entry, fld)
            assert isinstance(value, str) and value.strip(), f"{token}.{fld} empty"
        assert entry.classification == "temporary_exception"

    for token in CANONICAL_AF40_DEPENDENCY_TOKENS:
        entry = index.get(normalize_token(token))
        assert entry is not None, f"AF40 token '{token}' is not ledgered"
        for fld in REQUIRED_ENTRY_FIELDS:
            value = getattr(entry, fld)
            assert isinstance(value, str) and value.strip(), f"{token}.{fld} empty"

    assert CANONICAL_TEMPORARY_EXCEPTION_TOKENS == ()
    assert CANONICAL_AF40_CARRY_FORWARD_TOKENS == ()

    # F14 transferred both former carry-forward rows out of Core.
    for token in ("numpy", "aiosqlite"):
        entry = index[normalize_token(token)]
        assert entry.classification == "community_owned"
        assert entry.direct_dep_no_import is False
        assert entry.transitive_consumer is None
        assert entry.expected_source_import_roots == ()

    # asyncpg is the SAME shape (direct-dep + no-import) but classified `removed`.
    asyncpg = index[normalize_token("asyncpg")]
    assert asyncpg.classification == "removed"
    assert asyncpg.direct_dep_no_import is True
    assert asyncpg.transitive_consumer is None

    aiofiles = index[normalize_token("aiofiles")]
    assert aiofiles.classification == "removed"
    assert aiofiles.direct_dep_no_import is True
    assert aiofiles.transitive_consumer is None

    ladybug = index[normalize_token("ladybug")]
    assert ladybug.classification == "community_owned"
    assert ladybug.expected_source_import_roots == ("ladybug",)

    apscheduler = index[normalize_token("apscheduler")]
    assert apscheduler.classification == "community_owned"
    assert apscheduler.expected_source_import_roots == ("apscheduler",)

    sentence_transformers = index[normalize_token("sentence-transformers")]
    assert sentence_transformers.classification == "community_owned"
    assert sentence_transformers.expected_source_import_roots == ("sentence_transformers",)

    requests = index[normalize_token("requests")]
    assert requests.classification == "community_owned"
    assert requests.expected_source_import_roots == ("requests",)
    chardet = index[normalize_token("chardet")]
    assert chardet.classification == "community_owned"
    assert chardet.expected_source_import_roots == ("chardet",)

    # The real repo reports no accepted exception and all transferred tokens as
    # externally owned.
    report = audit_dependency_conformance(audit_wheel=False)
    assert report.accepted_exceptions == ()
    assert "ladybug" in report.community_owned
    assert "sentence-transformers" in report.community_owned
    assert "requests" in report.community_owned
    assert "chardet" in report.community_owned
    assert "aiosqlite" in report.community_owned
    assert "numpy" in report.community_owned


def test_af40_carry_forward_budget_is_terminal_zero():
    assert CANONICAL_AF40_CARRY_FORWARD_TOKENS == ()
    assert CANONICAL_TEMPORARY_EXCEPTION_TOKENS == ()
    assert audit_dependency_conformance(audit_wheel=False).accepted_exceptions == ()


def test_community_owned_ladybug_reintroduction_fails_closed(tmp_path):
    report = _synthetic_audit(
        tmp_path,
        pyproject_text=(
            '[project]\nname="x"\nversion="0"\n'
            'dependencies=["ladybug>=0.16.0"]\n'
        ),
        lock_text=(
            "version = 1\n"
            'requires-python = ">=3.11"\n\n'
            "[[package]]\n"
            'name = "okto-pulse-core"\n'
            'version = "0.3.0"\n'
            "dependencies = [\n"
            '    { name = "ladybug" },\n'
            "]\n"
        ),
        core_files={"kg_runtime.py": "import ladybug\n"},
    )
    assert report.ok is False
    codes = {v.diagnostic_code for v in report.violations}
    assert "external_owner_dependency_present" in codes
    assert "external_owner_import_present" in codes


# ===========================================================================
# ts_7068fa1e — ignore non-executable text, files outside core, transitive lock.
# ===========================================================================
def test_ts_7068fa1e_ignores_non_executable_text_and_outside_files(tmp_path):
    report = _synthetic_audit(
        tmp_path,
        pyproject_text='[project]\nname="x"\nversion="0"\n',
        core_files={
            # asyncpg appears ONLY as a string / comment — not an import.
            "notes.py": '# asyncpg is removed in R05-E\nDRIVER = "asyncpg"\nSQL = "import asyncpg here"\n',
        },
        outside_files={
            # a real import, but OUTSIDE src/okto_pulse/core -> must not be scanned.
            "tests/test_pg.py": "import asyncpg\n",
            "docs/guide.md": "use asyncpg for postgres\n",
        },
    )
    assert report.ok is True
    assert report.violations == ()


def test_ts_7068fa1e_transitive_only_lock_entry_does_not_reprove(tmp_path):
    # The core package declares NO asyncpg; another package depends on it
    # transitively. asyncpg must NOT be flagged (tr_ee04e7b2).
    lock_text = (
        "version = 1\n"
        'requires-python = ">=3.11"\n\n'
        "[[package]]\n"
        'name = "okto-pulse-core"\n'
        'version = "0.3.0"\n'
        'source = { editable = "." }\n'
        "dependencies = [\n"
        '    { name = "pydantic" },\n'
        "]\n\n"
        "[[package]]\n"
        'name = "some-postgres-tool"\n'
        'version = "1.0.0"\n'
        "dependencies = [\n"
        '    { name = "asyncpg" },\n'
        "]\n"
    )
    report = _synthetic_audit(
        tmp_path,
        pyproject_text='[project]\nname="x"\nversion="0"\ndependencies=["pydantic>=2"]\n',
        lock_text=lock_text,
    )
    assert "lock" in report.surfaces_audited
    assert report.ok is True
    assert all(v.token != "asyncpg" for v in report.violations)


# ===========================================================================
# ledger purity / isolation.
# ===========================================================================
def test_ledger_module_is_stdlib_only_at_top_level():
    tree = ast.parse(LEDGER_PY.read_text(encoding="utf-8"))
    top: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            top.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            top.add(node.module)
    assert top <= {"dataclasses", "typing", "__future__"}, f"non-stdlib import: {top}"


def test_conformance_module_imports_in_isolation(tmp_path):
    code = (
        "import sys\n"
        "import okto_pulse.core.application.boundary.dependency_conformance as m\n"
        "rep = m.audit_dependency_conformance(audit_wheel=False)\n"
        "assert hasattr(rep, 'ok')\n"
        "forbidden = ('asyncpg','aiofiles','ladybug','numpy','sentence_transformers','torch','apscheduler')\n"
        "leaked = [n for n in sys.modules if any(n == p or n.startswith(p+'.') for p in forbidden)]\n"
        "assert not leaked, 'leaked concrete imports: ' + repr(leaked)\n"
        "print('ISO_OK')\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "ISO_OK" in proc.stdout


# ===========================================================================
# OR-R05E-01 — actionable report.
# ===========================================================================
def test_report_is_actionable_and_serialisable():
    report = audit_dependency_conformance(audit_wheel=False)
    text = render_report(report)
    assert "R05-E dependency conformance:" in text
    assert "removed_dependencies" in text
    assert "asyncpg" in text
    assert "aiofiles" in text
    assert "temporary_exceptions" in text
    assert "temporary_exceptions (0)" in text
    assert "community_owned :" in text
    assert "apscheduler" in text
    assert "requests" in text
    assert "chardet" in text
    assert "ladybug" in text
    assert report.accepted_exceptions == ()

    # JSON-serialisable projection (no dataclasses / sets leak).
    import json

    payload = json.dumps(report.as_dict(), sort_keys=True)
    assert '"ledger_version": "R05-E.3"' in payload


def test_af05_readmes_document_dependency_owner_matrix():
    community_architecture_path = COMMUNITY_ROOT / "docs" / "ARCHITECTURE.md"
    if not community_architecture_path.exists():
        pytest.skip("Community sibling repo not available for architecture matrix check")

    core_architecture = (
        REPO_ROOT / "docs" / "ARCHITECTURE-OVERVIEW.md"
    ).read_text(encoding="utf-8")
    community_architecture = community_architecture_path.read_text(encoding="utf-8")

    for text in (core_architecture, community_architecture):
        assert "AF-05/AF40 dependency owner matrix" in text
        assert "`dependency_ledger.py`" in text
        assert "`CANONICAL_AF40_DEPENDENCY_TOKENS`" in text
        assert "`CANONICAL_TEMPORARY_EXCEPTION_TOKENS`" in text
        assert "`conformance_matrix.py`" in text
        for token, status in (
            ("aiofiles", "removed"),
            ("requests", "community_owned"),
            ("chardet", "community_owned"),
            ("aiosqlite", "community_owned"),
            ("numpy", "community_owned"),
            ("apscheduler", "community_owned"),
        ):
            assert f"| `{token}` | `{status}` |" in text

        assert "F14 dependency ownership" in text

    assert "published core dependency" in core_architecture
    assert "published `okto-pulse-core`" in community_architecture


def test_real_repo_is_conformant():
    report = audit_dependency_conformance(audit_wheel=False)
    assert report.ledger_integrity_ok is True
    assert report.ok is True, [v.as_dict() for v in report.violations]
    assert set(report.surfaces_audited) >= {"ledger", "manifest", "lock", "source"}
    assert "asyncpg" in report.removed_dependencies
    assert "aiofiles" in report.removed_dependencies
