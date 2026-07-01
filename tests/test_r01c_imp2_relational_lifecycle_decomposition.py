"""R01C IMP2 D3 — create_database decomposition manifest + startup-parity oracle
+ R01C removal ordering invariant (FR5 fr_4b186577, TR4 tr_418fc1a3, ac_af454ee3;
integration scenario ts_cdbe5d65).

Behavioral proof that:
  * the decomposition manifest covers ``database.py`` with NO drift (every
    module-level function is assigned to the R01B provider or R01C lifecycle concern);
  * startup parity is preserved — the exact SQLite PRAGMAs, the pool tuning and the
    preserved startup API are pinned, and a mutation is caught;
  * TR4 ordering is FAIL-CLOSED — the core may not retire its schema lifecycle until
    R01B registers the relational provider (UnitOfWorkFactory + PRAGMA installer).
"""

from __future__ import annotations

import pathlib

import pytest

from okto_pulse.core.infra.relational_lifecycle_decomposition import (
    R01B_PROVIDER_FUNCTIONS,
    R01C_LIFECYCLE_FUNCTIONS,
    R01C_MIGRATION_PREFIX,
    classify_function,
    decomposition_drift,
    r01c_lifecycle_removal_readiness,
    startup_parity_errors,
)

_DB = pathlib.Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core" / "infra" / "database.py"
_RR = pathlib.Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core" / "runtime_registry.py"


@pytest.fixture(autouse=True)
def _reset_relational_seams():
    from okto_pulse.core import runtime_registry as rr

    rr.reset_unit_of_work_factory()
    rr.reset_sqlite_pragma_installer()
    yield
    rr.reset_unit_of_work_factory()
    rr.reset_sqlite_pragma_installer()


# ---------------------------------------------------------------------------
# Decomposition manifest — no drift.
# ---------------------------------------------------------------------------

def test_decomposition_has_no_drift():
    d = decomposition_drift()
    assert d.ok, d.as_dict()
    assert d.unclassified == []
    assert d.stale == []


def test_provider_and_lifecycle_are_disjoint():
    assert R01B_PROVIDER_FUNCTIONS.isdisjoint(R01C_LIFECYCLE_FUNCTIONS)


def test_representative_classifications():
    assert classify_function("create_database") == "r01b"
    assert classify_function("get_engine") == "r01b"
    assert classify_function("close_db") == "r01b"
    assert classify_function("init_db") == "r01c"
    assert classify_function("_seed_builtin_presets") == "r01c"
    # any migration is auto-classified lifecycle by prefix
    assert classify_function(f"{R01C_MIGRATION_PREFIX}something_new") == "r01c"
    assert classify_function("totally_unknown") is None


# ---------------------------------------------------------------------------
# Startup-parity oracle.
# ---------------------------------------------------------------------------

def test_startup_parity_preserved_on_real_tree():
    assert startup_parity_errors() == []


def test_parity_catches_pool_tuning_drift(tmp_path):
    db = tmp_path / "database.py"
    db.write_text(_DB.read_text(encoding="utf-8").replace('"pool_size": 20', '"pool_size": 25'), encoding="utf-8")
    errors = startup_parity_errors(database_path=db, runtime_registry_path=_RR)
    assert any("pool_size" in e for e in errors)


def test_parity_catches_pragma_drift(tmp_path):
    rr = tmp_path / "runtime_registry.py"
    rr.write_text(_RR.read_text(encoding="utf-8").replace("PRAGMA busy_timeout=30000", "PRAGMA busy_timeout=5000"), encoding="utf-8")
    errors = startup_parity_errors(database_path=_DB, runtime_registry_path=rr)
    assert any("busy_timeout" in e for e in errors)


def test_parity_catches_missing_startup_api(tmp_path):
    db = tmp_path / "database.py"
    # rename create_database -> the preserved-API check must flag it missing
    db.write_text(_DB.read_text(encoding="utf-8").replace("def create_database(", "def create_database_renamed("), encoding="utf-8")
    errors = startup_parity_errors(database_path=db, runtime_registry_path=_RR)
    assert any("create_database" in e for e in errors)


# ---------------------------------------------------------------------------
# TR4 ordering invariant — fail-closed.
# ---------------------------------------------------------------------------

def test_removal_blocked_without_registered_provider():
    r = r01c_lifecycle_removal_readiness()
    assert r.allowed is False
    assert r.uow_factory_registered is False
    assert r.sqlite_pragma_installer_registered is False
    assert "register-before-remove" in r.reason


def test_removal_blocked_with_only_partial_registration():
    from okto_pulse.core import runtime_registry as rr

    rr.register_unit_of_work_factory(object())  # uow only, no pragma installer
    r = r01c_lifecycle_removal_readiness()
    assert r.allowed is False
    assert r.uow_factory_registered is True
    assert r.sqlite_pragma_installer_registered is False
    assert "SQLite PRAGMA installer" in r.reason


def test_removal_allowed_only_when_both_seams_registered():
    from okto_pulse.core import runtime_registry as rr

    rr.register_unit_of_work_factory(object())
    rr.register_sqlite_pragma_installer(lambda engine: None)
    r = r01c_lifecycle_removal_readiness()
    assert r.allowed is True
    assert r.uow_factory_registered is True
    assert r.sqlite_pragma_installer_registered is True
