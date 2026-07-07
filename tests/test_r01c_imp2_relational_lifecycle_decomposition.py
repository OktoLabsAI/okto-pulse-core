"""R01C IMP2 D3 — relational lifecycle decomposition manifest + boundary oracle
+ R01C removal ordering invariant (FR5 fr_4b186577, TR4 tr_418fc1a3, ac_af454ee3;
integration scenario ts_cdbe5d65).

Behavioral proof that:
  * the decomposition manifest covers ``database.py`` with NO drift (every
    module-level function is assigned to the R01B provider or R01C lifecycle concern);
  * startup boundary is preserved — concrete engine/session factories stay out of
    core while the injected runtime API is pinned;
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
    rr.reset_relational_runtime_factory()
    yield
    rr.reset_unit_of_work_factory()
    rr.reset_relational_runtime_factory()


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
    assert classify_function("configure_database_runtime") == "r01b"
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


def test_parity_catches_concrete_runtime_reintroduction(tmp_path):
    db = tmp_path / "database.py"
    db.write_text(_DB.read_text(encoding="utf-8") + "\ncreate_async_engine = object()\n", encoding="utf-8")
    errors = startup_parity_errors(database_path=db, runtime_registry_path=_RR)
    assert any("create_async_engine" in e for e in errors)


def test_parity_catches_missing_startup_api(tmp_path):
    db = tmp_path / "database.py"
    db.write_text(_DB.read_text(encoding="utf-8").replace("def configure_database_runtime(", "def configure_database_runtime_renamed("), encoding="utf-8")
    errors = startup_parity_errors(database_path=db, runtime_registry_path=_RR)
    assert any("configure_database_runtime" in e for e in errors)


# ---------------------------------------------------------------------------
# TR4 ordering invariant — fail-closed.
# ---------------------------------------------------------------------------

def test_removal_blocked_without_registered_provider():
    r = r01c_lifecycle_removal_readiness()
    assert r.allowed is False
    assert r.uow_factory_registered is False
    assert r.relational_runtime_factory_registered is False
    assert "register-before-remove" in r.reason


def test_removal_blocked_with_only_partial_registration():
    from okto_pulse.core import runtime_registry as rr

    rr.register_unit_of_work_factory(object())  # uow only, no runtime factory
    r = r01c_lifecycle_removal_readiness()
    assert r.allowed is False
    assert r.uow_factory_registered is True
    assert r.relational_runtime_factory_registered is False
    assert "relational runtime factory" in r.reason


def test_removal_allowed_only_when_both_seams_registered():
    from okto_pulse.core import runtime_registry as rr

    rr.register_unit_of_work_factory(object())
    rr.register_relational_runtime_factory(lambda url, echo=False: object())
    r = r01c_lifecycle_removal_readiness()
    assert r.allowed is True
    assert r.uow_factory_registered is True
    assert r.relational_runtime_factory_registered is True
