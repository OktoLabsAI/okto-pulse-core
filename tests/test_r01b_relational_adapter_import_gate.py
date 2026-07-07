"""R01B REPLAN-IMP2 (FR4 / AC4) — the relational adapter import gate.

Proves the alias-aware + import-aware gate BLOCKS any NEW import/use of the
relational runtime-adapter surface — the concrete package
``okto_pulse.core.repositories.sqlalchemy``, the engine factory
``create_async_engine``, the concrete session factory ``async_sessionmaker`` and
the UnitOfWork concretes — outside the TEMPORARY allowlist, reporting file + line.
The real core is clean BECAUSE the two strangled inbound seams (``api/deps.py`` /
``mcp/server.py``) no longer reference the adapter at all — they resolve the
registered provider via ``runtime_registry`` (FR3).

The negative cases run against a SYNTHETIC core tree (``core_root=tmp_path``) so
the gate's teeth are exercised WITHOUT mutating the shared real source tree.
"""

from __future__ import annotations

from okto_pulse.core.application.boundary.relational_adapter_import_gate import (
    ALLOWLIST,
    REMOVAL_CRITERION,
    SENSITIVE_SYMBOLS,
    run_relational_adapter_import_gate,
)


def test_real_core_is_clean_and_inbound_seams_dropped_the_adapter():
    # ts: the real core passes — every reference to the relational adapter surface
    # is an allowlisted definition / re-export / still-core-owned engine home.
    report = run_relational_adapter_import_gate()
    assert report.ok is True, report.violations
    assert report.as_dict()["removal_criterion"] == REMOVAL_CRITERION

    referencing_files = {f.file for f in report.findings}
    # Strangle proof: the two rewired inbound seams carry NO reference to the
    # relational adapter (FR3 repointed them onto runtime_registry).
    assert "api/deps.py" not in referencing_files
    assert "mcp/server.py" not in referencing_files
    # Every real-core finding is an allowlisted entry and carries a real line.
    assert all(f.allowlisted for f in report.findings)
    assert all(f.line >= 1 for f in report.findings)
    # Only the remaining concrete repository package and aggregator remain.
    assert "repositories/sqlalchemy/unit_of_work.py" in referencing_files
    assert "repositories/__init__.py" in referencing_files


def test_gate_blocks_create_async_engine_in_services_with_file_and_line(tmp_path):
    # ts (negative, HARD requirement): a NEW create_async_engine importer/user under
    # services/ is flagged with file AND line (the import line is deterministic).
    svc = tmp_path / "services"
    svc.mkdir(parents=True)
    # line 1: aliased import ; line 5: aliased call (assignment + call chain)
    (svc / "rogue_engine.py").write_text(
        "from sqlalchemy.ext.asyncio import create_async_engine as cae\n"  # 1
        "\n"  # 2
        "\n"  # 3
        "def build(url):\n"  # 4
        "    engine = cae(url)\n"  # 5
        "    return engine\n",  # 6
        encoding="utf-8",
    )
    report = run_relational_adapter_import_gate(core_root=tmp_path)
    assert report.ok is False
    triples = {(v.file, v.symbol, v.line) for v in report.violations}
    # the bare aliased import (line 1) AND the aliased call (line 5) are both caught
    assert ("services/rogue_engine.py", "create_async_engine", 1) in triples
    assert ("services/rogue_engine.py", "create_async_engine", 5) in triples


def test_gate_blocks_module_alias_engine_and_assignment_chain_factory(tmp_path):
    svc = tmp_path / "services"
    svc.mkdir(parents=True)
    # module-alias usage: import ... as aio ; aio.create_async_engine(...)
    (svc / "rogue_modalias.py").write_text(
        "import sqlalchemy.ext.asyncio as aio\n"  # 1
        "\n"  # 2
        "def build(url):\n"  # 3
        "    return aio.create_async_engine(url)\n",  # 4
        encoding="utf-8",
    )
    # assignment-chain alias of the UoW factory, then a call
    (svc / "rogue_uow.py").write_text(
        "from okto_pulse.core.repositories import SQLAlchemyUnitOfWorkFactory\n"
        "F = SQLAlchemyUnitOfWorkFactory\nG = F\nx = G(None)\n",
        encoding="utf-8",
    )
    report = run_relational_adapter_import_gate(core_root=tmp_path)
    assert report.ok is False
    pairs = {(v.file, v.symbol) for v in report.violations}
    assert ("services/rogue_modalias.py", "create_async_engine") in pairs
    assert ("services/rogue_uow.py", "SQLAlchemyUnitOfWorkFactory") in pairs


def test_gate_blocks_sensitive_module_import_and_respects_allowlist(tmp_path):
    svc = tmp_path / "services"
    svc.mkdir(parents=True)
    # direct concrete-package import (submodule) outside allowlist
    (svc / "rogue_pkg.py").write_text(
        "from okto_pulse.core.repositories.sqlalchemy.unit_of_work import "
        "SQLAlchemyUnitOfWork  # noqa: F401\n",
        encoding="utf-8",
    )
    # `from <parent> import sqlalchemy` form (subpackage import)
    (svc / "rogue_pkg2.py").write_text(
        "from okto_pulse.core.repositories import sqlalchemy  # noqa: F401\n",
        encoding="utf-8",
    )
    # allowlisted by prefix: a file inside the definition package may reference it
    sa = tmp_path / "repositories" / "sqlalchemy"
    sa.mkdir(parents=True)
    (sa / "internal.py").write_text(
        "from .unit_of_work import SQLAlchemyUnitOfWork\nu = SQLAlchemyUnitOfWork(None)\n",
        encoding="utf-8",
    )
    # engine/session construction is no longer allowlisted in infra/database.py
    infra = tmp_path / "infra"
    infra.mkdir(parents=True)
    (infra / "database.py").write_text(
        "from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker\n"
        "e = create_async_engine('sqlite://')\n"
        "sf = async_sessionmaker(e)\n",
        encoding="utf-8",
    )
    report = run_relational_adapter_import_gate(core_root=tmp_path)
    assert report.ok is False
    flagged = {(v.file, v.symbol) for v in report.violations}
    assert ("services/rogue_pkg.py", "repositories.sqlalchemy") in flagged
    assert ("services/rogue_pkg.py", "SQLAlchemyUnitOfWork") in flagged  # symbol too
    assert ("services/rogue_pkg2.py", "repositories.sqlalchemy") in flagged
    assert ("infra/database.py", "create_async_engine") in flagged
    assert ("infra/database.py", "async_sessionmaker") in flagged
    violation_files = {v.file for v in report.violations}
    assert "repositories/sqlalchemy/internal.py" not in violation_files


def test_sensitive_symbols_cover_engine_session_and_uow_concretes():
    assert set(SENSITIVE_SYMBOLS) == {
        "SQLAlchemyUnitOfWork",
        "SQLAlchemyUnitOfWorkFactory",
        "create_async_engine",
        "async_sessionmaker",
    }


def test_allowlist_is_governed_not_a_generic_escape():
    # ts (governance): every temporary waiver names an owner and a removal criterion
    # bound to R01C — surfaced in the report so a reviewer can audit each one.
    report = run_relational_adapter_import_gate().as_dict()
    governed = {e["pattern"]: e for e in report["allowlist"]}
    for entry in ALLOWLIST:
        assert entry.owner.startswith("okto-pulse-core/")
        assert "R01C" in entry.removal_criterion
        assert entry.reason  # non-empty rationale
        assert governed[entry.pattern]["removal_criterion"] == entry.removal_criterion

    assert "infra/database.py" not in governed
    assert "events/dispatcher.py" not in governed
