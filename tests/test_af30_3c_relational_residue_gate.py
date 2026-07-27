from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.relational_residue_gate import (
    run_relational_residue_gate,
)


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
COMMUNITY_ROOT = (
    Path(__file__).resolve().parents[2]
    / "okto_labs_pulse_community"
    / "src"
    / "okto_pulse"
    / "community"
)


def _write(root: Path, rel: str, source: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_af30_3c_synthetic_final_relational_residues_fail_closed(tmp_path: Path) -> None:
    core_root = tmp_path / "core"
    community_root = tmp_path / "community"
    _write(
        core_root,
        "events/handlers/consolidation_enqueuer.py",
        "from sqlalchemy.dialects.sqlite import insert as upsert_insert\n"
        "def enqueue(session):\n"
        "    dialect_name = session.bind.dialect.name\n"
        "    return upsert_insert(object).on_conflict_do_update(index_elements=['id'])\n",
    )
    _write(
        core_root,
        "infra/daily_tick.py",
        "from sqlalchemy import Column, DateTime, MetaData, Table\n"
        "kg_tick_runs = Table('kg_tick_runs', MetaData(), Column('completed_at', DateTime()))\n",
    )
    _write(
        core_root,
        "infra/config.py",
        "class CoreSettings:\n"
        "    database_url: str = 'sqlite+aiosqlite:///./dashboard.db'\n",
    )
    _write(
        core_root,
        "application/boundary/relational_adapter_import_gate.py",
        "ALLOWLIST = ('infra/database.py',)\n",
    )
    _write(
        community_root,
        "adapters/relational_schema_migrator.py",
        "from okto_pulse.core.infra import database as _database\n"
        "def run(step):\n"
        "    return getattr(_database, step.step_id)\n",
    )

    report = run_relational_residue_gate(
        core_root=core_root,
        community_root=community_root,
    )

    assert report.ok is False
    categories = {finding.category for finding in report.findings}
    assert {
        "dialect_sql",
        "ad_hoc_metadata",
        "sqlite_dsn_default",
        "engine_session_factory_allowlist",
        "dynamic_private_database_access",
    } <= categories


def test_af30_3c_real_core_and_community_are_green_state() -> None:
    report = run_relational_residue_gate(
        core_root=CORE_ROOT,
        community_root=COMMUNITY_ROOT if COMMUNITY_ROOT.exists() else None,
    )

    assert report.ok is True, report.as_dict()
