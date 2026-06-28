"""Spec #06 card 9b83f384 — KgSchemaImportClassificationGate (tr_6d1efe1a).

Proves the canonical kg.schema importer inventory + classification gate:
the table carries file/symbol/category/verdict/target_port/owner/rationale
(ac_70d3e4da); a non-adapter consumer using open_board_connection is blocked
with target GraphTransaction (ac_4c332301); board_kuzu_path / close_all_connections
target GraphPathResolver / GraphLifecycle (ac_eacf2ac1); read-only schema
constants are non-blocking; the count divergence (45 vs 42/77 vs live) is
reconciled (ac_1c413377); embedded adapters and dedup migration moved off
direct schema imports.
"""

from __future__ import annotations

from okto_pulse.core.kg.schema_import_classification_gate import (
    run_kg_schema_import_classification_gate as run_gate,
)


# --------------------------------------------------------------------------- #
# Canonical table + allowlist over the real core (ac_70d3e4da)
# --------------------------------------------------------------------------- #


def test_canonical_table_has_all_fields_real_core():
    report = run_gate()
    assert report.importers == []
    assert report.ledgered_exceptions == []
    assert report.ledger_detail == []
    assert report.ok is True
    assert report.violations == []


def test_synthetic_importer_entries_have_required_fields(tmp_path):
    (tmp_path / "consumer.py").write_text(
        "from okto_pulse.core.kg.schema import open_board_connection\n",
        encoding="utf-8",
    )
    report = run_gate(tmp_path)
    assert len(report.importers) == 1
    for entry in report.importers:
        assert entry.file
        assert entry.category
        assert entry.verdict in (
            "adapter_internal_legitimate",
            "migration_allowlisted",
            "needs_migration",
        )
        assert entry.target_port
        assert entry.owner
        assert entry.rationale
        assert isinstance(entry.blocking, bool)


def test_real_core_has_no_embedded_or_dedup_schema_imports():
    report = run_gate()
    verdict_by_file = {i.file: i.verdict for i in report.importers}
    assert not any(f.startswith("kg/providers/embedded/") for f in verdict_by_file)
    assert "kg/dedup_migration.py" not in verdict_by_file
    # The allowlisted CLI lives in tools/ (outside core/), but it no longer imports
    # kg.schema; it remains documented in the allowlist and correctly drops out of
    # the importer table.
    assert report.allowlist["migration_cli"] == "tools/kg_migrate_schema.py"
    assert "tools/kg_migrate_schema.py" not in verdict_by_file


def test_allowlisted_cli_without_schema_import_is_not_counted():
    report = run_gate()
    cli = next(
        (i for i in report.importers if i.file == "tools/kg_migrate_schema.py"), None
    )
    assert cli is None, (
        "the kg migrate-schema CLI no longer imports kg.schema and must not be "
        "invented as an importer"
    )
    assert report.allowlist["migration_cli"] == "tools/kg_migrate_schema.py"


def test_reconciliation_explains_count_divergence_real_core():
    report = run_gate()
    rec = report.reconciliation
    # Historical baselines are always reported (validator vs spec-local).
    assert rec["validator_baseline_importer_files"] == 45
    assert rec["spec_local_importer_files"] == 42
    assert rec["spec_local_import_statements"] == 77
    # The live count is computed and reconciled against those baselines. It is
    # NOT pinned to 42/77: spec #06 adds embedded port adapters (which legitimately
    # import kg.schema, raising the count) and migrates consumers (which lowers it).
    # The earlier core/-only scan under-counted by omitting the tools/ CLI.
    assert rec["current_importer_files"] == len(report.importers)
    assert rec["current_import_statements"] >= rec["current_importer_files"]
    assert "drift" in rec["explanation"]


def test_recounted_refs_present_real_core():
    refs = run_gate().recounted_refs
    assert set(refs["baseline"]) == {
        "open_board_connection",
        "board_kuzu_path",
        "close_all_connections",
    }
    assert all(refs["current"][k] >= 1 for k in refs["baseline"])


# --------------------------------------------------------------------------- #
# Blocking classification per forbidden symbol (ac_4c332301 / ac_eacf2ac1)
# --------------------------------------------------------------------------- #


def test_open_board_connection_consumer_is_blocked(tmp_path):
    (tmp_path / "consumer.py").write_text(
        "from okto_pulse.core.kg.schema import open_board_connection\n"
        "def f(bid):\n    return open_board_connection(bid)\n",
        encoding="utf-8",
    )
    report = run_gate(tmp_path)
    assert report.ok is False
    entry = next(i for i in report.importers if i.file == "consumer.py")
    assert entry.verdict == "needs_migration"
    assert entry.blocking is True
    assert entry.target_port == "GraphTransaction"
    assert "open_board_connection" in entry.symbols
    assert entry in report.violations


def test_path_and_lifecycle_symbols_target_correct_ports(tmp_path):
    (tmp_path / "p.py").write_text(
        "from okto_pulse.core.kg.schema import board_kuzu_path\n", encoding="utf-8"
    )
    (tmp_path / "l.py").write_text(
        "from okto_pulse.core.kg.schema import close_all_connections\n", encoding="utf-8"
    )
    report = run_gate(tmp_path)
    p = next(i for i in report.importers if i.file == "p.py")
    assert p.blocking is True and p.target_port == "GraphPathResolver"
    lc = next(i for i in report.importers if i.file == "l.py")
    assert lc.blocking is True and lc.target_port == "GraphLifecycle"


def test_read_only_schema_constants_are_non_blocking(tmp_path):
    (tmp_path / "c.py").write_text(
        "from okto_pulse.core.kg.schema import NODE_TYPES\n", encoding="utf-8"
    )
    report = run_gate(tmp_path)
    entry = next(i for i in report.importers if i.file == "c.py")
    assert entry.verdict == "needs_migration"
    assert entry.blocking is False
    assert entry.category == "schema_metadata"
    assert report.ok is True  # no blocking importer in this tree


def test_embedded_and_migration_paths_are_blocked_after_runtime_move(tmp_path):
    embedded = tmp_path / "kg" / "providers" / "embedded"
    embedded.mkdir(parents=True)
    (embedded / "adapter.py").write_text(
        "from okto_pulse.core.kg.schema import open_board_connection, _open_kuzu_db\n",
        encoding="utf-8",
    )
    dedup = tmp_path / "kg"
    (dedup / "dedup_migration.py").write_text(
        "from okto_pulse.core.kg.schema import open_board_connection\n", encoding="utf-8"
    )
    report = run_gate(tmp_path)
    adapter = next(
        i for i in report.importers if i.file == "kg/providers/embedded/adapter.py"
    )
    assert adapter.verdict == "needs_migration"
    assert adapter.blocking is True
    migration = next(i for i in report.importers if i.file == "kg/dedup_migration.py")
    assert migration.verdict == "needs_migration"
    assert migration.blocking is True
    assert report.ok is False


def test_module_wildcard_import_is_blocked(tmp_path):
    (tmp_path / "wild.py").write_text(
        "from okto_pulse.core.kg import schema\n"
        "def f(bid):\n    return schema.open_board_connection(bid)\n",
        encoding="utf-8",
    )
    report = run_gate(tmp_path)
    entry = next(i for i in report.importers if i.file == "wild.py")
    assert entry.blocking is True
    assert entry.category == "module_wildcard"
