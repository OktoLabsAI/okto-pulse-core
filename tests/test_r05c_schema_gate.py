"""R05-C (CORE target) — kg.schema import classification gate as a blocking
oracle with the register-before-remove ledger.

  ts_fe24d781 — the classification gate is deterministic, the blocking oracle
                PASSES over the real core (every blocking consumer is either an
                embedded adapter or a ledgered temporary exception), the embedded
                Kùzu runtime is ledgered (adapter_internal_legitimate, NOT
                removed), and a NEW unledgered consumer BLOCKS.
  ts_fe24d781 — (ruling option 2) surfaces with a DIRECT, behaviour-equivalent
                #06 port were MIGRATED off the ledger: services/main.py bootstrap
                → GraphSchemaManager.ensure_bootstrapped, AND every ASYNC +
                simple-execute open_board_connection call-site (class A) →
                GraphTransaction.begin/scope.execute (api/kg_tick.py +
                kg/canonical_learning_partition.py thereby leave the ledger).
                Every remaining ledgered exception (class B/C/D) carries an
                OBJECTIVE R05-E removal criterion; the transaction reason states
                the CORRECT cause (sync→async boundary), NOT a missing port
                surface — GraphTransactionScope.execute exists.
"""

from __future__ import annotations

import inspect
from pathlib import Path

from okto_pulse.core.kg.schema_import_classification_gate import (
    LEDGERED_EXCEPTIONS,
    _LEDGER_REASON_BY_CATEGORY,
    run_kg_schema_import_classification_gate,
)


def test_ts_fe24d781_gate_deterministic_blocking_oracle_with_ledger():
    r1 = run_kg_schema_import_classification_gate()
    r2 = run_kg_schema_import_classification_gate()

    # Deterministic report (or_c2810079).
    assert r1.as_dict() == r2.as_dict()
    assert r1.ledgered_exceptions == r2.ledgered_exceptions
    assert r1.ledger_detail == r2.ledger_detail

    # Blocking oracle PASSES: no out-of-allowlist blocking importer remains —
    # every production KG consumer is either an embedded adapter or ledgered.
    assert r1.ok is True
    assert r1.violations == []

    # The ledger is explicit + complete (every ledgered file is present).
    assert set(r1.ledgered_exceptions) == set(LEDGERED_EXCEPTIONS)
    # Migrated off the ledger: services/main.py (37->36), then the class-A
    # GraphTransaction migrations dropped api/kg_tick.py +
    # kg/canonical_learning_partition.py (36->34). R-P2-04 then migrated six
    # additional business consumers to CypherExecutor / GraphTransaction ports.
    assert len(r1.ledgered_exceptions) == 28
    for migrated in {
        "services/main.py",
        "api/kg_tick.py",
        "kg/canonical_learning_partition.py",
        "events/handlers/cancellation_decay.py",
        "events/handlers/card_boost_recompute.py",
        "kg/tier_power.py",
        "kg/workers/cognitive_closeout.py",
        "services/cognitive_effectiveness_service.py",
        "services/discovery_executor.py",
    }:
        assert migrated not in LEDGERED_EXCEPTIONS

    # The embedded Kùzu runtime stays ledgered (adapter_internal_legitimate) —
    # NOT removed (R05-C constraint).
    embedded = [i for i in r1.importers if i.file.startswith("kg/providers/embedded/")]
    assert embedded
    assert all(i.verdict == "adapter_internal_legitimate" for i in embedded)

    # Ledgered production consumers are non-blocking but recorded with the R05-C
    # REAL-exception rationale (register-before-remove).
    ledgered = [i for i in r1.importers if i.file in LEDGERED_EXCEPTIONS]
    assert ledgered
    assert all(not i.blocking and i.verdict == "migration_allowlisted" for i in ledgered)
    assert any("ledgered REAL exception" in i.rationale for i in ledgered)


def test_ts_fe24d781_every_ledger_exception_has_r05e_removal_contract():
    """The ruling's per-exception contract: owner / reason / target port /
    OBJECTIVE R05-E removal criterion, one record per ledgered file."""
    report = run_kg_schema_import_classification_gate()

    # One detail record per ledgered file present in the scan, in lockstep.
    assert {d["file"] for d in report.ledger_detail} == set(report.ledgered_exceptions)
    assert len(report.ledger_detail) == len(report.ledgered_exceptions)

    for d in report.ledger_detail:
        # Every required field is present and non-empty.
        assert d["owner"].startswith("core")
        assert d["target_port"] in {
            "GraphTransaction",
            "GraphSchemaManager",
            "GraphLifecycle",
            "GraphPathResolver",
            "SemanticGraphStore",
        }
        assert d["reason"] and len(d["reason"]) > 20
        # The removal criterion is OBJECTIVE: it names R05-E and a concrete trigger.
        assert d["r05e_removal_criterion"].startswith("R05-E")
        assert len(d["r05e_removal_criterion"]) > 20


def test_ts_fe24d781_migrated_surface_consumes_the_port_not_kg_schema():
    """services/main.py board-create bootstrap consumes the #06
    GraphSchemaManager port — NOT the direct kg.schema symbol — and is therefore
    no longer an importer at all (proves a REAL migration, not a ledger move)."""
    report = run_kg_schema_import_classification_gate()
    assert "services/main.py" not in {i.file for i in report.importers}

    from okto_pulse.core.services import main as services_main

    src = Path(inspect.getsourcefile(services_main)).read_text(encoding="utf-8")
    # The port is consumed...
    assert "graph_schema_manager.ensure_bootstrapped" in src
    # ...and the old direct kg.schema bootstrap import is gone.
    assert "from okto_pulse.core.kg.schema import ensure_board_graph_bootstrapped" not in src


def test_ts_fe24d781_class_a_sites_consume_graph_transaction_port():
    """Ruling option 2: every ASYNC + simple-execute open_board_connection
    call-site (class A) was migrated to GraphTransaction.begin / scope.execute —
    NOT left calling open_board_connection. Two files thereby left the ledger
    entirely; api/kg_routes.py keeps 2 SYNC sites (class C) so it stays ledgered
    but its async sites are now on the port."""
    import okto_pulse.core.api.kg_routes as kg_routes
    import okto_pulse.core.api.kg_tick as kg_tick
    import okto_pulse.core.kg.canonical_learning_partition as clp

    # Fully migrated files: no open_board_connection left, port consumed.
    for mod in (kg_tick, clp):
        src = Path(inspect.getsourcefile(mod)).read_text(encoding="utf-8")
        assert "graph_transaction.begin" in src, mod.__name__
        assert "open_board_connection" not in src, mod.__name__

    # kg_routes: the two async handlers consume the port; two sync helpers still
    # use open_board_connection (class C, ledgered).
    routes_src = Path(inspect.getsourcefile(kg_routes)).read_text(encoding="utf-8")
    assert routes_src.count("graph_transaction.begin") == 2
    assert "scope.execute" in routes_src

    # The fully-migrated files are off the ledger; kg_routes stays (sync sites).
    report = run_kg_schema_import_classification_gate()
    ledgered = set(report.ledgered_exceptions)
    assert "api/kg_tick.py" not in ledgered
    assert "kg/canonical_learning_partition.py" not in ledgered
    assert "api/kg_routes.py" in ledgered


def test_ts_fe24d781_p2_04_consumers_use_ports_not_raw_graph_connections():
    """R-P2-04: migrated business consumers use the KG ports for read/write
    surfaces. Residual schema imports in these files are limited to formal
    metadata/constants and are non-blocking; raw graph connection/path symbols
    must not reappear.
    """

    migrated_paths = [
        "src/okto_pulse/core/events/handlers/cancellation_decay.py",
        "src/okto_pulse/core/events/handlers/card_boost_recompute.py",
        "src/okto_pulse/core/kg/tier_power.py",
        "src/okto_pulse/core/kg/workers/cognitive_closeout.py",
        "src/okto_pulse/core/services/cognitive_effectiveness_service.py",
        "src/okto_pulse/core/services/discovery_executor.py",
    ]
    forbidden = {
        "open_board_connection",
        "board_kuzu_path",
        "BoardConnection",
    }

    for rel in migrated_paths:
        src = Path(rel).read_text(encoding="utf-8")
        assert not any(symbol in src for symbol in forbidden), rel

    assert "graph_transaction.begin" in Path(
        "src/okto_pulse/core/events/handlers/cancellation_decay.py"
    ).read_text(encoding="utf-8")
    assert "graph_transaction.begin" in Path(
        "src/okto_pulse/core/events/handlers/card_boost_recompute.py"
    ).read_text(encoding="utf-8")
    assert "execute_read_only" in Path(
        "src/okto_pulse/core/kg/tier_power.py"
    ).read_text(encoding="utf-8")


def test_ts_fe24d781_transaction_ledger_reason_is_factually_correct():
    """The corrected wording: the transaction-category reason must NOT claim the
    port lacks an execute surface (it has GraphTransactionScope.execute); it must
    attribute the remaining exceptions to the sync→async boundary (class C)."""
    reason, criterion = _LEDGER_REASON_BY_CATEGORY["transaction"]
    blob = (reason + " " + criterion).lower()
    # The false claim is gone.
    assert "does not expose" not in blob
    assert "no raw-connection" not in blob and "lacks" not in blob
    # The correct cause is stated.
    assert "boundary" in blob or "sync" in blob
    assert "scope.execute" in blob or "execute" in blob


def test_ts_fe24d781_new_unledgered_consumer_blocks(tmp_path):
    rogue = tmp_path / "rogue_kg_consumer.py"
    rogue.write_text(
        "from okto_pulse.core.kg.schema import open_board_connection\n"
        "def f(board_id):\n    return open_board_connection(board_id)\n",
        encoding="utf-8",
    )
    report = run_kg_schema_import_classification_gate(tmp_path)
    entry = next(i for i in report.importers if i.file == "rogue_kg_consumer.py")
    assert entry.blocking is True
    assert entry.verdict == "needs_migration"
    assert entry.target_port == "GraphTransaction"
    assert report.ok is False
    assert entry in report.violations
