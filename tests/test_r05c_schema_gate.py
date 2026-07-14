"""R05-C (CORE target) — kg.schema import classification gate as a blocking
oracle after the register-before-remove ledger was retired.

  ts_fe24d781 — the classification gate is deterministic, the blocking oracle
                PASSES over the real core (zero direct kg.schema runtime
                importers remain), and a NEW consumer BLOCKS.
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

    # Blocking oracle PASSES: no production KG consumer imports the legacy
    # schema runtime anymore.
    assert r1.ok is True
    assert r1.violations == []
    assert r1.importers == []

    # The R05-C ledger has been retired: the real core has no schema importers.
    assert LEDGERED_EXCEPTIONS == frozenset()
    assert r1.ledgered_exceptions == []
    # Migrated off the ledger: services/main.py (37->36), then the class-A
    # GraphTransaction migrations dropped api/kg_routes.py, api/kg_tick.py +
    # kg/canonical_learning_partition.py. R-P2-04 then migrated six additional
    # business consumers to CypherExecutor / GraphTransaction ports. R-P2-05 then
    # moved safe-write/schema lifecycle call sites behind registry ports and
    # outbox board reads behind CypherExecutor, board cascade wipes behind
    # GraphTransaction, orphan integrity scans behind graph ports, and cognitive
    # preservation behind CypherExecutor/GraphTransaction.
    assert len(r1.ledgered_exceptions) == 0
    for migrated in {
        "api/kg_rebuild.py",
        "services/main.py",
        "api/kg_routes.py",
        "api/kg_tick.py",
        "events/handlers/cognitive_extraction.py",
        "events/handlers/kg_decay_tick.py",
        "events/handlers/kg_hit_recompute.py",
        "kg/__init__.py",
        "kg/board_rebuild_adapter.py",
        "kg/canonical_partition_integrity.py",
        "kg/canonical_stale_reconciler.py",
        "kg/health.py",
        "kg/kg_service.py",
        "kg/cognitive_closeout_production.py",
        "kg/canonical_cognitive_preservation.py",
        "kg/global_discovery/clustering.py",
        "kg/global_discovery/outbox_worker.py",
        "kg/governance.py",
        "kg/hybrid_search/kuzu_adapter.py",
        "kg/orphan_integrity.py",
        "kg/primitives.py",
        "kg/rebuild_service.py",
        "kg/search.py",
        "kg/canonical_learning_partition.py",
        "kg/schema_layer_guard.py",
        "kg/stale_canonical_parity.py",
        "kg/workers/consolidation.py",
        "mcp/server.py",
        "events/handlers/cancellation_decay.py",
        "events/handlers/card_boost_recompute.py",
        "kg/tier_power.py",
        "kg/workers/cognitive_closeout.py",
        "services/cognitive_effectiveness_service.py",
        "services/discovery_executor.py",
        "services/kg_health_service.py",
    }:
        assert migrated not in LEDGERED_EXCEPTIONS

    # R-P2-05 removed the embedded Kuzu provider modules from core; no provider
    # under kg/providers/embedded may remain as an adapter-internal importer.
    embedded = [i for i in r1.importers if i.file.startswith("kg/providers/embedded/")]
    assert embedded == []

    # No ledgered production consumer remains.
    ledgered = [i for i in r1.importers if i.file in LEDGERED_EXCEPTIONS]
    assert ledgered == []


def test_ts_fe24d781_every_ledger_exception_has_r05e_removal_contract():
    """The ruling's ledger is empty once every exception is migrated."""
    report = run_kg_schema_import_classification_gate()

    # One detail record per ledgered file present in the scan, in lockstep.
    assert {d["file"] for d in report.ledger_detail} == set(report.ledgered_exceptions)
    assert len(report.ledger_detail) == len(report.ledgered_exceptions)

    assert report.ledger_detail == []


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
    call-site (class A) was migrated behind GraphTransaction / scope.execute and
    is NOT left calling open_board_connection. The Community tick route delegates
    through its UoW service; Core application orchestration owns the graph write.
    ``api/kg_routes.py`` also migrated its sync edge diagnostics to
    CypherExecutor, so it stays off the ledger.

    Since the R01A strangler (50e3193) only ``get_kg_metrics`` consumes the
    transaction port directly in the route module; ``boost_node`` now goes
    through ``BoostNodeUseCase`` and its graph read/SET lives in
    ``kg.governance.boost_node``, which consumes the same port."""
    import okto_pulse.community.api.kg_routes as kg_routes
    import okto_pulse.community.api.kg_tick as kg_tick
    import okto_pulse.core.application.kg_tick as kg_tick_application
    import okto_pulse.core.kg.canonical_learning_partition as clp
    import okto_pulse.core.kg.governance as kg_governance

    # The transport is persistence-agnostic and delegates through the composed
    # service catalog rather than opening a graph transaction itself.
    tick_route_src = Path(inspect.getsourcefile(kg_tick)).read_text(encoding="utf-8")
    assert "db.services.kg.dispatch_manual_tick" in tick_route_src
    assert "graph_transaction.begin" not in tick_route_src
    assert "open_board_connection" not in tick_route_src

    # The Core application policy owns the force-rebuild graph transaction.
    tick_application_src = Path(inspect.getsourcefile(kg_tick_application)).read_text(
        encoding="utf-8"
    )
    assert "get_kg_registry().graph_transaction" in tick_application_src
    assert "transaction.begin" in tick_application_src
    assert "open_board_connection" not in tick_application_src

    clp_src = Path(inspect.getsourcefile(clp)).read_text(encoding="utf-8")
    assert "graph_transaction.begin" in clp_src
    assert "open_board_connection" not in clp_src

    # kg_routes: get_kg_metrics consumes the transaction port and the
    # sync edge diagnostics consume CypherExecutor; no raw connection remains.
    # boost_node was strangled to the use-case layer in R01A (50e3193).
    routes_src = Path(inspect.getsourcefile(kg_routes)).read_text(encoding="utf-8")
    assert routes_src.count("resolve_graph_transaction().begin") == 1
    assert "scope.execute" in routes_src
    assert "open_board_connection" not in routes_src

    # The strangled boost path still consumes the port from governance —
    # it did not regress to a raw connection.
    governance_src = Path(inspect.getsourcefile(kg_governance)).read_text(encoding="utf-8")
    assert "graph_transaction.begin" in governance_src

    # The fully-migrated files are off the ledger.
    report = run_kg_schema_import_classification_gate()
    ledgered = set(report.ledgered_exceptions)
    assert "api/kg_routes.py" not in ledgered
    assert "api/kg_tick.py" not in ledgered
    assert "kg/canonical_learning_partition.py" not in ledgered


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
