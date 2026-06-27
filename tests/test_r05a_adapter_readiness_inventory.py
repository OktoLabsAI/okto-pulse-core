"""R05-A — adapter readiness inventory + fail-closed gate.

Scenario mapping (1:1):

  ts_a3695d08 — the initial inventory is complete (every required adapter once,
                all metadata filled).
  ts_6baac761 — the gate blocks an adapter that lacks evidence (negative).
  ts_150d1298 — the gate returns ready ONLY with complete evidence.
  ts_a5c3c55c — the deferred_to_05 reconciliation maps every marker to an
                inventory adapter_key (1:1).
  ts_106fe9f2 — the R05-A diff moves no adapter and changes no runtime: every
                catalogued module still exists and the inventory imports no
                concrete/runtime module (negative).
  ts_9dbe515a — the module imports in isolation without concrete providers /
                community (subprocess + AST).
  ts_088ab292 — the gate reproves a future removal attempt that is not ready
                (negative), with stable diagnostic reasons.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

import okto_pulse.core.application.boundary.adapter_readiness_inventory as _inv_mod
from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    PREDECESSOR_AXES,
    REQUIRED_ADAPTER_KEYS,
    REQUIRED_EVIDENCE,
    AdapterEvidence,
    build_adapter_inventory,
    evaluate_adapter_readiness,
    evaluate_removal,
    run_deferred_reconciliation_gate,
)

INV_PY = Path(_inv_mod.__file__)
SRC_ROOT = INV_PY.parents[4]  # .../src


def _by_key(adapter_key: str):
    return next(e for e in build_adapter_inventory() if e.adapter_key == adapter_key)


def _module_imports(tree) -> set[str]:
    mods: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.add(node.module)
    return mods


# ===========================================================================
# ts_a3695d08 — inventory complete + metadata filled.
# ===========================================================================
def test_ts_a3695d08_inventory_complete_and_metadata_filled():
    inv = build_adapter_inventory()
    keys = [e.adapter_key for e in inv]
    assert len(keys) == len(set(keys)), "duplicate adapter_key in the inventory"
    assert set(keys) == set(REQUIRED_ADAPTER_KEYS), (
        f"missing={set(REQUIRED_ADAPTER_KEYS) - set(keys)} "
        f"extra={set(keys) - set(REQUIRED_ADAPTER_KEYS)}"
    )

    for e in inv:
        assert e.adapter_key and e.owner and e.current_module and e.port_ref
        assert e.wave and e.target_destination and e.removal_criterion.strip()
        # predecessor_refs is PLURAL and every ref is a known axis.
        assert e.predecessor_refs, f"{e.adapter_key} has no predecessor_refs"
        assert all(r in PREDECESSOR_AXES for r in e.predecessor_refs), e.predecessor_refs
        assert e.packages, f"{e.adapter_key} has empty packages"
        assert e.oracles_required, f"{e.adapter_key} has empty oracles_required"
        assert e.status in ("ready", "blocked", "deferred")
        # current_module is a STRING path (not an import), pointing at a .py file.
        assert e.current_module.endswith(".py")
        # metadata is an immutable tuple (not a mutable dict).
        assert isinstance(e.metadata, tuple)

    # #16_schema_migrations is a recognised axis...
    assert "#16_schema_migrations" in PREDECESSOR_AXES
    # ...and every relational adapter (outbox/audit/asyncpg) depends on it,
    # alongside its repository/UoW axis (tr_68514f22).
    for key in (
        "sqlite_outbox_event_bus",
        "sqlalchemy_audit_repository",
        "asyncpg_postgres_driver",
    ):
        refs = _by_key(key).predecessor_refs
        assert "#16_schema_migrations" in refs, f"{key} missing #16"
        assert "#04_repository_uow" in refs, f"{key} missing #04"

    # The axis hints from the spec are honoured (plural refs).
    assert "#08_mcp_auth" in _by_key("mcp_auth_context").predecessor_refs
    assert "#16_schema_migrations" in _by_key("mcp_auth_context").predecessor_refs
    assert _by_key("mcp_auth_context").status == "deferred"  # only R08-A done
    assert "#10_telemetry" in _by_key("local_telemetry_store").predecessor_refs
    assert _by_key("asyncpg_postgres_driver").wave == "R05-E"
    assert "R05-E" in _by_key("asyncpg_postgres_driver").removal_criterion


def test_r_p2_01_raw_sqlite_residuals_are_nominally_governed():
    """R-P2-01: the two raw SQLite residuals must stay explicitly ledgered.

    This test is intentionally independent from REQUIRED_ADAPTER_KEYS so removing
    both the key and the entry cannot make the generic completeness test pass.
    """
    expected = {
        "board_source_store": {
            "module": "okto_pulse/core/kg/board_source_store.py",
            "oracles": {
                "board_source_fetch_parity",
                "source_materialization_unaffected",
            },
        },
        "board_rebuild_ingestion_adapter": {
            "module": "okto_pulse/core/kg/board_rebuild_adapter.py",
            "oracles": {
                "rebuild_enqueue_receipt_parity",
                "rebuild_quarantine_unaffected",
            },
        },
    }

    inv = {entry.adapter_key: entry for entry in build_adapter_inventory()}
    missing = sorted(set(expected) - set(inv))
    assert missing == []

    for key, checks in expected.items():
        entry = inv[key]
        assert entry.current_module == checks["module"]
        assert entry.owner == "okto-pulse-core/kg"
        assert "#04_repository_uow" in entry.predecessor_refs
        assert "stdlib(sqlite3)" in entry.packages
        assert set(checks["oracles"]).issubset(set(entry.oracles_required))
        assert entry.removal_criterion.strip()
        lowered = entry.removal_criterion.lower()
        assert "register-before-remove" in lowered
        assert "does not remove" in lowered
        assert entry.status == "blocked"


# ===========================================================================
# ts_6baac761 — gate blocks an adapter without evidence.
# ===========================================================================
def test_ts_6baac761_gate_blocks_without_evidence():
    entry = _by_key("filesystem_storage_provider")

    # port closed but nothing else -> blocked (fail-closed), with missing list.
    v = evaluate_adapter_readiness(entry, AdapterEvidence(port_closed=True))
    assert v.status == "blocked"
    assert v.is_ready is False
    assert "incomplete_evidence" in v.reasons
    assert set(v.missing_evidence) == set(REQUIRED_EVIDENCE) - {"port_closed"}

    # totally empty evidence -> deferred via the port gateway (never ready).
    v_empty = evaluate_adapter_readiness(entry, AdapterEvidence())
    assert v_empty.status == "deferred"
    assert v_empty.is_ready is False
    assert "port_not_closed" in v_empty.reasons
    # next_specs is PLURAL and lists every gating predecessor.
    assert v_empty.next_specs == entry.predecessor_refs


# ===========================================================================
# ts_150d1298 — ready ONLY with complete evidence.
# ===========================================================================
def _full_evidence() -> AdapterEvidence:
    return AdapterEvidence(**{name: True for name in REQUIRED_EVIDENCE})


def test_ts_150d1298_ready_requires_complete_evidence():
    entry = _by_key("filesystem_storage_provider")

    v = evaluate_adapter_readiness(entry, _full_evidence())
    assert v.status == "ready" and v.is_ready
    assert v.missing_evidence == () and v.failed_evidence == ()

    # dropping ANY single evidence -> not ready.
    for name in REQUIRED_EVIDENCE:
        kw = {n: True for n in REQUIRED_EVIDENCE}
        kw[name] = None
        v_missing = evaluate_adapter_readiness(entry, AdapterEvidence(**kw))
        assert v_missing.status != "ready", f"ready despite missing {name}"

    # a FAILED evidence -> blocked, listed in failed_evidence.
    kw = {n: True for n in REQUIRED_EVIDENCE}
    kw["oracle_passed"] = False
    v_failed = evaluate_adapter_readiness(entry, AdapterEvidence(**kw))
    assert v_failed.status == "blocked"
    assert "oracle_passed" in v_failed.failed_evidence


# ===========================================================================
# ts_a5c3c55c — deferred_to_05 reconciliation 1:1.
# ===========================================================================
def test_ts_a5c3c55c_deferred_to_05_reconciles_with_adapter_key():
    report = run_deferred_reconciliation_gate()
    assert report.ok, report
    assert report.marker_files, "no real deferred_to_05 markers found"
    # Aggregated authoritative deferred provider keys (composition +
    # composition_gate + singleton _global_db debt).
    assert set(report.marker_provider_keys) == {"kg_registry", "kuzu_store"}
    # The inventory covers exactly those keys (1:1).
    assert set(report.inventory_deferred_keys) == {"kg_registry", "kuzu_store"}
    assert report.uncovered_markers == ()  # no marker without an inventory entry
    assert report.phantom_inventory_keys == ()  # no inventory key without a marker
    assert report.entries_missing_criterion == ()  # all have a verifiable criterion


def test_ts_a5c3c55c_marker_files_point_to_real_sources_not_self():
    report = run_deferred_reconciliation_gate()
    # The gate aggregates the real canonical sources (not the ledger).
    assert report.canonical_sources == (
        "okto_pulse.core.composition.DEFERRED_TO_05_PROVIDERS",
        "okto_pulse.core.application.boundary.composition_gate.DEFERRED_SYMBOLS",
        "okto_pulse.core.application.boundary.singleton_gate.SINGLETON_LEDGER[_global_db]",
    )
    # marker_files NEVER include the ledger we just created (no self-reference).
    self_rel = "okto_pulse/core/application/boundary/adapter_readiness_inventory.py"
    assert self_rel not in report.marker_files
    # ...and they DO point at the real source/gate files.
    assert "okto_pulse/core/composition.py" in report.marker_files
    assert (
        "okto_pulse/core/application/boundary/composition_gate.py"
        in report.marker_files
    )
    assert (
        "okto_pulse/core/application/boundary/singleton_gate.py"
        in report.marker_files
    )


def test_metadata_is_immutable():
    entry = build_adapter_inventory()[0]
    # metadata is an immutable tuple, never a mutable dict.
    assert isinstance(entry.metadata, tuple)
    # frozen dataclass: cannot reassign the field.
    import dataclasses

    with pytest.raises(dataclasses.FrozenInstanceError):
        entry.metadata = (("x", "y"),)  # type: ignore[misc]
    # the tuple itself has no in-place mutation API.
    assert not hasattr(entry.metadata, "append")
    assert not hasattr(entry.metadata, "__setitem__")


# ===========================================================================
# R-P2-01 — the two raw-SQLite residuals are governed NOMINALLY (looked up
# DIRECTLY in build_adapter_inventory(), NOT via REQUIRED_ADAPTER_KEYS) so a
# coordinated removal of BOTH the key AND the entry cannot silently satisfy the
# generic coverage check. Governance-only: sqlite3.connect is NOT removed here.
# ===========================================================================
_R_P2_01_RESIDUALS = {
    "board_source_store": {
        "module": "okto_pulse/core/kg/board_source_store.py",
        "oracles": {"board_source_fetch_parity", "source_materialization_unaffected"},
    },
    "board_rebuild_ingestion_adapter": {
        "module": "okto_pulse/core/kg/board_rebuild_adapter.py",
        "oracles": {"rebuild_enqueue_receipt_parity", "rebuild_quarantine_unaffected"},
    },
}


@pytest.mark.parametrize("key", sorted(_R_P2_01_RESIDUALS))
def test_r_p2_01_sqlite_raw_residual_governed_nominally(key):
    spec = _R_P2_01_RESIDUALS[key]
    entry = next(
        (e for e in build_adapter_inventory() if e.adapter_key == key), None
    )
    assert entry is not None, (
        f"R-P2-01: {key} is not registered in build_adapter_inventory()"
    )
    assert entry.current_module == spec["module"]
    # Raw-SQLite governance fields the spec TR requires.
    assert "stdlib(sqlite3)" in entry.packages, (
        f"{key} packages must declare stdlib(sqlite3)"
    )
    assert "#04_repository_uow" in entry.predecessor_refs, (
        f"{key} must gate on the #04 relational boundary"
    )
    assert set(entry.oracles_required) == spec["oracles"], (
        f"{key} oracles_required={entry.oracles_required} != expected {spec['oracles']}"
    )
    # removal_criterion is non-empty AND encodes register-before-remove + the
    # governance-only (no removal in this spec) contract.
    assert entry.removal_criterion.strip()
    assert "register-before-remove" in entry.removal_criterion
    assert "does NOT remove" in entry.removal_criterion


# ===========================================================================
# ts_106fe9f2 — diff moves no adapter / changes no runtime.
# ===========================================================================
def test_ts_106fe9f2_inventory_moves_nothing_and_imports_no_concretes():
    # 1) every catalogued adapter module still EXISTS at its recorded path
    #    (nothing moved / removed by R05-A).
    for e in build_adapter_inventory():
        path = SRC_ROOT / e.current_module
        assert path.exists(), f"adapter module moved/removed: {e.current_module}"

    # 2) the inventory module imports NO concrete adapter / runtime / community.
    mods = _module_imports(ast.parse(INV_PY.read_text(encoding="utf-8")))
    forbidden = (
        "sqlalchemy", "sentence_transformers", "torch", "ladybug", "kuzu",
        "okto_pulse.community",
        "okto_pulse.core.kg.providers.embedded",
        "okto_pulse.core.kg.embedding",
        "okto_pulse.core.infra.storage",
        "okto_pulse.core.infra.database",
        "okto_pulse.core.telemetry",
        "okto_pulse.core.kg.rerank",
    )
    for mod in mods:
        for bad in forbidden:
            assert bad not in mod, f"inventory imports a concrete/runtime module: {mod}"


# ===========================================================================
# ts_9dbe515a — module imports in isolation.
# ===========================================================================
def test_ts_9dbe515a_module_imports_in_isolation(tmp_path):
    code = (
        "import sys\n"
        "import okto_pulse.core.application.boundary.adapter_readiness_inventory as m\n"
        "inv = m.build_adapter_inventory()\n"
        "assert len(inv) >= 1\n"
        "forbidden = (\n"
        "  'sqlalchemy', 'sentence_transformers', 'torch', 'ladybug', 'kuzu',\n"
        "  'okto_pulse.community', 'okto_pulse.core.kg.providers.embedded',\n"
        ")\n"
        "leaked = [n for n in sys.modules if any(\n"
        "  n == p or n.startswith(p + '.') for p in forbidden)]\n"
        "assert not leaked, 'module leaked concrete imports: ' + repr(leaked)\n"
        "print('ISOLATION_OK')\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=90,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "ISOLATION_OK" in proc.stdout

    # AST: the module's TOP-LEVEL imports are stdlib only.
    tree = ast.parse(INV_PY.read_text(encoding="utf-8"))
    top_level_mods: set[str] = set()
    for node in tree.body:  # module body only (not lazy in-function imports)
        if isinstance(node, ast.Import):
            top_level_mods.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            top_level_mods.add(node.module)
    assert top_level_mods <= {"ast", "dataclasses", "pathlib", "typing", "__future__"}, (
        f"non-stdlib top-level import: {top_level_mods}"
    )


# ===========================================================================
# ts_088ab292 — gate reproves a future removal without ready.
# ===========================================================================
def test_ts_088ab292_removal_blocked_without_ready():
    entry = _by_key("kuzu_graph_store")

    # synthetic removal with partial evidence -> blocked (never removed).
    partial = AdapterEvidence(port_closed=True, community_registered=True)
    v = evaluate_removal(entry, partial)
    assert v.status == "blocked"
    assert v.is_ready is False
    assert "incomplete_evidence" in v.reasons

    # diagnostic reasons are STABLE across identical inputs.
    v_again = evaluate_removal(entry, AdapterEvidence(port_closed=True, community_registered=True))
    assert v.reasons == v_again.reasons
    assert v.missing_evidence == v_again.missing_evidence

    # an upstream-gated adapter (mcp auth) is deferred to its predecessor(s).
    mcp = _by_key("mcp_auth_context")
    v_def = evaluate_removal(mcp, AdapterEvidence())
    assert v_def.status == "deferred"
    assert "#08_mcp_auth" in v_def.next_specs
    assert v_def.is_ready is False

    # MULTIPLE un-closed predecessors are ALL listed (never collapsed into one):
    # the asyncpg driver is gated by both #04 repository/UoW and #16 migrations.
    asyncpg = _by_key("asyncpg_postgres_driver")
    v_multi = evaluate_removal(asyncpg, AdapterEvidence())
    assert v_multi.status == "deferred"
    assert v_multi.next_specs == ("#04_repository_uow", "#16_schema_migrations")
    assert "awaiting:#04_repository_uow" in v_multi.reasons
    assert "awaiting:#16_schema_migrations" in v_multi.reasons

    # ONLY complete evidence (ready) permits the removal.
    full = AdapterEvidence(**{name: True for name in REQUIRED_EVIDENCE})
    v_ready = evaluate_removal(entry, full)
    assert v_ready.is_ready and v_ready.status == "ready"
