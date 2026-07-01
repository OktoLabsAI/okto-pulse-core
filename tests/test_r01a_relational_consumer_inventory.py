"""R01A IMP1 — the versioned relational-consumer inventory (FR1 / AC1).

AC1 (ac_6dcd13a6): the versioned inventory lists ``file, line, symbol, cluster,
owner, status`` for *every* call-site detected by rg/AST in scope. These tests
prove the inventory is complete (all six fields, every record), covers the four
functional cluster families (REST/MCP/service/worker), is reproducible
(versioned/diffable) and is no smaller than the documented relational baseline.
"""

from __future__ import annotations

from okto_pulse.core.repositories.relational_boundary_gate import (
    RELATIONAL_BASELINE,
    default_core_path,
    run_relational_boundary_gate,
)
from okto_pulse.core.repositories.relational_consumer_inventory import (
    RELATIONAL_CONSUMER_INVENTORY_SCHEMA_VERSION,
    STATUS_BASELINE,
    build_relational_consumer_inventory,
)

# Symbols FR1 names explicitly.
_EXPECTED_SYMBOL_ROOTS = {"AsyncSession", "select", "get_db", "get_db_for_mcp"}
_REQUIRED_FIELDS = ("file", "line", "symbol", "surface", "cluster", "owner", "status")
_CLUSTER_FAMILIES = {"rest", "mcp", "service", "worker"}


def _inv():
    return build_relational_consumer_inventory()


def test_every_record_carries_all_ac1_fields() -> None:
    """AC1: file, line, symbol, surface, cluster, owner, status for every item."""
    inv = _inv()
    assert inv.total > 0
    for c in inv.consumers:
        record = c.as_dict()
        for field_name in _REQUIRED_FIELDS:
            assert field_name in record, field_name
        assert record["file"] and "/" in record["file"]  # normalised path
        assert "\\" not in record["file"]  # OS-independent
        assert isinstance(record["line"], int) and record["line"] > 0
        assert record["symbol"]
        assert record["surface"] in {"rest", "mcp", "service"}
        assert ":" in record["cluster"]  # family:sub
        assert record["owner"]
        assert record["status"] == STATUS_BASELINE  # fresh scan = nothing migrated


def test_covers_the_four_functional_cluster_families() -> None:
    """FR1/FR3: REST routers, MCP families, service flows and workers are all
    inventoried as distinct cluster families."""
    inv = _inv()
    families = {c.cluster.split(":", 1)[0] for c in inv.consumers}
    assert _CLUSTER_FAMILIES <= families, families
    # roll-ups agree with the per-record families
    for fam in _CLUSTER_FAMILIES:
        assert inv.by_cluster.get(fam, 0) > 0, fam


def test_detects_every_fr1_symbol() -> None:
    """FR1 names Depends(get_db), get_db_for_mcp, AsyncSession, select and ORM."""
    inv = _inv()
    roots = {c.symbol.split("(")[0].split(":")[0] for c in inv.consumers}
    assert _EXPECTED_SYMBOL_ROOTS <= roots, roots
    # Depends(get_db)/Depends(get_db_for_mcp) call-sites are captured
    assert any(c.symbol.startswith("Depends(") for c in inv.consumers)
    # ORM model touches are captured as orm:<Model>
    assert any(c.symbol.startswith("orm:") for c in inv.consumers)


def test_both_transport_surfaces_present() -> None:
    """The MCP surface must NOT escape the inventory (validator's key finding):
    both REST (get_db) and MCP (get_db_for_mcp) call-sites are listed."""
    inv = _inv()
    assert inv.by_surface.get("rest", 0) > 0
    assert inv.by_surface.get("mcp", 0) > 0
    assert any(c.symbol == "get_db_for_mcp" or c.symbol == "Depends(get_db_for_mcp)"
               for c in inv.consumers)


def test_roll_ups_sum_to_total() -> None:
    inv = _inv()
    assert sum(inv.by_surface.values()) == inv.total
    assert sum(inv.by_cluster.values()) == inv.total
    assert sum(inv.by_status.values()) == inv.total


def test_inventory_is_exactly_the_gate_detection() -> None:
    """AC1 completeness, proven 1:1 against the single source of detection truth.

    The inventory must be EXACTLY the relational-boundary gate's detection over
    the whole core — no dropped, duplicated or invented records — only enriched
    with cluster/owner/status. A weak ``total >= floor`` check would pass even if
    the inventory silently dropped half the call-sites; exact set equality will
    not.
    """
    inv = _inv()
    report = run_relational_boundary_gate(root=default_core_path())

    assert inv.total == len(report.violations)

    def _norm(label: str) -> str:
        return label.replace("\\", "/")

    inv_set = {(c.file, c.line, c.symbol, c.surface) for c in inv.consumers}
    report_set = {
        (_norm(v.file), v.line, v.symbol, v.surface) for v in report.violations
    }
    assert inv_set == report_set

    # surface roll-up matches the gate's own bucketing exactly
    assert inv.by_surface == dict(report.violations_by_surface)


def test_inventory_no_smaller_than_documented_baseline() -> None:
    """Secondary sanity: the per-call-site inventory is at least as large as the
    documented core-wide relational baseline (it is finer-grained)."""
    inv = _inv()
    baseline_floor = (
        RELATIONAL_BASELINE["depends_get_db"]
        + RELATIONAL_BASELINE["get_db_for_mcp"]
    )
    assert inv.total >= baseline_floor


def test_inventory_is_versioned_and_deterministic() -> None:
    """AC1 'versionado': stable schema version + reproducible (diffable) output."""
    inv = _inv()
    assert inv.schema_version == RELATIONAL_CONSUMER_INVENTORY_SCHEMA_VERSION
    again = _inv()
    assert inv.as_dict() == again.as_dict()
    # deterministic (file, line, symbol) ordering
    keys = [(c.file, c.line, c.symbol) for c in inv.consumers]
    assert keys == sorted(keys)


def test_scope_is_the_core_package() -> None:
    inv = _inv()
    assert inv.core_root == str(default_core_path())
    assert inv.scanned_files > 0
    assert all(c.file.startswith("core/") for c in inv.consumers)
