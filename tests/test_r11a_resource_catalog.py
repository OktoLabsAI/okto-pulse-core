"""R11-A (CORE) — MCP resource catalog contracts + effective-catalog authority.

Scenario mapping (spec ts_ ids):
  ts_d36fb3e6 (TS01) — the core contracts import in ISOLATION with NO cycle:
       importing resource_catalog pulls neither okto_pulse.community nor the MCP
       server / FastMCP. + Protocol/DTO conformance (fixed per type).
  ts_c8545186 (TS02) — the registry uses the EFFECTIVE catalog as authority;
       _RESOURCE_REGISTRY is an IMMUTABLE derived projection; FREEZE is
       fail-closed (late register after composition raises).
  ts_5b8f32e1 (TS04) — payload budget + link integrity operate over the EFFECTIVE
       catalog (snapshot covers every spec; no broken okto-pulse:// links).
  ts_0862bec0 (TS05) — forbidden scan is SELECTIVE to common: a backend term /
       local path in a COMMON spec reproves; the SAME in an OPERATIONAL
       (Community) spec is allowed.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from okto_pulse.core.ports.mcp_resources import (
    CompositeMcpResourceCatalog,
    McpResourceCatalog,
    McpResourceSpec,
    StaticMcpResourceCatalog,
    catalog_link_integrity,
    scan_forbidden_terms,
)


@pytest.fixture(autouse=True)
def _reset_catalog():
    from okto_pulse.core.mcp import server as srv

    srv.reset_resource_catalog_for_tests()
    yield
    srv.reset_resource_catalog_for_tests()


def _op_spec(uri, content, kind="operational", edition="community"):
    return McpResourceSpec(
        uri=uri, description="d", category="operational", edition=edition,
        kind=kind, content=content,
    )


# ===========================================================================
# ts_d36fb3e6 (TS01) — pure contracts, no cycle + conformance.
# ===========================================================================
def test_ts_d36fb3e6_contracts_import_without_cycle():
    code = (
        "import sys\n"
        "import okto_pulse.core.ports.mcp_resources as rc\n"
        "from okto_pulse.core.ports.mcp_resources import (McpResourceSpec, "
        "McpResourceCatalog, CompositeMcpResourceCatalog, StaticMcpResourceCatalog)\n"
        "bad = [m for m in sys.modules if m.startswith('okto_pulse.community')]\n"
        "assert bad == [], bad\n"
        "assert 'okto_pulse.core.mcp.server' not in sys.modules\n"
        "assert 'fastmcp' not in sys.modules and 'mcp.server' not in sys.modules\n"
        "print('PURE_OK')\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "PURE_OK" in proc.stdout


def test_ts_d36fb3e6_conformance_fixed_per_type():
    # McpResourceCatalog is a runtime_checkable Protocol -> isinstance.
    static = StaticMcpResourceCatalog("core", ())
    comp = CompositeMcpResourceCatalog([static])
    assert isinstance(static, McpResourceCatalog)
    assert isinstance(comp, McpResourceCatalog)
    assert not isinstance(object(), McpResourceCatalog)
    # ...and the methods actually work (not just attribute presence).
    spec = _op_spec("okto-pulse://operational/x", "body")
    static2 = StaticMcpResourceCatalog("community", (spec,))
    assert static2.specs() == (spec,)
    assert comp.with_catalog(static2).specs()  # exercised

    # McpResourceSpec is a frozen DTO -> structural + exercise read()/fields.
    assert spec.uri == "okto-pulse://operational/x"
    assert spec.kind == "operational" and spec.is_common is False
    assert spec.read() == "body"  # content-loader exercised
    with pytest.raises(ValueError):
        McpResourceSpec(uri="bad-scheme", description="d", category="c",
                        edition="core", content="x")  # uri scheme enforced
    with pytest.raises(ValueError):
        McpResourceSpec(uri="okto-pulse://a", description="d", category="c",
                        edition="core", path="a.md")  # path needs base_dir (fail-closed)


# ===========================================================================
# ts_c8545186 (TS02) — effective catalog authority + projection + freeze.
# ===========================================================================
def test_ts_c8545186_effective_catalog_is_authority_projection_and_freeze():
    from okto_pulse.core.mcp import server as srv

    cat = srv.effective_resource_catalog()
    specs = cat.specs()
    assert len(specs) >= 45

    # _RESOURCE_REGISTRY is a DERIVED, IMMUTABLE projection of the catalog.
    assert isinstance(srv._RESOURCE_REGISTRY, tuple)
    assert [r[0] for r in srv._RESOURCE_REGISTRY] == [s.uri for s in specs]

    # Injection extends the effective catalog (authority) + the projection.
    n = len(specs)
    srv.register_resource_catalog(
        StaticMcpResourceCatalog("community", (_op_spec("okto-pulse://operational/inj", "x"),))
    )
    assert len(srv.effective_resource_catalog().specs()) == n + 1
    assert any(r[0] == "okto-pulse://operational/inj" for r in srv._RESOURCE_REGISTRY)

    # FREEZE is fail-closed: a late register/mutation after composition RAISES.
    srv.freeze_resource_catalog()
    with pytest.raises(RuntimeError):
        srv.register_resource_catalog(
            StaticMcpResourceCatalog("community", (_op_spec("okto-pulse://operational/late", "y"),))
        )


# ===========================================================================
# ts_5b8f32e1 (TS04) — payload budget + link integrity over effective catalog.
# ===========================================================================
def test_ts_5b8f32e1_budget_and_link_integrity_over_effective_catalog():
    from okto_pulse.core.mcp import payload_budget
    from okto_pulse.core.mcp import server as srv

    cat = srv.effective_resource_catalog()

    # link integrity: every okto-pulse:// link inside a spec resolves.
    assert catalog_link_integrity(cat) == ()

    # snapshot_resources is catalog-driven: one entry per spec, non-empty for the
    # core path-loaders (resources/read returns content).
    snap = payload_budget.snapshot_resources(srv)
    assert len(snap) == len(cat.specs())
    assert all(len(v) > 0 for v in snap.values())  # no silently-empty resource


# ===========================================================================
# ts_0862bec0 (TS05) — forbidden scan SELECTIVE to common.
# ===========================================================================
def test_ts_0862bec0_forbidden_scan_selective_common_vs_operational():
    common_rogue = StaticMcpResourceCatalog(
        "core",
        (
            McpResourceSpec(
                uri="okto-pulse://workflows/rogue", description="d",
                category="workflows", edition="core", kind="common",
                content=r"we keep graph.lbug in ~/.okto-pulse via Ladybug + SQLite",
            ),
        ),
    )
    operational_ok = StaticMcpResourceCatalog(
        "community",
        (
            McpResourceSpec(
                uri="okto-pulse://operational/kg", description="Ladybug ops",
                category="operational", edition="community", kind="operational",
                content=r"Ladybug graph.lbug at C:\Users\x\.okto-pulse (sqlite)",
            ),
        ),
    )
    comp = CompositeMcpResourceCatalog([common_rogue, operational_ok])
    findings = scan_forbidden_terms(comp)

    common_terms = {f["term"] for f in findings if "rogue" in f["uri"]}
    # backend terms + local path flagged in the COMMON spec.
    assert {".lbug", "ladybug", "sqlite", "<local_path>"} <= common_terms
    # the OPERATIONAL spec naming the same backend is EXEMPT.
    assert [f for f in findings if "operational" in f["uri"]] == []
