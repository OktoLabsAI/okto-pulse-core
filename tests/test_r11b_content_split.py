"""R11-B (CORE) — common forbidden-scan ENFORCED after the split + catalog-aware
gates over the effective catalog.

Scenario mapping (spec ts_ ids):
  ts_d17282f5 (TS02) — the CORE common catalog (no Community overlay) is CLEAN:
       the 10 R11-A debt findings are GONE (forbidden scan ok=True); a synthetic
       rogue backend term re-introduced in a COMMON spec reproves (fail-closed).
  ts_5b73ba44 (TS05) — the gates are catalog-aware over the EFFECTIVE catalog:
       link integrity / uri conflicts / forbidden scan all operate on the merged
       effective catalog; an authorized same-URI overlay merges (operational
       wins) and is NOT a raw conflict, while an UNAUTHORIZED duplicate still is.
  ts_b12ee5a7 (TS01, core slice) — the core common inventory is intact (>=45 URIs)
       and the 4 split URIs are present, scrubbed backend-free.
  ts_7c58a410 (TS06, core slice) — resources/read resolves from the effective
       catalog; the projection is derived (not the authority).
"""

from __future__ import annotations

from okto_pulse.core.ports.mcp_resources import (
    CompositeMcpResourceCatalog,
    McpResourceCatalogError,
    McpResourceReplacementTarget,
    McpResourceSpec,
    StaticMcpResourceCatalog,
    catalog_link_integrity,
    catalog_uri_conflicts,
    scan_forbidden_terms,
)

import pytest

_SPLIT_URIS = (
    "okto-pulse://workflows/kg",
    "okto-pulse://reference/errors",
    "okto-pulse://reference/tool-docs/kg",
    "okto-pulse://reference/tool-docs/decision",
)


@pytest.fixture(autouse=True)
def _reset_catalog():
    from okto_pulse.core.mcp import server as srv

    srv.reset_resource_catalog_for_tests()
    yield
    srv.reset_resource_catalog_for_tests()


# ===========================================================================
# ts_d17282f5 (TS02) — common forbidden scan enforced (the 10 findings gone).
# ===========================================================================
def test_ts_d17282f5_core_common_catalog_is_clean_after_split():
    from okto_pulse.core.mcp import server as srv

    common = srv.effective_resource_catalog()  # core-only = common catalog
    # ZERO forbidden findings (was 10 in R11-A): the split scrubbed the common.
    assert scan_forbidden_terms(common) == ()

    # The four split URIs are present and backend-free in the common catalog.
    specs = {s.uri: s for s in common.specs()}
    for uri in _SPLIT_URIS:
        body = specs[uri].read().lower()
        for term in (".lbug", "ladybug", "sqlite", "kuzu", "sentence-transformers", "~/.okto-pulse"):
            assert term not in body, (uri, term)


def test_ts_d17282f5_rogue_backend_term_in_common_reproves():
    rogue = StaticMcpResourceCatalog(
        "core",
        (
            McpResourceSpec(
                uri="okto-pulse://workflows/kg", description="d", category="workflows",
                edition="core", kind="common",
                content="store the graph in graph.lbug via LadybugDB",
            ),
        ),
    )
    findings = scan_forbidden_terms(rogue)
    terms = {f["term"] for f in findings}
    assert {".lbug", "ladybug"} <= terms  # common spec re-introducing terms reproves

    # ...but the SAME terms in an OPERATIONAL spec are EXEMPT (selective scan).
    operational = StaticMcpResourceCatalog(
        "community",
        (
            McpResourceSpec(
                uri="okto-pulse://operational/kg", description="d", category="operational",
                edition="community", kind="operational", provider="community-embedded-kg",
                capability="kg", content="store the graph in graph.lbug via LadybugDB",
            ),
        ),
    )
    assert scan_forbidden_terms(operational) == ()


# ===========================================================================
# ts_5b73ba44 (TS05) — catalog-aware gates over the effective catalog.
# ===========================================================================
def test_ts_5b73ba44_explicit_replacement_wins_undeclared_duplicate_fails():
    URI = "okto-pulse://workflows/kg"
    common = StaticMcpResourceCatalog(
        "core",
        (McpResourceSpec(uri=URI, description="KG workflow", category="workflows",
                         edition="core", kind="common",
                         content="The KG uses the embedded graph database."),),
    )
    replacement = StaticMcpResourceCatalog(
        "extension",
        (McpResourceSpec(uri=URI, description="ov", category="workflows",
                         edition="extension", kind="operational", provider="p",
                         capability="kg",
                         replace_target=McpResourceReplacementTarget(edition="core"),
                         content="The KG uses LadybugDB at ~/.okto-pulse/graph.lbug."),),
        precedence=10,
    )
    comp = CompositeMcpResourceCatalog([common, replacement])
    merged = {s.uri: s for s in comp.specs()}[URI]

    # The explicit higher-precedence declaration replaces the complete spec.
    assert merged.read() == "The KG uses LadybugDB at ~/.okto-pulse/graph.lbug."
    assert merged.description == "ov" and merged.kind == "operational"
    assert merged.replaced_source_identity == common.specs()[0].source_identity
    assert catalog_uri_conflicts(comp) == ()
    assert scan_forbidden_terms(comp) == ()
    assert catalog_link_integrity(comp) == ()

    # An undeclared same-URI cross-edition claim fails during resolution.
    dup = StaticMcpResourceCatalog(
        "extension",
        (McpResourceSpec(uri=URI, description="d", category="workflows",
                         edition="extension", kind="common", content="dup"),),
        precedence=10,
    )
    with pytest.raises(McpResourceCatalogError) as exc_info:
        CompositeMcpResourceCatalog([common, dup])
    assert exc_info.value.code == "undeclared_replace"


# ===========================================================================
# ts_b12ee5a7 (TS01, core slice) — inventory intact + ts_7c58a410 (TS06 slice).
# ===========================================================================
def test_ts_b12ee5a7_core_inventory_and_projection_derived():
    from okto_pulse.core.mcp import server as srv

    cat = srv.effective_resource_catalog()
    specs = cat.specs()
    assert len(specs) >= 45
    assert set(_SPLIT_URIS) <= {s.uri for s in specs}
    # resources/read resolves from the effective catalog; projection is derived.
    assert [r[0] for r in srv._RESOURCE_REGISTRY] == [s.uri for s in specs]
    assert isinstance(srv._RESOURCE_REGISTRY, tuple)
    # every common spec reads non-empty (no silently-empty after the split).
    assert all(len(s.read()) > 0 for s in specs)
