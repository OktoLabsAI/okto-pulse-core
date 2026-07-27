"""R11-C (CORE) — MCP-resource tests migrated to be CATALOG-AWARE (consume the
effective catalog, not server.py source text / _RESOURCE_REGISTRY as authority).

Test-only migration (no production change — snapshot_resources already reads the
effective catalog since R11-A). EXTENDS R11-A/B/D; does not revert/duplicate them.

Scenario mapping:
  TS02/TS06 — the legacy _RESOURCE_REGISTRY is a DERIVED, IMMUTABLE projection of
       the effective catalog (same order + URIs); the catalog is the authority.
  TS03 — payload_budget.snapshot_resources measures the EFFECTIVE composed
       catalog (one entry per effective spec, incl. Community overlays).
  TS05 — load-bearing direct negatives (broken link / unauthorized duplicate URI
       / common forbidden term / empty resource) fail deterministically; the
       freeze-late-mutation negative is COVERED by R11-A (referenced, not duped).
  TS07 — canonical-manifest regression guard: resource cardinality and URI order
       are derived from the frozen manifest; payload remains bounded. The wide
       PRE-EXISTING failures (r1 tool-count drift, f16/f17 KG-health) are
       TOOL/health baseline, OUT of R11-C scope.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okto_pulse.core.mcp import payload_budget
from okto_pulse.core.mcp import server as srv
from okto_pulse.core.ports.mcp_resources import (
    CompositeMcpResourceCatalog,
    McpResourceCatalogError,
    McpResourceReplacementTarget,
    McpResourceSpec,
    StaticMcpResourceCatalog,
    catalog_link_integrity,
    scan_forbidden_terms,
)

#: generous total-payload ceiling (chars) — catches content bloat, not normal docs.
_RESOURCE_PAYLOAD_CEILING = 800_000


@pytest.fixture(autouse=True)
def _reset_catalog():
    srv.reset_resource_catalog_for_tests()
    yield
    srv.reset_resource_catalog_for_tests()


# ===========================================================================
# TS02 / TS06 — projection is a derived, immutable bridge (not authority).
# ===========================================================================
def test_ts02_ts06_registry_is_derived_immutable_projection():
    cat = srv.effective_resource_catalog()
    specs = cat.specs()
    # the catalog is the authority; _RESOURCE_REGISTRY mirrors it (same ORDER+URIs).
    assert [r[0] for r in srv._RESOURCE_REGISTRY] == [s.uri for s in specs]
    # it is an IMMUTABLE tuple (not a mutable list) — cannot be mutated to drift.
    assert isinstance(srv._RESOURCE_REGISTRY, tuple)
    with pytest.raises((TypeError, AttributeError)):
        srv._RESOURCE_REGISTRY.append(("x", "y", "z"))  # type: ignore[attr-defined]
    # every entry is the (uri, projection-path, description) shape.
    assert all(len(e) == 3 for e in srv._RESOURCE_REGISTRY)


# ===========================================================================
# TS03 — payload budget snapshot measures the EFFECTIVE composed catalog.
# ===========================================================================
def test_ts03_snapshot_measures_effective_catalog():
    # core-only effective catalog: one snapshot entry per spec, all non-empty.
    cat = srv.effective_resource_catalog()
    snap = payload_budget.snapshot_resources(srv)
    assert len(snap) == len(cat.specs())
    assert all(len(v) > 0 for v in snap.values())

    # Inject an explicit higher-precedence replacement; the snapshot follows the
    # effective declaration selected by the resolver.
    replacement = StaticMcpResourceCatalog(
        "extension",
        (
            McpResourceSpec(
                uri="okto-pulse://workflows/kg",
                description="ov",
                category="workflows",
                edition="extension",
                kind="operational",
                provider="p",
                capability="kg",
                replace_target=McpResourceReplacementTarget(edition="core"),
                content="OVERLAY BODY",
            ),
        ),
        precedence=10,
    )
    srv.register_resource_catalog(replacement)
    snap2 = payload_budget.snapshot_resources(srv)
    # still one entry per effective spec (overlay merged in-place, not added).
    assert len(snap2) == len(srv.effective_resource_catalog().specs())
    # the merged overlay content is what the snapshot measures for that URI.
    assert snap2.get("okto-pulse://workflows/kg") == "OVERLAY BODY"


# ===========================================================================
# TS05 — load-bearing direct negatives (+ R11-A freeze reference).
# ===========================================================================
def test_ts05_load_bearing_negatives_fail_deterministically():
    # (1) broken okto-pulse:// link -> link integrity flags it.
    broken = StaticMcpResourceCatalog(
        "core",
        (
            McpResourceSpec(
                uri="okto-pulse://workflows/x",
                description="see okto-pulse://ghost/missing",
                category="workflows",
                edition="core",
                content="ref okto-pulse://also/missing",
            ),
        ),
    )
    bad_links = catalog_link_integrity(broken)
    assert {b["link"] for b in bad_links} == {
        "okto-pulse://ghost/missing",
        "okto-pulse://also/missing",
    }

    # (2) Undeclared cross-edition duplicate -> fail before publication.
    with pytest.raises(McpResourceCatalogError) as exc_info:
        CompositeMcpResourceCatalog(
            [
                StaticMcpResourceCatalog(
                    "core",
                    (
                        McpResourceSpec(
                            uri="okto-pulse://a/b",
                            description="d",
                            category="a",
                            edition="core",
                            content="1",
                        ),
                    ),
                ),
                StaticMcpResourceCatalog(
                    "extension",
                    (
                        McpResourceSpec(
                            uri="okto-pulse://a/b",
                            description="d",
                            category="a",
                            edition="extension",
                            content="2",
                        ),
                    ),
                    precedence=10,
                ),
            ]
        )
    assert exc_info.value.code == "undeclared_replace"

    # (3) common spec with a forbidden backend term -> selective scan reproves.
    rogue = StaticMcpResourceCatalog(
        "core",
        (
            McpResourceSpec(
                uri="okto-pulse://workflows/y",
                description="d",
                category="workflows",
                edition="core",
                kind="common",
                content="store graph.lbug via Ladybug",
            ),
        ),
    )
    assert {f["term"] for f in scan_forbidden_terms(rogue)} >= {".lbug", "ladybug"}

    # (4) empty resource (path-loader without base_dir) -> fail-closed at build.
    with pytest.raises(ValueError):
        McpResourceSpec(
            uri="okto-pulse://z",
            description="d",
            category="z",
            edition="core",
            path="x.md",
        )

    # (5) freeze-late-mutation: registering after freeze FAILS closed. This is the
    #     guard R11-A established (test_ts_c8545186); exercised here directly as a
    #     load-bearing negative (self-contained, no fragile cross-module import).
    srv.freeze_resource_catalog()
    with pytest.raises(RuntimeError):
        srv.register_resource_catalog(
            StaticMcpResourceCatalog(
                "community",
                (
                    McpResourceSpec(
                        uri="okto-pulse://late/mutation",
                        description="d",
                        category="late",
                        edition="community",
                        content="x",
                    ),
                ),
            )
        )


# ===========================================================================
# TS07 — manifest-driven guard + pre-existing-failure characterization.
# ===========================================================================
def test_ts07_resource_manifest_and_payload_are_consistent():
    frozen = srv.freeze_resource_catalog()
    specs = frozen.specs()
    manifest = frozen.manifest
    assert len(specs) == manifest.count
    assert tuple(spec.uri for spec in specs) == tuple(
        entry.uri for entry in manifest.entries
    )
    total = sum(len(s.read()) for s in specs)
    assert total <= _RESOURCE_PAYLOAD_CEILING, f"resource payload bloated: {total}"


def test_ts07_preexisting_wide_failures_are_out_of_scope_baseline():
    """Carry-forward #2: the wide PRE-EXISTING failures are a TOOL/health baseline,
    NOT MCP-resource regressions, so the R11-C resource guard must not own them."""
    # The R1 file owns a pinned tool-count guard; its exact reviewed count is a
    # tool-surface concern, orthogonal to the effective RESOURCE catalog.
    r1_src = (
        Path(__file__)
        .parent.joinpath("test_r1_tool_compaction.py")
        .read_text(encoding="utf-8")
    )
    assert "def test_tool_names_stable_after_compaction" in r1_src
    assert "assert len(names) ==" in r1_src
    resource_test_source = Path(__file__).read_text(encoding="utf-8")
    forbidden_literal = "_RESOURCE_COUNT_" + "BASELINE"
    assert forbidden_literal not in resource_test_source
    # the f16/f17 KG-health meta-suite is a health-gate concern, also not resources.
    f16_src = (
        Path(__file__)
        .parent.joinpath("test_r1_degraded_kg_fallback_gate.py")
        .read_text(encoding="utf-8")
    )
    assert "f16_f17" in f16_src
    # and R11-C's surface — the effective resource catalog — is stable + scan-clean
    # (these pre-existing failures touch neither).
    frozen = srv.freeze_resource_catalog()
    assert len(frozen.specs()) == frozen.manifest.count
    assert scan_forbidden_terms(frozen) == ()  # common catalog clean (R11-B)
