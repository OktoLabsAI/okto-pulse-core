"""
Doc-content guards for spec 8ba6d89d (F14 — bug-flow x content-lock reconciliation).

These are pure doc-content tests (``read_text`` + literal/substring assertions,
no server/board/graph), mirroring ``tests/test_mcp_resources.py``. They prove the
always-surfaced bug-flow docs (``workflows/cards.md`` + ``reference/card_types.md``),
the reference error matrix (``reference/errors.md``), and the card tool-docs
resource were reconciled to the two-path bug regression rule.

Covers the spec's six acceptance criteria:
- AC1 (ts_ea2507ac) — cards.md carries the traceability-only reuse rule.
- AC2 (ts_597f5975) — card_types.md drops the stale new-scenario wording.
- AC3 (ts_2ca22443) — the reconciliation assertion is load-bearing (negative-wiring).
- AC4 (ts_6f22a3ba) — canonical gate remains intact and tool-docs carry the
  two-path eligibility rule.
- AC5 (ts_c7c73270) — cards.md forward-points to the canonical deferred doc + SpecLockedError.
- AC6 (ts_4f725f5e) — edited resources remain valid registered MCP resources.
"""

import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

RESOURCES_DIR = (
    Path(__file__).parent.parent / "src" / "okto_pulse" / "core" / "mcp" / "resources"
)
CORE_DIR = Path(__file__).parent.parent / "src" / "okto_pulse" / "core"

CARDS_MD = RESOURCES_DIR / "workflows" / "cards.md"
CARD_TYPES_MD = RESOURCES_DIR / "reference" / "card_types.md"
ERRORS_MD = RESOURCES_DIR / "reference" / "errors.md"
CANONICAL_CARD_DOC = RESOURCES_DIR / "reference" / "tool-docs" / "card.md"
GATE_MAIN = CORE_DIR / "services" / "main.py"

# Stale wording that the reconciliation must remove (lower-cased comparison).
STALE_NEW_SCENARIO = "create new test scenario"
STALE_PREEXISTING_DONT_COUNT = "pre-existing scenarios don't count"


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Load-bearing predicates (shared by the positive checks and AC3 negative-wiring)
# ---------------------------------------------------------------------------


def _cards_md_reconciled(text: str) -> bool:
    """True iff workflows/cards.md describes the canonical bug paths."""
    low = text.lower()
    no_stale = STALE_NEW_SCENARIO not in low
    has_reuse = "reuse eligible existing scenario" in low
    has_traceability_only = "traceability-only update" in low
    has_unchanged_content = "leave validated spec content unchanged" in low
    has_lineage = "origin_task_id" in low and "affected_task_ids" in low
    has_semantic_gap = "path b" in low and "semantic gap" in low
    has_hotfix_lane = "path c" in low and "hotfix lane" in low
    return (
        no_stale
        and has_reuse
        and has_traceability_only
        and has_unchanged_content
        and has_lineage
        and has_semantic_gap
        and has_hotfix_lane
    )


def _card_types_reconciled(text: str) -> bool:
    """True iff reference/card_types.md states pre-existing scenarios DO count."""
    low = text.lower()
    no_stale_new = STALE_NEW_SCENARIO not in low
    no_stale_dont_count = STALE_PREEXISTING_DONT_COUNT not in low
    has_do_count = "pre-existing scenarios do count" in low
    has_task_temporal = "applies to the test task, not the scenario" in low
    has_lineage = "eligible by lineage" in low
    has_semantic_gap = "semantic gap" in low
    return (
        no_stale_new
        and no_stale_dont_count
        and has_do_count
        and has_task_temporal
        and has_lineage
        and has_semantic_gap
    )


# ---------------------------------------------------------------------------
# AC1 — workflows/cards.md carries the traceability-only reuse rule
# ---------------------------------------------------------------------------


def test_ac1_cards_md_carries_traceability_only_reuse_rule() -> None:
    low = _read(CARDS_MD).lower()
    assert STALE_NEW_SCENARIO not in low, (
        "cards.md still instructs 'create NEW test scenario' at the bug-flow step."
    )
    assert "reuse eligible existing scenario" in low
    assert "okto_pulse_resolve_bug_regression_scenarios" in low
    assert "origin_task_id" in low
    assert "affected_task_ids" in low
    assert "traceability-only update" in low
    assert "leave validated spec content unchanged" in low
    assert "path b" in low
    assert "semantic gap" in low
    assert "path c" in low
    assert "hotfix lane" in low


# ---------------------------------------------------------------------------
# AC2 — reference/card_types.md drops the stale new-scenario wording
# ---------------------------------------------------------------------------


def test_ac2_card_types_drops_stale_wording() -> None:
    low = _read(CARD_TYPES_MD).lower()
    assert STALE_NEW_SCENARIO not in low, "card_types.md still says 'create NEW test scenarios'."
    assert STALE_PREEXISTING_DONT_COUNT not in low, (
        "card_types.md still says \"pre-existing scenarios don't count\"."
    )
    assert "pre-existing scenarios do count" in low
    assert "applies to the test task, not the scenario" in low
    assert "eligible by lineage" in low
    assert "okto_pulse_resolve_bug_regression_scenarios" in low
    assert "same-spec membership alone is not enough" in low
    assert "semantic gap" in low


# ---------------------------------------------------------------------------
# AC3 — the reconciliation assertion is load-bearing (negative-wiring)
# ---------------------------------------------------------------------------


def test_ac3_reconciliation_is_load_bearing() -> None:
    # Positive: the real, edited docs satisfy the predicates.
    assert _cards_md_reconciled(_read(CARDS_MD))
    assert _card_types_reconciled(_read(CARD_TYPES_MD))

    # Negative-wiring: synthetic STALE text must FAIL the same predicates,
    # proving the checks are not vacuous passes.
    synthetic_stale_cards = "2. Triage & create NEW test scenario(s) on the spec\n"
    assert not _cards_md_reconciled(synthetic_stale_cards)

    synthetic_stale_card_types = (
        "2. Triage & create NEW test scenarios\n"
        "   pre-existing scenarios don't count\n"
    )
    assert not _card_types_reconciled(synthetic_stale_card_types)


# ---------------------------------------------------------------------------
# AC4 — canonical gate remains intact and tool-docs carry two-path guidance
# ---------------------------------------------------------------------------


def test_ac4_canonical_sources_intact() -> None:
    # The deferred-tier rule in the card tool-docs is now the canonical
    # long-form description for agents.
    card_doc = _read(CANONICAL_CARD_DOC)
    assert "leaves validated spec content unchanged" in card_doc
    assert "Bug regression coverage may reuse an" in card_doc
    assert "eligible by lineage" in card_doc
    assert "okto_pulse_resolve_bug_regression_scenarios" in card_doc
    assert "semantic_gap_required" in card_doc
    assert "next_action=escalate_semantic_gap" in card_doc
    assert "Do not move a spec directly from in_progress to approved" in card_doc

    # The relaxed move-card bug gate text in services/main.py is intact.
    gate = _read(GATE_MAIN)
    assert "class SpecLockedError(Exception):" in gate
    assert "leave spec content unchanged for Path A regression evidence" in gate
    assert "The referenced scenario may be an existing scenario on a" in gate


# ---------------------------------------------------------------------------
# AC5 — cards.md forward-points to the canonical deferred doc + SpecLockedError
# ---------------------------------------------------------------------------


def test_ac5_cards_md_forward_pointer() -> None:
    text = _read(CARDS_MD)
    assert "reference/tool-docs/card.md" in text
    assert "SpecLockedError" in text
    assert "reference/errors.md" in text

    # The forward pointer must resolve: errors.md actually carries a SpecLockedError entry.
    assert "SpecLockedError" in _read(ERRORS_MD)
    assert "reason=unrelated_scenario" in _read(ERRORS_MD)
    assert "reason=cross_spec_scenario" in _read(ERRORS_MD)
    assert "semantic_gap_required=true" in _read(ERRORS_MD)


def test_ac5_error_guidance_routes_semantic_gap_without_spec_editing() -> None:
    low = _read(ERRORS_MD).lower()
    assert "okto_pulse_resolve_bug_regression_scenarios" in low
    assert "scenario_not_found" in low
    assert "unrelated_scenario" in low
    assert "cross_spec_scenario" in low
    assert "path b" in low
    assert "semantic gap" in low
    assert "leave the current validated spec content unchanged for simple path a reuse" in low
    assert "assign_hotfix_lane" in low
    assert "activate_hotfix_lane" in low
    assert "in_progress -> approved" not in low
    assert "in_progress to approved" not in low


# ---------------------------------------------------------------------------
# AC6 — edited resources remain valid registered MCP resources
# ---------------------------------------------------------------------------


def test_ac6_edited_files_valid_registered_resources() -> None:
    edited = {
        "okto-pulse://workflows/cards": (CARDS_MD, "workflows/cards.md"),
        "okto-pulse://reference/card_types": (CARD_TYPES_MD, "reference/card_types.md"),
        "okto-pulse://reference/errors": (ERRORS_MD, "reference/errors.md"),
        "okto-pulse://reference/tool-docs/card": (
            CANONICAL_CARD_DOC,
            "reference/tool-docs/card.md",
        ),
    }

    # Frontmatter intact on every edited file.
    for uri, (path, _rel) in edited.items():
        content = _read(path)
        assert content.startswith("---"), f"{path.name}: missing leading frontmatter block."
        match = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
        assert match is not None, f"{path.name}: frontmatter block not closed."
        assert 'version: "1.0"' in match.group(1), f"{path.name}: frontmatter missing version."

    # URIs still resolve to the edited files in the live registry.
    from okto_pulse.core.mcp import server as _srv

    registered = {entry[0]: entry[1] for entry in _srv._RESOURCE_REGISTRY}
    for uri, (_path, rel) in edited.items():
        assert registered.get(uri) == rel, (
            f"Registry entry for {uri!r} is {registered.get(uri)!r}, expected {rel!r}."
        )


# ---------------------------------------------------------------------------
# AC5/TS3 (card 6c489eef) — the CONTENT SERVED through the okto-pulse:// resource
# mechanism (registry + _load_resource_file, the exact path an agent reads) must
# carry the reconciled Path B sequence and NONE of the stale/unsafe remediation
# instructions. This is stricter than asserting the raw file: it follows the
# registry URI -> relative path -> loader, so it also catches a registry that
# points a URI at the wrong/stale file. (The live MCP can still serve cached
# stale content if its long-running process was not restarted after an edit —
# that is an operational redeploy concern, NOT a source defect; this test pins
# the source + serving path the next restart will pick up.)
# ---------------------------------------------------------------------------

# The generic pre-Path-B remediation phrasings that must never be SERVED again.
_FORBIDDEN_PATH_B_REMEDIATION = (
    "refinement, spec revision, or hotfix spec",
    "route to amendment, refinement",
)

# Every Path B doc resource, by the URI an agent actually reads.
_PATH_B_RESOURCE_URIS = (
    "okto-pulse://workflows/cards",
    "okto-pulse://reference/errors",
    "okto-pulse://reference/tool-docs/card",
    "okto-pulse://reference/card_types",
)


def test_ac7_exposed_path_b_resources_serve_reconciled_content() -> None:
    from okto_pulse.core.mcp import server as _srv

    registry = {uri: rel for uri, rel, _ in _srv._RESOURCE_REGISTRY}

    # 1. No Path B resource SERVED via the registry may carry the old generic
    #    "amendment, refinement, spec revision, or hotfix spec" remediation.
    for uri in _PATH_B_RESOURCE_URIS:
        rel = registry.get(uri)
        assert rel is not None, f"{uri} is not registered."
        served = _srv._load_resource_file(rel)
        assert served, f"{uri} served empty content."
        low = served.lower()
        for bad in _FORBIDDEN_PATH_B_REMEDIATION:
            assert bad not in low, f"{uri} still SERVES stale Path B remediation: {bad!r}"

    # 2. The SERVED cards workflow + card_types reference satisfy the same
    #    load-bearing reconciliation predicates as their files (AC3 proves the
    #    predicates are non-vacuous), now exercised through the loader.
    assert _cards_md_reconciled(_srv._load_resource_file(registry["okto-pulse://workflows/cards"]))
    assert _card_types_reconciled(
        _srv._load_resource_file(registry["okto-pulse://reference/card_types"])
    )

    # 3. The served cards workflow points Path B at the formal amendment lineage
    #    (not a generic refinement/hotfix-spec detour) and keeps Path C non-substitutive.
    cards_low = _srv._load_resource_file(registry["okto-pulse://workflows/cards"]).lower()
    assert "amendmenthotfixrevision" in cards_low or "amendment revision" in cards_low
    # Path C is non-substitutive (markdown emphasis may wrap "not", so match the
    # distinctive tail rather than the full sentence).
    assert "replace path b" in cards_low

    # Negative-wiring: the forbidden-phrase guard is NOT vacuous — synthetic stale
    # remediation text must trip at least one forbidden marker (teeth-by-construction,
    # no shared-file mutation).
    synthetic_stale = "If none, route to amendment, refinement, spec revision, or hotfix spec."
    assert any(bad in synthetic_stale.lower() for bad in _FORBIDDEN_PATH_B_REMEDIATION)


def test_ac8_every_registered_resource_serves_nonempty_content() -> None:
    """Packaging guard against shipping a build/release that omits a load-bearing
    resource file. EVERY okto-pulse:// resource in the registry must load
    NON-EMPTY content through the serving loader: a wheel that failed to bundle a
    ``resources/*.md`` file surfaces here as an empty served body (the loader maps
    a missing file to ``""``). Run against the installed package in the release
    gate, this also pins that the docs the agent reads were actually shipped."""
    from okto_pulse.core.mcp import server as _srv

    empty = [
        (uri, rel)
        for uri, rel, _desc in _srv._RESOURCE_REGISTRY
        if not _srv._load_resource_file(rel).strip()
    ]
    assert not empty, f"Registered resources served EMPTY (missing from package?): {empty}"
