"""
Tests for MCP resource files introduced by spec P0.A (TR-A5).

Covers:
- All 12 resource .md files exist and have frontmatter
- Root agent_instructions.md is ≤500 lines
- _load_resource_file() handles missing files gracefully
- effective catalog URIs are consistent with their resolvable content
- the effective catalog serves okto-pulse:// URIs for all resources
"""

import re
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RESOURCES_DIR = (
    Path(__file__).parent.parent / "src" / "okto_pulse" / "core" / "mcp" / "resources"
)
MCP_DIR = Path(__file__).parent.parent / "src" / "okto_pulse" / "core" / "mcp"
SERVER_PY = MCP_DIR / "server.py"

# Core set that must ALWAYS exist (deleting one of these is a breaking change
# to the instructions' Quick Navigation table).
_REQUIRED_FILES = [
    "workflows/stories.md",
    "workflows/ideations.md",
    "workflows/refinements.md",
    "workflows/specs.md",
    "workflows/cards.md",
    "workflows/sprints.md",
    "workflows/kg.md",
    "workflows/preflight.md",
    "reference/errors.md",
    "reference/knowledge-governance.md",
    "reference/multivalue.md",
    "reference/destructive_ops.md",
    "reference/card_types.md",
    "reference/spec_gates.md",
    "reference/projection_profiles.md",
    "reference/policy-compliance.md",
]

# 2026-07-12 (auditoria MCP, achado #34): the gate now discovers EVERY
# bundled resource via glob, so a new .md automatically enters the
# frontmatter/version checks instead of silently drifting outside them.
EXPECTED_FILES = sorted(
    str(p.relative_to(RESOURCES_DIR)).replace("\\", "/")
    for p in RESOURCES_DIR.rglob("*.md")
)


def test_required_core_resources_present() -> None:
    missing = [f for f in _REQUIRED_FILES if f not in EXPECTED_FILES]
    assert missing == [], f"core resources missing from bundle: {missing}"

EXPECTED_URIS = [
    "okto-pulse://workflows/stories",
    "okto-pulse://workflows/ideations",
    "okto-pulse://workflows/refinements",
    "okto-pulse://workflows/specs",
    "okto-pulse://workflows/cards",
    "okto-pulse://workflows/sprints",
    "okto-pulse://workflows/kg",
    "okto-pulse://workflows/preflight",
    "okto-pulse://reference/errors",
    "okto-pulse://reference/knowledge-governance",
    "okto-pulse://reference/multivalue",
    "okto-pulse://reference/destructive_ops",
    "okto-pulse://reference/card_types",
    "okto-pulse://reference/spec_gates",
    "okto-pulse://reference/projection-profiles",
    "okto-pulse://reference/policy-compliance",
]


# ---------------------------------------------------------------------------
# 1. All 12 resource files must exist
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rel_path", EXPECTED_FILES)
def test_resource_file_exists(rel_path: str) -> None:
    """Each resource .md file must exist under resources/."""
    full = RESOURCES_DIR / rel_path
    assert full.exists(), f"Resource file missing: {full}"
    assert full.is_file(), f"Path exists but is not a file: {full}"


# ---------------------------------------------------------------------------
# 2. Each file must have frontmatter with version: "1.0"
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rel_path", EXPECTED_FILES)
def test_resource_file_has_frontmatter(rel_path: str) -> None:
    """Each resource .md file must begin with --- / version: '1.0' / --- frontmatter."""
    full = RESOURCES_DIR / rel_path
    if not full.exists():
        pytest.skip(f"File missing (covered by test_resource_file_exists): {rel_path}")
    content = full.read_text(encoding="utf-8")
    # Frontmatter must start at the very first line
    assert content.startswith("---"), (
        f"{rel_path}: file does not start with '---' frontmatter block"
    )
    # version: "1.0" must appear within the frontmatter
    # (between first '---' and second '---')
    match = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
    assert match is not None, f"{rel_path}: could not find closing '---' of frontmatter"
    frontmatter_body = match.group(1)
    # 2026-07-12 (achado #34): any versioned frontmatter is accepted — the
    # gate guards PRESENCE of a version marker, not a pinned value (files
    # legitimately rev, e.g. api-contract.md is at "1.1").
    assert re.search(r'version: "\d+\.\d+"', frontmatter_body), (
        f"{rel_path}: frontmatter missing a version marker. "
        f"Found: {frontmatter_body!r}"
    )


def test_cards_workflow_defers_to_typed_transition_read_model() -> None:
    content = (RESOURCES_DIR / "workflows" / "cards.md").read_text(
        encoding="utf-8"
    )

    assert "A normal card follows `not_started → started → in_progress`" in content
    assert "only when the transition tool advertises that edge" in content
    assert (
        '`okto_pulse_move_card(status="in_progress")` → begin work'
        not in content
    )


def test_destructive_ops_documents_auditable_fallback_without_fake_comments() -> None:
    content = (RESOURCES_DIR / "reference" / "destructive_ops.md").read_text(
        encoding="utf-8"
    )

    assert "board-scoped audit card" in content
    assert "exact target type + id" in content
    assert "never" in content and "unsupported comment call" in content


def test_design_system_board_docs_match_live_single_link_schema() -> None:
    content = (
        RESOURCES_DIR / "reference" / "tool-docs" / "board.md"
    ).read_text(encoding="utf-8")
    link = content.split("## `okto_pulse_link_board_design_system`", 1)[1].split(
        "## `okto_pulse_unlink_board_design_system`", 1
    )[0]
    unlink = content.split("## `okto_pulse_unlink_board_design_system`", 1)[1].split(
        "## `okto_pulse_get_board_design_system`", 1
    )[0]

    assert "no priority argument" in link
    assert "inline Design" in link and "same board" in link
    assert "no design_system_id argument" in unlink


def test_tool_family_docs_distinguish_registry_short_names_from_mcp_aliases() -> None:
    for filename, short_name in (
        ("qa_ask.md", "ask"),
        ("spec_entity_remove.md", "remove_spec_entity"),
    ):
        content = (
            RESOURCES_DIR / "reference" / "tool-families" / filename
        ).read_text(encoding="utf-8")
        assert f"Registry-only short name: `{short_name}`" in content
        assert "not** an MCP" in content
        assert "does not appear in `tools/list`" in content


@pytest.mark.parametrize(
    ("filename", "tool_name"),
    (
        ("comment.md", "okto_pulse_add_choice_comment"),
        ("ideation.md", "okto_pulse_ask_ideation_choice_question"),
        ("refinement.md", "okto_pulse_ask_refinement_choice_question"),
        ("spec.md", "okto_pulse_ask_spec_choice_question"),
    ),
)
def test_choice_tool_docs_explain_structured_options_json(
    filename: str,
    tool_name: str,
) -> None:
    content = (
        RESOURCES_DIR / "reference" / "tool-docs" / filename
    ).read_text(encoding="utf-8")
    section = content.split(f"## `{tool_name}`", 1)[1].split("\n## `", 1)[0]

    assert "options_json: Preferred structured options" in section
    assert "native array" in section
    assert '"recommended":true' in section
    assert "takes precedence over options" in section


# ---------------------------------------------------------------------------
# 3. Root agent_instructions.md must be ≤500 lines
# ---------------------------------------------------------------------------


def test_root_agent_instructions_line_count() -> None:
    """Root agent_instructions.md must have ≤500 lines after the rewrite."""
    root = MCP_DIR / "agent_instructions.md"
    assert root.exists(), "agent_instructions.md not found"
    lines = root.read_text(encoding="utf-8").splitlines()
    assert len(lines) <= 500, (
        f"agent_instructions.md has {len(lines)} lines — must be ≤500 after TR-A6 rewrite."
    )


# ---------------------------------------------------------------------------
# 4. _load_resource_file() handles missing path gracefully
# ---------------------------------------------------------------------------


def test_load_resource_file_missing_returns_empty() -> None:
    """_load_resource_file() must return '' (not raise) for a non-existent path."""
    # Import the function directly; the module-level side effects are skipped
    # because we only import the helpers, not run FastMCP.
    from okto_pulse.core.mcp import server as _srv

    # 2026-07-12: the old _resources_cache dict was removed in the loader
    # refactor (_load_bundled_text reads the immutable package directly) —
    # the contract under test is only the fail-soft return value.
    result = _srv._load_resource_file("__nonexistent_test_path__.md")
    assert result == "", (
        "_load_resource_file() should return '' for a missing file, not raise an exception."
    )


# ---------------------------------------------------------------------------
# 5. effective catalog contains all expected URIs
# ---------------------------------------------------------------------------


def test_resource_registry_contains_all_uris() -> None:
    """The EFFECTIVE resource catalog (the authority) must declare all expected
    URIs. R11-C: consumes ``effective_resource_catalog().specs()`` rather than
    ``_RESOURCE_REGISTRY`` (which is only its derived, read-only projection)."""
    from okto_pulse.core.mcp import server as _srv

    catalog_uris = {s.uri for s in _srv.effective_resource_catalog().specs()}
    for expected_uri in EXPECTED_URIS:
        assert expected_uri in catalog_uris, (
            f"URI missing from the effective resource catalog: {expected_uri}"
        )
    # the legacy projection mirrors the catalog (same URIs) — bridge, not authority.
    assert {entry[0] for entry in _srv._RESOURCE_REGISTRY} == catalog_uris


# ---------------------------------------------------------------------------
# 6. effective catalog specs resolve to non-empty content
# ---------------------------------------------------------------------------


def test_resource_registry_paths_exist() -> None:
    """Every EFFECTIVE catalog spec resolves to non-empty content via its
    deterministic loader. R11-C: catalog-aware ``read()`` non-empty replaces the
    old path-exists check (it also covers content-based / overlay specs that have
    no filesystem path)."""
    from okto_pulse.core.mcp import server as _srv

    for spec in _srv.effective_resource_catalog().specs():
        assert len(spec.read()) > 0, (
            f"resource {spec.uri!r} resolved to EMPTY content via the catalog loader"
        )


# ---------------------------------------------------------------------------
# 7. Smoke test — the EFFECTIVE catalog serves all okto-pulse:// URIs (R11-C:
#    catalog-aware; no longer a textual scan of server.py source).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("uri", EXPECTED_URIS)
def test_uri_in_effective_catalog(uri: str) -> None:
    """R11-C: each expected okto-pulse:// URI is SERVED by the effective resource
    catalog (the authority), not asserted against server.py source text — the
    catalog is the single source of truth after the R11-A/B refactor."""
    from okto_pulse.core.mcp import server as _srv

    catalog_uris = {s.uri for s in _srv.effective_resource_catalog().specs()}
    assert uri in catalog_uris, (
        f"URI {uri!r} is not served by the effective resource catalog"
    )


# ---------------------------------------------------------------------------
# 8. _load_resource_file() caches on second call (idempotent)
# ---------------------------------------------------------------------------


def test_load_resource_file_cache_idempotent() -> None:
    """Repeated loads of the same immutable resource return identical content.

    2026-07-12: object-identity (`is`) dropped — the loader refactor removed
    the _resources_cache dict; resources are immutable package files, so the
    stable contract is content equality across calls."""
    from okto_pulse.core.mcp import server as _srv

    path = "workflows/stories.md"
    first = _srv._load_resource_file(path)
    second = _srv._load_resource_file(path)
    assert first == second, (
        "_load_resource_file() should return identical content on repeat calls."
    )
    assert len(first) > 0, "stories.md returned empty content — file may be missing."


def test_related_context_resources_require_typed_artifact_references() -> None:
    """Workflow examples must match the boundary's typed-anchor contract."""
    from okto_pulse.core.mcp import server as _srv

    specs = _srv._load_resource_file("workflows/specs.md")
    refinements = _srv._load_resource_file("workflows/refinements.md")
    kg_workflow = _srv._load_resource_file("workflows/kg.md")
    kg_tool_docs = _srv._load_resource_file("reference/tool-docs/kg.md")

    assert 'artifact_id="spec:<uuid>"' in specs
    assert "`spec:<uuid>` or `card:<uuid>`" in refinements
    assert 'artifact_id="spec:<uuid>"' in kg_workflow
    assert '"card:<uuid>"' in kg_workflow
    assert "Typed source reference: ``spec:<uuid>`` or ``card:<uuid>``" in kg_tool_docs

    stale_examples = ("artifact_id=<spec_id>", "<formalized_node_or_artifact_id>")
    for body in (specs, refinements, kg_workflow, kg_tool_docs):
        assert not any(stale in body for stale in stale_examples)


def test_test_scenario_resource_documents_write_omission_and_raw_legacy_filter() -> None:
    """Agent guidance must match the asymmetric write/read type contract."""
    from okto_pulse.core.mcp import server as _srv

    tool_docs = _srv._load_resource_file(
        "reference/tool-docs/test-scenario.md"
    )
    update_section = tool_docs.split(
        "## `okto_pulse_update_test_scenario`", 1
    )[1].split("\n## `", 1)[0]
    list_section = tool_docs.split(
        "## `okto_pulse_list_test_scenarios`", 1
    )[1].split("\n## `", 1)[0]
    normalized_update = " ".join(update_section.split())
    normalized_list = " ".join(list_section.split())

    assert "omit `scenario_type` to preserve the current type" in normalized_update
    assert "an empty string is not part of the closed enum" in normalized_update
    assert 'scenario_type/notes: New value, or "" to leave as-is' not in update_section
    assert "raw persisted value" in normalized_list
    assert "Historical values such as regression" in normalized_list
    assert "read-only compatibility filter" in normalized_list


def test_guideline_resources_match_governed_lifecycle_and_priority_semantics() -> None:
    """Guard semantic guidance that checksums alone cannot validate."""
    from okto_pulse.core.mcp import server as _srv

    guideline = _srv._load_resource_file("reference/tool-docs/guideline.md")
    board = _srv._load_resource_file("reference/tool-docs/board.md")
    normalized_guideline = " ".join(guideline.split())
    normalized_board = " ".join(board.split())

    assert "Compatibility name for retiring a guideline" in normalized_guideline
    assert normalized_guideline.count("Deprecated direct-adoption shim") == 2
    assert "guideline_impact_preview_required" in normalized_guideline
    assert "ascending priority (lower values first)" in normalized_guideline
    assert "higher = more important" not in guideline
    assert "highest first" not in guideline

    assert "guidelines.adoption.manage" in normalized_board
    assert "equivalent pins does not require that additional capability" in (
        normalized_board
    )
    for field in (
        "revision_id",
        "revision_number",
        "semantic_version",
        "revision_digest",
    ):
        assert field in board


def test_spec_quality_guidance_routes_lifecycle_without_contract_duplication() -> None:
    """Spec workflow owns status routing; Quality keeps shared mechanics."""
    from okto_pulse.core.mcp import server as _srv

    specs = _srv._load_resource_file("workflows/specs.md")
    quality = _srv._load_resource_file("reference/quality-assessments.md")
    normalized_specs = " ".join(specs.split())
    normalized_quality = " ".join(quality.split())

    for heading in (
        "### Spec Quality — Canonical Agent Flow",
        "#### Surface responsibilities",
        "#### Agent flow by Spec status",
        "#### Token-efficient read sequence",
    ):
        assert heading in specs
    for status in (
        "`draft`",
        "`review`",
        "`approved`",
        "`validated`, `in_progress`, `done`",
        "`cancelled` or archived",
    ):
        assert status in specs

    assert "Quality is read-only for Specs" in normalized_specs
    assert "Validation is actionable at `approved`" in normalized_specs
    assert "its score is the finding count" in normalized_specs
    assert "No head means **no evidence**, not zero findings" in normalized_specs
    assert "is migrated audit evidence" in normalized_specs
    assert "System legacy import" in normalized_quality
    assert "not a native Quality receipt" in normalized_quality
    assert (
        "This resource intentionally does not repeat those lifecycle steps"
        in normalized_quality
    )

    # Shared operational details have one canonical home to control token use.
    for detail in (
        "limits `25|50|100`",
        "`subject_version_changed`, `content_changed`",
        "`{subject}.quality.read`",
    ):
        assert detail in quality
        assert detail not in specs


def test_stage3_resources_document_canonical_constraint_id_discovery() -> None:
    """Stage 3 must not force clients to guess worker-local constraint ids."""
    from okto_pulse.core.mcp import server as _srv

    specs = _srv._load_resource_file("workflows/specs.md")
    kg_workflow = _srv._load_resource_file("workflows/kg.md")
    kg_tool_docs = _srv._load_resource_file("reference/tool-docs/kg.md")

    for body in (specs, kg_workflow, kg_tool_docs):
        assert "canonical graph" in body
        assert "source_artifact_ref" in body
        assert "include_working=true" in body
    assert "RETURN c.id AS id" in kg_workflow
    assert 'params={"prefix":"spec:<spec-id>:"}' in kg_workflow


# ---------------------------------------------------------------------------
# 9. ts_edd149c4 — Initial footprint regression guard via tiktoken
# ---------------------------------------------------------------------------


def test_initial_footprint_under_budget() -> None:
    """ts_edd149c4 — Initial MCP server footprint stays within budget per component.

    Measures via tiktoken (cl100k_base) the two payloads any MCP client sees
    on session start:
      1. ``instructions=`` passed to FastMCP (loaded from agent_instructions.md).
      2. ``tools/list`` metadata: name + description + JSON schema per tool.

    Budget rationale (post-P0.A + post-P0.B, pre-P1 lazy-loading):
      - instructions ≤ 10K tokens — P0.A goal (was ~71K pre-rewrite).
      - tools metadata ≤ 47K tokens — reviewed ceiling for 312 tools, including
        the 20 closed policy-governance schemas; P1 lazy-loading by role will
        reduce this drastically per session.
      - combined ≤ 50K tokens — overall regression guard.

    A failure in any of the three asserts pinpoints which subsystem regressed.
    """
    import json

    tiktoken = pytest.importorskip("tiktoken")
    from okto_pulse.core.mcp import server as _srv

    enc = tiktoken.get_encoding("cl100k_base")

    instructions_tokens = len(enc.encode(_srv._load_instructions()))
    assert instructions_tokens <= 10_000, (
        f"instructions= footprint {instructions_tokens} tokens exceeds 10K budget — "
        f"P0.A rewrite regression."
    )

    parts: list[str] = []
    for tool_name, tool in _srv.mcp._tool_manager._tools.items():
        desc = getattr(tool, "description", "") or ""
        schema = json.dumps(getattr(tool, "parameters", {}), separators=(",", ":"))
        parts.append(f"{tool_name}\n{desc}\n{schema}")
    tools_tokens = len(enc.encode("\n".join(parts)))
    assert tools_tokens <= 47_000, (
        f"tools/list metadata {tools_tokens} tokens exceeds 47K guard — "
        f"P1 lazy-loading by role will reduce this per session."
    )

    total = instructions_tokens + tools_tokens
    assert total <= 50_000, (
        f"Combined initial footprint {total} tokens exceeds 50K guard "
        f"(instructions={instructions_tokens}, tools={tools_tokens})."
    )


# ---------------------------------------------------------------------------
# 10. ts_d06efe7a — Smoke detects broken okto-pulse:// links in docstrings
# ---------------------------------------------------------------------------


_RESOURCE_URI_PATTERN = re.compile(r"okto-pulse://[a-zA-Z0-9_/\-]+")


def test_smoke_no_broken_resource_links_in_docstrings() -> None:
    """ts_d06efe7a — Every ``okto-pulse://`` URI mentioned in a tool docstring
    must resolve to a URI served by the EFFECTIVE resource catalog (R11-C:
    catalog-aware resolution — the catalog is the authority, not the
    ``_RESOURCE_REGISTRY`` projection).

    Failure mode caught: a docstring references e.g.
    ``okto-pulse://workflows/onboardimg`` (typo) — no resource is in the catalog
    under that URI, so an agent following the link gets nothing back. This
    smoke fails CI naming the offending tool and the broken URI.
    """
    from okto_pulse.core.mcp import server as _srv

    catalogued: set[str] = {s.uri for s in _srv.effective_resource_catalog().specs()}
    assert catalogued, "No resources in the effective catalog — catalog import broken."

    broken: list[tuple[str, str]] = []
    for tool_name, tool in _srv.mcp._tool_manager._tools.items():
        desc = getattr(tool, "description", "") or ""
        for match in _RESOURCE_URI_PATTERN.findall(desc):
            if match not in catalogued:
                broken.append((tool_name, match))

    assert not broken, (
        "Tool docstrings reference non-catalogued okto-pulse:// URIs: "
        + ", ".join(f"{name} -> {uri}" for name, uri in broken)
    )


def test_smoke_detects_synthetic_broken_link() -> None:
    """ts_d06efe7a — Negative case: the smoke must FAIL when a docstring is
    artificially patched with a broken URI. Confirms the smoke is wired
    correctly (does not silently pass when the linkage is bad).
    """
    from okto_pulse.core.mcp import server as _srv

    catalogued = {s.uri for s in _srv.effective_resource_catalog().specs()}
    fake_uri = "okto-pulse://workflows/__definitely_missing__"
    assert fake_uri not in catalogued, "Synthetic URI accidentally in the catalog."

    fake_doc = f"This tool references {fake_uri} for guidance."
    found = [m for m in _RESOURCE_URI_PATTERN.findall(fake_doc) if m not in catalogued]
    assert found == [fake_uri], (
        "Pattern + effective-catalog lookup must surface the broken URI; "
        f"got {found!r}."
    )
