"""
Tests for MCP resource files introduced by spec P0.A (TR-A5).

Covers:
- All 12 resource .md files exist and have frontmatter
- Root agent_instructions.md is ≤500 lines
- _load_resource_file() handles missing files gracefully
- _RESOURCE_REGISTRY URIs are consistent with registered file paths
- server.py contains okto-pulse:// URIs for all 12 resources
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

EXPECTED_FILES = [
    "workflows/stories.md",
    "workflows/ideations.md",
    "workflows/refinements.md",
    "workflows/specs.md",
    "workflows/cards.md",
    "workflows/sprints.md",
    "workflows/kg.md",
    "reference/errors.md",
    "reference/multivalue.md",
    "reference/destructive_ops.md",
    "reference/card_types.md",
    "reference/spec_gates.md",
]

EXPECTED_URIS = [
    "okto-pulse://workflows/stories",
    "okto-pulse://workflows/ideations",
    "okto-pulse://workflows/refinements",
    "okto-pulse://workflows/specs",
    "okto-pulse://workflows/cards",
    "okto-pulse://workflows/sprints",
    "okto-pulse://workflows/kg",
    "okto-pulse://reference/errors",
    "okto-pulse://reference/multivalue",
    "okto-pulse://reference/destructive_ops",
    "okto-pulse://reference/card_types",
    "okto-pulse://reference/spec_gates",
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
    assert 'version: "1.0"' in frontmatter_body, (
        f"{rel_path}: frontmatter missing 'version: \"1.0\"'. "
        f"Found: {frontmatter_body!r}"
    )


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

    # Ensure the cache does not contain this key from a previous run
    _srv._resources_cache.pop("__nonexistent_test_path__.md", None)

    result = _srv._load_resource_file("__nonexistent_test_path__.md")
    assert result == "", (
        "_load_resource_file() should return '' for a missing file, not raise an exception."
    )


# ---------------------------------------------------------------------------
# 5. _RESOURCE_REGISTRY in server.py contains all 12 URIs
# ---------------------------------------------------------------------------


def test_resource_registry_contains_all_uris() -> None:
    """_RESOURCE_REGISTRY in server.py must declare all 12 expected URIs."""
    from okto_pulse.core.mcp import server as _srv

    registered_uris = {entry[0] for entry in _srv._RESOURCE_REGISTRY}
    for expected_uri in EXPECTED_URIS:
        assert expected_uri in registered_uris, (
            f"URI missing from _RESOURCE_REGISTRY: {expected_uri}"
        )


# ---------------------------------------------------------------------------
# 6. _RESOURCE_REGISTRY paths correspond to existing files
# ---------------------------------------------------------------------------


def test_resource_registry_paths_exist() -> None:
    """Every path in _RESOURCE_REGISTRY must correspond to an existing file."""
    from okto_pulse.core.mcp import server as _srv

    resource_dir = _srv._get_resource_dir()
    for uri, path, _desc in _srv._RESOURCE_REGISTRY:
        full = resource_dir / path
        assert full.exists(), (
            f"Registry entry for {uri!r} points to missing file: {full}"
        )


# ---------------------------------------------------------------------------
# 7. Smoke test — server.py source contains all okto-pulse:// URIs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("uri", EXPECTED_URIS)
def test_server_py_contains_uri(uri: str) -> None:
    """server.py source text must contain each expected okto-pulse:// URI."""
    assert SERVER_PY.exists(), "server.py not found"
    source = SERVER_PY.read_text(encoding="utf-8")
    assert uri in source, (
        f"URI {uri!r} not found in server.py. "
        "Check _RESOURCE_REGISTRY or the resource registration loop."
    )


# ---------------------------------------------------------------------------
# 8. _load_resource_file() caches on second call (idempotent)
# ---------------------------------------------------------------------------


def test_load_resource_file_cache_idempotent() -> None:
    """Calling _load_resource_file() twice for the same path returns the same object."""
    from okto_pulse.core.mcp import server as _srv

    path = "workflows/stories.md"
    # Clear cache entry to force a fresh load
    _srv._resources_cache.pop(path, None)

    first = _srv._load_resource_file(path)
    second = _srv._load_resource_file(path)
    assert first is second, (
        "_load_resource_file() should return the cached string object on second call."
    )
    assert len(first) > 0, "stories.md returned empty content — file may be missing."


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
      - tools metadata ≤ 45K tokens — current ceiling for ~226 tools; P1
        lazy-loading by role will reduce this drastically per session.
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
    assert tools_tokens <= 45_000, (
        f"tools/list metadata {tools_tokens} tokens exceeds 45K guard — "
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
    must resolve to a URI registered in ``_RESOURCE_REGISTRY``.

    Failure mode caught: a docstring references e.g.
    ``okto-pulse://workflows/onboardimg`` (typo) — no resource is registered
    under that URI, so an agent following the link gets nothing back. This
    smoke fails CI naming the offending tool and the broken URI.
    """
    from okto_pulse.core.mcp import server as _srv

    registered: set[str] = {entry[0] for entry in _srv._RESOURCE_REGISTRY}
    assert registered, "No resources registered — registry import broken."

    broken: list[tuple[str, str]] = []
    for tool_name, tool in _srv.mcp._tool_manager._tools.items():
        desc = getattr(tool, "description", "") or ""
        for match in _RESOURCE_URI_PATTERN.findall(desc):
            if match not in registered:
                broken.append((tool_name, match))

    assert not broken, (
        "Tool docstrings reference unregistered okto-pulse:// URIs: "
        + ", ".join(f"{name} -> {uri}" for name, uri in broken)
    )


def test_smoke_detects_synthetic_broken_link() -> None:
    """ts_d06efe7a — Negative case: the smoke must FAIL when a docstring is
    artificially patched with a broken URI. Confirms the smoke is wired
    correctly (does not silently pass when the linkage is bad).
    """
    from okto_pulse.core.mcp import server as _srv

    registered = {entry[0] for entry in _srv._RESOURCE_REGISTRY}
    fake_uri = "okto-pulse://workflows/__definitely_missing__"
    assert fake_uri not in registered, "Synthetic URI accidentally registered."

    fake_doc = f"This tool references {fake_uri} for guidance."
    found = [m for m in _RESOURCE_URI_PATTERN.findall(fake_doc) if m not in registered]
    assert found == [fake_uri], (
        "Pattern + registry lookup must surface the broken URI; "
        f"got {found!r}."
    )
