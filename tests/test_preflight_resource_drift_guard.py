"""
Registry-URI drift guard for spec 0022b885 (Refinement B of ideation 02eaa737 — F1).

Pure doc-content + registry tests (read_text + import of server._RESOURCE_REGISTRY,
as tests/test_mcp_resources.py already does; no board/graph). They lock in the
extraction of the Pre-Flight Checklist into a real okto-pulse://workflows/preflight
resource and prevent the F1 drift (a "READ FIRST" doc that is not retrievable) from
silently recurring.

Covers the spec's six acceptance criteria:
- AC1 (ts_d58a459a) — preflight.md exists with frontmatter + the 4 sequences verbatim.
- AC2 (ts_0ee73005) — the URI is registered and resolves to non-empty content.
- AC3 (ts_42e30be9) — agent_instructions.md carries the concise pointer, not the step bodies.
- AC4 (ts_edc27c0a) — the Quick-Nav cites the URI (not "(this file)"); all Quick-Nav URIs resolve.
- AC5 (ts_ac2cdcf1) — the registry-URI guard is load-bearing (negative-wiring).
- AC6 (ts_484b04a3) — the generic registration machinery is byte-unchanged.
"""

import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

MCP_DIR = Path(__file__).parent.parent / "src" / "okto_pulse" / "core" / "mcp"
RESOURCES_DIR = MCP_DIR / "resources"
PREFLIGHT_MD = RESOURCES_DIR / "workflows" / "preflight.md"
AGENT_INSTRUCTIONS = MCP_DIR / "agent_instructions.md"
SERVER_PY = MCP_DIR / "server.py"

PREFLIGHT_URI = "okto-pulse://workflows/preflight"
PREFLIGHT_REL = "workflows/preflight.md"

# A verbatim card-execution step — present in preflight.md, but the full fenced body
# must NOT remain inline in agent_instructions.md (Option X: pointer, no duplication).
VERBATIM_STEP = "okto_pulse_copy_mockups_to_card(board_id, spec_id, card_id)"

# The four sub-sequence section headers (as written in preflight.md).
PREFLIGHT_SEQUENCES = [
    "Session pre-flight",
    "Entity context pre-flight",
    "Card execution pre-flight",
    "Resource Gate pre-flight",
]

# The four sequences named in the inline pointer (Option X + truncation-safe steer).
POINTER_SEQUENCES = [
    "session pre-flight",
    "entity-context pre-flight",
    "card-execution pre-flight",
    "resource gate pre-flight",
]

_URI_RE = re.compile(r"okto-pulse://[a-zA-Z0-9_/\-]+")
STALE_QUICKNAV = "**Pre-Flight Checklist** (this file)"


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _registered_uris() -> set[str]:
    from okto_pulse.core.mcp import server as _srv

    return {entry[0] for entry in _srv._RESOURCE_REGISTRY}


def _quicknav_uris(agent_text: str) -> set[str]:
    """okto-pulse:// URIs cited in the Quick-Nav table rows."""
    uris: set[str] = set()
    for line in agent_text.splitlines():
        if line.startswith("|") and "okto-pulse://" in line:
            uris.update(_URI_RE.findall(line))
    return uris


def _preflight_guard_holds(registered: set[str], agent_text: str) -> bool:
    """Load-bearing predicate: preflight registered, no stale Quick-Nav text,
    and every Quick-Nav URI resolves in the registry."""
    return (
        PREFLIGHT_URI in registered
        and STALE_QUICKNAV not in agent_text
        and _quicknav_uris(agent_text).issubset(registered)
    )


# ---------------------------------------------------------------------------
# AC1 — preflight.md exists with frontmatter + the 4 sequences verbatim
# ---------------------------------------------------------------------------


def test_ac1_preflight_md_verbatim() -> None:
    assert PREFLIGHT_MD.exists(), "workflows/preflight.md was not created."
    text = _read(PREFLIGHT_MD)
    assert text.startswith("---"), "preflight.md missing leading frontmatter."
    match = re.match(r"^---\n(.*?)\n---", text, re.DOTALL)
    assert match is not None and 'version: "1.0"' in match.group(1), (
        "preflight.md frontmatter missing version."
    )
    for seq in PREFLIGHT_SEQUENCES:
        assert seq in text, f"preflight.md missing sub-sequence {seq!r}."
    assert VERBATIM_STEP in text, "preflight.md missing the verbatim card-execution step."


# ---------------------------------------------------------------------------
# AC2 — the URI is registered and resolves to non-empty content
# ---------------------------------------------------------------------------


def test_ac2_preflight_registered_and_resolves() -> None:
    from okto_pulse.core.mcp import server as _srv

    registered = {entry[0]: entry[1] for entry in _srv._RESOURCE_REGISTRY}
    assert registered.get(PREFLIGHT_URI) == PREFLIGHT_REL, (
        f"{PREFLIGHT_URI} not registered to {PREFLIGHT_REL}."
    )
    # exactly one registration (no accidental duplicate)
    uris = [entry[0] for entry in _srv._RESOURCE_REGISTRY]
    assert uris.count(PREFLIGHT_URI) == 1
    content = _srv._load_resource_file(PREFLIGHT_REL)
    assert content and len(content) > 0, "preflight resource resolves to empty content."


# ---------------------------------------------------------------------------
# AC3 — agent_instructions.md carries the pointer, not the full step bodies
# ---------------------------------------------------------------------------


def test_ac3_agent_instructions_pointer_no_bodies() -> None:
    text = _read(AGENT_INSTRUCTIONS)
    low = text.lower()
    assert PREFLIGHT_URI in text, "agent_instructions.md missing the preflight pointer URI."
    for seq in POINTER_SEQUENCES:
        assert seq in low, f"pointer does not name the {seq!r} sequence."
    # the full inline card-execution body is gone (no duplication)
    assert VERBATIM_STEP not in text, (
        "agent_instructions.md still carries the full inline pre-flight step body."
    )


# ---------------------------------------------------------------------------
# AC4 — the Quick-Nav cites the URI; every Quick-Nav URI resolves
# ---------------------------------------------------------------------------


def test_ac4_quicknav_cites_uri() -> None:
    text = _read(AGENT_INSTRUCTIONS)
    assert STALE_QUICKNAV not in text, 'Quick-Nav still says "(this file)" for the pre-flight.'
    assert PREFLIGHT_URI in text
    assert "Resource Fetching Protocol" in text, "Resource Fetching Protocol must stay inline."
    registered = _registered_uris()
    for uri in _quicknav_uris(text):
        assert uri in registered, f"Quick-Nav cites an unregistered URI: {uri}"


# ---------------------------------------------------------------------------
# AC5 — the registry-URI guard is load-bearing (negative-wiring)
# ---------------------------------------------------------------------------


def test_ac5_guard_is_load_bearing() -> None:
    registered = _registered_uris()
    agent_text = _read(AGENT_INSTRUCTIONS)
    # Positive: the real state satisfies the guard.
    assert _preflight_guard_holds(registered, agent_text)
    # Negative-wiring: a registry without preflight FAILS the guard.
    assert not _preflight_guard_holds(registered - {PREFLIGHT_URI}, agent_text)
    # Negative-wiring: a Quick-Nav that still says "(this file)" FAILS the guard.
    assert not _preflight_guard_holds(registered, agent_text + "\n" + STALE_QUICKNAV + "\n")


# ---------------------------------------------------------------------------
# AC6 — the generic registration machinery is byte-unchanged
# ---------------------------------------------------------------------------


def test_ac6_registration_machinery_intact() -> None:
    server = _read(SERVER_PY)
    assert "for _uri, _path, _desc in _RESOURCE_REGISTRY:" in server  # generic loop
    assert "_load_resource_file(_path)" in server  # pre-warm
    assert "def _make_resource_handler" in server
    assert "def _load_resource_file" in server
