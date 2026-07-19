"""tools_catalog drift gate (audit 2026-07-12, achados #28/#29/#57).

The committed ``resources/reference/tools_catalog.md`` must be BYTE-EQUAL
to the catalog rendered from the LIVE tool registry. Any tool added,
removed or renamed turns this red until::

    python -m okto_pulse.core.mcp.tools_catalog_generator

is run (and, for a brand-new family, one ``_RULES`` line is added).
This replaces the retired hand-maintained catalog (which had drifted to
167 of 265 tools + one phantom) and the dead schema_generator pilot as
the real anti-drift guard of the served tool surface.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from okto_pulse.core.mcp.tools_catalog_generator import (
    missing_tool_docs,
    render_catalog,
    tool_doc_index,
    unclassified,
)

_CATALOG = (
    Path(__file__).parent.parent
    / "src" / "okto_pulse" / "core" / "mcp"
    / "resources" / "reference" / "tools_catalog.md"
)


def _live_names() -> list[str]:
    from okto_pulse.core.mcp import server as _server

    return sorted(asyncio.run(_server.mcp.get_tools()).keys())


def test_every_live_tool_is_classified():
    missing = unclassified(_live_names())
    assert missing == [], (
        "tools sem seção no catálogo — adicione uma regra em "
        f"tools_catalog_generator._RULES: {missing}"
    )


def test_catalog_file_matches_live_registry():
    names = _live_names()
    expected = render_catalog(names)
    actual = _CATALOG.read_text(encoding="utf-8")
    assert actual == expected, (
        "tools_catalog.md drifted from the live registry — regenerate with "
        "`python -m okto_pulse.core.mcp.tools_catalog_generator`"
    )


def test_every_live_tool_has_one_exact_tool_doc_heading():
    index = tool_doc_index()
    assert missing_tool_docs(_live_names(), index) == []
    assert set(_live_names()) <= set(index)


def test_catalog_covers_exact_tool_set():
    """Belt-and-braces set equality: every live tool appears exactly once."""
    names = _live_names()
    content = _CATALOG.read_text(encoding="utf-8")
    import re

    listed = re.findall(r"`(okto_pulse_[a-z0-9_]+)`", content)
    # Ignore URI-like mentions (tool-docs pointers contain no okto_pulse_ prefix).
    assert sorted(set(listed)) == names, (
        f"catálogo≠registry: faltam {sorted(set(names) - set(listed))[:5]}..., "
        f"fantasmas {sorted(set(listed) - set(names))[:5]}..."
    )
    assert len(listed) == len(set(listed)), "tool listada em duplicidade"
