"""C5/TS7: the quality resources must describe `proposed_questions` truthfully.

``okto_pulse_record_ambiguity_assessment`` materializes proposed questions as
real Q&A items on the subject; the reference docs previously claimed the
opposite, which led agents to leave pending Q&A that blocks human close-out.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_RESOURCES = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "mcp"
    / "resources"
    / "reference"
)

QUALITY_ASSESSMENTS = _RESOURCES / "quality-assessments.md"
QUALITY_TOOL_DOCS = _RESOURCES / "tool-docs" / "quality.md"


def _text(path: Path) -> str:
    assert path.is_file(), f"missing MCP resource: {path}"
    return path.read_text(encoding="utf-8")


def test_quality_assessments_drops_never_materialize_claim():
    assert "never materialize" not in _text(QUALITY_ASSESSMENTS)


def test_quality_tool_docs_drops_never_create_or_mutate_claim():
    assert "never create or mutate the subject" not in _text(QUALITY_TOOL_DOCS)


@pytest.mark.parametrize("path", [QUALITY_ASSESSMENTS, QUALITY_TOOL_DOCS])
def test_resources_state_questions_are_materialized(path: Path):
    assert "materialized as Q&A items" in _text(path)
