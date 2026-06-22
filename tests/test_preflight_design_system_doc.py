"""Spec 24f2e786 / card cb8a295f — the preflight workflow resource carries the Design
System mandate so an agent reads it at session start (FR5), scenario ts_103d0933.

Doc-guard: the '### Design System pre-flight' subsection exists and, WITHIN that subsection,
cites the four actionable reason codes and the expected_* self-correction (plus the dangling
caveat). Scoping the assertions to the subsection makes this a real guard, not a substring
coincidence elsewhere in the doc.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_preflight_design_system_doc.py
"""

from __future__ import annotations

from pathlib import Path

_PREFLIGHT = (
    Path(__file__).resolve().parents[1]
    / "src" / "okto_pulse" / "core" / "mcp" / "resources" / "workflows" / "preflight.md"
)
_MARKER = "### Design System pre-flight"


def _doc() -> str:
    return _PREFLIGHT.read_text(encoding="utf-8")


def _ds_section() -> str:
    doc = _doc()
    assert _MARKER in doc, "the 'Design System pre-flight' subsection is missing from preflight.md"
    rest = doc[doc.index(_MARKER) + len(_MARKER):]
    nxt = rest.find("\n### ")
    return rest if nxt == -1 else rest[:nxt]


def test_preflight_has_design_system_subsection():
    assert _MARKER in _doc()


def test_subsection_cites_the_four_reason_codes():
    section = _ds_section()
    for code in (
        "design_system_required",
        "design_system_not_found",
        "design_system_version_mismatch",
        "design_system_evidence_missing",
    ):
        assert code in section, f"reason code {code!r} not documented in the Design System subsection"


def test_subsection_documents_expected_self_correction():
    section = _ds_section()
    assert "expected_design_system_id" in section
    assert "expected_design_system_version" in section


def test_subsection_documents_dangling_caveat():
    # validator's non-blocking note: mandate=true can still fail with not_found if dangling.
    section = _ds_section()
    assert "dangling" in section
    assert "design_system_not_found" in section


def test_subsection_points_to_discovery_tools():
    section = _ds_section()
    assert "okto_pulse_get_board" in section
    assert "okto_pulse_get_design_system" in section
