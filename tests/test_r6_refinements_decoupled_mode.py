"""R6 — refinements.md guidance is decoupled-mode aware (spec ba865e27, FR1-FR6).

Doc-tests backing TEST-A..D:
- AC1 ts_50a44e08, AC2 ts_88b69e42  (TEST-A)
- AC3 ts_da72f05c, AC4 ts_54d4f279  (TEST-B)
- AC5 ts_9f5e8f56, AC6 ts_fd6346b8  (TEST-C)
- AC7 ts_3295461a                    (TEST-D, guard)

Each test reads the CANONICAL refinements.md and asserts the scenario's ``then``
— the new decoupled-mode clause is present AND the old unconditional form is gone
(not a vacuous substring check). AC7 additionally guards that NO board-mode signal
(BoardSettings field / db column) was introduced — doc-only, owner path A
(dec_fa1e84e0).
"""

from __future__ import annotations

from pathlib import Path

import pytest

_CORE = Path(__file__).resolve().parent.parent / "src" / "okto_pulse" / "core"
REFINEMENTS_MD = _CORE / "mcp" / "resources" / "workflows" / "refinements.md"
SCHEMAS_PY = _CORE / "models" / "schemas.py"
DB_PY = _CORE / "models" / "db.py"


@pytest.fixture(scope="module")
def md() -> str:
    return REFINEMENTS_MD.read_text(encoding="utf-8")


def _row(md: str, label: str) -> str:
    """Return the full source-table row line for a given source label."""
    for line in md.splitlines():
        if line.startswith(f"| **{label}**"):
            return line
    raise AssertionError(f"source row not found: {label}")


def _analysis_valve(md: str) -> str:
    """The single line of the `analysis` deliverable that carries the N/A valve."""
    idx = md.index("If a source did not apply, state that explicitly")
    return md[idx : md.index("\n", idx)]


# ---------------------------------------------------------------------------
# AC1 (ts_50a44e08) — When it applies of the two code sources becomes conditional
# ---------------------------------------------------------------------------


def test_ac1_when_it_applies_is_conditional_not_bare_always(md: str):
    pf = _row(md, "Project files")
    sc = _row(md, "Source code")
    # New conditional form present on both code-source rows.
    assert "Always when a codebase is accessible" in pf
    assert "Always when a codebase is accessible" in sc
    # Old unconditional "Always — ..." form is gone (proves it was actually changed).
    assert "Always — the refinement must reflect the real shape" not in md
    assert "Always — anything the refinement claims about behaviour must be verifiable" not in md


# ---------------------------------------------------------------------------
# AC2 (ts_88b69e42) — source table declares N/A-eligible for decoupled boards
# ---------------------------------------------------------------------------


def test_ac2_source_table_declares_na_eligible_for_decoupled(md: str):
    pf = _row(md, "Project files")
    sc = _row(md, "Source code")
    for row in (pf, sc):
        assert "decoupled mode" in row
        assert "N/A with an explicit justification" in row
    # Explicitly mirrors the established Mockups/Architecture N/A pattern.
    assert "same pattern" in pf


# ---------------------------------------------------------------------------
# AC3 (ts_da72f05c) — analysis valve names Project files AND Source code
# ---------------------------------------------------------------------------


def test_ac3_analysis_valve_names_code_sources(md: str):
    valve = _analysis_valve(md)
    assert "Project files" in valve
    assert "Source code" in valve
    assert "N/A-with-justification" in valve
    assert "decoupled mode" in valve


# ---------------------------------------------------------------------------
# AC4 (ts_54d4f279) — stop condition recognises decoupled mode
# ---------------------------------------------------------------------------


def test_ac4_stop_condition_recognises_decoupled_mode(md: str):
    idx = md.index("Stop condition — the refinement is genuinely ready when")
    # The first bullet right after the heading carries the decoupled-mode clause.
    first_bullet = md[idx:].split("\n", 2)[1]
    assert "decoupled mode" in first_bullet
    assert "justified N/A of Project files / Source code satisfies this condition" in first_bullet


# ---------------------------------------------------------------------------
# AC5 (ts_9f5e8f56) — anti-pattern qualified: never fabricate path:line
# ---------------------------------------------------------------------------


def test_ac5_antipattern_never_fabricate_pathline(md: str):
    idx = md.index("Open the modules in scope; cite `path:line`")
    cell = md[idx : md.index("\n", idx)]
    # Rigor scoped to code-behaviour claims when a codebase exists...
    assert "when a codebase is accessible" in cell
    # ...and decoupled boards anchor to applicable sources, never fabricating.
    assert "decoupled board" in cell
    assert "never fabricate" in cell
    assert "KG node_id" in cell


# ---------------------------------------------------------------------------
# AC6 (ts_fd6346b8) — N/A of a code source requires a justification (no silent N/A)
# ---------------------------------------------------------------------------


def test_ac6_na_requires_justification_no_silent(md: str):
    pf = _row(md, "Project files")
    sc = _row(md, "Source code")
    assert "N/A with an explicit justification" in pf
    assert "N/A with an explicit justification" in sc
    # The analysis valve forbids a silent omission outright.
    assert "a silent omission is never acceptable" in md


# ---------------------------------------------------------------------------
# AC7 (ts_3295461a) — GUARD: codebase-present rigor intact AND zero board-mode signal
# ---------------------------------------------------------------------------


def test_ac7_rigor_preserved_and_no_board_mode_signal(md: str):
    # (a) With a codebase present, opening modules + path:line stays mandatory.
    assert (
        "cite `path:line` for every claim about code behaviour when a codebase is accessible"
        in md
    )
    assert "Always when a codebase is accessible — the refinement must reflect" in md
    # (b) Doc-only (owner path A): no board-mode field/flag leaked into schema or db.
    schemas = SCHEMAS_PY.read_text(encoding="utf-8")
    db = DB_PY.read_text(encoding="utf-8")
    for token in ("decoupled_mode", "has_repository"):
        assert token not in schemas, f"board-mode signal '{token}' leaked into schemas.py"
        assert token not in db, f"board-mode signal '{token}' leaked into db.py"
