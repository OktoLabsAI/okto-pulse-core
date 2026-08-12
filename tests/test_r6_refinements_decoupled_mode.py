"""R6 — refinements.md keeps repository inspection agent-owned.

Doc-tests backing TEST-A..D:
- AC1 ts_50a44e08, AC2 ts_88b69e42  (TEST-A)
- AC3 ts_da72f05c, AC4 ts_54d4f279  (TEST-B)
- AC5 ts_9f5e8f56, AC6 ts_fd6346b8  (TEST-C)
- AC7 ts_3295461a                    (TEST-D, guard)

Each test reads the CANONICAL refinements.md and asserts the scenario's ``then``
The original board-level ``decoupled_mode`` proposal was superseded by the
agent-mediated Code Traceability contract: Pulse/Community never inspect a
working directory. These guards keep access preflight, immutable evidence and
the explicit unavailable/waiver path visible without introducing board state.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repository_checkout_testing import community_source_for

_REPO = Path(__file__).resolve().parent.parent
_CORE = _REPO / "src" / "okto_pulse" / "core"
_COMMUNITY = community_source_for(_REPO)
REFINEMENTS_MD = _CORE / "mcp" / "resources" / "workflows" / "refinements.md"
SCHEMAS_PY = _CORE / "models" / "schemas.py"
DB_PY = _COMMUNITY / "adapters" / "sqlalchemy_models.py"


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
    """The `analysis` deliverable carrying the unavailable-access valve."""
    idx = md.index("1. **`analysis`**")
    return md[idx : md.index("\n", idx)]


# ---------------------------------------------------------------------------
# AC1 (ts_50a44e08) — When it applies of the two code sources becomes conditional
# ---------------------------------------------------------------------------


def test_ac1_when_it_applies_is_conditional_not_bare_always(md: str):
    pf = _row(md, "Project files")
    sc = _row(md, "Source code")
    assert "external agent" in pf
    assert "Pulse never opens the working directory" in pf
    assert "authenticated external agent" in sc
    assert "immutable Code Evidence" in sc


# ---------------------------------------------------------------------------
# AC2 (ts_88b69e42) — source table declares N/A-eligible for decoupled boards
# ---------------------------------------------------------------------------


def test_ac2_source_table_declares_na_eligible_for_decoupled(md: str):
    pf = _row(md, "Project files")
    sc = _row(md, "Source code")
    assert "`partial` or `unavailable`" in pf
    assert "explicit scoped waiver" in pf
    assert "unavailable capability is explicit evidence" in sc
    assert "never a fabricated code mapping" in sc


# ---------------------------------------------------------------------------
# AC3 (ts_da72f05c) — analysis valve names Project files AND Source code
# ---------------------------------------------------------------------------


def test_ac3_analysis_valve_names_code_sources(md: str):
    valve = _analysis_valve(md)
    assert "`evidence:<id>`" in valve
    assert "agent receipt" in valve
    assert "A bare `path:line` is not source truth" in valve
    assert "`partial|unavailable` receipt" in valve


# ---------------------------------------------------------------------------
# AC4 (ts_54d4f279) — stop condition recognises decoupled mode
# ---------------------------------------------------------------------------


def test_ac4_stop_condition_recognises_decoupled_mode(md: str):
    idx = md.index("Stop condition — the refinement is genuinely ready when")
    # The first bullet right after the heading carries the decoupled-mode clause.
    first_bullet = md[idx:].split("\n", 2)[1]
    assert "immutable Code Evidence" in first_bullet
    assert "explicit unavailable receipt/waiver" in first_bullet
    assert "without requiring Pulse to open a repository" in first_bullet


# ---------------------------------------------------------------------------
# AC5 (ts_9f5e8f56) — anti-pattern qualified: never fabricate path:line
# ---------------------------------------------------------------------------


def test_ac5_antipattern_never_fabricate_pathline(md: str):
    idx = md.index("Writing the refinement from ideation text alone")
    cell = md[idx : md.index("\n", idx)]
    assert "external agent preflight" in cell
    assert "`evidence:<id>`" in cell
    assert "explicit waiver path" in cell
    assert "never fabricate" in cell


# ---------------------------------------------------------------------------
# AC6 (ts_fd6346b8) — N/A of a code source requires a justification (no silent N/A)
# ---------------------------------------------------------------------------


def test_ac6_na_requires_justification_no_silent(md: str):
    valve = _analysis_valve(md)
    assert "explicit waiver/N/A decision" in valve
    assert "silent omission is never acceptable" in valve


# ---------------------------------------------------------------------------
# AC7 (ts_3295461a) — GUARD: codebase-present rigor intact AND zero board-mode signal
# ---------------------------------------------------------------------------


def test_ac7_rigor_preserved_and_no_board_mode_signal(md: str):
    assert "check access and capabilities in the authenticated agent's environment" in md
    assert "Pulse and Community never inspect source code" in md
    assert "There is no `decoupled_mode`" in md
    # (b) Doc-only (owner path A): no board-mode field/flag leaked into the Core
    # contract schema or the Community-owned relational mappings.
    schemas = SCHEMAS_PY.read_text(encoding="utf-8")
    db = DB_PY.read_text(encoding="utf-8")
    for token in ("decoupled_mode", "has_repository"):
        assert token not in schemas, f"board-mode signal '{token}' leaked into schemas.py"
        assert token not in db, (
            f"board-mode signal '{token}' leaked into Community SQLAlchemy mappings"
        )
