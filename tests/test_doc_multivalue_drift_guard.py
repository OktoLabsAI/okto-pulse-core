"""
Drift guard for spec a4bf738a (Refinement A of ideation 02eaa737 — F11/F2/F7/F12).

Pure doc-content tests (``read_text`` + literal/substring assertions, no
server/board/graph), mirroring ``tests/test_mcp_resources.py`` and
``tests/test_bug_flow_doc_reconciliation.py``. They lock in the reconciliation of
the always-surfaced multivalue + link_task docs to the strict comma-REJECT policy
and prevent the drift from silently recurring after a future policy/wording flip.

Covers the spec's six acceptance criteria:
- AC1 (ts_60f6fb8a) — the strict-coercion multivalue params no longer say "Comma-separated".
- AC2 (ts_604a98b7) — create_story (story.md) carries the canonical labels footer.
- AC3 (ts_41da18ec) — link_task legacy names are annotated with their target_type short codes.
- AC4 (ts_fff279a9) — legitimate comma usages (env var, include.split, footer legacy-shape) preserved.
- AC5 (ts_86fa8816) — the forbidden-phrase guard is load-bearing (negative-wiring).
- AC6 (ts_3ae6e6f5) — no production code touched (coerce_to_list_str / include parser intact).
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
TOOLDOCS = RESOURCES_DIR / "reference" / "tool-docs"
HELPERS = CORE_DIR / "mcp" / "helpers.py"
SERVER = CORE_DIR / "mcp" / "server.py"
CONFIG = CORE_DIR / "infra" / "config.py"

CANONICAL_MARKER = "Comma-only string is REJECTED"

# Stale wording that documented exactly the input the strict comma-REJECT
# coercion (coerce_to_list_str strict_mode) rejects. Must be gone.
FORBIDDEN_STALE = [
    "Comma-separated labels",
    "Comma-separated spec test scenario IDs",
    "Comma-separated spec business rule IDs",
    "Comma-separated scoped test scenario IDs",
    "Comma-separated scoped business rule IDs",
]

# tool-docs whose strict-coercion multivalue params were reconciled.
RECONCILED_TOOLDOCS = ["ideation.md", "refinement.md", "spec.md", "sprint.md"]

# Legitimate "Comma-separated" usages that MUST be preserved (NOT strict coercion):
# board include is a plain include.split(",") (server.py:1554), and the four
# canonical-footer legacy-shape lines correctly describe comma as the fragile form.
LEGIT_KEPT = [
    ("board.md", "Comma-separated list of collections"),
    ("comment.md", "Comma-separated (legacy, fragile"),
    ("ideation.md", "Comma-separated (legacy, fragile"),
    ("refinement.md", "Comma-separated (legacy, fragile"),
    ("spec.md", "Comma-separated (legacy, fragile"),
]

LINK_TASK_ANNOTATIONS = [
    'target_type="rule"',
    'target_type="decision"',
    'target_type="tr"',
    'target_type="ir"',
    'target_type="or"',
    'target_type="scenario"',
    'target_type="contract"',
    'target_type="spec"',
]

LINK_TASK_ARGS_SSOT = "One of: rule, decision, tr, ir, or, scenario, contract, spec."


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _normalized(text: str) -> str:
    """Collapse runs of whitespace (incl. newlines) so a line-wrapped canonical
    footer like "Comma-only string\\n        is REJECTED" matches the contiguous
    marker. The canonical footer wraps by house style (see spec.md:101-103)."""
    return re.sub(r"\s+", " ", text)


def _no_forbidden_stale(text: str) -> bool:
    """The load-bearing predicate: no strict-coercion param documents 'Comma-separated'."""
    return not any(s in text for s in FORBIDDEN_STALE)


# ---------------------------------------------------------------------------
# AC1 — strict-coercion multivalue params reconciled to the canonical footer
# ---------------------------------------------------------------------------


def test_ac1_strict_coercion_lines_reconciled() -> None:
    for fname in RECONCILED_TOOLDOCS:
        text = _read(TOOLDOCS / fname)
        assert _no_forbidden_stale(text), (
            f"{fname}: a strict-coercion multivalue param still says 'Comma-separated'."
        )
        assert CANONICAL_MARKER in _normalized(text), (
            f"{fname}: missing the canonical footer marker {CANONICAL_MARKER!r}."
        )


# ---------------------------------------------------------------------------
# AC2 — create_story carries the canonical labels footer
# ---------------------------------------------------------------------------


def test_ac2_create_story_has_canonical_footer() -> None:
    text = _read(TOOLDOCS / "story.md")
    assert "okto_pulse_create_story" in text
    assert "labels:" in text, "story.md still has no labels entry."
    assert CANONICAL_MARKER in _normalized(text)
    assert _no_forbidden_stale(text)


# ---------------------------------------------------------------------------
# AC3 — link_task legacy names annotated with their target_type short codes
# ---------------------------------------------------------------------------


def test_ac3_link_task_prose_annotated() -> None:
    text = _read(TOOLDOCS / "misc.md")
    for annotation in LINK_TASK_ANNOTATIONS:
        assert annotation in text, f"misc.md link_task prose missing {annotation}."
    # The Args line stays the single source of truth, byte-unchanged.
    assert LINK_TASK_ARGS_SSOT in text


# ---------------------------------------------------------------------------
# AC4 — legitimate comma usages preserved (not over-reached)
# ---------------------------------------------------------------------------


def test_ac4_legitimate_usages_preserved() -> None:
    for fname, phrase in LEGIT_KEPT:
        text = _read(TOOLDOCS / fname)
        assert phrase in text, (
            f"{fname}: legitimate usage {phrase!r} was wrongly removed."
        )
    # config.py env-var "Comma-separated" (a genuinely comma-delimited env var) is untouched.
    assert "Comma-separated" in _read(CONFIG)


# ---------------------------------------------------------------------------
# AC5 — the forbidden-phrase guard is load-bearing (negative-wiring)
# ---------------------------------------------------------------------------


def test_ac5_guard_is_load_bearing() -> None:
    corpus = "\n".join(_read(TOOLDOCS / f) for f in RECONCILED_TOOLDOCS)
    # Positive: the real reconciled corpus passes the guard.
    assert _no_forbidden_stale(corpus)
    # Negative-wiring: re-introducing a stale strict-coercion line FAILS the guard,
    # proving it is not a vacuous pass.
    synthetic_stale = corpus + "\n    labels: Comma-separated labels (optional)\n"
    assert not _no_forbidden_stale(synthetic_stale)


# ---------------------------------------------------------------------------
# AC6 — no production code touched (canonical sources intact)
# ---------------------------------------------------------------------------


def test_ac6_production_code_byte_unchanged() -> None:
    helpers = _read(HELPERS)
    assert "def coerce_to_list_str" in helpers
    assert "rejected by REJECT policy" in helpers  # the strict comma-REJECT logic
    server = _read(SERVER)
    assert 'include.split(",")' in server  # the board include parser (server.py:1554)
