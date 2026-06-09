"""F3 — Degraded-KG ``kg_health``-first protocol fallback (doc-only, spec a1230939).

Doc-content tests for the Degraded-KG Fallback Rule added to
``workflows/kg.md`` (single source of truth) and the reference pointer in
``workflows/ideations.md``. Pattern mirrors ``tests/test_mcp_resources.py``:
read the resource markdown via ``RESOURCES_DIR / "...".read_text()`` and make
pure file-content assertions (no server/board/graph required), plus a
predicate cross-check that imports the real ``_RISK_STATE_HARD_REJECT``.

Scenarios (spec a1230939):
  ts_dcf3aadd, ts_47cc9fc7  (AC1)
  ts_13ab47d1, ts_be456c60  (AC2 / AC4)
  ts_bfb501d5, ts_22ba3c22  (AC3 / AC5)
  ts_f3966e10, ts_dc56000c  (AC6 / AC7)
"""

import re
from pathlib import Path

import pytest

RESOURCES_DIR = (
    Path(__file__).parent.parent / "src" / "okto_pulse" / "core" / "mcp" / "resources"
)
KG_MD = RESOURCES_DIR / "workflows" / "kg.md"
IDEATIONS_MD = RESOURCES_DIR / "workflows" / "ideations.md"

# The pinned canonical sentence: it lives ONLY in kg.md and MUST NOT be
# duplicated verbatim in ideations.md (single-source-of-truth invariant).
PINNED_SENTENCE = (
    "Skipping the Stage 1 triad on a degraded graph is expected-and-logged "
    "and is **not a protocol violation**."
)


def _kg_text() -> str:
    return KG_MD.read_text(encoding="utf-8")


def _ideations_text() -> str:
    return IDEATIONS_MD.read_text(encoding="utf-8")


def _extract_rule_block(kg_text: str) -> str:
    """Return the Degraded-KG Fallback Rule block (from its heading up to the
    Stage 1 section that must follow it). Asserts the rule precedes Stage 1
    (the ``kg_health``-first ordering invariant)."""
    start = kg_text.find("Degraded-KG Fallback Rule")
    assert start != -1, "Degraded-KG Fallback Rule heading not found in kg.md"
    stage1 = kg_text.find("**Stage 1 — Ideation")
    assert stage1 != -1 and start < stage1, (
        "the Degraded-KG Fallback Rule must appear BEFORE the Stage 1 triad "
        "(kg_health-first ordering)"
    )
    return kg_text[start:stage1]


def _ac1_holds(kg_text: str) -> bool:
    """AC1 content predicate (reused by the negative-wiring test): the kg.md
    degraded-mode rule exists, is keyed on ``graph_state``, names ``kg_health``
    + both degraded literals, and is ``kg_health``-first (the rule precedes the
    Stage 1 triad). Returns True/False; never raises on rule-less input."""
    if "Degraded-KG Fallback Rule" not in kg_text:
        return False
    rule_pos = kg_text.find("Degraded-KG Fallback Rule")
    stage1_pos = kg_text.find("**Stage 1 — Ideation")
    if stage1_pos == -1 or rule_pos > stage1_pos:
        return False  # rule absent or not kg_health-first
    rule = kg_text[rule_pos:stage1_pos]
    required = ["okto_pulse_kg_health", "graph_state", "recovery_needed", "quarantined"]
    if not all(tok in rule for tok in required):
        return False
    return "first" in rule.lower()


# --- AC1 (ts_dcf3aadd, ts_47cc9fc7) -----------------------------------------


def test_ts_dcf3aadd_kg_rule_exists_graph_state_keyed_kg_health_first():
    assert _ac1_holds(_kg_text()), (
        "kg.md must contain the graph_state-keyed, kg_health-first degraded-mode rule"
    )


def test_ts_47cc9fc7_ac1_check_is_negative_wired():
    """Negative-wiring guard (mirrors test_smoke_detects_synthetic_broken_link):
    the AC1 predicate must PASS on the real edited kg.md and FAIL on a synthetic
    kg.md with the rule block excised — proving the check is load-bearing, not a
    vacuous pass."""
    real = _kg_text()
    assert _ac1_holds(real), "AC1 predicate must hold on the real edited kg.md"
    synthetic = real.replace(_extract_rule_block(real), "")
    assert "Degraded-KG Fallback Rule" not in synthetic
    assert not _ac1_holds(synthetic), (
        "AC1 predicate vacuously passed on rule-less text — the check is not wired"
    )


# --- AC2 / AC4 (ts_13ab47d1, ts_be456c60) -----------------------------------


def test_ts_13ab47d1_degrade_with_warning_not_a_violation():
    rule = _extract_rule_block(_kg_text())
    low = rule.lower()
    assert "proceed" in low and "warning" in low, (
        "rule must instruct proceed-with-a-warning on a degraded graph"
    )
    assert "record the degraded" in low, (
        "rule must instruct recording the degraded graph_state in the ideation"
    )
    assert "not a protocol violation" in low, (
        "rule must state the degraded triad skip is NOT a protocol violation"
    )


def test_ts_be456c60_ordered_forward_path_and_kuzu_error_expected():
    rule = _extract_rule_block(_kg_text())
    low = rule.lower()
    steps = [
        low.find("call `okto_pulse_kg_health"),          # (1) call kg_health
        low.find("branch on the degraded"),               # (2) observe/branch on graph_state
        low.find("record the degraded"),                  # (3) record degraded graph_state
        low.find("proceed past `okto_pulse_evaluate_ideation"),  # (4) proceed past evaluate
    ]
    assert all(p != -1 for p in steps), f"missing one of the 4 ordered steps: {steps}"
    assert steps == sorted(steps), f"the 4 forward-path steps are not in order: {steps}"
    assert "graph_unavailable" in low, "rule must name the structured graph_unavailable"
    assert "expected" in low, "rule must label the graph_unavailable as EXPECTED on a degraded board"
    assert "Use the explicit KG Health recovery flow" in rule, (
        "rule must surface the existing kuzu_error recovery-flow hint"
    )


# --- AC3 (ts_bfb501d5) -------------------------------------------------------


def test_ts_bfb501d5_ideations_references_without_duplication():
    ide = _ideations_text()
    kg = _kg_text()
    assert ("Degraded-KG Fallback Rule" in ide) or ("Degraded-KG exception" in ide), (
        "ideations.md must reference the degraded-KG rule"
    )
    assert "Knowledge Graph chapter" in ide, "ideations.md must point to the KG chapter (kg.md)"
    assert ("recovery_needed" in ide) or ("degraded" in ide.lower()), (
        "ideations.md must name the trigger condition"
    )
    assert PINNED_SENTENCE in kg, "pinned canonical sentence must live in kg.md"
    assert PINNED_SENTENCE not in ide, (
        "ideations.md must NOT duplicate the canonical rule sentence (single source of truth)"
    )


# --- AC5 (ts_22ba3c22) -------------------------------------------------------


def test_ts_22ba3c22_degraded_predicate_matches_backpressure():
    from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT

    expected = {getattr(s, "value", s) for s in _RISK_STATE_HARD_REJECT}
    assert expected == {"recovery_needed", "quarantined"}, (
        f"_RISK_STATE_HARD_REJECT changed underneath the doc: {expected}"
    )
    rule = _extract_rule_block(_kg_text())
    for literal in expected:
        assert literal in rule, f"rule must name the degraded trigger {literal!r}"
    # the rule must name NO OTHER HealthState literal as a degraded trigger
    for other in ("at_risk", "backpressure", "healthy"):
        assert other not in rule, (
            f"rule names a non-degraded HealthState literal {other!r}; the degraded "
            f"set must be exactly {expected}"
        )


# --- AC6 (ts_f3966e10) — NON-GOAL fences -------------------------------------


def test_ts_f3966e10_non_goal_fences():
    rule = _extract_rule_block(_kg_text()).lower()
    for imperative in (
        "rebuild the kg",
        "rebuild the graph",
        "run rebuild_preflight",
        "reset the graph",
        "repair the kg",
    ):
        assert imperative not in rule, (
            f"rule must not instruct '{imperative}' — recovery is a separate out-of-scope flow"
        )
    # graph_unavailable is the canonical PA-2 rename of the structured error signal;
    # this fence no longer prohibits it — the rule names it as the EXPECTED signal.
    # (The prohibition was against defining a NEW error code; graph_unavailable is a
    # rename of the existing kuzu_error, not a new code.)


# --- AC7 (ts_dc56000c) — files remain valid registered MCP resources ---------


def test_ts_dc56000c_files_remain_valid_resources():
    for md in (KG_MD, IDEATIONS_MD):
        content = md.read_text(encoding="utf-8")
        assert content.startswith("---"), f"{md.name} lost its frontmatter"
        m = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
        assert m and 'version: "1.0"' in m.group(1), f"{md.name} frontmatter broken"

    from okto_pulse.core.mcp import server as _srv

    registered = {entry[0]: entry[1] for entry in _srv._RESOURCE_REGISTRY}
    resource_dir = _srv._get_resource_dir()
    for uri in ("okto-pulse://workflows/kg", "okto-pulse://workflows/ideations"):
        assert uri in registered, f"{uri} no longer registered"
        assert (resource_dir / registered[uri]).exists(), f"{uri} points to a missing file"

    # the edits live in lazy resource files, OFF the always-loaded instructions
    # blob, so the initial-footprint budget must be unaffected.
    tiktoken = pytest.importorskip("tiktoken")
    enc = tiktoken.get_encoding("cl100k_base")
    instr_tokens = len(enc.encode(_srv._load_instructions()))
    assert instr_tokens <= 10_000, (
        f"instructions footprint regressed to {instr_tokens} tokens (>10K)"
    )
