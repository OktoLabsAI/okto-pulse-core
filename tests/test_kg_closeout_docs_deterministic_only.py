"""Spec f24c43f7 / card 3006d47c — KG closeout docs are aligned with the runtime
writer-ownership table: Criterion and Constraint are deterministic-only and must never
appear as free cognitive closeout candidates (FR1/FR2/FR3/FR4/FR7, AC1/AC2/AC3/AC7),
scenarios ts_2996a90e + ts_9806021d.

Docs regression guard: it FAILS if any official KG closeout instruction (the spec-done
trigger table, the mandatory closeout sequence, the node-type ownership allowlist, or
the consolidation tool-doc) ever lists Criterion/Constraint as a cognitive candidate.
The check is semantic, not exact-phrase: within the closeout/consolidation sections,
every mention of Criterion/Constraint must sit in a *deterministic* context (so a future
edit that re-adds them as free cognitive candidates trips the guard), while the benign
mentions elsewhere (the explain_constraint ``constraint_id`` param, spec-authoring prose)
are out of scope and untouched.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_kg_closeout_docs_deterministic_only.py
"""

from __future__ import annotations

import pathlib
import re

import pytest

import okto_pulse.core.mcp as mcp_pkg

_MCP_DIR = pathlib.Path(mcp_pkg.__file__).parent
WORKFLOW_KG = _MCP_DIR / "resources" / "workflows" / "kg.md"
TOOLDOCS_KG = _MCP_DIR / "resources" / "reference" / "tool-docs" / "kg.md"
AGENT_INSTRUCTIONS = _MCP_DIR / "agent_instructions.md"

# The node types a COGNITIVE writer may create (connectivity_guard.py ownership table:
# Decision=dual, Learning/Alternative/Assumption=cognitive). Criterion/Constraint are
# DETERMINISTIC-only and must not be cognitive candidates.
DETERMINISTIC_ONLY = ("Criterion", "Constraint")
_CRIT_CONSTR = re.compile(r"\b(Criterion|Constraint)\b")
_DETERMINISTIC = re.compile(r"deterministic", re.IGNORECASE)


def _logical_lines(block: str) -> list[str]:
    """Merge markdown continuation lines (indented wraps of a bullet) into one logical
    line, so a multi-line bullet is evaluated as a single unit."""
    out: list[str] = []
    for raw in block.splitlines():
        if raw[:1] in (" ", "\t") and out:
            out[-1] = out[-1] + " " + raw.strip()
        else:
            out.append(raw)
    return out


def _claim_units(block: str) -> list[str]:
    """Split a markdown block into individual 'claim units' so each is evaluated whole:
    table rows and bullets stay line-by-line (each is a distinct claim), while a wrapped
    prose paragraph is joined into one unit (its sentence shouldn't be split mid-wrap)."""
    units: list[str] = []
    for para in re.split(r"\n\s*\n", block):
        lines = [ln for ln in _logical_lines(para) if ln.strip()]
        if not lines:
            continue
        if all(ln.lstrip().startswith(("|", "-", "*")) for ln in lines):
            units.extend(lines)  # table / bullet list — one claim per row
        else:
            units.append(" ".join(ln.strip() for ln in lines))  # prose paragraph
    return units


def _section(text: str, header_substring: str) -> str:
    """Return the markdown block from the heading containing ``header_substring`` up to
    the next same-or-higher-level heading."""
    lines = text.splitlines()
    start = next(
        (i for i, ln in enumerate(lines) if ln.startswith("#") and header_substring in ln),
        None,
    )
    assert start is not None, f"section heading containing {header_substring!r} not found"
    level = len(lines[start]) - len(lines[start].lstrip("#"))
    end = len(lines)
    for j in range(start + 1, len(lines)):
        ln = lines[j]
        if ln.startswith("#"):
            lvl = len(ln) - len(ln.lstrip("#"))
            if lvl <= level:
                end = j
                break
    return "\n".join(lines[start:end])


def _assert_only_deterministic(block: str, where: str) -> None:
    """Every claim unit mentioning Criterion/Constraint must be in a deterministic
    context — i.e. it can never be presented as a free cognitive candidate."""
    for unit in _claim_units(block):
        if _CRIT_CONSTR.search(unit) and not _DETERMINISTIC.search(unit):
            raise AssertionError(
                f"{where}: '{unit.strip()}' mentions Criterion/Constraint without a "
                f"deterministic context — they must stay deterministic-only, never a "
                f"free cognitive closeout candidate."
            )


# ---------------------------------------------------------------------------
# ts_2996a90e — workflow trigger table + mandatory closeout sequence
# ---------------------------------------------------------------------------


def test_ts_2996a90e_workflow_kg_closeout_is_deterministic_only():
    text = WORKFLOW_KG.read_text(encoding="utf-8")

    # 1) the spec-done trigger row never lists Criterion/Constraint as cognitive.
    trigger_row = next(
        (ln for ln in text.splitlines() if "Spec reaches" in ln and "done" in ln), None
    )
    assert trigger_row is not None, "spec-done trigger row not found"
    _assert_only_deterministic(trigger_row, "kg.md spec-done trigger row")
    # the cognitive candidates it DOES name are the real cognitive node types.
    for cognitive in ("Decision", "Assumption", "Alternative"):
        assert cognitive in trigger_row

    # 2) the Cognitive KG Closeout section (mandatory sequence + ownership allowlist).
    closeout = _section(text, "Cognitive KG Closeout")
    _assert_only_deterministic(closeout, "kg.md Cognitive KG Closeout section")
    # the mandatory closeout sequence names the cognitive-writable types...
    assert "Identify cognitive candidates" in closeout
    for cognitive in ("Decision", "Assumption", "Alternative", "Learning"):
        assert cognitive in closeout
    # ...and documents the deterministic materialization/reference semantics (AC3).
    assert _DETERMINISTIC.search(closeout)
    assert "ownership" in closeout.lower() or "allowlist" in closeout.lower()


def test_ownership_allowlist_table_marks_criterion_constraint_deterministic():
    closeout = _section(WORKFLOW_KG.read_text(encoding="utf-8"), "Cognitive KG Closeout")
    # the allowlist must carry the wrong-writer rejection contract (FR5 reference, AC8).
    assert "source_type_not_supported" in closeout
    assert "writer_not_connectivity_owner" in closeout
    # and the deterministic worker is named as the owner of Criterion/Constraint.
    rows = [ln for ln in _logical_lines(closeout) if _CRIT_CONSTR.search(ln)]
    assert rows, "closeout section must explain Criterion/Constraint ownership"
    assert any("deterministic worker" in ln.lower() for ln in rows)


# ---------------------------------------------------------------------------
# ts_9806021d — tool-docs + agent instructions
# ---------------------------------------------------------------------------


def test_ts_9806021d_tool_docs_document_deterministic_references():
    text = TOOLDOCS_KG.read_text(encoding="utf-8")
    add_node = _section(text, "okto_pulse_kg_add_node_candidate")
    # the consolidation node-candidate tool-doc documents the deterministic-only rule...
    _assert_only_deterministic(add_node, "tool-docs/kg.md add_node_candidate")
    assert _DETERMINISTIC.search(add_node)
    assert "source_type_not_supported" in add_node
    assert "writer_not_connectivity_owner" in add_node
    # The condensed section delegates parameters to the live description; the
    # canonical workflow still documents the benign constraint_id argument.
    explain = _section(text, "okto_pulse_kg_explain_constraint")
    assert "live tool description" in explain
    assert "okto_pulse_kg_explain_constraint(board_id, constraint_id=" in (
        WORKFLOW_KG.read_text(encoding="utf-8")
    )


def test_agent_instructions_introduce_no_conflicting_cognitive_list():
    text = AGENT_INSTRUCTIONS.read_text(encoding="utf-8")
    # agent_instructions only references the kg workflow; it must not introduce its own
    # closeout candidate list that lists Criterion/Constraint as cognitive.
    assert "workflows/kg" in text
    _assert_only_deterministic(text, "agent_instructions.md")


# ---------------------------------------------------------------------------
# FR7 / AC7 — consolidated guard across every official closeout source
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path,section",
    [
        (WORKFLOW_KG, "When and How to Consolidate"),
        (WORKFLOW_KG, "Cognitive KG Closeout"),
        (TOOLDOCS_KG, "okto_pulse_kg_add_node_candidate"),
    ],
)
def test_fr7_no_official_closeout_list_recreates_criterion_constraint_as_cognitive(path, section):
    block = _section(path.read_text(encoding="utf-8"), section)
    _assert_only_deterministic(block, f"{path.name}:{section}")
    # the deterministic-only node types are never inside an explicit cognitive-candidate
    # enumeration (a unit that says "cognitive candidates ... Criterion/Constraint").
    for ln in _claim_units(block):
        low = ln.lower()
        if "cognitive candidate" in low or "you may create" in low:
            for dtype in DETERMINISTIC_ONLY:
                # allowed only when explicitly excluded as deterministic-only.
                if dtype in ln:
                    assert _DETERMINISTIC.search(ln), (
                        f"{path.name}:{section}: cognitive-candidate line lists {dtype} "
                        f"without marking it deterministic-only."
                    )
