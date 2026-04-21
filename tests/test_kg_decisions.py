"""Tests for formalized Decisions on a spec (spec b66d2562).

Covers the KG worker + Pydantic model + migration regex paths. The full
SpecService/MCP integration path is exercised by the running okto-pulse
instance via MCP roundtrips (TS1-TS4, TS10) and isn't duplicated here.

What is covered here:
  * TS5 — Pydantic Decision defaults + status literal validation
  * TS7 — migration regex extracts bullets (via re-running the same pattern
          the MCP tool uses, against a realistic spec.context fixture)
  * TS8 — migration regex is idempotent when no '## Decisions' block exists
  * TS9 — DeterministicWorker.process_spec emits a Decision EmittedNode for
          each active entry in spec.decisions[]
"""

from __future__ import annotations

import re

import pytest

from okto_pulse.core.kg.workers.deterministic_worker import (
    DeterministicWorker,
)
from okto_pulse.core.models.schemas import Decision, DecisionStatus


# ---------------------------------------------------------------------------
# TS5 — Pydantic model defaults + status validation
# ---------------------------------------------------------------------------


def test_ts5_decision_defaults():
    d = Decision(id="dec_abc12345", title="Use Kùzu", rationale="embedded graph DB")
    assert d.status == "active"
    assert d.context is None
    assert d.alternatives_considered is None
    assert d.supersedes_decision_id is None
    assert d.linked_requirements is None
    assert d.linked_task_ids is None
    assert d.notes is None


def test_ts5_decision_status_literal_roundtrip():
    for status in ("active", "superseded", "revoked"):
        d = Decision(id="dec_1", title="t", rationale="r", status=status)
        assert d.status == status


def test_ts5_decision_rejects_invalid_status():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Decision(id="dec_1", title="t", rationale="r", status="bogus")  # type: ignore[arg-type]


def test_ts5_decision_status_literal_type_exports():
    # DecisionStatus Literal alias is re-exported for external consumers
    # (e.g. MCP server type hints).
    assert DecisionStatus is not None


# ---------------------------------------------------------------------------
# TS7 / TS8 — migration regex behavior
# ---------------------------------------------------------------------------


MIGRATION_PATTERN = re.compile(
    r"(?m)^##\s+Decisions\s*\n((?:(?:[-*]\s+.*\n?)|\s*\n)+?)(?=^##\s+|\Z)"
)
BULLET_PATTERN = re.compile(r"^[-*]\s+(.+?)\s*$", re.MULTILINE)


def _extract_and_clean(context_text: str) -> tuple[list[str], str]:
    """Mirror of the logic in okto_pulse_migrate_spec_decisions (pure string
    manipulation)."""
    match = MIGRATION_PATTERN.search(context_text)
    if not match:
        return [], context_text
    bullets = [
        b.strip() for b in BULLET_PATTERN.findall(match.group(1)) if b.strip()
    ]
    new_context = MIGRATION_PATTERN.sub("", context_text).rstrip() + "\n"
    return bullets, new_context


def test_ts7_migration_extracts_bullets_and_removes_block():
    context = (
        "## Intro\n"
        "A\n"
        "\n"
        "## Decisions\n"
        "- Use Kùzu\n"
        "- Cache em Redis\n"
        "\n"
        "## Other\n"
        "B\n"
    )
    bullets, cleaned = _extract_and_clean(context)
    assert bullets == ["Use Kùzu", "Cache em Redis"]
    assert "## Decisions" not in cleaned
    assert "## Intro" in cleaned
    assert "## Other" in cleaned


def test_ts7_migration_with_star_bullets():
    context = "## Decisions\n* Alpha\n* Beta\n"
    bullets, cleaned = _extract_and_clean(context)
    assert bullets == ["Alpha", "Beta"]
    assert "## Decisions" not in cleaned


def test_ts8_migration_no_block_is_noop():
    context = "## Intro\nA\n\n## Other\nB\n"
    bullets, cleaned = _extract_and_clean(context)
    assert bullets == []
    # The cleaner rstrip+\n means the trailing whitespace normalises. Compare
    # after strip to avoid whitespace churn.
    assert cleaned.strip() == context.strip()


def test_ts8_migration_twice_is_idempotent():
    context = "## Decisions\n- One\n- Two\n\n## After\nX\n"
    bullets_first, cleaned_first = _extract_and_clean(context)
    bullets_second, cleaned_second = _extract_and_clean(cleaned_first)
    assert bullets_first == ["One", "Two"]
    # Second pass finds nothing to extract and returns context verbatim.
    assert bullets_second == []
    assert cleaned_second == cleaned_first


# ---------------------------------------------------------------------------
# TS9 — DeterministicWorker emits Decision nodes from spec.decisions[]
# ---------------------------------------------------------------------------


def _spec_with_formalized_decisions() -> dict:
    return {
        "id": "11111111-aaaa-4444-bbbb-222222222222",
        "title": "TS9 Spec",
        "description": "desc",
        "context": "plain context without markdown Decisions",
        "functional_requirements": [
            "FR A",
            "FR B",
        ],
        "technical_requirements": [],
        "acceptance_criteria": [],
        "test_scenarios": [],
        "business_rules": [],
        "api_contracts": [],
        "decisions": [
            {
                "id": "dec_ts9_one",
                "title": "Use Kùzu",
                "rationale": "embedded graph DB fits our use case",
                "status": "active",
                "linked_requirements": ["0"],
            },
            {
                "id": "dec_ts9_two",
                "title": "Cache em Redis",
                "rationale": "streaks need low-latency reads",
                "status": "active",
            },
            {
                "id": "dec_ts9_revoked",
                "title": "Use DuckDB",
                "rationale": "was the first idea — revoked",
                "status": "revoked",
            },
        ],
    }


def test_ts9_process_spec_emits_formalized_decisions():
    result = DeterministicWorker().process_spec(_spec_with_formalized_decisions())
    decision_nodes = [n for n in result.nodes if n.node_type == "Decision"]
    titles = {n.title for n in decision_nodes}
    assert "Use Kùzu" in titles
    assert "Cache em Redis" in titles
    # Revoked decisions are NOT emitted (superseded/revoked are excluded).
    assert "Use DuckDB" not in titles
    # All emitted decisions carry the spec artifact ref.
    for n in decision_nodes:
        assert n.source_artifact_ref == "spec:11111111-aaaa-4444-bbbb-222222222222"
        assert n.source_confidence == 1.0
        assert n.priority_boost == 0.0


def test_ts9_explicit_linked_requirements_use_high_confidence_edge():
    """Decision with linked_requirements=[0] emits derives_from with conf=1.0,
    while a Decision with no explicit links falls back to co-occurrence 0.6.
    """
    result = DeterministicWorker().process_spec(_spec_with_formalized_decisions())
    derives = [e for e in result.edges if e.edge_type == "derives_from"]
    assert derives  # sanity

    # 'Use Kùzu' has linked_requirements=["0"] → exactly one derives_from edge
    # with confidence=1.0 to FR index 0.
    kuzu_cids = [
        n.candidate_id for n in result.nodes
        if n.node_type == "Decision" and n.title == "Use Kùzu"
    ]
    assert len(kuzu_cids) == 1
    kuzu_cid = kuzu_cids[0]
    kuzu_edges = [e for e in derives if e.from_candidate_id == kuzu_cid]
    assert len(kuzu_edges) == 1
    assert kuzu_edges[0].confidence == 1.0

    # 'Cache em Redis' has no linked_requirements → co-occurrence edges to
    # every FR, each with confidence=0.6.
    cache_cids = [
        n.candidate_id for n in result.nodes
        if n.node_type == "Decision" and n.title == "Cache em Redis"
    ]
    assert len(cache_cids) == 1
    cache_cid = cache_cids[0]
    cache_edges = [e for e in derives if e.from_candidate_id == cache_cid]
    assert len(cache_edges) == 2  # 2 FRs in fixture
    assert all(e.confidence == 0.6 for e in cache_edges)


def test_ts9_formalized_shadows_legacy_markdown_by_title():
    """If a formalized Decision shares a title with a markdown bullet in
    context, the legacy extractor skips the duplicate."""
    spec = _spec_with_formalized_decisions()
    spec["context"] = (
        "## Decisions\n"
        "- Use Kùzu\n"  # same title as formal dec_ts9_one → skipped
        "- Legacy only\n"  # unique → still emitted via legacy path
    )
    result = DeterministicWorker().process_spec(spec)
    decision_nodes = [n for n in result.nodes if n.node_type == "Decision"]
    titles = [n.title for n in decision_nodes]
    # 'Use Kùzu' appears exactly once (from the formalized path).
    assert titles.count("Use Kùzu") == 1
    # 'Legacy only' still makes it through via the backward-compat path.
    assert "Legacy only" in titles
