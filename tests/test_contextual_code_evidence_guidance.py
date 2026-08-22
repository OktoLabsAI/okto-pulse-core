"""Documentary guards for contextual Code Evidence agent guidance.

These tests keep the bundled MCP resources aligned with the contextual V2
contract without pretending that human-only REST/UI mutations are MCP tools.
"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
MCP_DIR = ROOT / "src" / "okto_pulse" / "core" / "mcp"
RESOURCES = MCP_DIR / "resources"


def _read(relative_path: str) -> str:
    return (RESOURCES / relative_path).read_text(encoding="utf-8")


def test_canonical_guidance_separates_as_is_evidence_from_to_be_intent() -> None:
    canonical = _read("reference/code_traceability.md")

    for value in ("brownfield", "greenfield", "hybrid"):
        assert f"`{value}`" in canonical
    for source_role in (
        "current_implementation",
        "existing_scaffold",
        "existing_constraint",
        "reference_pattern",
        "uncategorized_legacy",
    ):
        assert f"`{source_role}`" in canonical
    for field in (
        "delivery_context",
        "interpretation_limit",
        "relevance_summary",
        "scope_relation",
        "source_origin",
        "baseline_provenance",
    ):
        assert f"`{field}`" in canonical

    assert "Code Evidence is always **AS-IS**" in canonical
    assert "Do not manufacture a future path and submit it as Evidence" in canonical
    assert "planned, omit this call and describe it as TO-BE" in canonical


def test_greenfield_absence_is_complete_outcome_and_v1_fails_closed() -> None:
    canonical = _read("reference/code_traceability.md")
    preflight = _read("workflows/preflight.md")

    assert (
        "`no_relevant_existing_implementation` is a successful, complete Greenfield"
        in canonical
    )
    assert "finding, not an access failure" in canonical
    assert "with complete source identity" in canonical
    assert "and no omissions" in canonical
    assert "V1 receipts and Evidence remain readable for compatibility" in canonical
    assert "New governed work must not author V1" in canonical
    assert "live inbound schema exposes only the legacy shape, stop" in canonical
    assert "If the\nlive inbound surface exposes only V1, stop" in preflight


def test_legacy_classification_is_actor_governed_append_only_and_exposed_to_mcp() -> None:
    canonical = _read("reference/code_traceability.md")
    tool_docs = _read("reference/tool-docs/code-traceability.md")

    assert "authorized human may use the UI/REST batch" in canonical
    assert "authorized\nagent may use `okto_pulse_classify_legacy_code_evidence`" in canonical
    assert "`code_traceability.evidence.classify_legacy`" in canonical
    assert "Classification is an append-only overlay" in canonical
    assert "original Evidence payload is never edited" in canonical
    assert "does not turn its V1 investigation receipt into a V2 receipt" in canonical
    assert "## `okto_pulse_classify_legacy_code_evidence`" in tool_docs
    assert "must request human input when the" in tool_docs

    documented_tools = re.findall(
        r"^## `(?P<name>okto_pulse_[^`]+)`$", tool_docs, re.MULTILINE
    )
    assert documented_tools
    assert "okto_pulse_classify_legacy_code_evidence" in documented_tools
    assert not any("rebase" in name for name in documented_tools)


def test_effective_projection_and_frozen_spec_rebase_are_explicit() -> None:
    canonical = _read("reference/code_traceability.md")
    specs = _read("workflows/specs.md")

    for origin in (
        "authored",
        "human_legacy_classification",
        "unclassified_legacy",
    ):
        assert f"`{origin}`" in canonical
    assert "complete effective evidence set even when" in canonical
    assert "classification revision/digest" in canonical
    assert "do not silently rewrite an existing\nSpec" in canonical
    assert "`preview_sha256`" in canonical
    assert "A stale preview fails closed" in canonical
    assert "apply that exact\n`preview_sha256`" in specs
    assert "`source_context_classification_inputs`" in canonical
    assert "current Refinement only" in canonical
    assert "always empty for `summary`, gate scope,\nSpec, and Card" in canonical
    assert "`provenance_note_required=true`" in canonical
    assert "`contextual_evidence_coverage`" in canonical
    assert "do not\nreinterpret the legacy `coverage` field" in canonical
    assert "`projection_complete=false`" in canonical
    assert "bounded lower bounds" in canonical


def test_workflows_and_tool_docs_carry_the_contextual_contract() -> None:
    expected_fragments = {
        "workflows/refinements.md": (
            "Delivery context is required",
            "contextual V2 Code Traceability investigation",
            "no_relevant_existing_implementation",
            "TO-BE paths",
            "authorized human uses the UI/REST classification",
        ),
        "workflows/specs.md": (
            "Establish delivery context, then investigate AS-IS source",
            "existing_scaffold",
            "reference_pattern",
            "source_context_items",
            "preview_sha256",
        ),
        "workflows/cards.md": (
            "effective `source_context`",
            "TO-BE Target intent",
            "existing_scaffold",
            "agents have no MCP mutation",
        ),
        "reference/tool-docs/refinement.md": (
            "delivery_context",
            "contextual V2 receipt",
            "Evidence is AS-IS only",
            "no_relevant_existing_implementation",
            "there is no MCP mutation",
        ),
        "reference/tool-docs/spec.md": (
            "delivery_context_override_reason",
            "inherits and pins the exact delivery-context provenance",
            "effective `source_context`",
            "append-only UI/REST action",
            "preview_sha256",
        ),
    }

    for path, fragments in expected_fragments.items():
        content = _read(path)
        for fragment in fragments:
            assert fragment in content, f"{path} is missing {fragment!r}"


def test_agent_bootstrap_contains_the_clean_context_safety_summary() -> None:
    instructions = (MCP_DIR / "agent_instructions.md").read_text(encoding="utf-8")

    for fragment in (
        "explicit `delivery_context`",
        "contextual V2 and AS-IS only",
        "Greenfield scaffold/base/reference",
        "planned TO-BE structure",
        "append-only human UI/REST governance with no MCP mutation",
        "derived Spec remains frozen",
    ):
        assert fragment in instructions


def test_operational_examples_do_not_teach_legacy_accessible_writes() -> None:
    canonical = _read("reference/code_traceability.md")

    examples = canonical.split("## Operational examples", 1)[1]
    assert 'outcome="accessible"' not in examples
    assert "contract_version=2" in examples
    assert 'outcome="evidence_applicable"' in examples
    assert 'source_role="existing_scaffold"' in examples
