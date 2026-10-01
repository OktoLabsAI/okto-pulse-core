"""Documentary guards for the native contextual Code Evidence contract."""

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


def test_greenfield_absence_is_complete_and_earlier_formats_are_rejected() -> None:
    canonical = _read("reference/code_traceability.md")
    preflight = _read("workflows/preflight.md")

    assert (
        "`no_relevant_existing_implementation` is a successful, complete Greenfield"
        in canonical
    )
    assert "finding, not an access failure" in canonical
    assert "with complete source identity" in canonical
    assert "and no omissions" in canonical
    assert "Earlier formats are rejected without conversion." in canonical
    assert "earlier formats are rejected without conversion." in preflight


def test_removed_classification_is_not_an_operational_instruction() -> None:
    for path in (
        "reference/code_traceability.md", "reference/tool-docs/code-traceability.md",
        "workflows/preflight.md", "workflows/cards.md", "workflows/specs.md",
        "workflows/refinements.md", "reference/tool-docs/refinement.md",
        "reference/tool-docs/spec.md",
    ):
        content = _read(path)
        assert "okto_pulse_classify_legacy_code_evidence" not in content
        assert "code_traceability.evidence.classify_legacy" not in content
    documented_tools = re.findall(
        r"^## `(?P<name>okto_pulse_[^`]+)`$",
        _read("reference/tool-docs/code-traceability.md"), re.MULTILINE,
    )
    assert documented_tools
    assert not any("rebase" in name for name in documented_tools)


def test_effective_projection_and_frozen_spec_rebase_are_explicit() -> None:
    canonical = _read("reference/code_traceability.md")
    specs = _read("workflows/specs.md")
    for fragment in (
        "context_origin=authored", "complete effective evidence set even when",
        "do not silently rewrite an existing\nSpec", "`preview_sha256`",
        "A stale preview fails closed", "`contextual_evidence_coverage`",
        "`projection_complete=false`", "bounded lower bounds",
    ):
        assert fragment in canonical
    assert "apply that exact\n`preview_sha256`" in specs


def test_workflows_and_tool_docs_carry_the_contextual_contract() -> None:
    expected_fragments = {
        "workflows/refinements.md": (
            "Delivery context is required",
            "contextual V2 Code Traceability investigation",
            "no_relevant_existing_implementation",
            "TO-BE paths",
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
        ),
        "reference/tool-docs/refinement.md": (
            "delivery_context",
            "contextual V2 receipt",
            "Evidence is AS-IS only",
            "no_relevant_existing_implementation",
        ),
        "reference/tool-docs/spec.md": (
            "delivery_context_override_reason",
            "inherits and pins the exact delivery-context provenance",
            "effective `source_context`",
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
