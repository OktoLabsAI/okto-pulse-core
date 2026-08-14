"""Deterministic checked-in manifest for governed agent-facing resources.

The general MCP resource manifest proves the effective runtime projection.  This
backward-compatible manifest entry point adds the review contracts introduced
by SK-A and SK-B: affected resource URIs, canonical content digests, required
Markdown headings, and verified cross-links.  It is generated from the live
Core resource catalog so a resource override, missing registration, stale
document, or broken navigation link cannot silently pass review.

Regenerate after changing an affected resource or the live tools catalog::

    python -m okto_pulse.core.mcp.ska_resource_manifest

Verify the checked-in artifact without writing::

    python -m okto_pulse.core.mcp.ska_resource_manifest --check
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from importlib.resources import files as package_files
from pathlib import Path
from typing import Any


MANIFEST_VERSION = "agent-resources/v2"
MANIFEST_CANONICALIZATION = "json-utf8-lf-sort-keys-v1"
MANIFEST_RELATIVE_PATH = "resources/ska_resource_manifest.json"

_RESOURCE_URI_RE = re.compile(r"okto-pulse://[A-Za-z0-9_/-]+")


@dataclass(frozen=True)
class _ResourceContract:
    uri: str
    required_headings: tuple[str, ...]
    required_cross_links: tuple[str, ...] = ()


_RESOURCE_CONTRACTS: tuple[_ResourceContract, ...] = (
    _ResourceContract(
        uri="okto-pulse://reference/destructive_ops",
        required_headings=("## Rules of Engagement",),
        required_cross_links=("okto-pulse://reference/quality-assessments",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/errors",
        required_headings=("## SK-A Quality, Research Decisions, and Checklist",),
        required_cross_links=("okto-pulse://reference/quality-assessments",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/list_tools",
        required_headings=("# Consolidated List Tools (P0.B)",),
        required_cross_links=("okto-pulse://reference/quality-assessments",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/projection-profiles",
        required_headings=("# Projection Profiles & the Response Envelope",),
        required_cross_links=("okto-pulse://reference/quality-assessments",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/policy-compliance",
        required_headings=(
            "# Semantic guideline protocol",
            "## Revision and adoption",
            "## Recording an assessment",
            "## Gate and lifecycle edition",
            "## Lists, pagination and errors",
            "## Capabilities and KG",
        ),
        required_cross_links=(
            "okto-pulse://reference/errors",
            "okto-pulse://reference/transitions",
            "okto-pulse://workflows/kg",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/code-traceability",
        required_headings=(
            "# Agent-mediated Code Traceability",
            "## Technical Evidence and Technical Anchors",
            "## Evidence and Targets",
            "## When the agent must record",
            "## Mandatory external preflight",
            "## Targets and Resolution receipts",
            "## Operational examples",
            "## Completion criteria",
            "## Advisory mode is non-blocking, not no-op",
            "## Security and untrusted code",
            "## Errors and remediation",
        ),
        required_cross_links=(
            "okto-pulse://reference/errors",
            "okto-pulse://reference/transitions",
            "okto-pulse://reference/projection-profiles",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/tool-docs/code-traceability",
        required_headings=(
            "# Tool docs — Code Traceability",
            "## Technical Evidence versus Technical Anchors",
            "## Mandatory operation and fence order",
            "## `okto_pulse_submit_code_evidence`",
            "## `okto_pulse_link_code_evidence`",
            "## `okto_pulse_create_implementation_target`",
            "## `okto_pulse_submit_implementation_target_resolution`",
            "## `okto_pulse_submit_implementation_target_execution_receipt`",
            "## Advisory outcome",
        ),
        required_cross_links=("okto-pulse://reference/code-traceability",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/quality-assessments",
        required_headings=(
            "# Quality assessments, pinpointing, and lifecycle results",
            "## Subject and assessment matrix",
            "## Mandatory pre-flight for a write",
            "## Pinpoint findings and questions",
            "## Lifecycle state and technical audit",
            "## Reads, projections, and pagination",
            "## Gate behavior and error parity",
            "## Related resources",
        ),
        required_cross_links=(
            "okto-pulse://reference/errors",
            "okto-pulse://reference/projection-profiles",
            "okto-pulse://reference/spec_gates",
            "okto-pulse://reference/tool-docs/quality",
            "okto-pulse://workflows/preflight",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/spec_gates",
        required_headings=(
            "### Canonical Spec Validation scoring rubric",
            "### Dimension boundaries — do not double-count automatically",
            "### Evidence, justifications, and pinpoint anchors",
            "### Calibrated examples",
            "### Recommendation semantics",
            "## Curated Spec Checklist Gate — `/specify/v1`",
        ),
        required_cross_links=(
            "okto-pulse://reference/tool-docs/spec",
            "okto-pulse://reference/transitions",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/tool-docs/guideline",
        required_headings=(
            "# Tool docs — `guideline`",
            "## Governed policy tool conventions",
        ),
        required_cross_links=("okto-pulse://reference/policy-compliance",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/tool-docs/quality",
        required_headings=(
            "## `okto_pulse_record_ambiguity_assessment`",
            "## `okto_pulse_record_requirement_lint`",
            "## `okto_pulse_get_requirement_lint_preflight`",
            "## `okto_pulse_get_current_quality_assessment`",
            "## `okto_pulse_get_quality_assessment_receipt`",
            "## `okto_pulse_list_quality_assessments`",
            "## `okto_pulse_list_quality_findings`",
        ),
        required_cross_links=("okto-pulse://reference/quality-assessments",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/tool-docs/refinement",
        required_headings=(
            "## `okto_pulse_append_research_decision`",
            "## `okto_pulse_list_research_decisions`",
        ),
        required_cross_links=("okto-pulse://workflows/refinements",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/tool-docs/spec",
        required_headings=(
            "## `okto_pulse_get_checklist_binding`",
            "## `okto_pulse_start_checklist_execution`",
            "## `okto_pulse_submit_checklist_execution`",
            "## `okto_pulse_get_checklist_receipt`",
            "## `okto_pulse_submit_spec_validation`",
        ),
        required_cross_links=("okto-pulse://reference/spec_gates",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/tool-docs/test-scenario",
        required_headings=(
            "## `okto_pulse_add_test_scenario`",
            "## `okto_pulse_delete_test_scenario`",
            "## `okto_pulse_list_test_scenarios`",
            "## `okto_pulse_update_test_scenario`",
            "## `okto_pulse_execute_test_scenario_evidence`",
            "## `okto_pulse_update_test_scenario_status`",
        ),
        required_cross_links=("okto-pulse://reference/card_types",),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/tools_catalog",
        required_headings=(
            "## Quality Assessments",
            "## Refinements",
            "## Specs — lifecycle & gates",
        ),
        required_cross_links=(
            "okto-pulse://reference/tool-docs/quality",
            "okto-pulse://reference/tool-docs/refinement",
            "okto-pulse://reference/tool-docs/spec",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://reference/transitions",
        required_headings=("## Spec transitions",),
        required_cross_links=("okto-pulse://reference/spec_gates",),
    ),
    _ResourceContract(
        uri="okto-pulse://workflows/cards",
        required_headings=(
            "### Quality evidence while executing a card",
            "## 2.12 Agent-mediated implementation targets",
        ),
        required_cross_links=(
            "okto-pulse://reference/quality-assessments",
            "okto-pulse://reference/policy-compliance",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://workflows/ideations",
        required_headings=("## 2.1a Lifecycle Ambiguity Results and Pinpointing",),
        required_cross_links=(
            "okto-pulse://reference/tool-docs/quality",
            "okto-pulse://reference/policy-compliance",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://workflows/kg",
        required_headings=("### SK-A relational authority and graph projection",),
        required_cross_links=(
            "okto-pulse://reference/quality-assessments",
            "okto-pulse://reference/policy-compliance",
            "okto-pulse://workflows/refinements",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://workflows/preflight",
        required_headings=(
            "### Quality Assessment pre-flight — before record/read/gate use",
        ),
        required_cross_links=(
            "okto-pulse://reference/quality-assessments",
            "okto-pulse://reference/policy-compliance",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://workflows/refinements",
        required_headings=(
            "## Lifecycle Ambiguity Results and Pinpointing",
            "## Operational Research Decision Ledger",
            "### Agent-mediated Code Traceability sequence",
        ),
        required_cross_links=(
            "okto-pulse://reference/tool-docs/quality",
            "okto-pulse://reference/policy-compliance",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://workflows/specs",
        required_headings=(
            "## 2.3a Detail Saturation — DO NOT Push Forward With Gaps",
            "## 2.3d Agent-mediated Code Traceability",
            "### Spec Quality — Canonical Agent Flow",
            "#### Surface responsibilities",
            "#### Agent flow by Spec status",
            "#### Token-efficient read sequence",
            "### The Spec Validation Gate",
        ),
        required_cross_links=(
            "okto-pulse://reference/spec_gates",
            "okto-pulse://reference/tool-docs/quality",
            "okto-pulse://reference/policy-compliance",
        ),
    ),
    _ResourceContract(
        uri="okto-pulse://workflows/sprints",
        required_headings=("# Sprints Workflow — Lifecycle & Evaluation",),
        required_cross_links=("okto-pulse://reference/policy-compliance",),
    ),
)

_INSTRUCTION_REQUIRED_HEADINGS = (
    "## Quick Navigation",
    "## Pre-Flight Checklist (READ FIRST)",
    "## Available Tools — Critical Categories",
)
_INSTRUCTION_REQUIRED_CROSS_LINKS = (
    "okto-pulse://reference/policy-compliance",
    "okto-pulse://reference/quality-assessments",
)
_INSTRUCTION_ALLOWED_TEMPLATE_LINKS = frozenset(
    {
        "okto-pulse://workflows/",
    }
)


def _normalized_text(content: str) -> str:
    return content.replace("\r\n", "\n").replace("\r", "\n")


def _content_sha256(content: str) -> str:
    return hashlib.sha256(_normalized_text(content).encode("utf-8")).hexdigest()


def _cross_links(content: str) -> tuple[str, ...]:
    return tuple(sorted(set(_RESOURCE_URI_RE.findall(content))))


def _validate_content(
    *,
    identity: str,
    content: str,
    required_headings: tuple[str, ...],
    required_cross_links: tuple[str, ...],
    known_uris: frozenset[str],
    allowed_template_links: frozenset[str] = frozenset(),
) -> tuple[str, ...]:
    lines = frozenset(_normalized_text(content).splitlines())
    missing_headings = tuple(
        heading for heading in required_headings if heading not in lines
    )
    if missing_headings:
        raise ValueError(
            f"{identity}: missing required headings: {', '.join(missing_headings)}"
        )

    links = _cross_links(content)
    missing_links = tuple(link for link in required_cross_links if link not in links)
    if missing_links:
        raise ValueError(
            f"{identity}: missing required cross-links: {', '.join(missing_links)}"
        )
    unresolved = tuple(
        link
        for link in links
        if link not in known_uris and link not in allowed_template_links
    )
    if unresolved:
        raise ValueError(f"{identity}: unresolved cross-links: {', '.join(unresolved)}")
    return links


def _effective_specs_by_uri() -> dict[str, Any]:
    from okto_pulse.core.mcp import effective_resource_catalog

    specs = effective_resource_catalog().specs()
    return {spec.uri: spec for spec in specs if spec.enabled}


def _instruction_content() -> str:
    resource = package_files("okto_pulse.core.mcp").joinpath("agent_instructions.md")
    return resource.read_text(encoding="utf-8")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def build_ska_resource_manifest() -> dict[str, Any]:
    """Build and validate the governed resource-review manifest."""

    specs_by_uri = _effective_specs_by_uri()
    known_uris = frozenset(specs_by_uri)
    missing_resources = tuple(
        contract.uri
        for contract in _RESOURCE_CONTRACTS
        if contract.uri not in specs_by_uri
    )
    if missing_resources:
        raise ValueError(
            "Governed resources are not registered: " + ", ".join(missing_resources)
        )

    resources: list[dict[str, Any]] = []
    for contract in sorted(_RESOURCE_CONTRACTS, key=lambda item: item.uri):
        content = specs_by_uri[contract.uri].read()
        links = _validate_content(
            identity=contract.uri,
            content=content,
            required_headings=contract.required_headings,
            required_cross_links=contract.required_cross_links,
            known_uris=known_uris,
        )
        resources.append(
            {
                "uri": contract.uri,
                "content_sha256": _content_sha256(content),
                "required_headings": list(contract.required_headings),
                "required_cross_links": list(contract.required_cross_links),
                "cross_links": list(links),
            }
        )

    instruction_content = _instruction_content()
    instruction_links = _validate_content(
        identity="agent_instructions.md",
        content=instruction_content,
        required_headings=_INSTRUCTION_REQUIRED_HEADINGS,
        required_cross_links=_INSTRUCTION_REQUIRED_CROSS_LINKS,
        known_uris=known_uris,
        allowed_template_links=_INSTRUCTION_ALLOWED_TEMPLATE_LINKS,
    )
    payload: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "canonicalization": MANIFEST_CANONICALIZATION,
        "resource_count": len(resources),
        "resources": resources,
        "instruction_bundle": {
            "source": "okto_pulse/core/mcp/agent_instructions.md",
            "content_sha256": _content_sha256(instruction_content),
            "required_headings": list(_INSTRUCTION_REQUIRED_HEADINGS),
            "required_cross_links": list(_INSTRUCTION_REQUIRED_CROSS_LINKS),
            "cross_links": list(instruction_links),
            "allowed_template_links": sorted(_INSTRUCTION_ALLOWED_TEMPLATE_LINKS),
        },
    }
    payload["manifest_sha256"] = hashlib.sha256(_canonical_bytes(payload)).hexdigest()
    return payload


def render_ska_resource_manifest() -> str:
    """Render deterministic, review-friendly UTF-8 JSON."""

    return (
        json.dumps(
            build_ska_resource_manifest(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def checked_in_manifest_path() -> Path:
    return Path(__file__).parent / MANIFEST_RELATIVE_PATH


def regenerate_file(path: Path | None = None) -> Path:
    target = checked_in_manifest_path() if path is None else path
    target.write_text(
        render_ska_resource_manifest(),
        encoding="utf-8",
        newline="\n",
    )
    return target


def verify_checked_in_manifest(path: Path | None = None) -> Path:
    target = checked_in_manifest_path() if path is None else path
    expected = render_ska_resource_manifest()
    try:
        actual = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ValueError(f"agent resource manifest is missing: {target}") from exc
    if actual != expected:
        raise ValueError(
            "agent resource manifest drift: regenerate with "
            "`python -m okto_pulse.core.mcp.ska_resource_manifest`"
        )
    return target


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify the checked-in manifest without writing it.",
    )
    args = parser.parse_args()
    target = verify_checked_in_manifest() if args.check else regenerate_file()
    print(target)


if __name__ == "__main__":
    _main()


__all__ = [
    "MANIFEST_CANONICALIZATION",
    "MANIFEST_VERSION",
    "build_ska_resource_manifest",
    "checked_in_manifest_path",
    "regenerate_file",
    "render_ska_resource_manifest",
    "verify_checked_in_manifest",
]
