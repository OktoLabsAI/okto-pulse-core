"""Deterministic contract freeze for the thirteen SK-A MCP tools.

The manifest binds each tool name to its agent-facing documentation, effective
permission policy, live FastMCP input-schema digest, and callable digest.  It is
generated from the live catalog so a rename, schema change, missing tool, or
permission-evidence drift cannot silently enter a release.

Regenerate::

    python -m okto_pulse.core.mcp.ska_tool_manifest

Verify without writing::

    python -m okto_pulse.core.mcp.ska_tool_manifest --check
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MANIFEST_VERSION = "SK-A-tools/v1"
MANIFEST_CANONICALIZATION = "json-utf8-lf-sort-keys-v1"
MANIFEST_RELATIVE_PATH = "resources/ska_tool_manifest.json"


@dataclass(frozen=True, slots=True)
class _ToolContract:
    name: str
    documentation_uri: str
    permission_policy: tuple[str, ...]
    required_source_tokens: tuple[str, ...]


_QUALITY_DOC = "okto-pulse://reference/tool-docs/quality"
_REFINEMENT_DOC = "okto-pulse://reference/tool-docs/refinement"
_SPEC_DOC = "okto-pulse://reference/tool-docs/spec"

_TOOL_CONTRACTS: tuple[_ToolContract, ...] = (
    _ToolContract(
        name="okto_pulse_record_ambiguity_assessment",
        documentation_uri=_QUALITY_DOC,
        permission_policy=(
            "SPECS_UPDATE",
            "{subject_type}.quality.assess",
        ),
        required_source_tokens=(
            "Permissions.SPECS_UPDATE",
            ".quality.assess",
        ),
    ),
    _ToolContract(
        name="okto_pulse_record_requirement_lint",
        documentation_uri=_QUALITY_DOC,
        permission_policy=("SPECS_UPDATE", "spec.quality.assess"),
        required_source_tokens=(
            "Permissions.SPECS_UPDATE",
            '"spec.quality.assess"',
        ),
    ),
    _ToolContract(
        name="okto_pulse_get_requirement_lint_preflight",
        documentation_uri=_QUALITY_DOC,
        permission_policy=("BOARD_READ", "spec.quality.read"),
        required_source_tokens=(
            "Permissions.BOARD_READ",
            '"spec.quality.read"',
        ),
    ),
    _ToolContract(
        name="okto_pulse_get_current_quality_assessment",
        documentation_uri=_QUALITY_DOC,
        permission_policy=("BOARD_READ", "{subject_type}.quality.read"),
        required_source_tokens=(
            "Permissions.BOARD_READ",
            ".quality.read",
        ),
    ),
    _ToolContract(
        name="okto_pulse_get_quality_assessment_receipt",
        documentation_uri=_QUALITY_DOC,
        permission_policy=(
            "BOARD_READ",
            "resolved receipt subject *.quality.read via preflight",
        ),
        required_source_tokens=(
            "Permissions.BOARD_READ",
            "GetQualityAssessmentReceiptCommand",
        ),
    ),
    _ToolContract(
        name="okto_pulse_list_quality_assessments",
        documentation_uri=_QUALITY_DOC,
        permission_policy=("BOARD_READ", "{subject_type}.quality.read"),
        required_source_tokens=(
            "Permissions.BOARD_READ",
            ".quality.read",
        ),
    ),
    _ToolContract(
        name="okto_pulse_list_quality_findings",
        documentation_uri=_QUALITY_DOC,
        permission_policy=("BOARD_READ", "{subject_type}.quality.read"),
        required_source_tokens=(
            "Permissions.BOARD_READ",
            ".quality.read",
        ),
    ),
    _ToolContract(
        name="okto_pulse_get_checklist_binding",
        documentation_uri=_SPEC_DOC,
        permission_policy=("BOARD_READ", "spec.checklist.read"),
        required_source_tokens=(
            "Permissions.BOARD_READ",
            '"spec.checklist.read"',
        ),
    ),
    _ToolContract(
        name="okto_pulse_start_checklist_execution",
        documentation_uri=_SPEC_DOC,
        permission_policy=("SPECS_UPDATE", "spec.checklist.execute"),
        required_source_tokens=(
            "Permissions.SPECS_UPDATE",
            '"spec.checklist.execute"',
        ),
    ),
    _ToolContract(
        name="okto_pulse_submit_checklist_execution",
        documentation_uri=_SPEC_DOC,
        permission_policy=("SPECS_UPDATE", "spec.checklist.execute"),
        required_source_tokens=(
            "Permissions.SPECS_UPDATE",
            '"spec.checklist.execute"',
        ),
    ),
    _ToolContract(
        name="okto_pulse_get_checklist_receipt",
        documentation_uri=_SPEC_DOC,
        permission_policy=("BOARD_READ", "spec.checklist.read"),
        required_source_tokens=(
            "Permissions.BOARD_READ",
            '"spec.checklist.read"',
        ),
    ),
    _ToolContract(
        name="okto_pulse_append_research_decision",
        documentation_uri=_REFINEMENT_DOC,
        permission_policy=(
            "SPECS_UPDATE",
            "refinement.research_decisions.append",
        ),
        required_source_tokens=(
            "Permissions.SPECS_UPDATE",
            '"refinement.research_decisions.append"',
        ),
    ),
    _ToolContract(
        name="okto_pulse_list_research_decisions",
        documentation_uri=_REFINEMENT_DOC,
        permission_policy=(
            "BOARD_READ",
            "refinement.research_decisions.read",
        ),
        required_source_tokens=(
            "Permissions.BOARD_READ",
            '"refinement.research_decisions.read"',
        ),
    ),
)


def _normalized_text(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _live_tools() -> dict[str, Any]:
    from okto_pulse.core.mcp import server

    return {tool.name: tool for tool in server.mcp.iter_tools()}


def _registered_resource_uris() -> frozenset[str]:
    from okto_pulse.core.mcp import effective_resource_catalog

    return frozenset(
        spec.uri for spec in effective_resource_catalog().specs() if spec.enabled
    )


def build_ska_tool_manifest() -> dict[str, Any]:
    """Build and validate the frozen contract for all twelve SK-A tools."""

    tools = _live_tools()
    resource_uris = _registered_resource_uris()
    expected_names = frozenset(contract.name for contract in _TOOL_CONTRACTS)
    missing = sorted(expected_names - tools.keys())
    if missing:
        raise ValueError("SK-A tools are not registered: " + ", ".join(missing))

    entries: list[dict[str, Any]] = []
    for contract in sorted(_TOOL_CONTRACTS, key=lambda item: item.name):
        if contract.documentation_uri not in resource_uris:
            raise ValueError(
                f"{contract.name}: documentation resource is not registered: "
                f"{contract.documentation_uri}"
            )
        tool = tools[contract.name]
        source = _normalized_text(inspect.getsource(tool.fn))
        missing_tokens = tuple(
            token for token in contract.required_source_tokens if token not in source
        )
        if missing_tokens:
            raise ValueError(
                f"{contract.name}: permission evidence drift: "
                + ", ".join(missing_tokens)
            )
        parameters = dict(tool.parameters)
        entries.append(
            {
                "name": contract.name,
                "documentation_uri": contract.documentation_uri,
                "permission_policy": list(contract.permission_policy),
                "schema_sha256": _sha256(_canonical_bytes(parameters)),
                "implementation_sha256": _sha256(source.encode("utf-8")),
            }
        )

    payload: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "canonicalization": MANIFEST_CANONICALIZATION,
        "tool_count": len(entries),
        "tools": entries,
    }
    payload["manifest_sha256"] = _sha256(_canonical_bytes(payload))
    return payload


def render_ska_tool_manifest() -> str:
    return (
        json.dumps(
            build_ska_tool_manifest(),
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
        render_ska_tool_manifest(),
        encoding="utf-8",
        newline="\n",
    )
    return target


def verify_checked_in_manifest(path: Path | None = None) -> Path:
    target = checked_in_manifest_path() if path is None else path
    expected = render_ska_tool_manifest()
    try:
        actual = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ValueError(f"SK-A tool manifest is missing: {target}") from exc
    if actual != expected:
        raise ValueError(
            "SK-A tool manifest drift: regenerate with "
            "`python -m okto_pulse.core.mcp.ska_tool_manifest`"
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
    target = (
        verify_checked_in_manifest()
        if args.check
        else regenerate_file()
    )
    print(target)


if __name__ == "__main__":
    _main()


__all__ = [
    "MANIFEST_CANONICALIZATION",
    "MANIFEST_VERSION",
    "build_ska_tool_manifest",
    "checked_in_manifest_path",
    "regenerate_file",
    "render_ska_tool_manifest",
    "verify_checked_in_manifest",
]
