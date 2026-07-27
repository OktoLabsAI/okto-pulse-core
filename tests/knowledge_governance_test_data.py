from __future__ import annotations

from typing import Any


def valid_governance_metadata(
    *, purpose: str = "Describe the reference contract"
) -> dict[str, Any]:
    return {
        "contract_version": 1,
        "authority": "advisory",
        "classification": "technical_reference",
        "purpose": purpose,
        "audience": ["agent", "maintainer"],
        "relevance_reason": "Needed to reproduce the baseline",
        "provenance": [{"kind": "code", "reference": "repo:core@abc123"}],
        "as_of": "2026-07-22T20:00:00-03:00",
        "version_ref": "commit:abc123",
        "version_not_applicable_reason": None,
        "scope": "Knowledge Base reads and writes",
        "limitations": "Advisory evidence only",
        "stable_references": [],
        "lifecycle_state": "current",
        "superseded_by": None,
        "superseded_reason": None,
        "exclusive_authority_check": "passed",
        "normative_destinations": [],
    }


__all__ = ["valid_governance_metadata"]
