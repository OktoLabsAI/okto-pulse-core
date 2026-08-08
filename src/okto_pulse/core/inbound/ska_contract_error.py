"""Shared REST/MCP error projection for the SK-A public families."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

SkaContractFamily = Literal["research_decision", "checklist"]

_ALIASES: dict[SkaContractFamily, dict[str, str]] = {
    "research_decision": {
        "resolved_research_evidence_required": "resolved_evidence_required",
    },
    "checklist": {
        "checklist_items_incomplete": "checklist_incomplete",
    },
}
_SPECIFIC_CODES: dict[SkaContractFamily, frozenset[str]] = {
    "research_decision": frozenset({"resolved_evidence_required"}),
    "checklist": frozenset(
        {
            "checklist_incomplete",
            "checklist_binding_off",
            "human_actor_required",
        }
    ),
}
_NEXT_ACTION = {
    "forbidden": "request_authority",
    "not_found": "verify_reference",
    "version_conflict": "refresh_and_retry",
    "validation_failed": "fix_input",
    "resolved_evidence_required": "provide_evidence_or_justification",
    "checklist_incomplete": "submit_all_curated_items",
    "checklist_binding_off": "request_human_binding_change",
    "human_actor_required": "use_human_session",
}


def project_ska_contract_error(
    error: Exception,
    *,
    family: SkaContractFamily,
) -> dict[str, object]:
    """Return one canonical, bounded error envelope for REST and MCP."""

    if family not in _ALIASES:
        raise ValueError("ska_contract_family_invalid")
    raw_code = str(getattr(error, "code", "") or "").strip()
    canonical = _ALIASES[family].get(raw_code, raw_code)
    if canonical not in _SPECIFIC_CODES[family]:
        canonical = _category_code(error, raw_code=raw_code)
    retryable = _retryable(error, code=canonical)
    raw_details = getattr(error, "details", {})
    details = (
        dict(raw_details)
        if isinstance(raw_details, Mapping)
        else {}
    )
    if raw_code:
        details["reason_code"] = raw_code
    return {
        "outcome": "error",
        "error": canonical,
        "code": canonical,
        "error_code": canonical,
        "retryable": retryable,
        "next_action": _NEXT_ACTION[canonical],
        "details": details,
    }


def _category_code(error: Exception, *, raw_code: str) -> str:
    category = str(
        getattr(getattr(error, "category", None), "value", "")
        or getattr(error, "category", "")
    ).lower()
    class_name = type(error).__name__.lower()
    lowered = raw_code.lower()
    if (
        "permission" in class_name
        or "forbidden" in class_name
        or "access_denied" in lowered
        or "permission_denied" in lowered
    ):
        return "forbidden"
    if "notfound" in class_name or "not_found" in lowered or category == "not_found":
        return "not_found"
    if (
        category == "conflict"
        or "conflict" in class_name
        or "conflict" in lowered
    ):
        return "version_conflict"
    return "validation_failed"


def _retryable(error: Exception, *, code: str) -> bool:
    if code in {
        "checklist_binding_off",
        "checklist_incomplete",
        "resolved_evidence_required",
        "human_actor_required",
        "forbidden",
        "not_found",
        "validation_failed",
    }:
        return False
    if code == "version_conflict":
        return bool(getattr(error, "retryable", True))
    return False


__all__ = ["SkaContractFamily", "project_ska_contract_error"]
