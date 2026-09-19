"""Deterministic, whole-contract draft. No inferred participant roles or HTTP verb."""

import copy
from typing import Any, Mapping


def architecture_promotion_suggestion(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Offer only source-backed fields; the author must accept/edit before writing.

    This proposal covers the whole contract. Partial adoption requires authored
    IR content for the selected scopes; the classification witness independently
    preserves all source clauses, including those unsupported by the IR model.
    """
    proposed: dict[str, Any] = {}
    for source, target in (
        ("name", "title"),
        ("description", "description"),
        ("endpoint", "endpoint"),
        ("schema_ref", "contract_ref"),
        ("notes", "notes"),
    ):
        value = contract.get(source)
        if isinstance(value, str) and value.strip():
            proposed[target] = value
    kind = contract.get("contract_type")
    if isinstance(kind, str):
        # Explicit structured discriminators only; no classification from names,
        # participant order, endpoint shape, schema content or protocol guesses.
        kind = kind.strip().lower()
        if kind == "http":
            proposed["integration_type"] = "api"
        elif kind in {
            "api",
            "queue",
            "stored_procedure",
            "data_contract",
            "event",
            "file",
            "other",
        }:
            proposed["integration_type"] = kind
    declared = {
        field: copy.deepcopy(contract[field])
        for field in (
            "request_schema",
            "response_schema",
            "event_schema",
            "error_contract",
            "direction",
            "protocol",
            "participants",
        )
        if contract.get(field) is not None
    }
    if declared:
        proposed["data_contract"] = declared
    return {
        "scope_paths": [""],
        "proposed_ir": proposed,
        "requires_author_review": True,
        "missing_required_fields": [
            field for field in ("title", "integration_type") if field not in proposed
        ],
    }
