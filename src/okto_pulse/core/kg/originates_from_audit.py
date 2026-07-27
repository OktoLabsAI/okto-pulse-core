"""Read-only advisory audit for persisted ``originates_from`` KG edges.

KGH-1 deliberately observes historical graph state only. The write-path schema
validator remains the authority for new edges; this module reports persisted
drift and unknown endpoint metadata without deleting, rebuilding or reprocessing
anything.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Sequence

from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.schema_contract import relationship_endpoint_pairs

RELATIONSHIP_TYPE = "originates_from"
CONTRACT = "Bug->Entity"
CLASSIFICATION_INVALID = "invalid_endpoint_types"
CLASSIFICATION_UNKNOWN = "unknown_endpoint_type"
CLASSIFICATION_OK = "ok"


@dataclass(frozen=True, slots=True)
class OriginatesFromAuditItem:
    relationship_id: str
    relationship_type: str
    source_id: str
    target_id: str
    source_type: str | None
    target_type: str | None
    contract: str
    classification: str
    confidence: str
    reason: str
    path: str
    mutated: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _row_value(row: Any, index: int, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        return row[index] if len(row) > index else None
    return getattr(row, key, None)


def _clean_type(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "unknown"}:
        return None
    return text


def _clean_id(value: Any) -> str:
    return "" if value is None else str(value)


def _relationship_id(rule_id: Any, source_id: str, target_id: str) -> str:
    text = "" if rule_id is None else str(rule_id).strip()
    if text:
        return text
    return f"{RELATIONSHIP_TYPE}:{source_id}->{target_id}"


def _path(source_type: str | None, source_id: str, target_type: str | None, target_id: str) -> str:
    return (
        f"{source_type or '?'}({source_id})-"
        f"[{RELATIONSHIP_TYPE}]->{target_type or '?'}({target_id})"
    )


def classify_originates_from_row(row: Any) -> OriginatesFromAuditItem:
    rule_id = _row_value(row, 0, "relationship_id")
    source_id = _clean_id(_row_value(row, 2, "source_id"))
    target_id = _clean_id(_row_value(row, 3, "target_id"))
    source_type = _clean_type(_row_value(row, 4, "source_type"))
    target_type = _clean_type(_row_value(row, 5, "target_type"))
    relationship_id = _relationship_id(rule_id, source_id, target_id)
    path = _path(source_type, source_id, target_type, target_id)

    if source_type is None or target_type is None:
        return OriginatesFromAuditItem(
            relationship_id=relationship_id,
            relationship_type=RELATIONSHIP_TYPE,
            source_id=source_id,
            target_id=target_id,
            source_type=source_type,
            target_type=target_type,
            contract=CONTRACT,
            classification=CLASSIFICATION_UNKNOWN,
            confidence="low",
            reason="unknown_endpoint_type",
            path=path,
        )

    allowed_pairs = set(relationship_endpoint_pairs(RELATIONSHIP_TYPE))
    if (source_type, target_type) not in allowed_pairs:
        expected = ", ".join(f"{src}->{dst}" for src, dst in sorted(allowed_pairs))
        return OriginatesFromAuditItem(
            relationship_id=relationship_id,
            relationship_type=RELATIONSHIP_TYPE,
            source_id=source_id,
            target_id=target_id,
            source_type=source_type,
            target_type=target_type,
            contract=CONTRACT,
            classification=CLASSIFICATION_INVALID,
            confidence="high",
            reason=f"invalid_edge_endpoint_types: expected {expected}",
            path=path,
        )

    return OriginatesFromAuditItem(
        relationship_id=relationship_id,
        relationship_type=RELATIONSHIP_TYPE,
        source_id=source_id,
        target_id=target_id,
        source_type=source_type,
        target_type=target_type,
        contract=CONTRACT,
        classification=CLASSIFICATION_OK,
        confidence="high",
        reason="schema_contract_satisfied",
        path=path,
    )


def audit_originates_from_contract(
    board_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
    include_ok: bool = False,
    cypher_executor: Any | None = None,
) -> dict[str, Any]:
    """Return an advisory report for persisted ``originates_from`` drift.

    The only graph operation is ``CypherExecutor.execute_read_only``. A missing
    or unsupported executor degrades to an advisory ``unavailable`` report.
    """

    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    executor = cypher_executor or get_kg_registry().cypher_executor
    if executor is None or not executor.is_supported():
        return {
            "board_id": board_id,
            "relationship_type": RELATIONSHIP_TYPE,
            "contract": CONTRACT,
            "status": "unavailable",
            "items": [],
            "counts": {
                CLASSIFICATION_INVALID: 0,
                CLASSIFICATION_UNKNOWN: 0,
                CLASSIFICATION_OK: 0,
            },
            "total": 0,
            "scanned": 0,
            "limit": bounded_limit,
            "offset": bounded_offset,
            "read_only": True,
            "mutated": False,
            "reason": "cypher_executor_unavailable",
        }

    result = executor.execute_read_only(
        board_id,
        "MATCH (a)-[r:originates_from]->(b) "
        "RETURN r.rule_id, r.created_by, a.id, b.id, label(a), label(b), r.layer",
        max_rows=max(10_000, bounded_limit + bounded_offset),
    )
    rows = list(result.get("rows", []) or [])
    classified = [classify_originates_from_row(row) for row in rows]
    counts = {
        CLASSIFICATION_INVALID: 0,
        CLASSIFICATION_UNKNOWN: 0,
        CLASSIFICATION_OK: 0,
    }
    for item in classified:
        counts[item.classification] = counts.get(item.classification, 0) + 1

    visible = [
        item for item in classified
        if include_ok or item.classification != CLASSIFICATION_OK
    ]
    page = visible[bounded_offset:bounded_offset + bounded_limit]
    return {
        "board_id": board_id,
        "relationship_type": RELATIONSHIP_TYPE,
        "contract": CONTRACT,
        "status": "ok" if not visible else "advisory_findings",
        "items": [item.as_dict() for item in page],
        "counts": counts,
        "total": len(visible),
        "scanned": len(rows),
        "limit": bounded_limit,
        "offset": bounded_offset,
        "read_only": True,
        "mutated": False,
        "truncated": bool(result.get("truncated")),
    }


__all__ = [
    "CLASSIFICATION_INVALID",
    "CLASSIFICATION_OK",
    "CLASSIFICATION_UNKNOWN",
    "CONTRACT",
    "RELATIONSHIP_TYPE",
    "OriginatesFromAuditItem",
    "audit_originates_from_contract",
    "classify_originates_from_row",
]
