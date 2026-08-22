"""Authorization policy for Code Traceability materialized in the KG.

The Code Traceability projection reuses the generic ``Entity`` node table, so
generic KG readers cannot authorize it from the physical label alone.  This
module owns the closed semantic subtype vocabulary and the all-of read
permission contract shared by REST and MCP use cases.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any


CODE_TRACEABILITY_KG_SUBTYPES: tuple[str, ...] = (
    "code_investigation_receipt",
    "code_evidence",
    "implementation_target",
)

CODE_TRACEABILITY_KG_READ_PERMISSIONS: tuple[str, ...] = (
    "code_traceability.investigation.read",
    "code_traceability.evidence.read",
    "code_traceability.target.read",
    "code_traceability.overlap.read",
)

# These names mirror the additive Code Traceability columns on ``Entity``.
# They live in the domain policy (rather than importing the physical KG schema)
# so every generic write surface can reserve the same closed vocabulary before
# it reaches an edition-owned graph provider.
CODE_TRACEABILITY_KG_WRITE_FIELDS: tuple[str, ...] = (
    "investigation_receipt_id",
    "source_ref",
    "attestor_actor_id",
    "declared_revision",
    "workspace_state_id",
    "code_path",
    "symbol_qualified_name",
    "symbol_kind",
    "selector_kind",
    "selector_fingerprint",
    "resolution_state",
)

CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH = "deterministic_worker"
CODE_TRACEABILITY_OPERATIONAL_REMEDIATION_CODE = (
    "code_traceability_deterministic_rebuild_required"
)


class CodeInvestigationReceiptKGStatus(str, Enum):
    """Closed lifecycle vocabulary emitted by relational CT projections.

    A receipt remains domain-accepted and immutable.  ``conflicted`` and
    ``revoked`` describe its current projection/maturity state in the KG; they
    are not additional acceptance outcomes.
    """

    ACCEPTED = "accepted"
    CONFLICTED = "conflicted"
    REVOKED = "revoked"


CODE_INVESTIGATION_RECEIPT_KG_STATUSES = frozenset(
    item.value for item in CodeInvestigationReceiptKGStatus
)


class KGDeadLetterReprocessScope(str, Enum):
    """Closed operational selection scopes for deterministic KG replay."""

    GENERIC = "generic"
    CODE_TRACEABILITY = "code_traceability"


class CodeTraceabilityKGWriteViolation(ValueError):
    """A generic/cognitive writer attempted to forge a CT projection."""

    def __init__(
        self,
        reason: str,
        *,
        candidate_id: str = "",
        reserved_fields: tuple[str, ...] = (),
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.candidate_id = candidate_id
        self.reserved_fields = reserved_fields


def _candidate_value(candidate: Any, name: str) -> Any:
    if isinstance(candidate, Mapping):
        return candidate.get(name)
    return getattr(candidate, name, None)


def normalize_code_traceability_subtype(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def is_code_traceability_subtype(value: Any) -> bool:
    return normalize_code_traceability_subtype(value) in frozenset(
        CODE_TRACEABILITY_KG_SUBTYPES
    )


def is_code_traceability_artifact_type(value: Any) -> bool:
    """CT queue artifacts use the same closed names as their KG subtypes."""

    return is_code_traceability_subtype(value)


def require_code_traceability_candidate_writer(
    candidate: Any,
    *,
    writer_path: str,
) -> None:
    """Reserve CT subtypes and metadata to the internal deterministic worker.

    ``writer_path`` is derived by the Core primitive from its authenticated
    runtime principal; it is deliberately absent from the public candidate
    payload, so a client cannot opt itself into the privileged path.
    """

    kind_of = _candidate_value(candidate, "kind_of")
    reserved_fields = tuple(
        field
        for field in CODE_TRACEABILITY_KG_WRITE_FIELDS
        if _candidate_value(candidate, field) is not None
    )
    declares_ct = is_code_traceability_subtype(kind_of)
    if not declares_ct and not reserved_fields:
        return

    candidate_id = str(_candidate_value(candidate, "candidate_id") or "")
    if writer_path != CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH:
        raise CodeTraceabilityKGWriteViolation(
            "Code Traceability KG projections are reserved to the deterministic worker",
            candidate_id=candidate_id,
            reserved_fields=reserved_fields,
        )

    node_type = _candidate_value(candidate, "node_type")
    node_type = getattr(node_type, "value", node_type)
    if str(node_type or "") != "Entity" or not declares_ct:
        raise CodeTraceabilityKGWriteViolation(
            "Code Traceability metadata requires a declared Entity CT subtype",
            candidate_id=candidate_id,
            reserved_fields=reserved_fields,
        )


@dataclass(frozen=True, slots=True)
class CodeTraceabilityKGReadDecision:
    """All-of decision used to shape generic KG reads."""

    missing_permissions: tuple[str, ...]
    authority_resolved: bool = True

    @property
    def allowed(self) -> bool:
        return self.authority_resolved and not self.missing_permissions


def code_traceability_kg_read_decision(
    granted_permissions: Iterable[str],
    *,
    authority_resolved: bool = True,
) -> CodeTraceabilityKGReadDecision:
    """Return the closed all-of visibility decision for generic KG surfaces."""

    granted = frozenset(granted_permissions)
    missing = tuple(
        permission
        for permission in CODE_TRACEABILITY_KG_READ_PERMISSIONS
        if permission not in granted
    )
    return CodeTraceabilityKGReadDecision(
        missing_permissions=missing,
        authority_resolved=authority_resolved,
    )


__all__ = [
    "CODE_INVESTIGATION_RECEIPT_KG_STATUSES",
    "CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH",
    "CODE_TRACEABILITY_KG_READ_PERMISSIONS",
    "CODE_TRACEABILITY_KG_SUBTYPES",
    "CODE_TRACEABILITY_KG_WRITE_FIELDS",
    "CODE_TRACEABILITY_OPERATIONAL_REMEDIATION_CODE",
    "CodeInvestigationReceiptKGStatus",
    "CodeTraceabilityKGWriteViolation",
    "CodeTraceabilityKGReadDecision",
    "KGDeadLetterReprocessScope",
    "code_traceability_kg_read_decision",
    "is_code_traceability_artifact_type",
    "is_code_traceability_subtype",
    "normalize_code_traceability_subtype",
    "require_code_traceability_candidate_writer",
]
