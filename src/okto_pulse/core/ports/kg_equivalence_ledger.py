"""EquivalenceLedger port (spec MKG-C-S1, contract api_33eee4f3).

Off-graph, append-only ledger of node-equivalence decisions (merges).
Before this port, ``kg dedup-entities`` re-pointed every edge and
physically ``DETACH DELETE``d the duplicates — a wrong merge had no
un-merge, and the bulk edge re-point is exactly the in-place mutation
class that corrupted the same engine elsewhere (marginalia ADR 0007;
KGD-01 history).

Contract (spec BR1/BR3, D1):
  * every merge APPENDS a record carrying the COMPLETE pre-operation
    snapshot (member node attrs + every incident edge with every
    property) BEFORE the first graph write — fail-closed: the graph is
    never mutated without its reversal evidence;
  * records are never UPDATEd destructively nor DELETEd — un-merge is
    ``revoke`` (stamps ``revoked_at``/``revoke_reason``, preserves the
    record for audit);
  * ``active_for_board`` feeds the query-time equivalence fold (FR6) and
    excludes revoked records.

Pure: stdlib ``dataclasses`` / ``typing`` only. No SQLAlchemy, engines or
``okto_pulse.community`` imports — the concrete Community adapter
(``sqlalchemy_kg_equivalence_ledger``) owns those.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable

__all__ = [
    "EquivalenceLedgerError",
    "EquivalenceRecord",
    "EquivalenceLedger",
    "register_equivalence_ledger",
    "require_equivalence_ledger",
    "reset_equivalence_ledger_for_tests",
    "resolve_equivalence_ledger",
]


class EquivalenceLedgerError(Exception):
    """Structured, fail-closed equivalence-ledger failure.

    Surfaced by curation operations as the stable error code
    ``kg_equivalence_ledger_unavailable``: the operation MUST abort —
    a merge may never mutate the graph without its ledger record (BR1).
    """

    def __init__(
        self,
        failure_reason: str,
        *,
        board_id: str | None = None,
        record_id: str | None = None,
        remediation: str | None = None,
    ) -> None:
        self.failure_reason = failure_reason
        self.board_id = board_id
        self.record_id = record_id
        self.remediation = remediation
        detail = " ".join(
            part
            for part in (
                f"board_id={board_id}" if board_id else "",
                f"record_id={record_id}" if record_id else "",
            )
            if part
        )
        super().__init__(f"{failure_reason}{(' [' + detail + ']') if detail else ''}")


@dataclass(frozen=True)
class EquivalenceRecord:
    """One equivalence decision: ``merged_ids`` fold into ``survivor_id``.

    ``evidence`` carries the COMPLETE pre-operation snapshot (node attrs +
    every incident edge with every property — spec R1/R2: the current
    repoint loses edges silently and copies only 5 hardcoded props).
    ``revoked_at`` non-null means the equivalence is no longer active
    (un-merge) — the record itself is preserved for audit.
    """

    record_id: str
    board_id: str
    node_type: str
    survivor_id: str
    merged_ids: tuple[str, ...]
    operation: str
    evidence: Mapping[str, Any] = field(default_factory=dict)
    created_by: str | None = None
    created_at: str | None = None
    revoked_at: str | None = None
    revoke_reason: str | None = None

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None


@runtime_checkable
class EquivalenceLedger(Protocol):
    """Append-only ledger of node-equivalence decisions."""

    async def append(self, record: EquivalenceRecord) -> str:
        """Persist ``record`` and return its record_id.

        MUST be idempotent per ``record_id``. Raises
        :class:`EquivalenceLedgerError` when the ledger is unavailable —
        callers abort the curation operation (fail-closed, BR1).
        """
        ...

    async def revoke(self, record_id: str, reason: str) -> EquivalenceRecord:
        """Mark ``record_id`` revoked (un-merge) and return the updated
        record. Revoking an already-revoked record is an idempotent no-op
        that returns the record unchanged. Raises
        :class:`EquivalenceLedgerError` for an unknown record_id.
        """
        ...

    async def get(self, record_id: str) -> EquivalenceRecord | None:
        """Return the record, or ``None`` when absent."""
        ...

    async def active_for_board(self, board_id: str) -> tuple[EquivalenceRecord, ...]:
        """Return every ACTIVE (non-revoked) record for ``board_id`` in a
        deterministic order (created_at, record_id) — feeds the fold cache.
        """
        ...


_equivalence_ledger: EquivalenceLedger | None = None


def register_equivalence_ledger(ledger: EquivalenceLedger) -> None:
    """Register the edition-owned adapter (called by community wiring)."""

    global _equivalence_ledger
    _equivalence_ledger = ledger


def resolve_equivalence_ledger() -> EquivalenceLedger | None:
    """Return the registered ledger, or ``None`` when absent (probe only)."""

    return _equivalence_ledger


def require_equivalence_ledger() -> EquivalenceLedger:
    """Fail-closed resolver: a missing ledger NEVER degrades to a no-op.

    Raising here (instead of skipping the ledger write) is what keeps a
    merge from mutating the graph without reversal evidence (BR1/D1).
    """

    if _equivalence_ledger is None:
        raise EquivalenceLedgerError(
            "kg_equivalence_ledger_unavailable",
            remediation=(
                "Register an EquivalenceLedger adapter (community: "
                "sqlalchemy_kg_equivalence_ledger) via "
                "register_equivalence_ledger() before running curation "
                "merges."
            ),
        )
    return _equivalence_ledger


def reset_equivalence_ledger_for_tests() -> None:
    """Test-only: clear the registered ledger."""

    global _equivalence_ledger
    _equivalence_ledger = None
