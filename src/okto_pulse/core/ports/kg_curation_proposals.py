"""CurationProposalStore port (spec MKG-C-S1 — FR7, D4).

Minimal persisted review lane for curation operations: a proposal carries
the canonical PLAN of a dedup run plus its deterministic ``proposal_hash``
(sha256 of the canonically serialized plan — same contract as the
rebuild's ``preflight_hash``). Approval recomputes the current plan and
compares hashes BEFORE any write: a divergence is ``stale_proposal`` and
mutates nothing (BR5).

Pure: stdlib only. The concrete Community adapter
(``sqlalchemy_kg_curation_proposals``) owns SQLAlchemy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable

__all__ = [
    "CurationProposal",
    "CurationProposalError",
    "CurationProposalStore",
    "register_curation_proposal_store",
    "require_curation_proposal_store",
    "reset_curation_proposal_store_for_tests",
    "resolve_curation_proposal_store",
]


class CurationProposalError(Exception):
    """Structured proposal-store failure (fail-closed on the write lane)."""

    def __init__(
        self,
        failure_reason: str,
        *,
        proposal_id: str | None = None,
        remediation: str | None = None,
    ) -> None:
        self.failure_reason = failure_reason
        self.proposal_id = proposal_id
        self.remediation = remediation
        super().__init__(
            f"{failure_reason}"
            f"{(' [proposal_id=' + proposal_id + ']') if proposal_id else ''}"
        )


@dataclass(frozen=True)
class CurationProposal:
    """One persisted curation proposal (pending until resolved)."""

    proposal_id: str
    board_id: str
    operation: str
    plan: Mapping[str, Any]
    proposal_hash: str
    created_by: str | None = None
    created_at: str | None = None
    status: str = "pending"
    resolved_at: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class CurationProposalStore(Protocol):
    async def append(self, proposal: CurationProposal) -> str:
        """Persist the proposal (idempotent per proposal_id)."""
        ...

    async def get(self, proposal_id: str) -> CurationProposal | None:
        ...

    async def resolve(self, proposal_id: str, status: str) -> CurationProposal:
        """Mark the proposal resolved/stale. Raises for unknown ids."""
        ...

    async def pending_for_board(self, board_id: str) -> tuple[CurationProposal, ...]:
        ...


_curation_proposal_store: CurationProposalStore | None = None


def register_curation_proposal_store(store: CurationProposalStore) -> None:
    global _curation_proposal_store
    _curation_proposal_store = store


def resolve_curation_proposal_store() -> CurationProposalStore | None:
    return _curation_proposal_store


def require_curation_proposal_store() -> CurationProposalStore:
    if _curation_proposal_store is None:
        raise CurationProposalError(
            "kg_curation_proposal_store_unavailable",
            remediation=(
                "Register a CurationProposalStore adapter (community: "
                "sqlalchemy_kg_curation_proposals) before using the "
                "propose/approve lane."
            ),
        )
    return _curation_proposal_store


def reset_curation_proposal_store_for_tests() -> None:
    global _curation_proposal_store
    _curation_proposal_store = None
