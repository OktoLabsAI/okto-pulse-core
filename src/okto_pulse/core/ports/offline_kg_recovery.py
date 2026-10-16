"""Public composition boundary for an offline, exact consolidation reservation.

The installer must first authenticate its manifest, backup, build pair and private
generation and exclude ordinary writers. These scopes expose the existing Core
recovery authority and queue policy; they do not authorize a public repair API.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Protocol

from .consolidation import ConsolidationClaimScope, ExactConsolidationBatchResult
from .coordination import WriteLockPort


class OfflineConsolidationReservation(Protocol):
    @property
    def claim_scope(self) -> ConsolidationClaimScope: ...

    def is_authorized(self) -> bool:
        """Prove the live recovery scope AND exact administrative lease token."""
        ...

    async def process_next(self) -> ExactConsolidationBatchResult:
        """Renew and run one batch; retain durable ACK even after authority loss.

        Subsequent batches/context exit fail closed on lost authority. An error
        never implies that earlier committed effects were rolled back.
        """
        ...


def issue_offline_recovery_capability(*, board_id: str, lifetime_probe: Callable[[], bool]) -> AbstractContextManager[object]:
    """Context manager over Core's sealed, one-operation recovery authority.

    Only trusted offline composition supplies this live external-lease probe.
    A serialized token, prior receipt or client boolean is not such authority.
    """
    from okto_pulse.core.kg.recovery_execution import issue_recovery_execution_capability
    return issue_recovery_execution_capability(board_id=board_id, lifetime_probe=lifetime_probe)


def reserve_offline_consolidation(*, claim_scope: ConsolidationClaimScope, recovery_capability: object,
        write_lock_port: WriteLockPort, relational_scope_factory: Callable, owner_id: str,
        ttl_seconds: int = 300) -> AbstractContextManager[OfflineConsolidationReservation]:
    """Hold an exact reservation without exposing Core's lock/worker internals.

    The caller owns physical source/generation fencing. This context neither
    enqueues work nor purges routes, promotes graphs, or certifies convergence.
    """
    from okto_pulse.core.application.offline_kg_recovery import reserve_consolidation
    return reserve_consolidation(claim_scope=claim_scope, recovery_capability=recovery_capability,
        write_lock_port=write_lock_port, relational_scope_factory=relational_scope_factory,
        owner_id=owner_id, ttl_seconds=ttl_seconds)
