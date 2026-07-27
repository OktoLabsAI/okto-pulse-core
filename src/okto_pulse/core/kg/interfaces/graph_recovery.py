"""GraphRecovery port (spec KGD-01, FR3/BR2/D3).

Owns degrau 2 of the recovery ladder (salvage -> wal-only -> restore/operator):
quarantine ONLY the board graph's WAL + checkpoint sidecars and re-probe the
open, WITHOUT exposing concrete filesystem/runtime calls to consumers. Async:
the contract is the boundary; edition adapters run the underlying quarantine +
reopen probe. ``recover_wal_only`` returns a structured report so consumers
never inspect raw paths or handles.

BR2 (invariant): no automated recovery path may move, rename or delete the
main graph file — adapters MUST always report ``main_untouched=True``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class WalRecoveryReport:
    """Structured outcome of a wal-only recovery attempt (degrau 2)."""

    board_id: str
    status: str  # "recovered" | "skipped" | "failed"
    quarantine_id: str | None = None
    files_moved: tuple[str, ...] = ()
    # BR2: the main graph file is NEVER touched by any automated recovery
    # path — adapters MUST always report True here. The field exists so the
    # invariant is observable/auditable at the port boundary, not implied.
    main_untouched: bool = True
    reason: str | None = None


@runtime_checkable
class GraphRecovery(Protocol):
    async def recover_wal_only(self, board_id: str) -> WalRecoveryReport:
        """Quarantine ONLY the WAL + checkpoint sidecars, then re-probe open.

        Non-destructive by contract: the main graph file stays in place; the
        quarantined files are preserved with a manifest for operator-driven
        restore. Returns ``status="recovered"`` when the reopen probe
        succeeds, ``"skipped"`` when there was nothing to quarantine, and
        ``"failed"`` when the quarantine or the reopen probe failed.
        """
        ...
