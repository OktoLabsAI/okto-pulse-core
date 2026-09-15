"""Provider-neutral read seam for a complete delivery-evidence projection."""

from __future__ import annotations

from typing import Protocol

from okto_pulse.core.models.delivery_evidence import DeliveryEvidenceCommand

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryEvidenceSnapshot,
    DeliveryScope,
)


class DeliveryEvidenceReadPort(Protocol):
    async def load_snapshot(self, scope: DeliveryScope) -> DeliveryEvidenceSnapshot:
        """Load under the caller's authorized, transaction-scoped unit of work.

        Resolve active obligations and canonical semantic hashes on the server;
        project only current target-execution and test-run heads, authenticated
        through the existing Code Traceability and Test Evidence contracts.
        Attach tests ONLY through real test cards and their linked scenarios.
        Never accept completion booleans, actor IDs or waiver authority from a
        client as facts. Unavailable/truncated reads must not report complete.

        A current failure/revocation must supersede an older successful record.
        Waivers require active authorization receipts bound to the exact scope,
        phase and obligation digest. Loading must neither execute tests nor
        mutate existing artifacts, graph content or lifecycle state.
        """
        ...


class DeliveryEvidenceStore(DeliveryEvidenceReadPort, Protocol):
    async def lock_scope(self, scope: DeliveryScope) -> None: ...

    async def projection(self, board_id: str, spec_id: str) -> dict: ...

    async def record(
        self, command: DeliveryEvidenceCommand, *, actor_id: str, actor_kind: str
    ) -> dict:
        """Validate receipts and current scope under the terminal-transition fence.

        Append immutable bindings/audit or revocation tombstones, with actor-scoped
        idempotency. Never commits the caller's transaction.
        """
        ...
