"""Provider-neutral read seam for a complete delivery-evidence projection."""

from __future__ import annotations

from typing import Protocol
from okto_pulse.core.models.code_traceability import ImplementationTargetExecutionSubmission


from okto_pulse.core.models.delivery_evidence import (
    CardDeliveryEvidenceWriteCommand,
    DeliveryEvidenceCommand,
)

from okto_pulse.core.domain.delivery_evidence import (
    CardDeliveryScope,
    DeliveryEvidenceSnapshot,
    DeliveryScope,
)


class DeliveryExecutionSubmitter(Protocol):
    async def __call__(self, submission: ImplementationTargetExecutionSubmission) -> str:
        """Admit through the origin service and stage its event; never commit.

        The returned ID belongs to the authenticated caller and same UoW.
        The Delivery adapter must include receipt and event in its savepoint.
        """
        ...


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
        """Record human exceptions under the terminal-transition fence.

        Append waivers or revocation tombstones with actor-scoped idempotency.
        Implementation/test writes must use the card-scoped port; obsolete
        spec-scoped proof requests fail with delivery_card_scope_required.
        Historical records remain readable. Never commits the caller's transaction.
        """
        ...


class CardDeliveryEvidenceStore(Protocol):
    """Card-scoped delivery seam: the task owns its bindings (FR-1/FR-2).

    The adapter reuses the authenticated fact validators of the spec ledger
    untouched; only the addressing scope and the CAS fence (card version)
    change. Waivers stay on the spec rollup surface and are human-only.
    """

    async def load_card_snapshot(self, scope: CardDeliveryScope) -> DeliveryEvidenceSnapshot:
        """Load the per-card projection under the caller's authorized unit of work.

        Obligations derive deterministically from the card's links in the
        spec collections (same refs/digests as the spec inventory); a card
        without links carries exactly the card:<id> fallback obligation.
        Facts project only current, authenticated target executions and test
        runs through the existing Code Traceability and Test Evidence
        contracts. Unavailable/truncated reads must not report complete.
        """
        ...

    async def record_card(
        self, command: CardDeliveryEvidenceWriteCommand, *, actor_id: str, actor_kind: str,
        execution_submitter: DeliveryExecutionSubmitter | None = None,
    ) -> dict:
        """Validate the card-scoped candidate under the card-version fence.

        Append immutable bindings or revocation tombstones to the card
        ledger, with actor-scoped idempotency; revocations are human-only.
        Validate before insert — a rejection never persists a partial
        binding. Never commits the caller's transaction.
        A batch is one Card/Spec/edition and one actor. Fence the delivery
        revision, authorize every kind in the use case, then save all entries
        or none. Exact envelope replay returns the same client_ref/record IDs.
        No waiver/revoke entry, lifecycle change or implicit proof promotion.
        """
        ...
