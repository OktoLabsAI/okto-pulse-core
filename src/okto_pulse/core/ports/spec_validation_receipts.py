"""Transaction-bound exact replay for the Spec planning conclusion.

This is a technical receipt, not an additional assessment history. The stored
result only contains native result references and the operation's conclusion.
"""
from dataclasses import dataclass
from typing import Protocol

from okto_pulse.core.domain.spec_validation import SpecValidationConflictError
from okto_pulse.core.models.schemas import SpecValidationConclusionResponse


class SpecValidationIdempotencyConflict(SpecValidationConflictError):
    code = "spec_validation_idempotency_conflict"


@dataclass(frozen=True, slots=True)
class SpecValidationReceiptScope:
    board_id: str
    spec_id: str
    actor_id: str
    actor_kind: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class SpecValidationOperationReceipt:
    scope: SpecValidationReceiptScope
    request_digest: str
    result: SpecValidationConclusionResponse


class SpecValidationReceiptStore(Protocol):
    """Bound to the caller's UoW, realm and write transaction; never commits."""

    async def get(
        self, scope: SpecValidationReceiptScope,
    ) -> SpecValidationOperationReceipt | None: ...

    async def save(self, receipt: SpecValidationOperationReceipt) -> None: ...
