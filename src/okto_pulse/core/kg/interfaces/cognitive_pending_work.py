"""Port for discovering durable cognitive pending ledger records.

Core owns the policy deciding which pending records are drainable. Runtime
editions own how ledger records are discovered from durable storage.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class CognitivePendingRecordRef:
    """Logical reference to one cognitive pending generation ledger."""

    board_id: str
    kg_generation_id: str


class CognitivePendingWorkProvider(Protocol):
    """Edition-provided discovery over cognitive pending ledger records."""

    def list_records(self) -> Sequence[CognitivePendingRecordRef]:
        ...


__all__ = [
    "CognitivePendingRecordRef",
    "CognitivePendingWorkProvider",
]
