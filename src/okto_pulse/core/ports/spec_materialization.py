"""Persistence port for legacy structured-spec materialization."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from okto_pulse.core.domain.spec_materialization import SpecMaterializationPlan


class SpecMaterializationStore(Protocol):
    async def list_specs(self, board_id: str) -> Sequence[object]: ...

    async def apply(self, plan: SpecMaterializationPlan) -> None: ...


__all__ = ["SpecMaterializationStore"]
