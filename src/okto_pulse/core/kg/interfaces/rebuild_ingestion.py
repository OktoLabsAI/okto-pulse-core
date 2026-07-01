"""Pure rebuild-ingestion port consumed by KG rebuild orchestration."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from okto_pulse.core.kg.rebuild_service import RebuildStepAdapter

RebuildSourceResolver = Callable[[Any], Sequence[Mapping[str, Any]]]


class RebuildIngestionPort(Protocol):
    """Factory for the synchronous rebuild step adapter.

    Concrete adapters own their storage/queue implementation outside core.
    Core consumers only resolve this port and pass the manifest-bound source
    resolver used by ``KGRebuildService``.
    """

    def build_step_adapter(
        self,
        source_resolver: RebuildSourceResolver,
    ) -> RebuildStepAdapter:
        ...


RebuildStepAdapterFactory = RebuildIngestionPort


__all__ = [
    "RebuildIngestionPort",
    "RebuildSourceResolver",
    "RebuildStepAdapterFactory",
]
