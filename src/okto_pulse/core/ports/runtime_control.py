"""RuntimeControl runtime port (spec #15, tr_6beb09c5 / api_70898f1b).

Startup/shutdown + provider access over a RuntimeComposition. The canonical
``RuntimeComposition`` contract is owned by spec #03; until #03 is implemented,
``RuntimeCompositionLike`` is a local Protocol alias with the minimal surface
RuntimeControl consumes. RuntimeControl does NOT own the composition root — it
consumes #03's composition when available.

Pure: Protocols only.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class RuntimeCompositionLike(Protocol):
    """Minimal composition surface consumed by RuntimeControl.

    Local alias for spec #03's ``RuntimeComposition`` (contract
    ``/internal/contracts/runtime/runtime-composition``). When #03 lands, its
    canonical contract structurally satisfies this Protocol.
    """

    def get_provider(self, key: str) -> Any:
        ...


@runtime_checkable
class RuntimeControl(Protocol):
    """Drives runtime startup/shutdown and exposes registered providers."""

    async def startup(self, composition: RuntimeCompositionLike) -> None:
        ...

    async def shutdown(self) -> None:
        ...

    def get_provider(self, key: str) -> Any:
        """Return the provider registered under ``key`` (runtime_provider_missing otherwise)."""
        ...
