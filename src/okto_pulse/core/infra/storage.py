"""Storage abstraction — provider pattern for file persistence.

R-P2-06A: the concrete ``FileSystemStorageProvider`` was extracted to the
Community edition (``community.adapters.storage.CommunityFileSystemStorage``,
wired via ``community_storage_provider`` in the composition root). The core keeps
ONLY the ``StorageProvider`` port + the fail-closed registry; a runtime
``StorageProvider`` MUST be injected via :func:`configure_storage`.

R02 (FR1/FR2/AC5/AC6): the port grows a read-side STREAMING contract so the core
can serve downloads through the provider WITHOUT ever touching a concrete
filesystem path — reproducing the HTTP contract the old ``FileResponse(path)``
produced (Content-Length / Last-Modified / ETag / Range) while never
materialising the whole file by default. ``stat`` + ``open_stream`` are
NON-abstract with byte-backed defaults (derived from ``load``) so every existing
provider keeps working unchanged; the Community filesystem adapter OVERRIDES them
with an offloaded, chunked implementation (AC6). The core adds NO local-filesystem
dependency here (TR4): the defaults only slice the bytes ``load`` already returns.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass

#: Default streaming chunk size — mirrors Starlette ``FileResponse.chunk_size`` so
#: a provider-backed download chunks identically to the FileResponse baseline.
DEFAULT_STREAM_CHUNK_SIZE = 64 * 1024


@dataclass(frozen=True)
class StorageObjectStat:
    """Read-side metadata the core needs to reproduce the download HTTP contract
    WITHOUT a concrete path.

    ``modified_time`` is the POSIX mtime (epoch seconds) when the backing store has
    a modification clock (a real filesystem); ``None`` when it does not — the core
    then omits ``Last-Modified``/``ETag`` (those headers do not exist in the
    baseline for such a provider, so omitting them is parity, not degradation).
    """

    size: int
    modified_time: float | None = None


class StorageProvider(ABC):
    """Abstract storage provider for file uploads."""

    @abstractmethod
    async def save(self, board_id: str, filename: str, content: bytes) -> str: ...

    @abstractmethod
    async def load(self, path: str) -> bytes: ...

    @abstractmethod
    async def delete(self, path: str) -> bool: ...

    async def stat(self, path: str) -> StorageObjectStat:
        """Return read-side metadata (size + optional mtime) for ``path``.

        Default: derive the size from :meth:`load` with NO modification clock. The
        Community filesystem adapter overrides this with an offloaded ``os.stat`` so
        the core can reproduce the baseline ``Last-Modified``/``ETag`` headers.
        MUST raise :class:`FileNotFoundError` when the object is absent (the core
        maps that to a controlled 404 — never a path leak).
        """
        data = await self.load(path)
        return StorageObjectStat(size=len(data))

    async def open_stream(
        self,
        path: str,
        *,
        start: int = 0,
        end: int | None = None,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        """Yield the ``[start:end)`` byte range of ``path`` in ``chunk_size`` chunks.

        Default: materialise via :meth:`load` and slice — acceptable for in-memory
        providers, where the bytes already live in memory. The Community filesystem
        adapter OVERRIDES this to read the file in offloaded chunks WITHOUT
        materialising the whole file on the request path (AC6). MUST raise
        :class:`FileNotFoundError` when the object is absent.
        """
        data = await self.load(path)
        view = data[start:end]
        for offset in range(0, len(view), chunk_size):
            yield view[offset : offset + chunk_size]


_RUNTIME_KEY = "infra.storage.provider"


def configure_storage(provider: StorageProvider) -> None:
    """Register the active StorageProvider at startup."""
    register_runtime_value(_RUNTIME_KEY, provider)


def get_storage_provider() -> StorageProvider:
    """Return the registered StorageProvider or raise."""
    from okto_pulse.core.composition import (
        current_runtime_composition,
    )

    composition = current_runtime_composition()
    if composition is not None and composition.storage_provider is not None:
        return composition.storage_provider
    provider = resolve_runtime_value(_RUNTIME_KEY)
    if provider is None:
        raise RuntimeError(
            "StorageProvider not configured. Call configure_storage() first."
        )
    return provider


def reset_storage_provider_for_tests() -> None:
    """Clear the active provider for fail-closed composition tests."""

    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "DEFAULT_STREAM_CHUNK_SIZE",
    "StorageObjectStat",
    "StorageProvider",
    "configure_storage",
    "get_storage_provider",
    "reset_storage_provider_for_tests",
]
