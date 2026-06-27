"""Storage abstraction — provider pattern for file persistence.

R-P2-06A: the concrete ``FileSystemStorageProvider`` was extracted to the
Community edition (``community.adapters.storage.CommunityFileSystemStorage``,
wired via ``community_storage_provider`` in the composition root). The core keeps
ONLY the ``StorageProvider`` port + the fail-closed registry; a runtime
``StorageProvider`` MUST be injected via :func:`configure_storage`.
"""

from abc import ABC, abstractmethod


class StorageProvider(ABC):
    """Abstract storage provider for file uploads."""

    @abstractmethod
    async def save(self, board_id: str, filename: str, content: bytes) -> str: ...

    @abstractmethod
    async def load(self, path: str) -> bytes: ...

    @abstractmethod
    async def delete(self, path: str) -> bool: ...


_storage_provider: StorageProvider | None = None


def configure_storage(provider: StorageProvider) -> None:
    """Register the active StorageProvider at startup."""
    global _storage_provider
    _storage_provider = provider


def get_storage_provider() -> StorageProvider:
    """Return the registered StorageProvider or raise."""
    if _storage_provider is None:
        raise RuntimeError("StorageProvider not configured. Call configure_storage() first.")
    return _storage_provider
