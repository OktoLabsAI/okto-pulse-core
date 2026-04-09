"""Storage abstraction — provider pattern for file persistence."""

import secrets
from abc import ABC, abstractmethod
from pathlib import Path


class StorageProvider(ABC):
    """Abstract storage provider for file uploads."""

    @abstractmethod
    async def save(self, board_id: str, filename: str, content: bytes) -> str: ...

    @abstractmethod
    async def load(self, path: str) -> bytes: ...

    @abstractmethod
    async def delete(self, path: str) -> bool: ...


class FileSystemStorageProvider(StorageProvider):
    """Local filesystem storage provider."""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    async def save(self, board_id: str, filename: str, content: bytes) -> str:
        safe_name = Path(filename).name
        unique_name = f"{secrets.token_hex(8)}_{safe_name}"
        upload_dir = self.base_dir / board_id
        upload_dir.mkdir(parents=True, exist_ok=True)
        file_path = upload_dir / unique_name
        file_path.write_bytes(content)
        return str(file_path)

    async def load(self, path: str) -> bytes:
        return Path(path).read_bytes()

    async def delete(self, path: str) -> bool:
        try:
            Path(path).unlink()
            return True
        except FileNotFoundError:
            return False


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
