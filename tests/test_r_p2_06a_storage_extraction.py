"""R-P2-06A — storage extraction import audit + port contract.

The concrete ``FileSystemStorageProvider`` was extracted to the Community edition
(``community.adapters.storage.CommunityFileSystemStorage``). The core common
package keeps ONLY the ``StorageProvider`` port + the fail-closed registry
(``configure_storage`` / ``get_storage_provider``).

Covers spec R-P2-06A (FR fr_d9fa9521, TR tr_8b73acd8, AC ac_70ec0340, BR
br_0a6d5e05) and scenario ts_9a9f4b8d:
  - import audit: ``FileSystemStorageProvider`` is NOT importable from the core
    runtime and is not exported by the core package;
  - fail-closed: ``get_storage_provider`` without a configured provider raises;
  - an explicit in-memory fake exercises save/load/delete via the port (no
    concrete core provider involved).
"""

from __future__ import annotations

import asyncio

import pytest

from okto_pulse.core.infra import storage as storage_mod
from okto_pulse.core.infra.storage import (
    StorageProvider,
    configure_storage,
    get_storage_provider,
)


@pytest.fixture(autouse=True)
def _reset_storage_registry():
    saved = storage_mod._storage_provider
    storage_mod._storage_provider = None
    try:
        yield
    finally:
        storage_mod._storage_provider = saved


# --- import audit: no concrete filesystem provider in the core ----------------
def test_filesystem_storage_provider_not_importable_from_core():
    with pytest.raises(ImportError):
        from okto_pulse.core.infra.storage import (  # noqa: F401
            FileSystemStorageProvider,
        )


def test_core_package_does_not_export_filesystem_storage_provider():
    import okto_pulse.core as core

    assert "FileSystemStorageProvider" not in core.__all__
    assert not hasattr(core, "FileSystemStorageProvider")
    # the port + fail-closed registry stay.
    assert hasattr(core, "StorageProvider")
    assert hasattr(core, "configure_storage")
    assert hasattr(core, "get_storage_provider")


def test_storage_module_exposes_only_port_and_registry():
    names = {n for n in vars(storage_mod) if not n.startswith("_")}
    assert "FileSystemStorageProvider" not in names
    assert {"StorageProvider", "configure_storage", "get_storage_provider"} <= names


# --- fail-closed registry -----------------------------------------------------
def test_get_storage_provider_without_configure_raises():
    with pytest.raises(RuntimeError, match="StorageProvider not configured"):
        get_storage_provider()


# --- explicit fake exercises the port contract (roundtrip) --------------------
class _InMemoryStorage(StorageProvider):
    """Explicit test fake — proves the port works with NO concrete core provider."""

    def __init__(self) -> None:
        self._blobs: dict[str, bytes] = {}

    async def save(self, board_id: str, filename: str, content: bytes) -> str:
        path = f"{board_id}/{filename}"
        self._blobs[path] = content
        return path

    async def load(self, path: str) -> bytes:
        return self._blobs[path]

    async def delete(self, path: str) -> bool:
        return self._blobs.pop(path, None) is not None


def test_configured_fake_provider_roundtrip():
    configure_storage(_InMemoryStorage())
    provider = get_storage_provider()
    assert isinstance(provider, StorageProvider)

    async def _exercise() -> None:
        path = await provider.save("board-1", "file.txt", b"hello")
        assert await provider.load(path) == b"hello"
        assert await provider.delete(path) is True
        # idempotent delete -> False (same semantics as the extracted concrete).
        assert await provider.delete(path) is False

    asyncio.run(_exercise())
