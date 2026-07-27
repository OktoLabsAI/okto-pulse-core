"""Package version provider contracts."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version as metadata_version
from typing import Mapping, Protocol, runtime_checkable


@runtime_checkable
class PackageVersionProvider(Protocol):
    """Resolve a package version without reading source checkout files."""

    def version(self, package_name: str) -> str | None:
        """Return the package version, or ``None`` when unknown."""
        ...


@dataclass(frozen=True)
class ImportlibMetadataVersionProvider:
    """Resolve versions from installed package metadata."""

    def version(self, package_name: str) -> str | None:
        try:
            return metadata_version(package_name)
        except PackageNotFoundError:
            return None


@dataclass(frozen=True)
class MappingPackageVersionProvider:
    """Static package version provider useful for edition composition/tests."""

    versions: Mapping[str, str]

    def version(self, package_name: str) -> str | None:
        return self.versions.get(package_name)


__all__ = [
    "ImportlibMetadataVersionProvider",
    "MappingPackageVersionProvider",
    "PackageVersionProvider",
]
