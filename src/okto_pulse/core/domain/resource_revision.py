"""Pure revision identity shared by resource-lineage consumers.

The canonical ResourceLineage v2 service re-exports this exact class from its
historical public module.  Keeping the value object in a domain leaf lets other
pure contracts carry lineage evidence without importing the legacy services
layer or defining a competing root/version/hash identity.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResourceRevisionStamp:
    """Storage-neutral revision evidence for one physical resource snapshot."""

    root_id: str
    immediate_parent_id: str | None = None
    source_revision: str | None = None
    source_content_sha256: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "root_id": self.root_id,
            "immediate_parent_id": self.immediate_parent_id,
            "source_revision": self.source_revision,
            "source_content_sha256": self.source_content_sha256,
        }


__all__ = ["ResourceRevisionStamp"]
