"""Storage-neutral Knowledge Base content fingerprints.

The relational editions may persist the digest, but Core owns the canonical
bytes used to calculate it. Historical records are allowed to omit the stored
hash; callers can resolve one lazily from the same semantic fields.

This pure policy module lives in ``core.domain`` so application writers can
import it without initializing the aggregate ``core.services`` package.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


KNOWLEDGE_CONTENT_HASH_FIELDS: tuple[str, ...] = (
    "id",
    "title",
    "description",
    "content",
    "mime_type",
)


def _value(item: Any, field: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(field)
    return getattr(item, field, None)


def knowledge_content_sha256(item: Any) -> str:
    """Return SHA-256 over the canonical Knowledge Base content envelope."""

    payload = {field: _value(item, field) for field in KNOWLEDGE_CONTENT_HASH_FIELDS}
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_knowledge_content_sha256(item: Any) -> str:
    """Compatibility alias with an explicit computation verb."""

    return knowledge_content_sha256(item)


def resolve_knowledge_content_sha256(item: Any) -> str:
    """Prefer persisted canonical aliases and lazily hash legacy records."""

    for field in ("source_content_sha256", "content_hash"):
        persisted = _value(item, field)
        if persisted not in (None, ""):
            return str(persisted)
    return knowledge_content_sha256(item)


__all__ = [
    "KNOWLEDGE_CONTENT_HASH_FIELDS",
    "compute_knowledge_content_sha256",
    "knowledge_content_sha256",
    "resolve_knowledge_content_sha256",
]
