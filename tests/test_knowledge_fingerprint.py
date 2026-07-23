from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

from okto_pulse.core.domain.knowledge_fingerprint import (
    KNOWLEDGE_CONTENT_HASH_FIELDS,
    compute_knowledge_content_sha256,
    knowledge_content_sha256,
    resolve_knowledge_content_sha256,
)


def _knowledge_payload() -> dict[str, object]:
    return {
        "id": "kb-1",
        "title": "Canonical reference",
        "description": "Storage-neutral bytes",
        "content": "conteudo com acento",
        "mime_type": "text/markdown",
    }


def test_knowledge_fingerprint_uses_the_frozen_canonical_field_contract() -> None:
    payload = _knowledge_payload()
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )

    assert KNOWLEDGE_CONTENT_HASH_FIELDS == (
        "id",
        "title",
        "description",
        "content",
        "mime_type",
    )
    assert (
        compute_knowledge_content_sha256(payload)
        == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    )
    assert knowledge_content_sha256(payload) == compute_knowledge_content_sha256(
        payload
    )
    assert compute_knowledge_content_sha256(
        SimpleNamespace(**payload)
    ) == compute_knowledge_content_sha256(payload)


def test_knowledge_fingerprint_is_order_stable_and_changes_with_content() -> None:
    payload = _knowledge_payload()
    reordered = dict(reversed(list(payload.items())))
    changed = {**payload, "content": "different bytes"}

    assert compute_knowledge_content_sha256(reordered) == (
        compute_knowledge_content_sha256(payload)
    )
    assert compute_knowledge_content_sha256(changed) != (
        compute_knowledge_content_sha256(payload)
    )


def test_resolver_prefers_persisted_hash_and_lazily_hashes_legacy_rows() -> None:
    payload = _knowledge_payload()
    persisted = {**payload, "content_hash": "persisted-sha256"}

    assert resolve_knowledge_content_sha256(persisted) == "persisted-sha256"
    assert resolve_knowledge_content_sha256(payload) == (
        compute_knowledge_content_sha256(payload)
    )
    assert resolve_knowledge_content_sha256(
        SimpleNamespace(**payload, content_hash=None)
    ) == compute_knowledge_content_sha256(payload)
