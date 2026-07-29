"""Historical board-source hash compatibility contracts."""

from __future__ import annotations

import hashlib
import json

from okto_pulse.core.kg import board_source_store
from okto_pulse.core.kg.board_source_store import projected_root_content_hash


_BASE_CONTENT_HASH = "a" * 64
_QUALITY_FINGERPRINTS = ("quality-z", "quality-a", "quality-z")
_RESEARCH_DECISION_FINGERPRINTS = ("rdl-b", "rdl-a")
_HISTORICAL_V3_BYTES = (
    b'{"base_content_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    b'aaaaaaaaaaaaaaaa","projection_schema_version":3,"quality_head_fingerprints":'
    b'["quality-a","quality-z"],"research_decision_head_fingerprints":'
    b'["rdl-a","rdl-b"]}'
)
_HISTORICAL_V3_HASH = "ec5b3ddf6ec48339d6793cdf2bbc08bd07d7df4054d6299e32560487797fb21e"


def _projected_hash() -> str:
    return projected_root_content_hash(
        _BASE_CONTENT_HASH,
        quality_head_fingerprints=_QUALITY_FINGERPRINTS,
        research_decision_head_fingerprints=_RESEARCH_DECISION_FINGERPRINTS,
    )


def test_projected_root_content_hash_v3_preserves_historical_bytes() -> None:
    payload = {
        "base_content_hash": _BASE_CONTENT_HASH,
        "projection_schema_version": 3,
        "quality_head_fingerprints": ["quality-a", "quality-z"],
        "research_decision_head_fingerprints": ["rdl-a", "rdl-b"],
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")

    assert encoded == _HISTORICAL_V3_BYTES
    assert hashlib.sha256(encoded).hexdigest() == _HISTORICAL_V3_HASH
    assert _projected_hash() == _HISTORICAL_V3_HASH


def test_manifest_version_bump_does_not_reinterpret_v3_root_hash(
    monkeypatch,
) -> None:
    before = _projected_hash()

    monkeypatch.setattr(board_source_store, "SPEC_SOURCE_MANIFEST_VERSION", 4)

    assert board_source_store.SPEC_SOURCE_MANIFEST_VERSION == 4
    assert _projected_hash() == before == _HISTORICAL_V3_HASH
