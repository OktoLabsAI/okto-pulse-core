"""Test-only embedding fakes for the KG registry.

This module is the sanctioned fake route used by ``_build_defaults`` and tests.
Runtime core modules must not import this provider as a production fallback.
"""

from __future__ import annotations

import hashlib
import math
import struct
from typing import Sequence

from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider

__all__ = ["TestingStubEmbeddingProvider", "build_testing_embedding_provider"]


class TestingStubEmbeddingProvider:
    """Deterministic hash-based provider for tests."""

    __test__ = False

    def __init__(self, dim: int = 384) -> None:
        self.dim = dim

    def encode_batch(self, texts: Sequence[str]) -> list[list[float]]:
        return [self.encode(t) for t in texts]

    def encode(self, text: str) -> list[float]:
        seed = hashlib.sha256((text or "").encode("utf-8")).digest()
        vec: list[float] = []
        counter = 0
        while len(vec) < self.dim:
            chunk = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
            for i in range(0, 32, 4):
                if len(vec) >= self.dim:
                    break
                u = struct.unpack(">I", chunk[i : i + 4])[0]
                vec.append((u / 0xFFFFFFFF) * 2.0 - 1.0)
            counter += 1

        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        return [x / norm for x in vec]

    def embedding_metadata(self) -> dict:
        return {
            "model_name": None,
            "embedding_dimension": self.dim,
            "is_loaded": True,
            "is_stub": True,
        }


def build_testing_embedding_provider(config: object) -> EmbeddingProvider:
    """Build the explicit test fake from a KGConfig-compatible object."""
    mode = str(getattr(config, "kg_embedding_mode", "stub") or "stub").lower()
    supported_test_modes = {
        "stub",
        "test",
        "testing",
        "sentence-transformers",
        "sentence_transformers",
        "st",
    }
    if mode not in supported_test_modes:
        raise ValueError(
            f"test-only embedding provider cannot satisfy mode {mode!r}; "
            "runtime editions must compose their own EmbeddingProvider"
        )
    return TestingStubEmbeddingProvider(dim=getattr(config, "kg_embedding_dim", 384))
