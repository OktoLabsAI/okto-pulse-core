"""Embedding provider port helpers with a zero-dep core stub.

The core package owns only the deterministic fallback used by tests and
unconfigured runtimes. Concrete ML providers, including sentence-transformers,
are supplied by the edition composition root through the EmbeddingProvider port.

- `stub` — deterministic hash-based 384-dim vectors. No external
  deps. Use in unit tests and CI so suites don't pay the cost of loading a
  transformer model.
- `sentence-transformers`/`st` — no concrete provider is instantiated in core;
  the builder degrades to the stub unless an edition has already registered an
  EmbeddingProvider in the KG registry.
"""

from __future__ import annotations

import hashlib
import logging
import math
import struct
from typing import Sequence

from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider

__all__ = [
    "EmbeddingProvider",
    "StubEmbeddingProvider",
    "get_embedding_provider",
    "reset_embedding_provider_cache",
]


logger = logging.getLogger(__name__)


class StubEmbeddingProvider:
    """Deterministic hash-based provider for tests.

    Maps text to a pseudo-random unit vector using SHA256 as a PRNG seed. Two
    invocations with the same text return the same vector. Cosine similarity
    between "identical" texts is 1.0, between random texts is ~0.
    """

    def __init__(self, dim: int = 384):
        self.dim = dim

    def encode_batch(self, texts: Sequence[str]) -> list[list[float]]:
        return [self.encode(t) for t in texts]

    def encode(self, text: str) -> list[float]:
        seed = hashlib.sha256((text or "").encode("utf-8")).digest()
        # Expand the 32-byte seed into dim floats by repeatedly hashing
        # (counter-mode). SHAKE would be cleaner but SHA256 is in stdlib.
        vec: list[float] = []
        counter = 0
        while len(vec) < self.dim:
            chunk = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
            # 8 floats per 32-byte chunk (4-byte uint32 → float in [-1, 1]).
            for i in range(0, 32, 4):
                if len(vec) >= self.dim:
                    break
                u = struct.unpack(">I", chunk[i : i + 4])[0]
                vec.append((u / 0xFFFFFFFF) * 2.0 - 1.0)
            counter += 1

        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        return [x / norm for x in vec]

    def embedding_metadata(self) -> dict:
        """Capability metadata (R13-A) — lets common surfaces describe this
        provider without ``isinstance``. The stub has no external artifact, so
        it is always 'loaded'."""
        return {
            "model_name": None,
            "embedding_dimension": self.dim,
            "is_loaded": True,
            "is_stub": True,
        }


def _build_provider_from_config(config) -> EmbeddingProvider:
    """Build an embedding provider from a KGConfig-compatible object.

    Called by the registry's _build_defaults() — must NOT go through the
    registry itself to avoid circular initialization.
    """
    mode = (config.kg_embedding_mode or "stub").lower()
    dim = config.kg_embedding_dim

    if mode == "stub":
        return StubEmbeddingProvider(dim=dim)
    if mode in ("sentence-transformers", "sentence_transformers", "st"):
        logger.warning(
            "kg_embedding_mode=%s requires an edition-owned EmbeddingProvider; "
            "core pure defaults to StubEmbeddingProvider until composition "
            "registers a concrete provider",
            mode,
        )
        return StubEmbeddingProvider(dim=dim)
    raise ValueError(
        f"unknown kg_embedding_mode: {mode!r} "
        f"(expected 'stub' or an edition-owned embedding provider mode)"
    )


def get_embedding_provider() -> EmbeddingProvider:
    """Return the configured embedding provider via the KG registry."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    return get_kg_registry().embedding_provider


def reset_embedding_provider_cache() -> None:
    """Drop the cached provider — resets the whole KG registry."""
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests

    reset_registry_for_tests()
