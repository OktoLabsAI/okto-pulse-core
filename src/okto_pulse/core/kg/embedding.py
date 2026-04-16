"""Embedding provider with a zero-dep stub mode for tests.

Two modes, selected by `kg_embedding_mode` in CoreSettings:

- `stub` (default) — deterministic hash-based 384-dim vectors. No external
  deps. Use in unit tests and CI so suites don't pay the cost of loading a
  transformer model.
- `sentence-transformers` — lazy-loads `sentence-transformers/all-MiniLM-L6-v2`
  (requires installing `okto-pulse-core[kg-embeddings]`). Production mode.

The provider is cached via `get_embedding_provider()` so settings changes at
runtime need a process restart (acceptable — this is embedded, single-process).
"""

from __future__ import annotations

import hashlib
import math
import struct
from abc import ABC, abstractmethod
from typing import Sequence


class EmbeddingProvider(ABC):
    """Minimal contract for producing dense vectors from text."""

    dim: int

    @abstractmethod
    def encode(self, text: str) -> list[float]:
        """Encode a single string. Returns a list of length `dim`."""

    def encode_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """Default batch path — providers override for efficiency."""
        return [self.encode(t) for t in texts]


class StubEmbeddingProvider(EmbeddingProvider):
    """Deterministic hash-based provider for tests.

    Maps text to a pseudo-random unit vector using SHA256 as a PRNG seed. Two
    invocations with the same text return the same vector. Cosine similarity
    between "identical" texts is 1.0, between random texts is ~0.
    """

    def __init__(self, dim: int = 384):
        self.dim = dim

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


class SentenceTransformerProvider(EmbeddingProvider):
    """Lazy-loaded sentence-transformers provider."""

    def __init__(self, model_name: str, dim: int = 384):
        self.model_name = model_name
        self.dim = dim
        self._model = None

    def _get_model(self):
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "sentence-transformers is not installed — "
                    "install with `pip install okto-pulse-core[kg-embeddings]` "
                    "or set kg_embedding_mode=stub for stub mode"
                ) from exc
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def encode(self, text: str) -> list[float]:
        model = self._get_model()
        vec = model.encode(text or "", normalize_embeddings=True)
        return vec.tolist() if hasattr(vec, "tolist") else list(vec)

    def encode_batch(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        model = self._get_model()
        batch = model.encode(list(texts), normalize_embeddings=True)
        return [row.tolist() if hasattr(row, "tolist") else list(row) for row in batch]


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
        return SentenceTransformerProvider(
            model_name=config.kg_embedding_model,
            dim=dim,
        )
    raise ValueError(
        f"unknown kg_embedding_mode: {mode!r} "
        f"(expected 'stub' or 'sentence-transformers')"
    )


def get_embedding_provider() -> EmbeddingProvider:
    """Return the configured embedding provider via the KG registry."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    return get_kg_registry().embedding_provider


def reset_embedding_provider_cache() -> None:
    """Drop the cached provider — resets the whole KG registry."""
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests

    reset_registry_for_tests()
