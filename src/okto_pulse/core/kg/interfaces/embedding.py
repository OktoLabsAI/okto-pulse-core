"""EmbeddingProvider Protocol — migrated from ABC to PEP 544 Protocol."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Minimal contract for producing dense vectors from text.

    Migrated from ABC (okto_pulse.core.kg.embedding) to Protocol.
    Existing implementations (StubEmbeddingProvider, SentenceTransformerProvider)
    satisfy this Protocol by duck typing without inheriting.
    """

    dim: int

    def encode(self, text: str) -> list[float]:
        """Encode a single string. Returns a list of length `dim`."""
        ...

    def encode_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """Batch encode. Default: iterate encode(). Providers override for efficiency."""
        ...
