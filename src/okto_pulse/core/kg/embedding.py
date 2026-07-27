"""Embedding provider port helpers.

The core package owns the ``EmbeddingProvider`` port and registry lookup helper
only. Concrete providers, including deterministic stubs and sentence-transformers
adapters, must be supplied explicitly by tests or by an edition composition root.
"""

from __future__ import annotations

from okto_pulse.core.kg.interfaces.embedding import EmbeddingProvider

__all__ = [
    "EmbeddingProvider",
    "get_embedding_provider",
    "reset_embedding_provider_cache",
]


def get_embedding_provider() -> EmbeddingProvider:
    """Return the configured embedding provider via the KG registry."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    return get_kg_registry().require_embedding_provider()


def reset_embedding_provider_cache() -> None:
    """Drop the cached provider — resets the whole KG registry."""
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests

    reset_registry_for_tests()
