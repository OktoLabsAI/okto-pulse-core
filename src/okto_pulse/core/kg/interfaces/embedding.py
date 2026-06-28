"""EmbeddingProvider Protocol — migrated from ABC to PEP 544 Protocol."""

from __future__ import annotations

from typing import Any, Protocol, Sequence, runtime_checkable


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Minimal contract for producing dense vectors from text.

    Migrated from ABC (okto_pulse.core.kg.embedding) to Protocol.
    Implementations such as the core StubEmbeddingProvider and edition-owned
    concrete providers satisfy this Protocol by duck typing without inheriting.
    """

    dim: int

    def encode(self, text: str) -> list[float]:
        """Encode a single string. Returns a list of length `dim`."""
        ...

    def encode_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """Batch encode. Default: iterate encode(). Providers override for efficiency."""
        ...

    # Optional capability metadata (R13-A). A provider MAY expose
    # ``embedding_metadata()`` returning ``{model_name, embedding_dimension,
    # is_loaded, is_stub}`` so common surfaces describe/select it via metadata
    # rather than ``isinstance`` against a concrete class. Absence is tolerated
    # — ``describe_embedding_provider`` falls back to best-effort introspection.


def describe_embedding_provider(provider: Any) -> dict[str, Any]:
    """Describe an embedding provider WITHOUT importing or ``isinstance``-checking
    any concrete provider class (R13-A — fr_r13a_provider_metadata).

    Common API/UI surfaces (e.g. ``/kg/settings``) use this to report the live
    provider state. Selection is driven by the provider's own
    ``embedding_metadata()`` capability when present; providers that do not
    declare it fall back to best-effort, load-free introspection. The function
    NEVER triggers a model load: it reads ``_model`` directly, mirroring the
    pre-R13-A behaviour.

    Returned shape is stable (observable contract of the settings banner):
    ``embedding_provider_name``, ``model_name``, ``embedding_dimension``,
    ``is_loaded``, ``is_stub``.
    """
    name = type(provider).__name__ if provider is not None else "NoneProvider"

    meta_fn = getattr(provider, "embedding_metadata", None)
    if callable(meta_fn):
        meta = meta_fn() or {}
    elif provider is None:
        meta = {
            "model_name": None,
            "embedding_dimension": 0,
            "is_loaded": False,
            "is_stub": True,
        }
    else:
        # Provider that does not declare capability metadata — best effort,
        # load-free (read ``_model`` directly, never call ``_get_model()``).
        meta = {
            "model_name": getattr(provider, "model_name", None),
            "embedding_dimension": getattr(provider, "dim", 0),
            "is_loaded": getattr(provider, "_model", None) is not None,
            "is_stub": False,
        }

    return {
        "embedding_provider_name": name,
        "model_name": meta.get("model_name"),
        "embedding_dimension": meta.get("embedding_dimension", 0),
        "is_loaded": meta.get("is_loaded", False),
        "is_stub": meta.get("is_stub", False),
    }
