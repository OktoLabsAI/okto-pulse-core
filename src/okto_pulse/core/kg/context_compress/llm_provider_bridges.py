"""Bridge adapting an :class:`LLMProvider` (R13-A port) to the legacy
context-compression callable (R13-C).

``compress_if_needed`` (context_compress/compress.py) takes a
``compress_llm_fn: Callable[[str], str]`` that receives the concatenated node
text and returns a summary. Its degradation contract (preserved here EXACTLY):

  * No callable / below threshold / no nodes -> ``applied=False`` (the callable
    is never invoked).
  * Callable raises -> caught by ``compress_if_needed`` -> ``applied=False``.
  * Callable returns a non-str / empty / whitespace summary -> ``applied=False``.
  * Valid non-empty summary above threshold -> ``applied=True``.

So this bridge maps a normalized provider failure (``LLMResponse.is_failure``)
and an empty/absent summary to ``""`` — which ``compress_if_needed`` treats as
an empty summary and keeps ``applied=False`` with the original rows intact. A
RAW provider exception is left to propagate; ``compress_if_needed`` already
catches it and likewise keeps ``applied=False`` (preserving the existing
exception path). The bridge NEVER replaces the original rows — it only ever
yields a summary string (or ``""``).

Fail-closed wiring: ``make_compress_llm_fn(None)`` raises ``ValueError``.

Cache identity: memoized per ``(provider, board_id, actor_id)`` so the SAME
context yields the SAME callable (stable id).

The canonical ``LLMRequest.purpose`` / ``flow`` label is ``context_compress``
(from ``api_r13c_cognitive_bridge_contract``).
"""

from __future__ import annotations

from typing import Callable

from okto_pulse.core.kg.interfaces.llm import (
    LLMProvider,
    LLMRequest,
    LLMResponse,
)
from okto_pulse.core.kg.llm_provider_bridge_cache import (
    bridge_cache_get_or_create,
    reset_bridge_cache_namespace,
)

__all__ = ["make_compress_llm_fn", "reset_bridge_cache"]

_BRIDGE_CACHE_NAMESPACE = "kg.context_compress"


def _memoize(key: tuple, factory) -> Callable[[str], str]:
    return bridge_cache_get_or_create(_BRIDGE_CACHE_NAMESPACE, key, factory)


def reset_bridge_cache() -> None:
    """Drop memoized bridge callables. Call in tests or when a wiring change
    requires fresh callable identities."""
    reset_bridge_cache_namespace(_BRIDGE_CACHE_NAMESPACE)


def _summary_from_response(resp: LLMResponse) -> str:
    """Extract a summary string. ``""`` on a normalized failure or an
    empty/absent payload -> ``compress_if_needed`` keeps ``applied=False``."""
    if resp.is_failure:
        return ""
    if isinstance(resp.text, str) and resp.text.strip():
        return resp.text
    if isinstance(resp.json, str) and resp.json.strip():
        return resp.json
    return ""


def make_compress_llm_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> Callable[[str], str]:
    """Adapt ``provider`` to the ``compress_llm_fn`` contract
    (``Callable[[str], str]``)."""
    if provider is None:
        raise ValueError(
            "Context-compress bridge requires an LLMProvider — absence is "
            "fail-closed (no silent no-op compressor)."
        )

    def _factory() -> Callable[[str], str]:
        def _compress(text: str) -> str:
            req = LLMRequest(
                purpose="context_compress",
                input=text,
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={"flow": "context_compress"},
            )
            # A raw provider exception is intentionally NOT caught here:
            # compress_if_needed already catches it and keeps applied=False.
            resp = provider.complete(req)
            return _summary_from_response(resp)

        return _compress

    return _memoize(("compress", id(provider), board_id, actor_id), _factory)
