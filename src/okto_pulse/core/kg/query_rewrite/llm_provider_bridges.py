"""Bridges adapting an :class:`LLMProvider` (R13-A port) to the legacy
query-rewrite callables (R13-B).

The query-rewrite rewriters each expect a plain callable injected by the
caller:

  * HyDE      ``Callable[[str], str]``            — query -> hypothetical passage
  * decompose ``Callable[[str], list[str]]``      — query -> sub-queries
  * fusion    ``Callable[[str, int], list[str]]`` — (query, k) -> K paraphrases

R13-A consolidated the LLM *contract* into ``LLMProvider`` / ``LLMRequest`` /
``LLMResponse`` (interfaces/llm.py). These bridges adapt a provider to the
callable shape WITHOUT touching the rewriter algorithm (prompts, parse, RRF).
A bridge only: builds the request, calls ``provider.complete`` and maps the
response back into the type the rewriter expects.

The ``LLMRequest.purpose`` (and the ``flow`` telemetry label) use the canonical
values from the ``api_r13b_query_bridge_contract``: ``query_hyde`` /
``query_decompose`` / ``query_fusion`` (the adaptive-hops bridge uses
``adaptive_hops``). These are the request *purpose* labels — distinct from the
factory *strategy* names (``hyde`` / ``decompose`` / ``fusion``), which are
unchanged.

Failure semantics — preserve the current rewriter behaviour exactly:
  * Each rewriter wraps its ``llm_fn`` call in ``try/except Exception`` and
    degrades to ``strategy="none"`` (passthrough). So a bridge RAISES
    ``LLMProviderError`` on a normalized provider failure (and lets a raw
    provider exception propagate); the rewriter then degrades silently —
    identical to the pre-R13-B behaviour where a failing callable degraded.
  * Absence of a provider is fail-closed at WIRING time: ``make_*_llm_fn``
    raises ``ValueError`` (mirroring the factory's "requires an llm_fn"),
    never returning a no-op callable. With no provider the caller passes
    ``llm_fn=None`` to ``get_rewriter("hyde")`` which still raises.

Cache identity — the factory caches rewriters by ``id(llm_fn)``. The bridge is
memoized per ``(provider, board_id, actor_id)`` so the SAME context yields the
SAME callable object (stable id -> factory cache hit) and DIFFERENT contexts
yield DIFFERENT callables (factory cache miss). The cache holds a strong ref to
the callable (which closes over the provider), keeping ``id(provider)`` stable
for the lifetime of the entry.
"""

from __future__ import annotations

from typing import Callable

from okto_pulse.core.kg.interfaces.llm import (
    LLMProvider,
    LLMProviderError,
    LLMRequest,
    LLMResponse,
)
from okto_pulse.core.kg.llm_provider_bridge_cache import (
    bridge_cache_get_or_create,
    reset_bridge_cache_namespace,
)

__all__ = [
    "make_hyde_llm_fn",
    "make_decompose_llm_fn",
    "make_fusion_llm_fn",
    "reset_bridge_cache",
]

_BRIDGE_CACHE_NAMESPACE = "kg.query_rewrite"


def _memoize(key: tuple, factory: Callable[[], Callable]) -> Callable:
    return bridge_cache_get_or_create(_BRIDGE_CACHE_NAMESPACE, key, factory)


def reset_bridge_cache() -> None:
    """Drop memoized bridge callables. Call in tests or when a wiring change
    requires fresh callable identities."""
    reset_bridge_cache_namespace(_BRIDGE_CACHE_NAMESPACE)


def _require_provider(provider: LLMProvider | None, flow: str) -> None:
    """Fail-closed at wiring time: a missing provider is an explicit error,
    never a silent no-op."""
    if provider is None:
        raise ValueError(
            f"Query-rewrite bridge {flow!r} requires an LLMProvider — "
            f"absence is fail-closed (no silent no-op rewriter)."
        )


def _raise_on_failure(resp: LLMResponse) -> None:
    """Surface a normalized provider failure as ``LLMProviderError`` so the
    rewriter's existing ``except Exception`` degrades it to passthrough."""
    if resp.is_failure:
        raise LLMProviderError(resp.status, message=resp.failure_reason or "")


def _texts_from_response(resp: LLMResponse) -> list[str]:
    """Best-effort ``list[str]`` extraction for decompose / fusion.

    Prefer a structured ``json`` list; otherwise split ``text`` into stripped,
    non-empty lines. The rewriter applies its own ``len < 2`` / empty-string
    filtering unchanged, so this only adapts the contract — it does not
    re-implement the decompose/fusion algorithm.
    """
    payload = resp.json
    if isinstance(payload, (list, tuple)):
        return [item for item in payload if isinstance(item, str)]
    text = resp.text
    if isinstance(text, str) and text.strip():
        return [line for line in (ln.strip() for ln in text.splitlines()) if line]
    return []


def make_hyde_llm_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> Callable[[str], str]:
    """Adapt ``provider`` to the HyDE callable ``Callable[[str], str]``."""
    _require_provider(provider, "hyde")

    def _factory() -> Callable[[str], str]:
        def _hyde(query: str) -> str:
            req = LLMRequest(
                purpose="query_hyde",
                input=query,
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={"flow": "query_hyde"},
            )
            resp = provider.complete(req)
            _raise_on_failure(resp)
            # HyDE wants a single passage string. Prefer text; fall back to a
            # json string. None/empty -> "" so the rewriter's empty-passage
            # degradation path triggers (behaviour unchanged).
            if isinstance(resp.text, str):
                return resp.text
            if isinstance(resp.json, str):
                return resp.json
            return ""

        return _hyde

    return _memoize(("hyde", id(provider), board_id, actor_id), _factory)


def make_decompose_llm_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> Callable[[str], list[str]]:
    """Adapt ``provider`` to the decompose callable
    ``Callable[[str], list[str]]``."""
    _require_provider(provider, "decompose")

    def _factory() -> Callable[[str], list[str]]:
        def _decompose(query: str) -> list[str]:
            req = LLMRequest(
                purpose="query_decompose",
                input=query,
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={"flow": "query_decompose"},
            )
            resp = provider.complete(req)
            _raise_on_failure(resp)
            return _texts_from_response(resp)

        return _decompose

    return _memoize(("decompose", id(provider), board_id, actor_id), _factory)


def make_fusion_llm_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> Callable[[str, int], list[str]]:
    """Adapt ``provider`` to the fusion callable
    ``Callable[[str, int], list[str]]``.

    The requested paraphrase count ``k`` is passed to the provider via
    ``telemetry_labels["paraphrase_count"]`` (the contract has no dedicated
    field); the rewriter still truncates the returned list to ``k`` itself.
    """
    _require_provider(provider, "fusion")

    def _factory() -> Callable[[str, int], list[str]]:
        def _fusion(query: str, k: int) -> list[str]:
            req = LLMRequest(
                purpose="query_fusion",
                input=query,
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={
                    "flow": "query_fusion",
                    "paraphrase_count": str(k),
                },
            )
            resp = provider.complete(req)
            _raise_on_failure(resp)
            return _texts_from_response(resp)

        return _fusion

    return _memoize(("fusion", id(provider), board_id, actor_id), _factory)
