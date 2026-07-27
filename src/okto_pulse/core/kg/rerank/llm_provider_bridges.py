"""Bridge adapting an :class:`LLMProvider` (R13-A port) to the legacy LLM
reranker callable (R13-C).

``LLMReranker`` (rerank/llm.py) expects an ``LLMRankerFn`` —
``Callable[[str, Sequence[object]], list[str]]`` that receives ``(query,
candidates)`` and returns the ordered ``node_id`` strings (most relevant
first). The reranker itself ignores unknown ids, fills omitted ones in input
order, and falls back to the input order when the callable RAISES.

R13-A consolidated the LLM *contract* into ``LLMProvider`` / ``LLMRequest`` /
``LLMResponse``. This bridge adapts a provider to the callable shape WITHOUT
touching the reranker algorithm: it composes the prompt via the existing
``build_default_prompt`` (the helper exposed for exactly this purpose), calls
``provider.complete`` and parses the response into ``list[str]``.

Failure semantics — preserve the reranker behaviour exactly:
  * On a normalized provider failure (``LLMResponse.is_failure``) the bridge
    RAISES ``LLMProviderError``; a raw provider exception propagates. Either
    way ``LLMReranker.rerank`` catches it and falls back to the input order.
  * An OK response that yields NO ids returns ``[]`` -> the reranker treats
    that as "empty ranking" and returns the input order (unchanged).

Fail-closed wiring: ``make_llm_ranker_fn(None)`` raises ``ValueError``.

Cache identity: memoized per ``(provider, board_id, actor_id)`` so the SAME
context yields the SAME callable (stable id). NOTE: the rerank factory does
NOT cache ``LLMReranker`` instances (each call may bind a different provider),
and this bridge does not change that — it only stabilizes the callable id.

The canonical ``LLMRequest.purpose`` / ``flow`` label is ``rerank_llm`` (from
``api_r13c_cognitive_bridge_contract``) — distinct from the factory *strategy*
name ``llm`` (unchanged).
"""

from __future__ import annotations

import json
from typing import Sequence

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

from .llm import LLMRankerFn, build_default_prompt

__all__ = ["make_llm_ranker_fn", "reset_bridge_cache"]

_BRIDGE_CACHE_NAMESPACE = "kg.rerank"


def _memoize(key: tuple, factory) -> LLMRankerFn:
    return bridge_cache_get_or_create(_BRIDGE_CACHE_NAMESPACE, key, factory)


def reset_bridge_cache() -> None:
    """Drop memoized bridge callables. Call in tests or when a wiring change
    requires fresh callable identities."""
    reset_bridge_cache_namespace(_BRIDGE_CACHE_NAMESPACE)


def _ids_from_response(resp: LLMResponse) -> list[str]:
    """Parse an ordered list of node_id strings from the response.

    Prefer a structured ``json`` array; otherwise parse ``text`` as a JSON
    array (the prompt asks for exactly that). Anything else -> ``[]`` so the
    reranker falls back to the input order (unchanged behaviour).
    """
    payload = resp.json
    if isinstance(payload, list):
        return [str(x) for x in payload]
    text = resp.text
    if isinstance(text, str) and text.strip():
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    return []


def make_llm_ranker_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> LLMRankerFn:
    """Adapt ``provider`` to the ``LLMRankerFn`` contract."""
    if provider is None:
        raise ValueError(
            "Rerank bridge requires an LLMProvider — absence is fail-closed "
            "(no silent passthrough reranker)."
        )

    def _factory() -> LLMRankerFn:
        def _rank(query: str, candidates: Sequence[object]) -> list[str]:
            req = LLMRequest(
                purpose="rerank_llm",
                input=build_default_prompt(query, candidates),
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={"flow": "rerank_llm"},
            )
            resp = provider.complete(req)
            if resp.is_failure:
                raise LLMProviderError(
                    resp.status, message=resp.failure_reason or ""
                )
            return _ids_from_response(resp)

        return _rank

    return _memoize(("rerank", id(provider), board_id, actor_id), _factory)
