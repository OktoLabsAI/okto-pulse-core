"""Bridge adapting an :class:`LLMProvider` (R13-A port) to the legacy
adaptive-hop callable (R13-B).

``LLMHopPlanner`` (adaptive_hops/llm.py) expects
``Callable[[str, str, list[str]], int]`` — ``(query, intent_name,
seed_titles) -> hops``. The planner wraps the callable in
``try/except Exception`` and, on failure, returns
``HopDecision(reason="llm_error_fallback")`` with the fixed fallback hops; on
success it ``clamp_hops``-es the raw int and reports ``reason="llm"``.

So this bridge, on a provider failure — a normalized ``LLMResponse.is_failure``
OR a raw exception from ``complete`` OR an OK response whose payload is not a
parseable int — RAISES ``LLMProviderError``. It NEVER returns a silent fallback
hop itself: the planner owns the fallback decision and must see the failure to
record ``llm_error_fallback``. The bridge also does NOT clamp; the planner's
``clamp_hops`` keeps the hop ceiling/floor semantics in one place.

Fail-closed wiring: ``make_hop_llm_fn(None)`` raises ``ValueError`` (mirroring
the factory's "strategy 'llm' requires an llm_fn"); a missing provider never
silently degrades the planner to the "fixed" strategy.

Cache identity: memoized per ``(provider, board_id, actor_id)`` so the SAME
context yields the SAME callable (stable id -> factory cache hit / shared LRU)
and DIFFERENT contexts yield DIFFERENT callables (factory cache miss).
"""

from __future__ import annotations

from typing import Callable

from okto_pulse.core.kg.interfaces.llm import (
    LLM_INVALID_RESPONSE,
    LLMProvider,
    LLMProviderError,
    LLMRequest,
    LLMResponse,
)
from okto_pulse.core.kg.llm_provider_bridge_cache import (
    bridge_cache_get_or_create,
    reset_bridge_cache_namespace,
)

__all__ = ["make_hop_llm_fn", "reset_bridge_cache"]

_BRIDGE_CACHE_NAMESPACE = "kg.adaptive_hops"


def _memoize(key: tuple, factory: Callable[[], Callable]) -> Callable:
    return bridge_cache_get_or_create(_BRIDGE_CACHE_NAMESPACE, key, factory)


def reset_bridge_cache() -> None:
    """Drop memoized bridge callables. Call in tests or when a wiring change
    requires fresh callable identities."""
    reset_bridge_cache_namespace(_BRIDGE_CACHE_NAMESPACE)


def _coerce_hops(resp: LLMResponse) -> int:
    """Extract a raw int hop count from an OK response.

    Prefer ``json`` (a plain int); else parse ``json``/``text`` as a decimal
    int. Anything that cannot be coerced is an INVALID response -> raise so the
    planner falls back. The returned value is NOT clamped here — the planner's
    ``clamp_hops`` enforces the [floor, ceiling] range.
    """
    payload = resp.json
    # bool is an int subclass but is never a valid hop signal.
    if isinstance(payload, bool):
        raise LLMProviderError(LLM_INVALID_RESPONSE, message="non-numeric hops")
    if isinstance(payload, int):
        return payload

    candidate: str | None = None
    if isinstance(payload, str):
        candidate = payload.strip()
    elif isinstance(resp.text, str):
        candidate = resp.text.strip()
    if candidate:
        try:
            return int(candidate)
        except (TypeError, ValueError):
            pass
    raise LLMProviderError(LLM_INVALID_RESPONSE, message="unparseable hops")


def make_hop_llm_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> Callable[[str, str, list[str]], int]:
    """Adapt ``provider`` to the adaptive-hop callable
    ``Callable[[str, str, list[str]], int]``."""
    if provider is None:
        raise ValueError(
            "Adaptive-hops bridge requires an LLMProvider — absence is "
            "fail-closed (no silent fixed-hops fallback at wiring time)."
        )

    def _factory() -> Callable[[str, str, list[str]], int]:
        def _hop(query: str, intent_name: str, seed_titles: list[str]) -> int:
            req = LLMRequest(
                purpose="adaptive_hops",
                input=query,
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={
                    "flow": "adaptive_hops",
                    "intent": intent_name,
                },
            )
            resp = provider.complete(req)
            if resp.is_failure:
                raise LLMProviderError(
                    resp.status, message=resp.failure_reason or ""
                )
            return _coerce_hops(resp)

        return _hop

    return _memoize(("hops", id(provider), board_id, actor_id), _factory)
