"""Bridge adapting an :class:`LLMProvider` (R13-A port) to the legacy critic
callable (R13-C).

``reflect`` / ``critic_evaluate`` (retrieve_critic/orchestrator.py) take a
``CriticFn = Callable[[str, list[dict]], dict]`` that receives ``(query,
rows)`` and returns a dict with ``adequacy`` / ``suggested_action`` /
``reason``. The orchestrator:

  * normalizes unknown enum values -> ``Adequacy.PARTIAL`` /
    ``CriticAction.ACCEPT`` (handled inside ``critic_evaluate``);
  * does NOT catch ``critic_fn`` exceptions inside ``critic_evaluate`` (it
    re-raises) — but ``reflect`` WRAPS ``critic_evaluate`` and, on any
    exception, stops with ``stopped_reason="critic_error"`` preserving the
    last rows;
  * caches decisions by ``id(critic_fn)`` (+ query + rows signature);
  * does NOT catch ``retrieval_fn`` exceptions — those propagate.

So this bridge, on a normalized provider failure (``LLMResponse.is_failure``),
RAISES ``LLMProviderError`` — and a raw provider exception propagates — so the
orchestrator's ``critic_error`` path fires (preserved EXACTLY). On a successful
response it returns a dict parsed from ``json`` / ``text``; missing or unknown
enum values flow through the orchestrator's existing fallback. It does NOT
touch ``retrieval_fn`` (the bridge only wraps the critic).

Fail-closed wiring: ``make_critic_fn(None)`` raises ``ValueError``.

Cache identity: memoized per ``(provider, board_id, actor_id)`` so the SAME
context yields the SAME callable (stable id) -> ``critic_evaluate``'s
``id(critic_fn)`` cache is preserved.

CONTRACT SEAM (flagged, not assumed): the canonical ``LLMRequest`` has no
dedicated field for the ``rows`` the critic must judge. This bridge composes a
compact prompt embedding the query AND the rows into ``input`` — mirroring how
the rerank bridge uses ``build_default_prompt``. ``purpose`` / ``flow`` is the
canonical ``retrieve_critic``.
"""

from __future__ import annotations

import json

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

from .orchestrator import CriticFn

__all__ = ["make_critic_fn", "reset_bridge_cache"]

_BRIDGE_CACHE_NAMESPACE = "kg.retrieve_critic"


def _memoize(key: tuple, factory) -> CriticFn:
    return bridge_cache_get_or_create(_BRIDGE_CACHE_NAMESPACE, key, factory)


def reset_bridge_cache() -> None:
    """Drop memoized bridge callables. Call in tests or when a wiring change
    requires fresh callable identities."""
    reset_bridge_cache_namespace(_BRIDGE_CACHE_NAMESPACE)


def _compose_critic_input(query: str, rows: list[dict]) -> str:
    """Compose the critic prompt — query + a compact view of the rows.

    Contract seam: ``rows`` have no dedicated request field, so they are
    embedded into ``input`` here (see module docstring).
    """
    lines = [
        f"Query: {query}",
        "",
        "Assess whether the retrieved rows adequately answer the query.",
        'Return ONLY a JSON object: {"adequacy": "sufficient|partial|'
        'irrelevant", "suggested_action": "accept|retry_with_rewrite|'
        'expand_hops|fallback_semantic|change_intent", "reason": "..."}.',
        "",
        "Rows:",
    ]
    for r in rows or []:
        nid = str(r.get("node_id", "")) if isinstance(r, dict) else str(r)
        sim = r.get("similarity", "") if isinstance(r, dict) else ""
        title = r.get("title", "") if isinstance(r, dict) else ""
        lines.append(f"- {nid} (sim={sim}): {title}")
    return "\n".join(lines)


def _decision_dict_from_response(resp: LLMResponse) -> dict:
    """Parse the critic decision dict from the response.

    Prefer a structured ``json`` dict; else parse ``text`` as a JSON object;
    else wrap the raw text under ``reason`` (missing adequacy/action keys flow
    through the orchestrator's unknown-enum fallback). A blank response yields
    ``{}`` -> the orchestrator maps it to PARTIAL/ACCEPT.
    """
    if isinstance(resp.json, dict):
        return resp.json
    text = resp.text
    if isinstance(text, str) and text.strip():
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, dict):
            return parsed
        return {"reason": text}
    return {}


def make_critic_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> CriticFn:
    """Adapt ``provider`` to the ``CriticFn`` contract
    (``Callable[[str, list[dict]], dict]``)."""
    if provider is None:
        raise ValueError(
            "Retrieve-critic bridge requires an LLMProvider — absence is "
            "fail-closed (no silent critic)."
        )

    def _factory() -> CriticFn:
        def _critic(query: str, rows: list[dict]) -> dict:
            req = LLMRequest(
                purpose="retrieve_critic",
                input=_compose_critic_input(query, rows),
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={"flow": "retrieve_critic"},
            )
            resp = provider.complete(req)
            if resp.is_failure:
                # Raise so reflect() records stopped_reason="critic_error".
                raise LLMProviderError(
                    resp.status, message=resp.failure_reason or ""
                )
            return _decision_dict_from_response(resp)

        return _critic

    return _memoize(("critic", id(provider), board_id, actor_id), _factory)
