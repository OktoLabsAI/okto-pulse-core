"""Bridges adapting an :class:`LLMProvider` (R13-A port) to the legacy grounding
callables (R13-D).

``verify_grounding`` (grounding/grounding.py) takes two injected callables:

  * ``ExtractorFn = Callable[[str], list[Claim]]`` — answer_text -> atomic claims
  * ``GrounderFn  = Callable[[list[Claim], list[dict]], list[dict]]`` — per-claim
    ``{"grounded": bool, "confidence": float, "supporting_ids": [str]}``

``verify_grounding`` NEVER raises. Its error contract (preserved here EXACTLY):

  * ``extractor_fn`` raises  -> ``GroundingResult(overall_grounded=False,
    confidence=0.0)`` (fail-closed).
  * extractor returns 0 claims -> vacuous truth ``(True, confidence=1.0)``.
  * ``grounder_fn`` raises (via ``score_semantic_grounding``) -> fail-closed
    ``(False, 0.0)``.
  * grounder returns a non-list / short / long list -> sanitized + padded /
    truncated inside ``score_semantic_grounding`` (UNCHANGED — the bridge never
    re-implements that).

So a normalized provider failure (``LLMResponse.is_failure``) RAISES
``LLMProviderError`` so the orchestrator maps it to fail-closed — it is NEVER
returned as ``[]`` (which would masquerade as the vacuous-truth 1.0 path). A
genuinely empty extractor list (the provider succeeded and found no claims)
returns ``[]`` -> the vacuous-truth path, preserved.

CONTRACT SEAM (flagged, not assumed):
  1. The grounder receives ``(claims, rows)`` but the canonical ``LLMRequest``
     has no field for them — they are composed into ``input`` here.
  2. A *successful but structurally invalid* extractor response (payload not a
     JSON array) is treated as fail-closed (raise), NOT vacuous-truth, because
     grounding is a SAFETY gate — a broken extractor must not make every answer
     look confidently grounded.

Fail-closed wiring: ``make_*_fn(None)`` raises ``ValueError``.
Cache identity: memoized per ``(provider, board_id, actor_id)`` (stable id).
Canonical purposes: ``grounding_extract`` / ``grounding_score``.
"""

from __future__ import annotations

import json

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

from .grounding import Claim, ExtractorFn, GrounderFn

__all__ = ["make_extractor_fn", "make_grounder_fn", "reset_bridge_cache"]

_BRIDGE_CACHE_NAMESPACE = "kg.grounding"


def _memoize(key: tuple, factory):
    return bridge_cache_get_or_create(_BRIDGE_CACHE_NAMESPACE, key, factory)


def reset_bridge_cache() -> None:
    """Drop memoized bridge callables. Call in tests or when a wiring change
    requires fresh callable identities."""
    reset_bridge_cache_namespace(_BRIDGE_CACHE_NAMESPACE)


def _raise_on_failure(resp: LLMResponse) -> None:
    if resp.is_failure:
        raise LLMProviderError(resp.status, message=resp.failure_reason or "")


def _payload_list(resp: LLMResponse):
    """Return a list payload from ``json`` or a JSON-array ``text``; ``None``
    when neither yields a list."""
    payload = resp.json
    if isinstance(payload, list):
        return payload
    if isinstance(resp.text, str) and resp.text.strip():
        try:
            parsed = json.loads(resp.text)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, list):
            return parsed
    return None


def make_extractor_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> ExtractorFn:
    """Adapt ``provider`` to the ``ExtractorFn`` (``answer -> list[Claim]``)."""
    if provider is None:
        raise ValueError(
            "Grounding extractor bridge requires an LLMProvider — absence is "
            "fail-closed."
        )

    def _factory() -> ExtractorFn:
        def _extract(answer_text: str) -> list[Claim]:
            req = LLMRequest(
                purpose="grounding_extract",
                input=answer_text,
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={"flow": "grounding_extract"},
            )
            resp = provider.complete(req)
            _raise_on_failure(resp)
            payload = _payload_list(resp)
            if payload is None:
                # SAFETY: a structurally invalid OK response is fail-closed,
                # NOT the vacuous-truth path. verify_grounding's extractor
                # except -> GroundingResult(False, 0.0).
                raise LLMProviderError(
                    LLM_INVALID_RESPONSE, message="extractor payload not a list"
                )
            claims: list[Claim] = []
            for item in payload:
                if isinstance(item, dict):
                    text = str(item.get("text", "")).strip()
                    raw_ents = item.get("mentioned_entities", []) or []
                    if isinstance(raw_ents, (list, tuple)):
                        ents = tuple(
                            str(e) for e in raw_ents
                            if isinstance(e, str) and e.strip()
                        )
                    else:
                        ents = ()
                    if text:
                        claims.append(Claim(text=text, mentioned_entities=ents))
                elif isinstance(item, str) and item.strip():
                    claims.append(Claim(text=item.strip()))
            # Empty list (provider found no claims) -> vacuous-truth path.
            return claims

        return _extract

    return _memoize(("extract", id(provider), board_id, actor_id), _factory)


def make_grounder_fn(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> GrounderFn:
    """Adapt ``provider`` to the ``GrounderFn``
    (``(claims, rows) -> list[dict]``). Sanitization/padding/truncation stays in
    ``score_semantic_grounding`` — the bridge only returns a raw list."""
    if provider is None:
        raise ValueError(
            "Grounding scorer bridge requires an LLMProvider — absence is "
            "fail-closed."
        )

    def _factory() -> GrounderFn:
        def _ground(claims: list[Claim], rows: list[dict]) -> list[dict]:
            req = LLMRequest(
                purpose="grounding_score",
                input=_compose_grounding_input(claims, rows),
                board_id=board_id,
                actor_id=actor_id,
                telemetry_labels={"flow": "grounding_score"},
            )
            resp = provider.complete(req)
            _raise_on_failure(resp)
            payload = _payload_list(resp)
            # Non-list OK response -> [] so score_semantic_grounding pads ALL
            # entries with defaults (grounded=False) — its sanitization owns
            # the per-claim shape, unchanged.
            return payload if payload is not None else []

        return _ground

    return _memoize(("ground", id(provider), board_id, actor_id), _factory)


def _compose_grounding_input(claims: list[Claim], rows: list[dict]) -> str:
    """Compose the grounding-scorer prompt. Contract seam: claims + rows have
    no dedicated request field, so they are embedded into ``input``."""
    lines = [
        "For each claim, decide if it is grounded in the retrieved rows.",
        'Return ONLY a JSON array, one object per claim in order: '
        '{"grounded": bool, "confidence": 0..1, "supporting_ids": [row ids]}.',
        "",
        "Claims:",
    ]
    for idx, c in enumerate(claims):
        ents = ", ".join(getattr(c, "mentioned_entities", ()) or ())
        lines.append(f"{idx}. {getattr(c, 'text', '')} [entities: {ents}]")
    lines.append("")
    lines.append("Rows:")
    for r in rows or []:
        nid = str(r.get("node_id", "")) if isinstance(r, dict) else str(r)
        title = r.get("title", "") if isinstance(r, dict) else ""
        lines.append(f"- {nid}: {title}")
    return "\n".join(lines)
