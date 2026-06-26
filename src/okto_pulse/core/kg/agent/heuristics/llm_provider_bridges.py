"""Bridge adapting an :class:`LLMProvider` (R13-A port) to the cognitive
heuristic ``HeuristicLLM`` protocol (R13-D).

Heuristics (contradiction / supersedence / depends_on) ask the LLM a single
binary polarity question via ``HeuristicLLM.ask_polarity(prompt_id, text_a,
text_b, context) -> LLMVerdict`` (``answer: bool``, ``confidence in [0,1]``,
``reasoning`` — an auditable citation). The heuristics gate emission on a
confidence FLOOR, so a low-confidence verdict never produces an edge.

Failure semantics — the heuristic consumers call ``ask_polarity`` WITHOUT a
try/except, so this bridge must NOT raise: on a normalized provider failure
(``LLMResponse.is_failure``), a raw provider exception, OR an unparseable OK
response, it returns a FAIL-CLOSED verdict ``answer=False, confidence=0.0`` with
an auditable secret-free reasoning. With ``confidence=0.0`` (below every
heuristic floor) NO cognitive edge is emitted — i.e. no cognitive creation
without evidence.

Fail-closed wiring: ``make_heuristic_llm(None)`` raises ``ValueError``.
Cache identity: memoized per ``(provider, board_id, actor_id)`` (stable id).
Canonical purpose: ``heuristic_polarity``.

No vendor SDK is imported here — the concrete provider is injected.
"""

from __future__ import annotations

import json
import threading

from okto_pulse.core.kg.interfaces.llm import (
    LLMProvider,
    LLMRequest,
    LLMResponse,
)

from .llm_protocol import HeuristicLLM, LLMVerdict

__all__ = ["make_heuristic_llm", "reset_bridge_cache"]

_FAIL_CLOSED_REASONING = (
    "LLM provider unavailable; polarity unverified (fail-closed, no edge)."
)

_bridge_cache: dict[tuple, HeuristicLLM] = {}
_bridge_lock = threading.Lock()


def _memoize(key: tuple, factory) -> HeuristicLLM:
    with _bridge_lock:
        obj = _bridge_cache.get(key)
        if obj is None:
            obj = factory()
            _bridge_cache[key] = obj
        return obj


def reset_bridge_cache() -> None:
    """Drop memoized bridge objects. Call in tests or when a wiring change
    requires fresh identities."""
    with _bridge_lock:
        _bridge_cache.clear()


def _clamp01(value) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    if f < 0.0:
        return 0.0
    if f > 1.0:
        return 1.0
    return f


def _fail_closed_verdict() -> LLMVerdict:
    return LLMVerdict(
        answer=False, confidence=0.0, reasoning=_FAIL_CLOSED_REASONING
    )


def _verdict_from_response(resp: LLMResponse) -> LLMVerdict:
    data = resp.json if isinstance(resp.json, dict) else None
    if data is None and isinstance(resp.text, str) and resp.text.strip():
        try:
            parsed = json.loads(resp.text)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, dict):
            data = parsed
    if data is None:
        # OK response but no usable verdict -> fail-closed (no edge emitted).
        return _fail_closed_verdict()
    answer = data.get("answer")
    if not isinstance(answer, bool):
        # SAFETY: `answer` must be a REAL bool. A structurally invalid value
        # (e.g. the string "false", which `bool(...)` would coerce to True, or a
        # number) is a malformed verdict -> fail-closed, NEVER emit an edge from
        # a coerced truthy value.
        return _fail_closed_verdict()
    reasoning = str(data.get("reasoning", "")).strip()
    if not reasoning:
        reasoning = "Provider polarity verdict (no reasoning text returned)."
    return LLMVerdict(
        answer=answer,
        confidence=_clamp01(data.get("confidence", 0.0)),
        reasoning=reasoning,
    )


class _ProviderHeuristicLLM:
    """``HeuristicLLM`` backed by an :class:`LLMProvider`."""

    def __init__(self, provider: LLMProvider, board_id, actor_id) -> None:
        self._provider = provider
        self._board_id = board_id
        self._actor_id = actor_id

    def ask_polarity(
        self,
        *,
        prompt_id: str,
        text_a: str,
        text_b: str,
        context: dict[str, str] | None = None,
    ) -> LLMVerdict:
        req = LLMRequest(
            purpose="heuristic_polarity",
            prompt_id=prompt_id,
            input=_compose_polarity_prompt(prompt_id, text_a, text_b, context),
            board_id=self._board_id,
            actor_id=self._actor_id,
            telemetry_labels={"flow": "heuristic_polarity"},
        )
        try:
            resp = self._provider.complete(req)
        except Exception:  # noqa: BLE001 — consumers don't catch; fail-closed
            return _fail_closed_verdict()
        if resp.is_failure:
            return _fail_closed_verdict()
        return _verdict_from_response(resp)


def make_heuristic_llm(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> HeuristicLLM:
    """Adapt ``provider`` to the ``HeuristicLLM`` protocol."""
    if provider is None:
        raise ValueError(
            "Heuristic LLM bridge requires an LLMProvider — absence is "
            "fail-closed."
        )
    return _memoize(
        ("heuristic", id(provider), board_id, actor_id),
        lambda: _ProviderHeuristicLLM(provider, board_id, actor_id),
    )


def _compose_polarity_prompt(
    prompt_id: str,
    text_a: str,
    text_b: str,
    context: dict[str, str] | None,
) -> str:
    """Compose the polarity prompt. Contract seam: text_a/text_b/context have
    no dedicated request fields, so they are embedded into ``input``."""
    lines = [
        f"Polarity check ({prompt_id}).",
        'Return ONLY a JSON object: '
        '{"answer": bool, "confidence": 0..1, "reasoning": "..."}.',
        "",
        f"A: {text_a}",
        f"B: {text_b}",
    ]
    if context:
        for k, v in context.items():
            lines.append(f"- {k}: {v}")
    return "\n".join(lines)
