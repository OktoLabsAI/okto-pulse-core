"""Bridge adapting an :class:`LLMProvider` (R13-A port) to the cognitive
``LearningSummariser`` used by the closeout extraction (R13-D).

The Learning extractor (``kg/agent/extractors/learnings.py``) takes a
``LearningSummariser`` with ``summarise(*, bug_title, action_plan, context)
-> (title, body)``. When EITHER the title or the body is empty,
``extract_learning_from_bug`` returns ``None`` — no Learning is created. The
RKG-03 handler decides WHETHER to summarise via
``cognitive_extraction._summariser_factory(llm_config)`` with these skips
preserved here:

  * ``no_llm_config``    — falsy ``cognitive_llm_config`` -> ``None`` (caller
    logs ``no_llm_config`` and skips Learning).
  * ``unknown_provider`` — a provider the registry doesn't know -> ``None``
    (caller logs ``unknown_provider``). The legacy ``openai`` deterministic
    stub is preserved by delegating to the existing ``_summariser_factory``.

This module ADDS an ``LLMProvider``-backed summariser WITHOUT modifying the
handler, the ledger boundary, or the worker. On a provider failure (normalized
``is_failure``, a raw exception, OR an empty/unparseable response) the bridge
returns ``("", "")`` — so ``extract_learning_from_bug`` yields no Learning. It
NEVER fabricates cognition without evidence.

Fail-closed wiring: ``make_learning_summariser(None)`` raises ``ValueError``.
Cache identity: memoized per ``(provider, board_id, actor_id)`` (stable id).
Canonical purpose: ``learning_summarise``.
"""

from __future__ import annotations

import json
import threading

from okto_pulse.core.kg.interfaces.llm import (
    LLMProvider,
    LLMRequest,
    LLMResponse,
)

# Reuse the handler's summariser protocol shape + openai stub registry so the
# no_llm_config / unknown_provider / openai-stub semantics are not duplicated.
from .cognitive_extraction import (
    LearningSummariser,
    _summariser_factory as _legacy_summariser_factory,
)

__all__ = [
    "make_learning_summariser",
    "summariser_from_config",
    "reset_bridge_cache",
]

_bridge_cache: dict[tuple, LearningSummariser] = {}
_bridge_lock = threading.Lock()


def _memoize(key: tuple, factory) -> LearningSummariser:
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


def _title_body_from_response(resp: LLMResponse) -> tuple[str, str]:
    """Extract ``(title, body)``. ``("", "")`` on failure/empty so the
    extractor creates no Learning (no fabrication without evidence)."""
    if resp.is_failure:
        return ("", "")
    if isinstance(resp.json, dict):
        return (
            str(resp.json.get("title", "")).strip(),
            str(resp.json.get("body", "")).strip(),
        )
    text = resp.text
    if isinstance(text, str) and text.strip():
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, dict):
            return (
                str(parsed.get("title", "")).strip(),
                str(parsed.get("body", "")).strip(),
            )
        # Plain text: first non-empty line is the title, full text the body.
        stripped = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
        title = stripped[0][:120] if stripped else ""
        return (title, text.strip())
    return ("", "")


class _ProviderLearningSummariser:
    """``LearningSummariser`` backed by an :class:`LLMProvider`."""

    def __init__(self, provider: LLMProvider, board_id, actor_id) -> None:
        self._provider = provider
        self._board_id = board_id
        self._actor_id = actor_id

    def summarise(
        self,
        *,
        bug_title: str,
        action_plan: str,
        context: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        req = LLMRequest(
            purpose="learning_summarise",
            input=_compose_summarise_prompt(bug_title, action_plan, context),
            board_id=self._board_id,
            actor_id=self._actor_id,
            telemetry_labels={"flow": "learning_summarise"},
        )
        try:
            resp = self._provider.complete(req)
        except Exception:  # noqa: BLE001 — no fabrication, no crash
            return ("", "")
        return _title_body_from_response(resp)


def make_learning_summariser(
    provider: LLMProvider,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> LearningSummariser:
    """Adapt ``provider`` to the ``LearningSummariser`` protocol."""
    if provider is None:
        raise ValueError(
            "Learning summariser bridge requires an LLMProvider — absence is "
            "fail-closed."
        )
    return _memoize(
        ("learning", id(provider), board_id, actor_id),
        lambda: _ProviderLearningSummariser(provider, board_id, actor_id),
    )


def summariser_from_config(
    llm_config: dict | None,
    *,
    provider: LLMProvider | None = None,
    board_id: str | None = None,
    actor_id: str | None = None,
) -> LearningSummariser | None:
    """Provider-aware analogue of ``_summariser_factory`` — preserves the
    handler's skip semantics, adding an ``LLMProvider`` path.

    - falsy ``llm_config`` -> ``None`` (``no_llm_config`` skip preserved).
    - ``provider`` supplied -> the ``LLMProvider``-backed summariser.
    - else -> delegate to the legacy ``_summariser_factory`` (openai
      deterministic stub for ``provider="openai"``; ``None`` for an unknown
      provider, i.e. the ``unknown_provider`` skip preserved).
    """
    if not llm_config:
        return None
    if provider is not None:
        return make_learning_summariser(
            provider, board_id=board_id, actor_id=actor_id
        )
    return _legacy_summariser_factory(llm_config)


def _compose_summarise_prompt(
    bug_title: str,
    action_plan: str,
    context: dict[str, str] | None,
) -> str:
    """Compose the summarisation prompt. Contract seam: bug_title/action_plan/
    context have no dedicated request fields, so they go into ``input``."""
    lines = [
        "Generalise this bug fix into a reusable lesson for future developers.",
        'Return ONLY a JSON object: {"title": "...", "body": "..."}.',
        "",
        f"Bug: {bug_title}",
        "Action plan:",
        action_plan,
    ]
    if context:
        for k, v in context.items():
            lines.append(f"- {k}: {v}")
    return "\n".join(lines)
