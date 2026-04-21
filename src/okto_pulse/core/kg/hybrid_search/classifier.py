"""Intent classifier — regex-first, LLM fallback (spec f565115d).

Policy:
    1. Normalise query (lowercase, strip).
    2. Score each intent by keyword overlap (substring match) — highest wins
       when score > 0.
    3. On tie or zero matches, fall back to an injected LLM (optional) that
       MUST choose from the catalog. Absence of LLM ⇒ IntentNotFoundError
       so the caller can surface it properly.

The classifier NEVER invents intents — any proposed name outside the
catalog is rejected (BR `Intent Catalog Closure`).
"""

from __future__ import annotations

import re
from typing import Callable

from .intents import INTENT_CATALOG, IntentNotFoundError, SearchIntent

_NON_WORD = re.compile(r"[^a-z0-9áéíóúãõâêôçà\-_]+", re.IGNORECASE)


def _normalise(query: str) -> str:
    return _NON_WORD.sub(" ", (query or "").strip().lower()).strip()


def _keyword_scores(query: str) -> list[tuple[str, int]]:
    """Count keyword hits per intent. Returns list of (intent_name, score)
    sorted by score DESC. Ties keep catalog order."""
    norm = _normalise(query)
    if not norm:
        return []
    scores: list[tuple[str, int]] = []
    for name, intent in INTENT_CATALOG.items():
        hits = sum(1 for kw in intent.keywords if kw.lower() in norm)
        scores.append((name, hits))
    scores.sort(key=lambda t: (-t[1], t[0]))
    return scores


def classify_intent(
    query: str,
    *,
    llm_fallback: Callable[[str, tuple[str, ...]], str] | None = None,
) -> SearchIntent:
    """Return the best `SearchIntent` for `query`.

    `llm_fallback(query, intent_names) -> str` may be provided. It is
    invoked ONLY when the keyword pass is inconclusive (zero hits or top
    two tie). The returned name must exist in the catalog — otherwise
    `IntentNotFoundError` bubbles up.
    """
    scores = _keyword_scores(query)
    if scores and scores[0][1] > 0:
        top_name, top_score = scores[0]
        # Unambiguous keyword winner.
        if len(scores) < 2 or scores[1][1] < top_score:
            return INTENT_CATALOG[top_name]

    supported = tuple(sorted(INTENT_CATALOG.keys()))
    if llm_fallback is not None:
        chosen = llm_fallback(query, supported)
        if chosen not in INTENT_CATALOG:
            raise IntentNotFoundError(chosen)
        return INTENT_CATALOG[chosen]

    raise IntentNotFoundError(query)
