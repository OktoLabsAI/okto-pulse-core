"""LLM protocol used by cognitive heuristics.

Every heuristic asks the LLM a binary question ("is X true about A and B?")
with a short textual justification. A concrete adapter is injected at the
call site so the library does not pull in any vendor SDK transitively —
production picks anthropic/openai, tests use DummyLLM.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class LLMVerdict:
    """Decision returned by the injected LLM.

    - `answer` is the yes/no polarity of the question.
    - `confidence` in [0, 1] is how sure the LLM claims to be. Heuristics
      multiply this by their own ceiling to keep provenance traceable.
    - `reasoning` is the citation text used by BR `Cognitive Edge Evidence
      Required` — must be ≥20 chars.
    """

    answer: bool
    confidence: float
    reasoning: str


class HeuristicLLM(Protocol):
    """Minimal interface a cognitive heuristic needs.

    Implementations may batch calls, cache by payload hash, or add retries
    — the protocol does not constrain those concerns.
    """

    def ask_polarity(
        self,
        *,
        prompt_id: str,
        text_a: str,
        text_b: str,
        context: dict[str, str] | None = None,
    ) -> LLMVerdict:
        """Return the LLM's verdict on whether the polarity claim holds.

        `prompt_id` identifies which heuristic is calling (e.g.
        "contradiction_v1", "supersedence_v1") so adapters can route to the
        right template or audit log.
        """
        ...
