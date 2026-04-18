"""Cognitive-layer heuristics (spec f565115d).

Each heuristic consumes a candidate Decision and proposes one cognitive edge
(contradicts | supersedes | depends_on) after (1) vector seed, (2) an
Entity/scope filter, (3) a polarity/prerequisite check by an injected LLM.

All heuristics are pure functions parametrised by `HeuristicLLM` so tests
can plug in a deterministic mock and avoid external calls. Confidence is
capped per BR `Cognitive Fallback Confidence Cap` when operating in the
fallback policy path; otherwise each heuristic publishes its own ceiling.
"""

from .llm_protocol import HeuristicLLM, LLMVerdict
from .contradiction import (
    CONTRADICTS_CEILING,
    ContradictionCandidate,
    run_contradiction_heuristic,
)
from .supersedence import (
    SUPERSEDES_CEILING,
    SupersedenceCandidate,
    run_supersedence_heuristic,
)
from .depends_on import (
    DEPENDS_ON_CEILING,
    DEPENDS_ON_FLOOR,
    DependsOnCandidate,
    run_depends_on_heuristic,
)

__all__ = [
    "HeuristicLLM",
    "LLMVerdict",
    "CONTRADICTS_CEILING",
    "ContradictionCandidate",
    "run_contradiction_heuristic",
    "SUPERSEDES_CEILING",
    "SupersedenceCandidate",
    "run_supersedence_heuristic",
    "DEPENDS_ON_CEILING",
    "DEPENDS_ON_FLOOR",
    "DependsOnCandidate",
    "run_depends_on_heuristic",
]
