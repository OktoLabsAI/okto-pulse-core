"""Cognitive-layer extractors producing Alternative + Learning nodes
(cards 14cd6bd9 + b4df0783, spec f565115d).

These extractors are pure parsers — no LLM required when the pattern is
deterministic (Alternative regex over "## Analysis"), LLM injected for
Learning summarisation of bug action plans.
"""

from .alternatives import AlternativeExtraction, extract_alternatives
from .learnings import (
    LEARNING_MIN_ACTION_PLAN_CHARS,
    LearningExtraction,
    extract_learning_from_bug,
)

__all__ = [
    "AlternativeExtraction",
    "extract_alternatives",
    "LEARNING_MIN_ACTION_PLAN_CHARS",
    "LearningExtraction",
    "extract_learning_from_bug",
]
