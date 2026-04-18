"""Extract Learning nodes from `done` bug cards with action plans
(card b4df0783, spec f565115d).

Pipeline:
    1. Filter: card.card_type == "bug" AND status == "done" AND
       len(action_plan) >= LEARNING_MIN_ACTION_PLAN_CHARS (default 50).
    2. LLM summarisation (injected): "generalise this action plan into a
       rule a future developer can apply".
    3. Emit Learning + `validates` edge to the Bug node.

Learning summarisation is LLM-driven because mechanical extraction loses
nuance; TR `tr_5f83925b` calls out this requirement. The function is pure
with an injected LLM protocol so tests stay deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

LEARNING_MIN_ACTION_PLAN_CHARS = 50
LEARNING_DEFAULT_CONFIDENCE = 0.9


class LearningSummariser(Protocol):
    """Summarises a bug's action plan into a generic lesson.

    Returns a tuple of (learning_title, learning_body). The adapter owns
    prompt engineering; the extractor owns filter + shape.
    """

    def summarise(
        self, *, bug_title: str, action_plan: str, context: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        ...


@dataclass(frozen=True)
class LearningExtraction:
    bug_node_id: str
    learning_title: str
    learning_body: str
    confidence: float
    cognitive_evidence: str
    # Optional reinforcement hint: when the action plan references a BR id,
    # callers can wire a validates→Constraint link via this field.
    linked_constraint_hint: str | None = None


def extract_learning_from_bug(
    *,
    bug_node_id: str,
    bug_title: str,
    bug_status: str,
    card_type: str,
    action_plan: str,
    summariser: LearningSummariser,
    min_action_plan_chars: int = LEARNING_MIN_ACTION_PLAN_CHARS,
    confidence: float = LEARNING_DEFAULT_CONFIDENCE,
    linked_constraint_hint: str | None = None,
) -> LearningExtraction | None:
    """Return a LearningExtraction or None if the bug does not qualify.

    The filter mirrors BR `Cognitive Edge Evidence Required` — the
    `action_plan` text itself becomes the cognitive_evidence so reviewers
    can trace the learning back to the concrete fix.
    """
    if card_type != "bug":
        return None
    if bug_status != "done":
        return None
    plan = (action_plan or "").strip()
    if len(plan) < min_action_plan_chars:
        return None

    title, body = summariser.summarise(
        bug_title=bug_title,
        action_plan=plan,
        context={"bug_node_id": bug_node_id},
    )
    if not title or not body:
        return None

    return LearningExtraction(
        bug_node_id=bug_node_id,
        learning_title=title.strip()[:120],
        learning_body=body.strip(),
        confidence=confidence,
        cognitive_evidence=plan[:500],  # first 500 chars as citation
        linked_constraint_hint=linked_constraint_hint,
    )
