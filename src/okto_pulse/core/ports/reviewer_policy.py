"""Public, persistence-free reviewer policy boundary for edition adapters.

Adapters provide authenticated identity and authoritative authorship facts.
Core owns the decision; this surface never grants validation or waiver authority.
"""

from typing import Protocol, Sequence

from okto_pulse.core.services.reviewer_separation import (
    ReviewerSeparationDecision,
    evaluate_decision_reviewer_separation,
    resolve_reviewer_separation_mode,
)


class DecisionReviewerPolicyPort(Protocol):
    def __call__(
        self, *, board: object | None, reviewer_id: str,
        author_ids: Sequence[str], authors_known: bool,
        cards: Sequence[object] = (), executor_ids: Sequence[str] = (),
    ) -> ReviewerSeparationDecision:
        """Evaluate current policy from edition-loaded facts, without persistence."""
        ...


__all__ = [
    "DecisionReviewerPolicyPort", "ReviewerSeparationDecision",
    "evaluate_decision_reviewer_separation", "resolve_reviewer_separation_mode",
]
