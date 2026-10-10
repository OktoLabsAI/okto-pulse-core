"""Current reviewer/executor separation policy for task validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence


REVIEWER_SEPARATION_MODES = frozenset({"off", "warn", "enforce"})


@dataclass(frozen=True, slots=True)
class ReviewerSeparationDecision:
    mode: str
    allowed: bool
    warning: bool
    conflicts: tuple[str, ...]
    source: str

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "allowed": self.allowed,
            "warning": self.warning,
            "conflicts": list(self.conflicts),
            "source": self.source,
        }


def resolve_reviewer_separation_mode(board: object | None) -> tuple[str, str]:
    settings = getattr(board, "settings", None) if board is not None else None
    if settings is None:
        settings = {}
    if not isinstance(settings, Mapping):
        raise ValueError("reviewer_separation_policy_invalid")
    if "reviewer_separation_mode" not in settings:
        return "enforce", "board_default"
    mode = settings["reviewer_separation_mode"]
    if not isinstance(mode, str) or mode not in REVIEWER_SEPARATION_MODES:
        raise ValueError("reviewer_separation_policy_invalid")
    return mode, "board_settings"


def evaluate_reviewer_separation(
    *,
    board: object | None,
    reviewer_id: str,
    cards: Sequence[object] = (),
) -> ReviewerSeparationDecision:
    """Evaluate a reviewer against card authorship and execution facts.

    Card executor facts come from native append-only conclusion entries.
    An incompatible record is refused; missing authors must not imply that
    the reviewer is independent.
    """
    mode, source = resolve_reviewer_separation_mode(board)
    conflicts: list[str] = []
    for card in cards:
        if reviewer_id == str(getattr(card, "assignee_id", "") or ""):
            conflicts.append(f"card_assignee:{getattr(card, 'id', '')}")
        if reviewer_id == str(getattr(card, "created_by", "") or ""):
            conflicts.append(f"card_creator:{getattr(card, 'id', '')}")
        conclusions = getattr(card, "conclusions", None)
        if conclusions is None:
            conclusions = ()
        if not isinstance(conclusions, (list, tuple)):
            raise ValueError("reviewer_separation_conclusion_invalid")
        for conclusion in conclusions:
            if (
                not isinstance(conclusion, Mapping)
                or {"actor_id", "author_agent_id", "author", "created_by"}.intersection(conclusion)
                or not isinstance(conclusion.get("author_id"), str)
                or not conclusion["author_id"].strip()
            ):
                raise ValueError("reviewer_separation_conclusion_invalid")
            if reviewer_id == conclusion["author_id"]:
                conflicts.append(f"card_executor:{getattr(card, 'id', '')}")
    unique = tuple(dict.fromkeys(conflicts))
    conflict = bool(unique)
    return ReviewerSeparationDecision(
        mode=mode,
        allowed=not (mode == "enforce" and conflict),
        warning=mode == "warn" and conflict,
        conflicts=unique,
        source=source,
    )


def evaluate_task_reviewer_separation(
    *,
    board: object | None,
    reviewer_id: str,
    card: object,
) -> ReviewerSeparationDecision:
    """Evaluate creator/assignee/executor conflicts for one task validation."""

    return evaluate_reviewer_separation(
        board=board,
        reviewer_id=reviewer_id,
        cards=(card,),
    )


def evaluate_decision_reviewer_separation(*, board, reviewer_id, author_ids, authors_known, cards=(), executor_ids=()):
    """Apply the same explicit Board policy to a direct Decision inspection."""
    base = evaluate_reviewer_separation(board=board, reviewer_id=reviewer_id, cards=cards)
    conflicts = list(base.conflicts)
    if not authors_known:
        conflicts.append("decision_authorship_unknown")
    if reviewer_id in author_ids:
        conflicts.append("decision_author")
    if reviewer_id in executor_ids:
        conflicts.append("decision_scope_executor")
    for card in cards:
        if not isinstance(getattr(card, "created_by", None), str) or not card.created_by.strip():
            conflicts.append(f"card_authorship_unknown:{getattr(card, 'id', '')}")
    return ReviewerSeparationDecision(base.mode, not (base.mode == "enforce" and conflicts),
        bool(base.mode == "warn" and conflicts), tuple(sorted(set(conflicts))), base.source)


__all__ = [
    "REVIEWER_SEPARATION_MODES",
    "ReviewerSeparationDecision",
    "evaluate_reviewer_separation",
    "evaluate_task_reviewer_separation",
    "resolve_reviewer_separation_mode",
]
