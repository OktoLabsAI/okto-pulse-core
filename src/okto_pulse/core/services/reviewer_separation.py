"""Reviewer/executor separation policy shared by review surfaces.

The board setting is intentionally resolved in one place so sprint evaluation
and task validation cannot drift.  Persisted legacy boards that do not carry
the setting retain their historical permissive behaviour, but the decision is
still explicit and auditable through ``source=legacy_absent_compat``.
"""

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
    if not isinstance(settings, Mapping) or "reviewer_separation_mode" not in settings:
        # Compatibility is explicit: existing boards/templates with no field keep
        # their historical behavior; new templates materialize ``enforce``.
        return "off", "legacy_absent_compat"
    mode = str(settings.get("reviewer_separation_mode") or "off").strip().lower()
    if mode not in REVIEWER_SEPARATION_MODES:
        return "off", "invalid_value_fail_compat"
    return mode, "board_settings"


def evaluate_reviewer_separation(
    *,
    board: object | None,
    reviewer_id: str,
    sprint: object | None = None,
    cards: Sequence[object] = (),
) -> ReviewerSeparationDecision:
    """Evaluate a reviewer against sprint/card authorship and execution facts.

    ``sprint`` is optional so the same policy can govern a single task
    validation.  Card executor facts come from append-only conclusion entries;
    both the canonical ``author_id`` and historical ``actor_id`` forms are
    understood.
    """
    mode, source = resolve_reviewer_separation_mode(board)
    conflicts: list[str] = []
    if (
        sprint is not None
        and reviewer_id
        and reviewer_id == str(getattr(sprint, "created_by", "") or "")
    ):
        conflicts.append("sprint_creator")
    for card in cards:
        if reviewer_id == str(getattr(card, "assignee_id", "") or ""):
            conflicts.append(f"card_assignee:{getattr(card, 'id', '')}")
        if reviewer_id == str(getattr(card, "created_by", "") or ""):
            conflicts.append(f"card_creator:{getattr(card, 'id', '')}")
        for conclusion in getattr(card, "conclusions", None) or ():
            if isinstance(conclusion, Mapping) and reviewer_id == str(
                conclusion.get("author_id")
                or conclusion.get("actor_id")
                or conclusion.get("author_agent_id")
                or conclusion.get("author")
                or conclusion.get("created_by")
                or ""
            ):
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


__all__ = [
    "REVIEWER_SEPARATION_MODES",
    "ReviewerSeparationDecision",
    "evaluate_reviewer_separation",
    "evaluate_task_reviewer_separation",
    "resolve_reviewer_separation_mode",
]
