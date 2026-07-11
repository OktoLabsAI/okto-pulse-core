"""Canonical aggregate ownership classification for realm enforcement."""

from __future__ import annotations

from typing import Literal

AggregateOwnership = Literal["global", "tenant"]

# Names are application entities, not adapter table/model names.
AGGREGATE_OWNERSHIP: dict[str, AggregateOwnership] = {
    "activity_log": "tenant",
    "agent": "global",
    "agent_board": "tenant",
    "agent_seen_item": "tenant",
    "amendment_hotfix_revision": "tenant",
    "architecture_design": "tenant",
    "attachment": "tenant",
    "board": "tenant",
    "board_guideline": "tenant",
    "board_share": "tenant",
    "card": "tenant",
    "card_dependency": "tenant",
    "comment": "tenant",
    "guideline": "global",
    "ideation": "tenant",
    "ideation_history": "tenant",
    "ideation_knowledge_base": "tenant",
    "ideation_qa_item": "tenant",
    "ideation_snapshot": "tenant",
    "permission_preset": "global",
    "qa_item": "tenant",
    "refinement": "tenant",
    "refinement_history": "tenant",
    "refinement_knowledge_base": "tenant",
    "refinement_qa_item": "tenant",
    "refinement_snapshot": "tenant",
    "spec": "tenant",
    "spec_history": "tenant",
    "spec_knowledge_base": "tenant",
    "spec_qa_item": "tenant",
    "sprint": "tenant",
    "sprint_history": "tenant",
    "sprint_qa_item": "tenant",
    "story": "tenant",
    "story_ideation_link": "tenant",
    "topic": "tenant",
}

TENANT_OWNED_AGGREGATES = frozenset(
    name for name, ownership in AGGREGATE_OWNERSHIP.items() if ownership == "tenant"
)
GLOBAL_AGGREGATES = frozenset(
    name for name, ownership in AGGREGATE_OWNERSHIP.items() if ownership == "global"
)


def aggregate_ownership(entity: str) -> AggregateOwnership:
    try:
        return AGGREGATE_OWNERSHIP[entity]
    except KeyError as exc:
        raise ValueError(f"unclassified_aggregate:{entity}") from exc


__all__ = [
    "AGGREGATE_OWNERSHIP",
    "AggregateOwnership",
    "GLOBAL_AGGREGATES",
    "TENANT_OWNED_AGGREGATES",
    "aggregate_ownership",
]
