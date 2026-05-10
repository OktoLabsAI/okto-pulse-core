"""Permission helpers for Stories and Topics."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


STORY_MOVE_PERMISSIONS: dict[tuple[str, str], str] = {
    ("draft", "triage"): "story.move.draft_to_triage",
    ("draft", "ready"): "story.move.draft_to_ready",
    ("triage", "draft"): "story.move.triage_to_draft",
    ("triage", "ready"): "story.move.triage_to_ready",
    ("ready", "triage"): "story.move.ready_to_triage",
}

STORY_EDIT_FIELDS = {
    "title",
    "description",
    "topic_id",
    "actor",
    "goal",
    "benefit",
    "screen_mockups",
}

TOPIC_EDIT_FIELDS = {
    "name",
    "description",
}


def status_value(status: Any) -> str:
    """Return enum/string status as its serialized value."""
    return getattr(status, "value", status)


def story_state(status: Any, *, archived: bool = False) -> str:
    """Return the state used by story.interact_in.* checks."""
    return "archived" if archived else str(status_value(status))


def story_move_permission(current_status: Any, target_status: Any) -> str | None:
    """Return the permission flag required for a Story lifecycle transition."""
    current = str(status_value(current_status))
    target = str(status_value(target_status))
    if current == target:
        return None
    return STORY_MOVE_PERMISSIONS.get((current, target))


def story_update_permissions(update_data: Mapping[str, Any]) -> list[str]:
    """Return permission flags required by a Story partial update payload."""
    permissions: list[str] = []
    if any(field in update_data for field in STORY_EDIT_FIELDS):
        permissions.append("story.entity.edit_fields")
    if "assignee_id" in update_data:
        permissions.append("story.entity.assign")
    if "labels" in update_data:
        permissions.append("story.entity.label")
    return permissions


def topic_update_permissions(update_data: Mapping[str, Any], *, current_archived: bool = False) -> list[str]:
    """Return permission flags required by a Topic partial update payload."""
    permissions: list[str] = []
    if any(field in update_data for field in TOPIC_EDIT_FIELDS):
        permissions.append("topic.entity.edit_fields")
    if "archived" in update_data and update_data["archived"] is not None:
        requested_archived = bool(update_data["archived"])
        if requested_archived and not current_archived:
            permissions.append("topic.entity.archive")
        elif not requested_archived and current_archived:
            permissions.append("topic.entity.restore")
    return permissions
