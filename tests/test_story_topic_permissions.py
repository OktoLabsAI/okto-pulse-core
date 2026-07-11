"""Permission mapping tests for Stories and Topics."""

from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

from okto_pulse.community.api import stories as stories_api
from okto_pulse.core.application.use_cases import PermissionDeniedError
from okto_pulse.core.application.use_cases import stories_crud
from okto_pulse.core.infra.permissions import PermissionSet
from okto_pulse.core.services import main as services_main
from sqlalchemy_test_models import StoryStatus
from okto_pulse.core.mcp.server import (
    _mcp_check_story_state_permission,
    _mcp_permission_error_response,
)
from okto_pulse.core.services.story_permissions import (
    story_move_permission,
    story_state,
    story_update_permissions,
    topic_update_permissions,
)


class StoryStub:
    def __init__(self, status: StoryStatus = StoryStatus.DRAFT, archived: bool = False):
        self.status = status
        self.archived = archived


def test_story_move_permission_maps_valid_transitions() -> None:
    assert story_move_permission(StoryStatus.DRAFT, StoryStatus.TRIAGE) == "story.move.draft_to_triage"
    assert story_move_permission(StoryStatus.DRAFT, StoryStatus.READY) == "story.move.draft_to_ready"
    assert story_move_permission(StoryStatus.TRIAGE, StoryStatus.DRAFT) == "story.move.triage_to_draft"
    assert story_move_permission(StoryStatus.TRIAGE, StoryStatus.READY) == "story.move.triage_to_ready"
    assert story_move_permission(StoryStatus.READY, StoryStatus.TRIAGE) == "story.move.ready_to_triage"


def test_story_move_permission_ignores_noop_and_unknown_transitions() -> None:
    assert story_move_permission(StoryStatus.DRAFT, StoryStatus.DRAFT) is None
    assert story_move_permission(StoryStatus.READY, StoryStatus.CONVERTED) is None
    assert story_move_permission(StoryStatus.CONVERTED, StoryStatus.READY) is None


def test_story_update_permissions_split_fields_assignment_and_labels() -> None:
    permissions = story_update_permissions(
        {
            "title": "New title",
            "assignee_id": "agent-1",
            "labels": ["resource-gate"],
        }
    )

    assert permissions == [
        "story.entity.edit_fields",
        "story.entity.assign",
        "story.entity.label",
    ]


def test_topic_update_permissions_split_edit_archive_restore() -> None:
    assert topic_update_permissions({"name": "Resource Gate"}) == ["topic.entity.edit_fields"]
    assert topic_update_permissions({"archived": True}, current_archived=False) == ["topic.entity.archive"]
    assert topic_update_permissions({"archived": False}, current_archived=True) == ["topic.entity.restore"]
    assert topic_update_permissions({"archived": True}, current_archived=True) == []


def test_story_state_uses_archived_interaction_bucket() -> None:
    assert story_state(StoryStatus.READY, archived=False) == "ready"
    assert story_state(StoryStatus.READY, archived=True) == "archived"


def test_mcp_story_state_permission_reports_missing_flag_context() -> None:
    flags = {
        "story": {
            "move": {"draft_to_ready": False},
            "interact_in": {"draft": True},
        }
    }
    error = _mcp_check_story_state_permission(
        PermissionSet(flags),
        "story.move.draft_to_ready",
        StoryStub(StoryStatus.DRAFT),
    )

    payload = json.loads(error or "{}")
    assert payload["error"] == "Permission denied"
    assert payload["reason"] == "permission_missing"
    assert payload["required_permission"] == "story.move.draft_to_ready"


def test_mcp_story_state_permission_reports_interaction_context() -> None:
    flags = {
        "story": {
            "move": {"draft_to_ready": True},
            "interact_in": {"draft": False},
        }
    }
    error = _mcp_check_story_state_permission(
        PermissionSet(flags),
        "story.move.draft_to_ready",
        StoryStub(StoryStatus.DRAFT),
    )

    payload = json.loads(error or "{}")
    assert payload["error"] == "Permission denied"
    assert payload["reason"] == "interact_in_blocked"
    assert payload["required_permission"] == "story.interact_in.draft"
    assert payload["current_state"] == "draft"


def test_mcp_permission_error_response_preserves_structured_payload() -> None:
    response = _mcp_permission_error_response(
        json.dumps(
            {
                "error": "Permission denied",
                "reason": "permission_missing",
                "required_permission": "topic.entity.create",
            }
        )
    )

    assert json.loads(response) == {
        "error": "Permission denied",
        "reason": "permission_missing",
        "required_permission": "topic.entity.create",
    }


# Since R01A REST-FU6-S2 the api-level guard is gone: the structured-403
# contract spans the transport-free guard (stories_crud._require_permissions
# raises PermissionDeniedError carrying the JSON payload) plus the adapter
# mapping (stories_api._raise_permission_denied json-decodes it into the 403
# detail). resolve_user_permissions is imported at call time inside the guard,
# so the patch targets the services.main module attribute.
@pytest.mark.asyncio
async def test_api_require_permissions_reports_story_create_context(monkeypatch) -> None:
    async def fake_permissions(_db, _user_id, _board_id):
        return PermissionSet({"story": {"entity": {"create": False}}})

    monkeypatch.setattr(services_main, "resolve_user_permissions", fake_permissions)

    with pytest.raises(PermissionDeniedError) as denied:
        await stories_crud._require_permissions(None, "user-1", "board-1", "story.entity.create")

    with pytest.raises(HTTPException) as excinfo:
        stories_api._raise_permission_denied(denied.value.message)

    assert excinfo.value.status_code == 403
    assert excinfo.value.detail["reason"] == "permission_missing"
    assert excinfo.value.detail["required_permission"] == "story.entity.create"


@pytest.mark.asyncio
async def test_api_require_permissions_reports_topic_sensitive_context(monkeypatch) -> None:
    async def fake_permissions(_db, _user_id, _board_id):
        return PermissionSet({"topic": {"entity": {"merge": False, "delete": False}}})

    monkeypatch.setattr(services_main, "resolve_user_permissions", fake_permissions)

    with pytest.raises(PermissionDeniedError) as denied:
        await stories_crud._require_permissions(None, "user-1", "board-1", "topic.entity.merge")

    with pytest.raises(HTTPException) as excinfo:
        stories_api._raise_permission_denied(denied.value.message)

    assert excinfo.value.status_code == 403
    assert excinfo.value.detail["reason"] == "permission_missing"
    assert excinfo.value.detail["required_permission"] == "topic.entity.merge"
