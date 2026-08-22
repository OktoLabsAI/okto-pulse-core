from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.schemas import (
    BoardSettings,
    FlowHealthSettings,
    FlowHealthSettingsUpdate,
)
from okto_pulse.core.ports.application_persistence import ApplicationRecord
from okto_pulse.core.services.flow_health_read_model import (
    resolve_flow_health_policy,
)
from okto_pulse.core.services.flow_health_settings import (
    FlowHealthSettingsVersionConflict,
    board_settings_with_next_flow_health_policy,
)


def _update(*, expected_version: int = 1) -> FlowHealthSettingsUpdate:
    return FlowHealthSettingsUpdate(
        expected_version=expected_version,
        general_stale_hours=48,
        rejected_stale_hours=80,
        overrides={"in_progress": 24},
    )


def test_board_settings_publish_closed_versioned_flow_health_defaults() -> None:
    policy = BoardSettings().analytics.flow_health

    assert policy.model_dump(mode="json") == {
        "version": 1,
        "general_stale_hours": 72,
        "rejected_stale_hours": 96,
        "overrides": {},
    }
    with pytest.raises(ValidationError, match="extra_forbidden"):
        FlowHealthSettings.model_validate({"unknown_threshold": 10})
    with pytest.raises(ValidationError):
        FlowHealthSettings.model_validate({"general_stale_hours": "72"})
    with pytest.raises(ValidationError, match="unsupported flow_health override"):
        FlowHealthSettings.model_validate({"overrides": {"cancelled": 10}})


def test_successor_policy_preserves_the_complete_board_settings_document() -> None:
    persisted = {
        "skip_test_coverage_global": True,
        "skip_code_evidence_coverage_global": True,
        "allow_agent_self_answering": True,
        "analytics": {
            "version": 1,
            "flow_health": {
                "version": 1,
                "general_stale_hours": 72,
                "rejected_stale_hours": 96,
                "overrides": {},
            },
        },
    }

    root, successor = board_settings_with_next_flow_health_policy(
        persisted,
        expected_version=1,
        update=_update(),
    )

    assert successor.version == 2
    assert successor.general_stale_hours == 48
    assert root.skip_test_coverage_global is True
    assert root.skip_code_evidence_coverage_global is True
    assert root.allow_agent_self_answering is True


def test_restore_uses_defaults_and_advances_revision() -> None:
    root, successor = board_settings_with_next_flow_health_policy(
        {
            "analytics": {
                "version": 1,
                "flow_health": {
                    "version": 4,
                    "general_stale_hours": 12,
                    "rejected_stale_hours": 18,
                    "overrides": {"pending": 6},
                },
            }
        },
        expected_version=4,
        update=None,
    )

    assert successor.model_dump(mode="json") == {
        "version": 5,
        "general_stale_hours": 72,
        "rejected_stale_hours": 96,
        "overrides": {},
    }
    assert root.analytics.flow_health == successor


def test_flow_projection_resolves_the_typed_nested_policy() -> None:
    policy = resolve_flow_health_policy(
        SimpleNamespace(
            id="board-1",
            settings={
                "analytics": {
                    "version": 1,
                    "flow_health": {
                        "version": 3,
                        "general_stale_hours": 48,
                        "rejected_stale_hours": 80,
                        "overrides": {"pending": 12},
                    },
                }
            },
        )
    )

    assert policy.version == 3
    assert policy.general_stale_hours == 48
    assert policy.rejected_stale_hours == 80
    assert [(item.state.value, item.stale_hours) for item in policy.overrides] == [
        ("pending", 12)
    ]
    assert policy.authority_ref == (
        "board:board-1:settings:analytics:flow-health:v3"
    )


@pytest.mark.asyncio
async def test_board_service_write_fences_the_full_settings_document(monkeypatch) -> None:
    from okto_pulse.core.services import main as main_service

    persisted = {
        "skip_test_coverage_global": True,
        "analytics": {
            "version": 1,
            "flow_health": {
                "version": 1,
                "general_stale_hours": 72,
                "rejected_stale_hours": 96,
                "overrides": {},
            },
        },
    }
    board = ApplicationRecord(
        "board",
        {"id": "board-1", "owner_id": "user-1", "settings": persisted},
    )
    service = main_service.BoardService(object())
    service.get_board = AsyncMock(return_value=board)
    service._log_activity = AsyncMock()
    fence = AsyncMock(return_value=True)
    monkeypatch.setattr(main_service, "_application_fence", fence)
    monkeypatch.setattr(
        main_service,
        "resolve_actor_name",
        AsyncMock(return_value="User One"),
    )

    successor = await service.compare_and_swap_flow_health_settings(
        "board-1",
        "user-1",
        expected_version=1,
        update=_update(),
    )

    assert successor.version == 2
    assert board.settings["skip_test_coverage_global"] is True
    assert board.settings["analytics"]["flow_health"]["version"] == 2
    fence.assert_awaited_once_with(
        service.db,
        "board",
        "board-1",
        expected_values={"settings": persisted},
    )


@pytest.mark.asyncio
async def test_board_service_lost_update_race_fails_before_mutation(monkeypatch) -> None:
    from okto_pulse.core.services import main as main_service

    stale_settings = {
        "skip_rules_coverage_global": True,
        "analytics": {
            "version": 1,
            "flow_health": {
                "version": 1,
                "general_stale_hours": 72,
                "rejected_stale_hours": 96,
                "overrides": {},
            },
        },
    }
    stale = ApplicationRecord(
        "board",
        {"id": "board-1", "owner_id": "user-1", "settings": stale_settings},
    )
    latest = ApplicationRecord(
        "board",
        {
            "id": "board-1",
            "owner_id": "user-1",
            "settings": {
                **stale_settings,
                "analytics": {
                    "version": 1,
                    "flow_health": {
                        "version": 2,
                        "general_stale_hours": 60,
                        "rejected_stale_hours": 90,
                        "overrides": {},
                    },
                },
            },
        },
    )
    service = main_service.BoardService(object())
    service.get_board = AsyncMock(return_value=stale)
    service._log_activity = AsyncMock()
    monkeypatch.setattr(
        main_service,
        "_application_fence",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        main_service,
        "_application_get",
        AsyncMock(return_value=latest),
    )

    with pytest.raises(FlowHealthSettingsVersionConflict) as raised:
        await service.compare_and_swap_flow_health_settings(
            "board-1",
            "user-1",
            expected_version=1,
            update=_update(),
        )

    assert raised.value.current_version == 2
    assert stale.settings == stale_settings
    assert stale.dirty_fields == set()
    service._log_activity.assert_not_awaited()
