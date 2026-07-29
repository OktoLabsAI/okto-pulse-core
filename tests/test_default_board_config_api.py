"""Spec 9df814bc / card 7da43521 — administrative API + MCP for default board
configuration (FR7), scenario ts_dc86816b.

REST orchestrator (DefaultBoardConfigApiService) + the MCP twin tools expose the
active template, version history, and the board default-config field-level diff,
with structured errors — sharing the umbrella DefaultBoardConfigurationService.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_default_board_config_api.py
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import delete

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    Board,
    DefaultBoardConfiguration,
    DefaultBoardConfigurationAudit,
)
from okto_pulse.core.models.schemas import BoardCreate, BoardSettings
from okto_pulse.core.services.default_board_config_api import DefaultBoardConfigApiService
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import BoardService

pytestmark = pytest.mark.asyncio

USER_ID = "dbc-api-user"


class _Ctx:
    agent_id = USER_ID
    permissions: list = []
    realm_id = "local"


@pytest_asyncio.fixture(autouse=True)
async def _isolate_committed_global_templates(db_factory):
    async def clear() -> None:
        async with db_factory() as db:
            await db.execute(
                delete(DefaultBoardConfigurationAudit).where(
                    DefaultBoardConfigurationAudit.scope == "global"
                )
            )
            await db.execute(
                delete(DefaultBoardConfiguration).where(
                    DefaultBoardConfiguration.scope == "global"
                )
            )
            await db.execute(delete(Board).where(Board.owner_id == USER_ID))
            await db.commit()

    await clear()
    yield
    await clear()


async def _call(name: str, **kwargs) -> dict:
    from unittest.mock import AsyncMock, patch

    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


async def _seed_versions_and_board(db):
    """v1 active -> board created under v1 with an override -> v2 active."""
    svc = DefaultBoardConfigurationService(db)
    v1 = await svc.create_version(
        settings_payload=BoardSettings(max_scenarios_per_card=3), actor=USER_ID, activate=True
    )
    board = await BoardService(db).create_board(
        USER_ID,
        BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}", settings=BoardSettings(max_scenarios_per_card=8)),
    )
    v2 = await svc.create_version(
        settings_payload=BoardSettings(max_scenarios_per_card=5), actor=USER_ID, activate=True
    )
    return v1, v2, board


async def test_ts_dc86816b_admin_surface_active_history_and_diff():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        v1, v2, board = await _seed_versions_and_board(db)
        api = DefaultBoardConfigApiService(db)

        active = await api.get_active()
        assert active["active"]["id"] == v2.id
        assert active["active"]["version"] == v2.version
        assert active["active"]["status"] == "active"
        assert active["active"]["spec_checklist_mode"] == "advisory"

        versions = await api.list_versions()
        assert versions["active_id"] == v2.id
        assert {v["version"] for v in versions["versions"]} == {1, 2}

        diff = await api.get_board_diff(board_id=board.id)
        assert diff["snapshot_state"] == "applied"
        assert diff["applied_template_version"] == v1.version  # board applied v1
        assert diff["active_template_version"] == v2.version
        assert diff["is_outdated"] is True
        fields = {f["field"]: (f["template_value"], f["current_value"]) for f in diff["fields"]}
        # the override (8) vs the applied template value (3) is a field-level diff.
        assert fields["max_scenarios_per_card"] == (3, 8)
        assert all(f["state"] == "overridden" for f in diff["fields"])

        # structured error for a missing/inaccessible board.
        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await api.get_board_diff(board_id="does-not-exist")
        assert exc.value.code == "board_not_found"


async def test_get_active_none_when_no_template():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        active = await DefaultBoardConfigApiService(db).get_active()
        assert active == {
            "scope": "global",
            "presence": "absent",
            "baseline_available": False,
            "comparable": False,
            "active": None,
        }


async def test_get_active_rejects_corrupt_persisted_checklist_mode():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        db.add(
            DefaultBoardConfiguration(
                id=str(uuid.uuid4()),
                version=1,
                status="active",
                is_active=True,
                scope="global",
                settings_payload={},
                spec_checklist_mode="unsupported",
                created_by=USER_ID,
            )
        )
        await db.commit()

        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await DefaultBoardConfigApiService(db).get_active()

        assert exc.value.code == "invalid_spec_checklist_mode"


async def test_diff_legacy_board_reports_no_snapshot():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        # board created with no active template -> no snapshot (legacy-like).
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        diff = await DefaultBoardConfigApiService(db).get_board_diff(board_id=board.id)
        assert diff["snapshot_state"] == "legacy_no_snapshot"
        assert diff["fields"] == []


async def test_empty_snapshot_is_distinct_from_absent_and_not_comparable():
    board = SimpleNamespace(
        id="board-empty-snapshot",
        default_config_snapshot={},
        settings=BoardSettings().model_dump(mode="json"),
    )
    svc = DefaultBoardConfigurationService(object())

    described = await svc.describe_board_config(board)
    diff = await svc.diff_board_config(board)

    assert described == {
        "state": "empty_snapshot",
        "board_id": board.id,
        "configuration_presence": "empty",
        "baseline_available": False,
        "comparable": False,
    }
    assert diff["snapshot_state"] == "empty_snapshot"
    assert diff["configuration_presence"] == "empty"
    assert diff["comparable"] is False
    assert diff["fields"] == []


async def test_ts_a0d901b3_snapshot_presence_and_empty_template_exact_oracles(
    monkeypatch,
):
    from okto_pulse.core.ports.default_board_configuration import (
        DefaultBoardTemplateRecord,
    )
    from okto_pulse.core.services import default_board_configuration as module

    empty_template = DefaultBoardTemplateRecord(
        id="template-empty",
        version=1,
        status="active",
        is_active=True,
        scope="global",
        settings_payload={},
        guideline_default_refs=[],
        design_system_default_ref=None,
        created_by=USER_ID,
    )

    class _Store:
        async def resolve_active(self, context, *, scope):
            return empty_template

        async def get_template(self, context, *, template_id):
            return empty_template if template_id == empty_template.id else None

    monkeypatch.setattr(
        module,
        "get_default_board_configuration_store",
        lambda: _Store(),
    )
    svc = DefaultBoardConfigurationService(object())
    absent = await svc.describe_board_config(SimpleNamespace(id="absent"))
    null = await svc.describe_board_config(
        SimpleNamespace(id="null", default_config_snapshot=None)
    )
    empty = await svc.describe_board_config(
        SimpleNamespace(id="empty", default_config_snapshot={})
    )
    configured_board = SimpleNamespace(
        id="configured",
        default_config_snapshot={
            "template_id": empty_template.id,
            "template_version": 1,
            "scope": "global",
            "override_summary": {},
        },
        settings=BoardSettings().model_dump(mode="json"),
    )
    configured = await svc.diff_board_config(configured_board)

    assert {
        "absent": (
            absent["configuration_presence"],
            absent["baseline_available"],
            absent["comparable"],
        ),
        "null": (
            null["configuration_presence"],
            null["baseline_available"],
            null["comparable"],
        ),
        "empty": (
            empty["configuration_presence"],
            empty["baseline_available"],
            empty["comparable"],
        ),
        "configured": (
            configured["configuration_presence"],
            configured["baseline_available"],
            configured["comparable"],
        ),
    } == {
        "absent": ("absent", False, False),
        "null": ("null", False, False),
        "empty": ("empty", False, False),
        "configured": ("configured", True, True),
    }
    assert configured["template_settings_presence"] == "empty"
    assert configured["fields"] == []


async def test_mcp_twin_active_versions_and_diff():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        v1, v2, board = await _seed_versions_and_board(db)
        await db.commit()
        ids = {"board": board.id, "v1": v1.version, "v2": v2.version}

    active = await _call("okto_pulse_get_active_default_board_config", board_id=ids["board"])
    assert active["active"]["version"] == ids["v2"]

    versions = await _call("okto_pulse_list_default_board_config_versions", board_id=ids["board"])
    assert {v["version"] for v in versions["versions"]} == {ids["v1"], ids["v2"]}

    diff = await _call("okto_pulse_get_board_default_config_diff", board_id=ids["board"])
    assert diff["snapshot_state"] == "applied"
    assert diff["applied_template_version"] == ids["v1"]

    # structured error twin (board not found), never a raw exception.
    err = await _call("okto_pulse_get_board_default_config_diff", board_id="does-not-exist")
    assert err["code"] == "board_not_found"


async def test_mcp_version_history_uses_bounded_adjacent_pages():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        svc = DefaultBoardConfigurationService(db)
        for index in range(27):
            await svc.create_version(
                settings_payload=BoardSettings(
                    max_scenarios_per_card=2 + (index % 7)
                ),
                actor=USER_ID,
                activate=index == 26,
            )
        await db.commit()
        board_id = board.id

    default_page = await _call(
        "okto_pulse_list_default_board_config_versions",
        board_id=board_id,
    )
    first = await _call(
        "okto_pulse_list_default_board_config_versions",
        board_id=board_id,
        offset=0,
        limit=7,
    )
    second = await _call(
        "okto_pulse_list_default_board_config_versions",
        board_id=board_id,
        offset=7,
        limit=7,
    )
    last = await _call(
        "okto_pulse_list_default_board_config_versions",
        board_id=board_id,
        offset=21,
        limit=7,
    )

    assert default_page["total_count"] == 27
    assert default_page["limit"] == 20
    assert default_page["returned_count"] == 20
    assert default_page["truncated"] is True
    assert default_page["next_offset"] == 20

    assert first["total_count"] == second["total_count"] == last["total_count"] == 27
    assert first["returned_count"] == second["returned_count"] == 7
    assert last["returned_count"] == 6
    assert first["next_offset"] == 7
    assert second["next_offset"] == 14
    assert last["next_offset"] is None
    assert last["has_more"] is False

    first_ids = [item["id"] for item in first["versions"]]
    second_ids = [item["id"] for item in second["versions"]]
    assert not set(first_ids).intersection(second_ids)

    clamped = await _call(
        "okto_pulse_list_default_board_config_versions",
        board_id=board_id,
        limit=500,
    )
    assert clamped["limit"] == 200
    assert clamped["returned_count"] == 27
    assert clamped["truncated"] is False


async def test_mcp_twin_create_and_activate_version():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        await db.commit()
        board_id = board.id

    created = await _call(
        "okto_pulse_create_default_board_config_version",
        board_id=board_id,
        settings_payload={"max_scenarios_per_card": 7},
        activate=True,
    )
    assert created["status"] == "active" and created["is_active"] is True
    assert created["settings_payload"]["max_scenarios_per_card"] == 7

    deactivated = await _call(
        "okto_pulse_deactivate_default_board_config_version",
        board_id=board_id, template_id=created["id"],
    )
    assert deactivated["is_active"] is False and deactivated["status"] == "inactive"
