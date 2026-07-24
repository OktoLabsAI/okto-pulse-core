"""Design System detail operations are owner- and board-scoped fail-closed."""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete, func, select

from mcp_runtime_testing import register_mcp_test_runtime
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.design_systems import router as design_systems_router
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.services.design_system import DesignSystemService
from sqlalchemy_test_models import (
    Board,
    BoardDesignSystem,
    BoardShare,
    DesignSystem,
    DesignSystemGateAudit,
)

PREFIX = "/api/v1"


class _Ctx:
    permissions: list = []

    def __init__(self, actor_id: str) -> None:
        self.agent_id = actor_id


def _rest_client(actor_id: str) -> TestClient:
    app = FastAPI()
    app.include_router(design_systems_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_uow():
        async with session_factory() as session:
            try:
                yield resolve_unit_of_work_factory().wrap(session)
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    app.dependency_overrides[get_unit_of_work] = _override_uow
    app.dependency_overrides[require_user] = lambda: actor_id
    return TestClient(app)


async def _mcp_call(actor_id: str, name: str, **kwargs) -> dict:
    register_mcp_test_runtime(get_session_factory())
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=_Ctx(actor_id)),
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


async def _seed() -> dict[str, str]:
    ids = {
        "owner_a": f"ds-owner-a-{uuid.uuid4().hex[:8]}",
        "owner_b": f"ds-owner-b-{uuid.uuid4().hex[:8]}",
        "board_a": str(uuid.uuid4()),
        "board_b": str(uuid.uuid4()),
    }
    async with get_session_factory()() as db:
        db.add_all(
            [
                Board(
                    id=ids["board_a"],
                    name="DS scope A",
                    owner_id=ids["owner_a"],
                    realm_id="local",
                ),
                Board(
                    id=ids["board_b"],
                    name="DS scope B",
                    owner_id=ids["owner_b"],
                    realm_id="local",
                ),
            ]
        )
        await db.flush()
        service = DesignSystemService(db)
        global_a = await service.create_design_system(
            ids["owner_a"],
            title="Global A",
            payload={"owner": "a"},
        )
        global_b = await service.create_design_system(
            ids["owner_b"],
            title="Global B",
            payload={"owner": "b"},
        )
        inline_a = await service.create_design_system(
            ids["owner_a"],
            title="Inline A",
            scope="inline",
            board_id=ids["board_a"],
            payload={"inline": "a"},
        )
        await service.link_design_system_to_board(ids["board_a"], global_a.id)
        await service.link_design_system_to_board(ids["board_b"], global_b.id)
        ids.update(
            {
                "global_a": global_a.id,
                "global_b": global_b.id,
                "inline_a": inline_a.id,
            }
        )
        await db.commit()
    return ids


async def _state(ids: dict[str, str]) -> tuple[dict, dict, int]:
    async with get_session_factory()() as db:
        rows = (
            (
                await db.execute(
                    select(DesignSystem).where(
                        DesignSystem.id.in_(
                            [ids["global_a"], ids["global_b"], ids["inline_a"]]
                        )
                    )
                )
            )
            .scalars()
            .all()
        )
        systems = {
            row.id: (
                row.scope,
                row.board_id,
                row.title,
                row.payload,
                row.version,
                row.status,
                row.owner_id,
            )
            for row in rows
        }
        links = {
            row.board_id: (row.design_system_id, row.design_system_version)
            for row in (
                await db.execute(
                    select(BoardDesignSystem).where(
                        BoardDesignSystem.board_id.in_([ids["board_a"], ids["board_b"]])
                    )
                )
            ).scalars()
        }
        audit_count = int(
            await db.scalar(select(func.count()).select_from(DesignSystemGateAudit))
            or 0
        )
        return systems, links, audit_count


async def _cleanup(ids: dict[str, str]) -> None:
    async with get_session_factory()() as db:
        await db.execute(
            delete(DesignSystemGateAudit).where(
                DesignSystemGateAudit.board_id.in_([ids["board_a"], ids["board_b"]])
            )
        )
        await db.execute(
            delete(BoardDesignSystem).where(
                BoardDesignSystem.board_id.in_([ids["board_a"], ids["board_b"]])
            )
        )
        await db.execute(
            delete(DesignSystem).where(
                DesignSystem.owner_id.in_(
                    [value for key, value in ids.items() if key.startswith("owner_")]
                )
            )
        )
        await db.execute(
            delete(BoardShare).where(
                BoardShare.board_id.in_([ids["board_a"], ids["board_b"]])
            )
        )
        await db.execute(
            delete(Board).where(Board.id.in_([ids["board_a"], ids["board_b"]]))
        )
        await db.commit()


def _assert_rest_not_found(response) -> None:
    assert response.status_code == 404, response.text
    detail = response.json()["detail"]
    assert detail["error"] == "design_system_not_found"
    assert detail["code"] == "design_system_not_found"
    assert detail["status_code"] == 404


def _assert_mcp_not_found(result: dict) -> None:
    assert result["error"] == "design_system_not_found"
    assert result["code"] == "design_system_not_found"
    assert result["status_code"] == 404


@pytest.mark.asyncio
async def test_mcp_design_system_id_operations_hide_cross_owner_board_and_missing():
    ids = await _seed()
    try:
        happy = await _mcp_call(
            ids["owner_a"],
            "okto_pulse_get_design_system",
            board_id=ids["board_a"],
            design_system_id=ids["global_a"],
        )
        assert happy["id"] == ids["global_a"]

        updated = await _mcp_call(
            ids["owner_a"],
            "okto_pulse_update_design_system",
            board_id=ids["board_a"],
            design_system_id=ids["global_a"],
            payload={"owner": "a", "updated": True},
        )
        assert updated["version"] == 2
        baseline = await _state(ids)

        probes = [
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_get_design_system",
                board_id=ids["board_a"],
                design_system_id=ids["global_b"],
            ),
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_get_design_system",
                board_id=ids["board_b"],
                design_system_id=ids["global_a"],
            ),
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_get_design_system",
                board_id=ids["board_b"],
                design_system_id=ids["inline_a"],
            ),
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_get_design_system",
                board_id=ids["board_a"],
                design_system_id="missing-design-system",
            ),
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_update_design_system",
                board_id=ids["board_a"],
                design_system_id=ids["global_b"],
                payload={"stolen": True},
            ),
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_update_design_system",
                board_id=ids["board_b"],
                design_system_id=ids["global_a"],
                title="Cross-board write",
            ),
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_delete_design_system",
                board_id=ids["board_a"],
                design_system_id=ids["global_b"],
            ),
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_delete_design_system",
                board_id=ids["board_b"],
                design_system_id=ids["global_a"],
            ),
            await _mcp_call(
                ids["owner_a"],
                "okto_pulse_delete_design_system",
                board_id=ids["board_a"],
                design_system_id="missing-design-system",
            ),
        ]
        for probe in probes:
            _assert_mcp_not_found(probe)
        assert await _state(ids) == baseline

        deleted = await _mcp_call(
            ids["owner_a"],
            "okto_pulse_delete_design_system",
            board_id=ids["board_a"],
            design_system_id=ids["inline_a"],
        )
        assert deleted == {"deleted": True, "id": ids["inline_a"]}
    finally:
        await _cleanup(ids)


@pytest.mark.asyncio
async def test_catalog_lists_exports_import_duplicates_and_mcp_pages_are_owner_scoped():
    ids = await _seed()
    client_a = _rest_client(ids["owner_a"])
    client_b = _rest_client(ids["owner_b"])
    try:
        async with get_session_factory()() as db:
            extra = await DesignSystemService(db).create_design_system(
                ids["owner_a"],
                title="Global A second",
                payload={"owner": "a", "ordinal": 2},
            )
            await db.commit()

        listed_a = client_a.get(f"{PREFIX}/design-systems")
        assert listed_a.status_code == 200, listed_a.text
        assert {item["id"] for item in listed_a.json()["items"]} == {
            ids["global_a"],
            extra.id,
        }
        listed_b = client_b.get(f"{PREFIX}/design-systems")
        assert listed_b.status_code == 200, listed_b.text
        assert {item["id"] for item in listed_b.json()["items"]} == {ids["global_b"]}

        exported_a = client_a.get(f"{PREFIX}/design-systems/export")
        assert exported_a.status_code == 200, exported_a.text
        assert {item["title"] for item in exported_a.json()["items"]} == {
            "Global A",
            "Global A second",
        }

        first_page = await _mcp_call(
            ids["owner_a"],
            "okto_pulse_list_design_systems",
            board_id=ids["board_a"],
            limit=1,
        )
        assert first_page["count"] == 1
        assert first_page["items"][0]["owner_id"] == ids["owner_a"]
        assert first_page["next_cursor"]
        second_page = await _mcp_call(
            ids["owner_a"],
            "okto_pulse_list_design_systems",
            board_id=ids["board_a"],
            limit=1,
            cursor=first_page["next_cursor"],
        )
        assert second_page["count"] == 1
        assert second_page["items"][0]["owner_id"] == ids["owner_a"]
        assert second_page["next_cursor"] is None
        assert {
            first_page["items"][0]["id"],
            second_page["items"][0]["id"],
        } == {ids["global_a"], extra.id}

        # Another owner's equal title is not a duplicate in this actor's
        # catalog partition and must be recreated through the normal writer.
        imported = client_b.post(
            f"{PREFIX}/design-systems/import",
            json={
                "schema_version": "1",
                "kind": "design_systems",
                "items": [
                    {
                        "title": "Global A",
                        "scope": "global",
                        "board_id": None,
                        "payload": {"owner": "b", "same_title": True},
                        "status": "active",
                    }
                ],
            },
        )
        assert imported.status_code == 200, imported.text
        assert imported.json()["created"] == 1
        assert imported.json()["skipped"] == []
    finally:
        client_a.close()
        client_b.close()
        await _cleanup(ids)


@pytest.mark.asyncio
async def test_board_design_system_reads_allow_viewer_and_writes_require_editor():
    ids = await _seed()
    ids["owner_viewer"] = f"ds-viewer-{uuid.uuid4().hex[:8]}"
    ids["owner_editor"] = f"ds-editor-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add_all(
            [
                BoardShare(
                    board_id=ids["board_a"],
                    user_id=ids["owner_viewer"],
                    realm_id="local",
                    permission="viewer",
                    shared_by=ids["owner_a"],
                ),
                BoardShare(
                    board_id=ids["board_a"],
                    user_id=ids["owner_editor"],
                    realm_id="local",
                    permission="editor",
                    shared_by=ids["owner_a"],
                ),
            ]
        )
        await db.commit()

    owner = _rest_client(ids["owner_a"])
    viewer = _rest_client(ids["owner_viewer"])
    editor = _rest_client(ids["owner_editor"])
    outsider = _rest_client(ids["owner_b"])
    try:
        viewer_inline = viewer.get(
            f"{PREFIX}/design-systems",
            params={"scope": "inline", "board_id": ids["board_a"]},
        )
        assert viewer_inline.status_code == 200, viewer_inline.text
        assert {item["id"] for item in viewer_inline.json()["items"]} == {
            ids["inline_a"]
        }
        viewer_effective = viewer.get(f"{PREFIX}/boards/{ids['board_a']}/design-system")
        assert viewer_effective.status_code == 200, viewer_effective.text
        assert (
            viewer_effective.json()["effective"]["design_system_id"] == ids["global_a"]
        )

        denied_create = viewer.post(
            f"{PREFIX}/design-systems",
            json={
                "title": "Viewer must not create inline",
                "scope": "inline",
                "board_id": ids["board_a"],
            },
        )
        assert denied_create.status_code == 404
        assert denied_create.json()["detail"]["code"] == "board_not_found"
        denied_unlink = viewer.delete(f"{PREFIX}/boards/{ids['board_a']}/design-system")
        assert denied_unlink.status_code == 404
        assert denied_unlink.json()["detail"]["code"] == "board_not_found"

        outsider_read = outsider.get(f"{PREFIX}/boards/{ids['board_a']}/design-system")
        assert outsider_read.status_code == 404
        assert outsider_read.json()["detail"]["code"] == "board_not_found"

        # Board ownership does not authorize linking somebody else's global
        # catalog artifact, and the current link remains untouched.
        foreign_link = owner.post(
            f"{PREFIX}/boards/{ids['board_a']}/design-system",
            json={"design_system_id": ids["global_b"]},
        )
        assert foreign_link.status_code == 404
        assert foreign_link.json()["detail"]["code"] == "design_system_not_found"
        unchanged = owner.get(f"{PREFIX}/boards/{ids['board_a']}/design-system")
        assert unchanged.json()["effective"]["design_system_id"] == ids["global_a"]

        editor_global = editor.post(
            f"{PREFIX}/design-systems",
            json={"title": "Editor global"},
        )
        assert editor_global.status_code == 200, editor_global.text
        editor_inline = editor.post(
            f"{PREFIX}/design-systems",
            json={
                "title": "Editor inline",
                "scope": "inline",
                "board_id": ids["board_a"],
            },
        )
        assert editor_inline.status_code == 200, editor_inline.text
        linked = editor.post(
            f"{PREFIX}/boards/{ids['board_a']}/design-system",
            json={"design_system_id": editor_global.json()["id"]},
        )
        assert linked.status_code == 200, linked.text
        assert linked.json()["design_system_id"] == editor_global.json()["id"]
        assert (
            editor.delete(f"{PREFIX}/boards/{ids['board_a']}/design-system").status_code
            == 204
        )
    finally:
        owner.close()
        viewer.close()
        editor.close()
        outsider.close()
        await _cleanup(ids)


@pytest.mark.asyncio
async def test_rest_detail_export_update_delete_hide_cross_scope_without_mutation():
    ids = await _seed()
    client_a = _rest_client(ids["owner_a"])
    client_b = _rest_client(ids["owner_b"])
    try:
        owner_only = client_a.get(f"{PREFIX}/design-systems/{ids['global_a']}")
        assert owner_only.status_code == 200
        assert owner_only.json()["id"] == ids["global_a"]

        linked = client_a.get(
            f"{PREFIX}/design-systems/{ids['global_a']}",
            params={"board_id": ids["board_a"]},
        )
        assert linked.status_code == 200
        exported = client_a.get(
            f"{PREFIX}/design-systems/{ids['global_a']}/export",
            params={"board_id": ids["board_a"]},
        )
        assert exported.status_code == 200
        assert exported.json()["items"][0]["payload"] == {"owner": "a"}

        happy_update = client_a.patch(
            f"{PREFIX}/design-systems/{ids['global_a']}",
            params={"board_id": ids["board_a"]},
            json={"payload": {"owner": "a", "rest_updated": True}},
        )
        assert happy_update.status_code == 200, happy_update.text
        assert happy_update.json()["version"] == 2
        baseline = await _state(ids)

        for client, board_id in (
            (client_b, None),
            (client_a, ids["board_b"]),
        ):
            params = {"board_id": board_id} if board_id else None
            _assert_rest_not_found(
                client.get(
                    f"{PREFIX}/design-systems/{ids['global_a']}",
                    params=params,
                )
            )
            _assert_rest_not_found(
                client.get(
                    f"{PREFIX}/design-systems/{ids['global_a']}/export",
                    params=params,
                )
            )
            _assert_rest_not_found(
                client.patch(
                    f"{PREFIX}/design-systems/{ids['global_a']}",
                    params=params,
                    json={"title": "Unauthorized", "payload": {"stolen": True}},
                )
            )
            _assert_rest_not_found(
                client.delete(
                    f"{PREFIX}/design-systems/{ids['global_a']}",
                    params=params,
                )
            )

        missing = "missing-design-system"
        _assert_rest_not_found(client_a.get(f"{PREFIX}/design-systems/{missing}"))
        _assert_rest_not_found(
            client_a.get(f"{PREFIX}/design-systems/{missing}/export")
        )
        _assert_rest_not_found(
            client_a.patch(
                f"{PREFIX}/design-systems/{missing}",
                json={"payload": {"created": "by-idor"}},
            )
        )
        _assert_rest_not_found(client_a.delete(f"{PREFIX}/design-systems/{missing}"))

        assert await _state(ids) == baseline

        deleted = client_a.delete(
            f"{PREFIX}/design-systems/{ids['inline_a']}",
            params={"board_id": ids["board_a"]},
        )
        assert deleted.status_code == 204, deleted.text
    finally:
        client_a.close()
        client_b.close()
        await _cleanup(ids)
