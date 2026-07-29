"""ITEM 19 — JSON import/export for Guidelines, Design Systems, Presets and
Default Board Configs (schema_version-1 envelope, REST → use case → UoW).

Per family: ROUNDTRIP (create via the normal API → export → import into a
clean tenant/board → equivalent objects re-created through the normal creation
path), dry-run mutates nothing, and an invalid item → 400 with NO mutation
(all-or-nothing per request).

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_import_export_api.py
"""

from __future__ import annotations

import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.auth_deps import require_principal, require_user
from okto_pulse.community.api.default_board_config import (
    router as default_board_config_router,
)
from okto_pulse.community.api.design_systems import router as design_systems_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.guidelines import router as guidelines_router
from okto_pulse.community.api.presets import router as presets_router
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.ports.authentication import Principal
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from sqlalchemy_test_models import Board

PREFIX = "/api/v1"


def _client(
    user_id: str,
    *,
    roles: tuple[str, ...] = ("admin",),
    permissions: dict | None = None,
) -> TestClient:
    app = FastAPI()
    # Same registration order as api/router.py: default_board_config BEFORE
    # guidelines so literal /guidelines/* routes are not shadowed.
    app.include_router(default_board_config_router, prefix=PREFIX)
    app.include_router(guidelines_router, prefix=PREFIX)
    app.include_router(presets_router, prefix=f"{PREFIX}/presets")
    app.include_router(design_systems_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_uow():
        # Mirror the production request UoW contract: commit on success and
        # rollback on error, while keeping the adapter outside Core.
        async with session_factory() as session:
            try:
                yield resolve_unit_of_work_factory().wrap(session)
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    app.dependency_overrides[get_unit_of_work] = _override_uow
    app.dependency_overrides[require_user] = lambda: user_id
    claims: dict = {"roles": list(roles)}
    if permissions is not None:
        claims["permissions"] = permissions
    app.dependency_overrides[require_principal] = lambda: Principal(
        subject=user_id,
        realm_id="local",
        claims=claims,
    )
    return TestClient(app)


async def _seed_board(owner_id: str) -> str:
    board_id = f"impexp-board-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="ImportExport Board",
                owner_id=owner_id,
                realm_id="local",
            )
        )
        await db.commit()
    return board_id


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ===========================================================================
# Guidelines
# ===========================================================================


@pytest.mark.asyncio
async def test_guidelines_roundtrip_export_import_clean_board():
    user_a = _uid("impexp-guide-a")
    user_b = _uid("impexp-guide-b")
    client_a = _client(user_a)
    board_a = await _seed_board(user_a)

    global_title = _uid("Global rule")
    inline_title = _uid("Inline rule")
    created_global = client_a.post(
        f"{PREFIX}/guidelines",
        json={
            "title": global_title,
            "content": "Review API contracts.",
            "tags": ["review", "api"],
            "scope": "global",
        },
    )
    assert created_global.status_code == 201, created_global.text
    created_inline = client_a.post(
        f"{PREFIX}/boards/{board_a}/guidelines",
        json={"title": inline_title, "content": "Board-only rule.", "tags": ["board"]},
    )
    assert created_inline.status_code == 201, created_inline.text

    exported = client_a.get(f"{PREFIX}/guidelines/export", params={"board_id": board_a})
    assert exported.status_code == 200, exported.text
    envelope = exported.json()
    assert envelope["schema_version"] == "1"
    assert envelope["kind"] == "guidelines"
    assert envelope["exported_at"]
    items = envelope["items"]
    assert {(i["title"], i["scope"]) for i in items} == {
        (global_title, "global"),
        (inline_title, "inline"),
    }
    # Server-generated fields must NOT leak into the export.
    for item in items:
        assert set(item) == {"title", "content", "tags", "scope", "board_id"}

    # Import into a CLEAN tenant (different user) and a CLEAN board.
    client_b = _client(user_b)
    board_b = await _seed_board(user_b)
    imported = client_b.post(
        f"{PREFIX}/guidelines/import",
        params={"board_id": board_b},
        json=envelope,
    )
    assert imported.status_code == 200, imported.text
    body = imported.json()
    assert body == {"created": 2, "skipped": [], "errors": [], "dry_run": False}

    globals_b = client_b.get(f"{PREFIX}/guidelines").json()
    match = [g for g in globals_b if g["title"] == global_title]
    assert len(match) == 1
    assert match[0]["content"] == "Review API contracts."
    assert match[0]["tags"] == ["review", "api"]
    assert match[0]["scope"] == "global"

    board_b_items = client_b.get(f"{PREFIX}/boards/{board_b}/guidelines").json()
    inline_match = [e for e in board_b_items if e["guideline"]["title"] == inline_title]
    assert len(inline_match) == 1
    assert inline_match[0]["guideline"]["scope"] == "inline"
    assert inline_match[0]["guideline"]["board_id"] == board_b

    # Re-import: natural key (title within partition) → skipped duplicates.
    again = client_b.post(
        f"{PREFIX}/guidelines/import", params={"board_id": board_b}, json=envelope
    )
    assert again.status_code == 200, again.text
    body = again.json()
    assert body["created"] == 0
    assert {s["reason"] for s in body["skipped"]} == {
        "duplicate_global_title",
        "duplicate_inline_title",
    }


@pytest.mark.asyncio
async def test_guidelines_import_dry_run_does_not_mutate():
    user = _uid("impexp-guide-dry")
    client = _client(user)
    board = await _seed_board(user)
    envelope = {
        "schema_version": "1",
        "kind": "guidelines",
        "exported_at": "2026-07-10T00:00:00+00:00",
        "items": [
            {
                "title": "Dry global",
                "content": "c",
                "tags": None,
                "scope": "global",
                "board_id": None,
            },
            {
                "title": "Dry inline",
                "content": "c",
                "tags": None,
                "scope": "inline",
                "board_id": None,
            },
        ],
    }
    resp = client.post(
        f"{PREFIX}/guidelines/import",
        params={"board_id": board, "dry_run": "true"},
        json=envelope,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"created": 2, "skipped": [], "errors": [], "dry_run": True}

    assert client.get(f"{PREFIX}/guidelines").json() == []
    assert client.get(f"{PREFIX}/boards/{board}/guidelines").json() == []


def test_guidelines_import_invalid_item_400_without_mutation():
    user = _uid("impexp-guide-bad")
    client = _client(user)

    envelope = {
        "schema_version": "1",
        "kind": "guidelines",
        "items": [
            {"title": "Valid one", "content": "c", "scope": "global"},
            {"title": "", "content": "c", "scope": "global"},  # min_length=1
        ],
    }
    resp = client.post(f"{PREFIX}/guidelines/import", json=envelope)
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert detail["created"] == 0
    assert detail["errors"] and detail["errors"][0]["index"] == 1
    # All-or-nothing: the valid item was NOT created either.
    assert client.get(f"{PREFIX}/guidelines").json() == []

    # Envelope guards: wrong kind and wrong schema_version are rejected.
    wrong_kind = client.post(
        f"{PREFIX}/guidelines/import",
        json={"schema_version": "1", "kind": "presets", "items": []},
    )
    assert wrong_kind.status_code == 400
    assert wrong_kind.json()["detail"]["error"] == "invalid_envelope"
    wrong_version = client.post(
        f"{PREFIX}/guidelines/import",
        json={"schema_version": "2", "kind": "guidelines", "items": []},
    )
    assert wrong_version.status_code == 400
    assert wrong_version.json()["detail"]["error"] == "invalid_envelope"


# ===========================================================================
# Design Systems
# ===========================================================================


def test_design_systems_roundtrip_single_and_bulk():
    user = _uid("impexp-ds")
    client = _client(user)

    title_1 = _uid("DS Alpha")
    title_2 = _uid("DS Beta")
    ds1 = client.post(
        f"{PREFIX}/design-systems",
        json={
            "title": title_1,
            "scope": "global",
            "payload": {"tokens": {"radius": 8}},
        },
    )
    assert ds1.status_code == 200, ds1.text
    ds2 = client.post(
        f"{PREFIX}/design-systems",
        json={
            "title": title_2,
            "scope": "global",
            "payload": {"content": "text"},
            "status": "active",
        },
    )
    assert ds2.status_code == 200, ds2.text
    ds1_id = ds1.json()["id"]

    single = client.get(f"{PREFIX}/design-systems/{ds1_id}/export")
    assert single.status_code == 200, single.text
    assert single.json()["kind"] == "design_systems"
    assert single.json()["items"] == [
        {
            "title": title_1,
            "scope": "global",
            "board_id": None,
            "payload": {"tokens": {"radius": 8}},
            "status": "active",
        }
    ]

    bulk = client.get(f"{PREFIX}/design-systems/export")
    assert bulk.status_code == 200, bulk.text
    envelope = bulk.json()
    assert envelope["schema_version"] == "1"
    mine = [i for i in envelope["items"] if i["title"] in {title_1, title_2}]
    assert len(mine) == 2
    for item in mine:
        assert set(item) == {"title", "scope", "board_id", "payload", "status"}

    # "Clean" the catalog of the originals, then import them back.
    assert client.delete(f"{PREFIX}/design-systems/{ds1_id}").status_code == 204
    assert (
        client.delete(f"{PREFIX}/design-systems/{ds2.json()['id']}").status_code == 204
    )

    import_env = {"schema_version": "1", "kind": "design_systems", "items": mine}
    imported = client.post(f"{PREFIX}/design-systems/import", json=import_env)
    assert imported.status_code == 200, imported.text
    assert imported.json() == {
        "created": 2,
        "skipped": [],
        "errors": [],
        "dry_run": False,
    }

    catalog = client.get(f"{PREFIX}/design-systems").json()["items"]
    by_title = {d["title"]: d for d in catalog if d["title"] in {title_1, title_2}}
    assert set(by_title) == {title_1, title_2}
    # Catalog lists use the bounded summary projection; the full payload is
    # available only through the explicit detail read.
    assert "payload" not in by_title[title_1]
    assert by_title[title_1]["status"] == "active"
    imported_detail = client.get(f"{PREFIX}/design-systems/{by_title[title_1]['id']}")
    assert imported_detail.status_code == 200, imported_detail.text
    assert imported_detail.json()["payload"] == {"tokens": {"radius": 8}}
    assert imported_detail.json()["version"] == 1  # recreated via the normal path

    # Re-import: (scope, board_id, title) natural key → skipped duplicates.
    again = client.post(f"{PREFIX}/design-systems/import", json=import_env)
    assert again.status_code == 200
    assert again.json()["created"] == 0
    assert len(again.json()["skipped"]) == 2


def test_design_systems_import_dry_run_and_invalid_item():
    user = _uid("impexp-ds2")
    client = _client(user)

    dry_title = _uid("DS Dry")
    dry = client.post(
        f"{PREFIX}/design-systems/import",
        params={"dry_run": "true"},
        json={
            "schema_version": "1",
            "kind": "design_systems",
            "items": [
                {
                    "title": dry_title,
                    "scope": "global",
                    "payload": None,
                    "status": "active",
                }
            ],
        },
    )
    assert dry.status_code == 200, dry.text
    assert dry.json() == {"created": 1, "skipped": [], "errors": [], "dry_run": True}
    titles = {
        d["title"] for d in client.get(f"{PREFIX}/design-systems").json()["items"]
    }
    assert dry_title not in titles

    # Second item hits the SERVICE creation validator (inline requires board):
    # all-or-nothing → the valid first item must not persist.
    valid_title = _uid("DS Valid")
    bad = client.post(
        f"{PREFIX}/design-systems/import",
        json={
            "schema_version": "1",
            "kind": "design_systems",
            "items": [
                {"title": valid_title, "scope": "global"},
                {"title": "Inline missing board", "scope": "inline"},
            ],
        },
    )
    assert bad.status_code == 400, bad.text
    detail = bad.json()["detail"]
    assert detail["created"] == 0
    assert detail["errors"][0]["index"] == 1
    assert (
        detail["errors"][0]["detail"]["code"] == "design_system_inline_requires_board"
    )
    titles = {
        d["title"] for d in client.get(f"{PREFIX}/design-systems").json()["items"]
    }
    assert valid_title not in titles

    # Unknown fields are rejected by the SAME create request model (extra=forbid).
    unknown = client.post(
        f"{PREFIX}/design-systems/import",
        json={
            "schema_version": "1",
            "kind": "design_systems",
            "items": [{"title": "X", "scope": "global", "bogus": 1}],
        },
    )
    assert unknown.status_code == 400


# ===========================================================================
# Permission presets
# ===========================================================================


def test_presets_roundtrip_clean_tenant_and_duplicates():
    user_a = _uid("impexp-preset-a")
    user_b = _uid("impexp-preset-b")
    client_a = _client(user_a)

    preset_name = _uid("Preset Custom")
    flags = {"board": {"read": True, "create": False}}
    created = client_a.post(
        f"{PREFIX}/presets",
        json={"name": preset_name, "description": "exported preset", "flags": flags},
    )
    assert created.status_code == 201, created.text

    exported = client_a.get(f"{PREFIX}/presets/export")
    assert exported.status_code == 200, exported.text
    envelope = exported.json()
    assert envelope["kind"] == "presets"
    mine = [i for i in envelope["items"] if i["name"] == preset_name]
    assert mine == [
        {
            "name": preset_name,
            "description": "exported preset",
            "flags": flags,
            "is_builtin": False,
        }
    ]

    # Same user re-import → duplicate name is skipped, nothing created.
    same = client_a.post(
        f"{PREFIX}/presets/import",
        json={"schema_version": "1", "kind": "presets", "items": mine},
    )
    assert same.status_code == 200, same.text
    assert same.json()["created"] == 0
    assert same.json()["skipped"][0]["reason"] == "duplicate_name"

    # Clean tenant (different user) → created as a NEW custom preset.
    client_b = _client(user_b)
    imported = client_b.post(
        f"{PREFIX}/presets/import",
        json={"schema_version": "1", "kind": "presets", "items": mine},
    )
    assert imported.status_code == 200, imported.text
    assert imported.json() == {
        "created": 1,
        "skipped": [],
        "errors": [],
        "dry_run": False,
    }
    listed = client_b.get(f"{PREFIX}/presets").json()
    match = [p for p in listed if p["name"] == preset_name]
    assert len(match) == 1
    assert match[0]["flags"] == flags
    assert match[0]["is_builtin"] is False
    assert match[0]["owner_id"] == user_b  # new record, new ownership


def test_presets_import_dry_run_and_invalid_item():
    user = _uid("impexp-preset-dry")
    client = _client(user)
    name = _uid("Preset Dry")

    dry = client.post(
        f"{PREFIX}/presets/import",
        params={"dry_run": "true"},
        json={
            "schema_version": "1",
            "kind": "presets",
            "items": [{"name": name, "description": "", "flags": None}],
        },
    )
    assert dry.status_code == 200, dry.text
    assert dry.json()["created"] == 1
    assert dry.json()["dry_run"] is True
    assert name not in {p["name"] for p in client.get(f"{PREFIX}/presets").json()}

    bad = client.post(
        f"{PREFIX}/presets/import",
        json={
            "schema_version": "1",
            "kind": "presets",
            "items": [{"name": _uid("ok")}, {"description": "missing name"}],
        },
    )
    assert bad.status_code == 400, bad.text
    detail = bad.json()["detail"]
    assert detail["created"] == 0
    assert detail["errors"][0]["index"] == 1


# ===========================================================================
# Default board configuration
# ===========================================================================


def test_board_config_global_writes_require_real_admin_principal():
    user = _uid("impexp-dbc-denied")
    client = _client(user, roles=())
    before = len(
        client.get(f"{PREFIX}/default-board-config/versions").json()["versions"]
    )
    template_id = f"missing-{uuid.uuid4().hex}"
    calls = (
        (
            f"{PREFIX}/default-board-config/versions",
            {"json": {"settings_payload": {"max_scenarios_per_card": 7}}},
        ),
        (
            f"{PREFIX}/default-board-config/import",
            {
                "json": {
                    "schema_version": "1",
                    "kind": "board_config",
                    "items": [
                        {
                            "scope": "global",
                            "settings_payload": {"max_scenarios_per_card": 7},
                            "is_active": False,
                        }
                    ],
                }
            },
        ),
        (f"{PREFIX}/default-board-config/versions/{template_id}/activate", {}),
        (f"{PREFIX}/default-board-config/versions/{template_id}/deactivate", {}),
        (
            f"{PREFIX}/default-board-configurations/{template_id}/guidelines",
            {"json": {"guideline_default_refs": []}},
        ),
        (
            f"{PREFIX}/default-board-configurations/{template_id}/design-system",
            {"json": {"design_system_id": f"missing-{uuid.uuid4().hex}"}},
        ),
    )
    for url, kwargs in calls:
        denied = client.post(url, **kwargs)
        assert denied.status_code == 403, denied.text
        assert "admin or operator capability" in denied.json()["detail"]

    after = len(
        client.get(f"{PREFIX}/default-board-config/versions").json()["versions"]
    )
    assert after == before

    capability_client = _client(
        _uid("impexp-dbc-capability"),
        roles=(),
        permissions={"admin": {"catalog": {"write": True}}},
    )
    allowed = capability_client.post(
        f"{PREFIX}/default-board-config/versions",
        json={
            "scope": f"capability-{uuid.uuid4().hex}",
            "settings_payload": {"max_scenarios_per_card": 6},
        },
    )
    assert allowed.status_code == 200, allowed.text


def test_board_config_roundtrip_versions_and_active():
    user = _uid("impexp-dbc")
    client = _client(user)

    marker = {"max_scenarios_per_card": 7, "min_confidence": 71}
    created = client.post(
        f"{PREFIX}/default-board-config/versions",
        json={
            "settings_payload": marker,
            "spec_checklist_mode": "blocking",
            "activate": True,
        },
    )
    assert created.status_code == 200, created.text

    exported = client.get(f"{PREFIX}/default-board-config/export")
    assert exported.status_code == 200, exported.text
    envelope = exported.json()
    assert envelope["schema_version"] == "1"
    assert envelope["kind"] == "board_config"
    items = envelope["items"]
    active_items = [i for i in items if i["is_active"]]
    assert len(active_items) == 1  # the active version is marked
    assert active_items[0]["settings_payload"]["max_scenarios_per_card"] == 7
    assert active_items[0]["spec_checklist_mode"] == "blocking"
    for item in items:
        assert set(item) == {
            "scope",
            "settings_payload",
            "guideline_default_refs",
            "design_system_default_ref",
            "spec_checklist_mode",
            "is_active",
        }
        assert item["spec_checklist_mode"] in {
            "off",
            "advisory",
            "blocking",
        }

    before = client.get(f"{PREFIX}/default-board-config/versions").json()
    before_count = len(before["versions"])

    imported = client.post(f"{PREFIX}/default-board-config/import", json=envelope)
    assert imported.status_code == 200, imported.text
    body = imported.json()
    assert body["created"] == len(items)
    assert body["skipped"] == []  # append-only history: no natural key, no dedup

    after = client.get(f"{PREFIX}/default-board-config/versions").json()
    assert len(after["versions"]) == before_count + len(items)
    active = client.get(f"{PREFIX}/default-board-config/active").json()["active"]
    assert active is not None
    assert active["settings_payload"]["max_scenarios_per_card"] == 7
    assert active["settings_payload"]["min_confidence"] == 71
    assert active["spec_checklist_mode"] == "blocking"


def test_board_config_import_dry_run_and_invalid_item():
    user = _uid("impexp-dbc2")
    client = _client(user)

    before = len(
        client.get(f"{PREFIX}/default-board-config/versions").json()["versions"]
    )

    dry = client.post(
        f"{PREFIX}/default-board-config/import",
        params={"dry_run": "true"},
        json={
            "schema_version": "1",
            "kind": "board_config",
            "items": [
                {
                    "scope": "global",
                    "settings_payload": {"max_scenarios_per_card": 4},
                    "guideline_default_refs": [],
                    "design_system_default_ref": None,
                    "is_active": False,
                }
            ],
        },
    )
    assert dry.status_code == 200, dry.text
    assert dry.json()["created"] == 1
    assert dry.json()["dry_run"] is True
    assert (
        len(client.get(f"{PREFIX}/default-board-config/versions").json()["versions"])
        == before
    )

    # Second item fails the BoardSettings creation validator → all-or-nothing.
    bad = client.post(
        f"{PREFIX}/default-board-config/import",
        json={
            "schema_version": "1",
            "kind": "board_config",
            "items": [
                {"scope": "global", "settings_payload": {"max_scenarios_per_card": 4}},
                {
                    "scope": "global",
                    "settings_payload": {"max_scenarios_per_card": "not-a-number"},
                },
            ],
        },
    )
    assert bad.status_code == 400, bad.text
    detail = bad.json()["detail"]
    assert detail["created"] == 0
    assert detail["errors"][0]["index"] == 1
    assert (
        len(client.get(f"{PREFIX}/default-board-config/versions").json()["versions"])
        == before
    )
