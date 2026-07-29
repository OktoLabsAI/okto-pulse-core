"""Spec R01A REST-FU7-S1 — Board endpoints on the UnitOfWork.

The ``api/boards.py`` endpoints that still bound a raw request session — list /
get / update / delete board, create-card, columns, archive/restore tree, and the
board-share CRUD — now route through the ``boards_crud`` use cases +
``get_unit_of_work``; each adapter only maps the result/errors to HTTP. Oracles
exercise the migrated status codes + bodies (list 200, get 200/404, update 200
re-fetched body / 404, delete 204→404, create-card 404 / 400, columns 200 / 404,
archive+restore 200 + bad-entity 400, share 201 / 403-self, list-shares 200 /
404, update-share 200 / 403, revoke-share 204 / 403), the use case raising
``EntityNotFoundError`` for a missing board, and an AST signature check proving
every board endpoint takes ``uow`` (not a raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import boards as boards_api
from okto_pulse.community.api.boards import router as boards_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu7-s1-user"
OTHER = "r01a-fu7-s1-other"
PREFIX = "/api/v1/boards"

# Every board endpoint must take ``uow`` (the whole module is get_db-free now).
_ENDPOINTS = (
    "create_board",
    "list_boards",
    "get_board",
    "update_board",
    "delete_board",
    "create_card",
    "get_board_columns",
    "archive_tree",
    "restore_tree",
    "share_board",
    "list_board_shares",
    "update_board_share",
    "revoke_board_share",
)


@pytest.fixture
def client(tmp_path):
    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.community.adapters.storage import CommunityFileSystemStorage
    from okto_pulse.core.infra.storage import (
        configure_storage,
        reset_storage_provider_for_tests,
    )
    from okto_pulse.core.kg.providers.testing.memory_global_discovery_runtime import (
        InMemoryGlobalDiscoveryRuntime,
    )

    app = FastAPI()
    app.include_router(boards_router, prefix=PREFIX)
    session_factory = get_session_factory()
    configure_storage(CommunityFileSystemStorage(str(tmp_path / "uploads")))
    # The governed board-deletion path (DELETE /boards/{id}) runs a STRICT physical
    # Global Discovery erasure via ``global_runtime.erase_storage_for_privacy``. The
    # suite's real-if-available registry supplies a GlobalDiscovery runtime WITHOUT
    # that physical-erasure capability, so in isolation the delete fails 503
    # ``global_discovery_physical_erasure_unavailable``. Override ONLY the
    # GlobalDiscovery slot with the in-memory runtime — it implements the strict
    # erasure/verified-absence contract (nothing relaxed); the real graph providers
    # stay in place for the per-board graph purge. Bootstrap it so its global graph
    # "exists": the erasure cascade issues cypher DELETEs through ``execute()``, which
    # raises ``global_graph_absent`` on an un-bootstrapped graph. Board-scoped privacy
    # erasure removes the target projection while preserving the shared global runtime.
    global_discovery_runtime = InMemoryGlobalDiscoveryRuntime()
    global_discovery_runtime.bootstrap()
    configure_test_kg_registry(global_discovery_runtime=global_discovery_runtime)

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    try:
        yield TestClient(app)
    finally:
        reset_storage_provider_for_tests()
        # Restore the ambient (real-if-available) test registry for subsequent tests.
        configure_test_kg_registry()


async def _seed_board(
    owner: str = USER,
    name: str = "fu7s1",
    *,
    realm_id: str = LOCAL_REALM_ID,
) -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu7s1-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name=name,
                owner_id=owner,
                realm_id=realm_id,
            )
        )
        await db.commit()
        return bid


async def _seed_board_spec_card(owner: str = USER) -> tuple[str, str, str]:
    """Board + Spec + Card via raw models (bypasses CardService.create_card's
    spec-status gate — these endpoints read/group/archive, not create)."""
    from sqlalchemy_test_models import Board, Card, Spec

    bid = f"board-fu7s1-{uuid.uuid4().hex[:8]}"
    sid = f"spec-fu7s1-{uuid.uuid4().hex[:8]}"
    cid = f"card-fu7s1-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name="fu7s1",
                owner_id=owner,
                realm_id=LOCAL_REALM_ID,
            )
        )
        db.add(Spec(id=sid, board_id=bid, title="fu7s1-spec", created_by=owner))
        db.add(
            Card(
                id=cid, board_id=bid, spec_id=sid, title="fu7s1-card", created_by=owner
            )
        )
        await db.commit()
        return bid, sid, cid


async def _grant_share(board_id: str, permission: str) -> None:
    from sqlalchemy_test_models import BoardShare

    async with get_session_factory()() as db:
        db.add(
            BoardShare(
                board_id=board_id,
                user_id=USER,
                realm_id=LOCAL_REALM_ID,
                permission=permission,
                shared_by=OTHER,
            )
        )
        await db.commit()


def _missing(kind: str = "board") -> str:
    return f"{kind}-missing-{uuid.uuid4().hex[:8]}"


# --- list -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_boards_200(client) -> None:
    board_id = await _seed_board()
    resp = client.get(PREFIX)
    assert resp.status_code == 200, resp.text
    assert board_id in {b["id"] for b in resp.json()}


# --- get --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_200(client) -> None:
    board_id = await _seed_board(name="fu7s1-get")
    resp = client.get(f"{PREFIX}/{board_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == board_id
    assert body["name"] == "fu7s1-get"


@pytest.mark.asyncio
async def test_get_board_compact_200(client) -> None:
    board_id = await _seed_board()
    resp = client.get(f"{PREFIX}/{board_id}", params={"compact": "true"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == board_id


@pytest.mark.asyncio
async def test_get_board_404(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- update -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_board_200_refetched_body(client) -> None:
    board_id = await _seed_board()
    resp = client.patch(f"{PREFIX}/{board_id}", json={"name": "fu7s1-renamed"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == board_id
    assert body["name"] == "fu7s1-renamed"


@pytest.mark.asyncio
async def test_update_board_404(client) -> None:
    resp = client.patch(f"{PREFIX}/{_missing()}", json={"name": "nope"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- delete -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_board_204_then_404(client) -> None:
    from okto_pulse.core.kg.interfaces import get_kg_registry

    board_id = await _seed_board()
    resp = client.delete(f"{PREFIX}/{board_id}")
    assert resp.status_code == 204, resp.text
    global_runtime = get_kg_registry().require_global_discovery_runtime()
    assert global_runtime.state().exists is True
    assert global_runtime.purged_reasons == ["board_right_to_erasure"]
    gone = client.delete(f"{PREFIX}/{board_id}")
    assert gone.status_code == 404
    assert client.get(f"{PREFIX}/{board_id}").status_code == 404


@pytest.mark.asyncio
async def test_delete_board_404(client) -> None:
    resp = client.delete(f"{PREFIX}/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_delete_board_commits_relational_erasure_before_external_stores(
    monkeypatch,
) -> None:
    from types import SimpleNamespace

    from okto_pulse.core.application.use_cases import boards_crud
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.boards_crud import (
        DeleteBoardCommand,
        DeleteBoardUseCase,
    )

    events: list[object] = []

    async def _require_owned(_uow, board_id, actor):
        events.append(("authorize", board_id, actor.actor_id))
        return object()

    class _Erasure:
        def ensure_owned(self):
            events.append("ensure_lease")

    class _Scope:
        async def __aenter__(self):
            events.append("enter_erasure_scope")
            return _Erasure()

        async def __aexit__(self, *_exc):
            events.append("exit_erasure_scope")

    class _KG:
        async def get_board_erasure_job(self, _board_id):
            return None

        def board_erasure_scope(self, board_id, *, actor_id):
            events.append(("create_erasure_scope", board_id, actor_id))
            return _Scope()

        async def stage_board_relational_erasure(self, board_id, *, actor_id):
            events.append(("stage_relational_erasure", board_id, actor_id))

        async def right_to_erasure(
            self,
            board_id,
            *,
            strict,
            commit,
            global_writer_guarded,
            purge_relational,
        ):
            events.append(
                (
                    "erase_kg",
                    board_id,
                    strict,
                    commit,
                    global_writer_guarded,
                    purge_relational,
                )
            )
            return {"board_id": board_id, "sqlite_purged": True}

        async def complete_board_erasure_job(self, board_id):
            events.append(("complete_erasure_job", board_id))
            return True

    class _Boards:
        async def delete_board(self, board_id, user_id):
            events.append(("delete_source", board_id, user_id))
            return True

    class _Uow:
        services = SimpleNamespace(kg=_KG(), boards=_Boards())

        async def synchronize(self):
            events.append("synchronize")

        async def commit(self):
            events.append("commit")

    monkeypatch.setattr(boards_crud, "_require_owned_board", _require_owned)
    await DeleteBoardUseCase().execute(
        DeleteBoardCommand("board-strict-erasure"),
        actor=ActorContext("owner", "rest"),
        uow=_Uow(),
    )

    assert events == [
        ("authorize", "board-strict-erasure", "owner"),
        ("create_erasure_scope", "board-strict-erasure", "owner"),
        "enter_erasure_scope",
        ("stage_relational_erasure", "board-strict-erasure", "owner"),
        "ensure_lease",
        ("delete_source", "board-strict-erasure", "owner"),
        "synchronize",
        "ensure_lease",
        "commit",
        "ensure_lease",
        ("erase_kg", "board-strict-erasure", True, False, True, False),
        "ensure_lease",
        ("complete_erasure_job", "board-strict-erasure"),
        "commit",
        "exit_erasure_scope",
    ]


@pytest.mark.asyncio
async def test_delete_board_relational_erasure_failure_skips_commit_and_physical_purge(
    monkeypatch,
) -> None:
    from types import SimpleNamespace

    from okto_pulse.core.application.use_cases import boards_crud
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.boards_crud import (
        DeleteBoardCommand,
        DeleteBoardUseCase,
    )

    events: list[str] = []

    async def _require_owned(_uow, _board_id, _actor):
        events.append("authorize")
        return object()

    class _Erasure:
        def ensure_owned(self):
            events.append("ensure_lease")

    class _Scope:
        async def __aenter__(self):
            events.append("enter_erasure_scope")
            return _Erasure()

        async def __aexit__(self, *_exc):
            events.append("exit_erasure_scope")

    class _KG:
        async def get_board_erasure_job(self, _board_id):
            return None

        def board_erasure_scope(self, _board_id, *, actor_id):
            assert actor_id == "owner"
            events.append("create_erasure_scope")
            return _Scope()

        async def stage_board_relational_erasure(self, _board_id, *, actor_id):
            assert actor_id == "owner"
            events.append("stage_relational_erasure")
            raise RuntimeError("relational erasure failed")

        async def right_to_erasure(self, *_args, **_kwargs):
            pytest.fail("physical purge must not start before relational commit")

    class _Boards:
        async def delete_board(self, _board_id, _user_id):
            events.append("delete_source")
            return True

    class _Uow:
        services = SimpleNamespace(kg=_KG(), boards=_Boards())

        async def synchronize(self):
            events.append("synchronize")

        async def commit(self):
            events.append("commit")

    monkeypatch.setattr(boards_crud, "_require_owned_board", _require_owned)
    with pytest.raises(RuntimeError, match="relational erasure failed"):
        await DeleteBoardUseCase().execute(
            DeleteBoardCommand("board-strict-erasure"),
            actor=ActorContext("owner", "rest"),
            uow=_Uow(),
        )

    assert events == [
        "authorize",
        "create_erasure_scope",
        "enter_erasure_scope",
        "stage_relational_erasure",
        "exit_erasure_scope",
    ]


@pytest.mark.asyncio
async def test_delete_board_external_failure_occurs_only_after_source_commit(
    monkeypatch,
) -> None:
    from types import SimpleNamespace

    from okto_pulse.core.application.use_cases import boards_crud
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.boards_crud import (
        DeleteBoardCommand,
        DeleteBoardUseCase,
    )

    events: list[str] = []

    async def _require_owned(*_args):
        events.append("authorize")
        return object()

    class _Erasure:
        def ensure_owned(self):
            events.append("ensure")

    class _Scope:
        async def __aenter__(self):
            events.append("enter")
            return _Erasure()

        async def __aexit__(self, *_exc):
            events.append("exit")

    class _KG:
        async def get_board_erasure_job(self, _board_id):
            return None

        def board_erasure_scope(self, *_args, **_kwargs):
            return _Scope()

        async def stage_board_relational_erasure(self, _board_id, *, actor_id):
            assert actor_id == "owner"
            events.append("stage_relational")

        async def right_to_erasure(self, *_args, **kwargs):
            assert kwargs["purge_relational"] is False
            events.append("external")
            raise RuntimeError("external purge failed")

        async def record_board_erasure_failure(self, _board_id, error):
            assert str(error) == "external purge failed"
            events.append("record_failure")

    class _Boards:
        async def delete_board(self, *_args):
            events.append("delete")
            return True

    class _Uow:
        services = SimpleNamespace(kg=_KG(), boards=_Boards())

        async def synchronize(self):
            events.append("synchronize")

        async def commit(self):
            events.append("commit")

    monkeypatch.setattr(boards_crud, "_require_owned_board", _require_owned)
    with pytest.raises(RuntimeError, match="external purge failed"):
        await DeleteBoardUseCase().execute(
            DeleteBoardCommand("board-external-failure"),
            actor=ActorContext("owner", "rest"),
            uow=_Uow(),
        )

    assert events.index("commit") < events.index("external")
    assert events.index("external") < events.index("record_failure")
    assert events.count("commit") == 2
    assert events[-1] == "exit"


@pytest.mark.asyncio
async def test_delete_board_resumes_durable_erasure_after_source_is_absent(
    monkeypatch,
) -> None:
    from types import SimpleNamespace

    from okto_pulse.core.application.use_cases import boards_crud
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.boards_crud import (
        DeleteBoardCommand,
        DeleteBoardUseCase,
    )

    events: list[str] = []
    pending = SimpleNamespace(actor_id="owner")

    async def _unexpected_authorize(*_args):
        pytest.fail("a durable continuation must not require the deleted Board")

    class _Erasure:
        def ensure_owned(self):
            events.append("ensure")

    class _Scope:
        async def __aenter__(self):
            events.append("enter")
            return _Erasure()

        async def __aexit__(self, *_exc):
            events.append("exit")

    class _KG:
        async def get_board_erasure_job(self, _board_id):
            return pending

        def board_erasure_scope(self, *_args, **_kwargs):
            return _Scope()

        async def right_to_erasure(self, *_args, **kwargs):
            assert kwargs["purge_relational"] is False
            events.append("external")

        async def complete_board_erasure_job(self, _board_id):
            events.append("complete")
            return True

    class _Boards:
        async def delete_board(self, *_args):
            pytest.fail("resume must not attempt a second source deletion")

    class _Uow:
        services = SimpleNamespace(kg=_KG(), boards=_Boards())

        async def commit(self):
            events.append("commit")

    monkeypatch.setattr(boards_crud, "_require_owned_board", _unexpected_authorize)
    await DeleteBoardUseCase().execute(
        DeleteBoardCommand("board-resume-erasure"),
        actor=ActorContext("owner", "rest"),
        uow=_Uow(),
    )

    assert events == [
        "enter",
        "external",
        "ensure",
        "complete",
        "commit",
        "exit",
    ]


@pytest.mark.asyncio
async def test_delete_board_does_not_disclose_another_actors_erasure_job() -> None:
    from types import SimpleNamespace

    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.application.use_cases.boards_crud import (
        DeleteBoardCommand,
        DeleteBoardUseCase,
    )

    class _KG:
        async def get_board_erasure_job(self, _board_id):
            return SimpleNamespace(actor_id="original-owner")

    class _Uow:
        services = SimpleNamespace(kg=_KG())

    with pytest.raises(EntityNotFoundError):
        await DeleteBoardUseCase().execute(
            DeleteBoardCommand("board-private-erasure"),
            actor=ActorContext("different-actor", "rest"),
            uow=_Uow(),
        )


@pytest.mark.asyncio
async def test_delete_board_cancellation_drains_terminal_erasure(
    monkeypatch,
) -> None:
    import asyncio
    from types import SimpleNamespace

    from okto_pulse.core.application.use_cases import boards_crud
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.boards_crud import (
        DeleteBoardCommand,
        DeleteBoardUseCase,
    )

    events: list[str] = []
    external_started = asyncio.Event()
    finish_external = asyncio.Event()

    async def _require_owned(*_args):
        return object()

    class _Erasure:
        def ensure_owned(self):
            events.append("ensure")

    class _Scope:
        async def __aenter__(self):
            events.append("enter")
            return _Erasure()

        async def __aexit__(self, *_exc):
            events.append("exit")

    class _KG:
        async def get_board_erasure_job(self, _board_id):
            return None

        def board_erasure_scope(self, *_args, **_kwargs):
            return _Scope()

        async def stage_board_relational_erasure(self, _board_id, *, actor_id):
            assert actor_id == "owner"
            events.append("stage_relational")

        async def right_to_erasure(self, *_args, **_kwargs):
            events.append("external_started")
            external_started.set()
            await finish_external.wait()
            events.append("external_finished")

        async def complete_board_erasure_job(self, _board_id):
            events.append("complete_job")
            return True

    class _Boards:
        async def delete_board(self, *_args):
            events.append("delete")
            return True

    class _Uow:
        services = SimpleNamespace(kg=_KG(), boards=_Boards())

        async def synchronize(self):
            events.append("synchronize")

        async def commit(self):
            events.append("commit")

    monkeypatch.setattr(boards_crud, "_require_owned_board", _require_owned)
    request = asyncio.create_task(
        DeleteBoardUseCase().execute(
            DeleteBoardCommand("board-cancel-terminal"),
            actor=ActorContext("owner", "rest"),
            uow=_Uow(),
        )
    )
    await asyncio.wait_for(external_started.wait(), timeout=2)
    request.cancel()
    await asyncio.sleep(0)
    assert not request.done()

    finish_external.set()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert "commit" in events
    assert events[-5:] == [
        "external_finished",
        "ensure",
        "complete_job",
        "commit",
        "exit",
    ]


# --- create card ------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_card_board_not_found_404(client) -> None:
    # Valid CardCreate payload, but the board does not exist → create_card
    # returns None → EntityNotFoundError → the legacy 404 detail.
    resp = client.post(
        f"{PREFIX}/{_missing()}/cards",
        json={"title": "fu7s1-card", "spec_id": _missing("spec")},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found or not owned by user"


@pytest.mark.asyncio
async def test_create_card_missing_spec_400(client) -> None:
    # Board exists/owned, but no spec_id → CardService raises ValueError → 400.
    board_id = await _seed_board()
    resp = client.post(f"{PREFIX}/{board_id}/cards", json={"title": "fu7s1-card"})
    assert resp.status_code == 400, resp.text
    assert "spec" in resp.json()["detail"].lower()


# --- columns ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_columns_200(client) -> None:
    board_id, _spec_id, card_id = await _seed_board_spec_card()
    resp = client.get(f"{PREFIX}/{board_id}/columns")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["board_id"] == board_id
    all_card_ids = {c["id"] for col in body["columns"].values() for c in col}
    assert card_id in all_card_ids


@pytest.mark.asyncio
async def test_get_board_columns_404(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}/columns")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


# --- archive / restore ------------------------------------------------------


@pytest.mark.asyncio
async def test_archive_then_restore_spec_200(client) -> None:
    board_id, spec_id, _card_id = await _seed_board_spec_card()

    arch = client.post(f"{PREFIX}/{board_id}/archive/spec/{spec_id}")
    assert arch.status_code == 200, arch.text
    arch_body = arch.json()
    assert arch_body["success"] is True
    assert arch_body["archived_count"]["specs"] >= 1

    rest = client.post(f"{PREFIX}/{board_id}/restore/spec/{spec_id}")
    assert rest.status_code == 200, rest.text
    rest_body = rest.json()
    assert rest_body["success"] is True
    assert rest_body["restored_count"]["specs"] >= 1


@pytest.mark.asyncio
async def test_archive_invalid_entity_type_400(client) -> None:
    board_id = await _seed_board()
    resp = client.post(f"{PREFIX}/{board_id}/archive/bogus/{_missing('x')}")
    assert resp.status_code == 400, resp.text
    assert "entity_type" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_restore_invalid_entity_type_400(client) -> None:
    board_id = await _seed_board()
    resp = client.post(f"{PREFIX}/{board_id}/restore/bogus/{_missing('x')}")
    assert resp.status_code == 400, resp.text
    assert "entity_type" in resp.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "initial_archived"),
    [("archive", False), ("restore", True)],
)
async def test_tree_root_foreign_board_matches_missing_without_mutation(
    client, operation: str, initial_archived: bool
) -> None:
    from sqlalchemy import select
    from sqlalchemy_test_models import ActivityLog, Card, Spec

    owned_board = await _seed_board()
    _foreign_board, foreign_spec, foreign_card = await _seed_board_spec_card(
        owner=OTHER
    )
    async with get_session_factory()() as db:
        spec = await db.get(Spec, foreign_spec)
        card = await db.get(Card, foreign_card)
        spec.archived = initial_archived
        card.archived = initial_archived
        await db.commit()

    foreign = client.post(f"{PREFIX}/{owned_board}/{operation}/spec/{foreign_spec}")
    missing = client.post(f"{PREFIX}/{owned_board}/{operation}/spec/{_missing('spec')}")

    assert foreign.status_code == missing.status_code == 404
    assert foreign.content == missing.content
    async with get_session_factory()() as db:
        assert (await db.get(Spec, foreign_spec)).archived is initial_archived
        assert (await db.get(Card, foreign_card)).archived is initial_archived
        activities = (
            (
                await db.execute(
                    select(ActivityLog).where(
                        ActivityLog.board_id == owned_board,
                        ActivityLog.action.in_(["tree_archived", "tree_restored"]),
                    )
                )
            )
            .scalars()
            .all()
        )
        assert activities == []


# --- shares -----------------------------------------------------------------


def _share(client, board_id: str, target: str, permission: str = "viewer"):
    return client.post(
        f"{PREFIX}/{board_id}/shares",
        json={"user_id": target, "permission": permission},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", ["viewer", "editor", "admin"])
async def test_shared_board_read_and_columns_follow_share_permission(
    client, permission: str
) -> None:
    board_id, _spec_id, card_id = await _seed_board_spec_card(owner=OTHER)
    await _grant_share(board_id, permission)

    board = client.get(f"{PREFIX}/{board_id}")
    columns = client.get(f"{PREFIX}/{board_id}/columns")

    assert board.status_code == 200, board.text
    assert columns.status_code == 200, columns.text
    assert card_id in {
        card["id"] for column in columns.json()["columns"].values() for card in column
    }


@pytest.mark.asyncio
async def test_unshared_board_read_and_columns_are_same_not_found(client) -> None:
    board_id = await _seed_board(owner=OTHER)
    missing_id = _missing()

    assert (
        client.get(f"{PREFIX}/{board_id}").content
        == client.get(f"{PREFIX}/{missing_id}").content
    )
    assert (
        client.get(f"{PREFIX}/{board_id}/columns").content
        == client.get(f"{PREFIX}/{missing_id}/columns").content
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("via_share", [False, True])
async def test_cross_realm_owner_or_share_is_not_board_access(
    client, via_share: bool
) -> None:
    tenant_realm = f"tenant-{uuid.uuid4().hex[:8]}"
    board_id = await _seed_board(
        owner=OTHER if via_share else USER,
        realm_id=tenant_realm,
    )
    if via_share:
        from sqlalchemy_test_models import BoardShare

        async with get_session_factory()() as db:
            db.add(
                BoardShare(
                    board_id=board_id,
                    user_id=USER,
                    realm_id=tenant_realm,
                    permission="admin",
                    shared_by=OTHER,
                )
            )
            await db.commit()

    foreign = client.get(f"{PREFIX}/{board_id}")
    missing = client.get(f"{PREFIX}/{_missing()}")
    assert foreign.status_code == missing.status_code == 404
    assert foreign.content == missing.content


@pytest.mark.asyncio
async def test_viewer_share_cannot_mutate_board_or_archive_tree(client) -> None:
    board_id, spec_id, _card_id = await _seed_board_spec_card(owner=OTHER)
    await _grant_share(board_id, "viewer")

    updated = client.patch(f"{PREFIX}/{board_id}", json={"name": "forbidden"})
    archived = client.post(f"{PREFIX}/{board_id}/archive/spec/{spec_id}")

    assert updated.status_code == 404
    assert archived.status_code == 404
    from sqlalchemy_test_models import Spec

    async with get_session_factory()() as db:
        assert (await db.get(Spec, spec_id)).archived is False


@pytest.mark.asyncio
async def test_editor_share_can_archive_but_not_update_board_metadata(client) -> None:
    board_id, spec_id, _card_id = await _seed_board_spec_card(owner=OTHER)
    await _grant_share(board_id, "editor")

    updated = client.patch(f"{PREFIX}/{board_id}", json={"name": "forbidden"})
    archived = client.post(f"{PREFIX}/{board_id}/archive/spec/{spec_id}")

    assert updated.status_code == 404
    assert archived.status_code == 200, archived.text


@pytest.mark.asyncio
async def test_share_board_201(client) -> None:
    board_id = await _seed_board()
    resp = _share(client, board_id, OTHER, "viewer")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["user_id"] == OTHER
    assert body["permission"] == "viewer"
    assert body["board_id"] == board_id


@pytest.mark.asyncio
async def test_share_board_with_self_403(client) -> None:
    board_id = await _seed_board()
    resp = _share(client, board_id, USER)
    assert resp.status_code == 403
    assert resp.json()["detail"] == (
        "Not authorized to share this board or invalid target user"
    )


@pytest.mark.asyncio
async def test_list_board_shares_200(client) -> None:
    board_id = await _seed_board()
    _share(client, board_id, OTHER, "editor")
    resp = client.get(f"{PREFIX}/{board_id}/shares")
    assert resp.status_code == 200, resp.text
    assert OTHER in {s["user_id"] for s in resp.json()}


@pytest.mark.asyncio
async def test_list_board_shares_no_access_404(client) -> None:
    # Board owned by someone else, no share for USER → no permission → 404.
    foreign_board = await _seed_board(owner=OTHER)
    resp = client.get(f"{PREFIX}/{foreign_board}/shares")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_update_board_share_200(client) -> None:
    board_id = await _seed_board()
    share_id = _share(client, board_id, OTHER, "viewer").json()["id"]
    resp = client.patch(
        f"{PREFIX}/{board_id}/shares/{share_id}", json={"permission": "admin"}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["permission"] == "admin"


@pytest.mark.asyncio
async def test_update_board_share_missing_404(client) -> None:
    board_id = await _seed_board()
    resp = client.patch(
        f"{PREFIX}/{board_id}/shares/{_missing('share')}", json={"permission": "admin"}
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board or share not found"


@pytest.mark.asyncio
async def test_revoke_board_share_204(client) -> None:
    board_id = await _seed_board()
    share_id = _share(client, board_id, OTHER).json()["id"]
    resp = client.delete(f"{PREFIX}/{board_id}/shares/{share_id}")
    assert resp.status_code == 204, resp.text


@pytest.mark.asyncio
async def test_revoke_board_share_missing_404(client) -> None:
    board_id = await _seed_board()
    resp = client.delete(f"{PREFIX}/{board_id}/shares/{_missing('share')}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Board or share not found"


@pytest.mark.asyncio
async def test_share_id_cannot_cross_the_board_route_parent(client) -> None:
    board_a = await _seed_board(name="share-parent-a")
    board_b = await _seed_board(name="share-parent-b")
    share_id = _share(client, board_a, OTHER, "viewer").json()["id"]

    update = client.patch(
        f"{PREFIX}/{board_b}/shares/{share_id}",
        json={"permission": "admin"},
    )
    revoke = client.delete(f"{PREFIX}/{board_b}/shares/{share_id}")

    assert update.status_code == revoke.status_code == 404
    assert update.json() == revoke.json() == {"detail": "Board or share not found"}
    shares = client.get(f"{PREFIX}/{board_a}/shares").json()
    assert (
        next(item for item in shares if item["id"] == share_id)["permission"]
        == "viewer"
    )


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_board_use_case_raises_for_missing_board() -> None:
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.application.use_cases.boards_crud import (
        GetBoardCommand,
        GetBoardUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest", realm_id=LOCAL_REALM_ID)
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetBoardUseCase().execute(
                GetBoardCommand(_missing()), actor=actor, uow=uow
            )


def test_fu7_s1_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(boards_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
