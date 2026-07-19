"""Authorization regression matrix for the REST Architecture surface."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select, update

from okto_pulse.community.api.architecture import router as architecture_router
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.application.use_cases.architecture_crud import (
    DeleteArchitectureDesignCommand,
    DeleteArchitectureDesignUseCase,
    GetArchitectureDesignCommand,
    GetArchitectureDesignUseCase,
    GetArchitectureDiagramPayloadCommand,
    GetArchitectureDiagramPayloadUseCase,
    GetArchitectureDiffCommand,
    GetArchitectureDiffUseCase,
    ImportExcalidrawArchitectureDiagramCommand,
    ImportExcalidrawArchitectureDiagramUseCase,
    UpdateArchitectureDesignCommand,
    UpdateArchitectureDesignUseCase,
    UpdateArchitectureDiagramPayloadCommand,
    UpdateArchitectureDiagramPayloadUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.schemas import ArchitectureDesignUpdate

PREFIX = "/api/v1"
ATTACKER = "architecture-scope-attacker"
OWNER = "architecture-scope-owner"
DIAGRAM_ID = "diagram-main"


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(architecture_router, prefix=PREFIX)
    app.dependency_overrides[require_user] = lambda: ATTACKER
    return TestClient(app)


def _architecture_body(title: str = "Scoped Architecture") -> dict[str, Any]:
    return {
        "title": title,
        "global_description": "Scope-safe architecture fixture.",
        "entities": [
            {
                "id": "client",
                "name": "Client",
                "entity_type": "web_app",
                "responsibility": "Call the service.",
            },
            {
                "id": "service",
                "name": "Pulse API",
                "entity_type": "service",
                "responsibility": "Serve requests.",
            },
        ],
        "interfaces": [
            {
                "id": "request",
                "name": "Request",
                "endpoint": "GET /resource",
                "protocol": "REST",
                "contract_type": "request_response",
                "participants": ["client", "service"],
            }
        ],
        "diagrams": [
            {
                "id": DIAGRAM_ID,
                "title": "Context",
                "diagram_type": "context",
                "format": "excalidraw_json",
                "adapter_payload": {
                    "type": "excalidraw",
                    "version": 2,
                    "elements": [
                        {
                            "id": "node-client",
                            "type": "rectangle",
                            "linkedEntityId": "client",
                        },
                        {
                            "id": "node-service",
                            "type": "rectangle",
                            "linkedEntityId": "service",
                        },
                        {
                            "id": "edge",
                            "type": "arrow",
                            "sourceElementId": "node-client",
                            "targetElementId": "node-service",
                            "linkedInterfaceId": "request",
                        },
                    ],
                    "appState": {},
                    "files": {},
                },
            }
        ],
    }


async def _seed_graph(*, owner_id: str = OWNER, shared: bool = False) -> dict[str, str]:
    from okto_pulse.core.services.architecture import ArchitectureDesignRepository
    from sqlalchemy_test_models import (
        Board,
        BoardShare,
        Card,
        Ideation,
        Refinement,
        Spec,
    )

    suffix = uuid.uuid4().hex[:10]
    ids = {
        "board": f"arch-board-{suffix}",
        "ideation": f"arch-ideation-{suffix}",
        "refinement": f"arch-refinement-{suffix}",
        "spec": f"arch-spec-{suffix}",
        "card": f"arch-card-{suffix}",
    }
    async with get_session_factory()() as db:
        db.add(Board(id=ids["board"], name="Architecture scope", owner_id=owner_id))
        db.add(
            Ideation(
                id=ids["ideation"],
                board_id=ids["board"],
                title="Ideation",
                created_by=owner_id,
            )
        )
        db.add(
            Refinement(
                id=ids["refinement"],
                board_id=ids["board"],
                ideation_id=ids["ideation"],
                title="Refinement",
                created_by=owner_id,
            )
        )
        db.add(
            Spec(
                id=ids["spec"],
                board_id=ids["board"],
                title="Spec",
                created_by=owner_id,
            )
        )
        db.add(
            Card(
                id=ids["card"],
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="Card",
                created_by=owner_id,
            )
        )
        if shared:
            db.add(
                BoardShare(
                    board_id=ids["board"],
                    user_id=ATTACKER,
                    realm_id="local",
                    permission="editor",
                    shared_by=owner_id,
                )
            )
        await db.commit()
        repo = ArchitectureDesignRepository(db)
        design = await repo.create(
            "ideation",
            ids["ideation"],
            _architecture_body(),
            owner_id,
        )
        ids["design"] = design.id
        await db.commit()
    return ids


async def _architecture_state(design_id: str) -> tuple[Any, ...]:
    from sqlalchemy_test_models import (
        ArchitectureDesign,
        ArchitectureDesignVersion,
        ArchitectureFinding,
        ArchitectureFindingRun,
        ArchitectureWarningAcknowledgement,
    )

    async with get_session_factory()() as db:
        design = await db.get(ArchitectureDesign, design_id)
        counts = []
        for model in (
            ArchitectureDesignVersion,
            ArchitectureFindingRun,
            ArchitectureFinding,
            ArchitectureWarningAcknowledgement,
        ):
            counts.append(
                (await db.execute(select(func.count()).select_from(model))).scalar_one()
            )
        return (design.version if design else None, bool(design), *counts)


def _missing(kind: str) -> str:
    return f"missing-{kind}-{uuid.uuid4().hex}"


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_type", ["ideation", "refinement", "spec", "card"])
@pytest.mark.parametrize("method", ["get", "post"])
async def test_foreign_parent_list_and_create_are_byte_equivalent_to_missing(
    client: TestClient,
    parent_type: str,
    method: str,
) -> None:
    ids = await _seed_graph()
    path = f"{PREFIX}/{parent_type}s/{ids[parent_type]}/architecture"
    missing_path = f"{PREFIX}/{parent_type}s/{_missing(parent_type)}/architecture"

    if method == "get":
        denied = client.get(path)
        missing = client.get(missing_path)
    else:
        denied = client.post(path, json=_architecture_body("Denied"))
        missing = client.post(missing_path, json=_architecture_body("Missing"))

    assert denied.status_code == missing.status_code == 404
    assert denied.content == missing.content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path_suffix", "kwargs"),
    [
        ("get", "", {}),
        ("patch", "", {"json": {"global_description": "denied"}}),
        ("delete", "", {}),
        ("get", f"/diagrams/{DIAGRAM_ID}/payload", {}),
        (
            "put",
            f"/diagrams/{DIAGRAM_ID}/payload",
            {"json": {"payload": {"type": "excalidraw", "version": 2, "elements": []}}},
        ),
        (
            "post",
            "/diagrams/import-excalidraw",
            {
                "json": {
                    "title": "Denied import",
                    "payload": {"type": "excalidraw", "version": 2, "elements": []},
                }
            },
        ),
        ("get", "/diff", {"params": {"from_version": 1, "to_version": 1}}),
    ],
)
async def test_foreign_design_surface_is_not_found_without_version_or_audit_mutation(
    client: TestClient,
    method: str,
    path_suffix: str,
    kwargs: dict[str, Any],
) -> None:
    ids = await _seed_graph()
    before = await _architecture_state(ids["design"])
    foreign_path = f"{PREFIX}/architecture/{ids['design']}{path_suffix}"
    missing_path = f"{PREFIX}/architecture/{_missing('design')}{path_suffix}"

    denied = client.request(method, foreign_path, **kwargs)
    missing = client.request(method, missing_path, **kwargs)

    assert denied.status_code == missing.status_code == 404
    assert denied.content == missing.content
    assert await _architecture_state(ids["design"]) == before


@pytest.mark.asyncio
async def test_foreign_propagation_report_is_byte_equivalent_to_missing_board(
    client: TestClient,
) -> None:
    ids = await _seed_graph()
    denied = client.get(
        f"{PREFIX}/architecture/propagation-legacy-report",
        params={"board_id": ids["board"]},
    )
    missing = client.get(
        f"{PREFIX}/architecture/propagation-legacy-report",
        params={"board_id": _missing("board")},
    )

    assert denied.status_code == missing.status_code == 404
    assert denied.content == missing.content == b'{"detail":"Board not found"}'


@pytest.mark.asyncio
async def test_foreign_copy_is_not_found_without_copy_version_or_audit(
    client: TestClient,
) -> None:
    from sqlalchemy import select
    from sqlalchemy_test_models import ArchitectureDesign

    ids = await _seed_graph()
    before = await _architecture_state(ids["design"])
    denied = client.post(
        f"{PREFIX}/cards/{ids['card']}/copy-architecture-from-spec/{ids['spec']}",
        json={},
    )
    missing = client.post(
        f"{PREFIX}/cards/{_missing('card')}/copy-architecture-from-spec/{ids['spec']}",
        json={},
    )

    assert denied.status_code == missing.status_code == 404
    assert denied.content == missing.content == b'{"detail":"card not found"}'
    async with get_session_factory()() as db:
        copied = (
            (
                await db.execute(
                    select(ArchitectureDesign).where(
                        ArchitectureDesign.card_id == ids["card"]
                    )
                )
            )
            .scalars()
            .all()
        )
    assert copied == []
    assert await _architecture_state(ids["design"]) == before


@pytest.mark.asyncio
async def test_copy_requires_the_card_linked_spec_and_same_board(
    client: TestClient,
) -> None:
    from okto_pulse.core.services.architecture import ArchitectureDesignRepository
    from sqlalchemy_test_models import ArchitectureDesign, Board, Card, Spec

    suffix = uuid.uuid4().hex[:10]
    board_a = f"copy-board-a-{suffix}"
    board_b = f"copy-board-b-{suffix}"
    spec_a = f"copy-spec-a-{suffix}"
    spec_a_other = f"copy-spec-a-other-{suffix}"
    spec_b = f"copy-spec-b-{suffix}"
    card_id = f"copy-card-{suffix}"
    async with get_session_factory()() as db:
        db.add_all(
            [
                Board(id=board_a, name="A", owner_id=ATTACKER),
                Board(id=board_b, name="B", owner_id=ATTACKER),
                Spec(id=spec_a, board_id=board_a, title="A", created_by=ATTACKER),
                Spec(
                    id=spec_a_other,
                    board_id=board_a,
                    title="A other",
                    created_by=ATTACKER,
                ),
                Spec(id=spec_b, board_id=board_b, title="B", created_by=ATTACKER),
                Card(
                    id=card_id,
                    board_id=board_a,
                    spec_id=spec_a,
                    title="A card",
                    created_by=ATTACKER,
                ),
            ]
        )
        await db.commit()
        repo = ArchitectureDesignRepository(db)
        source = await repo.create(
            "spec", spec_b, _architecture_body("Foreign source"), ATTACKER
        )
        await repo.create(
            "spec", spec_a_other, _architecture_body("Unlinked source"), ATTACKER
        )
        await db.commit()

    cross_board = client.post(
        f"{PREFIX}/cards/{card_id}/copy-architecture-from-spec/{spec_b}", json={}
    )
    wrong_parent = client.post(
        f"{PREFIX}/cards/{card_id}/copy-architecture-from-spec/{spec_a_other}", json={}
    )
    assert cross_board.status_code == wrong_parent.status_code == 404
    assert cross_board.content == wrong_parent.content == b'{"detail":"spec not found"}'
    async with get_session_factory()() as db:
        copied_count = (
            await db.execute(
                select(func.count())
                .select_from(ArchitectureDesign)
                .where(ArchitectureDesign.card_id == card_id)
            )
        ).scalar_one()
        assert await db.get(ArchitectureDesign, source.id) is not None
    assert copied_count == 0


@pytest.mark.asyncio
async def test_shared_editor_preserves_happy_rest_list_create_and_get(
    client: TestClient,
) -> None:
    ids = await _seed_graph(shared=True)

    listing = client.get(f"{PREFIX}/ideations/{ids['ideation']}/architecture")
    created = client.post(
        f"{PREFIX}/ideations/{ids['ideation']}/architecture",
        json=_architecture_body("Shared editor design"),
    )
    fetched = client.get(f"{PREFIX}/architecture/{ids['design']}")

    assert listing.status_code == 200
    assert [item["id"] for item in listing.json()] == [ids["design"]]
    assert created.status_code == 201, created.text
    assert fetched.status_code == 200


@pytest.mark.asyncio
async def test_inconsistent_design_parent_board_is_hidden_from_get_and_list(
    client: TestClient,
) -> None:
    from sqlalchemy_test_models import ArchitectureDesign, Board

    ids = await _seed_graph(owner_id=ATTACKER)
    other_board = f"other-{uuid.uuid4().hex}"
    async with get_session_factory()() as db:
        db.add(Board(id=other_board, name="Other", owner_id=ATTACKER))
        await db.execute(
            update(ArchitectureDesign)
            .where(ArchitectureDesign.id == ids["design"])
            .values(board_id=other_board)
        )
        await db.commit()

    fetched = client.get(f"{PREFIX}/architecture/{ids['design']}")
    listing = client.get(f"{PREFIX}/ideations/{ids['ideation']}/architecture")

    assert fetched.status_code == 404
    assert fetched.json() == {"detail": "Architecture design not found"}
    assert listing.status_code == 200
    assert listing.json() == []


class _Boards:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def get(self, board_id: str) -> Any:
        self.events.append("board")
        return SimpleNamespace(id=board_id, owner_id=OWNER)


class _Shares:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def get_user_permission(self, board_id: str, actor_id: str) -> None:
        self.events.append("share")
        return None


class _ArchitectureRepo:
    def __init__(self, events: list[str], design: Any) -> None:
        self.events = events
        self.design = design

    async def get(self, design_id: str, include_payloads: bool = False) -> Any:
        self.events.append(f"design:{include_payloads}")
        return self.design

    def parent_id_for(self, design: Any) -> str:
        return design.ideation_id

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        self.events.append("update")
        return self.design

    async def delete(self, *args: Any, **kwargs: Any) -> bool:
        self.events.append("delete")
        return True

    async def diff(self, *args: Any, **kwargs: Any) -> Any:
        self.events.append("diff")
        return {}

    def to_response(self, design: Any) -> Any:
        return design


@dataclass
class _DeniedUow:
    events: list[str]
    services: Any
    boards: Any


def _denied_uow() -> _DeniedUow:
    events: list[str] = []
    design = SimpleNamespace(
        id="design-b",
        board_id="board-b",
        parent_type="ideation",
        ideation_id="ideation-b",
        diagrams=[{"id": DIAGRAM_ID, "adapter_payload_ref": "secret"}],
    )
    services = SimpleNamespace(
        architecture_designs=_ArchitectureRepo(events, design),
        architecture_diagrams=SimpleNamespace(),
        shares=_Shares(events),
    )
    return _DeniedUow(events, services, _Boards(events))


def _design_cases() -> list[tuple[Any, Any]]:
    return [
        (
            GetArchitectureDesignUseCase(),
            GetArchitectureDesignCommand("design-b", True),
        ),
        (
            UpdateArchitectureDesignUseCase(),
            UpdateArchitectureDesignCommand(
                "design-b", ArchitectureDesignUpdate(title="x")
            ),
        ),
        (
            DeleteArchitectureDesignUseCase(),
            DeleteArchitectureDesignCommand("design-b"),
        ),
        (
            GetArchitectureDiagramPayloadUseCase(),
            GetArchitectureDiagramPayloadCommand("design-b", DIAGRAM_ID),
        ),
        (
            UpdateArchitectureDiagramPayloadUseCase(),
            UpdateArchitectureDiagramPayloadCommand(
                "design-b", DIAGRAM_ID, "raw", {}, None, None
            ),
        ),
        (
            ImportExcalidrawArchitectureDiagramUseCase(),
            ImportExcalidrawArchitectureDiagramCommand(
                "design-b", "x", {}, "other", None, 0, None, None, None
            ),
        ),
        (GetArchitectureDiffUseCase(), GetArchitectureDiffCommand("design-b", 1, 2)),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("use_case", "command"), _design_cases())
async def test_denied_design_stops_before_payload_version_or_mutation(
    use_case: Any,
    command: Any,
) -> None:
    uow = _denied_uow()

    with pytest.raises(EntityNotFoundError):
        await use_case.execute(
            command,
            actor=ActorContext(ATTACKER, "rest"),
            uow=uow,
        )

    assert uow.events == ["design:False", "board", "share"]
