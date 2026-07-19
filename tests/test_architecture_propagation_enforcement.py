"""Spec B — enforcement of the canonical propagation eligibility across all copy/
propagation call-sites.

Covers:
- TS-B1 (negative): copy_from_parent blocks a blocking source before any create/update.
- TS-B2 (integration): copy_spec_to_card and copy_effective_spec_to_card surface the
  same canonical error (REST/MCP convergence lives in test_architecture_rest.py /
  test_architecture_mcp.py).
- TS-B3 (integration): propagate_architecture_designs (derive flows, incl. ideation->
  refinement / main.py:7894) blocks before creating a downstream snapshot.
- TS-B4 (integration): propagate_effective_resources_to_spec fails closed without
  creating a copied design on the spec.
- TS-B5 (negative): SpecResourcePropagationService._copy_architecture blocks the
  repository.update refresh branch before mutating an existing target.
- TS-B6 (negative): _copy_architecture blocks the copy_spec_to_card creation branch
  before creating a target.
- TS-B7 (e2e): a clean source propagates and preserves source identity.

Spec B uses fail-closed RAISE on every path (explicit and auto/bulk) — the Spec C
Resource Gate backstop for a controlled skip is not yet active.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import uuid

import pytest
from sqlalchemy import func, select

from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    Refinement,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import (
    ArchitectureDesignCreate,
    ArchitectureWarningAcknowledgementRequest,
)
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitectureDesignSelectionError,
    ArchitecturePropagationBlocked,
    ArchitecturePropagationService,
)
from okto_pulse.core.services.effective_resource_propagation import (
    ResourcePropagationError,
    propagate_effective_resources_to_spec,
)
from okto_pulse.core.services.main import propagate_architecture_designs
from okto_pulse.core.services.spec_resource_propagation import SpecResourcePropagationService

USER_ID = "arch-propagation-enforcement-user"

CANONICAL_CODE = "architecture_propagation_blocked"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _ack() -> ArchitectureWarningAcknowledgementRequest:
    return ArchitectureWarningAcknowledgementRequest(
        accepted=True, statement="author acknowledges the warnings"
    )


def _blocking_create() -> ArchitectureDesignCreate:
    """An API entity with no covering diagram — the critic flags it as a structured
    warning (active finding), so the design is INELIGIBLE for propagation. The author
    acknowledgement lets it be SAVED but never authorizes propagating it."""
    return ArchitectureDesignCreate(
        title="Blocking arch",
        global_description="An API entity with no diagram — the critic flags it.",
        entities=[
            {"id": "svc-api", "name": "Demo API", "entity_type": "api",
             "responsibility": "Handles demo traffic."}
        ],
        interfaces=[],
        diagrams=[],
        architecture_warning_acknowledgement=_ack(),
    )


_CLEAN_ENTITIES = [
    {"id": "customer-portal", "name": "Customer Portal", "entity_type": "web_app",
     "responsibility": "Sends checkout data to the API."},
    {"id": "checkout-api", "name": "Checkout API", "entity_type": "api",
     "responsibility": "Handles checkout and orders."},
]
_CLEAN_INTERFACES = [
    {"id": "create-order", "name": "Create order", "endpoint": "POST /orders",
     "description": "Portal sends checkout data to Checkout API.",
     "direction": "source_to_target", "protocol": "REST", "contract_type": "OpenAPI",
     "source_entity_id": "customer-portal", "target_entity_id": "checkout-api"},
]
_CLEAN_DIAGRAMS = [
    {"id": "diagram-runtime", "title": "Runtime context", "diagram_type": "context",
     "format": "excalidraw_json",
     "adapter_payload": {"type": "excalidraw", "version": 2, "elements": [
         {"id": "node-customer-portal", "type": "rectangle", "label": "Customer Portal",
          "linkedEntityId": "customer-portal"},
         {"id": "node-checkout-api", "type": "rectangle", "label": "Checkout API",
          "linkedEntityId": "checkout-api"},
         {"id": "edge-create-order", "type": "arrow", "sourceElementId": "node-customer-portal",
          "targetElementId": "node-checkout-api", "linkedInterfaceIds": ["create-order"]},
     ], "appState": {}, "files": {}}},
]


def _clean_create() -> ArchitectureDesignCreate:
    return ArchitectureDesignCreate(
        title="Clean arch",
        global_description="Two connected services, eligible for propagation.",
        entities=[dict(e) for e in _CLEAN_ENTITIES],
        interfaces=[dict(i) for i in _CLEAN_INTERFACES],
        diagrams=[dict(d) for d in _CLEAN_DIAGRAMS],
    )


async def _seed_spec_card(db_factory) -> tuple[str, str, str]:
    """Minimal board (auto-derive OFF by default) so repo.create does not auto-propagate
    while these tests drive the propagation call-sites explicitly."""
    board_id = _id("propenf-board")
    spec_id = _id("propenf-spec")
    card_id = _id("propenf-card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Propagation Enforcement Board", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=SpecStatus.APPROVED,
                    created_by=USER_ID, functional_requirements=["FR"], acceptance_criteria=["AC"],
                    test_scenarios=[], business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="card",
                    status=CardStatus.NOT_STARTED, card_type=CardType.NORMAL, created_by=USER_ID))
        await db.commit()
    return board_id, spec_id, card_id


async def _seed_ideation_refinement_spec(db_factory) -> tuple[str, str, str, str]:
    board_id = _id("propenf-board")
    ideation_id = _id("propenf-ideation")
    refinement_id = _id("propenf-refinement")
    spec_id = _id("propenf-spec")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Propagation Enforcement Board", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="ideation", created_by=USER_ID))
        db.add(Refinement(id=refinement_id, board_id=board_id, ideation_id=ideation_id,
                          title="refinement", created_by=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, refinement_id=refinement_id, title="spec",
                    created_by=USER_ID, functional_requirements=[], technical_requirements=[],
                    acceptance_criteria=[], test_scenarios=[], business_rules=[], api_contracts=[]))
        await db.commit()
    return board_id, ideation_id, refinement_id, spec_id


async def _count_card_designs(db_factory, card_id: str) -> int:
    async with db_factory() as db:
        return (await db.execute(
            select(func.count()).select_from(ArchitectureDesign)
            .where(ArchitectureDesign.parent_type == "card", ArchitectureDesign.card_id == card_id)
        )).scalar_one()


# --------------------------------------------------------------------------- #
# TS-B1: copy_from_parent blocks before any create/update.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b1_copy_from_parent_blocks_before_create(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create("spec", spec_id, _blocking_create(), USER_ID)
        await db.commit()

    async with db_factory() as db:
        service = ArchitecturePropagationService(db)
        with pytest.raises(ArchitecturePropagationBlocked) as exc:
            await service.copy_from_parent("spec", spec_id, "card", card_id, USER_ID)
    assert exc.value.to_payload()["code"] == CANONICAL_CODE
    assert await _count_card_designs(db_factory, card_id) == 0


# --------------------------------------------------------------------------- #
# TS-B2: explicit copy services share the same canonical error.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b2_copy_paths_share_canonical_error(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create("spec", spec_id, _blocking_create(), USER_ID)
        await db.commit()

    async with db_factory() as db:
        service = ArchitecturePropagationService(db)
        with pytest.raises(ArchitecturePropagationBlocked) as e_spec_to_card:
            await service.copy_spec_to_card(spec_id, card_id, USER_ID)
    async with db_factory() as db:
        service = ArchitecturePropagationService(db)
        with pytest.raises(ArchitecturePropagationBlocked) as e_effective:
            await service.copy_effective_spec_to_card(
                board_id=board_id, spec_id=spec_id, card_id=card_id, actor_id=USER_ID,
            )

    p1 = e_spec_to_card.value.to_payload()
    p2 = e_effective.value.to_payload()
    assert p1["code"] == p2["code"] == CANONICAL_CODE
    assert p1["source_design_id"] == p2["source_design_id"]
    # Even an explicit copy-scoped acknowledgement is audit-only and does not bypass.
    async with db_factory() as db:
        service = ArchitecturePropagationService(db)
        with pytest.raises(ArchitecturePropagationBlocked):
            await service.copy_spec_to_card(
                spec_id, card_id, USER_ID,
                architecture_warning_acknowledgement={"accepted": True, "statement": "copy ack"},
            )
    assert await _count_card_designs(db_factory, card_id) == 0


# --------------------------------------------------------------------------- #
# TS-B3: propagate_architecture_designs blocks derive flows before downstream snapshot.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b3_propagate_architecture_designs_blocks_derive(db_factory):
    board_id, ideation_id, refinement_id, _spec_id = await _seed_ideation_refinement_spec(db_factory)
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create("ideation", ideation_id, _blocking_create(), USER_ID)
        await db.commit()

    # ideation -> refinement (the main.py:7894 derive path) must block before snapshot.
    async with db_factory() as db:
        with pytest.raises(ArchitecturePropagationBlocked) as exc:
            await propagate_architecture_designs(
                db, source_parent_type="ideation", source_parent_id=ideation_id,
                target_parent_type="refinement", target_parent_id=refinement_id,
                actor_id=USER_ID, mode="copy",
            )
    assert exc.value.to_payload()["code"] == CANONICAL_CODE
    async with db_factory() as db:
        count = (await db.execute(
            select(func.count()).select_from(ArchitectureDesign)
            .where(ArchitectureDesign.parent_type == "refinement",
                   ArchitectureDesign.refinement_id == refinement_id)
        )).scalar_one()
    assert count == 0


@pytest.mark.asyncio
async def test_multihop_selection_resolves_physical_root_ref_and_resource_gate_tokens(
    db_factory,
):
    _board_id, ideation_id, refinement_id, spec_id = (
        await _seed_ideation_refinement_spec(db_factory)
    )
    async with db_factory() as db:
        root = await ArchitectureDesignRepository(db).create(
            "ideation", ideation_id, _clean_create(), USER_ID
        )
        await db.commit()
        root_id = root.id
    async with db_factory() as db:
        copied = await ArchitecturePropagationService(db).copy_from_parent(
            "ideation", ideation_id, "refinement", refinement_id, USER_ID
        )
        await db.commit()
        intermediate_id = copied[0].id
        intermediate_source_ref = copied[0].source_ref

    # Reopen between every hop: this exercises persisted lineage, not identity-map
    # state. Every supported token must converge on the same target snapshot.
    for token in (
        intermediate_id,
        root_id,
        intermediate_source_ref,
        f"architecture:{root_id}",
        f"architecture_design:{root_id}",
    ):
        async with db_factory() as db:
            copied = await ArchitecturePropagationService(db).copy_from_parent(
                "refinement",
                refinement_id,
                "spec",
                spec_id,
                USER_ID,
                design_ids=[token],
            )
            await db.commit()
            assert len(copied) == 1
            assert copied[0].source_design_id == root_id

    async with db_factory() as db:
        rows = (
            await db.execute(
                select(ArchitectureDesign).where(
                    ArchitectureDesign.parent_type == "spec",
                    ArchitectureDesign.spec_id == spec_id,
                )
            )
        ).scalars().all()
    assert len(rows) == 1
    assert rows[0].source_design_id == root_id


@pytest.mark.asyncio
async def test_explicit_mixed_valid_and_foreign_selection_fails_atomically(db_factory):
    _board_id, ideation_id, refinement_id, _spec_id = (
        await _seed_ideation_refinement_spec(db_factory)
    )
    second_spec_id = _id("propenf-spec-target")
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create(
            "ideation", ideation_id, _clean_create(), USER_ID
        )
        db.add(
            Spec(
                id=second_spec_id,
                board_id=_board_id,
                refinement_id=refinement_id,
                title="atomic target",
                created_by=USER_ID,
            )
        )
        await db.commit()
    async with db_factory() as db:
        copied = await ArchitecturePropagationService(db).copy_from_parent(
            "ideation", ideation_id, "refinement", refinement_id, USER_ID
        )
        await db.commit()
        valid_id = copied[0].id

    foreign_id = "architecture:foreign-root"
    async with db_factory() as db:
        with pytest.raises(ArchitectureDesignSelectionError) as caught:
            await ArchitecturePropagationService(db).copy_from_parent(
                "refinement",
                refinement_id,
                "spec",
                second_spec_id,
                USER_ID,
                design_ids=[valid_id, foreign_id],
            )
        payload = caught.value.to_error_dict()
        assert payload["requested"] == [valid_id, foreign_id]
        assert payload["matched"] == [valid_id]
        assert payload["missing"] == [foreign_id]

    async with db_factory() as db:
        count = (
            await db.execute(
                select(func.count())
                .select_from(ArchitectureDesign)
                .where(
                    ArchitectureDesign.parent_type == "spec",
                    ArchitectureDesign.spec_id == second_spec_id,
                )
            )
        ).scalar_one()
    assert count == 0


# --------------------------------------------------------------------------- #
# TS-B4: propagate_effective_resources_to_spec fails closed without copying a design.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b4_effective_resources_to_spec_fails_closed(db_factory):
    board_id, _ideation_id, refinement_id, spec_id = await _seed_ideation_refinement_spec(db_factory)
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create("refinement", refinement_id, _blocking_create(), USER_ID)
        await db.commit()

    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        with pytest.raises((ResourcePropagationError, ArchitecturePropagationBlocked)):
            await propagate_effective_resources_to_spec(
                db, board_id=board_id, spec=spec, refinement_id=refinement_id,
                user_id=USER_ID, architecture_propagation_mode="copy",
            )

    async with db_factory() as db:
        count = (await db.execute(
            select(func.count()).select_from(ArchitectureDesign)
            .where(ArchitectureDesign.parent_type == "spec", ArchitectureDesign.spec_id == spec_id)
        )).scalar_one()
    assert count == 0


# --------------------------------------------------------------------------- #
# TS-B5: _copy_architecture blocks the repository.update refresh branch.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b5_copy_architecture_update_branch_blocks(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        source = await repo.create("spec", spec_id, _blocking_create(), USER_ID)
        # Manually seed an EXISTING card target at a DIFFERENT source_version (drift) so
        # _copy_architecture would take the repository.update refresh branch.
        target_id = _id("propenf-target")
        db.add(ArchitectureDesign(
            id=target_id, board_id=board_id, parent_type="card", card_id=card_id,
            title="stale snapshot", global_description="stale snapshot of the source.",
            entities=[], interfaces=[], diagrams=[], version=1,
            source_ref=f"architecture_design:{source.id}", source_version=0,
            source_design_id=source.id, created_by=USER_ID,
        ))
        await db.commit()

    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        card = await db.get(Card, card_id)
        with pytest.raises(ArchitecturePropagationBlocked):
            await SpecResourcePropagationService(db)._copy_architecture(spec, card, USER_ID)

    # The stale target was not mutated (its source_version is still 0).
    async with db_factory() as db:
        target = await db.get(ArchitectureDesign, target_id)
        assert target is not None
        assert target.source_version == 0


# --------------------------------------------------------------------------- #
# TS-B6: _copy_architecture blocks the copy_spec_to_card creation branch.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b6_copy_architecture_create_branch_blocks(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create("spec", spec_id, _blocking_create(), USER_ID)
        await db.commit()

    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        card = await db.get(Card, card_id)
        with pytest.raises(ArchitecturePropagationBlocked):
            await SpecResourcePropagationService(db)._copy_architecture(spec, card, USER_ID)

    assert await _count_card_designs(db_factory, card_id) == 0


# --------------------------------------------------------------------------- #
# TS-B7: a clean source propagates and preserves source identity.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b7_clean_source_propagates_preserving_identity(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        source = await ArchitectureDesignRepository(db).create("spec", spec_id, _clean_create(), USER_ID)
        source_id = source.id
        source_version = source.version
        await db.commit()

    async with db_factory() as db:
        service = ArchitecturePropagationService(db)
        copied = await service.copy_spec_to_card(spec_id, card_id, USER_ID)
        await db.commit()

    assert len(copied) == 1
    async with db_factory() as db:
        card_designs = (await db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == "card", ArchitectureDesign.card_id == card_id)
        )).scalars().all()
    assert len(card_designs) == 1
    target = card_designs[0]
    assert target.source_design_id == source_id
    assert target.source_ref == f"architecture_design:{source_id}"
    assert target.source_version == source_version


# --------------------------------------------------------------------------- #
# TS-B2 (REST): the REST copy endpoint serializes the canonical error (422).
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b2_rest_copy_returns_canonical_error(db_factory):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okto_pulse.community.api.architecture import router as architecture_router
    from okto_pulse.community.api import auth_deps as _auth_mod
    from okto_pulse.core.infra.database import get_db

    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create("spec", spec_id, _blocking_create(), USER_ID)
        await db.commit()

    app = FastAPI()
    app.include_router(architecture_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"
    client = TestClient(app)

    resp = client.post(f"/api/v1/cards/{card_id}/copy-architecture-from-spec/{spec_id}")
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail["code"] == CANONICAL_CODE
    assert detail["finding_keys"]


# --------------------------------------------------------------------------- #
# TS-B2 (MCP): the MCP copy tool returns the canonical error JSON.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_b2_mcp_copy_returns_canonical_error(db_factory):
    import json
    from unittest.mock import AsyncMock, patch

    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.mcp import server as mcp_server

    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create("spec", spec_id, _blocking_create(), USER_ID)
        await db.commit()

    ctx = type("Ctx", (), {
        "agent_id": USER_ID, "agent_name": "enforcement-agent", "board_id": board_id,
        "permissions": ["board:read", "cards:update", "specs:update"],
    })()
    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool("okto_pulse_copy_architecture_to_card")
        raw = await tool.fn(board_id=board_id, spec_id=spec_id, card_id=card_id)

    payload = json.loads(raw)
    assert payload["code"] == CANONICAL_CODE
    assert payload.get("finding_keys")
