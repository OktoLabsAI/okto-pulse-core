"""Spec C — Resource Gate fail-closed, remediation, and read-only legacy diagnostic.

Covers:
- TS-C1 (negative): Resource Gate surfaces an inherited-source-ineligible block + remediation,
  WITHOUT marking architecture N/A.
- TS-C2 (integration): the blocked copy payload is the canonical structured error (all fields).
- TS-C3 (manual->doc): docs state acknowledgement is audit-only / not a propagation bypass.
- TS-C4 (integration): the legacy diagnostic lists problematic snapshots read-only (no mutation).
- TS-C5 (e2e): correcting the source unblocks propagation and preserves source identity.
- TS-C6 (unit): resolved/superseded findings and valid suppressed warnings do not block (AFG.01).
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import pathlib
import uuid

import pytest
from sqlalchemy import func, select

from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import (
    ArchitectureDesignCreate,
    ArchitectureDesignUpdate,
    ArchitectureWarningAcknowledgementRequest,
)
from okto_pulse.core.services.architecture import (
    PROPAGATION_VERDICT_CURRENT,
    ArchitectureDesignRepository,
    ArchitecturePropagationBlocked,
    ArchitecturePropagationService,
    build_propagation_eligibility,
)
from okto_pulse.core.services.architecture_propagation_legacy import (
    LEGACY_STATUS_SOURCE_BLOCKED,
    build_propagation_legacy_report,
)
from okto_pulse.core.services.resource_gate import ResourceGateService

USER_ID = "arch-propagation-legacy-user"
_CORE = pathlib.Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _ack() -> ArchitectureWarningAcknowledgementRequest:
    return ArchitectureWarningAcknowledgementRequest(accepted=True, statement="author ack")


def _blocking_create() -> ArchitectureDesignCreate:
    return ArchitectureDesignCreate(
        title="Blocking arch",
        global_description="An API entity with no diagram — the critic flags it.",
        entities=[{"id": "svc-api", "name": "Demo API", "entity_type": "api",
                   "responsibility": "Handles demo traffic."}],
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


def _clean_update() -> ArchitectureDesignUpdate:
    return ArchitectureDesignUpdate(
        title="Clean arch",
        global_description="Two connected services, eligible for propagation.",
        entities=[dict(e) for e in _CLEAN_ENTITIES],
        interfaces=[dict(i) for i in _CLEAN_INTERFACES],
        diagrams=[dict(d) for d in _CLEAN_DIAGRAMS],
        change_summary="Fix the source so the critic no longer flags it.",
    )


async def _seed_spec_card(db_factory) -> tuple[str, str, str]:
    board_id = _id("propc-board")
    spec_id = _id("propc-spec")
    card_id = _id("propc-card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Propagation Legacy Board", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=SpecStatus.APPROVED,
                    created_by=USER_ID, functional_requirements=["FR"], acceptance_criteria=["AC"],
                    test_scenarios=[], business_rules=[], api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="card",
                    status=CardStatus.NOT_STARTED, card_type=CardType.NORMAL, created_by=USER_ID))
        await db.commit()
    return board_id, spec_id, card_id


# --------------------------------------------------------------------------- #
# TS-C1: Resource Gate surfaces inherited-source-ineligible block + remediation.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_c1_resource_gate_blocks_inherited_ineligible_without_na(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).create("spec", spec_id, _blocking_create(), USER_ID)
        await db.commit()

    async with db_factory() as db:
        summary = await ResourceGateService(db).get_summary(board_id, "card", card_id)

    propagation = summary["architecture_propagation"]
    assert summary["architecture_propagation_blocking"] is True
    assert propagation["blocking"] is True
    assert propagation["remediation"]
    assert "N/A" in propagation["remediation"]  # explicitly tells operator NOT to mark N/A
    assert propagation["ineligible_sources"]
    assert propagation["ineligible_sources"][0]["code"] == "architecture_propagation_blocked"
    # The gate did not auto-mark architecture not_applicable.
    arch_state = next(r for r in summary["resources"] if r["resource_type"] == "architecture")
    assert arch_state["state"] != "not_applicable"
    # And there is a structured warning the agent can act on.
    assert any(w["code"] == "architecture_propagation_blocked_on_inherited" for w in summary["warnings"])


# --------------------------------------------------------------------------- #
# TS-C2: the blocked copy payload is the canonical structured error (all fields).
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_c2_blocked_copy_payload_is_fully_structured(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        source = await ArchitectureDesignRepository(db).create("spec", spec_id, _blocking_create(), USER_ID)
        source_id = source.id
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(ArchitecturePropagationBlocked) as exc:
            await ArchitecturePropagationService(db).copy_spec_to_card(spec_id, card_id, USER_ID)

    payload = exc.value.to_payload()
    for field in ("code", "design_id", "source_design_id", "source_ref", "source_version",
                  "parent_source", "critic_run_id", "design_version", "finding_keys", "issues",
                  "warnings", "verdict_status", "remediation"):
        assert field in payload, f"missing canonical field {field}"
    assert payload["code"] == "architecture_propagation_blocked"
    # The blocking SOURCE design (the thing being copied) is identified by design_id;
    # parent_source locates it; source_design_id is its own lineage (None when authored).
    assert payload["design_id"] == source_id
    assert payload["parent_source"]["parent_type"] == "spec"
    assert payload["parent_source"]["parent_id"] == spec_id
    assert payload["finding_keys"]
    assert payload["remediation"]


# --------------------------------------------------------------------------- #
# TS-C3 (manual): docs state acknowledgement is audit-only / not a propagation bypass.
# --------------------------------------------------------------------------- #
def test_ts_c3_docs_state_ack_is_not_a_propagation_bypass():
    errors_md = (_CORE / "mcp" / "resources" / "reference" / "errors.md").read_text(encoding="utf-8")
    tool_docs = (_CORE / "mcp" / "resources" / "reference" / "tool-docs" / "architecture.md").read_text(encoding="utf-8")

    assert "architecture_propagation_blocked" in errors_md
    assert "AUDIT-ONLY" in errors_md and "does NOT authorize the copy" in errors_md

    assert "audit-only" in tool_docs.lower()
    assert "NOT a propagation bypass" in tool_docs
    assert "okto_pulse_list_architecture_propagation_legacy" in tool_docs


# --------------------------------------------------------------------------- #
# TS-C4: the legacy diagnostic lists problematic snapshots read-only (no mutation).
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_c4_legacy_report_is_read_only(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        source = await repo.create("spec", spec_id, _clean_create(), USER_ID)
        source_id = source.id
        # Copy the (clean) source to the card → a legacy snapshot with source_design_id.
        copied = await ArchitecturePropagationService(db).copy_spec_to_card(spec_id, card_id, USER_ID)
        target_id = copied[0].id
        await db.commit()

    # Now the source becomes ineligible (re-authored with a warning-bearing payload + ack).
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).update(source_id, _blocking_create_update(), USER_ID)
        await db.commit()

    before = await _count_arch_designs(db_factory, board_id)
    async with db_factory() as db:
        report = await build_propagation_legacy_report(db, board_id=board_id)
    after = await _count_arch_designs(db_factory, board_id)

    assert report["mutation_performed"] is False
    assert before == after  # read-only: no rows created/removed
    item = next(i for i in report["items"] if i["target_design_id"] == target_id)
    assert item["source_design_id"] == source_id
    assert item["legacy_status"] == LEGACY_STATUS_SOURCE_BLOCKED
    assert item["finding_keys"]
    assert item["mutation_performed"] is False


# --------------------------------------------------------------------------- #
# TS-C5 (e2e): correcting the source unblocks propagation, preserving identity.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_c5_corrected_source_unblocks_and_preserves_identity(db_factory):
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        source = await ArchitectureDesignRepository(db).create("spec", spec_id, _blocking_create(), USER_ID)
        source_id = source.id
        await db.commit()

    # While the source is ineligible, the copy is blocked.
    async with db_factory() as db:
        with pytest.raises(ArchitecturePropagationBlocked):
            await ArchitecturePropagationService(db).copy_spec_to_card(spec_id, card_id, USER_ID)

    # Correct the source so the critic no longer emits blocking findings.
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).update(source_id, _clean_update(), USER_ID)
        await db.commit()

    async with db_factory() as db:
        copied = await ArchitecturePropagationService(db).copy_spec_to_card(spec_id, card_id, USER_ID)
        await db.commit()
    assert len(copied) == 1

    async with db_factory() as db:
        target = (await db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == "card", ArchitectureDesign.card_id == card_id)
        )).scalars().one()
    assert target.source_design_id == source_id
    assert target.source_ref == f"architecture_design:{source_id}"


# --------------------------------------------------------------------------- #
# TS-C6 (unit): resolved/superseded/suppressed do not block (AFG.01 taxonomy).
# --------------------------------------------------------------------------- #
def test_ts_c6_resolved_superseded_suppressed_do_not_block():
    eligibility = build_propagation_eligibility(
        design_id="design-1",
        source_design_id=None, source_ref=None, source_version=None,
        parent_type="spec", parent_id="spec-1",
        design_version=2, critic_run_id="critic-1",
        verdict_status=PROPAGATION_VERDICT_CURRENT, revalidation_reason=None,
        issues=[], blocking_warnings=[],
        suppressed_warnings=[{"code": "conceptual_runtime_only", "justification": "valid"}],
        resolved_count=3, superseded_count=2,
    )
    assert eligibility.eligible is True
    assert eligibility.blocker_counts["total"] == 0
    assert eligibility.non_blocking["resolved_findings_count"] == 3
    assert eligibility.non_blocking["superseded_findings_count"] == 2
    assert eligibility.non_blocking["suppressed_warnings_count"] == 1
    assert eligibility.remediation is None


# --------------------------------------------------------------------------- #
# Helpers used by TS-C4/TS-C5.
# --------------------------------------------------------------------------- #
def _blocking_create_update() -> ArchitectureDesignUpdate:
    return ArchitectureDesignUpdate(
        title="Blocking arch",
        global_description="An API entity with no diagram — the critic flags it.",
        entities=[{"id": "svc-api", "name": "Demo API", "entity_type": "api",
                   "responsibility": "Handles demo traffic."}],
        interfaces=[],
        diagrams=[],
        change_summary="Re-author with a warning-bearing payload.",
        architecture_warning_acknowledgement=_ack(),
    )


async def _count_arch_designs(db_factory, board_id: str) -> int:
    async with db_factory() as db:
        return (await db.execute(
            select(func.count()).select_from(ArchitectureDesign)
            .where(ArchitectureDesign.board_id == board_id)
        )).scalar_one()


async def _seed_legacy_snapshot(db_factory) -> tuple[str, str, str]:
    """Board + spec + card with a card snapshot whose source is now ineligible."""
    board_id, spec_id, card_id = await _seed_spec_card(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        source = await repo.create("spec", spec_id, _clean_create(), USER_ID)
        source_id = source.id
        await ArchitecturePropagationService(db).copy_spec_to_card(spec_id, card_id, USER_ID)
        await db.commit()
    async with db_factory() as db:
        await ArchitectureDesignRepository(db).update(source_id, _blocking_create_update(), USER_ID)
        await db.commit()
    return board_id, spec_id, source_id


# --------------------------------------------------------------------------- #
# TS-C4 (IR-C1 / api contract): the MCP + REST legacy report twins.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_ts_c4_mcp_legacy_report_twin(db_factory):
    import json
    from unittest.mock import AsyncMock, patch

    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.mcp import server as mcp_server

    board_id, _spec_id, source_id = await _seed_legacy_snapshot(db_factory)

    ctx = type("Ctx", (), {"agent_id": USER_ID, "agent_name": "legacy-agent",
                           "board_id": board_id, "permissions": ["board:read"]})()
    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)), \
         patch.object(mcp_server, "_mcp_check_architecture_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool("okto_pulse_list_architecture_propagation_legacy")
        raw = await tool.fn(board_id=board_id)
    payload = json.loads(raw)

    assert payload["success"] is True
    assert payload["mutation_performed"] is False
    assert any(
        i["source_design_id"] == source_id and i["legacy_status"] == LEGACY_STATUS_SOURCE_BLOCKED
        for i in payload["items"]
    )


@pytest.mark.asyncio
async def test_ts_c4_rest_legacy_report_twin(db_factory):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okto_pulse.community.api.architecture import router as architecture_router
    from okto_pulse.community.api.auth_deps import require_user
    from okto_pulse.core.infra.database import get_db

    board_id, _spec_id, source_id = await _seed_legacy_snapshot(db_factory)

    app = FastAPI()
    app.include_router(architecture_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER_ID
    client = TestClient(app)

    resp = client.get("/api/v1/architecture/propagation-legacy-report", params={"board_id": board_id})
    assert resp.status_code == 200
    body = resp.json()
    assert body["mutation_performed"] is False
    assert any(
        i["source_design_id"] == source_id and i["legacy_status"] == LEGACY_STATUS_SOURCE_BLOCKED
        for i in body["items"]
    )
