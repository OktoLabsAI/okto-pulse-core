from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.application.use_cases.spec_crud import (
    _spec_update_permission_requirements,
)
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Card, CardStatus, CardType, Spec, SpecStatus
from okto_pulse.core.models.schemas import SpecUpdate
from okto_pulse.core.services.analytics_service import spec_coverage_summary
from okto_pulse.core.services.main import CardService, SpecService


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def test_ir_or_spec_update_permissions_detect_create_edit_delete_and_links():
    spec = SimpleNamespace(
        integration_requirements=[
            {"id": "ir_1", "title": "Existing IR", "linked_task_ids": ["card_1"]}
        ],
        observability_requirements=[
            {"id": "or_1", "title": "Existing OR", "linked_task_ids": None}
        ],
    )

    requirements = _spec_update_permission_requirements(
        spec,
        SpecUpdate(
            integration_requirements=[
                {"id": "ir_1", "title": "Edited IR", "linked_task_ids": ["card_2"]},
                {"id": "ir_2", "title": "New IR"},
            ],
            observability_requirements=[],
        ),
    )

    assert "spec.integration_requirements.create" in requirements
    assert "spec.integration_requirements.edit" in requirements
    assert "spec.integration_requirements.link_task" in requirements
    assert "card.link_to.ir" in requirements
    assert "spec.observability_requirements.delete" in requirements


def test_ir_or_spec_update_permissions_do_not_treat_default_materialization_as_edit():
    spec = SimpleNamespace(
        integration_requirements=[{"id": "ir_1", "title": "Existing IR"}],
        observability_requirements=[{"id": "or_1", "title": "Existing OR"}],
    )

    requirements = _spec_update_permission_requirements(
        spec,
        SpecUpdate(
            integration_requirements=[
                {"id": "ir_1", "title": "Existing IR", "linked_task_ids": ["card_1"]}
            ],
            observability_requirements=[
                {"id": "or_1", "title": "Existing OR", "linked_task_ids": ["card_1"]}
            ],
        ),
    )

    assert requirements == {
        "spec.integration_requirements.link_task",
        "card.link_to.ir",
        "spec.observability_requirements.link_task",
        "card.link_to.or",
    }


@pytest.mark.asyncio
async def test_ir_or_coverage_requires_linked_tasks(db_factory):
    board_id = _id("board")
    actor_id = _id("user")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        board = Board(id=board_id, name="IR OR Board", owner_id=actor_id)
        db.add(board)
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Implement integration and telemetry",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=actor_id,
            )
        )
        spec = Spec(
            id=spec_id,
            board_id=board_id,
            title="Spec with IR and OR",
            status=SpecStatus.APPROVED,
            created_by=actor_id,
            functional_requirements=["Send metrics"],
            integration_requirements=[
                {"id": "ir_1", "title": "Send metric event", "status": "active"}
            ],
            observability_requirements=[
                {"id": "or_1", "title": "Ingestion dashboard", "status": "active"}
            ],
        )
        db.add(spec)
        await db.flush()

        service = CardService(db)
        with pytest.raises(ValueError, match="integration requirement"):
            await service.check_ir_coverage(spec, board)
        with pytest.raises(ValueError, match="observability requirement"):
            await service.check_or_coverage(spec, board)

        spec.integration_requirements[0]["linked_task_ids"] = [card_id]
        spec.observability_requirements[0]["linked_task_ids"] = [card_id]
        await service.check_ir_coverage(spec, board)
        await service.check_or_coverage(spec, board)


@pytest.mark.asyncio
async def test_ir_or_link_reference_validation(db_factory):
    board_id = _id("board")
    actor_id = _id("user")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Validation Board", owner_id=actor_id))
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Implement",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=actor_id,
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec",
                status=SpecStatus.DRAFT,
                created_by=actor_id,
                functional_requirements=["FR"],
                api_contracts=[
                    {"id": "api_1", "method": "POST", "path": "/metrics"}
                ],
            )
        )
        await db.flush()

        service = SpecService(db)
        bad_ir = [
            {
                "id": "ir_1",
                "title": "Invalid contract link",
                "linked_api_contracts": ["missing_api"],
            }
        ]
        with pytest.raises(ValueError, match="linked_api_contracts"):
            await service.update_spec(
                spec_id,
                actor_id,
                SpecUpdate(integration_requirements=bad_ir),
            )

        good_ir = [
            {
                "id": "ir_1",
                "title": "Metric API ingestion",
                "linked_api_contracts": ["api_1"],
                "linked_task_ids": [card_id],
            }
        ]
        bad_or = [
            {
                "id": "or_1",
                "title": "Invalid IR link",
                "linked_integration_requirements": ["missing_ir"],
            }
        ]
        with pytest.raises(ValueError, match="linked_integration_requirements"):
            await service.update_spec(
                spec_id,
                actor_id,
                SpecUpdate(
                    integration_requirements=good_ir,
                    observability_requirements=bad_or,
                ),
            )


@pytest.mark.asyncio
async def test_ir_or_linked_requirements_accept_structured_tr_refs(db_factory):
    board_id = _id("board")
    actor_id = _id("user")
    spec_id = _id("spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="IR OR TR refs", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="IR OR TR refs",
                status=SpecStatus.DRAFT,
                created_by=actor_id,
                functional_requirements=[
                    {"id": "fr_login", "text": "User can log in", "status": "active"}
                ],
                technical_requirements=[
                    {
                        "id": "tr_audit",
                        "text": "Login API must emit audit events",
                        "linked_task_ids": [],
                    },
                    {
                        "id": "tr_session",
                        "text": "Session timeout must be configurable",
                        "linked_task_ids": [],
                    },
                ],
            )
        )
        await db.flush()

        service = SpecService(db)
        await service.update_spec(
            spec_id,
            actor_id,
            SpecUpdate(
                integration_requirements=[
                    {
                        "id": "ir_1",
                        "title": "Audit export",
                        "linked_requirements": ["tr_audit"],
                    }
                ],
                observability_requirements=[
                    {
                        "id": "or_1",
                        "title": "Timeout metric",
                        "linked_requirements": ["Session timeout must be configurable"],
                    }
                ],
            ),
        )
        spec = await service.get_spec(spec_id)

    assert spec.integration_requirements[0]["linked_requirements"] == ["tr_audit"]
    assert spec.observability_requirements[0]["linked_requirements"] == [
        "Session timeout must be configurable"
    ]


@pytest.mark.asyncio
async def test_add_integration_requirement_accepts_external_service_mcp(db_factory):
    board_id = _id("board")
    actor_id = _id("user")
    spec_id = _id("spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="External service IR Board", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec",
                status=SpecStatus.DRAFT,
                created_by=actor_id,
                functional_requirements=["FR"],
                integration_requirements=[],
                observability_requirements=[],
            )
        )
        await db.commit()

    ctx = type(
        "Ctx",
        (),
        {"agent_id": actor_id, "agent_name": actor_id, "permissions": None},
    )()
    register_mcp_test_runtime(db_factory)
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)):
        raw = await mcp_server.okto_pulse_add_integration_requirement.fn(
            board_id=board_id,
            spec_id=spec_id,
            title="Third-party CRM",
            integration_type="external_service",
            provider="CRM",
            consumer="Pulse",
        )

    import json

    payload = json.loads(raw)
    assert payload.get("success") is True, payload
    assert payload["integration_requirement"]["integration_type"] == "external_service"


@pytest.mark.asyncio
async def test_add_integration_requirement_accepts_mcp_tool_type(db_factory):
    board_id = _id("board")
    actor_id = _id("user")
    spec_id = _id("spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="MCP tool IR Board", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec",
                status=SpecStatus.DRAFT,
                created_by=actor_id,
                functional_requirements=["FR"],
                integration_requirements=[],
                observability_requirements=[],
            )
        )
        await db.commit()

    ctx = type(
        "Ctx",
        (),
        {"agent_id": actor_id, "agent_name": actor_id, "permissions": None},
    )()
    register_mcp_test_runtime(db_factory)
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)):
        raw = await mcp_server.okto_pulse_add_integration_requirement.fn(
            board_id=board_id,
            spec_id=spec_id,
            title="Pulse MCP gateway",
            integration_type="mcp_tool",
            provider="Pulse",
            consumer="Agent",
        )

    import json

    payload = json.loads(raw)
    assert payload.get("success") is True, payload
    assert payload["integration_requirement"]["integration_type"] == "mcp_tool"


def test_ir_or_coverage_summary_and_kg_mapping():
    spec = {
        "id": "spec_1",
        "title": "Metrics ingestion",
        "functional_requirements": ["Receive metrics"],
        "technical_requirements": [],
        "acceptance_criteria": [],
        "business_rules": [],
        "test_scenarios": [],
        "api_contracts": [
            {
                "id": "api_1",
                "method": "POST",
                "path": "/metrics",
                "description": "Receive metric payload",
                "linked_requirements": ["Receive metrics"],
            }
        ],
        "integration_requirements": [
            {
                "id": "ir_1",
                "title": "Metric ingestion contract",
                "integration_type": "api",
                "description": "Client sends metrics to backend",
                "linked_api_contracts": ["api_1"],
                "linked_task_ids": ["card_1"],
            }
        ],
        "observability_requirements": [
            {
                "id": "or_1",
                "title": "Ingestion visibility",
                "signal_type": "dashboard",
                "description": "Dashboard shows accepted and rejected events",
                "linked_task_ids": [],
            }
        ],
        "decisions": [],
        "architecture_designs": [],
    }

    class FakeSpec:
        acceptance_criteria = []
        functional_requirements = ["Receive metrics"]
        test_scenarios = []
        business_rules = []
        api_contracts = spec["api_contracts"]
        technical_requirements = []
        decisions = []
        integration_requirements = spec["integration_requirements"]
        observability_requirements = spec["observability_requirements"]

    coverage = spec_coverage_summary(FakeSpec())
    assert coverage["irs_total"] == 1
    assert coverage["irs_linked"] == 1
    assert coverage["ors_total"] == 1
    assert coverage["ors_uncovered_ids"] == ["or_1"]

    result = DeterministicWorker().process_spec(spec)
    nodes_by_title = {node.title: node for node in result.nodes}
    assert nodes_by_title["Metric ingestion contract"].node_type == "Requirement"
    assert nodes_by_title["Ingestion visibility"].node_type == "Constraint"
    assert any(
        edge.edge_type == "implements"
        and edge.to_candidate_id == nodes_by_title["Metric ingestion contract"].candidate_id
        for edge in result.edges
    )
