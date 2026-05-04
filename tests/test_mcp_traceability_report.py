from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    ArchitectureDesign,
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecKnowledgeBase,
    SpecStatus,
    Sprint,
    SprintStatus,
)
from okto_pulse.core.services.traceability import build_lineage_graph


USER_ID = "traceability-report-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": ["board:read"],
        },
    )()


async def _call(name: str, **kwargs) -> dict:
    mcp_server.register_session_factory(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


@pytest.mark.asyncio
async def test_traceability_report_lists_sdlc_chain_without_duplicate_direct_specs():
    db_factory = get_session_factory()
    board_id = _id("trace-board")
    ideation_id = _id("trace-ideation")
    refinement_id = _id("trace-refinement")
    spec_id = _id("trace-spec")
    direct_spec_id = _id("trace-direct-spec")
    sprint_id = _id("trace-sprint")
    task_id = _id("trace-task")
    test_card_id = _id("trace-test-card")
    bug_card_id = _id("trace-bug-card")
    spec_kb_id = _id("trace-spec-kb")
    architecture_id = _id("trace-card-architecture")
    spec_architecture_id = _id("trace-spec-architecture")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Traceability Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Traceability Ideation",
                status=IdeationStatus.DONE,
                created_by=USER_ID,
                screen_mockups=[
                    {"id": "ideation-mockup", "title": "Ideation Mockup"}
                ],
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                board_id=board_id,
                ideation_id=ideation_id,
                title="Traceability Refinement",
                status=RefinementStatus.DONE,
                created_by=USER_ID,
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                ideation_id=ideation_id,
                refinement_id=refinement_id,
                title="Traceability Spec",
                status=SpecStatus.DONE,
                created_by=USER_ID,
                functional_requirements=["FR-1"],
                technical_requirements=[
                    {"id": "tr-1", "text": "TR-1", "linked_task_ids": [task_id]}
                ],
                acceptance_criteria=["AC-1"],
                test_scenarios=[
                    {
                        "id": "ts-1",
                        "title": "Happy path",
                        "status": "passed",
                        "linked_criteria": ["0"],
                        "linked_task_ids": [task_id],
                    }
                ],
                business_rules=[
                    {
                        "id": "br-1",
                        "title": "Traceability rule",
                        "rule": "Preserve references",
                        "when": "Exporting context",
                        "then": "Resolve linked resources",
                        "linked_requirements": ["0"],
                        "linked_task_ids": [task_id],
                    }
                ],
                api_contracts=[
                    {
                        "id": "contract-1",
                        "method": "GET",
                        "path": "/traceability",
                        "description": "Traceability endpoint",
                        "linked_requirements": ["0"],
                        "linked_task_ids": [task_id],
                    }
                ],
                decisions=[
                    {
                        "id": "decision-1",
                        "title": "Resolve references in context",
                        "status": "active",
                        "rationale": "Validators need inherited artifacts.",
                        "linked_requirements": ["0"],
                        "linked_task_ids": [task_id],
                    }
                ],
                screen_mockups=[{"id": "spec-mockup", "title": "Spec Mockup"}],
            )
        )
        db.add(
            Spec(
                id=direct_spec_id,
                board_id=board_id,
                ideation_id=ideation_id,
                title="Direct Traceability Spec",
                status=SpecStatus.DONE,
                created_by=USER_ID,
                functional_requirements=["FR-direct"],
                acceptance_criteria=["AC-direct"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            SpecKnowledgeBase(
                id=spec_kb_id,
                spec_id=spec_id,
                title="Spec KB",
                description="Spec knowledge",
                content="Context for the spec",
                created_by=USER_ID,
            )
        )
        db.add(
            Sprint(
                id=sprint_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Traceability Sprint",
                status=SprintStatus.CLOSED,
                created_by=USER_ID,
            )
        )
        db.add(
            Card(
                id=task_id,
                board_id=board_id,
                spec_id=spec_id,
                sprint_id=sprint_id,
                title="Implement traceable feature",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
                test_scenario_ids=["ts-1"],
                conclusions=[{"text": "Implemented", "author_id": USER_ID}],
                validations=[{"id": "validation-1", "outcome": "success"}],
                knowledge_bases=[
                    {
                        "id": "card-kb",
                        "title": "Card KB",
                        "description": "Card knowledge",
                        "content": "Implementation details",
                        "mime_type": "text/markdown",
                        "source_type": "manual",
                    }
                ],
                screen_mockups=[{"id": "card-mockup", "title": "Card Mockup"}],
            )
        )
        db.add(
            Card(
                id=test_card_id,
                board_id=board_id,
                spec_id=spec_id,
                sprint_id=sprint_id,
                title="Validate traceable feature",
                status=CardStatus.DONE,
                card_type=CardType.TEST,
                created_by=USER_ID,
                test_scenario_ids=["ts-1"],
                conclusions=[{"text": "Passed", "author_id": USER_ID}],
            )
        )
        db.add(
            Card(
                id=bug_card_id,
                board_id=board_id,
                spec_id=spec_id,
                sprint_id=sprint_id,
                title="Fix traceability regression",
                status=CardStatus.DONE,
                card_type=CardType.BUG,
                severity=BugSeverity.MAJOR,
                origin_task_id=task_id,
                expected_behavior="Report contains linked artifacts",
                observed_behavior="Report missed linked artifacts",
                linked_test_task_ids=[test_card_id],
                created_by=USER_ID,
            )
        )
        db.add(
            ArchitectureDesign(
                id=architecture_id,
                board_id=board_id,
                parent_type="card",
                card_id=task_id,
                title="Card architecture",
                global_description="Architecture linked to the card",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=USER_ID,
            )
        )
        db.add(
            ArchitectureDesign(
                id=spec_architecture_id,
                board_id=board_id,
                parent_type="spec",
                spec_id=spec_id,
                title="Spec architecture",
                global_description="Architecture inherited by task context",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=USER_ID,
            )
        )
        await db.commit()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        report = await _call(
            "okto_pulse_get_traceability_report",
            board_id=board_id,
            ideation_id=ideation_id,
            include_artifacts="true",
        )
        global_report = await _call(
            "okto_pulse_get_traceability_report",
            board_id=board_id,
            include_artifacts="false",
        )
        task_context = await _call(
            "okto_pulse_get_task_context",
            board_id=board_id,
            card_id=task_id,
            include_knowledge="true",
            include_mockups="true",
            include_architecture="true",
        )

    assert report["summary"]["ideations"] == 1
    assert report["summary"]["specs"] == 2
    assert report["summary"]["cards"] == 3
    assert report["summary"]["orphan_specs"] == 0

    ideation = report["ideations"][0]
    assert ideation["direct_specs"][0]["id"] == direct_spec_id
    refinement = ideation["refinements"][0]
    spec = refinement["specs"][0]
    assert spec["id"] == spec_id
    assert spec["tests"][0]["linked_task_ids"] == [task_id]
    assert spec["card_counts"] == {
        "total": 3,
        "normal": 1,
        "test": 1,
        "bug": 1,
        "done": 3,
    }
    assert spec["artifacts"]["knowledge_bases"][0]["id"] == spec_kb_id

    task = next(card for card in spec["cards"] if card["id"] == task_id)
    assert task["conclusions_count"] == 1
    assert task["validations_count"] == 1
    assert task["artifacts"]["knowledge_bases"][0]["id"] == "card-kb"
    assert task["artifacts"]["knowledge_bases"][0]["source_type"] == "manual"
    assert task["artifacts"]["mockups"][0]["id"] == "card-mockup"
    assert task["artifacts"]["architecture_designs"][0]["id"] == architecture_id
    assert {kb["id"] for kb in task["resolved_artifacts"]["knowledge_bases"]} == {
        "card-kb",
        spec_kb_id,
    }
    assert {mockup["id"] for mockup in task["resolved_artifacts"]["screen_mockups"]} == {
        "card-mockup",
        "spec-mockup",
    }
    assert {arch["id"] for arch in task["resolved_artifacts"]["architecture_designs"]} == {
        architecture_id,
        spec_architecture_id,
    }

    resolved = task_context["resolved_references"]
    assert {kb["id"] for kb in resolved["knowledge_bases"]} == {"card-kb", spec_kb_id}
    assert {mockup["id"] for mockup in resolved["screen_mockups"]} == {
        "card-mockup",
        "spec-mockup",
    }
    assert {arch["id"] for arch in resolved["architecture_designs"]} == {
        architecture_id,
        spec_architecture_id,
    }
    assert resolved["technical_requirements"][0]["reference_type"] == "linked_task"
    assert resolved["business_rules"][0]["reference_type"] == "linked_task"
    assert resolved["api_contracts"][0]["reference_type"] == "linked_task"
    assert resolved["decisions"][0]["reference_type"] == "linked_task"
    assert resolved["functional_requirements"][0]["referenced_by_task"] is True
    assert resolved["acceptance_criteria"][0]["referenced_by_task"] is True

    bug = spec["bugs"][0]
    assert bug["id"] == bug_card_id
    assert bug["bug"]["severity"] == "major"
    assert bug["bug"]["linked_test_task_ids"] == [test_card_id]

    global_ideation = next(
        item for item in global_report["ideations"] if item["id"] == ideation_id
    )
    assert [spec["id"] for spec in global_ideation["direct_specs"]] == [
        direct_spec_id
    ]
    assert global_report["summary"]["orphan_specs"] == 0

    async with db_factory() as db:
        graph = await build_lineage_graph(
            db,
            board_id,
            entity_type="spec",
            entity_id=spec_id,
            include_artifacts=True,
        )

    node_ids = {node["id"] for node in graph["nodes"]}
    edge_pairs = {(edge["source"], edge["target"]) for edge in graph["edges"]}
    node_by_id = {node["id"]: node for node in graph["nodes"]}
    assert graph["root_ideation"]["id"] == ideation_id
    assert f"spec:{direct_spec_id}" in node_ids
    assert f"spec:{spec_id}" in node_ids
    assert f"task:{task_id}" in node_ids
    assert f"bug:{bug_card_id}" in node_ids
    assert all(node["entity_type"] != "artifact" for node in graph["nodes"])
    assert graph["summary"]["artifacts"] == 0
    assert node_by_id[f"bug:{bug_card_id}"]["stage"] == 5
    assert (f"ideation:{ideation_id}", f"spec:{direct_spec_id}") in edge_pairs
    assert (f"refinement:{refinement_id}", f"spec:{spec_id}") in edge_pairs
    assert (f"task:{task_id}", f"bug:{bug_card_id}") in edge_pairs
