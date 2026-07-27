"""Regression coverage for zero-orphan bootstrap of a derived Spec draft."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
    EmittedNode,
)
from okto_pulse.core.application.processors.consolidation import (
    ConsolidationProcessor,
)
from okto_pulse.core.domain.enums import (
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
)
from okto_pulse.core.kg.connectivity_guard import KGNodeConnectivityGuard
from kg_schema_testing import bootstrap_board_graph
from sqlalchemy_test_models import (
    Board,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    Ideation,
    Refinement,
    Spec,
)


BOARD_ID = "2cd4d5ac-054c-4fa7-bc77-bdab3213322e"
SPEC_ID = "38ea3c78-c67f-43d8-b8c1-92cd4ae2fda3"
REFINEMENT_ID = "9fe1c921-486b-4137-ac26-1866a9f14af6"


def _derived_empty_draft() -> dict:
    return {
        "id": SPEC_ID,
        "board_id": BOARD_ID,
        "ideation_id": "5f7c2856-4d54-4ab1-82cf-29041bd21b84",
        "refinement_id": REFINEMENT_ID,
        "title": "R-TRX — Restore transaction state",
        "description": "Derived from a completed refinement.",
        "context": (
            "## Refinement Description\n"
            "Investigate the failure.\n\n"
            "## Parent Ideation Context\n"
            "Inherited narrative only.\n\n"
            "## Decisions\n"
            "- Reuse the transaction-lifecycle precedent.\n"
            "- Roll back through the persistence port.\n"
        ),
        "status": "draft",
        "functional_requirements": [],
        "technical_requirements": [],
        "acceptance_criteria": [],
        "business_rules": [],
        "test_scenarios": [],
        "api_contracts": [],
        "integration_requirements": [],
        "observability_requirements": [],
        "decisions": [],
        "architecture_designs": [],
    }


def test_empty_derived_draft_emits_connected_backbone_not_inherited_decisions():
    result = DeterministicWorker().process_spec(_derived_empty_draft())

    assert [node.node_type for node in result.nodes].count("Decision") == 0
    assert {
        (node.node_type, node.source_artifact_ref)
        for node in result.nodes
    } == {
        ("Entity", f"spec:{SPEC_ID}"),
        ("Entity", f"board:{BOARD_ID}"),
    }
    assert any(
        edge.edge_type == "belongs_to"
        and edge.from_candidate_id == f"spec_{SPEC_ID[:8]}_entity"
        and edge.to_candidate_id == f"board_{BOARD_ID[:8]}_entity"
        for edge in result.edges
    )

    validation = KGNodeConnectivityGuard().validate(
        board_id=BOARD_ID,
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=result.nodes,
        edges=result.edges,
    )
    assert validation.passed is True
    assert validation.violations == ()


def test_populated_refinement_derived_spec_does_not_duplicate_parent_decisions():
    spec = _derived_empty_draft()
    spec["functional_requirements"] = ["The worker restores its transaction."]
    spec["decisions"] = [{
        "id": "dec_spec_owned",
        "title": "Retry only after rollback",
        "rationale": "The next transaction must start from a clean session.",
        "linked_requirements": ["0"],
        "status": "active",
    }]

    result = DeterministicWorker().process_spec(spec)

    decisions = [node for node in result.nodes if node.node_type == "Decision"]
    assert [decision.title for decision in decisions] == ["Retry only after rollback"]
    assert decisions[0].source_artifact_ref == (
        f"spec:{SPEC_ID}:decision:dec_spec_owned"
    )
    assert any(
        edge.edge_type == "derives_from"
        and edge.from_candidate_id == decisions[0].candidate_id
        for edge in result.edges
    )


def test_ideation_derived_spec_keeps_legacy_decision_compatibility():
    spec = _derived_empty_draft()
    spec["refinement_id"] = None
    spec["functional_requirements"] = ["The worker restores its transaction."]

    result = DeterministicWorker().process_spec(spec)

    decisions = [node for node in result.nodes if node.node_type == "Decision"]
    assert [decision.title for decision in decisions] == [
        "Reuse the transaction-lifecycle precedent.",
        "Roll back through the persistence port.",
    ]


def test_true_orphan_entity_remains_rejected_by_connectivity_guard():
    orphan = EmittedNode(
        candidate_id="orphan_entity",
        node_type="Entity",
        title="Actually orphaned",
        content="No provenance edge exists.",
        source_artifact_ref="spec:orphan",
    )

    validation = KGNodeConnectivityGuard().validate(
        board_id=BOARD_ID,
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[orphan],
        edges=[],
    )

    assert validation.passed is False
    assert len(validation.violations) == 1
    assert validation.violations[0].reason == "missing_required_edge"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "acceptance_criteria",
    [[], ["The derived draft remains connected after an AC-only update."]],
    ids=["empty", "acceptance-criteria-only"],
)
async def test_refinement_derived_draft_is_acked_without_dead_letter(
    db_factory,
    acceptance_criteria,
):
    """Exercise the production queue path, including lineage materialization."""

    token = uuid.uuid4().hex[:10]
    board_id = f"draft-bootstrap-{token}"
    ideation_id = f"ideation-{token}"
    refinement_id = f"refinement-{token}"
    spec_id = f"spec-{token}"
    bootstrap_board_graph(board_id)

    async with db_factory() as session:
        # Keep the board-aware single-entry claim hermetic in the shared test DB.
        await session.execute(ConsolidationQueue.__table__.delete())
        session.add(Board(id=board_id, name="draft bootstrap", owner_id="owner"))
        session.add(Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Parent ideation",
            description="Parent narrative.",
            status=IdeationStatus.DONE,
            created_by="owner",
        ))
        session.add(Refinement(
            id=refinement_id,
            ideation_id=ideation_id,
            board_id=board_id,
            title="Parent refinement",
            description="Refined narrative.",
            decisions=["Rollback through the persistence port."],
            status=RefinementStatus.DONE,
            created_by="owner",
        ))
        session.add(Spec(
            id=spec_id,
            board_id=board_id,
            ideation_id=ideation_id,
            refinement_id=refinement_id,
            title="Derived empty draft",
            description="Derived from the completed refinement.",
            context=(
                "## Refinement Description\nRefined narrative.\n\n"
                "## Decisions\n- Rollback through the persistence port.\n"
            ),
            functional_requirements=[],
            technical_requirements=[],
            acceptance_criteria=acceptance_criteria,
            business_rules=[],
            test_scenarios=[],
            api_contracts=[],
            integration_requirements=[],
            observability_requirements=[],
            decisions=[],
            status=SpecStatus.DRAFT,
            created_by="owner",
        ))
        queue_entry = ConsolidationQueue(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=spec_id,
            priority="normal",
            source="event:spec.derived",
            triggered_by_event="spec.derived",
            status="pending",
        )
        session.add(queue_entry)
        await session.commit()
        queue_entry_id = queue_entry.id

    processed = await ConsolidationProcessor(db_factory, batch_size=1).process_batch()

    async with db_factory() as session:
        remaining = await session.get(ConsolidationQueue, queue_entry_id)
        dead_letters = (
            await session.execute(
                select(ConsolidationDeadLetter).where(
                    ConsolidationDeadLetter.board_id == board_id,
                    ConsolidationDeadLetter.artifact_type == "spec",
                    ConsolidationDeadLetter.artifact_id == spec_id,
                )
            )
        ).scalars().all()

    assert processed == 1
    assert remaining is None
    assert dead_letters == []
