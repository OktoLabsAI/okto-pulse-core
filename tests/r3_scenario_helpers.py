"""Shared harness + real-pipeline seeding for the R3 scenario test cards.

NOT a test module (no ``test_`` prefix). Provides the MCP tool harness and the
real lineage seeding (board -> ideation -> refinement -> spec/card) the R3
scenario tests drive through the REAL ``okto_pulse_*`` tools and the REAL Resource
Gate / lineage services.
"""

from __future__ import annotations

from datetime import datetime, timezone

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
import pytest
from unittest.mock import AsyncMock, patch

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementKnowledgeBase,
    RefinementSnapshot,
    RefinementStatus,
    Spec,
)
from okto_pulse.core.domain.research_decision_ledger import (
    ResearchDecisionLedgerSnapshot,
)
from okto_pulse.core.domain.code_traceability import (
    DeliveryContext,
    RefinementDeliveryContextProvenance,
    RefinementSourceContextManifestV2,
    build_source_context_summary_v2,
)
from okto_pulse.core.ports.relational_application import (
    require_relational_application_adapter,
)

USER_ID = "user-r3-scenarios"


@pytest.fixture(autouse=True)
def native_knowledge_port(_knowledge_propagation_empty_test_port, request):
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.community.adapters.sqlalchemy_knowledge_propagation import (
        CommunitySqlAlchemyKnowledgePropagationStore,
    )
    from okto_pulse.core.ports.knowledge_propagation import (
        register_knowledge_propagation_port, register_knowledge_mutation_audit_sink,
        reset_knowledge_mutation_audit_sink_for_tests,
    )
    from okto_pulse.core.domain.realm import RealmScope

    from okto_pulse.community.adapters.sqlalchemy_resource_gate_service import (
        CommunitySqlAlchemyResourceGateAdapter,
    )
    from okto_pulse.core.ports.relational_services import (
        register_resource_gate_adapter_factory, resolve_resource_gate_adapter_factory,
    )

    previous_gate_factory = resolve_resource_gate_adapter_factory()
    request.addfinalizer(lambda: register_resource_gate_adapter_factory(previous_gate_factory))
    register_resource_gate_adapter_factory(CommunitySqlAlchemyResourceGateAdapter)

    factory = get_session_factory()
    previous_info = dict(factory.kw.get("info", {}))
    request.addfinalizer(lambda: factory.configure(info=previous_info))
    factory.configure(info={**previous_info, "realm_scope": RealmScope.local()})
    store = CommunitySqlAlchemyKnowledgePropagationStore(factory)
    register_knowledge_propagation_port(store)
    register_knowledge_mutation_audit_sink(store)
    request.addfinalizer(reset_knowledge_mutation_audit_sink_for_tests)




def sid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r3 scenario tester"
        self.permissions = ["*"]


async def call_tool(name: str, **kwargs) -> dict:
    """Invoke a real MCP tool against the test DB with a stubbed auth ctx."""
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_architecture_copy_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def new_board(db_factory) -> str:
    board_id = sid("board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r3 scenarios", owner_id=USER_ID))
        await db.flush()
        from okto_pulse.core.domain.checklist import ChecklistBinding, ChecklistMode
        from okto_pulse.community.adapters.sqlalchemy_checklist import CommunitySqlAlchemyChecklist
        await CommunitySqlAlchemyChecklist(db).apply_binding_cas(
            ChecklistBinding(board_id=board_id, mode=ChecklistMode.OFF, version=1),
            expected_version=0, expected_digest=None,
        )
        await db.commit()
    return board_id


async def freeze_refinement_completion_fixture(db, refinement) -> None:
    """Persist both immutable completion snapshots required by derivation."""

    raw_context = getattr(refinement, "delivery_context", None)
    delivery_context = (
        None if raw_context is None else DeliveryContext(raw_context)
    )
    provenance = (
        None
        if delivery_context is None
        else RefinementDeliveryContextProvenance(
            value=delivery_context,
            source_refinement_id=refinement.id,
            source_refinement_version=refinement.version,
        )
    )
    source_context = RefinementSourceContextManifestV2(
        refinement_id=refinement.id,
        refinement_version=refinement.version,
        summary=build_source_context_summary_v2(
            delivery_context=delivery_context,
            delivery_context_provenance=provenance,
            current_investigation_outcomes=(),
            evidence=(),
        ),
        current_receipts=(),
    )

    db.add(
        RefinementSnapshot(
            refinement_id=refinement.id,
            version=refinement.version,
            title=refinement.title,
            description=refinement.description,
            in_scope=refinement.in_scope,
            out_of_scope=refinement.out_of_scope,
            analysis=refinement.analysis,
            decisions=refinement.decisions,
            delivery_context=getattr(refinement, "delivery_context", None),
            labels=refinement.labels,
            qa_snapshot=[],
            code_evidence_manifest=[],
            source_context_manifest=source_context.as_dict(),
            source_context_sha256=source_context.payload_sha256,
            created_by=USER_ID,
        )
    )
    await require_relational_application_adapter().research_decisions(
        db
    ).save_snapshot(
        ResearchDecisionLedgerSnapshot(
            id=sid("rdl-snapshot"),
            board_id=refinement.board_id,
            refinement_id=refinement.id,
            refinement_version=refinement.version,
            heads=(),
            created_at=datetime.now(timezone.utc),
        )
    )


async def seed_refinement(
    db_factory, board_id, *, status="done", kb=False, mockup=False, architecture=False,
) -> dict:
    """Refinement (with parent ideation) optionally carrying DIRECT KB / mockup /
    architecture. Returns the seeded ids."""
    ideation_id = sid("idea")
    refinement_id = sid("ref")
    kb_id = sid("refkb")
    mockup_id = sid("refmock")
    design_id = sid("refdesign")
    async with db_factory() as db:
        db.add(Ideation(id=ideation_id, board_id=board_id, title="R3 ideation",
                        status=IdeationStatus.DONE, created_by=USER_ID))
        refinement = Refinement(
            id=refinement_id, board_id=board_id, ideation_id=ideation_id,
            title="R3 refinement", created_by=USER_ID,
            status=RefinementStatus(status),
            delivery_context=DeliveryContext.BROWNFIELD.value,
            screen_mockups=(
                [{"id": mockup_id, "title": "Ref mockup", "screen_type": "form",
                  "html_content": "<div/>"}] if mockup else []
            ),
        )
        db.add(refinement)
        if kb:
            db.add(RefinementKnowledgeBase(
                id=kb_id, refinement_id=refinement_id, title="Ref KB",
                description="d", content="ref content", mime_type="text/markdown",
                created_by=USER_ID,
            ))
        if architecture:
            db.add(ArchitectureDesign(
                id=design_id, board_id=board_id, parent_type="refinement",
                refinement_id=refinement_id, title="Ref design", global_description="g",
                entities=[], interfaces=[], diagrams=[], created_by=USER_ID,
            ))
        if refinement.status is RefinementStatus.DONE:
            await db.flush()
            await freeze_refinement_completion_fixture(db, refinement)
        await db.commit()
    return {"ideation_id": ideation_id, "refinement_id": refinement_id,
            "kb_id": kb_id, "mockup_id": mockup_id, "design_id": design_id}


async def seed_native_spec_with_card(db_factory, board_id, ref) -> dict:
    """A native Spec with explicit architecture adoption and inherited resources."""
    spec_id = sid("spec")
    card_id = sid("card")
    from okto_pulse.core.domain.architecture_adoption import ArchitectureAdoptionScope

    async with db_factory() as db:
        design = await db.get(ArchitectureDesign, ref["design_id"])
        adoption = ArchitectureAdoptionScope(
            board_id=board_id, spec_id=spec_id, adopted_in_edition=1,
            actor_id=USER_ID,
            inherited_resource_ids=(() if design is None else (f"architecture:{design.id}",)),
        )
        db.add(Spec(id=spec_id, board_id=board_id, refinement_id=ref["refinement_id"],
                    ideation_id=ref["ideation_id"], title="Native inherited spec",
                    architecture_adoption=adoption.model_dump(mode="json"),
                    created_by=USER_ID))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="impl card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()
    async with db_factory() as db:
        has_knowledge = await db.get(RefinementKnowledgeBase, ref["kb_id"]) is not None
    if has_knowledge:
        for target_type, target_id in (("spec", spec_id), ("card", card_id)):
            await select_native_knowledge(db_factory, board_id, target_type, target_id, [ref["kb_id"]])
    return {"spec_id": spec_id, "card_id": card_id}


async def add_card(db_factory, board_id, spec_id) -> str:
    card_id = sid("card")
    async with db_factory() as db:
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="impl card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()
    return card_id


async def select_native_knowledge(db_factory, board_id, target_type, target_id, knowledge_ids, *, actor_id=USER_ID):
    """Author explicit reference assignments through the native service/store."""
    from okto_pulse.core.domain.knowledge_selection import KnowledgeSelection
    from okto_pulse.core.ports.knowledge_propagation import KnowledgeTargetKey
    from okto_pulse.core.services.knowledge_propagation import (
        KnowledgeMutationCommand, KnowledgePropagationService,
    )

    async with db_factory() as db:
        receipt = await KnowledgePropagationService().mutate(
            db, KnowledgeMutationCommand(
                target=KnowledgeTargetKey(board_id, target_type, target_id),
                selection=KnowledgeSelection.explicit_ids(knowledge_ids, mode="reference"),
                actor_id=actor_id, expected_revision=0,
                idempotency_key=str(uuid.uuid4()),
                justification="Explicit native fixture selection for resource context",
            ),
        )
        assert receipt.revision == 1
        await db.commit()
