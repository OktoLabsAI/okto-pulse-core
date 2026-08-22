from __future__ import annotations

import pytest

from okto_pulse.core.domain.enums import IdeationStatus, RefinementStatus
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    Refinement,
    Spec,
    SpecKnowledgeBase,
    SpecStatus,
)
from okto_pulse.core.domain.knowledge_selection import KnowledgeSelectionState
from okto_pulse.core.models.schemas import CardUpdate, SpecUpdate
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgePropagationPortError,
    KnowledgePropagationScope,
)
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgePropagationServiceError,
)
from okto_pulse.core.services.main import (
    CardService,
    GovernedArtifactDeletionReceipt,
    SpecLineagePreflightError,
    SpecService,
    _reset_v2_knowledge_for_relink,
)
from r3_scenario_helpers import freeze_refinement_completion_fixture


BOARD_ID = "imp5-relink-board"
ACTOR_ID = "imp5-relink-actor"


class _V2RelinkPort:
    def __init__(self, *, revision: int = 7, fail_stage: bool = False) -> None:
        self.revision = revision
        self.fail_stage = fail_stage
        self.scope_lookups = []
        self.idempotency_lookups = []
        self.staged = []
        self.attempts = []
        self._ledger_by_key = {}

    async def get_idempotency_entry(self, _context, request):
        self.idempotency_lookups.append(request)
        return self._ledger_by_key.get(request.idempotency_key)

    async def load_scope(self, _context, request):
        self.scope_lookups.append(request)
        return KnowledgePropagationScope(
            target=request.target,
            scope_revision=self.revision,
            v2_active=True,
            selection_state=KnowledgeSelectionState.EXPLICIT_EMPTY,
        )

    async def stage_mutation(self, _context, plan):
        if self.fail_stage:
            raise KnowledgePropagationPortError(
                "knowledge_relink_stage_failed",
                "injected relink failure",
            )
        self.staged.append(plan)
        assert plan.ledger_entry is not None
        self._ledger_by_key[plan.idempotency_key] = plan.ledger_entry
        return plan.ledger_entry.receipt

    async def stage_attempt(self, _context, attempt):
        self.attempts.append(attempt)


def _spec(
    spec_id: str,
    *,
    ideation_id: str | None = None,
    refinement_id: str | None = None,
    status: SpecStatus = SpecStatus.APPROVED,
) -> Spec:
    return Spec(
        id=spec_id,
        board_id=BOARD_ID,
        ideation_id=ideation_id,
        refinement_id=refinement_id,
        title=spec_id,
        status=status,
        created_by=ACTOR_ID,
        functional_requirements=[],
        acceptance_criteria=[],
        test_scenarios=[],
        business_rules=[],
        api_contracts=[],
        technical_requirements=[],
        decisions=[],
    )


def _card(card_id: str, spec_id: str | None) -> Card:
    return Card(
        id=card_id,
        board_id=BOARD_ID,
        spec_id=spec_id,
        title=card_id,
        status=CardStatus.NOT_STARTED,
        card_type=CardType.NORMAL,
        created_by=ACTOR_ID,
    )


async def _seed_card_relink(db) -> None:
    db.add(
        Board(
            id=BOARD_ID,
            name="IMP5 relink",
            owner_id=ACTOR_ID,
            settings={
                "auto_derive_spec_resources_enabled": True,
                "auto_derive_spec_resource_types": ["knowledge_base"],
            },
        )
    )
    db.add_all((_spec("spec-old"), _spec("spec-new")))
    db.add(
        SpecKnowledgeBase(
            id="kb-new-parent",
            spec_id="spec-new",
            title="Must not be copied",
            content="v1 physical copy is forbidden after relink",
            mime_type="text/markdown",
            created_by=ACTOR_ID,
        )
    )
    card = _card("card-relink", "spec-old")
    card.knowledge_bases = [{"id": "legacy-history", "title": "History"}]
    db.add(card)
    await db.flush()


@pytest.mark.asyncio
async def test_card_update_resets_v2_before_reparenting(db_factory) -> None:
    port = _V2RelinkPort()
    async with db_factory() as db:
        await _seed_card_relink(db)

        updated = await CardService(
            db,
            knowledge_propagation_port=port,
        ).update_card(
            "card-relink",
            ACTOR_ID,
            CardUpdate(spec_id="spec-new"),
        )

        assert updated is not None
        assert updated.spec_id == "spec-new"
        assert updated.knowledge_bases == [
            {"id": "legacy-history", "title": "History"}
        ]
        assert len(port.staged) == 1
        plan = port.staged[0]
        assert plan.target.target_id == "card-relink"
        assert plan.target.target_type.value == "card"
        assert plan.parent is not None
        assert plan.parent.parent_id == "spec-old"
        assert plan.next_scope_v2_active is True
        assert plan.next_scope_selection_state is KnowledgeSelectionState.OMITTED
        assert plan.expected_revision == 7
        assert plan.next_revision == 8
        assert plan.ledger_entry.receipt.details["relink"]["next_parent"][
            "parent_id"
        ] == "spec-new"


@pytest.mark.asyncio
async def test_card_relink_failure_leaves_parent_unchanged(db_factory) -> None:
    port = _V2RelinkPort(fail_stage=True)
    async with db_factory() as db:
        await _seed_card_relink(db)

        with pytest.raises(
            KnowledgePropagationServiceError,
        ) as raised:
            await CardService(
                db,
                knowledge_propagation_port=port,
            ).update_card(
                "card-relink",
                ACTOR_ID,
                CardUpdate(spec_id="spec-new"),
            )
        assert raised.value.code == "knowledge_relink_stage_failed"

        unchanged = await CardService(db).get_card("card-relink")
        assert unchanged is not None
        assert unchanged.spec_id == "spec-old"


@pytest.mark.asyncio
async def test_card_relink_validates_new_parent_before_v2_write(db_factory) -> None:
    port = _V2RelinkPort()
    async with db_factory() as db:
        await _seed_card_relink(db)

        with pytest.raises(ValueError, match="Spec not found on this board"):
            await CardService(
                db,
                knowledge_propagation_port=port,
            ).update_card(
                "card-relink",
                ACTOR_ID,
                CardUpdate(spec_id="spec-missing"),
            )

        unchanged = await CardService(db).get_card("card-relink")
        assert unchanged is not None
        assert unchanged.spec_id == "spec-old"
        assert port.scope_lookups == []
        assert port.staged == []


@pytest.mark.asyncio
async def test_relink_reset_key_is_replay_stable(db_factory) -> None:
    port = _V2RelinkPort()
    async with db_factory() as db:
        await _seed_card_relink(db)
        kwargs = {
            "board_id": BOARD_ID,
            "target_type": "card",
            "target_id": "card-relink",
            "previous_parent": ("spec", "spec-old"),
            "next_parent": ("spec", "spec-new"),
            "actor_id": ACTOR_ID,
            "port": port,
        }

        assert await _reset_v2_knowledge_for_relink(db, **kwargs)
        first_receipt = port.staged[0].ledger_entry.receipt
        assert await _reset_v2_knowledge_for_relink(db, **kwargs)

        assert len(port.staged) == 1
        replay_lookup = port.idempotency_lookups[-1]
        assert replay_lookup.idempotency_key == port.staged[0].idempotency_key
        replay_entry = port._ledger_by_key[replay_lookup.idempotency_key]
        assert replay_entry.receipt == first_receipt


@pytest.mark.asyncio
async def test_spec_update_validates_and_resets_governed_parent(db_factory) -> None:
    port = _V2RelinkPort(revision=3)
    async with db_factory() as db:
        db.add(Board(id=BOARD_ID, name="IMP5 relink", owner_id=ACTOR_ID))
        db.add_all(
            (
                Ideation(
                    id="ideation-parent",
                    board_id=BOARD_ID,
                    title="Parent",
                    created_by=ACTOR_ID,
                    status=IdeationStatus.DONE,
                ),
                Refinement(
                    id="refinement-old",
                    board_id=BOARD_ID,
                    ideation_id="ideation-parent",
                    title="Old",
                    created_by=ACTOR_ID,
                    status=RefinementStatus.DONE,
                    delivery_context="brownfield",
                ),
                Refinement(
                    id="refinement-new",
                    board_id=BOARD_ID,
                    ideation_id="ideation-parent",
                    title="New",
                    created_by=ACTOR_ID,
                    status=RefinementStatus.DONE,
                    delivery_context="brownfield",
                ),
                _spec(
                    "spec-relink",
                    ideation_id="ideation-parent",
                    refinement_id="refinement-old",
                    status=SpecStatus.DRAFT,
                ),
            )
        )
        await db.flush()
        target_refinement = await db.get(Refinement, "refinement-new")
        assert target_refinement is not None
        await freeze_refinement_completion_fixture(db, target_refinement)

        updated = await SpecService(
            db,
            knowledge_propagation_port=port,
        ).update_spec(
            "spec-relink",
            ACTOR_ID,
            SpecUpdate(refinement_id="refinement-new"),
        )

        assert updated is not None
        assert updated.refinement_id == "refinement-new"
        assert len(port.staged) == 1
        plan = port.staged[0]
        assert plan.target.target_type.value == "spec"
        assert plan.target.target_id == "spec-relink"
        assert plan.parent is not None
        assert plan.parent.parent_type.value == "refinement"
        assert plan.parent.parent_id == "refinement-old"
        assert plan.ledger_entry.receipt.details["relink"]["next_parent"][
            "parent_id"
        ] == "refinement-new"


@pytest.mark.asyncio
async def test_spec_relink_validates_new_parent_before_v2_write(db_factory) -> None:
    port = _V2RelinkPort()
    async with db_factory() as db:
        db.add(Board(id=BOARD_ID, name="IMP5 relink", owner_id=ACTOR_ID))
        db.add(
            Ideation(
                id="ideation-parent",
                board_id=BOARD_ID,
                title="Parent",
                created_by=ACTOR_ID,
                status=IdeationStatus.DONE,
            )
        )
        db.add(
            _spec(
                "spec-relink",
                ideation_id="ideation-parent",
                status=SpecStatus.DRAFT,
            )
        )
        await db.flush()

        with pytest.raises(SpecLineagePreflightError) as raised:
            await SpecService(
                db,
                knowledge_propagation_port=port,
            ).update_spec(
                "spec-relink",
                ACTOR_ID,
                SpecUpdate(refinement_id="refinement-missing"),
            )
        assert raised.value.code == "spec_refinement_not_found"

        unchanged = await SpecService(db).get_spec("spec-relink")
        assert unchanged is not None
        assert unchanged.refinement_id is None
        assert port.scope_lookups == []
        assert port.staged == []


@pytest.mark.asyncio
async def test_spec_link_unlink_and_delete_reset_card_v2_scopes(
    db_factory,
    monkeypatch,
) -> None:
    port = _V2RelinkPort()
    async with db_factory() as db:
        db.add(Board(id=BOARD_ID, name="IMP5 relink", owner_id=ACTOR_ID))
        db.add_all((_spec("spec-a"), _spec("spec-b")))
        db.add_all(
            (
                _card("card-link", None),
                _card("card-unlink", "spec-a"),
            )
        )
        await db.flush()
        service = SpecService(db, knowledge_propagation_port=port)

        assert await service.link_card("spec-b", "card-link", ACTOR_ID)
        assert await service.unlink_card("card-unlink", ACTOR_ID)

        assert [plan.target.target_id for plan in port.staged] == [
            "card-link",
            "card-unlink",
        ]
        assert port.staged[0].parent is None
        assert port.staged[1].parent is not None
        assert port.staged[1].parent.parent_id == "spec-a"

        async def _skip_takedown(*_args, **kwargs):
            artifact_type = kwargs["artifact_type"]
            artifact_id = kwargs["artifact_id"]
            return GovernedArtifactDeletionReceipt(
                board_id=kwargs["board_id"],
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                delete_event_id=f"delete-{artifact_type}-{artifact_id}",
                generation=1,
                reconcile_intent_id=f"intent-{artifact_type}-{artifact_id}",
                delivery_key=f"delivery-{artifact_type}-{artifact_id}",
            )

        monkeypatch.setattr(
            "okto_pulse.core.services.main._prepare_governed_artifact_deletion",
            _skip_takedown,
        )
        assert await service.delete_spec("spec-b", ACTOR_ID)
        assert [plan.target.target_id for plan in port.staged] == [
            "card-link",
            "card-unlink",
            "card-link",
        ]
        assert port.staged[-1].parent is not None
        assert port.staged[-1].parent.parent_id == "spec-b"
