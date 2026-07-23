"""Application/MCP contracts for selective Knowledge propagation v2 (IMP4)."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.knowledge_propagation_projection import (
    project_card_create_response,
    project_knowledge_propagation_error,
    project_knowledge_mutation_response,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.knowledge_propagation import (
    CreateCardKnowledgeV2Command,
    CreateCardKnowledgeV2UseCase,
    KnowledgeCreationRaceError,
)
from okto_pulse.core.application.use_cases.mcp_spec_crud import (
    McpDeriveSpecCommand,
    McpDeriveSpecUseCase,
)
from okto_pulse.core.domain.knowledge_selection import (
    KnowledgeAssignment,
    KnowledgePropagationContractError,
    KnowledgePropagationMode,
    KnowledgeSelectionState,
)
from okto_pulse.core.domain.resource_revision import ResourceRevisionStamp
from okto_pulse.core.models.knowledge_propagation import (
    DeriveSpecKnowledgeRequest,
    KnowledgeAssignmentDropRequest,
    KnowledgeAssignmentRefreshRequest,
    KnowledgeAssignmentReplaceRequest,
    KnowledgePropagationEnvelopeV2,
)
from okto_pulse.core.models.schemas import CardCreate
from okto_pulse.core.ports.application_persistence import (
    ApplicationRecordConflictError,
)
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgeMutationKind,
    KnowledgeMutationOutcome,
    KnowledgeMutationReceipt,
    KnowledgePropagationPortError,
    KnowledgeTargetKey,
)
from okto_pulse.core.ports.spec_resource_propagation import (
    ResourcePropagationBoardFact,
    ResourcePropagationCardRecord,
    ResourcePropagationSpecFact,
    register_spec_resource_propagation_store,
)
from okto_pulse.core.services.spec_resource_propagation import (
    SpecResourcePropagationService,
)
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgeMutationPreparation,
    KnowledgeMutationResultV2,
    KnowledgeMutationResultV2Projector,
    KnowledgePropagationServiceError,
)


NOW = datetime(2026, 7, 23, 16, 0, tzinfo=timezone.utc)


def _envelope(
    *,
    idempotency_key: str = "create-card-1",
) -> KnowledgePropagationEnvelopeV2:
    return KnowledgePropagationEnvelopeV2(
        selection_state="explicit_empty",
        mode="drop",
        knowledge_ids=[],
        justification="No Knowledge is relevant to this card.",
        idempotency_key=idempotency_key,
        expected_revision=0,
    )


def _card_data(
    *,
    idempotency_key: str = "create-card-1",
) -> CardCreate:
    return CardCreate(
        title="Implement governed propagation",
        spec_id="spec-1",
        knowledge_propagation=_envelope(idempotency_key=idempotency_key),
    )


def _operation_kind(selection_state: KnowledgeSelectionState) -> KnowledgeMutationKind:
    if selection_state is KnowledgeSelectionState.OMITTED:
        return KnowledgeMutationKind.REPLACE_OMITTED
    if selection_state is KnowledgeSelectionState.EXPLICIT_EMPTY:
        return KnowledgeMutationKind.REPLACE_EMPTY
    return KnowledgeMutationKind.REPLACE


def _receipt(
    mutation_command: Any,
    *,
    replayed: bool = False,
) -> KnowledgeMutationReceipt:
    result = KnowledgeMutationResultV2(
        operation_id="operation-1",
        target=mutation_command.target,
        operation_kind=_operation_kind(
            mutation_command.selection.selection_state
        ),
        previous_revision=0,
        revision=1,
        selection_state=mutation_command.selection.selection_state,
        assignments=(),
        creation_result=mutation_command.creation_result,
    )
    return KnowledgeMutationReceipt(
        operation_id=result.operation_id,
        target=result.target,
        operation_kind=result.operation_kind,
        previous_revision=0,
        revision=1,
        request_hash="a" * 64,
        applied_at=NOW,
        replayed=replayed,
        outcome=(
            KnowledgeMutationOutcome.REPLAYED
            if replayed
            else KnowledgeMutationOutcome.APPLIED
        ),
        original_outcome=(
            KnowledgeMutationOutcome.APPLIED if replayed else None
        ),
        details={"result_v2": result.to_dict()},
    )


class _KnowledgeFacade:
    def __init__(
        self,
        events: list[str],
        *,
        replay: bool = False,
    ) -> None:
        self.events = events
        self.replay = replay
        self.target_id: str | None = None

    async def preflight_creation(self, command: Any) -> Any:
        self.events.append("knowledge_preflight")
        mutation_command = command.to_mutation_command()
        self.target_id = mutation_command.target.target_id
        if self.replay:
            return _receipt(mutation_command, replayed=True)
        return KnowledgeMutationPreparation(
            parent=command.parent,
            command=mutation_command,
            evidence_fingerprint="b" * 64,
        )

    async def mutate(self, preparation: KnowledgeMutationPreparation) -> Any:
        self.events.append("knowledge_mutate")
        return _receipt(preparation.command)

    @staticmethod
    def result_from_receipt(
        receipt: KnowledgeMutationReceipt,
    ) -> KnowledgeMutationResultV2:
        return KnowledgeMutationResultV2Projector.from_receipt(receipt)


class _Cards:
    def __init__(
        self,
        events: list[str],
        knowledge: _KnowledgeFacade,
        *,
        target_exists: bool = False,
        creation_conflict: bool = False,
        origin_spec_id: str | None = None,
    ) -> None:
        self.events = events
        self.knowledge = knowledge
        self.target_exists = target_exists
        self.creation_conflict = creation_conflict
        self.origin_spec_id = origin_spec_id
        self.created = 0

    async def get_card(self, card_id: str) -> Any:
        self.events.append("card_read")
        if self.origin_spec_id is not None and card_id == "origin-card":
            return SimpleNamespace(
                id=card_id,
                board_id="board-1",
                spec_id=self.origin_spec_id,
            )
        if self.target_exists and card_id == self.knowledge.target_id:
            return SimpleNamespace(
                id=card_id,
                board_id="board-1",
                spec_id="spec-1",
                title="mutated after the original response",
            )
        return None

    async def create_card(
        self,
        board_id: str,
        actor_id: str,
        data: CardCreate,
        **kwargs: Any,
    ) -> Any:
        self.events.append("card_create")
        self.created += 1
        assert kwargs["knowledge_propagation_v2"] is True
        assert kwargs["target_id"] == self.knowledge.target_id
        if self.creation_conflict:
            raise ApplicationRecordConflictError("card", kwargs["target_id"])
        return SimpleNamespace(
            id=kwargs["target_id"],
            board_id=board_id,
            spec_id=data.spec_id,
        )


class _Specs:
    async def get_spec(self, spec_id: str) -> Any:
        if spec_id in {"spec-1", "spec-2"}:
            return SimpleNamespace(id=spec_id, board_id="board-1")
        return None


class _Boards:
    async def _log_activity(self, **kwargs: Any) -> None:
        del kwargs


class _Uow:
    def __init__(
        self,
        *,
        replay: bool = False,
        target_exists: bool = False,
        creation_conflict: bool = False,
        origin_spec_id: str | None = None,
    ) -> None:
        self.events: list[str] = []
        knowledge = _KnowledgeFacade(self.events, replay=replay)
        cards = _Cards(
            self.events,
            knowledge,
            target_exists=target_exists,
            creation_conflict=creation_conflict,
            origin_spec_id=origin_spec_id,
        )
        self.services = SimpleNamespace(
            knowledge_propagation=knowledge,
            cards=cards,
            specs=_Specs(),
            boards=_Boards(),
        )
        self.cards = cards
        self.commits = 0
        self.synchronize_error: Exception | None = None

    async def synchronize(
        self,
        *,
        conflict_error: Exception | None = None,
    ) -> None:
        self.events.append("synchronize")
        self.synchronize_error = conflict_error

    async def commit(self) -> None:
        self.events.append("commit")
        self.commits += 1


def _actor() -> ActorContext:
    return ActorContext(
        "agent-1",
        "mcp",
        actor_name="Agent",
        board_id="board-1",
    )


async def test_create_card_v2_preflights_before_target_and_stages_atomically() -> None:
    uow = _Uow()

    result = await CreateCardKnowledgeV2UseCase().execute(
        CreateCardKnowledgeV2Command("board-1", _card_data()),
        actor=_actor(),
        uow=uow,  # type: ignore[arg-type]
    )

    assert uow.events.index("knowledge_preflight") < uow.events.index("card_create")
    assert uow.events.index("card_create") < uow.events.index("synchronize")
    assert uow.events.index("synchronize") < uow.events.index("knowledge_mutate")
    assert uow.events[-1] == "commit"
    assert isinstance(uow.synchronize_error, KnowledgeCreationRaceError)
    projected = project_card_create_response(result)
    assert projected.card["id"] == uow.services.knowledge_propagation.target_id
    assert projected.card["title"] == "Implement governed propagation"
    assert projected.selection_state is KnowledgeSelectionState.EXPLICIT_EMPTY


async def test_create_card_v2_replay_uses_receipt_and_does_not_recreate_target() -> None:
    uow = _Uow(replay=True, target_exists=True)

    result = await CreateCardKnowledgeV2UseCase().execute(
        CreateCardKnowledgeV2Command("board-1", _card_data()),
        actor=_actor(),
        uow=uow,  # type: ignore[arg-type]
    )

    assert uow.cards.created == 0
    assert "synchronize" not in uow.events
    assert "knowledge_mutate" not in uow.events
    assert uow.commits == 1
    projected = project_card_create_response(result)
    assert projected.replayed is True
    assert projected.card["title"] == "Implement governed propagation"


async def test_create_card_v2_fails_closed_on_target_without_ledger() -> None:
    uow = _Uow(target_exists=True)

    with pytest.raises(KnowledgePropagationServiceError) as caught:
        await CreateCardKnowledgeV2UseCase().execute(
            CreateCardKnowledgeV2Command("board-1", _card_data()),
            actor=_actor(),
            uow=uow,  # type: ignore[arg-type]
        )

    assert caught.value.code == "knowledge_creation_target_collision"
    assert uow.cards.created == 0
    assert "knowledge_mutate" not in uow.events
    assert "commit" not in uow.events


async def test_create_card_v2_maps_immediate_insert_collision_to_creation_race() -> None:
    uow = _Uow(creation_conflict=True)

    with pytest.raises(KnowledgeCreationRaceError) as caught:
        await CreateCardKnowledgeV2UseCase().execute(
            CreateCardKnowledgeV2Command("board-1", _card_data()),
            actor=_actor(),
            uow=uow,  # type: ignore[arg-type]
        )

    assert caught.value.target.target_id == uow.services.knowledge_propagation.target_id
    assert isinstance(caught.value.__cause__, ApplicationRecordConflictError)
    assert "synchronize" not in uow.events
    assert "knowledge_mutate" not in uow.events
    assert "commit" not in uow.events


async def test_create_bug_v2_resolves_authoritative_parent_before_preflight() -> None:
    uow = _Uow(origin_spec_id="spec-1")
    data = CardCreate(
        title="Bug governed by origin",
        card_type="bug",
        origin_task_id="origin-card",
        severity="major",
        expected_behavior="Expected",
        observed_behavior="Observed",
        spec_id=None,
        knowledge_propagation=_envelope(idempotency_key="bug-create-1"),
    )

    await CreateCardKnowledgeV2UseCase().execute(
        CreateCardKnowledgeV2Command("board-1", data),
        actor=_actor(),
        uow=uow,  # type: ignore[arg-type]
    )

    assert data.spec_id == "spec-1"
    assert uow.events.index("card_read") < uow.events.index("knowledge_preflight")


async def test_create_bug_v2_rejects_spec_divergent_from_origin_before_preflight() -> None:
    uow = _Uow(origin_spec_id="spec-1")
    data = CardCreate(
        title="Bug with divergent parent",
        card_type="bug",
        origin_task_id="origin-card",
        severity="major",
        expected_behavior="Expected",
        observed_behavior="Observed",
        spec_id="spec-2",
        knowledge_propagation=_envelope(idempotency_key="bug-create-2"),
    )

    with pytest.raises(KnowledgePropagationServiceError) as caught:
        await CreateCardKnowledgeV2UseCase().execute(
            CreateCardKnowledgeV2Command("board-1", data),
            actor=_actor(),
            uow=uow,  # type: ignore[arg-type]
        )

    assert caught.value.code == "knowledge_propagation_parent_conflict"
    assert "knowledge_preflight" not in uow.events
    assert "card_create" not in uow.events


async def test_mcp_refinement_v2_rejects_legacy_kb_ids_before_target() -> None:
    command = McpDeriveSpecCommand(
        "refinement",
        "refinement-1",
        kb_ids=[],
        knowledge_propagation=_envelope(idempotency_key="derive-1"),
    )

    with pytest.raises(KnowledgePropagationServiceError) as caught:
        await McpDeriveSpecUseCase().execute(
            command,
            actor=_actor(),
            uow=SimpleNamespace(),  # type: ignore[arg-type]
        )

    assert caught.value.code == "conflicting_propagation_parameters"


def test_public_v2_request_models_preserve_tri_state_and_linkage_contract() -> None:
    omitted = KnowledgePropagationEnvelopeV2(
        selection_state="omitted",
        idempotency_key=" omit ",
    )
    assert omitted.idempotency_key == "omit"
    assert omitted.to_selection().selection_state is KnowledgeSelectionState.OMITTED
    conflict_probe = DeriveSpecKnowledgeRequest(
        knowledge_propagation=omitted,
        kb_ids=["legacy-kb"],
    )
    assert conflict_probe.kb_ids == ["legacy-kb"]

    drop_all = KnowledgeAssignmentDropRequest(
        knowledge_ids=[],
        justification=" remove all ",
        idempotency_key=" drop-all ",
        expected_revision=4,
    )
    drop_selection = drop_all.to_envelope().to_selection()
    assert drop_selection.selection_state is KnowledgeSelectionState.EXPLICIT_EMPTY
    assert drop_selection.mode is KnowledgePropagationMode.DROP

    replace = KnowledgeAssignmentReplaceRequest(
        knowledge_ids=["root-2", "root-1"],
        mode="snapshot",
        justification=" relevant ",
        idempotency_key=" replace ",
        expected_revision=3,
        linkage=[
            {
                "entity_type": "functional_requirement",
                "entity_id": " fr-1 ",
            }
        ],
    )
    assert replace.knowledge_ids == ["root-1", "root-2"]
    assert replace.linkage[0].entity_id == "fr-1"
    assert replace.model_dump(mode="json")["linkage"][0]["entity_id"] == "fr-1"

    refresh_schema = KnowledgeAssignmentRefreshRequest.model_json_schema()
    assert "justification" not in refresh_schema["properties"]

    with pytest.raises(ValidationError):
        KnowledgePropagationEnvelopeV2(
            selection_state="explicit_empty",
            mode="drop",
            justification="why",
            idempotency_key="create",
            expected_revision=1,
        )
    with pytest.raises(ValidationError):
        KnowledgePropagationEnvelopeV2(
            selection_state="omitted",
            idempotency_key="   ",
        )
    with pytest.raises(ValidationError):
        KnowledgeAssignmentReplaceRequest(
            knowledge_ids=["root-1"],
            mode="reference",
            justification="   ",
            idempotency_key="replace",
            expected_revision=0,
        )
    with pytest.raises(ValidationError):
        KnowledgeAssignmentReplaceRequest(
            knowledge_ids=["root-1"],
            mode="reference",
            justification="why",
            idempotency_key="replace",
            expected_revision=0,
            linkage=[
                {
                    "entity_type": "acceptance_criterion",
                    "entity_id": "  ",
                }
            ],
        )

    for missing_justification in (None, "   "):
        drop_without_reason = KnowledgeAssignmentDropRequest(
            knowledge_ids=[],
            justification=missing_justification,
            idempotency_key="drop-without-reason",
            expected_revision=0,
        )
        with pytest.raises(KnowledgePropagationContractError) as caught:
            drop_without_reason.to_envelope()
        assert caught.value.code == "knowledge_drop_justification_required"


def test_creation_race_is_a_retryable_service_error() -> None:
    target = KnowledgeTargetKey("board-1", "card", "card-1")
    error = KnowledgeCreationRaceError(target)

    assert isinstance(error, KnowledgePropagationServiceError)
    assert error.code == "knowledge_creation_race"
    assert error.retryable is True
    assert error.details["target"] == target.to_dict()


@pytest.mark.parametrize(
    "error",
    [
        KnowledgePropagationPortError(
            "knowledge_creation_race",
            "concurrent deterministic creation won",
        ),
        KnowledgePropagationServiceError(
            "knowledge_creation_race",
            "concurrent deterministic creation won",
        ),
    ],
)
def test_creation_race_code_is_retryable_across_error_layers(
    error: Exception,
) -> None:
    assert project_knowledge_propagation_error(error)["retryable"] is True


@pytest.mark.parametrize(
    ("mode", "state", "operation_kind"),
    [
        ("reference", "active", KnowledgeMutationKind.REPLACE),
        ("drop", "dropped", KnowledgeMutationKind.DROP_DELTA),
    ],
)
def test_mutation_projector_flattens_public_assignment_shape(
    mode: str,
    state: str,
    operation_kind: KnowledgeMutationKind,
) -> None:
    target = KnowledgeTargetKey("board-1", "card", "card-1")
    assignment = KnowledgeAssignment(
        assignment_id=f"assignment-{mode}",
        board_id="board-1",
        target_type="card",
        target_id="card-1",
        source_knowledge_id="source-kb-1",
        revision_stamp=ResourceRevisionStamp(
            root_id="root-kb-1",
            source_revision="7",
            source_content_sha256=hashlib.sha256(b"content").hexdigest(),
        ),
        mode=mode,
        state=state,
        origin_class="v2",
        actor_id="agent-1",
        revision=1,
        justification="relevant" if mode != "drop" else "remove obsolete context",
    )
    result_v2 = KnowledgeMutationResultV2(
        operation_id=f"operation-{mode}",
        target=target,
        operation_kind=operation_kind,
        previous_revision=0,
        revision=1,
        selection_state="explicit_ids",
        assignments=(assignment,),
    )
    receipt = KnowledgeMutationReceipt(
        operation_id=result_v2.operation_id,
        target=target,
        operation_kind=operation_kind,
        previous_revision=0,
        revision=1,
        request_hash="c" * 64,
        applied_at=NOW,
        details={"result_v2": result_v2.to_dict()},
    )

    projected = project_knowledge_mutation_response(
        SimpleNamespace(receipt=receipt, result_v2=result_v2)
    ).model_dump(mode="json")

    assert projected["assignments"] == [
        {
            "root_knowledge_id": "root-kb-1",
            "source_knowledge_id": "source-kb-1",
            "mode": mode,
            "state": state,
            "stale": False,
        }
    ]


async def test_v2_resource_exclusion_preserves_non_knowledge_autocopy(
    monkeypatch: Any,
) -> None:
    card = ResourcePropagationCardRecord(
        id="card-1",
        board_id="board-1",
        knowledge_bases=[],
        screen_mockups=[],
    )
    spec = ResourcePropagationSpecFact(
        id="spec-1",
        board_id="board-1",
        screen_mockups=({"id": "screen-1", "title": "Primary screen"},),
        version=1,
    )

    class _Store:
        def __init__(self) -> None:
            self.audits: list[dict[str, Any]] = []

        async def get_board(self, context: Any, *, board_id: str) -> Any:
            del context
            assert board_id == "board-1"
            return ResourcePropagationBoardFact(
                id=board_id,
                settings={
                    "auto_derive_spec_resources_enabled": True,
                    "auto_derive_spec_resource_types": [
                        "knowledge_base",
                        "mockup",
                    ],
                },
            )

        async def get_spec(self, context: Any, *, spec_id: str) -> Any:
            del context
            return spec if spec_id == spec.id else None

        async def get_card(self, context: Any, *, card_id: str) -> Any:
            del context
            return card if card_id == card.id else None

        async def list_spec_knowledge_bases(
            self,
            context: Any,
            *,
            spec_id: str,
        ) -> Any:
            del context, spec_id
            raise AssertionError("v2 card creation must not invoke v1 KB copy")

        async def save_card(
            self,
            context: Any,
            record: Any,
            *,
            changed_fields: Any,
        ) -> None:
            del context
            assert record is card
            assert tuple(changed_fields) == ("screen_mockups",)

        async def record_audit(self, context: Any, **kwargs: Any) -> None:
            del context
            self.audits.append(kwargs)

    async def _allow_mockups(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    from okto_pulse.core.services import design_system

    store = _Store()
    register_spec_resource_propagation_store(store)  # type: ignore[arg-type]
    monkeypatch.setattr(
        design_system,
        "gate_entity_screen_mockups",
        _allow_mockups,
    )

    result = await SpecResourcePropagationService(object()).propagate_for_card(
        board_id="board-1",
        spec_id="spec-1",
        card_id="card-1",
        actor_id="agent-1",
        trigger="card_created",
        excluded_resource_types={"knowledge_base"},
    )

    assert result["resource_types"] == ["mockup"]
    assert result["results"]["mockup"]["copied_ids"] == ["screen-1"]
    assert card.knowledge_bases == []
    assert [item["id"] for item in card.screen_mockups] == ["screen-1"]
    assert store.audits[0]["details"]["resource_types"] == ["mockup"]
