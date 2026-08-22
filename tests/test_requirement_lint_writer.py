from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.quality_canonicalization import (
    SEMANTIC_FIELD_MANIFEST_V1,
)
from okto_pulse.core.models.schemas import SpecCreate
from okto_pulse.core.ports.requirement_lint import (
    RequirementLintExecutionFailed,
    RequirementLintWriteCommand,
    RequirementLintWriteResult,
    RequirementLintWriter,
    RequirementLintWriterContractError,
    register_requirement_lint_writer_hook,
)
from okto_pulse.core.services.main import SpecService
from okto_pulse.core.services.requirement_lint_writer import (
    spec_requirement_lint_payload,
    stage_spec_requirement_lint,
)
from sqlalchemy_test_models import Board, Spec


def _canonical_requirement(child_id: str, text: str) -> dict[str, str]:
    return {"id": child_id, "text": text, "status": "active"}


def _spec_snapshot() -> SimpleNamespace:
    return SimpleNamespace(
        id="spec-writer-1",
        board_id="board-writer-1",
        version=3,
        status=SpecStatus.DRAFT,
        archived=False,
        title="Governed writer",
        description="A complete post-mutation snapshot.",
        context="Requirement-lint contract verification.",
        functional_requirements=[
            _canonical_requirement("fr_writer_1", "The API returns a receipt.")
        ],
        technical_requirements=[
            _canonical_requirement("tr_writer_1", "Persist through the caller UoW.")
        ],
        acceptance_criteria=[
            _canonical_requirement("ac_writer_1", "The hook runs exactly once.")
        ],
        test_scenarios=[],
        business_rules=[],
        api_contracts=[],
        integration_requirements=[],
        observability_requirements=[],
        decisions=[],
        labels=["not-semantic"],
        current_validation_id="not-semantic",
        created_by="not-semantic",
    )


def _valid_result(*, replayed: bool = False) -> RequirementLintWriteResult:
    return RequirementLintWriteResult(
        receipt_id="qar_writer_1",
        head_revision=4,
        evaluated_rule_count=8,
        finding_count=1,
        replayed=replayed,
    )


class _RecordingHook:
    def __init__(
        self,
        *,
        result: object | None = None,
        failure: BaseException | None = None,
    ) -> None:
        self.result = _valid_result() if result is None else result
        self.failure = failure
        self.calls: list[tuple[object, RequirementLintWriteCommand]] = []

    async def stage_requirement_lint(
        self,
        context: object,
        command: RequirementLintWriteCommand,
    ) -> RequirementLintWriteResult:
        self.calls.append((context, command))
        if self.failure is not None:
            raise self.failure
        return self.result  # type: ignore[return-value]


def test_write_command_carries_the_complete_canonical_post_mutation_payload() -> None:
    spec = _spec_snapshot()
    payload = spec_requirement_lint_payload(spec)

    command = RequirementLintWriteCommand(
        board_id=spec.board_id,
        spec_id=spec.id,
        spec_version=spec.version,
        actor_id="agent-writer",
        writer=RequirementLintWriter.BULK_UPDATE,
        spec_status=spec.status.value,
        spec_archived=spec.archived,
        changed_fields=["functional_requirements", "technical_requirements"],
        spec_payload=payload,
    )

    assert command.changed_fields == (
        "functional_requirements",
        "technical_requirements",
    )
    assert command.writer is RequirementLintWriter.BULK_UPDATE
    assert command.spec_status == "draft"
    assert command.spec_payload == payload
    assert set(payload) == set(SEMANTIC_FIELD_MANIFEST_V1["spec"]) | {
        "id",
        "board_id",
        "version",
    }
    assert {
        "status",
        "archived",
        "labels",
        "current_validation_id",
        "created_by",
    }.isdisjoint(payload)


@pytest.mark.parametrize(
    ("payload_patch", "error_code"),
    [
        ({"board_id": "another-board"}, "requirement_lint_spec_identity_mismatch"),
        (
            {
                "functional_requirements": [
                    {"id": "fr_missing_status", "text": "Missing status"}
                ]
            },
            "requirement_lint_requirement_not_canonical",
        ),
        (
            {
                "functional_requirements": [
                    {
                        "id": "fr_invalid_locale",
                        "text": "Unsupported locale must fail closed.",
                        "status": "active",
                        "locale": "pt-BR",
                    }
                ]
            },
            "requirement_lint_locale_invalid",
        ),
    ],
)
def test_write_command_rejects_an_invalid_or_noncanonical_payload(
    payload_patch: dict[str, object],
    error_code: str,
) -> None:
    spec = _spec_snapshot()
    payload = spec_requirement_lint_payload(spec)
    payload.update(payload_patch)

    with pytest.raises(RequirementLintWriterContractError, match=error_code):
        RequirementLintWriteCommand(
            board_id=spec.board_id,
            spec_id=spec.id,
            spec_version=spec.version,
            actor_id="agent-writer",
            writer=RequirementLintWriter.BULK_UPDATE,
            spec_status=spec.status.value,
            spec_archived=False,
            changed_fields=("functional_requirements",),
            spec_payload=payload,
        )


@pytest.mark.parametrize("writer", tuple(RequirementLintWriter))
async def test_stage_invokes_the_hook_once_and_preserves_writer_identity(
    writer: RequirementLintWriter,
) -> None:
    context = object()
    spec = _spec_snapshot()
    hook = _RecordingHook(result=_valid_result(replayed=True))
    register_requirement_lint_writer_hook(hook)

    result = await stage_spec_requirement_lint(
        context,
        spec,
        actor_id="agent-writer",
        writer=writer,
        changed_fields=("functional_requirements",),
    )

    assert result == _valid_result(replayed=True)
    assert len(hook.calls) == 1
    received_context, command = hook.calls[0]
    assert received_context is context
    assert command.writer is writer
    assert command.actor_id == "agent-writer"
    assert command.spec_id == spec.id
    assert command.spec_version == spec.version


async def test_hook_failure_is_normalized_to_the_single_writer_error() -> None:
    class HookFailure(RuntimeError):
        pass

    hook = _RecordingHook(failure=HookFailure("adapter exploded"))
    register_requirement_lint_writer_hook(hook)

    with pytest.raises(RequirementLintExecutionFailed) as raised:
        await stage_spec_requirement_lint(
            object(),
            _spec_snapshot(),
            actor_id="agent-writer",
            writer=RequirementLintWriter.STRUCTURED_CRUD,
            changed_fields=("acceptance_criteria",),
        )

    assert raised.value.code == "requirement_lint_execution_failed"
    assert raised.value.stage == "writer_hook"
    assert raised.value.detail == "HookFailure"
    assert isinstance(raised.value.__cause__, HookFailure)
    assert len(hook.calls) == 1


async def test_invalid_hook_result_is_fail_closed_and_normalized() -> None:
    hook = _RecordingHook(result={"receipt_id": "not-a-contract-result"})
    register_requirement_lint_writer_hook(hook)

    with pytest.raises(RequirementLintExecutionFailed) as raised:
        await stage_spec_requirement_lint(
            object(),
            _spec_snapshot(),
            actor_id="agent-writer",
            writer=RequirementLintWriter.SEED,
            changed_fields=("functional_requirements",),
        )

    assert raised.value.code == "requirement_lint_execution_failed"
    assert raised.value.stage == "writer_hook"
    assert raised.value.detail == "TypeError"
    assert isinstance(raised.value.__cause__, TypeError)
    assert str(raised.value.__cause__) == "requirement_lint_writer_hook_result_invalid"
    assert len(hook.calls) == 1


def test_payload_is_a_defensive_snapshot_of_nested_semantic_state() -> None:
    spec = _spec_snapshot()
    payload = spec_requirement_lint_payload(spec)

    spec.functional_requirements[0]["text"] = "Mutation after snapshot"
    assert payload["functional_requirements"][0]["text"] == (
        "The API returns a receipt."
    )

    payload["technical_requirements"][0]["text"] = "Mutation inside snapshot"
    assert spec.technical_requirements[0]["text"] == ("Persist through the caller UoW.")


def test_payload_canonicalizes_legacy_requirements_without_mutating_source() -> None:
    spec = _spec_snapshot()
    spec.functional_requirements = ["Legacy FR"]
    spec.acceptance_criteria = ["Legacy AC"]
    spec.business_rules = [{"linked_requirements": ["0"]}]

    payload = spec_requirement_lint_payload(spec)

    assert spec.functional_requirements == ["Legacy FR"]
    assert spec.acceptance_criteria == ["Legacy AC"]
    assert spec.business_rules == [{"linked_requirements": ["0"]}]
    assert payload["functional_requirements"] == [
        {
            "id": payload["functional_requirements"][0]["id"],
            "text": "Legacy FR",
            "status": "active",
        }
    ]
    assert payload["acceptance_criteria"] == [
        {
            "id": payload["acceptance_criteria"][0]["id"],
            "text": "Legacy AC",
            "status": "active",
        }
    ]
    assert payload["business_rules"] == [{"linked_requirements": ["0"]}]


async def test_spec_creation_does_not_invoke_or_roll_back_for_legacy_lint_hook_failure(
    db_factory,
) -> None:
    board_id = f"board-lint-rollback-{uuid4().hex[:10]}"
    actor_id = "agent-lint-rollback"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Lint rollback", owner_id=actor_id, settings={}))
        await db.commit()

    class PersistenceFailure(RuntimeError):
        pass

    hook = _RecordingHook(failure=PersistenceFailure("receipt insert failed"))
    register_requirement_lint_writer_hook(hook)

    async with db_factory() as db:
        created = await SpecService(db).create_spec(
            board_id,
            actor_id,
            SpecCreate(
                title="External lint is independent",
                delivery_context="brownfield",
                functional_requirements=["The API must return a result."],
            ),
        )
        assert created is not None
        created_spec_id = created.id
        assert hook.calls == []
        await db.commit()

    async with db_factory() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(Spec).where(Spec.board_id == board_id)
            )
            == 1
        )
        persisted = await db.get(Spec, created_spec_id)
        assert persisted is not None
        assert persisted.title == "External lint is independent"


async def test_create_spec_has_no_requirement_lint_writer_route_coupling(
    db_factory,
) -> None:
    board_id = f"board-lint-route-{uuid4().hex[:10]}"
    actor_id = "agent-lint-route"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Lint route", owner_id=actor_id, settings={}))
        await db.commit()

    hook = _RecordingHook()
    register_requirement_lint_writer_hook(hook)
    async with db_factory() as db:
        created = await SpecService(db).create_spec(
            board_id,
            actor_id,
            SpecCreate(
                title="External agent owns requirement lint",
                delivery_context="brownfield",
            ),
        )
        assert created is not None
        await db.commit()

    assert hook.calls == []
