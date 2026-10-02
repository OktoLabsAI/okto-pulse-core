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
    RequirementLintWriteCommand,
    RequirementLintWriter,
    RequirementLintWriterContractError,
)
from okto_pulse.core.services.main import SpecService
from semantic_spec_testing import semantic_spec_payload
from sqlalchemy_test_models import Board, Spec, QualityAssessmentReceiptFixture


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


def test_write_command_carries_the_complete_canonical_post_mutation_payload() -> None:
    spec = _spec_snapshot()
    payload = semantic_spec_payload(spec)

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
    payload = semantic_spec_payload(spec)
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


async def test_spec_creation_commits_without_automatic_lint(
    db_factory,
) -> None:
    board_id = f"board-lint-rollback-{uuid4().hex[:10]}"
    actor_id = "agent-lint-rollback"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Lint rollback", owner_id=actor_id, settings={}))
        await db.commit()
        assert await db.scalar(
            select(func.count()).select_from(QualityAssessmentReceiptFixture)
            .where(QualityAssessmentReceiptFixture.board_id == board_id)
        ) == 0


    async with db_factory() as db:
        created = await SpecService(db).create_spec(
            board_id,
            actor_id,
            SpecCreate(
                title="External lint is independent",
                delivery_context="brownfield",
                functional_requirements=[{"text": "The API must return a result."}],
            ),
        )
        assert created is not None
        created_spec_id = created.id
        await db.commit()
        assert await db.scalar(
            select(func.count()).select_from(QualityAssessmentReceiptFixture)
            .where(QualityAssessmentReceiptFixture.board_id == board_id)
        ) == 0

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


async def test_create_spec_has_no_automatic_lint_route_coupling(
    db_factory,
) -> None:
    board_id = f"board-lint-route-{uuid4().hex[:10]}"
    actor_id = "agent-lint-route"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Lint route", owner_id=actor_id, settings={}))
        await db.commit()
        assert await db.scalar(
            select(func.count()).select_from(QualityAssessmentReceiptFixture)
            .where(QualityAssessmentReceiptFixture.board_id == board_id)
        ) == 0

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
        assert await db.scalar(
            select(func.count()).select_from(QualityAssessmentReceiptFixture)
            .where(QualityAssessmentReceiptFixture.board_id == board_id)
        ) == 0
