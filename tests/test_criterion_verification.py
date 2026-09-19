"""P2 criterion ownership, exact references and the existing governed writers."""

import copy
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import select, func

from sqlalchemy_test_models import Board, Spec, SpecHistory, DomainEventRow
from okto_pulse.core.domain.criterion_verification import (
    criterion_verification_fields,
    validate_criterion_requirement_links,
    VERIFICATION_REQUIREMENT_FIELDS,
)
from okto_pulse.core.infra.permissions import get_builtin_presets, resolve_permissions
from okto_pulse.core.models.schemas import SpecCreate, SpecUpdate
from okto_pulse.core.services.main import SpecService
from okto_pulse.core.services.spec_structured_entities import (
    StructuredSpecEntityCommand,
    StructuredSpecEntityService,
)


def link(kind="functional_requirement", target="fr_one", **extra):
    return {"requirement_type": kind, "requirement_id": target, **extra}


def criterion(**extra):
    return {
        "id": "ac_one",
        "text": "After five failures, access is blocked",
        "verification_profile": "functional",
        "requirement_links": [link()],
        **extra,
    }


@pytest.mark.parametrize(
    "metadata",
    [
        {"verification_profile": "none"},
        {"verification_profile": False},
        {"requirement_links": [link(requirement_id=1)]},
        {"requirement_links": [link(target=" ")]},
        {"requirement_links": [link(aspect=" ")]},
        {"requirement_links": [link(verified=True)]},
        {"requirement_links": [link(kind="unknown")]},
        {"requirement_links": [link(), link(aspect="another aspect")]},
        {"requirement_links": [link(target=str(i)) for i in range(101)]},
    ],
)
def test_closed_metadata_rejects_bypasses_and_ambiguous_links(metadata):
    with pytest.raises(ValidationError):
        criterion_verification_fields(metadata)


@pytest.mark.parametrize("kind,field", VERIFICATION_REQUIREMENT_FIELDS.items())
def test_each_supported_type_requires_its_exact_unambiguous_identity(kind, field):
    row = criterion(requirement_links=[link(kind=kind)])
    validate_criterion_requirement_links([row], {field: [{"id": "fr_one"}]})
    for values in (
        [],
        [{"id": "other", "text": "fr_one"}],
        [{"id": "fr_one"}, {"id": "fr_one"}],
    ):
        with pytest.raises(ValueError, match="criterion_requirement_link_unresolved"):
            validate_criterion_requirement_links([row], {field: values})


def test_draft_gaps_are_not_filled_and_inactive_references_keep_history():
    assert criterion_verification_fields({"text": "legacy"}) == {}
    assert criterion_verification_fields(
        {"verification_profile": None, "requirement_links": []}
    ) == {"verification_profile": None, "requirement_links": []}
    # Activity and readiness are separate from referential integrity.
    validate_criterion_requirement_links(
        [criterion()],
        {"functional_requirements": [{"id": "fr_one", "status": "revoked"}]},
    )


async def seed(db_factory):
    board_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Verification", owner_id="author"))
        await db.commit()
    return board_id


async def create(db, board_id, **fields):
    return await SpecService(db).create_spec(
        board_id,
        "author",
        SpecCreate(
            title="Verification",
            delivery_context="brownfield",
            functional_requirements=[
                {"id": "fr_one", "text": "Lock after five failures"}
            ],
            **fields,
        ),
    )


@pytest.mark.asyncio
async def test_bulk_create_update_preserve_owned_metadata_and_reject_dangling_refs(
    db_factory,
):
    board_id = await seed(db_factory)
    async with db_factory() as db:
        spec = await create(db, board_id, acceptance_criteria=[criterion()])
        await db.commit()
        spec_id = spec.id
    async with db_factory() as db:
        updated = await SpecService(db).update_spec(
            spec_id,
            "author",
            SpecUpdate(
                acceptance_criteria=[
                    criterion(
                        text="Five attempts observed",
                        requirement_links=[link(aspect="attempt count")],
                    )
                ],
            ),
        )
        await db.commit()
        assert updated.acceptance_criteria[0]["id"] == "ac_one"
        assert (
            updated.acceptance_criteria[0]["requirement_links"][0]["aspect"]
            == "attempt count"
        )
        assert "criteria" not in updated.functional_requirements[0]
    async with db_factory() as db:
        with pytest.raises(ValueError, match="criterion_requirement_link_unresolved"):
            await SpecService(db).update_spec(
                spec_id, "author", SpecUpdate(functional_requirements=[])
            )
        await db.rollback()
    async with db_factory() as db:
        assert (await db.get(Spec, spec_id)).functional_requirements[0][
            "id"
        ] == "fr_one"


@pytest.mark.asyncio
@pytest.mark.parametrize("reference", ["0", "Lock after five failures", "foreign-fr"])
async def test_create_rejects_text_position_and_foreign_ids_before_any_spec_write(
    db_factory, reference
):
    board_id = await seed(db_factory)
    async with db_factory() as db:
        with pytest.raises(ValueError, match="criterion_requirement_link_unresolved"):
            await create(
                db,
                board_id,
                acceptance_criteria=[
                    criterion(requirement_links=[link(target=reference)])
                ],
            )
        assert (
            await db.scalar(
                select(func.count()).select_from(Spec).where(Spec.board_id == board_id)
            )
            == 0
        )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(DomainEventRow)
                .where(DomainEventRow.board_id == board_id)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_structured_write_keeps_version_history_and_content_when_rejected(
    db_factory,
):
    board_id = await seed(db_factory)
    flags = next(p["flags"] for p in get_builtin_presets() if p["name"] == "Spec")
    permissions = resolve_permissions(None, flags, None)
    async with db_factory() as db:
        spec = await create(
            db,
            board_id,
            acceptance_criteria=[{"id": "ac_one", "text": "Existing condition"}],
        )
        await db.commit()
        spec_id = spec.id
    async with db_factory() as db:
        service = StructuredSpecEntityService(db)

        def command(payload, version):
            return StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id="author",
                entity_type="acceptance_criterion",
                entity_id="ac_one",
                operation="update",
                expected_spec_version=version,
                payload=payload,
                permission_set=permissions,
            )

        result = await service.mutate(
            command(criterion_verification_fields(criterion()), 1)
        )
        assert result.success
        await db.commit()
        persisted = await db.get(Spec, spec_id)
        before = copy.deepcopy(persisted.acceptance_criteria)
        assert before[0]["text"] == "Existing condition"
        assert before[0]["verification_profile"] == "functional"
        version = persisted.version
        histories = await db.scalar(
            select(func.count())
            .select_from(SpecHistory)
            .where(SpecHistory.spec_id == spec_id)
        )
        refused = await service.mutate(
            command({"requirement_links": [link(target="missing")]}, version)
        )
        assert not refused.success
        assert persisted.acceptance_criteria == before
        assert persisted.version == version
        assert (
            await db.scalar(
                select(func.count())
                .select_from(SpecHistory)
                .where(SpecHistory.spec_id == spec_id)
            )
            == histories
        )
        # A text-only edit must preserve these new fields.
        result = await service.mutate(command({"text": "Edited condition"}, version))
        assert result.success
        await db.commit()
        assert (
            persisted.acceptance_criteria[0]["requirement_links"]
            == before[0]["requirement_links"]
        )

        preview = await service.mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id="author",
                entity_type="functional_requirement",
                entity_id="fr_one",
                operation="revoke",
                expected_spec_version=version + 1,
                permission_set=permissions,
            )
        )
        assert not preview.success
        assert preview.error_code == "impact_ack_required"
        assert any(
            ref["target_type"] == "acceptance_criterion" and ref["target_id"] == "ac_one"
            for ref in preview.impact_report["impacted_refs"]
        )


@pytest.mark.asyncio
async def test_mcp_criterion_metadata_reaches_the_same_store(db_factory, monkeypatch):
    import json
    from types import SimpleNamespace
    from mcp_runtime_testing import register_mcp_test_runtime
    from okto_pulse.core.mcp import server

    board_id = await seed(db_factory)
    async with db_factory() as db:
        spec = await create(
            db, board_id, acceptance_criteria=[{"id": "ac_one", "text": "Condition"}]
        )
        await db.commit()
        spec_id = spec.id
    flags = next(p["flags"] for p in get_builtin_presets() if p["name"] == "Spec")
    context = SimpleNamespace(
        agent_id="author",
        agent_name="author",
        board_id=board_id,
        permissions=resolve_permissions(None, flags, None),
    )

    async def get_context(_board):
        return context

    register_mcp_test_runtime(db_factory)
    monkeypatch.setattr(server, "_get_agent_ctx", get_context)
    tool = await server.mcp.get_tool("okto_pulse_update_spec_entity")
    body = json.loads(
        await tool.fn(
            board_id=board_id,
            spec_id=spec_id,
            entity_type="acceptance_criterion",
            entity_id="ac_one",
            operation="update",
            payload_json=criterion_verification_fields(criterion()),
            expected_spec_version=1,
        )
    )
    assert body["success"] is True
    async with db_factory() as db:
        stored = await db.get(Spec, spec_id)
        assert (
            stored.acceptance_criteria[0]["requirement_links"]
            == criterion()["requirement_links"]
        )
        assert stored.acceptance_criteria[0]["verification_profile"] == "functional"
