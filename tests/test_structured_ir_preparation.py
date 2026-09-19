"""Whole-set IR preflight shares the real structured writer without writes."""

import copy
import uuid

import pytest

from sqlalchemy_test_models import Spec, SpecStatus
from test_spec_structured_entities import (
    _permission_set,
    _seed_spec,
    _side_effect_counts,
    _spec_json_snapshot,
)
from okto_pulse.core.domain.human_validation_cycle import SubjectEditRequiresDraftError
from okto_pulse.core.services.spec_structured_entities import (
    PreparedIntegrationRequirementCreates,
    StructuredSpecEntityCommand,
    StructuredSpecEntityErrorCode as Error,
    StructuredSpecEntityService,
)


def command(spec_id, board_id, **overrides):
    values = dict(
        spec_id=spec_id,
        board_id=board_id,
        actor_id="author",
        entity_type="integration_requirement",
        operation="create",
        expected_spec_version=1,
        expected_spec_edition=6,
        permission_set=_permission_set("Spec"),
    )
    return StructuredSpecEntityCommand(**(values | overrides))


def payloads():
    return [
        {
            "id": "ir_event",
            "title": "Publish orders",
            "integration_type": "event",
            "data_contract": {"type": "object"},
            "linked_requirements": ["fr_existing"],
        },
        {"id": "ir_mcp", "title": "Read orders", "integration_type": "mcp_tool"},
    ]


@pytest.mark.asyncio
async def test_ir_preparation_canonicalizes_legacy_requirements_and_remaps_references(
    db_factory,
):
    board_id, spec_id = str(uuid.uuid4()), str(uuid.uuid4())
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id="author")
        row = await db.get(Spec, spec_id)
        row.functional_requirements = ["Legacy order requirement"]
        row.acceptance_criteria = ["Legacy order criterion"]
        await db.flush()
        before = await _spec_json_snapshot(db, spec_id)
        prepared = await StructuredSpecEntityService(
            db
        ).prepare_integration_requirement_creates(
            command(spec_id, board_id),
            [
                {
                    "title": "Publish orders",
                    "integration_type": "event",
                    "linked_requirements": ["0"],
                }
            ],
        )
        assert isinstance(prepared, PreparedIntegrationRequirementCreates)
        fr = prepared.update_data["functional_requirements"][0]
        ac = prepared.update_data["acceptance_criteria"][0]
        assert fr["id"].startswith("fr_") and fr["text"] == "Legacy order requirement"
        assert ac["id"].startswith("ac_") and ac["text"] == "Legacy order criterion"
        assert prepared.update_data["integration_requirements"][0][
            "linked_requirements"
        ] == [fr["id"]]
        await db.commit()
        assert await _spec_json_snapshot(db, spec_id) == before


@pytest.mark.asyncio
async def test_ir_preparation_matches_individual_writer_final_state_without_saving(
    db_factory,
):
    board_id, spec_id = str(uuid.uuid4()), str(uuid.uuid4())
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id="author")
        before = await _spec_json_snapshot(db, spec_id)
        effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        authored = payloads()
        untouched = copy.deepcopy(authored)
        service = StructuredSpecEntityService(db)
        prepared = await service.prepare_integration_requirement_creates(
            command(spec_id, board_id), authored
        )
        assert isinstance(prepared, PreparedIntegrationRequirementCreates)
        assert prepared.expected_spec_edition == 6
        assert prepared.expected_spec_version == 1
        assert prepared.entity_ids == ("ir_event", "ir_mcp")
        assert prepared.board_id == board_id and prepared.spec_id == spec_id
        assert authored == untouched
        assert await _spec_json_snapshot(db, spec_id) == before
        assert (
            await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == effects
        )
        # Same inputs cross the existing writer; the new seam does not invent
        # defaults, reference semantics or a competing IR normalization path.
        for index, payload in enumerate(authored):
            result = await service.mutate(
                command(
                    spec_id,
                    board_id,
                    payload=payload,
                    expected_spec_version=1 + index,
                )
            )
            assert result.success, result.as_dict()
        row = await db.get(Spec, spec_id)
        for field, value in prepared.update_data.items():
            assert getattr(row, field) == value
        assert row.edition == 6 and row.version == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"permission_set": None}, Error.AUTHORIZATION_DENIED),
        ({"board_id": "wrong-board"}, Error.VALIDATION_FAILED),
        ({"expected_spec_version": 9}, Error.VERSION_CONFLICT),
        ({"expected_spec_edition": 5}, Error.VERSION_CONFLICT),
        ({"expected_spec_version": None}, Error.VALIDATION_FAILED),
        ({"expected_spec_edition": None}, Error.VALIDATION_FAILED),
        ({"expected_spec_version": True}, Error.VALIDATION_FAILED),
        ({"expected_spec_edition": True}, Error.VALIDATION_FAILED),
        ({"preview_only": True}, Error.VALIDATION_FAILED),
        ({"operation": "update"}, Error.UNSUPPORTED_OPERATION),
    ],
)
async def test_ir_preparation_requires_permissions_scope_and_exact_fences(
    db_factory, overrides, error
):
    board_id, spec_id = str(uuid.uuid4()), str(uuid.uuid4())
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id="author")
        before = await _spec_json_snapshot(db, spec_id)
        effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        request = command(spec_id, board_id)
        for key, value in overrides.items():
            setattr(request, key, value)
        result = await StructuredSpecEntityService(
            db
        ).prepare_integration_requirement_creates(request, payloads())
        assert result.success is False
        assert result.error_code == error
        assert await _spec_json_snapshot(db, spec_id) == before
        assert (
            await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == effects
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["duplicate_id", "unknown_field", "invalid_link", "not_object", "empty"]
)
async def test_invalid_later_ir_never_persists_the_valid_first_ir(db_factory, case):
    board_id, spec_id = str(uuid.uuid4()), str(uuid.uuid4())
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id="author")
        before = await _spec_json_snapshot(db, spec_id)
        effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        authored = payloads()
        if case == "duplicate_id":
            authored[1]["id"] = authored[0]["id"]
        elif case == "unknown_field":
            authored[1]["unknown"] = "must not be ignored"
        elif case == "invalid_link":
            authored[1]["linked_requirements"] = ["fr_missing"]
        elif case == "not_object":
            authored[1] = "malformed"
        else:
            authored = []
        result = await StructuredSpecEntityService(
            db
        ).prepare_integration_requirement_creates(
            command(spec_id, board_id),
            authored,
        )
        assert result.success is False
        assert result.error_code in {Error.VALIDATION_FAILED, Error.LINK_TARGET_INVALID}
        # Commit on purpose: even an accidental caller commit cannot publish
        # the good first item or speculative history from a failed preflight.
        await db.commit()
        assert await _spec_json_snapshot(db, spec_id) == before
        assert (
            await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == effects
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["reporter", "archived", "content_lock", "approved"])
async def test_ir_preparation_keeps_existing_authority_and_content_locks(
    db_factory, case
):
    board_id, spec_id = str(uuid.uuid4()), str(uuid.uuid4())
    async with db_factory() as db:
        await _seed_spec(db, board_id=board_id, spec_id=spec_id, actor_id="author")
        row = await db.get(Spec, spec_id)
        if case == "archived":
            row.archived = True
        if case == "approved":
            row.status = SpecStatus.APPROVED
        if case == "content_lock":
            row.current_validation_id = "successful-validation"
            row.validations = [
                {"id": row.current_validation_id, "outcome": "success", "edition": 6}
            ]
        await db.flush()
        before = await _spec_json_snapshot(db, spec_id)
        effects = await _side_effect_counts(db, board_id=board_id, spec_id=spec_id)
        request = command(spec_id, board_id)
        if case == "reporter":
            request.permission_set = _permission_set("Reporter")
        service = StructuredSpecEntityService(db)
        if case == "approved":
            with pytest.raises(SubjectEditRequiresDraftError):
                await service.prepare_integration_requirement_creates(
                    request, payloads()
                )
        else:
            result = await service.prepare_integration_requirement_creates(
                request, payloads()
            )
            assert result.success is False
            assert (
                result.error_code
                == {
                    "reporter": Error.AUTHORIZATION_DENIED,
                    "archived": Error.VALIDATION_FAILED,
                    "content_lock": Error.SPEC_LOCKED,
                }[case]
            )
        assert await _spec_json_snapshot(db, spec_id) == before
        assert (
            await _side_effect_counts(db, board_id=board_id, spec_id=spec_id) == effects
        )
