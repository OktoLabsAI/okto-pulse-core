from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.models.schemas import SpecUpdate
from okto_pulse.core.services.main import SpecService


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        (
            "business_rules",
            [
                {
                    "id": "br_1",
                    "title": "Governed rule",
                    "rule": "The action is audited",
                    "when": "an action occurs",
                    "then": "write an audit record",
                }
            ],
        ),
        (
            "api_contracts",
            [
                {
                    "id": "api_1",
                    "contract_type": "in_process",
                    "description": "Internal governed call",
                }
            ],
        ),
        (
            "integration_requirements",
            [{"id": "ir_1", "title": "Publish the integration event"}],
        ),
        (
            "observability_requirements",
            [{"id": "or_1", "title": "Monitor delivery latency"}],
        ),
        (
            "decisions",
            [
                {
                    "id": "dec_12345678",
                    "title": "Use an outbox",
                    "rationale": "Preserve atomic delivery semantics",
                }
            ],
        ),
    ],
)
async def test_legacy_semantic_bulk_write_bumps_spec_version(
    db_factory,
    field_name,
    value,
):
    """Legacy list writers must not bypass structured-writer CAS versions."""
    board_id = f"legacy-version-board-{uuid.uuid4()}"
    spec_id = f"legacy-version-spec-{uuid.uuid4()}"
    actor_id = "legacy-version-owner"

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Version Board", owner_id=actor_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Versioned Spec",
                status=SpecStatus.DRAFT,
                created_by=actor_id,
                version=1,
                functional_requirements=[],
                technical_requirements=[],
                acceptance_criteria=[],
                business_rules=[],
                api_contracts=[],
                integration_requirements=[],
                observability_requirements=[],
                decisions=[],
                test_scenarios=[],
                screen_mockups=[],
            )
        )
        await db.commit()

        updated = await SpecService(db).update_spec(
            spec_id,
            actor_id,
            SpecUpdate(**{field_name: value}),
        )

        assert updated.version == 2
        assert getattr(updated, field_name)
