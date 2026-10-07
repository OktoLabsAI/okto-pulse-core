"""Explicit authority records for native entities seeded directly by tests."""

from datetime import datetime, timezone

from okto_pulse.community.adapters.sqlalchemy_semantic_guideline_assessment import (
    CommunitySqlAlchemySemanticGuidelineAssessment,
)
from okto_pulse.core.domain.guideline_policy import PolicyEntityType
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from sqlalchemy_test_models import Card, Ideation, Refinement, Spec


async def record_native_subject_authority(session):
    subjects = [
        (row, kind)
        for row in tuple(session.new)
        for model, kind in (
            (Ideation, PolicyEntityType.IDEATION),
            (Refinement, PolicyEntityType.REFINEMENT),
            (Spec, PolicyEntityType.SPEC),
            (Card, PolicyEntityType.CARD),
        )
        if isinstance(row, model)
    ]
    await session.flush()
    adapter = CommunitySqlAlchemySemanticGuidelineAssessment(session)
    for row, kind in subjects:
        key = f"native-fixture:{kind.value}:{row.id}"
        await adapter.record_semantic_subject_mutation(
            board_id=row.board_id, entity_type=kind, subject_id=row.id,
            actor_id=row.created_by, idempotency_key=key,
            request_digest=canonical_sha256({"fixture": key}),
            changed_at=datetime.now(timezone.utc),
        )
