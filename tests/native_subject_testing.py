"""Explicit authority records for native entities seeded directly by tests."""

from datetime import datetime, timezone

from okto_pulse.community.adapters.sqlalchemy_semantic_guideline_assessment import (
    CommunitySqlAlchemySemanticGuidelineAssessment,
)
from okto_pulse.core.domain.guideline_policy import PolicyEntityType
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from sqlalchemy_test_models import Card, Ideation, Refinement, Spec


async def record_native_subject_authority(session, *, rows=None, revision=None):
    """Seal an explicit seed or explicitly supplied edited fixture state."""
    source_rows = tuple(session.new) if rows is None else tuple(rows)
    subjects = [
        (row, kind)
        for row in source_rows
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
        if revision is not None:
            key += f":{revision}"
        await adapter.record_semantic_subject_mutation(
            board_id=row.board_id, entity_type=kind, subject_id=row.id,
            actor_id=row.created_by, idempotency_key=key,
            request_digest=canonical_sha256({"fixture": key}),
            changed_at=datetime.now(timezone.utc),
        )
