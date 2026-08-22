from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
)
from okto_pulse.core.application.use_cases.submit_spec_validation import (
    SubmitSpecValidationCommand,
    SubmitSpecValidationUseCase,
)
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.guideline_semantic_v2 import (
    AnchorSnapshot,
    SemanticAnchorAvailability,
)
from okto_pulse.core.domain.spec_validation import (
    SpecValidationMetric,
    SpecValidationPinpoint,
    SpecValidationPinpointAnchorType,
)
from okto_pulse.core.ports.semantic_subject_projection import (
    SemanticSubjectProjectionError,
    SemanticSubjectProjectionFailure,
)
from okto_pulse.core.models.schemas import SpecValidationResponse


def _anchor() -> AnchorSnapshot:
    return AnchorSnapshot(
        label="AC-1: An explicit acceptance criterion",
        excerpt="Given a valid request, when it is submitted, then it succeeds.",
        source_version="17:edition:3",
        availability_at_seal=SemanticAnchorAvailability.AVAILABLE,
    )


def _pinpoint() -> SpecValidationPinpoint:
    return SpecValidationPinpoint(
        metric=SpecValidationMetric.DECIDABILITY,
        anchor_type=SpecValidationPinpointAnchorType.STRUCTURED_CHILD,
        anchor_ref="ac_123",
        detail="Quantify the expected response time.",
    )


def test_sealed_snapshot_round_trips_and_legacy_is_explicit() -> None:
    sealed = _pinpoint().seal(_anchor())
    projected = sealed.to_historical_dict()
    snapshot = projected["anchor_snapshot"]
    assert snapshot["label"].startswith("AC-1")
    assert snapshot["text"].startswith("Given a valid request")
    assert snapshot["excerpt"] == snapshot["text"]
    assert len(snapshot["source_digest"]) == 64
    assert snapshot["source_version"] == "17:edition:3"
    assert SpecValidationPinpoint.from_dict(projected) == sealed

    legacy = _pinpoint().to_historical_dict()["anchor_snapshot"]
    assert legacy == {
        "contract_version": "spec-validation-pinpoint-snapshot/v1",
        "availability_at_seal": "legacy_unavailable",
    }

    response = SpecValidationResponse.model_validate(
        {"id": "legacy-validation", "pinpoints": [_pinpoint().to_dict()]}
    ).model_dump(exclude_none=True)
    assert response["pinpoints"][0]["anchor_snapshot"] == legacy


class _Projection:
    async def resolve_semantic_anchor(self, request: object) -> AnchorSnapshot:
        assert request.anchor.anchor_type.value in {
            "whole_artifact",
            "field",
            "structured_child",
            "qa",
        }
        return _anchor()


class _ForbiddenProjection:
    async def resolve_semantic_anchor(self, request: object) -> AnchorSnapshot:
        del request
        raise SemanticSubjectProjectionError(
            SemanticSubjectProjectionFailure.FORBIDDEN
        )


class _Specs:
    def __init__(self) -> None:
        self.spec = SimpleNamespace(
            id="spec-1",
            board_id="board-1",
            version=17,
            edition=3,
            status=SpecStatus.APPROVED,
        )
        self.submitted: dict[str, object] | None = None

    async def get_spec(self, spec_id: str) -> object:
        assert spec_id == "spec-1"
        return self.spec

    async def submit_spec_validation(self, **kwargs: object) -> dict[str, object]:
        self.submitted = kwargs["data"]
        return {"id": "validation-1", **self.submitted}


class _Uow:
    def __init__(self) -> None:
        self.specs = _Specs()
        self.services = SimpleNamespace(specs=self.specs)
        self.semantic_subject_projection = _Projection()
        self.commit_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1


def _payload(
    *,
    anchor_type: str = "structured_child",
    anchor_ref: str | None = "ac_123",
) -> dict[str, object]:
    return {
        "expected_validation_edition": 3,
        "expected_spec_version": 17,
        "expected_head_revision": 0,
        "confidence": 90,
        "confidence_justification": "The evaluator reviewed the whole Spec.",
        "clarity": 90,
        "clarity_justification": "The problem and requirements are explicit.",
        "assertiveness": 90,
        "assertiveness_justification": "Requirements use measurable language.",
        "decidability": 90,
        "decidability_justification": "The constraints direct concrete choices.",
        "ambiguity": 5,
        "ambiguity_justification": "Every term has one contextual meaning.",
        "recommendation": "approve",
        "pinpoints": [
            {
                "metric": "decidability",
                "anchor_type": anchor_type,
                "detail": "Quantify the expected response time.",
            }
            | ({"anchor_ref": anchor_ref} if anchor_ref is not None else {})
        ],
    }


@pytest.mark.asyncio
async def test_submit_use_case_seals_snapshot_before_persistence() -> None:
    uow = _Uow()
    result = await SubmitSpecValidationUseCase().execute(
        SubmitSpecValidationCommand("spec-1", _payload()),
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_name="Evaluator",
            board_id="board-1",
            permissions=["*"],
        ),
        uow=uow,
    )

    snapshot = result.payload["pinpoints"][0]["anchor_snapshot"]
    assert snapshot["availability_at_seal"] == "available"
    assert snapshot["label"].startswith("AC-1")
    assert uow.commit_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("anchor_type", "anchor_ref"),
    (
        ("whole_artifact", None),
        ("field", "description"),
        ("structured_child", "ac_123"),
        ("qa", "qa_123"),
    ),
)
async def test_all_anchor_kinds_are_sealed_from_authorized_projection(
    anchor_type: str,
    anchor_ref: str | None,
) -> None:
    uow = _Uow()
    result = await SubmitSpecValidationUseCase().execute(
        SubmitSpecValidationCommand(
            "spec-1",
            _payload(anchor_type=anchor_type, anchor_ref=anchor_ref),
        ),
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_name="Evaluator",
            board_id="board-1",
            permissions=["*"],
        ),
        uow=uow,
    )
    assert result.payload["pinpoints"][0]["anchor_snapshot"][
        "availability_at_seal"
    ] == "available"


@pytest.mark.asyncio
async def test_forbidden_anchor_content_is_not_persisted() -> None:
    uow = _Uow()
    uow.semantic_subject_projection = _ForbiddenProjection()
    with pytest.raises(
        CommandValidationError,
        match="spec_validation_pinpoint_anchor_forbidden",
    ):
        await SubmitSpecValidationUseCase().execute(
            SubmitSpecValidationCommand("spec-1", _payload()),
            actor=ActorContext(
                "agent-1",
                "mcp",
                actor_name="Evaluator",
                board_id="board-1",
                permissions=["*"],
            ),
            uow=uow,
        )
    assert uow.specs.submitted is None
    assert uow.commit_calls == 0
