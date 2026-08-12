"""Receipt-backed ambiguity-gate integration coverage.

The transition service must use the immutable quality-assessment receipt, not
the legacy ``scope_assessment["ambiguity"]`` projection.  These tests exercise
the real Ideation/Refinement mutation paths while injecting only the
edition-owned persistence port.
"""

from __future__ import annotations

from datetime import datetime, timezone
import uuid

import pytest

from sqlalchemy_test_models import (
    Board,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
)
from okto_pulse.core.domain.quality_assessment import (
    AssessmentKind,
    AssessmentOrigin,
    AssessmentOutcome,
    AssessmentReceipt,
    AssessmentSource,
    AssessmentSubjectHead,
    AssessmentSubjectRef,
    AssessmentSubjectType,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.models.schemas import IdeationMove, RefinementMove
from okto_pulse.core.ports.quality_assessment import (
    QualityAssessmentAdapterMissing,
)
from okto_pulse.core.services.ambiguity_assessment import (
    AMBIGUITY_SCALE,
    AmbiguityGateConfiguration,
    AmbiguityGateError,
    AmbiguityGateReason,
    AmbiguityGateService,
    ambiguity_digest_set,
    ambiguity_versions_v1,
)
from okto_pulse.core.services.main import IdeationService, RefinementService
from okto_pulse.core.services.resource_gate import (
    ResourceGateService,
    ResourceGateViolation,
)

ACTOR = "ambiguity-gate-actor"
NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed(
    db,
    *,
    gate_enabled: bool,
    threshold: int = 3,
    legacy_ambiguity=None,
    skip: bool = False,
    status: IdeationStatus = IdeationStatus.EVALUATING,
    settings_override=None,
) -> tuple[str, str]:
    board_id = _id("board")
    ideation_id = _id("idea")
    if settings_override is not None:
        settings = settings_override
    else:
        settings = {
            "require_ideation_ambiguity_gate": gate_enabled,
            "max_ideation_ambiguity": threshold,
        }
    scope = (
        None
        if legacy_ambiguity is None
        else {"ambiguity": legacy_ambiguity}
    )
    db.add(Board(id=board_id, name="Gate", owner_id=ACTOR, settings=settings))
    db.add(
        Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Gate ideation",
            created_by=ACTOR,
            status=status,
            scope_assessment=scope,
            skip_ambiguity_gate=skip,
            skip_ambiguity_gate_edition=1 if skip else None,
        )
    )
    await db.flush()
    return board_id, ideation_id


class _ReceiptPersistence:
    """Minimal edition-port fake; the Core test adapter intentionally lacks it."""

    def __init__(self, receipt: AssessmentReceipt | None) -> None:
        self.receipt = receipt
        self.calls = 0

    async def get_current(self, **_: object):
        self.calls += 1
        if self.receipt is None:
            return None
        receipt = self.receipt
        return (
            receipt,
            AssessmentSubjectHead(
                board_id=receipt.subject.board_id,
                subject_type=receipt.subject.subject_type,
                subject_id=receipt.subject.subject_id,
                assessment_kind=receipt.assessment_kind,
                receipt_id=receipt.id,
                revision=1,
                updated_at=NOW,
            ),
        )


def _receipt_for(
    subject,
    *,
    subject_type: AssessmentSubjectType,
    threshold: int,
    score: int,
) -> AssessmentReceipt:
    configuration = AmbiguityGateConfiguration(
        required=True,
        maximum_score=threshold,
    )
    digests = ambiguity_digest_set(
        subject_type=subject_type,
        subject=subject,
        qa_items=[],
        configuration=configuration,
    )
    subject_ref = AssessmentSubjectRef(
        board_id=subject.board_id,
        subject_type=subject_type,
        subject_id=subject.id,
        subject_version=subject.version,
        subject_edition=subject.edition,
    )
    return AssessmentReceipt(
        id=_id("receipt"),
        subject=subject_ref,
        assessment_kind=AssessmentKind.AMBIGUITY,
        origin=AssessmentOrigin.HUMAN_OR_AGENT,
        source=AssessmentSource.NATIVE,
        channel="rest",
        outcome=AssessmentOutcome.RECORDED,
        scale=AMBIGUITY_SCALE,
        score=score,
        justification="Pinpointed ambiguity findings support this score.",
        digests=digests,
        versions=ambiguity_versions_v1(),
        run_identity_digest=canonical_sha256(
            [subject_ref.subject_version, digests.input_digest]
        ),
        authority_digest=canonical_sha256("test-authority"),
        idempotency_key=_id("ambiguity-idempotency"),
        request_digest=canonical_sha256("test-request"),
        created_by=ACTOR,
        created_at=NOW,
    )


def _install_receipt_gate(
    service: IdeationService | RefinementService,
    receipt: AssessmentReceipt | None,
) -> _ReceiptPersistence:
    persistence = _ReceiptPersistence(receipt)
    service._ambiguity_gate_service_factory = (  # type: ignore[attr-defined]
        lambda _db: AmbiguityGateService(persistence)  # type: ignore[arg-type]
    )
    return persistence


async def _satisfy_resource_gate(db, board_id: str, ideation_id: str) -> None:
    """Mark every ideation resource type N/A so ResourceGate allows done."""
    service = ResourceGateService(db)
    for resource_type in ("architecture", "mockup", "knowledge_base"):
        await service.mark_not_applicable(
            board_id,
            "ideation",
            ideation_id,
            resource_type,
            ACTOR,
            justification=f"{resource_type} not applicable in this test.",
            source_channel="ui",
        )


# ---------------------------------------------------------------------------
# Missing receipt / missing adapter (fail-closed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_receipt_blocks_done_with_actionable_error(db_factory):
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True)
        service = IdeationService(db)
        persistence = _install_receipt_gate(service, None)

        with pytest.raises(AmbiguityGateError) as exc_info:
            await service.move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )

        assert exc_info.value.code == (
            AmbiguityGateReason.ASSESSMENT_MISSING.value
        )
        assert "no current ambiguity assessment exists" in str(exc_info.value)
        assert persistence.calls == 1

@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_score", [1, "high"])
async def test_legacy_scope_assessment_never_satisfies_receipt_gate(
    db_factory,
    legacy_score,
):
    async with db_factory() as db:
        _, ideation_id = await _seed(
            db,
            gate_enabled=True,
            legacy_ambiguity=legacy_score,
        )
        service = IdeationService(db)
        _install_receipt_gate(service, None)

        with pytest.raises(AmbiguityGateError) as exc_info:
            await service.move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )

        assert exc_info.value.code == (
            AmbiguityGateReason.ASSESSMENT_MISSING.value
        )


@pytest.mark.asyncio
async def test_missing_edition_adapter_fails_closed_before_resource_gate(
    db_factory,
):
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True)

        with pytest.raises(
            QualityAssessmentAdapterMissing,
            match="no quality-assessment persistence",
        ):
            await IdeationService(db).move_ideation(
                ideation_id,
                ACTOR,
                IdeationMove(status=IdeationStatus.DONE),
            )

        ideation = await db.get(Ideation, ideation_id)
        assert ideation is not None
        assert ideation.status is IdeationStatus.EVALUATING


# ---------------------------------------------------------------------------
# Above / equal threshold from the current receipt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_above_threshold_blocks_with_score_and_max_in_error(db_factory):
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True, threshold=3)
        ideation = await db.get(Ideation, ideation_id)
        assert ideation is not None
        service = IdeationService(db)
        _install_receipt_gate(
            service,
            _receipt_for(
                ideation,
                subject_type=AssessmentSubjectType.IDEATION,
                threshold=3,
                score=4,
            ),
        )

        with pytest.raises(
            AmbiguityGateError,
            match="score 4 exceeds configured max 3",
        ) as exc_info:
            await service.move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )
        assert exc_info.value.code == (
            AmbiguityGateReason.SCORE_EXCEEDS_THRESHOLD.value
        )


@pytest.mark.asyncio
async def test_equal_threshold_passes_gate_and_reaches_done(db_factory):
    async with db_factory() as db:
        board_id, ideation_id = await _seed(
            db,
            gate_enabled=True,
            threshold=3,
        )
        ideation = await db.get(Ideation, ideation_id)
        assert ideation is not None
        service = IdeationService(db)
        _install_receipt_gate(
            service,
            _receipt_for(
                ideation,
                subject_type=AssessmentSubjectType.IDEATION,
                threshold=3,
                score=3,
            ),
        )
        await _satisfy_resource_gate(db, board_id, ideation_id)
        moved = await service.move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
        )
        assert moved.status == IdeationStatus.DONE


# ---------------------------------------------------------------------------
# Precedence: ambiguity gate runs BEFORE ResourceGate (BR4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ambiguity_error_precedes_resource_gate(db_factory):
    # Bare ideation (ResourceGate WOULD block) + gate enabled + missing receipt.
    # The ambiguity error must surface, proving it runs first.
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True)
        service = IdeationService(db)
        _install_receipt_gate(service, None)
        with pytest.raises(AmbiguityGateError) as exc_info:
            await service.move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )
        assert exc_info.value.code == (
            AmbiguityGateReason.ASSESSMENT_MISSING.value
        )


# ---------------------------------------------------------------------------
# Skip bypasses ONLY the Max ambiguity gate (FR10, BR3, AC9)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skip_bypasses_only_ambiguity_gate_resource_gate_still_runs(db_factory):
    # skip=True with no receipt: ambiguity gate is bypassed, but the
    # bare ideation still trips ResourceGate (not AmbiguityGateError).
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True, skip=True)
        with pytest.raises(ResourceGateViolation):
            await IdeationService(db).move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )


@pytest.mark.asyncio
async def test_skip_lets_done_complete_when_resources_satisfied(db_factory):
    async with db_factory() as db:
        board_id, ideation_id = await _seed(
            db,
            gate_enabled=True,
            threshold=3,
            skip=True,
        )
        await _satisfy_resource_gate(db, board_id, ideation_id)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
        )
        assert moved.status == IdeationStatus.DONE


# ---------------------------------------------------------------------------
# Gate only on evaluating -> done (FR6)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gate_does_not_fire_on_non_done_transitions(db_factory):
    # evaluating -> approved with gate enabled + no receipt must succeed.
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.APPROVED)
        )
        assert moved.status == IdeationStatus.APPROVED


# ---------------------------------------------------------------------------
# Disabled gate + legacy defaults are no-ops (FR7, FR13, BR1, BR6, AC5)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disabled_gate_does_not_block_done(db_factory):
    async with db_factory() as db:
        board_id, ideation_id = await _seed(db, gate_enabled=False)
        await _satisfy_resource_gate(db, board_id, ideation_id)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
        )
        assert moved.status == IdeationStatus.DONE


def test_ambiguity_gate_error_is_valueerror_for_mcp_surfacing():
    # okto_pulse_move_ideation surfaces gate failures through its existing
    # `except ValueError -> {"error": str(e)}`; the subclass relationship is
    # the contract that keeps the MCP detail equivalent to REST (TR9).
    assert issubclass(AmbiguityGateError, ValueError)


@pytest.mark.asyncio
async def test_legacy_board_without_settings_defaults_gate_off(db_factory):
    # No gate settings at all defaults to disabled; assessment storage is not
    # required and only ResourceGate governs.
    async with db_factory() as db:
        board_id, ideation_id = await _seed(
            db,
            gate_enabled=False,
            legacy_ambiguity=5,
            settings_override={},
        )
        await _satisfy_resource_gate(db, board_id, ideation_id)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
        )
        assert moved.status == IdeationStatus.DONE


# ---------------------------------------------------------------------------
# Refinement completion order: Ambiguity -> Resource -> Cognitive
# ---------------------------------------------------------------------------


async def _seed_refinement(db) -> tuple[str, str]:
    board_id = _id("refinement-board")
    ideation_id = _id("parent-ideation")
    refinement_id = _id("refinement")
    db.add(
        Board(
            id=board_id,
            name="Refinement gate order",
            owner_id=ACTOR,
            settings={
                "require_refinement_ambiguity_gate": True,
                "max_refinement_ambiguity": 3,
            },
        )
    )
    db.add(
        Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Parent ideation",
            created_by=ACTOR,
            status=IdeationStatus.DONE,
        )
    )
    db.add(
        Refinement(
            id=refinement_id,
            board_id=board_id,
            ideation_id=ideation_id,
            title="Receipt-backed refinement",
            description="Bounded refinement description.",
            analysis="The analyzed behavior is explicit.",
            in_scope=["ambiguity gate"],
            status=RefinementStatus.APPROVED,
            version=1,
            created_by=ACTOR,
        )
    )
    await db.flush()
    return board_id, refinement_id


@pytest.mark.asyncio
async def test_refinement_done_runs_ambiguity_resource_cognitive_in_order(
    db_factory,
    monkeypatch,
):
    calls: list[str] = []
    async with db_factory() as db:
        _, refinement_id = await _seed_refinement(db)
        refinement = await db.get(Refinement, refinement_id)
        assert refinement is not None
        receipt = _receipt_for(
            refinement,
            subject_type=AssessmentSubjectType.REFINEMENT,
            threshold=3,
            score=2,
        )

        class _OrderedPersistence(_ReceiptPersistence):
            async def get_current(self, **kwargs):
                calls.append("ambiguity")
                return await super().get_current(**kwargs)

        persistence = _OrderedPersistence(receipt)
        service = RefinementService(db)
        service._ambiguity_gate_service_factory = (  # type: ignore[attr-defined]
            lambda _db: AmbiguityGateService(persistence)  # type: ignore[arg-type]
        )

        async def allow_resource(*_args, **_kwargs):
            calls.append("resource")

        async def allow_cognitive(*_args, **_kwargs):
            calls.append("cognitive")

        monkeypatch.setattr(
            ResourceGateService,
            "validate_or_raise_entity_completion",
            allow_resource,
        )
        service._validate_cognitive_done = allow_cognitive  # type: ignore[method-assign]

        moved = await service.move_refinement(
            refinement_id,
            ACTOR,
            RefinementMove(status=RefinementStatus.DONE),
        )

        assert moved is not None
        assert moved.status is RefinementStatus.DONE
    assert calls == ["ambiguity", "resource", "cognitive", "ambiguity"]


@pytest.mark.asyncio
async def test_refinement_done_rechecks_ambiguity_after_lifecycle_fence(
    db_factory,
    monkeypatch,
):
    """A concurrent PASS -> FAIL head replacement cannot be promoted."""

    calls: list[str] = []
    async with db_factory() as db:
        _, refinement_id = await _seed_refinement(db)
        refinement = await db.get(Refinement, refinement_id)
        assert refinement is not None
        receipts = [
            _receipt_for(
                refinement,
                subject_type=AssessmentSubjectType.REFINEMENT,
                threshold=3,
                score=2,
            ),
            _receipt_for(
                refinement,
                subject_type=AssessmentSubjectType.REFINEMENT,
                threshold=3,
                score=5,
            ),
        ]

        class _ReplacingPersistence(_ReceiptPersistence):
            async def get_current(self, **_kwargs):
                calls.append("ambiguity")
                receipt = receipts[min(len(calls) - 1, 1)]
                return (
                    receipt,
                    AssessmentSubjectHead(
                        board_id=receipt.subject.board_id,
                        subject_type=receipt.subject.subject_type,
                        subject_id=receipt.subject.subject_id,
                        assessment_kind=receipt.assessment_kind,
                        receipt_id=receipt.id,
                        revision=len(calls),
                        updated_at=NOW,
                    ),
                )

        service = RefinementService(db)
        service._ambiguity_gate_service_factory = (  # type: ignore[attr-defined]
            lambda _db: AmbiguityGateService(_ReplacingPersistence(receipts[0]))  # type: ignore[arg-type]
        )

        async def allow_resource(*_args, **_kwargs):
            calls.append("resource")

        async def allow_cognitive(*_args, **_kwargs):
            calls.append("cognitive")

        monkeypatch.setattr(
            ResourceGateService,
            "validate_or_raise_entity_completion",
            allow_resource,
        )
        service._validate_cognitive_done = allow_cognitive  # type: ignore[method-assign]

        with pytest.raises(AmbiguityGateError) as raised:
            await service.move_refinement(
                refinement_id,
                ACTOR,
                RefinementMove(status=RefinementStatus.DONE),
            )

        assert (
            raised.value.code
            == AmbiguityGateReason.SCORE_EXCEEDS_THRESHOLD.value
        )
        assert calls == ["ambiguity", "resource", "cognitive", "ambiguity"]
        unchanged = await db.get(Refinement, refinement_id)
        assert unchanged is not None
        assert unchanged.status is RefinementStatus.APPROVED


@pytest.mark.asyncio
async def test_refinement_missing_receipt_stops_before_later_gates_and_mutation(
    db_factory,
    monkeypatch,
):
    calls: list[str] = []
    async with db_factory() as db:
        _, refinement_id = await _seed_refinement(db)
        service = RefinementService(db)

        class _MissingPersistence(_ReceiptPersistence):
            async def get_current(self, **kwargs):
                calls.append("ambiguity")
                return await super().get_current(**kwargs)

        persistence = _MissingPersistence(None)
        service._ambiguity_gate_service_factory = (  # type: ignore[attr-defined]
            lambda _db: AmbiguityGateService(persistence)  # type: ignore[arg-type]
        )

        async def unexpected_resource(*_args, **_kwargs):
            calls.append("resource")

        async def unexpected_cognitive(*_args, **_kwargs):
            calls.append("cognitive")

        monkeypatch.setattr(
            ResourceGateService,
            "validate_or_raise_entity_completion",
            unexpected_resource,
        )
        service._validate_cognitive_done = unexpected_cognitive  # type: ignore[method-assign]

        with pytest.raises(AmbiguityGateError) as exc_info:
            await service.move_refinement(
                refinement_id,
                ACTOR,
                RefinementMove(status=RefinementStatus.DONE),
            )

        assert exc_info.value.code == (
            AmbiguityGateReason.ASSESSMENT_MISSING.value
        )
        assert calls == ["ambiguity"]
        unchanged = await db.get(Refinement, refinement_id)
        assert unchanged is not None
        assert unchanged.status is RefinementStatus.APPROVED
        assert unchanged.version == 1
