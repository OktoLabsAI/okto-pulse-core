from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.quality_assessment import (
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentVersionSet,
)
from okto_pulse.core.domain.quality_assessment_legacy_import import (
    QUALITY_ASSESSMENT_LEGACY_IMPORT_CODE_DIGEST,
    QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH,
    QUALITY_ASSESSMENT_LEGACY_SELECTOR_MANIFEST_V1,
    LegacyIdeationSnapshot,
    LegacyImportCandidateKey,
    LegacyImportCandidateOutcome,
    LegacyImportCheckpoint,
    LegacyImportContractError,
    LegacyImportPlan,
    LegacyImportPostcondition,
    LegacyImportResolution,
    LegacyImportRunRequest,
    LegacyImportSelectionReason,
    LegacyImportSourceSnapshot,
    LegacySpecSnapshot,
    strict_legacy_ideation_score,
    strict_legacy_spec_score,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.services.quality_assessment_legacy_import import (
    LegacyImportExecutionError,
    QualityAssessmentLegacyImportService,
)

NOW = datetime(2026, 7, 27, 18, 0, tzinfo=timezone.utc)


def _digests(seed: str) -> AssessmentDigestSet:
    return AssessmentDigestSet(
        content_digest=canonical_sha256(f"{seed}:content"),
        clarification_digest=canonical_sha256(f"{seed}:clarification"),
        ruleset_digest=canonical_sha256("rules"),
        taxonomy_digest=canonical_sha256("taxonomy"),
        policy_digest=canonical_sha256("policy"),
    )


def _versions() -> AssessmentVersionSet:
    return AssessmentVersionSet(
        ruleset_version="rules/v1",
        taxonomy_version="taxonomy/v1",
        analyzer_version="legacy-import/v1",
        policy_version="policy/v1",
    )


def _ideation(
    subject_id: str = "i1",
    *,
    ambiguity: object = 3,
    gate: bool = True,
    active: bool = True,
    observed_at: datetime = NOW - timedelta(hours=2),
) -> LegacyIdeationSnapshot:
    return LegacyIdeationSnapshot.capture(
        subject=AssessmentSubjectRef(
            board_id="b1",
            subject_type=AssessmentSubjectType.IDEATION,
            subject_id=subject_id,
            subject_version=2,
        ),
        observed_at=observed_at,
        active_at_cutoff=active,
        gate_enabled_at_cutoff=gate,
        scope_assessment={
            "ambiguity": ambiguity,
            "ambiguity_justification": "Legacy Ideation assessment.",
        },
        digests=_digests(subject_id),
        versions=_versions(),
    )


def _validation(
    *,
    validation_id: str = "v1",
    board_id: str = "b1",
    spec_id: str = "s1",
    outcome: str = "success",
    ambiguity: object = 24,
    created_at: datetime = NOW - timedelta(hours=1),
) -> dict[str, object]:
    return {
        "id": validation_id,
        "board_id": board_id,
        "spec_id": spec_id,
        "outcome": outcome,
        "ambiguity": ambiguity,
        "ambiguity_justification": "Legacy successful validation.",
        "created_at": created_at.isoformat(),
    }


def _spec(
    subject_id: str = "s1",
    *,
    current_validation_id: str | None = "v1",
    validations: object | None = None,
    active: bool = True,
    observed_at: datetime = NOW - timedelta(minutes=30),
) -> LegacySpecSnapshot:
    return LegacySpecSnapshot.capture(
        subject=AssessmentSubjectRef(
            board_id="b1",
            subject_type=AssessmentSubjectType.SPEC,
            subject_id=subject_id,
            subject_version=4,
        ),
        observed_at=observed_at,
        active_at_cutoff=active,
        current_validation_id=current_validation_id,
        validations=(
            [_validation(spec_id=subject_id)]
            if validations is None
            else validations
        ),
        digests=_digests(subject_id),
        versions=_versions(),
    )


def _snapshot(
    *,
    board_id: str = "b1",
    ideations: tuple[LegacyIdeationSnapshot, ...] = (),
    specs: tuple[LegacySpecSnapshot, ...] = (),
    cutoff: datetime = NOW,
) -> LegacyImportSourceSnapshot:
    return LegacyImportSourceSnapshot(
        board_id=board_id,
        cutoff=cutoff,
        ideations=ideations,
        specs=specs,
    )


class _Source:
    def __init__(self, snapshot: LegacyImportSourceSnapshot) -> None:
        self.snapshot = snapshot
        self.calls = 0

    async def capture_legacy_snapshot(self, *, board_id, cutoff):
        self.calls += 1
        assert board_id == self.snapshot.board_id
        assert cutoff == self.snapshot.cutoff
        return self.snapshot


class _Persistence:
    def __init__(
        self,
        source_snapshot: LegacyImportSourceSnapshot | None = None,
    ) -> None:
        self.source_snapshot = source_snapshot
        self.plan = None
        self.checkpoint = None
        self.load_plan_calls = 0
        self.plan_build_calls = 0
        self.begin_immediate_count = 0
        self.apply_calls = []
        self.complete_calls = []
        self.native_keys = set()
        self.crash_after_ordinal = None
        self._crashed = False
        self.postcondition_flags = {
            "all_candidates_resolved": True,
            "unique_identity_satisfied": True,
            "checkpoint_consistent": True,
            "zero_orphans": True,
            "audit_bundles_consistent": True,
            "epoch_closed": True,
        }

    async def load_plan(self, *, board_id, epoch):
        self.load_plan_calls += 1
        assert board_id == "b1"
        assert epoch == QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH
        return self.plan

    async def acquire_or_build_plan(self, *, board_id, cutoff, planner):
        self.plan_build_calls += 1
        self.begin_immediate_count += 1
        assert board_id == "b1"
        if self.plan is None:
            assert self.source_snapshot is not None
            assert cutoff == self.source_snapshot.cutoff
            first = planner(self.source_snapshot)
            # The callback is pure and deterministic inside the locked source
            # snapshot; invoking it again cannot drift either digest.
            second = planner(self.source_snapshot)
            assert first.plan_digest == second.plan_digest
            assert first.candidate_digest == second.candidate_digest
            self.plan = first
        return self.plan

    async def load_checkpoint(self, *, board_id, epoch, plan_digest):
        assert board_id == "b1"
        assert epoch == QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH
        if self.plan is not None:
            assert plan_digest == self.plan.plan_digest
        return self.checkpoint

    async def apply_candidate_checkpoint(self, work):
        self.apply_calls.append(work)
        native = work.candidate.key.physical_identity in self.native_keys
        resolution = (
            LegacyImportResolution.NATIVE_WINS
            if native
            else LegacyImportResolution.IMPORTED
        )
        previous = work.expected_checkpoint
        self.checkpoint = LegacyImportCheckpoint(
            board_id=previous.board_id,
            epoch=previous.epoch,
            plan_digest=previous.plan_digest,
            candidate_digest=previous.candidate_digest,
            processed_count=previous.processed_count + 1,
            imported_count=(
                previous.imported_count
                + int(resolution is LegacyImportResolution.IMPORTED)
            ),
            native_wins_count=(
                previous.native_wins_count
                + int(resolution is LegacyImportResolution.NATIVE_WINS)
            ),
            cursor_ordinal=work.ordinal,
            last_candidate_key=work.candidate.key,
            updated_at=NOW + timedelta(seconds=work.ordinal + 1),
        )
        if (
            self.crash_after_ordinal == work.ordinal
            and not self._crashed
        ):
            self._crashed = True
            raise RuntimeError("injected_crash_after_atomic_checkpoint")
        return LegacyImportCandidateOutcome(
            work=work,
            resolution=resolution,
            receipt_id=(
                f"native:{work.candidate.key.subject_id}"
                if native
                else work.write_identity.receipt_id
            ),
            checkpoint=self.checkpoint,
            atomic_resolution_applied=True,
        )

    async def verify_and_complete(self, expectation):
        self.complete_calls.append(expectation)
        return LegacyImportPostcondition(
            expectation=expectation,
            completed_at=NOW + timedelta(minutes=1),
            **self.postcondition_flags,
        )


def test_epoch_and_selector_manifest_are_frozen() -> None:
    assert (
        QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH
        == "quality-assessment-legacy-import/v1"
    )
    assert QUALITY_ASSESSMENT_LEGACY_IMPORT_CODE_DIGEST == canonical_sha256(
        QUALITY_ASSESSMENT_LEGACY_SELECTOR_MANIFEST_V1
    )
    assert QUALITY_ASSESSMENT_LEGACY_SELECTOR_MANIFEST_V1[
        "native_assessment"
    ] == "wins"


@pytest.mark.parametrize("value", [True, False])
def test_bool_is_never_a_legacy_score(value: bool) -> None:
    assert strict_legacy_ideation_score(value) is None
    assert strict_legacy_spec_score(value) is None

    service = QualityAssessmentLegacyImportService()
    ideation = service.select_ideation(_ideation(ambiguity=value), cutoff=NOW)
    spec = service.select_spec(
        _spec(validations=[_validation(ambiguity=value)]),
        cutoff=NOW,
    )

    assert ideation.reason is LegacyImportSelectionReason.SCORE_INVALID
    assert spec.reason is LegacyImportSelectionReason.SCORE_INVALID
    assert ideation.candidate is None
    assert spec.candidate is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [(1, 1), (5.0, 5), (" 3 ", 3), (0, None), (6, None), (2.5, None)],
)
def test_ideation_selector_preserves_historical_strict_parser(
    value: object,
    expected: int | None,
) -> None:
    assert strict_legacy_ideation_score(value) == expected


def test_ideation_requires_initial_gate_active_and_cutoff_eligibility() -> None:
    service = QualityAssessmentLegacyImportService()

    gate_off = service.select_ideation(_ideation(gate=False), cutoff=NOW)
    archived = service.select_ideation(
        _ideation(active=False),
        cutoff=NOW,
    )
    future = service.select_ideation(
        _ideation(observed_at=NOW + timedelta(seconds=1)),
        cutoff=NOW,
    )

    assert gate_off.reason is LegacyImportSelectionReason.IDEATION_GATE_DISABLED
    assert archived.reason is LegacyImportSelectionReason.SUBJECT_INELIGIBLE
    assert future.reason is LegacyImportSelectionReason.AFTER_CUTOFF


@pytest.mark.parametrize(
    ("snapshot", "reason"),
    [
        (
            _spec(current_validation_id=None),
            LegacyImportSelectionReason.CURRENT_VALIDATION_MISSING,
        ),
        (
            _spec(current_validation_id="absent"),
            LegacyImportSelectionReason.CURRENT_VALIDATION_NOT_FOUND,
        ),
        (
            _spec(
                validations=[
                    _validation(),
                    _validation(),
                ]
            ),
            LegacyImportSelectionReason.CURRENT_VALIDATION_DUPLICATE,
        ),
        (
            _spec(validations=[_validation(board_id="other")]),
            LegacyImportSelectionReason.VALIDATION_SCOPE_MISMATCH,
        ),
        (
            _spec(validations=[_validation(spec_id="other")]),
            LegacyImportSelectionReason.VALIDATION_SCOPE_MISMATCH,
        ),
        (
            _spec(validations=[_validation(outcome="failed")]),
            LegacyImportSelectionReason.VALIDATION_NOT_SUCCESSFUL,
        ),
        (
            _spec(
                validations=[
                    {
                        **_validation(),
                        "created_at": "not-a-timestamp",
                    }
                ]
            ),
            LegacyImportSelectionReason.LEGACY_PAYLOAD_INVALID,
        ),
        (
            _spec(
                validations=[
                    _validation(
                        created_at=NOW + timedelta(seconds=1),
                    )
                ]
            ),
            LegacyImportSelectionReason.AFTER_CUTOFF,
        ),
    ],
)
def test_spec_selector_accepts_only_current_success_same_scope_before_cutoff(
    snapshot: LegacySpecSnapshot,
    reason: LegacyImportSelectionReason,
) -> None:
    selection = QualityAssessmentLegacyImportService().select_spec(
        snapshot,
        cutoff=NOW,
    )
    assert selection.reason is reason
    assert selection.candidate is None


def test_candidates_use_exact_physical_identity_and_exclude_refinement() -> None:
    service = QualityAssessmentLegacyImportService()
    ideation = service.select_ideation(_ideation(), cutoff=NOW).candidate
    spec = service.select_spec(_spec(), cutoff=NOW).candidate
    assert ideation is not None
    assert spec is not None

    assert ideation.key.physical_identity == (
        "b1",
        "ideation",
        "i1",
        "ambiguity",
        QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH,
    )
    assert spec.key.physical_identity == (
        "b1",
        "spec",
        "s1",
        "spec_validation",
        QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH,
    )

    with pytest.raises(
        LegacyImportContractError,
        match="legacy_import_subject_type_unsupported",
    ):
        LegacyImportCandidateKey(
            board_id="b1",
            subject_type=AssessmentSubjectType.REFINEMENT,
            subject_id="r1",
            assessment_kind=AssessmentKind.AMBIGUITY,
        )


def test_candidate_plan_is_order_independent_immutable_and_digest_bound() -> None:
    service = QualityAssessmentLegacyImportService()
    first, _ = service.build_plan(
        _snapshot(
            ideations=(_ideation("i2"), _ideation("i1")),
            specs=(_spec("s2"), _spec("s1")),
        )
    )
    second, _ = service.build_plan(
        _snapshot(
            ideations=(_ideation("i1"), _ideation("i2")),
            specs=(_spec("s1"), _spec("s2")),
        )
    )

    assert first.candidates == second.candidates
    assert first.candidate_digest == second.candidate_digest
    assert first.plan_digest == second.plan_digest
    assert first.candidate_count == 4

    with pytest.raises(FrozenInstanceError):
        first.cutoff = NOW + timedelta(days=1)  # type: ignore[misc]
    with pytest.raises(
        LegacyImportContractError,
        match="legacy_import_candidate_digest_mismatch",
    ):
        replace(first, candidate_digest="0" * 64)
    with pytest.raises(
        LegacyImportContractError,
        match="legacy_import_candidate_order_invalid",
    ):
        replace(first, candidates=tuple(reversed(first.candidates)))


def test_duplicate_five_column_candidate_identity_fails_closed() -> None:
    with pytest.raises(
        LegacyImportContractError,
        match="legacy_import_candidate_identity_duplicate",
    ):
        QualityAssessmentLegacyImportService().build_plan(
            _snapshot(
                ideations=(_ideation("same"), _ideation("same")),
            )
        )


def test_write_identity_is_core_owned_and_replay_stable() -> None:
    service = QualityAssessmentLegacyImportService()
    plan, _ = service.build_plan(
        _snapshot(ideations=(_ideation(),))
    )
    candidate = plan.candidates[0]

    first = service.prepare_write_identity(
        plan=plan,
        candidate=candidate,
        occurred_at=NOW,
    )
    retry = service.prepare_write_identity(
        plan=plan,
        candidate=candidate,
        occurred_at=NOW + timedelta(minutes=5),
    )

    assert first.receipt_id == retry.receipt_id
    assert first.event_id == retry.event_id
    assert first.history_id == retry.history_id
    assert first.outbox_id == retry.outbox_id
    assert first.idempotency_key == retry.idempotency_key
    assert first.request_digest == retry.request_digest
    assert first.run_identity_digest == retry.run_identity_digest
    assert first.authority_digest == retry.authority_digest
    assert first.occurred_at != retry.occurred_at


@pytest.mark.asyncio
async def test_dry_run_uses_real_selector_and_digest_with_zero_ledger_writes() -> None:
    snapshot = _snapshot(
        ideations=(_ideation(), _ideation("invalid", ambiguity=True)),
        specs=(_spec(),),
    )
    source = _Source(snapshot)
    persistence = _Persistence()
    service = QualityAssessmentLegacyImportService(clock=lambda: NOW)

    result = await service.run(
        LegacyImportRunRequest(board_id="b1", dry_run=True, cutoff=NOW),
        source_port=source,
        persistence=persistence,
    )

    assert result.dry_run is True
    assert result.plan.candidate_count == 2
    assert result.plan.rejected_count == 1
    assert result.checkpoint is None
    assert result.postcondition is None
    assert source.calls == 1
    assert persistence.load_plan_calls == 0
    assert persistence.plan_build_calls == 0
    assert persistence.apply_calls == []
    assert persistence.complete_calls == []


@pytest.mark.asyncio
async def test_native_wins_and_checkpoint_advance_are_atomic() -> None:
    snapshot = _snapshot(ideations=(_ideation(),), specs=(_spec(),))
    source = _Source(snapshot)
    persistence = _Persistence(snapshot)
    persistence.native_keys.add(
        (
            "b1",
            "spec",
            "s1",
            "spec_validation",
            QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH,
        )
    )
    service = QualityAssessmentLegacyImportService(clock=lambda: NOW)

    result = await service.run(
        LegacyImportRunRequest(board_id="b1", cutoff=NOW),
        source_port=source,
        persistence=persistence,
    )

    assert result.checkpoint is not None
    assert result.checkpoint.processed_count == 2
    assert result.checkpoint.imported_count == 1
    assert result.checkpoint.native_wins_count == 1
    assert all(
        work.native_wins_required for work in persistence.apply_calls
    )
    assert all(
        work.write_identity.receipt_id.startswith("qar_lgi_")
        for work in persistence.apply_calls
    )
    assert len(
        {
            work.write_identity.idempotency_key
            for work in persistence.apply_calls
        }
    ) == 2
    assert result.postcondition is not None
    assert result.postcondition.satisfied
    assert source.calls == 0
    assert persistence.begin_immediate_count == 1


@pytest.mark.asyncio
async def test_actual_plan_is_built_from_locked_snapshot_not_prior_read() -> None:
    unlocked = _snapshot(ideations=(_ideation("outside-lock"),))
    locked = _snapshot(
        ideations=(_ideation("inside-lock"),),
        specs=(_spec(),),
    )
    source = _Source(unlocked)
    persistence = _Persistence(locked)
    service = QualityAssessmentLegacyImportService(clock=lambda: NOW)

    result = await service.run(
        LegacyImportRunRequest(board_id="b1", cutoff=NOW),
        source_port=source,
        persistence=persistence,
    )
    expected, _ = service.build_plan(locked)

    assert source.calls == 0
    assert result.plan.plan_digest == expected.plan_digest
    assert result.plan.candidate_digest == expected.candidate_digest
    assert {
        item.key.subject_id for item in result.plan.candidates
    } == {"inside-lock", "s1"}
    assert persistence.begin_immediate_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_after_ordinal", [0, 1, 2])
async def test_crash_after_candidate_n_resumes_without_reimport(
    crash_after_ordinal: int,
) -> None:
    snapshot = _snapshot(
        ideations=(_ideation("i1"), _ideation("i2")),
        specs=(_spec(),),
    )
    source = _Source(snapshot)
    persistence = _Persistence(snapshot)
    persistence.crash_after_ordinal = crash_after_ordinal
    service = QualityAssessmentLegacyImportService(clock=lambda: NOW)

    with pytest.raises(
        RuntimeError,
        match="injected_crash_after_atomic_checkpoint",
    ):
        await service.run(
            LegacyImportRunRequest(board_id="b1", cutoff=NOW),
            source_port=source,
            persistence=persistence,
        )

    assert persistence.checkpoint is not None
    assert persistence.checkpoint.processed_count == crash_after_ordinal + 1
    assert len(persistence.apply_calls) == crash_after_ordinal + 1

    result = await service.run(
        LegacyImportRunRequest(board_id="b1"),
        source_port=source,
        persistence=persistence,
    )

    assert result.checkpoint is not None
    assert result.checkpoint.processed_count == 3
    assert [work.ordinal for work in persistence.apply_calls] == [0, 1, 2]
    # The immutable initial plan is reused; changed current state/cutoff cannot
    # reopen or rebuild the epoch.
    assert source.calls == 0
    assert persistence.plan_build_calls == 1
    assert persistence.begin_immediate_count == 1

    previous_apply_count = len(persistence.apply_calls)
    await service.run(
        LegacyImportRunRequest(board_id="b1"),
        source_port=source,
        persistence=persistence,
    )
    assert len(persistence.apply_calls) == previous_apply_count
    assert source.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_flag",
    [
        "all_candidates_resolved",
        "unique_identity_satisfied",
        "checkpoint_consistent",
        "zero_orphans",
        "audit_bundles_consistent",
        "epoch_closed",
    ],
)
async def test_code_digest_and_postcondition_fail_closed(
    failed_flag: str,
) -> None:
    snapshot = _snapshot(ideations=(_ideation(),))
    source = _Source(snapshot)
    service = QualityAssessmentLegacyImportService(clock=lambda: NOW)

    wrong_plan, _ = service.build_plan(snapshot)
    persistence = _Persistence()
    persistence.plan = LegacyImportPlan(
        board_id=wrong_plan.board_id,
        cutoff=wrong_plan.cutoff,
        code_digest=canonical_sha256("different-selector"),
        candidates=wrong_plan.candidates,
        rejection_counts=wrong_plan.rejection_counts,
    )
    with pytest.raises(
        LegacyImportExecutionError,
        match="legacy_import_authoritative_plan_unsupported",
    ):
        await service.run(
            LegacyImportRunRequest(board_id="b1"),
            source_port=source,
            persistence=persistence,
        )

    persistence = _Persistence(snapshot)
    persistence.postcondition_flags[failed_flag] = False
    with pytest.raises(
        LegacyImportExecutionError,
        match="legacy_import_postcondition_failed",
    ):
        await service.run(
            LegacyImportRunRequest(board_id="b1", cutoff=NOW),
            source_port=source,
            persistence=persistence,
        )
