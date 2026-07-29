"""Policy and orchestration for the one-shot quality legacy import."""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone

from okto_pulse.core.domain.quality_assessment import (
    AssessmentKind,
    AssessmentSubjectType,
)
from okto_pulse.core.domain.quality_assessment_legacy_import import (
    QUALITY_ASSESSMENT_LEGACY_IMPORT_CODE_DIGEST,
    QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH,
    LegacyIdeationSnapshot,
    LegacyImportCandidate,
    LegacyImportCandidateKey,
    LegacyImportCandidateWork,
    LegacyImportCheckpoint,
    LegacyImportCompletionExpectation,
    LegacyImportContractError,
    LegacyImportPlan,
    LegacyImportPostcondition,
    LegacyImportRejectionCount,
    LegacyImportRunRequest,
    LegacyImportRunResult,
    LegacyImportSelection,
    LegacyImportSelectionReason,
    LegacyImportSourceSnapshot,
    LegacySpecSnapshot,
    LegacyImportWriteIdentity,
    ideation_ambiguity_scale,
    spec_validation_ambiguity_scale,
    strict_legacy_ideation_score,
    strict_legacy_spec_score,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.ports.quality_assessment_legacy_import import (
    QualityAssessmentLegacyImportPersistencePort,
    QualityAssessmentLegacySourcePort,
)


class LegacyImportExecutionError(RuntimeError):
    """The durable epoch could not safely advance."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _selection(
    snapshot: LegacyIdeationSnapshot | LegacySpecSnapshot,
    reason: LegacyImportSelectionReason,
    candidate: LegacyImportCandidate | None = None,
) -> LegacyImportSelection:
    return LegacyImportSelection(
        board_id=snapshot.subject.board_id,
        subject_type=snapshot.subject.subject_type,
        subject_id=snapshot.subject.subject_id,
        reason=reason,
        candidate=candidate,
    )


def _legacy_ideation_score_reason(
    value: object,
) -> LegacyImportSelectionReason:
    if value is None:
        return LegacyImportSelectionReason.SCORE_MISSING
    if isinstance(value, bool):
        return LegacyImportSelectionReason.SCORE_INVALID
    parseable = (
        isinstance(value, int)
        or (
            isinstance(value, float)
            and math.isfinite(value)
            and value.is_integer()
        )
        or (
            isinstance(value, str)
            and value.strip().lstrip("-").isdigit()
        )
    )
    if not parseable:
        return LegacyImportSelectionReason.SCORE_INVALID
    return LegacyImportSelectionReason.SCORE_OUT_OF_RANGE


def _legacy_spec_score_reason(value: object) -> LegacyImportSelectionReason:
    if value is None:
        return LegacyImportSelectionReason.SCORE_MISSING
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(float(value))
    ):
        return LegacyImportSelectionReason.SCORE_INVALID
    return LegacyImportSelectionReason.SCORE_OUT_OF_RANGE


def _justification(
    payload: Mapping[str, object],
    *,
    fallback: str,
) -> str:
    for key in (
        "ambiguity_justification",
        "justification",
        "general_justification",
    ):
        raw = payload.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return fallback


def _record_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


class QualityAssessmentLegacyImportService:
    """Build, resume, and close the durable v1 import epoch."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def select_ideation(
        self,
        snapshot: LegacyIdeationSnapshot,
        *,
        cutoff: datetime,
    ) -> LegacyImportSelection:
        if snapshot.observed_at > cutoff:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.AFTER_CUTOFF,
            )
        if not snapshot.active_at_cutoff:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.SUBJECT_INELIGIBLE,
            )
        if not snapshot.gate_enabled_at_cutoff:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.IDEATION_GATE_DISABLED,
            )
        payload = snapshot.scope_assessment
        if not isinstance(payload, Mapping):
            return _selection(
                snapshot,
                LegacyImportSelectionReason.LEGACY_PAYLOAD_INVALID,
            )
        raw_score = payload.get("ambiguity")
        score = strict_legacy_ideation_score(raw_score)
        if score is None:
            return _selection(
                snapshot,
                _legacy_ideation_score_reason(raw_score),
            )
        candidate = LegacyImportCandidate(
            key=LegacyImportCandidateKey(
                board_id=snapshot.subject.board_id,
                subject_type=AssessmentSubjectType.IDEATION,
                subject_id=snapshot.subject.subject_id,
                assessment_kind=AssessmentKind.AMBIGUITY,
            ),
            subject_version=snapshot.subject.subject_version,
            scale=ideation_ambiguity_scale(),
            score=score,
            justification=_justification(
                payload,
                fallback=(
                    "Imported from the legacy Ideation scope assessment."
                ),
            ),
            digests=snapshot.digests,
            versions=snapshot.versions,
            legacy_source_id="scope_assessment",
            legacy_source_digest=canonical_sha256(payload),
            observed_at=snapshot.observed_at,
        )
        return _selection(
            snapshot,
            LegacyImportSelectionReason.ELIGIBLE,
            candidate,
        )

    def select_spec(
        self,
        snapshot: LegacySpecSnapshot,
        *,
        cutoff: datetime,
    ) -> LegacyImportSelection:
        if snapshot.observed_at > cutoff:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.AFTER_CUTOFF,
            )
        if not snapshot.active_at_cutoff:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.SUBJECT_INELIGIBLE,
            )
        current_id = snapshot.current_validation_id
        if current_id is None:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.CURRENT_VALIDATION_MISSING,
            )
        validations = snapshot.validations
        if not isinstance(validations, Sequence) or isinstance(
            validations,
            str | bytes | bytearray,
        ):
            return _selection(
                snapshot,
                LegacyImportSelectionReason.LEGACY_PAYLOAD_INVALID,
            )
        matches = [
            item
            for item in validations
            if isinstance(item, Mapping) and item.get("id") == current_id
        ]
        if not matches:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.CURRENT_VALIDATION_NOT_FOUND,
            )
        if len(matches) != 1:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.CURRENT_VALIDATION_DUPLICATE,
            )
        record = matches[0]
        if (
            record.get("board_id") != snapshot.subject.board_id
            or record.get("spec_id") != snapshot.subject.subject_id
        ):
            return _selection(
                snapshot,
                LegacyImportSelectionReason.VALIDATION_SCOPE_MISMATCH,
            )
        if record.get("outcome") != "success":
            return _selection(
                snapshot,
                LegacyImportSelectionReason.VALIDATION_NOT_SUCCESSFUL,
            )
        record_created_at = _record_timestamp(record.get("created_at"))
        if record_created_at is None:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.LEGACY_PAYLOAD_INVALID,
            )
        if record_created_at > cutoff:
            return _selection(
                snapshot,
                LegacyImportSelectionReason.AFTER_CUTOFF,
            )
        raw_score = record.get("ambiguity")
        score = strict_legacy_spec_score(raw_score)
        if score is None:
            return _selection(
                snapshot,
                _legacy_spec_score_reason(raw_score),
            )
        candidate = LegacyImportCandidate(
            key=LegacyImportCandidateKey(
                board_id=snapshot.subject.board_id,
                subject_type=AssessmentSubjectType.SPEC,
                subject_id=snapshot.subject.subject_id,
                assessment_kind=AssessmentKind.SPEC_VALIDATION,
            ),
            subject_version=snapshot.subject.subject_version,
            scale=spec_validation_ambiguity_scale(),
            score=score,
            justification=_justification(
                record,
                fallback=(
                    "Imported from the current successful legacy "
                    "SpecValidation."
                ),
            ),
            digests=snapshot.digests,
            versions=snapshot.versions,
            legacy_source_id=current_id,
            legacy_source_digest=canonical_sha256(record),
            observed_at=record_created_at,
        )
        return _selection(
            snapshot,
            LegacyImportSelectionReason.ELIGIBLE,
            candidate,
        )

    def build_plan(
        self,
        source: LegacyImportSourceSnapshot,
    ) -> tuple[LegacyImportPlan, tuple[LegacyImportSelection, ...]]:
        if not isinstance(source, LegacyImportSourceSnapshot):
            raise LegacyImportContractError(
                "legacy_import_source_snapshot_invalid"
            )
        selections = tuple(
            [
                self.select_ideation(item, cutoff=source.cutoff)
                for item in source.ideations
            ]
            + [
                self.select_spec(item, cutoff=source.cutoff)
                for item in source.specs
            ]
        )
        candidates = tuple(
            sorted(
                (
                    item.candidate
                    for item in selections
                    if item.candidate is not None
                ),
                key=lambda item: item.key.physical_identity,
            )
        )
        rejected = Counter(
            item.reason
            for item in selections
            if item.reason is not LegacyImportSelectionReason.ELIGIBLE
        )
        rejection_counts = tuple(
            LegacyImportRejectionCount(reason=reason, count=count)
            for reason, count in sorted(
                rejected.items(),
                key=lambda item: item[0].value,
            )
        )
        plan = LegacyImportPlan(
            board_id=source.board_id,
            cutoff=source.cutoff,
            code_digest=QUALITY_ASSESSMENT_LEGACY_IMPORT_CODE_DIGEST,
            candidates=candidates,
            rejection_counts=rejection_counts,
        )
        return plan, selections

    def validate_checkpoint(
        self,
        *,
        plan: LegacyImportPlan,
        checkpoint: LegacyImportCheckpoint,
    ) -> None:
        if (
            checkpoint.board_id != plan.board_id
            or checkpoint.epoch != plan.epoch
            or checkpoint.plan_digest != plan.plan_digest
            or checkpoint.candidate_digest != plan.candidate_digest
            or checkpoint.processed_count > plan.candidate_count
        ):
            raise LegacyImportExecutionError(
                "legacy_import_checkpoint_plan_mismatch"
            )
        if checkpoint.processed_count:
            expected_last = plan.candidates[
                checkpoint.processed_count - 1
            ].key
            if checkpoint.last_candidate_key != expected_last:
                raise LegacyImportExecutionError(
                    "legacy_import_checkpoint_cursor_mismatch"
                )

    def validate_authoritative_plan(self, plan: LegacyImportPlan) -> None:
        if (
            plan.epoch != QUALITY_ASSESSMENT_LEGACY_IMPORT_EPOCH
            or plan.code_digest
            != QUALITY_ASSESSMENT_LEGACY_IMPORT_CODE_DIGEST
        ):
            raise LegacyImportExecutionError(
                "legacy_import_authoritative_plan_unsupported"
            )

    def validate_postcondition(
        self,
        *,
        expectation: LegacyImportCompletionExpectation,
        postcondition: LegacyImportPostcondition,
    ) -> None:
        if postcondition.expectation != expectation:
            raise LegacyImportExecutionError(
                "legacy_import_postcondition_expectation_mismatch"
            )
        if not postcondition.satisfied:
            raise LegacyImportExecutionError(
                "legacy_import_postcondition_failed"
            )

    def prepare_write_identity(
        self,
        *,
        plan: LegacyImportPlan,
        candidate: LegacyImportCandidate,
        occurred_at: datetime,
    ) -> LegacyImportWriteIdentity:
        """Derive replay-stable receipt/audit IDs before entering the adapter."""

        identity_seed = canonical_sha256(
            {
                "epoch": plan.epoch,
                "plan_digest": plan.plan_digest,
                "candidate_key": candidate.key.physical_identity,
            }
        )
        request_digest = canonical_sha256(
            {
                "epoch": plan.epoch,
                "candidate": candidate.canonical_payload(),
                "origin": "legacy_import",
                "source": "legacy_migration",
                "channel": "migration",
            }
        )
        run_identity_digest = canonical_sha256(
            {
                "board_id": candidate.key.board_id,
                "subject_type": candidate.key.subject_type.value,
                "subject_id": candidate.key.subject_id,
                "subject_version": candidate.subject_version,
                "assessment_kind": candidate.key.assessment_kind.value,
                "ruleset_version": candidate.versions.ruleset_version,
                "analyzer_version": candidate.versions.analyzer_version,
                "input_digest": candidate.digests.input_digest,
                "epoch": plan.epoch,
            }
        )
        authority_digest = canonical_sha256(
            {
                "authority": "system_legacy_import",
                "epoch": plan.epoch,
                "code_digest": plan.code_digest,
            }
        )
        return LegacyImportWriteIdentity(
            receipt_id=f"qar_lgi_{identity_seed[:24]}",
            event_id=f"qae_lgi_{identity_seed[:24]}",
            history_id=f"qah_lgi_{identity_seed[:24]}",
            outbox_id=f"qao_lgi_{identity_seed[:24]}",
            idempotency_key=f"{plan.epoch}:{identity_seed}",
            request_digest=request_digest,
            run_identity_digest=run_identity_digest,
            authority_digest=authority_digest,
            actor_id="system:quality-assessment-legacy-import",
            occurred_at=occurred_at,
        )

    async def run(
        self,
        request: LegacyImportRunRequest,
        *,
        source_port: QualityAssessmentLegacySourcePort,
        persistence: QualityAssessmentLegacyImportPersistencePort,
    ) -> LegacyImportRunResult:
        if not isinstance(request, LegacyImportRunRequest):
            raise LegacyImportContractError("legacy_run_request_invalid")

        # A dry-run always executes the same current selector and digest path,
        # while avoiding every durable-ledger method.
        if request.dry_run:
            cutoff = request.cutoff or self._clock()
            source = await source_port.capture_legacy_snapshot(
                board_id=request.board_id,
                cutoff=cutoff,
            )
            if (
                source.board_id != request.board_id
                or source.cutoff != cutoff
            ):
                raise LegacyImportExecutionError(
                    "legacy_import_source_scope_or_cutoff_mismatch"
                )
            plan, selections = self.build_plan(source)
            return LegacyImportRunResult(
                plan=plan,
                selections=selections,
                dry_run=True,
            )

        authoritative = await persistence.load_plan(
            board_id=request.board_id,
            epoch=request.epoch,
        )
        selections: tuple[LegacyImportSelection, ...]
        if authoritative is None:
            cutoff = request.cutoff or self._clock()

            def build_locked_plan(
                source: LegacyImportSourceSnapshot,
            ) -> LegacyImportPlan:
                if (
                    source.board_id != request.board_id
                    or source.cutoff != cutoff
                ):
                    raise LegacyImportExecutionError(
                        "legacy_import_source_scope_or_cutoff_mismatch"
                    )
                return self.build_plan(source)[0]

            authoritative = await persistence.acquire_or_build_plan(
                board_id=request.board_id,
                cutoff=cutoff,
                planner=build_locked_plan,
            )
        if authoritative.board_id != request.board_id:
            raise LegacyImportExecutionError(
                "legacy_import_authoritative_plan_board_mismatch"
            )
        selections = tuple(
            LegacyImportSelection(
                board_id=item.key.board_id,
                subject_type=item.key.subject_type,
                subject_id=item.key.subject_id,
                reason=LegacyImportSelectionReason.ELIGIBLE,
                candidate=item,
            )
            for item in authoritative.candidates
        )
        self.validate_authoritative_plan(authoritative)

        checkpoint = await persistence.load_checkpoint(
            board_id=authoritative.board_id,
            epoch=authoritative.epoch,
            plan_digest=authoritative.plan_digest,
        )
        if checkpoint is None:
            checkpoint = LegacyImportCheckpoint.initial(
                plan=authoritative,
                updated_at=self._clock(),
            )
        self.validate_checkpoint(
            plan=authoritative,
            checkpoint=checkpoint,
        )

        for ordinal in range(
            checkpoint.processed_count,
            authoritative.candidate_count,
        ):
            work = LegacyImportCandidateWork(
                plan_digest=authoritative.plan_digest,
                candidate_digest=authoritative.candidate_digest or "",
                ordinal=ordinal,
                candidate=authoritative.candidates[ordinal],
                expected_checkpoint=checkpoint,
                write_identity=self.prepare_write_identity(
                    plan=authoritative,
                    candidate=authoritative.candidates[ordinal],
                    occurred_at=self._clock(),
                ),
            )
            outcome = await persistence.apply_candidate_checkpoint(work)
            if outcome.work != work:
                raise LegacyImportExecutionError(
                    "legacy_import_outcome_work_mismatch"
                )
            checkpoint = outcome.checkpoint
            self.validate_checkpoint(
                plan=authoritative,
                checkpoint=checkpoint,
            )

        expectation = LegacyImportCompletionExpectation(
            board_id=authoritative.board_id,
            epoch=authoritative.epoch,
            plan_digest=authoritative.plan_digest,
            candidate_digest=authoritative.candidate_digest or "",
            candidate_count=authoritative.candidate_count,
            checkpoint=checkpoint,
        )
        postcondition = await persistence.verify_and_complete(expectation)
        self.validate_postcondition(
            expectation=expectation,
            postcondition=postcondition,
        )
        return LegacyImportRunResult(
            plan=authoritative,
            selections=selections,
            dry_run=False,
            checkpoint=checkpoint,
            postcondition=postcondition,
        )
