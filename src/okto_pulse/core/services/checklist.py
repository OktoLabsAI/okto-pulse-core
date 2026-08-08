"""Pure orchestration for A3 curated-checklist bindings and executions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

from okto_pulse.core.domain.checklist import (
    CHECKLIST_PAGE_LIMIT_MAX,
    SPECIFY_CHECKLIST_ITEM_IDS,
    SPECIFY_CHECKLIST_ITEM_ID_SET,
    SPECIFY_CHECKLIST_TEMPLATE_DIGEST,
    SPECIFY_CHECKLIST_TEMPLATE_VERSION,
    ChecklistBinding,
    ChecklistCommitResult,
    ChecklistContractError,
    ChecklistCurrentness,
    ChecklistExecution,
    ChecklistExecutionHead,
    ChecklistExecutionStartResult,
    ChecklistExecutionStatus,
    ChecklistGateDecision,
    ChecklistItemOutcome,
    ChecklistItemResult,
    ChecklistMode,
    ChecklistPage,
    ChecklistPhase,
    ChecklistPreflight,
    ChecklistReceipt,
    ChecklistReceiptSource,
    ChecklistReceiptView,
    ChecklistSpecSnapshot,
    ChecklistSubmission,
    ChecklistTargetType,
    ChecklistWriteBundle,
    checklist_execution_request_digest_v1,
    checklist_submission_digest_v1,
    evaluate_checklist_currentness,
    evaluate_checklist_gate,
    require_specify_checklist_item,
)
from okto_pulse.core.ports.application_persistence import PAGE_OFFSET_MAX
from okto_pulse.core.ports.checklist import (
    ChecklistBindingConflict as ChecklistBindingCasConflict,
)
from okto_pulse.core.ports.checklist import (
    ChecklistContentDigestConflict,
    ChecklistExecutionRevisionConflict,
    ChecklistExecutionStateConflict,
    ChecklistHeadRevisionConflict,
    ChecklistIdempotencyConflict,
    ChecklistInputDigestConflict,
    ChecklistListQuery,
    ChecklistPersistencePort,
    ChecklistSpecLifecycleConflict,
    ChecklistSpecVersionConflict,
    ChecklistTemplateConflict,
)


class ChecklistErrorCategory(str, Enum):
    INVALID_ARGUMENT = "invalid_argument"
    UNPROCESSABLE = "unprocessable"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    INTERNAL = "internal"


class ChecklistError(RuntimeError):
    """Transport-neutral structured A3 failure."""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        category: ChecklistErrorCategory,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message or code
        self.category = category
        self.retryable = retryable
        self.details = dict(details or {})
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "message": self.message,
            "category": self.category.value,
            "retryable": self.retryable,
            "details": dict(self.details),
        }


class ChecklistValidationError(ChecklistError):
    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        category: ChecklistErrorCategory = (ChecklistErrorCategory.UNPROCESSABLE),
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            code,
            message,
            category=category,
            retryable=False,
            details=details,
        )


class ChecklistConflictError(ChecklistError):
    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        retryable: bool = True,
    ) -> None:
        super().__init__(
            code,
            message,
            category=ChecklistErrorCategory.CONFLICT,
            retryable=retryable,
        )


class ChecklistNotFoundError(ChecklistError):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(
            code,
            message,
            category=ChecklistErrorCategory.NOT_FOUND,
            retryable=False,
        )


class ChecklistPortContractError(ChecklistError):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(
            code,
            message,
            category=ChecklistErrorCategory.INTERNAL,
            retryable=False,
        )


@dataclass(frozen=True, slots=True)
class CurrentChecklistView:
    receipt: ChecklistReceipt
    head: ChecklistExecutionHead
    currentness: ChecklistCurrentness
    gate: ChecklistGateDecision


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _strict_page_window(offset: int, limit: int) -> tuple[int, int]:
    if (
        not isinstance(offset, int)
        or isinstance(offset, bool)
        or offset < 0
        or offset > PAGE_OFFSET_MAX
        or not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= CHECKLIST_PAGE_LIMIT_MAX
    ):
        raise ChecklistValidationError(
            "invalid_pagination",
            (
                f"offset must be 0..{PAGE_OFFSET_MAX} and limit must be "
                f"1..{CHECKLIST_PAGE_LIMIT_MAX}"
            ),
            category=ChecklistErrorCategory.INVALID_ARGUMENT,
        )
    return offset, limit


class ChecklistService:
    """Prepare immutable values, apply through CAS, and validate reads."""

    def __init__(
        self,
        *,
        id_factory: Callable[[str], str] = _default_id_factory,
        clock: Callable[[], datetime] = _default_clock,
    ) -> None:
        self._id_factory = id_factory
        self._clock = clock

    @staticmethod
    def prepare_binding(
        *,
        board_id: str,
        mode: ChecklistMode,
        current_binding: ChecklistBinding | None,
    ) -> ChecklistBinding:
        """Prepare the next immutable board binding without persistence."""

        try:
            if not isinstance(mode, ChecklistMode):
                raise ChecklistContractError("checklist_binding_mode_invalid")
            if current_binding is None:
                return ChecklistBinding(
                    board_id=board_id,
                    mode=mode,
                    version=1,
                )
            if not isinstance(current_binding, ChecklistBinding):
                raise ChecklistContractError("checklist_binding_invalid")
            if (
                not isinstance(board_id, str)
                or board_id.strip() != current_binding.board_id
            ):
                raise ChecklistContractError("checklist_binding_board_mismatch")
            if current_binding.is_synthetic:
                return ChecklistBinding(
                    board_id=current_binding.board_id,
                    mode=mode,
                    version=1,
                )
            if current_binding.mode is mode:
                raise ChecklistValidationError("checklist_binding_unchanged")
            return ChecklistBinding(
                board_id=current_binding.board_id,
                mode=mode,
                version=current_binding.version + 1,
            )
        except ChecklistError:
            raise
        except ChecklistContractError as exc:
            raise ChecklistValidationError(exc.code, exc.message) from exc

    async def apply_binding(
        self,
        binding: ChecklistBinding,
        *,
        previous_binding: ChecklistBinding | None,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistBinding:
        """Stage the next binding version through adapter-owned CAS."""

        if not isinstance(binding, ChecklistBinding):
            raise ChecklistValidationError("checklist_binding_invalid")
        if previous_binding is not None and not isinstance(
            previous_binding,
            ChecklistBinding,
        ):
            raise ChecklistValidationError("checklist_binding_predecessor_invalid")
        if previous_binding is None or previous_binding.is_synthetic:
            if previous_binding is not None and (
                previous_binding.board_id != binding.board_id
                or previous_binding.target_type is not binding.target_type
                or previous_binding.phase is not binding.phase
            ):
                raise ChecklistValidationError("checklist_binding_predecessor_invalid")
            if binding.version != 1:
                raise ChecklistValidationError("checklist_binding_version_invalid")
            expected_version = 0
            expected_digest = None
        else:
            if (
                previous_binding.board_id != binding.board_id
                or previous_binding.target_type is not binding.target_type
                or previous_binding.phase is not binding.phase
                or binding.version != previous_binding.version + 1
            ):
                raise ChecklistValidationError("checklist_binding_predecessor_invalid")
            expected_version = previous_binding.version
            expected_digest = previous_binding.digest
        try:
            resolved = await persistence.apply_binding_cas(
                binding,
                expected_version=expected_version,
                expected_digest=expected_digest,
            )
        except ChecklistBindingCasConflict as exc:
            raise ChecklistConflictError("checklist_binding_conflict") from exc
        if not isinstance(resolved, ChecklistBinding):
            raise ChecklistPortContractError("checklist_binding_port_invalid")
        if resolved != binding:
            raise ChecklistPortContractError("checklist_binding_port_mismatch")
        return resolved

    async def get_binding(
        self,
        *,
        board_id: str,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistBinding:
        if not isinstance(board_id, str) or not board_id.strip():
            raise ChecklistValidationError(
                "checklist_binding_board_id_required",
                category=ChecklistErrorCategory.INVALID_ARGUMENT,
            )
        binding = await persistence.get_binding(
            board_id=board_id.strip(),
            target_type=ChecklistTargetType.SPEC,
            phase=ChecklistPhase.SPEC_VALIDATION,
        )
        if binding is None:
            raise ChecklistNotFoundError("checklist_binding_not_found")
        if (
            not isinstance(binding, ChecklistBinding)
            or binding.board_id != board_id.strip()
        ):
            raise ChecklistPortContractError("checklist_binding_port_mismatch")
        return binding

    def prepare_execution_start(
        self,
        *,
        preflight: ChecklistPreflight,
        actor_id: str,
        idempotency_key: str,
    ) -> ChecklistExecution:
        """Freeze one server-issued execution before any item is submitted."""

        if not isinstance(preflight, ChecklistPreflight):
            raise ChecklistValidationError("checklist_preflight_invalid")
        actor = actor_id.strip() if isinstance(actor_id, str) else ""
        key = idempotency_key.strip() if isinstance(idempotency_key, str) else ""
        if not actor:
            raise ChecklistValidationError("checklist_actor_required")
        if not key:
            raise ChecklistValidationError("checklist_idempotency_key_required")
        if preflight.subject.archived:
            raise ChecklistConflictError(
                "checklist_spec_lifecycle_conflict",
                retryable=False,
            )
        if preflight.binding.mode is ChecklistMode.OFF:
            raise ChecklistConflictError(
                "checklist_binding_off",
                retryable=False,
            )
        now = self._clock()
        if (
            not isinstance(now, datetime)
            or now.tzinfo is None
            or now.utcoffset() is None
        ):
            raise ChecklistValidationError("checklist_clock_must_be_timezone_aware")
        now = now.astimezone(timezone.utc)
        subject = preflight.subject
        binding = preflight.binding
        request_digest = checklist_execution_request_digest_v1(
            board_id=subject.board_id,
            spec_id=subject.spec_id,
            spec_version=subject.spec_version,
            content_digest=subject.content_digest,
            input_digest=subject.input_digest,
            template_version=SPECIFY_CHECKLIST_TEMPLATE_VERSION,
            template_digest=SPECIFY_CHECKLIST_TEMPLATE_DIGEST,
            binding_digest=binding.digest or "",
            actor_id=actor,
        )
        try:
            return ChecklistExecution(
                id=self._id_factory("cle"),
                board_id=subject.board_id,
                spec_id=subject.spec_id,
                spec_version=subject.spec_version,
                content_digest=subject.content_digest,
                input_digest=subject.input_digest,
                template_version=SPECIFY_CHECKLIST_TEMPLATE_VERSION,
                template_digest=SPECIFY_CHECKLIST_TEMPLATE_DIGEST,
                binding_version=binding.version,
                binding_digest=binding.digest or "",
                binding_mode=binding.mode,
                request_digest=request_digest,
                idempotency_key=key,
                created_by=actor,
                created_at=now,
            )
        except ChecklistContractError as exc:
            raise ChecklistValidationError(exc.code, exc.message) from exc

    async def start_execution(
        self,
        *,
        preflight: ChecklistPreflight,
        actor_id: str,
        idempotency_key: str,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistExecutionStartResult:
        execution = self.prepare_execution_start(
            preflight=preflight,
            actor_id=actor_id,
            idempotency_key=idempotency_key,
        )
        try:
            result = await persistence.start_execution_cas(execution)
        except ChecklistIdempotencyConflict as exc:
            raise ChecklistConflictError(
                "checklist_idempotency_conflict",
                retryable=False,
            ) from exc
        except ChecklistSpecVersionConflict as exc:
            raise ChecklistConflictError("checklist_spec_version_conflict") from exc
        except ChecklistContentDigestConflict as exc:
            raise ChecklistConflictError("checklist_content_digest_conflict") from exc
        except ChecklistInputDigestConflict as exc:
            raise ChecklistConflictError("checklist_input_digest_conflict") from exc
        except ChecklistBindingCasConflict as exc:
            raise ChecklistConflictError("checklist_binding_conflict") from exc
        except ChecklistSpecLifecycleConflict as exc:
            raise ChecklistConflictError(
                "checklist_spec_lifecycle_conflict",
                retryable=False,
            ) from exc
        if not isinstance(result, ChecklistExecutionStartResult):
            raise ChecklistPortContractError("checklist_execution_result_invalid")
        if (
            result.execution.board_id != execution.board_id
            or result.execution.spec_id != execution.spec_id
            or result.execution.request_digest != execution.request_digest
        ):
            raise ChecklistPortContractError("checklist_execution_result_mismatch")
        return result

    @staticmethod
    def validate_submission_envelope(
        submission: ChecklistSubmission,
        *,
        actor_id: str,
    ) -> str:
        if not isinstance(submission, ChecklistSubmission):
            raise ChecklistValidationError("checklist_submission_invalid")
        if not isinstance(actor_id, str) or not actor_id.strip():
            raise ChecklistValidationError("checklist_actor_required")
        return actor_id.strip()

    def submission_fingerprint(
        self,
        submission: ChecklistSubmission,
        *,
        actor_id: str,
    ) -> str:
        actor = self.validate_submission_envelope(
            submission,
            actor_id=actor_id,
        )
        try:
            return checklist_submission_digest_v1(
                submission,
                actor_id=actor,
            )
        except ChecklistContractError as exc:
            raise ChecklistValidationError(exc.code, exc.message) from exc

    @staticmethod
    def _canonical_native_items(
        submission: ChecklistSubmission,
    ) -> tuple[ChecklistItemResult, ...]:
        if submission.is_legacy_manual:
            return ()
        actual_ids = tuple(item.item_id for item in submission.items)
        actual_set = set(actual_ids)
        duplicate_ids = sorted(
            item_id for item_id in actual_set if actual_ids.count(item_id) > 1
        )
        unknown_ids = sorted(actual_set - SPECIFY_CHECKLIST_ITEM_ID_SET)
        missing_ids = [
            item_id
            for item_id in SPECIFY_CHECKLIST_ITEM_IDS
            if item_id not in actual_set
        ]
        if (
            len(actual_ids) != len(SPECIFY_CHECKLIST_ITEM_IDS)
            or duplicate_ids
            or unknown_ids
            or missing_ids
        ):
            raise ChecklistValidationError(
                "checklist_items_incomplete",
                "All ten curated checklist items must be present exactly once.",
                details={
                    "expected_count": len(SPECIFY_CHECKLIST_ITEM_IDS),
                    "actual_count": len(actual_ids),
                    "duplicate_item_ids": duplicate_ids,
                    "unknown_item_ids": unknown_ids,
                    "missing_item_ids": missing_ids,
                },
            )
        item_by_id = {item.item_id: item for item in submission.items}
        ordered = tuple(item_by_id[item_id] for item_id in SPECIFY_CHECKLIST_ITEM_IDS)
        for result in ordered:
            template_item = require_specify_checklist_item(result.item_id)
            if (
                result.outcome is ChecklistItemOutcome.NOT_APPLICABLE
                and not template_item.allow_na
            ):
                raise ChecklistValidationError(
                    "checklist_item_na_not_allowed",
                    details={"item_id": result.item_id},
                )
        return ordered

    @staticmethod
    def _validate_preflight(
        submission: ChecklistSubmission,
        preflight: ChecklistPreflight,
    ) -> None:
        if not isinstance(preflight, ChecklistPreflight):
            raise ChecklistValidationError("checklist_preflight_invalid")
        subject = preflight.subject
        binding = preflight.binding
        if (
            submission.board_id != subject.board_id
            or submission.spec_id != subject.spec_id
        ):
            raise ChecklistValidationError("checklist_subject_mismatch")
        if submission.spec_version != subject.spec_version:
            raise ChecklistConflictError("checklist_spec_version_conflict")
        if submission.content_digest != subject.content_digest:
            raise ChecklistConflictError("checklist_content_digest_conflict")
        if submission.input_digest != subject.input_digest:
            raise ChecklistConflictError("checklist_input_digest_conflict")
        if subject.archived:
            raise ChecklistConflictError(
                "checklist_spec_lifecycle_conflict",
                retryable=False,
            )
        if binding.mode is ChecklistMode.OFF:
            raise ChecklistConflictError(
                "checklist_binding_off",
                retryable=False,
            )
        if submission.expected_head_revision != preflight.current_head_revision:
            raise ChecklistConflictError("checklist_head_revision_conflict")
        if (
            submission.template_version != SPECIFY_CHECKLIST_TEMPLATE_VERSION
            or submission.template_digest != SPECIFY_CHECKLIST_TEMPLATE_DIGEST
        ):
            raise ChecklistValidationError("checklist_template_mismatch")
        if (
            binding.board_id != subject.board_id
            or submission.binding_digest != binding.digest
            or submission.template_version != binding.template_version
        ):
            raise ChecklistConflictError("checklist_binding_conflict")

    def prepare_execution(
        self,
        submission: ChecklistSubmission,
        *,
        actor_id: str,
        preflight: ChecklistPreflight,
    ) -> ChecklistWriteBundle:
        """Purely prepare a complete immutable receipt and CAS bundle."""

        try:
            actor = self.validate_submission_envelope(
                submission,
                actor_id=actor_id,
            )
            self._validate_preflight(submission, preflight)
            ordered_items = self._canonical_native_items(submission)
            now = self._clock()
            if (
                not isinstance(now, datetime)
                or now.tzinfo is None
                or now.utcoffset() is None
            ):
                raise ChecklistValidationError("checklist_clock_must_be_timezone_aware")
            now = now.astimezone(timezone.utc)
            request_digest = self.submission_fingerprint(
                submission,
                actor_id=actor,
            )
            source = (
                ChecklistReceiptSource.LEGACY_UNVERIFIED
                if submission.is_legacy_manual
                else ChecklistReceiptSource.NATIVE
            )
            receipt = ChecklistReceipt(
                id=self._id_factory("clr"),
                board_id=preflight.subject.board_id,
                spec_id=preflight.subject.spec_id,
                spec_version=preflight.subject.spec_version,
                content_digest=preflight.subject.content_digest,
                input_digest=preflight.subject.input_digest,
                template_version=SPECIFY_CHECKLIST_TEMPLATE_VERSION,
                template_digest=SPECIFY_CHECKLIST_TEMPLATE_DIGEST,
                binding_version=preflight.binding.version,
                binding_digest=preflight.binding.digest or "",
                binding_mode=preflight.binding.mode,
                items=ordered_items,
                source=source,
                request_digest=request_digest,
                created_by=actor,
                created_at=now,
                head_revision=submission.expected_head_revision + 1,
                idempotency_key=submission.idempotency_key,
                manual_checklist_ref=submission.manual_checklist_ref,
                predecessor_receipt_id=preflight.current_head_receipt_id,
            )
            next_head = ChecklistExecutionHead(
                board_id=receipt.board_id,
                spec_id=receipt.spec_id,
                phase=ChecklistPhase.SPEC_VALIDATION,
                receipt_id=receipt.id,
                revision=receipt.head_revision,
                updated_at=now,
            )
            return ChecklistWriteBundle(
                request_digest=request_digest,
                expected_spec_version=preflight.subject.spec_version,
                expected_content_digest=preflight.subject.content_digest,
                expected_input_digest=preflight.subject.input_digest,
                expected_template_version=SPECIFY_CHECKLIST_TEMPLATE_VERSION,
                expected_template_digest=SPECIFY_CHECKLIST_TEMPLATE_DIGEST,
                expected_binding_version=preflight.binding.version,
                expected_binding_digest=preflight.binding.digest or "",
                expected_binding_mode=preflight.binding.mode,
                expected_head_revision=preflight.current_head_revision,
                expected_head_receipt_id=preflight.current_head_receipt_id,
                expected_spec_status=preflight.subject.status,
                expected_spec_archived=preflight.subject.archived,
                receipt=receipt,
                next_head=next_head,
            )
        except ChecklistError:
            raise
        except ChecklistContractError as exc:
            raise ChecklistValidationError(exc.code, exc.message) from exc

    def resolve_replay(
        self,
        submission: ChecklistSubmission,
        *,
        actor_id: str,
        result: ChecklistCommitResult,
    ) -> ChecklistCommitResult:
        """Validate a native read-side replay before returning it."""

        if not isinstance(submission, ChecklistSubmission):
            raise ChecklistValidationError("checklist_submission_invalid")
        if submission.is_legacy_manual:
            raise ChecklistValidationError("manual_checklist_non_replayable")
        if not isinstance(result, ChecklistCommitResult):
            raise ChecklistPortContractError("checklist_commit_result_invalid")
        if not result.replayed:
            raise ChecklistPortContractError("checklist_replay_flag_invalid")
        fingerprint = self.submission_fingerprint(
            submission,
            actor_id=actor_id,
        )
        if result.request_digest != fingerprint:
            raise ChecklistConflictError(
                "checklist_idempotency_conflict",
                retryable=False,
            )
        self._validate_commit_result(
            bundle=None,
            submission=submission,
            fingerprint=fingerprint,
            result=result,
        )
        return result

    async def apply_prepared(
        self,
        bundle: ChecklistWriteBundle,
        *,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistCommitResult:
        """Apply a prepared bundle through the adapter's transaction CAS."""

        if not isinstance(bundle, ChecklistWriteBundle):
            raise ChecklistValidationError("checklist_bundle_invalid")
        try:
            result = await persistence.apply_execution_cas(bundle)
        except ChecklistHeadRevisionConflict as exc:
            raise ChecklistConflictError("checklist_head_revision_conflict") from exc
        except ChecklistSpecVersionConflict as exc:
            raise ChecklistConflictError("checklist_spec_version_conflict") from exc
        except ChecklistContentDigestConflict as exc:
            raise ChecklistConflictError("checklist_content_digest_conflict") from exc
        except ChecklistInputDigestConflict as exc:
            raise ChecklistConflictError("checklist_input_digest_conflict") from exc
        except ChecklistTemplateConflict as exc:
            raise ChecklistConflictError("checklist_template_conflict") from exc
        except ChecklistBindingCasConflict as exc:
            raise ChecklistConflictError("checklist_binding_conflict") from exc
        except ChecklistIdempotencyConflict as exc:
            raise ChecklistConflictError(
                "checklist_idempotency_conflict",
                retryable=False,
            ) from exc
        except ChecklistSpecLifecycleConflict as exc:
            raise ChecklistConflictError(
                "checklist_spec_lifecycle_conflict",
                retryable=False,
            ) from exc
        self._validate_commit_result(
            bundle=bundle,
            submission=None,
            fingerprint=bundle.request_digest,
            result=result,
        )
        return result

    async def submit_started_execution(
        self,
        execution: ChecklistExecution,
        submission: ChecklistSubmission,
        *,
        expected_execution_revision: int,
        actor_id: str,
        preflight: ChecklistPreflight,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistCommitResult:
        """Submit all results and close the frozen execution in one CAS."""

        if (
            not isinstance(execution, ChecklistExecution)
            or execution.status is not ChecklistExecutionStatus.OPEN
        ):
            raise ChecklistConflictError(
                "checklist_execution_state_conflict",
                retryable=False,
            )
        if (
            not isinstance(expected_execution_revision, int)
            or isinstance(expected_execution_revision, bool)
            or expected_execution_revision < 1
        ):
            raise ChecklistValidationError("checklist_execution_revision_invalid")
        if expected_execution_revision != execution.revision:
            raise ChecklistConflictError("checklist_execution_revision_conflict")
        if (
            submission.board_id != execution.board_id
            or submission.spec_id != execution.spec_id
            or submission.spec_version != execution.spec_version
            or submission.content_digest != execution.content_digest
            or submission.input_digest != execution.input_digest
            or submission.template_version != execution.template_version
            or submission.template_digest != execution.template_digest
            or submission.binding_version != execution.binding_version
            or submission.binding_digest != execution.binding_digest
        ):
            raise ChecklistConflictError(
                "checklist_execution_fence_conflict",
                retryable=False,
            )
        if execution.binding_mode is ChecklistMode.OFF:
            raise ChecklistConflictError(
                "checklist_binding_off",
                retryable=False,
            )
        bundle = self.prepare_execution(
            submission,
            actor_id=actor_id,
            preflight=preflight,
        )
        try:
            result = await persistence.submit_execution_cas(
                execution,
                expected_revision=expected_execution_revision,
                bundle=bundle,
            )
        except ChecklistExecutionRevisionConflict as exc:
            raise ChecklistConflictError(
                "checklist_execution_revision_conflict"
            ) from exc
        except ChecklistExecutionStateConflict as exc:
            raise ChecklistConflictError(
                "checklist_execution_state_conflict",
                retryable=False,
            ) from exc
        except ChecklistHeadRevisionConflict as exc:
            raise ChecklistConflictError("checklist_head_revision_conflict") from exc
        except ChecklistSpecVersionConflict as exc:
            raise ChecklistConflictError("checklist_spec_version_conflict") from exc
        except ChecklistContentDigestConflict as exc:
            raise ChecklistConflictError("checklist_content_digest_conflict") from exc
        except ChecklistInputDigestConflict as exc:
            raise ChecklistConflictError("checklist_input_digest_conflict") from exc
        except ChecklistTemplateConflict as exc:
            raise ChecklistConflictError("checklist_template_conflict") from exc
        except ChecklistBindingCasConflict as exc:
            raise ChecklistConflictError("checklist_binding_conflict") from exc
        except ChecklistIdempotencyConflict as exc:
            raise ChecklistConflictError(
                "checklist_idempotency_conflict",
                retryable=False,
            ) from exc
        except ChecklistSpecLifecycleConflict as exc:
            raise ChecklistConflictError(
                "checklist_spec_lifecycle_conflict",
                retryable=False,
            ) from exc
        self._validate_commit_result(
            bundle=bundle,
            submission=None,
            fingerprint=bundle.request_digest,
            result=result,
        )
        return result

    async def evaluate_spec_gate(
        self,
        *,
        board_id: str,
        spec_id: str,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistGateDecision:
        """Resolve the single shared readiness predicate for every Spec path."""

        snapshot = await persistence.get_spec_snapshot(
            board_id=board_id,
            spec_id=spec_id,
        )
        if snapshot is None:
            raise ChecklistNotFoundError("checklist_spec_not_found")
        if not isinstance(snapshot, ChecklistSpecSnapshot):
            raise ChecklistPortContractError("checklist_subject_port_invalid")
        binding = await persistence.get_binding(
            board_id=board_id,
            target_type=ChecklistTargetType.SPEC,
            phase=ChecklistPhase.SPEC_VALIDATION,
        )
        # A missing binding is the deliberate legacy-board compatibility state:
        # effective OFF, with no retroactive row materialization.
        if binding is None:
            binding = ChecklistBinding.synthetic_off(board_id=board_id)
        current = await persistence.get_current(
            board_id=board_id,
            spec_id=spec_id,
            phase=ChecklistPhase.SPEC_VALIDATION,
        )
        receipt = None if current is None else current[0]
        try:
            return evaluate_checklist_gate(
                binding=binding,
                current_subject=snapshot,
                receipt=receipt,
            )
        except ChecklistContractError as exc:
            raise ChecklistPortContractError("checklist_gate_input_invalid") from exc

    @staticmethod
    def _validate_commit_result(
        *,
        bundle: ChecklistWriteBundle | None,
        submission: ChecklistSubmission | None,
        fingerprint: str,
        result: ChecklistCommitResult,
    ) -> None:
        if not isinstance(result, ChecklistCommitResult):
            raise ChecklistPortContractError("checklist_commit_result_invalid")
        if bundle is None and submission is None:
            raise ChecklistPortContractError(
                "checklist_commit_validation_input_missing"
            )
        if result.request_digest != fingerprint:
            raise ChecklistPortContractError("checklist_commit_digest_mismatch")
        receipt = bundle.receipt if bundle is not None else None
        expected_board_id = (
            receipt.board_id if receipt is not None else submission.board_id
        )
        expected_spec_id = (
            receipt.spec_id if receipt is not None else submission.spec_id
        )
        expected_spec_version = (
            receipt.spec_version if receipt is not None else submission.spec_version
        )
        expected_head_revision = (
            bundle.expected_head_revision + 1
            if bundle is not None
            else submission.expected_head_revision + 1
        )
        if (
            result.board_id != expected_board_id
            or result.spec_id != expected_spec_id
            or result.spec_version != expected_spec_version
            or result.head_revision != expected_head_revision
        ):
            raise ChecklistPortContractError("checklist_commit_identity_mismatch")
        if bundle is not None:
            if (
                bundle.receipt.source is ChecklistReceiptSource.LEGACY_UNVERIFIED
                and result.replayed
            ):
                raise ChecklistPortContractError("manual_checklist_replay_forbidden")
            if not result.replayed and result.receipt_id != bundle.receipt.id:
                raise ChecklistPortContractError("checklist_commit_receipt_mismatch")

    async def submit(
        self,
        submission: ChecklistSubmission,
        *,
        actor_id: str,
        preflight: ChecklistPreflight,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistCommitResult:
        bundle = self.prepare_execution(
            submission,
            actor_id=actor_id,
            preflight=preflight,
        )
        return await self.apply_prepared(bundle, persistence=persistence)

    async def get_current(
        self,
        *,
        board_id: str,
        spec_id: str,
        current_subject: ChecklistSpecSnapshot,
        current_binding: ChecklistBinding,
        persistence: ChecklistPersistencePort,
    ) -> CurrentChecklistView:
        resolved = await persistence.get_current(
            board_id=board_id,
            spec_id=spec_id,
            phase=ChecklistPhase.SPEC_VALIDATION,
        )
        if resolved is None:
            raise ChecklistNotFoundError("checklist_current_not_found")
        if (
            not isinstance(resolved, tuple | list)
            or len(resolved) != 2
            or not isinstance(resolved[0], ChecklistReceipt)
            or not isinstance(resolved[1], ChecklistExecutionHead)
        ):
            raise ChecklistPortContractError("checklist_current_port_invalid")
        receipt, head = resolved
        if (
            receipt.id != head.receipt_id
            or receipt.board_id != board_id
            or receipt.spec_id != spec_id
            or head.board_id != board_id
            or head.spec_id != spec_id
            or head.revision != receipt.head_revision
        ):
            raise ChecklistPortContractError("checklist_current_port_mismatch")
        try:
            currentness = evaluate_checklist_currentness(
                receipt,
                current_subject=current_subject,
                current_binding=current_binding,
            )
            gate = evaluate_checklist_gate(
                binding=current_binding,
                current_subject=current_subject,
                receipt=receipt,
            )
        except ChecklistContractError as exc:
            raise ChecklistValidationError(exc.code, exc.message) from exc
        return CurrentChecklistView(
            receipt=receipt,
            head=head,
            currentness=currentness,
            gate=gate,
        )

    async def get_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistReceipt:
        receipt = await persistence.get_receipt(
            board_id=board_id,
            receipt_id=receipt_id,
        )
        if receipt is None:
            raise ChecklistNotFoundError("checklist_receipt_not_found")
        if not isinstance(receipt, ChecklistReceipt):
            raise ChecklistPortContractError("checklist_receipt_port_invalid")
        if receipt.board_id != board_id or receipt.id != receipt_id:
            raise ChecklistPortContractError("checklist_receipt_port_mismatch")
        return receipt

    async def list_executions(
        self,
        query: ChecklistListQuery,
        *,
        current_subject: ChecklistSpecSnapshot,
        current_binding: ChecklistBinding,
        head_receipt_id: str | None,
        persistence: ChecklistPersistencePort,
    ) -> ChecklistPage[ChecklistReceiptView]:
        if (
            not isinstance(query, ChecklistListQuery)
            or not isinstance(query.board_id, str)
            or not query.board_id.strip()
            or not isinstance(query.spec_id, str)
            or not query.spec_id.strip()
            or not isinstance(current_subject, ChecklistSpecSnapshot)
            or not isinstance(current_binding, ChecklistBinding)
            or current_subject.board_id != query.board_id
            or current_subject.spec_id != query.spec_id
            or current_binding.board_id != query.board_id
            or (
                head_receipt_id is not None
                and (
                    not isinstance(head_receipt_id, str) or not head_receipt_id.strip()
                )
            )
        ):
            raise ChecklistValidationError(
                "checklist_list_query_invalid",
                category=ChecklistErrorCategory.INVALID_ARGUMENT,
            )
        _strict_page_window(query.offset, query.limit)
        page = await persistence.list_executions(query)
        if not isinstance(page, ChecklistPage):
            raise ChecklistPortContractError("checklist_page_contract_invalid")
        if page.offset != query.offset or page.limit != query.limit:
            raise ChecklistPortContractError("checklist_page_window_mismatch")
        if any(not isinstance(item, ChecklistReceipt) for item in page.items):
            raise ChecklistPortContractError("checklist_page_item_invalid")
        expected_order = sorted(
            page.items,
            key=lambda item: (item.created_at, item.id),
            reverse=True,
        )
        if list(page.items) != expected_order:
            raise ChecklistPortContractError("checklist_page_order_mismatch")
        views: list[ChecklistReceiptView] = []
        for receipt in page.items:
            if receipt.board_id != query.board_id or receipt.spec_id != query.spec_id:
                raise ChecklistPortContractError("checklist_page_scope_mismatch")
            try:
                currentness = evaluate_checklist_currentness(
                    receipt,
                    current_subject=current_subject,
                    current_binding=current_binding,
                )
                gate = evaluate_checklist_gate(
                    binding=current_binding,
                    current_subject=current_subject,
                    receipt=receipt,
                )
                views.append(
                    ChecklistReceiptView(
                        receipt=receipt,
                        is_head=receipt.id == head_receipt_id,
                        currentness=currentness,
                        gate=gate,
                    )
                )
            except ChecklistContractError as exc:
                raise ChecklistPortContractError("checklist_page_item_invalid") from exc
        return ChecklistPage(
            items=tuple(views),
            total=page.total,
            offset=page.offset,
            limit=page.limit,
        )


__all__ = [
    "ChecklistConflictError",
    "ChecklistError",
    "ChecklistErrorCategory",
    "ChecklistNotFoundError",
    "ChecklistPortContractError",
    "ChecklistService",
    "ChecklistValidationError",
    "CurrentChecklistView",
]
