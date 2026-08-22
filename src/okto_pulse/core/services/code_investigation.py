"""Application policy for inbound, agent-attested source investigations.

This service issues bounded challenges and validates structured receipts.  It
does not contact an agent, open a repository, invoke Git, read a filesystem,
query a provider, search code, or resolve symbols.  All writes are staged via
the transaction-bound :class:`CodeInvestigationStore` supplied by the caller.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
from types import MappingProxyType
from typing import Protocol, runtime_checkable
from uuid import uuid4

from okto_pulse.core.domain.code_traceability import (
    CODE_INVESTIGATION_CANONICALIZATION_PROFILE,
    CODE_INVESTIGATION_LIMITS_PROFILE,
    DEFAULT_CODE_TRACEABILITY_LIMITS,
    CodeDeliveryContextRequired,
    CodeInvestigationActorKindRequired,
    CodeInvestigationAttestorMismatch,
    CodeInvestigationCapability,
    CodeInvestigationCapabilityMissing,
    CodeInvestigationChallengeConsumed,
    CodeInvestigationChallengeInvalid,
    CodeInvestigationCurrentnessUnknown,
    CodeInvestigationHead,
    CodeInvestigationHeadConflict,
    CodeInvestigationHeadState,
    CodeInvestigationIdempotencyConflict,
    CodeInvestigationNoRelevantExistingImplementationInvalid,
    CodeInvestigationOmission,
    CodeInvestigationOutcome,
    CodeInvestigationPayloadDigestMismatch,
    CodeInvestigationProfileMismatch,
    CodeInvestigationReceipt,
    CodeInvestigationReceiptCommitResult,
    CodeInvestigationReceiptConflicted,
    CodeInvestigationReceiptCurrentness,
    CodeInvestigationReceiptExpired,
    CodeInvestigationReceiptRevocation,
    CodeInvestigationReceiptRevoked,
    CodeInvestigationRequest,
    CodeInvestigationRequestNotFound,
    CodeInvestigationRequestNotOpen,
    CodeInvestigationRequestStatus,
    CodeInvestigationSourceScopeMismatch,
    CodeInvestigationSubjectVersionConflict,
    CodeInvestigationTooling,
    CodeInvestigationTrustInsufficient,
    CodeInvestigationTrustLevel,
    CodeInvestigationUnavailable,
    ContextualInvestigationOutcomeV2,
    CodeTraceabilityContractError,
    CodeTraceabilitySubjectType,
    DeliveryContext,
    ObservedWorkspaceStateRef,
    canonical_code_traceability_json_bytes,
    canonical_code_traceability_sha256,
    code_investigation_observation_sha256,
    code_investigation_observation_sha256_v2,
    code_investigation_omission_digest,
    code_investigation_receipt_currentness,
    legacy_code_investigation_outcome,
    normalize_code_source_ref,
)
from okto_pulse.core.models.code_traceability import (
    CodeInvestigationReceiptSubmission,
    CodeInvestigationReceiptSubmissionV2,
    StartCodeInvestigationInput,
)
from okto_pulse.core.ports.code_investigation import CodeInvestigationStore


Clock = Callable[[], datetime]
IdFactory = Callable[[str], str]


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _aware_utc(value: datetime, code: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CodeTraceabilityContractError(code)
    return value.astimezone(timezone.utc)


def require_code_attestor(actor_id: str, actor_kind: str) -> str:
    """Require a non-forgeable authenticated agent identity."""

    if actor_kind != "agent":
        raise CodeInvestigationActorKindRequired()
    if not isinstance(actor_id, str) or not actor_id.strip():
        raise CodeInvestigationAttestorMismatch()
    return actor_id.strip()


@dataclass(frozen=True, slots=True)
class CodeInvestigationChallengeMaterial:
    key_id: str
    token: str
    token_hash: str

    def __post_init__(self) -> None:
        if not self.key_id or not self.token:
            raise CodeInvestigationChallengeInvalid()
        expected_hash = hashlib.sha256(self.token.encode("utf-8")).hexdigest()
        if not hmac.compare_digest(self.token_hash, expected_hash):
            raise CodeInvestigationChallengeInvalid()


@runtime_checkable
class CodeInvestigationChallengePolicy(Protocol):
    @property
    def active_key_id(self) -> str: ...

    def issue(
        self,
        binding: Mapping[str, object],
    ) -> CodeInvestigationChallengeMaterial: ...

    def regenerate(
        self,
        *,
        key_id: str,
        binding: Mapping[str, object],
    ) -> CodeInvestigationChallengeMaterial: ...

    def verify(
        self,
        *,
        key_id: str,
        binding: Mapping[str, object],
        token: str,
        expected_token_hash: str,
    ) -> bool: ...


class HmacCodeInvestigationChallengePolicy:
    """Deterministic challenge derivation with explicit key rotation support."""

    def __init__(
        self,
        *,
        keys: Mapping[str, bytes],
        active_key_id: str,
    ) -> None:
        if not isinstance(keys, Mapping) or not keys:
            raise CodeInvestigationChallengeInvalid(
                details={"reason": "challenge_keys_missing"}
            )
        normalized: dict[str, bytes] = {}
        for key_id, key in keys.items():
            if (
                not isinstance(key_id, str)
                or not key_id.strip()
                or not isinstance(key, bytes)
                or len(key) < 32
            ):
                raise CodeInvestigationChallengeInvalid(
                    details={"reason": "challenge_key_invalid"}
                )
            normalized[key_id.strip()] = bytes(key)
        if active_key_id not in normalized:
            raise CodeInvestigationChallengeInvalid(
                details={"reason": "active_challenge_key_missing"}
            )
        self._keys = MappingProxyType(normalized)
        self._active_key_id = active_key_id

    @property
    def active_key_id(self) -> str:
        return self._active_key_id

    def _material(
        self,
        *,
        key_id: str,
        binding: Mapping[str, object],
    ) -> CodeInvestigationChallengeMaterial:
        key = self._keys.get(key_id)
        if key is None:
            raise CodeInvestigationChallengeInvalid(
                details={"reason": "challenge_key_unavailable"}
            )
        message = canonical_code_traceability_json_bytes(
            {"key_id": key_id, "binding": binding}
        )
        digest = hmac.new(key, message, hashlib.sha256).digest()
        token = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        return CodeInvestigationChallengeMaterial(
            key_id=key_id,
            token=token,
            token_hash=hashlib.sha256(token.encode("utf-8")).hexdigest(),
        )

    def issue(
        self,
        binding: Mapping[str, object],
    ) -> CodeInvestigationChallengeMaterial:
        return self._material(key_id=self.active_key_id, binding=binding)

    def regenerate(
        self,
        *,
        key_id: str,
        binding: Mapping[str, object],
    ) -> CodeInvestigationChallengeMaterial:
        return self._material(key_id=key_id, binding=binding)

    def verify(
        self,
        *,
        key_id: str,
        binding: Mapping[str, object],
        token: str,
        expected_token_hash: str,
    ) -> bool:
        try:
            expected = self.regenerate(key_id=key_id, binding=binding)
        except CodeInvestigationChallengeInvalid:
            return False
        actual_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        return hmac.compare_digest(token, expected.token) and hmac.compare_digest(
            actual_hash,
            expected_token_hash,
        )


@dataclass(frozen=True, slots=True)
class StartedCodeInvestigation:
    request: CodeInvestigationRequest
    challenge_token: str | None
    consumed_receipt_id: str | None = None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class SubmittedCodeInvestigationReceipt:
    receipt: CodeInvestigationReceipt
    generation: int
    head_revision: int
    replayed: bool


@dataclass(frozen=True, slots=True)
class AcceptedCodeInvestigation:
    receipt: CodeInvestigationReceipt
    head: CodeInvestigationHead
    currentness: CodeInvestigationReceiptCurrentness


@dataclass(frozen=True, slots=True)
class InspectedCodeInvestigationReceipt:
    receipt: CodeInvestigationReceipt
    head: CodeInvestigationHead | None
    revocation: CodeInvestigationReceiptRevocation | None
    currentness: CodeInvestigationReceiptCurrentness


def selector_scope_digest_for_subject(
    *,
    board_id: str,
    subject_type: CodeTraceabilitySubjectType,
    subject_id: str,
    subject_version: int,
) -> str:
    """Bind an Evidence-capable parent/version without admitting a locator."""

    return canonical_code_traceability_sha256(
        {
            "contract": "code-investigation-selector-scope/v1",
            "board_id": board_id,
            "subject_type": subject_type,
            "subject_id": subject_id,
            "subject_version": subject_version,
            "selector_policy": "bounded_agent_submission",
        }
    )


def selector_scope_digest_for_card_targets(
    *,
    board_id: str,
    card_id: str,
    card_version: int,
    targets: Sequence[tuple[str, int]],
) -> str:
    """Bind a Card preflight to exact Target IDs and optimistic revisions."""

    canonical_targets = tuple(
        sorted((str(target_id), int(revision)) for target_id, revision in targets)
    )
    if len({target_id for target_id, _ in canonical_targets}) != len(canonical_targets):
        raise CodeTraceabilityContractError(
            "code_investigation_selector_scope_target_duplicate"
        )
    return canonical_code_traceability_sha256(
        {
            "contract": "code-investigation-selector-scope/v1",
            "board_id": board_id,
            "subject_type": CodeTraceabilitySubjectType.CARD,
            "subject_id": card_id,
            "subject_version": card_version,
            "targets": canonical_targets,
        }
    )


def required_capabilities_for_subject(
    subject_type: CodeTraceabilitySubjectType,
) -> tuple[CodeInvestigationCapability, ...]:
    common = {
        CodeInvestigationCapability.SOURCE_IDENTITY,
        CodeInvestigationCapability.REVISION_IDENTITY,
        CodeInvestigationCapability.WORKSPACE_FINGERPRINT,
        CodeInvestigationCapability.FILE_READ,
        CodeInvestigationCapability.PATH_CONTAINMENT,
        CodeInvestigationCapability.SYMLINK_CONTAINMENT,
        CodeInvestigationCapability.SECRET_SCAN,
        CodeInvestigationCapability.BINARY_DETECTION,
    }
    if subject_type is CodeTraceabilitySubjectType.CARD:
        common.update(
            {
                CodeInvestigationCapability.SYMBOL_RESOLUTION,
                CodeInvestigationCapability.RENAME_OBSERVATION,
            }
        )
    else:
        common.add(CodeInvestigationCapability.SAFE_EXCERPT)
    return tuple(sorted(common, key=lambda item: item.value))


def effective_required_capabilities_for_subject(
    subject_type: CodeTraceabilitySubjectType,
    *,
    receipt_content: str,
) -> tuple[CodeInvestigationCapability, ...]:
    """Apply the server-owned receipt-content policy to one preflight set."""

    required = set(required_capabilities_for_subject(subject_type))
    if receipt_content == "metadata_only":
        required.discard(CodeInvestigationCapability.SAFE_EXCERPT)
    return tuple(sorted(required, key=lambda item: item.value))


class CodeInvestigationService:
    """Issue requests, accept receipts, and validate receipt dependencies."""

    def __init__(
        self,
        *,
        challenge_policy: CodeInvestigationChallengePolicy | None = None,
        clock: Clock = _default_clock,
        id_factory: IdFactory = _default_id_factory,
        challenge_ttl_seconds: int = 600,
    ) -> None:
        if (
            type(challenge_ttl_seconds) is not int
            or not 1
            <= challenge_ttl_seconds
            <= DEFAULT_CODE_TRACEABILITY_LIMITS.challenge_ttl_seconds
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_challenge_ttl_invalid"
            )
        self._challenge_policy = challenge_policy
        self._clock = clock
        self._id_factory = id_factory
        self._challenge_ttl_seconds = challenge_ttl_seconds

    def _policy(self) -> CodeInvestigationChallengePolicy:
        policy = self._challenge_policy
        if policy is None:
            raise CodeInvestigationUnavailable(
                details={"reason": "challenge_policy_missing"}
            )
        return policy

    def _now(self) -> datetime:
        return _aware_utc(self._clock(), "code_investigation_clock_invalid")

    def _started_from_replay(
        self,
        replay: object,
        *,
        request_payload_sha256: str,
        now: datetime,
    ) -> StartedCodeInvestigation:
        replay_request = getattr(replay, "request", None)
        if (
            not isinstance(replay_request, CodeInvestigationRequest)
            or replay_request.request_payload_sha256 != request_payload_sha256
        ):
            raise CodeInvestigationIdempotencyConflict()
        effective_request = replay_request
        token: str | None = None
        if (
            effective_request.status is CodeInvestigationRequestStatus.OPEN
            and now < effective_request.expires_at
        ):
            material = self._policy().regenerate(
                key_id=effective_request.challenge_key_id,
                binding=self._request_challenge_binding(effective_request),
            )
            if not hmac.compare_digest(
                material.token_hash,
                effective_request.challenge_token_hash,
            ):
                raise CodeInvestigationChallengeInvalid()
            token = material.token
        elif (
            effective_request.status is CodeInvestigationRequestStatus.OPEN
            and now >= effective_request.expires_at
        ):
            effective_request = replace(
                effective_request,
                status=CodeInvestigationRequestStatus.EXPIRED,
            )
        return StartedCodeInvestigation(
            request=effective_request,
            challenge_token=token,
            consumed_receipt_id=getattr(replay, "consumed_receipt_id", None),
            replayed=True,
        )

    @staticmethod
    def _request_challenge_binding(
        request: CodeInvestigationRequest,
    ) -> Mapping[str, object]:
        return {
            "request_id": request.id,
            "board_id": request.board_id,
            "subject_type": request.subject_type,
            "subject_id": request.subject_id,
            "subject_version": request.subject_version,
            "issued_to_actor_id": request.issued_to_actor_id,
            "source_ref": request.source_ref,
            "required_capabilities": request.required_capabilities,
            "selector_scope_digest": request.selector_scope_digest,
            "expected_head_generation": request.expected_head_generation,
            "expected_predecessor_receipt_id": (
                request.expected_predecessor_receipt_id
            ),
            "canonicalization_profile": request.canonicalization_profile,
            "limits_profile": request.limits_profile,
            "challenge_key_id": request.challenge_key_id,
            "expires_at": request.expires_at,
            "request_payload_sha256": request.request_payload_sha256,
            "idempotency_key": request.idempotency_key,
        }

    @staticmethod
    def _start_payload_sha256(
        submission: StartCodeInvestigationInput,
        *,
        actor_id: str,
    ) -> str:
        return canonical_code_traceability_sha256(
            {
                "operation": "start_code_investigation",
                "actor_id": actor_id,
                "board_id": submission.board_id,
                "subject_type": submission.subject_type,
                "subject_id": submission.subject_id,
                "subject_version": submission.expected_subject_version,
                "requested_source_ref": submission.source_ref,
                "idempotency_key": submission.idempotency_key,
            }
        )

    async def start(
        self,
        submission: StartCodeInvestigationInput,
        *,
        actor_id: str,
        actor_kind: str,
        selector_scope_digest: str,
        required_capabilities: Sequence[CodeInvestigationCapability],
        store: CodeInvestigationStore,
    ) -> StartedCodeInvestigation:
        actor = require_code_attestor(actor_id, actor_kind)
        if not isinstance(submission, StartCodeInvestigationInput):
            raise CodeTraceabilityContractError(
                "code_investigation_start_submission_invalid"
            )
        now = self._now()
        request_payload_sha256 = self._start_payload_sha256(
            submission,
            actor_id=actor,
        )
        if submission.source_ref is not None:
            source_ref = normalize_code_source_ref(submission.source_ref)
            head = await store.get_current_head(
                board_id=submission.board_id,
                source_ref=source_ref,
            )
            if head is None:
                raise CodeInvestigationSourceScopeMismatch(
                    details={"reason": "source_ref_not_authorized"}
                )
        else:
            source_ref = normalize_code_source_ref(self._id_factory("source"))
            head = await store.get_current_head(
                board_id=submission.board_id,
                source_ref=source_ref,
            )
            if head is not None:
                raise CodeInvestigationUnavailable(
                    details={"reason": "allocated_source_ref_collision"}
                )
        expected_generation = 0 if head is None else head.generation
        expected_predecessor = None if head is None else head.latest_receipt_id
        capabilities = tuple(
            sorted(set(required_capabilities), key=lambda item: item.value)
        )
        key_id = self._policy().active_key_id
        expires_at = now + timedelta(seconds=self._challenge_ttl_seconds)
        request_without_hash = CodeInvestigationRequest(
            id=self._id_factory("code_request"),
            board_id=submission.board_id,
            subject_type=submission.subject_type,
            subject_id=submission.subject_id,
            subject_version=submission.expected_subject_version,
            issued_to_actor_id=actor,
            source_ref=source_ref,
            required_capabilities=capabilities,
            selector_scope_digest=selector_scope_digest,
            expected_head_generation=expected_generation,
            expected_predecessor_receipt_id=expected_predecessor,
            canonicalization_profile=(CODE_INVESTIGATION_CANONICALIZATION_PROFILE),
            limits_profile=CODE_INVESTIGATION_LIMITS_PROFILE,
            challenge_key_id=key_id,
            challenge_token_hash="0" * 64,
            status=CodeInvestigationRequestStatus.OPEN,
            single_use=True,
            expires_at=expires_at,
            requested_by=actor,
            created_at=now,
            consumed_at=None,
            request_payload_sha256=request_payload_sha256,
            idempotency_key=submission.idempotency_key,
        )
        material = self._policy().issue(
            self._request_challenge_binding(request_without_hash)
        )
        if material.key_id != key_id:
            raise CodeInvestigationChallengeInvalid(
                details={"reason": "challenge_key_changed_during_issue"}
            )
        persisted_request = replace(
            request_without_hash,
            challenge_token_hash=material.token_hash,
        )
        created = await store.create_request_if_below_open_limit(
            request=persisted_request,
            at=now,
            max_open_requests=(
                DEFAULT_CODE_TRACEABILITY_LIMITS.open_requests_per_actor_board
            ),
        )
        if created.replayed:
            return self._started_from_replay(
                created,
                request_payload_sha256=request_payload_sha256,
                now=now,
            )
        persisted = created.request
        if persisted != persisted_request:
            raise CodeInvestigationPayloadDigestMismatch(details={"field": "request"})
        return StartedCodeInvestigation(
            request=persisted,
            challenge_token=material.token,
            replayed=False,
        )

    @staticmethod
    def _receipt_payload_sha256(
        submission: (
            CodeInvestigationReceiptSubmission
            | CodeInvestigationReceiptSubmissionV2
        ),
        *,
        request: CodeInvestigationRequest,
        actor_id: str,
        delivery_context: DeliveryContext | None = None,
    ) -> str:
        agent_payload = submission.model_dump(
            mode="python",
            exclude={"challenge_token"},
        )
        if isinstance(submission, CodeInvestigationReceiptSubmissionV2):
            if delivery_context is None:
                raise CodeDeliveryContextRequired()
            return canonical_code_traceability_sha256(
                {
                    "operation": "submit_code_investigation_receipt_v2",
                    "actor_id": actor_id,
                    "request_id": request.id,
                    "board_id": request.board_id,
                    "source_ref": request.source_ref,
                    "subject_type": request.subject_type,
                    "subject_id": request.subject_id,
                    "subject_version": request.subject_version,
                    "generation": request.expected_head_generation + 1,
                    "predecessor_receipt_id": (
                        request.expected_predecessor_receipt_id
                    ),
                    "selector_scope_digest": request.selector_scope_digest,
                    "canonicalization_profile": request.canonicalization_profile,
                    "limits_profile": request.limits_profile,
                    "delivery_context": delivery_context,
                    "agent_payload": agent_payload,
                }
            )
        return canonical_code_traceability_sha256(
            {
                "operation": "submit_code_investigation_receipt",
                "actor_id": actor_id,
                "request_id": request.id,
                "board_id": request.board_id,
                "source_ref": request.source_ref,
                "subject_type": request.subject_type,
                "subject_id": request.subject_id,
                "subject_version": request.subject_version,
                "generation": request.expected_head_generation + 1,
                "predecessor_receipt_id": (request.expected_predecessor_receipt_id),
                "selector_scope_digest": request.selector_scope_digest,
                "canonicalization_profile": request.canonicalization_profile,
                "limits_profile": request.limits_profile,
                "agent_payload": agent_payload,
            }
        )

    async def submit_receipt(
        self,
        submission: (
            CodeInvestigationReceiptSubmission
            | CodeInvestigationReceiptSubmissionV2
        ),
        *,
        actor_id: str,
        actor_kind: str,
        freshness_seconds: int,
        store: CodeInvestigationStore,
        delivery_context: DeliveryContext | None = None,
    ) -> SubmittedCodeInvestigationReceipt:
        actor = require_code_attestor(actor_id, actor_kind)
        if not isinstance(
            submission,
            (
                CodeInvestigationReceiptSubmission,
                CodeInvestigationReceiptSubmissionV2,
            ),
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_receipt_submission_invalid"
            )
        contextual_submission = isinstance(
            submission,
            CodeInvestigationReceiptSubmissionV2,
        )
        resolved_delivery_context: DeliveryContext | None = None
        if contextual_submission:
            try:
                resolved_delivery_context = DeliveryContext(delivery_context)
            except (TypeError, ValueError) as exc:
                raise CodeDeliveryContextRequired() from exc
            if (
                submission.outcome
                is ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION
                and resolved_delivery_context is not DeliveryContext.GREENFIELD
            ):
                raise CodeInvestigationNoRelevantExistingImplementationInvalid(
                    details={
                        "delivery_context": resolved_delivery_context.value,
                    }
                )
        if type(freshness_seconds) is not int or not 60 <= freshness_seconds <= 86_400:
            raise CodeInvestigationProfileMismatch(
                details={"field": "preflight_freshness_seconds"}
            )
        request = await store.get_request(
            board_id=submission.board_id,
            request_id=submission.request_id,
        )
        if request is None:
            raise CodeInvestigationRequestNotFound()
        if request.issued_to_actor_id != actor:
            raise CodeInvestigationAttestorMismatch()
        payload_sha256 = self._receipt_payload_sha256(
            submission,
            request=request,
            actor_id=actor,
            delivery_context=resolved_delivery_context,
        )
        replay = await store.resolve_receipt_replay(
            board_id=submission.board_id,
            attestor_actor_id=actor,
            request_id=request.id,
            idempotency_key=submission.idempotency_key,
        )
        if replay is not None:
            if replay.payload_sha256 != payload_sha256:
                raise CodeInvestigationIdempotencyConflict()
            return SubmittedCodeInvestigationReceipt(
                receipt=replay,
                generation=replay.generation,
                head_revision=replay.generation,
                replayed=True,
            )
        now = self._now()
        if request.status is CodeInvestigationRequestStatus.CONSUMED:
            raise CodeInvestigationChallengeConsumed()
        if request.status is not CodeInvestigationRequestStatus.OPEN:
            raise CodeInvestigationRequestNotOpen(
                details={"status": request.status.value}
            )
        if now >= request.expires_at:
            raise CodeInvestigationReceiptExpired()
        token = submission.challenge_token.get_secret_value()
        if not self._policy().verify(
            key_id=request.challenge_key_id,
            binding=self._request_challenge_binding(request),
            token=token,
            expected_token_hash=request.challenge_token_hash,
        ):
            raise CodeInvestigationChallengeInvalid()
        submitted_capabilities = set(submission.capabilities)
        missing = set(request.required_capabilities) - submitted_capabilities
        complete_outcome = (
            submission.outcome is CodeInvestigationOutcome.ACCESSIBLE
            if not contextual_submission
            else submission.outcome
            in {
                ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
                ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION,
            }
        )
        if missing and complete_outcome:
            raise CodeInvestigationCapabilityMissing(
                details={"capabilities": tuple(sorted(item.value for item in missing))}
            )
        head = await store.get_current_head(
            board_id=request.board_id,
            source_ref=request.source_ref,
        )
        actual_generation = 0 if head is None else head.generation
        actual_predecessor = None if head is None else head.latest_receipt_id
        if (
            actual_generation != request.expected_head_generation
            or actual_predecessor != request.expected_predecessor_receipt_id
        ):
            raise CodeInvestigationHeadConflict()
        observed_at = _aware_utc(
            submission.observed_at,
            "code_investigation_observed_at_invalid",
        )
        if (
            abs((now - observed_at).total_seconds())
            > DEFAULT_CODE_TRACEABILITY_LIMITS.observed_at_clock_skew_seconds
        ):
            raise CodeTraceabilityContractError(
                "code_investigation_observed_at_clock_skew"
            )
        workspace_state = None
        if submission.workspace_state is not None:
            state = submission.workspace_state
            workspace_state = ObservedWorkspaceStateRef(
                declared_revision=submission.declared_revision,
                workspace_state_id=state.workspace_state_id,
                declared_dirty=state.declared_dirty,
                observed_at=observed_at,
                reproducibility_claim=state.reproducibility_claim,
                fingerprint_algorithm=state.fingerprint_algorithm,
                manifest_digest=state.manifest_digest,
                manifest_entry_count=state.manifest_entry_count,
            )
        omissions = tuple(
            CodeInvestigationOmission(
                reason_code=item.reason_code,
                affected_scope_digest=item.affected_scope_digest,
                count=item.count,
            )
            for item in submission.omission_manifest
        )
        if contextual_submission:
            observation_sha256 = code_investigation_observation_sha256_v2(
                source_ref=request.source_ref,
                selector_scope_digest=request.selector_scope_digest,
                delivery_context=resolved_delivery_context,
                outcome=submission.outcome,
                capabilities=submission.capabilities,
                source_identity_digest=submission.source_identity_digest,
                declared_revision=submission.declared_revision,
                workspace_state=workspace_state,
                omission_manifest=omissions,
            )
            legacy_outcome = legacy_code_investigation_outcome(submission.outcome)
            contextual_outcome = submission.outcome
        else:
            observation_sha256 = code_investigation_observation_sha256(
                source_ref=request.source_ref,
                selector_scope_digest=request.selector_scope_digest,
                outcome=submission.outcome,
                capabilities=submission.capabilities,
                source_identity_digest=submission.source_identity_digest,
                declared_revision=submission.declared_revision,
                workspace_state=workspace_state,
                omission_manifest=omissions,
            )
            legacy_outcome = submission.outcome
            contextual_outcome = None
        predecessor = (
            None
            if actual_predecessor is None
            else await store.get_receipt(
                board_id=request.board_id,
                receipt_id=actual_predecessor,
            )
        )
        same_scope_lineage = (
            predecessor is not None
            and predecessor.selector_scope_digest == request.selector_scope_digest
        )
        trust_level = CodeInvestigationTrustLevel.SINGLE_ATTESTATION
        if same_scope_lineage:
            # Corroboration and contradiction are meaningful only inside the
            # same frozen selector scope.  The head is global per source_ref,
            # so sequential checks for another Task/Refinement legitimately
            # carry a different observation digest even when they saw the same
            # source state.  Treating that scope change as a contradiction
            # would poison the global head and prevent the next dependent Task
            # from refreshing its own resolutions.
            if predecessor.observation_sha256 != observation_sha256:
                trust_level = CodeInvestigationTrustLevel.CONFLICTED
            elif (
                predecessor.attestor_actor_id != actor
                or predecessor.trust_level is CodeInvestigationTrustLevel.CORROBORATED
            ):
                trust_level = CodeInvestigationTrustLevel.CORROBORATED
        if (
            head is not None
            and head.state is CodeInvestigationHeadState.CONFLICTED
            and same_scope_lineage
            and trust_level is not CodeInvestigationTrustLevel.CORROBORATED
        ):
            # A conflicted lineage can only become current after an independent
            # actor corroborates the latest observation.  The same actor
            # repeating its own divergent claim must not self-resolve it.
            trust_level = CodeInvestigationTrustLevel.CONFLICTED
        generation = actual_generation + 1
        receipt = CodeInvestigationReceipt(
            id=self._id_factory("code_receipt"),
            request_id=request.id,
            board_id=request.board_id,
            subject_type=request.subject_type,
            subject_id=request.subject_id,
            subject_version=request.subject_version,
            attestor_actor_id=actor,
            generation=generation,
            predecessor_receipt_id=actual_predecessor,
            trust_level=trust_level,
            acceptance_status="accepted",
            outcome=legacy_outcome,
            capabilities=submission.capabilities,
            source_ref=request.source_ref,
            source_identity_digest=submission.source_identity_digest,
            canonicalization_profile=request.canonicalization_profile,
            limits_profile=request.limits_profile,
            selector_scope_digest=request.selector_scope_digest,
            declared_revision=submission.declared_revision,
            workspace_state=workspace_state,
            omission_manifest=omissions,
            omission_digest=code_investigation_omission_digest(omissions),
            omission_count=sum(item.count for item in omissions),
            tooling=CodeInvestigationTooling(
                tool_id=submission.tooling.tool_id,
                tool_version=submission.tooling.tool_version,
                method_id=submission.tooling.method_id,
            ),
            observed_at=observed_at,
            received_at=now,
            expires_at=now + timedelta(seconds=freshness_seconds),
            observation_sha256=observation_sha256,
            payload_sha256=payload_sha256,
            idempotency_key=submission.idempotency_key,
            delivery_context=resolved_delivery_context,
            contextual_outcome=contextual_outcome,
            context_contract_version=(2 if contextual_submission else None),
        )
        consumed_request = replace(
            request,
            status=CodeInvestigationRequestStatus.CONSUMED,
            consumed_at=now,
        )
        head_revision = 1 if head is None else head.revision + 1
        next_head = CodeInvestigationHead(
            board_id=request.board_id,
            source_ref=request.source_ref,
            generation=generation,
            latest_receipt_id=receipt.id,
            current_receipt_id=(None if head is None else head.current_receipt_id)
            if trust_level is CodeInvestigationTrustLevel.CONFLICTED
            else receipt.id,
            state=(
                CodeInvestigationHeadState.CONFLICTED
                if trust_level is CodeInvestigationTrustLevel.CONFLICTED
                else CodeInvestigationHeadState.CURRENT
            ),
            revision=head_revision,
            updated_at=now,
        )
        committed = await store.consume_request_append_receipt_and_advance_head(
            request=consumed_request,
            receipt=receipt,
            head=next_head,
            expected_head_revision=None if head is None else head.revision,
        )
        if not isinstance(committed, CodeInvestigationReceiptCommitResult):
            raise CodeInvestigationPayloadDigestMismatch(
                details={"field": "commit_result"}
            )
        return SubmittedCodeInvestigationReceipt(
            receipt=committed.receipt,
            generation=committed.head.generation,
            head_revision=committed.head.revision,
            replayed=committed.replayed,
        )

    async def require_current_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
        store: CodeInvestigationStore,
        actor_id: str | None = None,
        subject_type: CodeTraceabilitySubjectType | None = None,
        subject_id: str | None = None,
        subject_version: int | None = None,
        source_ref: str | None = None,
        required_capabilities: Sequence[CodeInvestigationCapability] = (),
        minimum_trust: CodeInvestigationTrustLevel = (
            CodeInvestigationTrustLevel.SINGLE_ATTESTATION
        ),
        require_committed_state: bool = False,
    ) -> AcceptedCodeInvestigation:
        receipt = await store.get_receipt(
            board_id=board_id,
            receipt_id=receipt_id,
        )
        if receipt is None:
            raise CodeInvestigationRequestNotFound(
                details={"entity": "receipt", "receipt_id": receipt_id}
            )
        if actor_id is not None and receipt.attestor_actor_id != actor_id:
            raise CodeInvestigationAttestorMismatch()
        if subject_type is not None and receipt.subject_type is not subject_type:
            raise CodeInvestigationSourceScopeMismatch()
        if subject_id is not None and receipt.subject_id != subject_id:
            raise CodeInvestigationSourceScopeMismatch()
        if subject_version is not None and receipt.subject_version != subject_version:
            raise CodeInvestigationSubjectVersionConflict()
        if source_ref is not None and receipt.source_ref != source_ref:
            raise CodeInvestigationSourceScopeMismatch()
        revocation = await store.get_receipt_revocation(
            board_id=board_id,
            receipt_id=receipt.id,
        )
        head = await store.get_current_head(
            board_id=board_id,
            source_ref=receipt.source_ref,
        )
        currentness = code_investigation_receipt_currentness(
            receipt,
            head=head,
            at=self._now(),
            revocation=revocation,
        )
        if currentness is CodeInvestigationReceiptCurrentness.REVOKED:
            raise CodeInvestigationReceiptRevoked()
        if currentness is CodeInvestigationReceiptCurrentness.EXPIRED:
            raise CodeInvestigationReceiptExpired()
        if currentness is CodeInvestigationReceiptCurrentness.CONFLICTED:
            raise CodeInvestigationReceiptConflicted()
        if currentness is not CodeInvestigationReceiptCurrentness.CURRENT:
            raise CodeInvestigationCurrentnessUnknown(
                details={"currentness": currentness.value}
            )
        if head is None:  # narrowed by CURRENT, kept fail-closed for adapters
            raise CodeInvestigationCurrentnessUnknown()
        if receipt.outcome is CodeInvestigationOutcome.UNAVAILABLE:
            raise CodeInvestigationUnavailable()
        if receipt.trust_level is CodeInvestigationTrustLevel.CONFLICTED:
            raise CodeInvestigationReceiptConflicted()
        if (
            minimum_trust is CodeInvestigationTrustLevel.CORROBORATED
            and receipt.trust_level is not CodeInvestigationTrustLevel.CORROBORATED
        ):
            raise CodeInvestigationTrustInsufficient()
        missing = set(required_capabilities) - set(receipt.capabilities)
        if missing:
            raise CodeInvestigationCapabilityMissing(
                details={"capabilities": tuple(sorted(item.value for item in missing))}
            )
        if receipt.workspace_state is None:
            raise CodeInvestigationCapabilityMissing(
                details={"capability": "workspace_fingerprint"}
            )
        if require_committed_state and receipt.workspace_state.declared_dirty:
            raise CodeInvestigationUnavailable(
                details={"reason": "committed_workspace_required"}
            )
        return AcceptedCodeInvestigation(
            receipt=receipt,
            head=head,
            currentness=currentness,
        )

    async def inspect_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
        store: CodeInvestigationStore,
    ) -> InspectedCodeInvestigationReceipt:
        """Return bounded ledger currentness without performing any new check."""

        receipt = await store.get_receipt(
            board_id=board_id,
            receipt_id=receipt_id,
        )
        if receipt is None:
            raise CodeInvestigationRequestNotFound(
                details={"entity": "receipt", "receipt_id": receipt_id}
            )
        revocation = await store.get_receipt_revocation(
            board_id=board_id,
            receipt_id=receipt.id,
        )
        head = await store.get_current_head(
            board_id=board_id,
            source_ref=receipt.source_ref,
        )
        return InspectedCodeInvestigationReceipt(
            receipt=receipt,
            head=head,
            revocation=revocation,
            currentness=code_investigation_receipt_currentness(
                receipt,
                head=head,
                at=self._now(),
                revocation=revocation,
            ),
        )

    async def revoke_receipt(
        self,
        *,
        board_id: str,
        receipt_id: str,
        reason_code: str,
        justification: str,
        actor_id: str,
        store: CodeInvestigationStore,
    ) -> CodeInvestigationReceiptRevocation:
        receipt = await store.get_receipt(board_id=board_id, receipt_id=receipt_id)
        if receipt is None:
            raise CodeInvestigationRequestNotFound(
                details={"entity": "receipt", "receipt_id": receipt_id}
            )
        existing = await store.get_receipt_revocation(
            board_id=board_id,
            receipt_id=receipt_id,
        )
        if existing is not None:
            return existing
        revocation = CodeInvestigationReceiptRevocation(
            id=self._id_factory("code_receipt_revocation"),
            receipt_id=receipt.id,
            board_id=board_id,
            reason_code=reason_code,
            justification=justification,
            revoked_by=actor_id,
            revoked_at=self._now(),
        )
        return await store.append_receipt_revocation(revocation)


__all__ = [
    "AcceptedCodeInvestigation",
    "CodeInvestigationChallengeMaterial",
    "CodeInvestigationChallengePolicy",
    "CodeInvestigationService",
    "HmacCodeInvestigationChallengePolicy",
    "InspectedCodeInvestigationReceipt",
    "StartedCodeInvestigation",
    "SubmittedCodeInvestigationReceipt",
    "effective_required_capabilities_for_subject",
    "required_capabilities_for_subject",
    "require_code_attestor",
    "selector_scope_digest_for_card_targets",
    "selector_scope_digest_for_subject",
]
