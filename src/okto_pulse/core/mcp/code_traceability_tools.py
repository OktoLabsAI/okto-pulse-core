"""Typed MCP adapters for agent-mediated Code Traceability.

The handlers are intentionally thin.  They authenticate a board actor, build
closed commands, and delegate to Core use cases.  They never inspect a source
tree, repository, provider, filesystem, or language runtime.  Deterministic
investigation happens in the authenticated external agent's own environment.
"""

from __future__ import annotations

import base64
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from functools import wraps
import hashlib
import json
from typing import Annotated, Any, Callable, Mapping

from pydantic import Field, SecretStr, ValidationError

from okto_pulse.core.application.use_cases.base import (
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceAttestationState,
    CodeEvidenceDispositionKind,
    CodeEvidenceSelectorKind,
    CodeEvidenceSpecRelationType,
    CodeEvidenceType,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityPage,
    CodeTraceabilityPageCursor,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverReason,
    CodeTraceabilityWaiverScope,
    ImplementationTargetExecutionDisposition,
    ImplementationTargetResolutionState,
    ImplementationTargetRole,
    ImplementationTargetSelectorKind,
    SpecEntityType,
    TargetOverlapDisposition,
)
from okto_pulse.core.mcp.outcome import McpToolOutcome
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceDispositionInput,
    CodeEvidenceSelectorInput,
    CodeEvidenceSpecLinkInput,
    CodeEvidenceSpecUnlinkInput,
    CodeEvidenceSubmission,
    CodeEvidenceSupersessionSubmission,
    CodeInvestigationOmissionInput,
    CodeInvestigationReceiptSubmission,
    CodeInvestigationToolingInput,
    CodeTraceabilityWaiverClearInput,
    CodeTraceabilityWaiverInput,
    ImplementationTargetCreateInput,
    ImplementationTargetEvidenceLinkInput,
    ImplementationTargetExecutionSubmission,
    ImplementationTargetResolutionSubmission,
    ImplementationTargetSpecLinkInput,
    ImplementationTargetUpdateInput,
    ObservedWorkspaceStateSubmission,
    ResolutionCandidateInput,
    StartCodeInvestigationInput,
    TargetOverlapAcknowledgementInput,
)
from okto_pulse.core.ports.code_traceability import (
    CodeEvidenceQuery,
    ImplementationTargetQuery,
    TargetOverlapQuery,
)
from okto_pulse.core.services.code_evidence import CodeEvidenceService
from okto_pulse.core.services.code_investigation import (
    CodeInvestigationService,
    HmacCodeInvestigationChallengePolicy,
)
from okto_pulse.core.services.implementation_targets import (
    ImplementationTargetService,
)
from okto_pulse.core.services.code_overlap import CodeOverlapService


CODE_TRACEABILITY_RESOURCE_URI = "okto-pulse://reference/code-traceability"

BoundedId = Annotated[str, Field(min_length=1, max_length=512)]
BoundedText = Annotated[str, Field(min_length=1, max_length=20_000)]
OptionalBoundedText = Annotated[str | None, Field(default=None, max_length=20_000)]
Digest = Annotated[str, Field(pattern=r"^[0-9a-fA-F]{64}$")]
OptionalDigest = Annotated[str | None, Field(default=None, pattern=r"^[0-9a-fA-F]{64}$")]
PageLimit = Annotated[int, Field(ge=1, le=200)]
CursorToken = Annotated[str, Field(min_length=1, max_length=4096)]


def _native(value: object) -> object:
    """Project closed Core values without leaking secret wrappers."""

    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, SecretStr):
        return "**********"
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _native(item) for key, item in value.items()}
    if isinstance(value, tuple | list | set | frozenset):
        return [_native(item) for item in value]
    if is_dataclass(value):
        payload = {
            field.name: _native(getattr(value, field.name))
            for field in fields(value)
            if getattr(value, field.name) is not None
        }
        if isinstance(value, CodeTraceabilityPage):
            payload["next_cursor"] = (
                _encode_cursor(value.next_cursor)
                if value.next_cursor is not None
                else None
            )
            payload["has_more"] = value.next_cursor is not None
        return payload
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    raise TypeError(f"code_traceability_mcp_projection_unsupported:{type(value).__name__}")


def _encode_cursor(cursor: CodeTraceabilityPageCursor) -> str:
    payload = json.dumps(
        {
            "created_at": _native(cursor.created_at),
            "item_id": cursor.item_id,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None) -> CodeTraceabilityPageCursor | None:
    if cursor is None:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict) or set(payload) != {"created_at", "item_id"}:
            raise ValueError("shape")
        created_at = datetime.fromisoformat(str(payload["created_at"]).replace("Z", "+00:00"))
        return CodeTraceabilityPageCursor(
            created_at=created_at,
            item_id=str(payload["item_id"]),
        )
    except Exception as exc:
        raise CodeTraceabilityContractError(
            "code_traceability_cursor_invalid",
            details={"reason": "opaque_cursor_invalid"},
        ) from exc


def _error_outcome(error: Exception) -> McpToolOutcome:
    if isinstance(error, CodeTraceabilityContractError):
        projected = error.as_dict()
    elif hasattr(error, "to_error_dict"):
        projected = getattr(error, "to_error_dict")()
    elif isinstance(error, ValidationError):
        for issue in error.errors(include_input=False, include_url=False):
            context = issue.get("ctx")
            cause = context.get("error") if isinstance(context, Mapping) else None
            if isinstance(cause, CodeTraceabilityContractError):
                return _error_outcome(cause)
        projected = {
            "code": "validation_failed",
            "message": "Code Traceability input validation failed.",
            "details": {
                "errors": error.errors(
                    include_context=False,
                    include_input=False,
                    include_url=False,
                ),
            },
            "remediation": [],
        }
    elif isinstance(error, PermissionDeniedError):
        projected = {
            "code": "forbidden",
            "message": "Code Traceability permission denied.",
            "details": {},
            "remediation": [],
        }
    elif isinstance(error, EntityNotFoundError):
        projected = {
            "code": "not_found",
            "message": str(error),
            "details": {},
            "remediation": [],
        }
    else:
        raise TypeError(
            f"code_traceability_mcp_error_unsupported:{type(error).__name__}"
        ) from error

    remediation = list(projected.get("remediation") or [])
    next_action = None
    if remediation:
        first = remediation[0]
        if isinstance(first, Mapping):
            next_action = {
                "rel": str(first.get("action") or "follow_remediation"),
                **(
                    {"tool": str(first["tool"])}
                    if first.get("tool")
                    else {}
                ),
            }
    return McpToolOutcome.error(
        code=str(projected.get("code") or "code_traceability_error"),
        message=str(projected.get("message") or "Code Traceability failed."),
        retryable=str(projected.get("code") or "").endswith("_conflict"),
        next_action=next_action,
        details={
            **dict(projected.get("details") or {}),
            "remediation": remediation,
        },
    )


def _challenge_service(get_settings: Callable[[], object]) -> CodeInvestigationService:
    """Build a stable HMAC policy from an edition-injected secret.

    The existing composed cursor secret is domain-separated before use.  Core
    has no random/source fallback; Community and SaaS inject the stable secret
    through their normal composition/secret-store boundary.
    """

    settings = get_settings()
    configured = getattr(settings, "guideline_policy_cursor_signing_key", None)
    if isinstance(configured, SecretStr):
        raw = configured.get_secret_value()
    elif isinstance(configured, str):
        raw = configured
    else:
        raw = ""
    if len(raw.encode("utf-8")) < 32:
        return CodeInvestigationService()
    derived = hashlib.sha256(
        b"okto-pulse-code-investigation-challenge-v1\x00" + raw.encode("utf-8")
    ).digest()
    policy = HmacCodeInvestigationChallengePolicy(
        keys={"composed-v1": derived},
        active_key_id="composed-v1",
    )
    return CodeInvestigationService(challenge_policy=policy)


def _closed_tool(mcp: object, fn: Callable[..., Any]) -> None:
    @wraps(fn)
    async def guarded(*args: object, **kwargs: object) -> McpToolOutcome:
        try:
            return await fn(*args, **kwargs)
        except Exception as error:
            return _error_outcome(error)

    setattr(guarded, "__mcp_closed_schema__", True)
    getattr(mcp, "tool")()(guarded)


def _closed_input(model: type, values: Mapping[str, object]) -> object:
    """Build a closed Pydantic input from same-named handler arguments only."""

    payload = {name: values[name] for name in model.model_fields if name in values}
    if model is ImplementationTargetCreateInput:
        for field_name in ("spec_links", "evidence_links"):
            if payload.get(field_name) is None:
                payload.pop(field_name, None)
    elif model is ImplementationTargetUpdateInput:
        # The Python handler receives default ``None`` values for omitted MCP
        # parameters.  Removing those defaults preserves PATCH semantics:
        # only fields supplied with a concrete value enter ``model_fields_set``
        # and the service inherits every other field from the current target.
        for field_name in tuple(payload):
            if field_name not in {
                "board_id",
                "card_id",
                "target_id",
                "expected_revision",
                "change_reason",
            } and payload[field_name] is None:
                payload.pop(field_name)
    return model.model_validate(payload)


def register_code_traceability_tools(
    mcp: object,
    *,
    get_board_agent: Callable[[str], Any],
    get_uow: Callable[[], Any],
    get_settings: Callable[[], object],
) -> None:
    """Register the reviewed 19-tool Code Traceability inventory."""

    async def _execute(board_id: str, command: object, use_case: object) -> McpToolOutcome:
        context = await get_board_agent(board_id)
        if context is None:
            return McpToolOutcome.error(
                code="authentication_required",
                message="Authentication failed or board access denied.",
                retryable=False,
                next_action={"rel": "provide_credentials_or_board_access"},
            )
        from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

        actor = MCPAdapterContract.actor(context, board_id=board_id)
        try:
            async with get_uow()(actor=actor) as uow:
                result = await getattr(use_case, "execute")(
                    command,
                    actor=actor,
                    uow=uow,
                )
            return McpToolOutcome.success(_native(result))
        except Exception as exc:
            try:
                return _error_outcome(exc)
            except TypeError:
                raise exc

    def _services() -> tuple[CodeInvestigationService, CodeEvidenceService, ImplementationTargetService]:
        return (
            _challenge_service(get_settings),
            CodeEvidenceService(),
            ImplementationTargetService(),
        )

    async def okto_pulse_start_code_investigation(
        board_id: BoundedId,
        subject_type: CodeTraceabilitySubjectType,
        subject_id: BoundedId,
        expected_subject_version: Annotated[int, Field(ge=1)],
        idempotency_key: BoundedId,
        source_ref: BoundedId | None = None,
    ) -> McpToolOutcome:
        """Start an external-agent preflight request. Read the Code Traceability resource."""
        from okto_pulse.core.application.use_cases.code_traceability import StartCodeInvestigationUseCase

        command = StartCodeInvestigationInput(
            board_id=board_id,
            subject_type=subject_type,
            subject_id=subject_id,
            expected_subject_version=expected_subject_version,
            source_ref=source_ref,
            idempotency_key=idempotency_key,
        )
        investigation, _, _ = _services()
        return await _execute(board_id, command, StartCodeInvestigationUseCase(investigation))

    async def okto_pulse_submit_code_investigation_receipt(
        board_id: BoundedId,
        request_id: BoundedId,
        challenge_token: Annotated[str, Field(min_length=1, max_length=4096)],
        outcome: Annotated[str, Field(pattern="^(accessible|partial|unavailable)$")],
        capabilities: list[str],
        tooling: CodeInvestigationToolingInput,
        observed_at: datetime,
        idempotency_key: BoundedId,
        source_identity_digest: OptionalDigest = None,
        declared_revision: OptionalBoundedText = None,
        workspace_state: ObservedWorkspaceStateSubmission | None = None,
        omission_manifest: list[CodeInvestigationOmissionInput] | None = None,
    ) -> McpToolOutcome:
        """Persist the authenticated external agent's bounded capability/access receipt."""
        from okto_pulse.core.application.use_cases.code_traceability import SubmitCodeInvestigationReceiptUseCase

        command = CodeInvestigationReceiptSubmission(
            board_id=board_id,
            request_id=request_id,
            challenge_token=challenge_token,
            outcome=outcome,
            capabilities=tuple(capabilities),
            source_identity_digest=source_identity_digest,
            declared_revision=declared_revision,
            workspace_state=workspace_state,
            omission_manifest=tuple(omission_manifest or ()),
            tooling=tooling,
            observed_at=observed_at,
            idempotency_key=idempotency_key,
        )
        investigation, _, _ = _services()
        return await _execute(board_id, command, SubmitCodeInvestigationReceiptUseCase(investigation))

    async def okto_pulse_get_code_investigation_receipt(
        board_id: BoundedId,
        receipt_id: BoundedId,
    ) -> McpToolOutcome:
        """Read bounded receipt metadata and computed currentness."""
        from okto_pulse.core.application.use_cases.code_traceability import GetCodeInvestigationReceiptCommand, GetCodeInvestigationReceiptUseCase

        investigation, _, _ = _services()
        return await _execute(
            board_id,
            GetCodeInvestigationReceiptCommand(board_id, receipt_id),
            GetCodeInvestigationReceiptUseCase(investigation),
        )

    def _evidence_command(
        *,
        board_id: str,
        investigation_receipt_id: str,
        parent_type: CodeTraceabilitySubjectType,
        parent_id: str,
        evidence_type: CodeEvidenceType,
        claim: str,
        selector_kind: CodeEvidenceSelectorKind,
        relative_path: str | None,
        language: str | None,
        symbol_kind: str | None,
        qualified_symbol: str | None,
        symbol_signature: str | None,
        line_start: int | None,
        line_end: int | None,
        excerpt: str | None,
        excerpt_sha256: str | None,
        declared_file_blob_sha256: str | None,
        declared_source_content_sha256: str,
        idempotency_key: str,
        supersedes_evidence_id: str | None = None,
        supersession_reason: str | None = None,
        **_ignored: object,
    ) -> CodeEvidenceSubmission:
        payload: dict[str, object] = {
            "board_id": board_id,
            "investigation_receipt_id": investigation_receipt_id,
            "parent_type": parent_type,
            "parent_id": parent_id,
            "evidence_type": evidence_type,
            "claim": claim,
            "selector": CodeEvidenceSelectorInput(
                kind=selector_kind,
                relative_path=relative_path,
                language=language,
                symbol_kind=symbol_kind,
                qualified_symbol=qualified_symbol,
                symbol_signature=symbol_signature,
                line_start=line_start,
                line_end=line_end,
            ),
            "excerpt": excerpt,
            "excerpt_sha256": excerpt_sha256,
            "declared_file_blob_sha256": declared_file_blob_sha256,
            "declared_source_content_sha256": declared_source_content_sha256,
            "idempotency_key": idempotency_key,
        }
        if supersedes_evidence_id is not None:
            payload["supersedes_evidence_id"] = supersedes_evidence_id
            payload["supersession_reason"] = supersession_reason
            return CodeEvidenceSupersessionSubmission.model_validate(payload)
        return CodeEvidenceSubmission.model_validate(payload)

    async def okto_pulse_submit_code_evidence(
        board_id: BoundedId,
        investigation_receipt_id: BoundedId,
        parent_type: CodeTraceabilitySubjectType,
        parent_id: BoundedId,
        evidence_type: CodeEvidenceType,
        claim: BoundedText,
        selector_kind: CodeEvidenceSelectorKind,
        declared_source_content_sha256: Digest,
        idempotency_key: BoundedId,
        relative_path: OptionalBoundedText = None,
        language: OptionalBoundedText = None,
        symbol_kind: OptionalBoundedText = None,
        qualified_symbol: OptionalBoundedText = None,
        symbol_signature: OptionalBoundedText = None,
        line_start: Annotated[int | None, Field(default=None, ge=1)] = None,
        line_end: Annotated[int | None, Field(default=None, ge=1)] = None,
        excerpt: str | None = None,
        excerpt_sha256: OptionalDigest = None,
        declared_file_blob_sha256: OptionalDigest = None,
    ) -> McpToolOutcome:
        """Submit immutable Code Evidence from an accepted agent receipt."""
        from okto_pulse.core.application.use_cases.code_traceability import SubmitCodeEvidenceUseCase

        command = _evidence_command(**locals())
        investigation, evidence, _ = _services()
        return await _execute(board_id, command, SubmitCodeEvidenceUseCase(investigation, evidence))

    async def okto_pulse_get_code_evidence(
        board_id: BoundedId,
        evidence_id: BoundedId,
        profile: CodeTraceabilityProjectionProfile = CodeTraceabilityProjectionProfile.DETAIL,
    ) -> McpToolOutcome:
        """Read one immutable Evidence projection."""
        from okto_pulse.core.application.use_cases.code_traceability import GetCodeEvidenceCommand, GetCodeEvidenceUseCase

        return await _execute(board_id, GetCodeEvidenceCommand(board_id, evidence_id, profile), GetCodeEvidenceUseCase())

    async def okto_pulse_list_code_evidence(
        board_id: BoundedId,
        parent_type: CodeTraceabilitySubjectType | None = None,
        parent_id: BoundedId | None = None,
        status: CodeTraceabilityLifecycleStatus | None = None,
        attestation_state: CodeEvidenceAttestationState | None = None,
        limit: PageLimit = 50,
        cursor: CursorToken | None = None,
        profile: CodeTraceabilityProjectionProfile = CodeTraceabilityProjectionProfile.SUMMARY,
    ) -> McpToolOutcome:
        """List bounded Code Evidence projections with a keyset cursor."""
        from okto_pulse.core.application.use_cases.code_traceability import ListCodeEvidenceCommand, ListCodeEvidenceUseCase

        query = CodeEvidenceQuery(
            board_id=board_id,
            parent_type=parent_type,
            parent_id=parent_id,
            lifecycle_status=status,
            attestation_state=attestation_state,
            limit=limit,
            cursor=_decode_cursor(cursor),
        )
        return await _execute(board_id, ListCodeEvidenceCommand(query, profile), ListCodeEvidenceUseCase())

    async def okto_pulse_supersede_code_evidence(
        board_id: BoundedId,
        investigation_receipt_id: BoundedId,
        parent_type: CodeTraceabilitySubjectType,
        parent_id: BoundedId,
        supersedes_evidence_id: BoundedId,
        supersession_reason: BoundedText,
        evidence_type: CodeEvidenceType,
        claim: BoundedText,
        selector_kind: CodeEvidenceSelectorKind,
        declared_source_content_sha256: Digest,
        idempotency_key: BoundedId,
        relative_path: OptionalBoundedText = None,
        language: OptionalBoundedText = None,
        symbol_kind: OptionalBoundedText = None,
        qualified_symbol: OptionalBoundedText = None,
        symbol_signature: OptionalBoundedText = None,
        line_start: Annotated[int | None, Field(default=None, ge=1)] = None,
        line_end: Annotated[int | None, Field(default=None, ge=1)] = None,
        excerpt: str | None = None,
        excerpt_sha256: OptionalDigest = None,
        declared_file_blob_sha256: OptionalDigest = None,
    ) -> McpToolOutcome:
        """Create an immutable Evidence correction that supersedes its predecessor."""
        from okto_pulse.core.application.use_cases.code_traceability import SupersedeCodeEvidenceUseCase

        command = _evidence_command(**locals())
        investigation, evidence, _ = _services()
        return await _execute(board_id, command, SupersedeCodeEvidenceUseCase(investigation, evidence))

    async def okto_pulse_link_code_evidence(
        board_id: BoundedId,
        spec_id: BoundedId,
        evidence_id: BoundedId,
        entity_type: SpecEntityType,
        entity_id: BoundedId,
        relation_type: CodeEvidenceSpecRelationType,
        rationale: BoundedText,
        expected_spec_version: Annotated[int, Field(ge=1)],
    ) -> McpToolOutcome:
        """Link immutable Evidence to one current normative Spec entity."""
        from okto_pulse.core.application.use_cases.code_traceability import LinkCodeEvidenceToSpecUseCase

        command = _closed_input(CodeEvidenceSpecLinkInput, locals())
        return await _execute(board_id, command, LinkCodeEvidenceToSpecUseCase(CodeEvidenceService()))

    async def okto_pulse_unlink_code_evidence(
        board_id: BoundedId,
        spec_id: BoundedId,
        link_id: BoundedId,
        expected_spec_version: Annotated[int, Field(ge=1)],
    ) -> McpToolOutcome:
        """Remove one version-fenced Evidence-to-Spec link."""
        from okto_pulse.core.application.use_cases.code_traceability import UnlinkCodeEvidenceFromSpecUseCase

        command = _closed_input(CodeEvidenceSpecUnlinkInput, locals())
        return await _execute(board_id, command, UnlinkCodeEvidenceFromSpecUseCase(CodeEvidenceService()))

    async def okto_pulse_set_code_evidence_disposition(
        board_id: BoundedId,
        spec_id: BoundedId,
        evidence_id: BoundedId,
        disposition: CodeEvidenceDispositionKind,
        justification: BoundedText,
        expected_spec_version: Annotated[int, Field(ge=1)],
    ) -> McpToolOutcome:
        """Record a version-fenced disposition for inherited Evidence."""
        from okto_pulse.core.application.use_cases.code_traceability import SetCodeEvidenceDispositionUseCase

        command = _closed_input(CodeEvidenceDispositionInput, locals())
        return await _execute(board_id, command, SetCodeEvidenceDispositionUseCase(CodeEvidenceService()))

    async def okto_pulse_create_implementation_target(
        board_id: BoundedId,
        card_id: BoundedId,
        source_ref: BoundedId,
        selector_kind: ImplementationTargetSelectorKind,
        role: ImplementationTargetRole,
        intent: BoundedText,
        expected_spec_version: Annotated[int, Field(ge=1)],
        required: bool = True,
        relative_path_hint: OptionalBoundedText = None,
        language: OptionalBoundedText = None,
        symbol_kind: OptionalBoundedText = None,
        qualified_symbol: OptionalBoundedText = None,
        symbol_signature: OptionalBoundedText = None,
        baseline_evidence_id: BoundedId | None = None,
        spec_links: list[ImplementationTargetSpecLinkInput] | None = None,
        evidence_links: list[ImplementationTargetEvidenceLinkInput] | None = None,
    ) -> McpToolOutcome:
        """Create semantic implementation intent; Pulse does not discover source targets."""
        from okto_pulse.core.application.use_cases.code_traceability import CreateImplementationTargetUseCase

        command = _closed_input(ImplementationTargetCreateInput, locals())
        investigation, _, targets = _services()
        return await _execute(board_id, command, CreateImplementationTargetUseCase(investigation, targets))

    async def okto_pulse_update_implementation_target(
        board_id: BoundedId,
        card_id: BoundedId,
        target_id: BoundedId,
        expected_revision: Annotated[int, Field(ge=1)],
        change_reason: BoundedText,
        selector_kind: ImplementationTargetSelectorKind | None = None,
        relative_path_hint: OptionalBoundedText = None,
        language: OptionalBoundedText = None,
        symbol_kind: OptionalBoundedText = None,
        qualified_symbol: OptionalBoundedText = None,
        symbol_signature: OptionalBoundedText = None,
        role: ImplementationTargetRole | None = None,
        intent: OptionalBoundedText = None,
        required: bool | None = None,
        baseline_evidence_id: BoundedId | None = None,
        lifecycle_status: CodeTraceabilityLifecycleStatus | None = None,
        spec_links: list[ImplementationTargetSpecLinkInput] | None = None,
        evidence_links: list[ImplementationTargetEvidenceLinkInput] | None = None,
    ) -> McpToolOutcome:
        """Update one semantic Target under optimistic revision control."""
        from okto_pulse.core.application.use_cases.code_traceability import UpdateImplementationTargetUseCase

        command = _closed_input(ImplementationTargetUpdateInput, locals())
        return await _execute(board_id, command, UpdateImplementationTargetUseCase(ImplementationTargetService()))

    async def okto_pulse_list_implementation_targets(
        board_id: BoundedId,
        card_id: BoundedId | None = None,
        source_ref: BoundedId | None = None,
        lifecycle_status: CodeTraceabilityLifecycleStatus | None = None,
        role: ImplementationTargetRole | None = None,
        limit: PageLimit = 50,
        cursor: CursorToken | None = None,
    ) -> McpToolOutcome:
        """List semantic Targets without source discovery."""
        from okto_pulse.core.application.use_cases.code_traceability import ListImplementationTargetsUseCase

        command = ImplementationTargetQuery(
            board_id=board_id,
            card_id=card_id,
            source_ref=source_ref,
            lifecycle_status=lifecycle_status,
            role=role,
            limit=limit,
            cursor=_decode_cursor(cursor),
        )
        return await _execute(board_id, command, ListImplementationTargetsUseCase())

    async def okto_pulse_submit_implementation_target_resolution(
        board_id: BoundedId,
        card_id: BoundedId,
        target_id: BoundedId,
        investigation_receipt_id: BoundedId,
        state: ImplementationTargetResolutionState,
        tooling: CodeInvestigationToolingInput,
        agent_observed_at: datetime,
        idempotency_key: BoundedId,
        resolved_relative_path: OptionalBoundedText = None,
        resolved_language: OptionalBoundedText = None,
        resolved_symbol_kind: OptionalBoundedText = None,
        resolved_qualified_symbol: OptionalBoundedText = None,
        resolved_symbol_signature: OptionalBoundedText = None,
        resolved_line_start: Annotated[int | None, Field(default=None, ge=1)] = None,
        resolved_line_end: Annotated[int | None, Field(default=None, ge=1)] = None,
        symbol_fingerprint: OptionalDigest = None,
        declared_file_blob_sha256: OptionalDigest = None,
        confidence: Annotated[float | None, Field(default=None, ge=0, le=1)] = None,
        reason_code: OptionalBoundedText = None,
        candidates: list[ResolutionCandidateInput] | None = None,
    ) -> McpToolOutcome:
        """Submit an agent resolution bound to the current Target and source receipt."""
        from okto_pulse.core.application.use_cases.code_traceability import SubmitImplementationTargetResolutionUseCase

        values = locals()
        payload = {
            name: values[name]
            for name in ImplementationTargetResolutionSubmission.model_fields
            if name in values
        }
        payload["candidates"] = tuple(candidates or ())
        command = ImplementationTargetResolutionSubmission.model_validate(payload)
        investigation, _, targets = _services()
        return await _execute(board_id, command, SubmitImplementationTargetResolutionUseCase(investigation, targets))

    async def okto_pulse_get_implementation_overlaps(
        board_id: BoundedId,
        card_id: BoundedId,
        include_informational: bool = True,
    ) -> McpToolOutcome:
        """Read overlaps derived only from persisted current Target resolutions."""
        from okto_pulse.core.application.use_cases.code_traceability import GetImplementationOverlapsUseCase

        command = TargetOverlapQuery(
            board_id=board_id,
            card_id=card_id,
            include_informational=include_informational,
        )
        return await _execute(board_id, command, GetImplementationOverlapsUseCase())

    async def okto_pulse_acknowledge_implementation_overlap(
        board_id: BoundedId,
        card_id: BoundedId,
        target_a_id: BoundedId,
        target_b_id: BoundedId,
        resolution_a_id: BoundedId,
        resolution_b_id: BoundedId,
        disposition: TargetOverlapDisposition,
        justification: BoundedText,
    ) -> McpToolOutcome:
        """Acknowledge one exact current overlap pair; changed resolutions stale it."""
        from okto_pulse.core.application.use_cases.code_traceability import AcknowledgeImplementationOverlapUseCase

        command = _closed_input(TargetOverlapAcknowledgementInput, locals())
        return await _execute(
            board_id,
            command,
            AcknowledgeImplementationOverlapUseCase(CodeOverlapService()),
        )

    async def okto_pulse_submit_implementation_target_execution_receipt(
        board_id: BoundedId,
        card_id: BoundedId,
        target_id: BoundedId,
        result_investigation_receipt_id: BoundedId,
        disposition: ImplementationTargetExecutionDisposition,
        justification: BoundedText,
        idempotency_key: BoundedId,
        actual_relative_path: OptionalBoundedText = None,
        actual_qualified_symbol: OptionalBoundedText = None,
        replacement_target_id: BoundedId | None = None,
    ) -> McpToolOutcome:
        """Submit an authenticated agent Execution Disposition for one Target."""
        from okto_pulse.core.application.use_cases.code_traceability import SubmitImplementationTargetExecutionUseCase

        command = _closed_input(ImplementationTargetExecutionSubmission, locals())
        investigation, _, targets = _services()
        return await _execute(board_id, command, SubmitImplementationTargetExecutionUseCase(investigation, targets))

    async def okto_pulse_mark_code_traceability_not_applicable(
        board_id: BoundedId,
        entity_type: CodeTraceabilityWaiverEntityType,
        entity_id: BoundedId,
        scope: CodeTraceabilityWaiverScope,
        reason_code: CodeTraceabilityWaiverReason,
        justification: BoundedText,
    ) -> McpToolOutcome:
        """Record an explicit human waiver; never fabricate an agent attestation."""
        from okto_pulse.core.application.use_cases.code_traceability import MarkCodeTraceabilityNotApplicableUseCase

        command = _closed_input(CodeTraceabilityWaiverInput, locals())
        return await _execute(board_id, command, MarkCodeTraceabilityNotApplicableUseCase())

    async def okto_pulse_clear_code_traceability_not_applicable(
        board_id: BoundedId,
        waiver_id: BoundedId,
    ) -> McpToolOutcome:
        """Clear one active Code Traceability waiver with an audit record."""
        from okto_pulse.core.application.use_cases.code_traceability import ClearCodeTraceabilityNotApplicableUseCase

        command = _closed_input(CodeTraceabilityWaiverClearInput, locals())
        return await _execute(board_id, command, ClearCodeTraceabilityNotApplicableUseCase())

    for handler in (
        okto_pulse_start_code_investigation,
        okto_pulse_submit_code_investigation_receipt,
        okto_pulse_get_code_investigation_receipt,
        okto_pulse_submit_code_evidence,
        okto_pulse_get_code_evidence,
        okto_pulse_list_code_evidence,
        okto_pulse_supersede_code_evidence,
        okto_pulse_link_code_evidence,
        okto_pulse_unlink_code_evidence,
        okto_pulse_set_code_evidence_disposition,
        okto_pulse_create_implementation_target,
        okto_pulse_update_implementation_target,
        okto_pulse_list_implementation_targets,
        okto_pulse_submit_implementation_target_resolution,
        okto_pulse_get_implementation_overlaps,
        okto_pulse_acknowledge_implementation_overlap,
        okto_pulse_submit_implementation_target_execution_receipt,
        okto_pulse_mark_code_traceability_not_applicable,
        okto_pulse_clear_code_traceability_not_applicable,
    ):
        _closed_tool(mcp, handler)


__all__ = [
    "CODE_TRACEABILITY_RESOURCE_URI",
    "register_code_traceability_tools",
]
