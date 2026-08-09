"""Board-scoped MCP surface for versioned semantic guideline governance.

The handlers in this module are deliberately thin.  They publish closed,
bounded schemas, resolve the authenticated board actor, decode/encode the
shared HMAC keyset cursor, and delegate every semantic decision to the B13
application use cases.  Authoritative IDs, timestamps, actor identity and
digests are never accepted from a client.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Callable, Literal, Mapping
import uuid

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    ASSESSMENTS_READ,
    ASSESSMENTS_RECORD,
    IMPACT_PREVIEW,
    METRICS_AUTHOR,
    REVISIONS_CREATE,
    REVISIONS_READ,
    REVISIONS_RETIRE,
    WAIVER_READ,
    WAIVER_REQUEST,
    WAIVER_REVALIDATE,
    WAIVER_REVIEW,
    WAIVER_REVOKE,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH,
    GUIDELINE_TITLE_MAX_LENGTH,
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_FINDING_ID_MAX_LENGTH,
    POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
    POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH,
    POLICY_METRIC_CODE_MAX_LENGTH,
    POLICY_METRIC_ID_MAX_LENGTH,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
    POLICY_SUBJECT_ID_MAX_LENGTH,
    POLICY_WAIVER_ID_MAX_LENGTH,
)
from okto_pulse.core.domain.guideline_lifecycle import GuidelineVersionBump
from okto_pulse.core.domain.guideline_semantic_v2 import (
    SEMANTIC_PINPOINT_DETAIL_MAX_LENGTH,
    SEMANTIC_PINPOINT_KEY_MAX_LENGTH,
    SEMANTIC_PINPOINT_REMEDIATION_MAX_LENGTH,
    SEMANTIC_PINPOINT_TITLE_MAX_LENGTH,
)
from okto_pulse.core.mcp.outcome import McpToolOutcome


POLICY_PAGE_LIMIT_DEFAULT = 50
POLICY_PAGE_LIMIT_MAX = 200
SEMANTIC_GUIDELINE_RESOURCE_URI = "okto-pulse://reference/policy-compliance"
# Transitional explicit-import alias; the stable resource URI is intentionally
# retained while policy/v1 naming leaves the active Python surface.
POLICY_COMPLIANCE_RESOURCE_URI = SEMANTIC_GUIDELINE_RESOURCE_URI
POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION = {
    "list_revisions": (REVISIONS_READ,),
    "get_revision": (REVISIONS_READ,),
    "create_revision": (REVISIONS_CREATE,),
    "retire_guideline": (REVISIONS_RETIRE,),
    "preview_impact": (IMPACT_PREVIEW,),
    "get_impact": (IMPACT_PREVIEW,),
    "list_impact_items": (IMPACT_PREVIEW,),
    "adopt_revision": (ADOPTION_MANAGE,),
    "record_assessment": (ASSESSMENTS_RECORD,),
    "record_assessment_v2": (ASSESSMENTS_RECORD,),
    "list_assessments": (ASSESSMENTS_READ,),
    "get_assessment": (ASSESSMENTS_READ,),
    "get_current_assessment": (ASSESSMENTS_READ,),
    "list_findings": (ASSESSMENTS_READ,),
    "list_waivers": (WAIVER_READ,),
    "get_waiver": (WAIVER_READ,),
    "list_waiver_events": (WAIVER_READ,),
    "request_waiver": (WAIVER_REQUEST,),
    "review_waiver": (WAIVER_REVIEW,),
    "revoke_waiver": (WAIVER_REVOKE,),
    "revalidate_waiver": (WAIVER_REVALIDATE,),
}

BoardId = Annotated[
    str,
    Field(min_length=1, max_length=POLICY_BOARD_ID_MAX_LENGTH),
]
GuidelineId = Annotated[
    str,
    Field(min_length=1, max_length=GUIDELINE_ID_MAX_LENGTH),
]
RevisionId = Annotated[
    str,
    Field(min_length=1, max_length=GUIDELINE_REVISION_ID_MAX_LENGTH),
]
ImpactReceiptId = Annotated[
    str,
    Field(min_length=1, max_length=POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH),
]
ComplianceReceiptId = Annotated[
    str,
    Field(min_length=1, max_length=POLICY_RECEIPT_ID_MAX_LENGTH),
]
WaiverId = Annotated[
    str,
    Field(min_length=1, max_length=POLICY_WAIVER_ID_MAX_LENGTH),
]
IdempotencyKey = Annotated[
    str,
    Field(min_length=1, max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
]
CursorToken = Annotated[str, Field(min_length=1, max_length=8192)]
PageLimit = Annotated[int, Field(ge=1, le=POLICY_PAGE_LIMIT_MAX)]
PositiveRevision = Annotated[int, Field(ge=1, le=POLICY_SQL_INTEGER_MAX)]
PolicyProjectionValue = Literal["summary", "detail"]
SemanticGuidelineProjectionValue = Literal["summary", "detail", "full"]
PolicyEntityTypeValue = Literal[
    "ideation",
    "refinement",
    "spec",
    "sprint",
    "card",
    "test_scenario",
]
GuidelineEnforcementValue = Literal["advisory", "blocking"]
GuidelineLifecycleValue = Literal["retired", "superseded"]
GuidelineImpactItemKindValue = Literal[
    "binding",
    "target",
    "artifact",
    "waiver",
]
PolicyCurrentnessValue = Literal["current", "stale"]
SemanticAssessmentOutcomeValue = Literal[
    "passed",
    "metric_threshold_failed",
]
SemanticMetricOutcomeValue = Literal["pass", "fail"]
PolicyWaiverStatusValue = Literal[
    "requested",
    "approved",
    "rejected",
    "revoked",
    "expired",
]
PolicyWaiverDecisionValue = Literal["approve", "reject"]
_SERVER_ID_NAMESPACE = uuid.UUID("dd3e22d4-c700-5e18-a5f6-ce7fa2ff27ad")


class _ClosedInput(BaseModel):
    """Base for nested MCP values with a closed JSON-object contract."""

    model_config = ConfigDict(extra="forbid")


class GuidelineMetricInput(_ClosedInput):
    metric_id: str = Field(min_length=1, max_length=POLICY_METRIC_ID_MAX_LENGTH)
    code: str = Field(min_length=1, max_length=POLICY_METRIC_CODE_MAX_LENGTH)
    title: str = Field(min_length=1, max_length=500)
    description: str = Field(min_length=1)
    evaluation_rubric: str = Field(min_length=1)
    target_entity_types: list[PolicyEntityTypeValue] = Field(min_length=1)
    direction: Literal["minimum", "maximum"]
    default_threshold: int = Field(ge=0, le=100)


class GuidelineRevisionPatchInput(_ClosedInput):
    title: str | None = Field(
        default=None,
        min_length=1,
        max_length=GUIDELINE_TITLE_MAX_LENGTH,
    )
    content: str | None = Field(default=None, min_length=1)
    tags: list[str] | None = None
    metrics: list[GuidelineMetricInput] | None = None

    @model_validator(mode="after")
    def require_change(self) -> GuidelineRevisionPatchInput:
        if all(
            value is None
            for value in (self.title, self.content, self.tags, self.metrics)
        ):
            raise ValueError("guideline_revision_patch_empty")
        return self


class SemanticEvidenceRefInput(_ClosedInput):
    source_type: str = Field(min_length=1, max_length=100)
    source_id: str = Field(min_length=1, max_length=500)
    source_version: int = Field(ge=1, le=POLICY_SQL_INTEGER_MAX)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class SemanticPinpointInput(_ClosedInput):
    anchor_type: Literal[
        "whole_artifact",
        "field",
        "structured_child",
        "qa",
    ]
    anchor_ref: str | None = Field(default=None, min_length=1)
    excerpt_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )

    @model_validator(mode="after")
    def validate_anchor_shape(self) -> SemanticPinpointInput:
        if self.anchor_type == "whole_artifact" and self.anchor_ref is not None:
            raise ValueError("finding_whole_artifact_ref_forbidden")
        if self.anchor_type != "whole_artifact" and self.anchor_ref is None:
            raise ValueError("finding_anchor_ref_required")
        return self


class SemanticMetricAssessmentInput(_ClosedInput):
    metric_id: str = Field(min_length=1, max_length=POLICY_METRIC_ID_MAX_LENGTH)
    score: int = Field(ge=0, le=100)
    rationale: str = Field(min_length=1, max_length=20_000)
    evidence_refs: list[SemanticEvidenceRefInput] = Field(
        min_length=1,
        max_length=200,
    )
    pinpoints: list[SemanticPinpointInput] = Field(
        min_length=1,
        max_length=200,
    )


class SemanticAnchorV2Input(_ClosedInput):
    anchor_type: Literal[
        "whole_artifact",
        "field",
        "structured_child",
        "qa",
    ]
    anchor_ref: str | None = Field(default=None, min_length=1, max_length=500)
    excerpt_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_anchor_shape(self) -> "SemanticAnchorV2Input":
        if self.anchor_type == "whole_artifact" and self.anchor_ref is not None:
            raise ValueError("finding_whole_artifact_ref_forbidden")
        if self.anchor_type != "whole_artifact" and self.anchor_ref is None:
            raise ValueError("finding_anchor_ref_required")
        return self


class SemanticPinpointV2Input(_ClosedInput):
    contract_version: Literal["v2"]
    pinpoint_key: str = Field(min_length=1, max_length=SEMANTIC_PINPOINT_KEY_MAX_LENGTH)
    kind: Literal["evidence", "issue"]
    title: str = Field(min_length=1, max_length=SEMANTIC_PINPOINT_TITLE_MAX_LENGTH)
    detail: str = Field(min_length=1, max_length=SEMANTIC_PINPOINT_DETAIL_MAX_LENGTH)
    severity: Literal["low", "medium", "high", "critical"] | None = None
    remediation: str | None = Field(
        default=None,
        min_length=1,
        max_length=SEMANTIC_PINPOINT_REMEDIATION_MAX_LENGTH,
    )
    anchor: SemanticAnchorV2Input

    @model_validator(mode="after")
    def require_issue_severity(self) -> "SemanticPinpointV2Input":
        if self.kind == "issue" and self.severity is None:
            raise ValueError("semantic_pinpoint_v2_issue_severity_required")
        return self


class SemanticMetricAssessmentV2Input(_ClosedInput):
    contract_version: Literal["v2"]
    metric_id: str = Field(min_length=1, max_length=POLICY_METRIC_ID_MAX_LENGTH)
    score: int = Field(ge=0, le=100)
    rationale: str = Field(min_length=1, max_length=20_000)
    evidence_refs: list[SemanticEvidenceRefInput] = Field(
        min_length=1,
        max_length=200,
    )
    pinpoints: list[SemanticPinpointV2Input] = Field(
        min_length=1,
        max_length=200,
    )


def _server_id(kind: str, board_id: str, idempotency_key: str) -> str:
    return str(
        uuid.uuid5(
            _SERVER_ID_NAMESPACE,
            f"{kind}:{board_id}:{idempotency_key}",
        )
    )


def _native(value: object) -> object:
    """Project immutable Core values to JSON-native values without stringifying."""

    # IntEnum is also an ``int``; project the public semantic literal before
    # the primitive fast path so REST and MCP expose the same closed contract.
    if isinstance(value, GuidelineVersionBump):
        return value.name.lower()
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {
            field.name: _native(getattr(value, field.name))
            for field in fields(value)
            if getattr(value, field.name) is not None
        }
    if isinstance(value, Mapping):
        return {str(key): _native(item) for key, item in value.items()}
    if isinstance(value, tuple | list | set | frozenset):
        return [_native(item) for item in value]
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    raise TypeError(f"policy_mcp_projection_unsupported:{type(value).__name__}")


def _page_payload(page: object, codec: object) -> dict[str, object]:
    next_cursor = getattr(page, "next_cursor", None)
    encode = getattr(codec, "encode")
    payload: dict[str, object] = {
        "items": _native(getattr(page, "items")),
        "limit": getattr(page, "limit"),
        "has_more": getattr(page, "has_more"),
        "next_cursor": encode(next_cursor) if next_cursor is not None else None,
    }
    projection = getattr(page, "projection", None)
    if projection is not None:
        payload["projection"] = _native(projection)
    return payload


def _result_payload(result: object, codec: object | None) -> object:
    from okto_pulse.core.application.use_cases.semantic_guideline_v2 import (
        SealSemanticGuidelineAssessmentV2Result,
        semantic_assessment_v2_write_projection,
    )

    if isinstance(result, SealSemanticGuidelineAssessmentV2Result):
        return semantic_assessment_v2_write_projection(result)
    page = getattr(result, "page", None)
    if page is not None:
        if codec is None:
            raise RuntimeError("guideline_policy_cursor_codec_missing")
        return _page_payload(page, codec)
    return _native(result)


def _error_outcome(error: Exception) -> McpToolOutcome:
    from okto_pulse.core.inbound.guideline_policy_error import (
        project_guideline_policy_error,
    )

    projected = project_guideline_policy_error(error)
    next_action = projected.get("next_action")
    details = {
        "category": projected["category"],
        "status_category": projected["status_category"],
        "http_status": projected["http_status"],
        **dict(projected.get("details") or {}),
    }
    return McpToolOutcome.error(
        code=str(projected["code"]),
        message=str(projected["message"]),
        retryable=bool(projected["retryable"]),
        next_action=(
            {"rel": next_action}
            if isinstance(next_action, str) and next_action
            else None
        ),
        details=details,
    )


def _authentication_error() -> McpToolOutcome:
    return McpToolOutcome.error(
        code="authentication_required",
        message="Authentication failed or board access denied.",
        retryable=False,
        next_action={"rel": "provide_credentials_or_board_access"},
    )


def _domain_metric(payload: GuidelineMetricInput) -> object:
    from okto_pulse.core.domain.guideline_policy import (
        GuidelineMetric,
        GuidelineMetricDirection,
        PolicyEntityType,
    )

    return GuidelineMetric(
        metric_id=payload.metric_id,
        code=payload.code,
        title=payload.title,
        description=payload.description,
        evaluation_rubric=payload.evaluation_rubric,
        target_entity_types=tuple(
            PolicyEntityType(value) for value in payload.target_entity_types
        ),
        direction=GuidelineMetricDirection(payload.direction),
        default_threshold=payload.default_threshold,
    )


def _domain_evidence_refs(
    payloads: list[SemanticEvidenceRefInput],
) -> tuple[object, ...]:
    from okto_pulse.core.domain.quality_assessment import EvidenceRef

    return tuple(
        EvidenceRef(
            source_type=item.source_type,
            source_id=item.source_id,
            source_version=item.source_version,
            content_hash=item.content_hash,
        )
        for item in payloads
    )


def _domain_patch(payload: GuidelineRevisionPatchInput) -> object:
    from okto_pulse.core.domain.guideline_lifecycle import GuidelineRevisionPatch

    return GuidelineRevisionPatch(
        title=payload.title,
        content=payload.content,
        tags=(tuple(payload.tags) if payload.tags is not None else None),
        metrics=(
            tuple(_domain_metric(item) for item in payload.metrics)
            if payload.metrics is not None
            else None
        ),
    )


def _closed_tool(mcp: object, fn: Callable[..., Any]) -> None:
    """Register one handler with an explicitly closed root argument object."""

    setattr(fn, "__mcp_closed_schema__", True)
    getattr(mcp, "tool")()(fn)


def authorize_policy_governance_mcp(
    actor: object,
    *,
    operation: str,
    capabilities: tuple[str, ...],
) -> None:
    """Apply the policy capability fence and emit only bounded decisions."""

    from okto_pulse.core.application.use_cases import (
        require_policy_governance_capabilities,
    )
    from okto_pulse.core.services.governance_observability import (
        METRIC_POLICY_GOVERNANCE_AUTHORIZATION_DECISION,
        emit_governance_metric,
    )

    for capability in capabilities:
        try:
            require_policy_governance_capabilities(actor, capability)
        except Exception:
            emit_governance_metric(
                {
                    "metric_name": (METRIC_POLICY_GOVERNANCE_AUTHORIZATION_DECISION),
                    "surface": "mcp",
                    "operation": operation,
                    "capability": capability,
                    "outcome": "deny",
                }
            )
            raise
        emit_governance_metric(
            {
                "metric_name": METRIC_POLICY_GOVERNANCE_AUTHORIZATION_DECISION,
                "surface": "mcp",
                "operation": operation,
                "capability": capability,
                "outcome": "allow",
            }
        )


def register_policy_governance_tools(
    mcp: object,
    *,
    get_board_agent: Callable[[str], Any],
    get_uow: Callable[[], Any],
    get_settings: Callable[[], object],
) -> None:
    """Register the semantic-guideline governance MCP inventory."""

    async def _execute(
        board_id: str,
        operation: str,
        command: object | None,
        use_case: object,
        *,
        build_command: (Callable[[object | None, object], object] | None) = None,
        extra_capabilities: tuple[str, ...] = (),
        paginated: bool = False,
    ) -> McpToolOutcome:
        semantic_contract_version = {
            "record_assessment": "v1",
            "record_assessment_v2": "v2",
        }.get(operation)
        context = await get_board_agent(board_id)
        if context is None:
            if semantic_contract_version is not None:
                from okto_pulse.core.services.governance_observability import (
                    emit_semantic_assessment_write_metric,
                )

                emit_semantic_assessment_write_metric(
                    surface="mcp",
                    contract_version=semantic_contract_version,
                    outcome="error",
                    reason_code="authentication_required",
                )
            return _authentication_error()

        from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

        actor = MCPAdapterContract.actor(context, board_id=board_id)
        codec = None
        try:
            required = (
                *POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION[operation],
                *extra_capabilities,
            )
            authorize_policy_governance_mcp(
                actor,
                operation=operation,
                capabilities=required,
            )
            if paginated:
                from okto_pulse.core.inbound.guideline_policy_cursor import (
                    policy_cursor_codec_from_settings,
                )

                codec = policy_cursor_codec_from_settings(get_settings())
            if build_command is not None:
                command = build_command(codec, actor)
            if command is None:
                raise TypeError("policy_governance_command_required")
            async with get_uow()(actor=actor) as uow:
                result = await use_case.execute(command, actor=actor, uow=uow)
            if semantic_contract_version is not None:
                from okto_pulse.core.services.governance_observability import (
                    emit_semantic_assessment_write_metric,
                )

                emit_semantic_assessment_write_metric(
                    surface="mcp",
                    contract_version=semantic_contract_version,
                    outcome="success",
                )
            return McpToolOutcome.success(_result_payload(result, codec))
        except Exception as exc:
            try:
                outcome = _error_outcome(exc)
                if semantic_contract_version is not None:
                    from okto_pulse.core.services.governance_observability import (
                        emit_semantic_assessment_write_metric,
                    )

                    emit_semantic_assessment_write_metric(
                        surface="mcp",
                        contract_version=semantic_contract_version,
                        outcome="error",
                        reason_code=outcome.code,
                        capability_state=(
                            str(outcome.details["capability_state"])
                            if "capability_state" in outcome.details
                            else None
                        ),
                    )
                return outcome
            except TypeError:
                # Programming errors and unsupported exception classes are not
                # converted into a misleading successful domain response.
                raise exc

    def _decode(
        codec: object | None,
        cursor: str | None,
        *,
        kind: str,
    ) -> object | None:
        if cursor is not None and codec is None:
            raise TypeError("policy_governance_cursor_codec_required")
        return (
            getattr(codec, "decode")(cursor, expected_kind=kind)
            if cursor is not None
            else None
        )

    async def okto_pulse_list_guideline_revisions(
        board_id: BoardId,
        guideline_id: GuidelineId,
        limit: PageLimit = POLICY_PAGE_LIMIT_DEFAULT,
        cursor: CursorToken | None = None,
        profile: PolicyProjectionValue = "summary",
    ) -> McpToolOutcome:
        """List immutable guideline revisions with an opaque keyset cursor.

        Read ``okto-pulse://reference/policy-compliance`` before use.
        """

        from okto_pulse.core.application.use_cases import (
            ListGuidelineRevisionsCommand,
            ListGuidelineRevisionsUseCase,
        )
        from okto_pulse.core.domain.guideline_compliance import PolicyProjection

        def build_command(
            codec: object | None,
            _actor: object,
        ) -> object:
            return ListGuidelineRevisionsCommand(
                board_id=board_id,
                guideline_id=guideline_id,
                limit=limit,
                cursor=_decode(codec, cursor, kind="revision"),
                projection=PolicyProjection(profile),
            )

        return await _execute(
            board_id,
            "list_revisions",
            None,
            ListGuidelineRevisionsUseCase(),
            build_command=build_command,
            paginated=True,
        )

    async def okto_pulse_get_guideline_revision(
        board_id: BoardId,
        guideline_id: GuidelineId,
        revision_id: RevisionId,
    ) -> McpToolOutcome:
        """Read one immutable revision and its current authority context."""

        from okto_pulse.core.application.use_cases import (
            GetGuidelineRevisionCommand,
            GetGuidelineRevisionUseCase,
        )

        try:
            command = GetGuidelineRevisionCommand(
                board_id=board_id,
                guideline_id=guideline_id,
                revision_id=revision_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "get_revision",
            command,
            GetGuidelineRevisionUseCase(),
        )

    async def okto_pulse_create_guideline_revision(
        board_id: BoardId,
        guideline_id: GuidelineId,
        idempotency_key: IdempotencyKey,
        patch: GuidelineRevisionPatchInput,
        declared_semantic_version: Annotated[
            str | None,
            Field(
                default=None,
                min_length=5,
                max_length=GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH,
            ),
        ] = None,
    ) -> McpToolOutcome:
        """Create a semantic revision; server owns revision identity and time."""

        from okto_pulse.core.application.use_cases import (
            CreateGuidelineRevisionCommand,
            CreateGuidelineRevisionUseCase,
        )

        try:
            command = CreateGuidelineRevisionCommand(
                board_id=board_id,
                guideline_id=guideline_id,
                patch=_domain_patch(patch),
                idempotency_key=idempotency_key,
                declared_semantic_version=declared_semantic_version,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "create_revision",
            command,
            CreateGuidelineRevisionUseCase(),
            extra_capabilities=((METRICS_AUTHOR,) if patch.metrics is not None else ()),
        )

    async def okto_pulse_retire_guideline(
        board_id: BoardId,
        guideline_id: GuidelineId,
        status: GuidelineLifecycleValue,
        reason: Annotated[str, Field(min_length=1)],
        idempotency_key: IdempotencyKey,
        superseded_by_guideline_id: GuidelineId | None = None,
    ) -> McpToolOutcome:
        """Retire or supersede a guideline with a server-issued tombstone."""

        from okto_pulse.core.application.use_cases import (
            RetireGuidelineCommand,
            RetireGuidelineUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import (
            GuidelineLifecycleStatus,
        )

        try:
            if status == "superseded" and superseded_by_guideline_id is None:
                raise ValueError("guideline_retirement_successor_required")
            if status == "retired" and superseded_by_guideline_id is not None:
                raise ValueError("guideline_retirement_successor_unexpected")
            command = RetireGuidelineCommand(
                board_id=board_id,
                guideline_id=guideline_id,
                retirement_id=_server_id(
                    "guideline-retirement",
                    board_id,
                    idempotency_key,
                ),
                status=GuidelineLifecycleStatus(status),
                reason=reason,
                idempotency_key=idempotency_key,
                superseded_by_guideline_id=superseded_by_guideline_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "retire_guideline",
            command,
            RetireGuidelineUseCase(),
        )

    async def okto_pulse_preview_guideline_impact(
        board_id: BoardId,
        guideline_id: GuidelineId,
        proposed_priority: Annotated[
            int,
            Field(ge=0, le=POLICY_SQL_INTEGER_MAX),
        ],
        proposed_enforcement: GuidelineEnforcementValue,
        proposed_minimum_confidence: Annotated[int, Field(ge=0, le=100)],
        idempotency_key: IdempotencyKey,
        proposed_metric_threshold_overrides: dict[str, int] | None = None,
        to_revision_id: RevisionId | None = None,
    ) -> McpToolOutcome:
        """Create immutable impact evidence before adoption."""

        from okto_pulse.core.application.use_cases import (
            PreviewGuidelineImpactCommand,
            PreviewGuidelineImpactUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import GuidelineEnforcement

        try:
            command = PreviewGuidelineImpactCommand(
                board_id=board_id,
                guideline_id=guideline_id,
                proposed_priority=proposed_priority,
                proposed_enforcement=GuidelineEnforcement(proposed_enforcement),
                proposed_minimum_confidence=proposed_minimum_confidence,
                proposed_metric_threshold_overrides=(
                    proposed_metric_threshold_overrides or {}
                ),
                idempotency_key=idempotency_key,
                to_revision_id=to_revision_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "preview_impact",
            command,
            PreviewGuidelineImpactUseCase(),
        )

    async def okto_pulse_get_guideline_impact(
        board_id: BoardId,
        guideline_id: GuidelineId,
        impact_receipt_id: ImpactReceiptId,
    ) -> McpToolOutcome:
        """Read one immutable guideline impact receipt."""

        from okto_pulse.core.application.use_cases import (
            GetGuidelineImpactCommand,
            GetGuidelineImpactUseCase,
        )

        try:
            command = GetGuidelineImpactCommand(
                board_id=board_id,
                guideline_id=guideline_id,
                impact_receipt_id=impact_receipt_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "get_impact",
            command,
            GetGuidelineImpactUseCase(),
        )

    async def okto_pulse_list_guideline_impact_items(
        board_id: BoardId,
        guideline_id: GuidelineId,
        impact_receipt_id: ImpactReceiptId,
        limit: PageLimit = POLICY_PAGE_LIMIT_DEFAULT,
        cursor: CursorToken | None = None,
        entity_type: PolicyEntityTypeValue | None = None,
        item_kind: GuidelineImpactItemKindValue | None = None,
        profile: PolicyProjectionValue = "summary",
    ) -> McpToolOutcome:
        """Page the exact targets and artifacts captured by an impact receipt."""

        from okto_pulse.core.application.use_cases import (
            ListGuidelineImpactItemsCommand,
            ListGuidelineImpactItemsUseCase,
        )
        from okto_pulse.core.domain.guideline_compliance import PolicyProjection
        from okto_pulse.core.domain.guideline_policy import (
            GuidelineImpactItemKind,
        )
        from okto_pulse.core.ports.guideline_policy import GuidelineImpactListQuery

        def build_command(
            codec: object | None,
            _actor: object,
        ) -> object:
            query = GuidelineImpactListQuery(
                board_id=board_id,
                impact_receipt_id=impact_receipt_id,
                guideline_id=guideline_id,
                limit=limit,
                cursor=_decode(codec, cursor, kind="impact"),
                entity_type=entity_type,
                item_kind=(
                    GuidelineImpactItemKind(item_kind)
                    if item_kind is not None
                    else None
                ),
                projection=PolicyProjection(profile),
            )
            return ListGuidelineImpactItemsCommand(
                guideline_id=guideline_id,
                query=query,
            )

        return await _execute(
            board_id,
            "list_impact_items",
            None,
            ListGuidelineImpactItemsUseCase(),
            build_command=build_command,
            paginated=True,
        )

    async def okto_pulse_adopt_guideline_revision(
        board_id: BoardId,
        guideline_id: GuidelineId,
        impact_receipt_id: ImpactReceiptId,
        impact_digest: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")],
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Adopt only the exact server-issued impact evidence supplied."""

        from okto_pulse.core.application.use_cases import (
            AdoptGuidelineRevisionCommand,
            AdoptGuidelineRevisionUseCase,
        )

        try:
            command = AdoptGuidelineRevisionCommand(
                board_id=board_id,
                guideline_id=guideline_id,
                impact_receipt_id=impact_receipt_id,
                impact_digest=impact_digest,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "adopt_revision",
            command,
            AdoptGuidelineRevisionUseCase(),
        )

    async def okto_pulse_record_semantic_guideline_assessment(
        board_id: BoardId,
        entity_type: PolicyEntityTypeValue,
        subject_id: Annotated[
            str,
            Field(min_length=1, max_length=POLICY_SUBJECT_ID_MAX_LENGTH),
        ],
        expected_subject_version: PositiveRevision,
        binding_id: Annotated[str, Field(min_length=1, max_length=128)],
        expected_binding_revision: PositiveRevision,
        guideline_revision_id: RevisionId,
        idempotency_key: IdempotencyKey,
        confidence: Annotated[int, Field(ge=0, le=100)],
        metric_results: Annotated[
            list[SemanticMetricAssessmentInput],
            Field(min_length=1, max_length=200),
        ],
        model_id: Annotated[
            str | None,
            Field(default=None, min_length=1, max_length=200),
        ] = None,
    ) -> McpToolOutcome:
        """Record complete agent-produced metric evidence against exact fences.

        Read ``okto-pulse://reference/policy-compliance`` before use. Pulse
        validates structure, current authority, thresholds and aggregation; it
        never performs the cognitive assessment itself.
        """

        from okto_pulse.core.application.use_cases import (
            RecordSemanticGuidelineAssessmentCommand,
            RecordSemanticGuidelineAssessmentUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import (
            PolicyEntityType,
            PolicySubjectRef,
        )
        from okto_pulse.core.domain.guideline_semantic_assessment import (
            SemanticAssessmentAssessor,
            SemanticGuidelineAssessmentSubmission,
            SemanticMetricAssessment,
        )
        from okto_pulse.core.domain.quality_assessment import (
            EvidenceRef,
            FindingAnchorType,
            UnboundFindingAnchor,
        )

        def build_command(
            _codec: object | None,
            actor: object,
        ) -> object:
            submission = SemanticGuidelineAssessmentSubmission(
                subject=PolicySubjectRef(
                    board_id=board_id,
                    entity_type=PolicyEntityType(entity_type),
                    subject_id=subject_id,
                    subject_version=expected_subject_version,
                ),
                binding_id=binding_id,
                expected_binding_revision=expected_binding_revision,
                guideline_revision_id=guideline_revision_id,
                idempotency_key=idempotency_key,
                confidence=confidence,
                assessor=SemanticAssessmentAssessor(
                    agent_id=str(getattr(actor, "actor_id")),
                    model_id=model_id,
                ),
                metric_results=tuple(
                    SemanticMetricAssessment(
                        metric_id=item.metric_id,
                        score=item.score,
                        rationale=item.rationale,
                        evidence_refs=tuple(
                            EvidenceRef(
                                source_type=evidence.source_type,
                                source_id=evidence.source_id,
                                source_version=evidence.source_version,
                                content_hash=evidence.content_hash,
                            )
                            for evidence in item.evidence_refs
                        ),
                        pinpoints=tuple(
                            UnboundFindingAnchor(
                                anchor_type=FindingAnchorType(pinpoint.anchor_type),
                                anchor_ref=pinpoint.anchor_ref,
                                excerpt_hash=pinpoint.excerpt_hash,
                            )
                            for pinpoint in item.pinpoints
                        ),
                    )
                    for item in metric_results
                ),
            )
            return RecordSemanticGuidelineAssessmentCommand(
                board_id=board_id,
                submission=submission,
                receipt_id=_server_id(
                    "semantic-guideline-assessment",
                    board_id,
                    idempotency_key,
                ),
            )

        return await _execute(
            board_id,
            "record_assessment",
            None,
            RecordSemanticGuidelineAssessmentUseCase(),
            build_command=build_command,
        )

    async def okto_pulse_record_semantic_guideline_assessment_v2(
        board_id: BoardId,
        contract_version: Literal["v2"],
        subject_type: PolicyEntityTypeValue,
        subject_id: Annotated[
            str,
            Field(min_length=1, max_length=POLICY_SUBJECT_ID_MAX_LENGTH),
        ],
        expected_subject_version: PositiveRevision,
        binding_id: Annotated[str, Field(min_length=1, max_length=128)],
        expected_binding_revision: PositiveRevision,
        guideline_revision_id: RevisionId,
        idempotency_key: IdempotencyKey,
        confidence: Annotated[int, Field(ge=0, le=100)],
        metric_results: Annotated[
            list[SemanticMetricAssessmentV2Input],
            Field(min_length=1, max_length=200),
        ],
        model_id: Annotated[
            str | None,
            Field(default=None, min_length=1, max_length=200),
        ] = None,
    ) -> McpToolOutcome:
        """Record an actionable, human-readable semantic assessment v2."""

        from okto_pulse.core.application.use_cases.semantic_guideline_v2 import (
            SealSemanticGuidelineAssessmentV2Command,
            SealSemanticGuidelineAssessmentV2UseCase,
        )
        from okto_pulse.core.domain.guideline_policy import (
            PolicyEntityType,
            PolicySubjectRef,
        )
        from okto_pulse.core.domain.guideline_semantic_assessment import (
            SemanticAssessmentAssessor,
        )
        from okto_pulse.core.domain.guideline_semantic_v2 import (
            SemanticAssessmentDraftV2,
            SemanticMetricAssessmentDraftV2,
            SemanticPinpointDraftV2,
            SemanticPinpointKind,
        )
        from okto_pulse.core.domain.quality_assessment import (
            EvidenceRef,
            FindingAnchorType,
            FindingSeverity,
            UnboundFindingAnchor,
        )

        def build_command(_codec: object | None, actor: object) -> object:
            draft = SemanticAssessmentDraftV2(
                subject=PolicySubjectRef(
                    board_id=board_id,
                    entity_type=PolicyEntityType(subject_type),
                    subject_id=subject_id,
                    subject_version=expected_subject_version,
                ),
                binding_id=binding_id,
                expected_binding_revision=expected_binding_revision,
                guideline_revision_id=guideline_revision_id,
                idempotency_key=idempotency_key,
                confidence=confidence,
                assessor=SemanticAssessmentAssessor(
                    agent_id=str(getattr(actor, "actor_id")),
                    model_id=model_id,
                ),
                metric_results=tuple(
                    SemanticMetricAssessmentDraftV2(
                        metric_id=item.metric_id,
                        score=item.score,
                        rationale=item.rationale,
                        evidence_refs=tuple(
                            EvidenceRef(
                                source_type=evidence.source_type,
                                source_id=evidence.source_id,
                                source_version=evidence.source_version,
                                content_hash=evidence.content_hash,
                            )
                            for evidence in item.evidence_refs
                        ),
                        pinpoints=tuple(
                            SemanticPinpointDraftV2(
                                pinpoint_key=pinpoint.pinpoint_key,
                                kind=SemanticPinpointKind(pinpoint.kind),
                                title=pinpoint.title,
                                detail=pinpoint.detail,
                                severity=(
                                    FindingSeverity(pinpoint.severity)
                                    if pinpoint.severity is not None
                                    else None
                                ),
                                remediation=pinpoint.remediation,
                                anchor=UnboundFindingAnchor(
                                    anchor_type=FindingAnchorType(
                                        pinpoint.anchor.anchor_type
                                    ),
                                    anchor_ref=pinpoint.anchor.anchor_ref,
                                    excerpt_hash=pinpoint.anchor.excerpt_hash,
                                ),
                            )
                            for pinpoint in item.pinpoints
                        ),
                    )
                    for item in metric_results
                ),
            )
            return SealSemanticGuidelineAssessmentV2Command(
                board_id=board_id,
                actor_id=str(getattr(actor, "actor_id")),
                draft=draft,
            )

        return await _execute(
            board_id,
            "record_assessment_v2",
            None,
            SealSemanticGuidelineAssessmentV2UseCase(),
            build_command=build_command,
        )

    async def okto_pulse_list_semantic_guideline_assessments(
        board_id: BoardId,
        limit: PageLimit = POLICY_PAGE_LIMIT_DEFAULT,
        cursor: CursorToken | None = None,
        entity_type: PolicyEntityTypeValue | None = None,
        subject_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_SUBJECT_ID_MAX_LENGTH,
            ),
        ] = None,
        guideline_id: GuidelineId | None = None,
        binding_id: Annotated[
            str | None,
            Field(default=None, min_length=1, max_length=128),
        ] = None,
        outcome: SemanticAssessmentOutcomeValue | None = None,
        currentness: PolicyCurrentnessValue | None = None,
        profile: SemanticGuidelineProjectionValue = "summary",
    ) -> McpToolOutcome:
        """List semantic assessment receipts with honest currentness."""

        from okto_pulse.core.application.use_cases import (
            ListSemanticGuidelineAssessmentsCommand,
            ListSemanticGuidelineAssessmentsUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import (
            PolicyCurrentness,
            PolicyEntityType,
        )
        from okto_pulse.core.domain.guideline_semantic_assessment import (
            SemanticAssessmentState,
        )
        from okto_pulse.core.domain.guideline_semantic_projection import (
            SemanticGuidelineProjection,
        )
        from okto_pulse.core.ports.guideline_policy import (
            SemanticAssessmentListQuery,
        )

        def build_command(
            codec: object | None,
            _actor: object,
        ) -> object:
            return ListSemanticGuidelineAssessmentsCommand(
                SemanticAssessmentListQuery(
                    board_id=board_id,
                    limit=limit,
                    cursor=_decode(
                        codec,
                        cursor,
                        kind="semantic_assessment",
                    ),
                    entity_type=(
                        PolicyEntityType(entity_type)
                        if entity_type is not None
                        else None
                    ),
                    subject_id=subject_id,
                    guideline_id=guideline_id,
                    binding_id=binding_id,
                    outcome=(
                        SemanticAssessmentState(outcome)
                        if outcome is not None
                        else None
                    ),
                    currentness=(
                        PolicyCurrentness(currentness)
                        if currentness is not None
                        else None
                    ),
                    projection=SemanticGuidelineProjection(profile),
                )
            )

        return await _execute(
            board_id,
            "list_assessments",
            None,
            ListSemanticGuidelineAssessmentsUseCase(),
            build_command=build_command,
            paginated=True,
        )

    async def okto_pulse_get_semantic_guideline_assessment(
        board_id: BoardId,
        receipt_id: ComplianceReceiptId,
        profile: SemanticGuidelineProjectionValue = "full",
    ) -> McpToolOutcome:
        """Read one immutable semantic assessment at the requested profile."""

        from okto_pulse.core.application.use_cases import (
            GetSemanticGuidelineAssessmentCommand,
            GetSemanticGuidelineAssessmentUseCase,
        )
        from okto_pulse.core.domain.guideline_semantic_projection import (
            SemanticGuidelineProjection,
        )

        try:
            command = GetSemanticGuidelineAssessmentCommand(
                board_id=board_id,
                receipt_id=receipt_id,
                projection=SemanticGuidelineProjection(profile),
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "get_assessment",
            command,
            GetSemanticGuidelineAssessmentUseCase(),
        )

    async def okto_pulse_get_current_semantic_guideline_assessment(
        board_id: BoardId,
        entity_type: PolicyEntityTypeValue,
        subject_id: Annotated[
            str,
            Field(min_length=1, max_length=POLICY_SUBJECT_ID_MAX_LENGTH),
        ],
        binding_id: Annotated[str, Field(min_length=1, max_length=128)],
        profile: SemanticGuidelineProjectionValue = "full",
    ) -> McpToolOutcome:
        """Read the current receipt for one exact subject and binding."""

        from okto_pulse.core.application.use_cases import (
            GetCurrentSemanticGuidelineAssessmentCommand,
        )
        from okto_pulse.core.application.use_cases.semantic_guideline_v2 import (
            GetCurrentSemanticGuidelineAssessmentAnyUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import PolicyEntityType
        from okto_pulse.core.domain.guideline_semantic_projection import (
            SemanticGuidelineProjection,
        )

        try:
            command = GetCurrentSemanticGuidelineAssessmentCommand(
                board_id=board_id,
                entity_type=PolicyEntityType(entity_type),
                subject_id=subject_id,
                binding_id=binding_id,
                projection=SemanticGuidelineProjection(profile),
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "get_current_assessment",
            command,
            GetCurrentSemanticGuidelineAssessmentAnyUseCase(),
        )

    async def okto_pulse_list_semantic_guideline_findings(
        board_id: BoardId,
        limit: PageLimit = POLICY_PAGE_LIMIT_DEFAULT,
        cursor: CursorToken | None = None,
        receipt_id: ComplianceReceiptId | None = None,
        guideline_id: GuidelineId | None = None,
        binding_id: Annotated[
            str | None,
            Field(default=None, min_length=1, max_length=128),
        ] = None,
        metric_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_METRIC_ID_MAX_LENGTH,
            ),
        ] = None,
        entity_type: PolicyEntityTypeValue | None = None,
        subject_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_SUBJECT_ID_MAX_LENGTH,
            ),
        ] = None,
        outcome: SemanticMetricOutcomeValue | None = None,
        profile: SemanticGuidelineProjectionValue = "summary",
    ) -> McpToolOutcome:
        """List pinpointed semantic findings with a signed keyset cursor."""

        from okto_pulse.core.application.use_cases import (
            ListSemanticGuidelineFindingsCommand,
            ListSemanticGuidelineFindingsUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import PolicyEntityType
        from okto_pulse.core.domain.guideline_semantic_assessment import (
            SemanticMetricOutcome,
        )
        from okto_pulse.core.domain.guideline_semantic_projection import (
            SemanticGuidelineProjection,
        )
        from okto_pulse.core.ports.guideline_policy import (
            SemanticFindingListQuery,
        )

        def build_command(
            codec: object | None,
            _actor: object,
        ) -> object:
            return ListSemanticGuidelineFindingsCommand(
                SemanticFindingListQuery(
                    board_id=board_id,
                    limit=limit,
                    cursor=_decode(
                        codec,
                        cursor,
                        kind="semantic_finding",
                    ),
                    receipt_id=receipt_id,
                    guideline_id=guideline_id,
                    binding_id=binding_id,
                    metric_id=metric_id,
                    entity_type=(
                        PolicyEntityType(entity_type)
                        if entity_type is not None
                        else None
                    ),
                    subject_id=subject_id,
                    outcome=(
                        SemanticMetricOutcome(outcome) if outcome is not None else None
                    ),
                    projection=SemanticGuidelineProjection(profile),
                )
            )

        return await _execute(
            board_id,
            "list_findings",
            None,
            ListSemanticGuidelineFindingsUseCase(),
            build_command=build_command,
            paginated=True,
        )

    async def okto_pulse_list_semantic_guideline_waivers(
        board_id: BoardId,
        evaluated_at: datetime,
        limit: PageLimit = POLICY_PAGE_LIMIT_DEFAULT,
        cursor: CursorToken | None = None,
        finding_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_FINDING_ID_MAX_LENGTH,
            ),
        ] = None,
        metric_result_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
            ),
        ] = None,
        receipt_id: ComplianceReceiptId | None = None,
        guideline_id: GuidelineId | None = None,
        binding_id: Annotated[
            str | None,
            Field(default=None, min_length=1, max_length=128),
        ] = None,
        metric_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_METRIC_ID_MAX_LENGTH,
            ),
        ] = None,
        entity_type: PolicyEntityTypeValue | None = None,
        subject_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_SUBJECT_ID_MAX_LENGTH,
            ),
        ] = None,
        status: PolicyWaiverStatusValue | None = None,
        profile: SemanticGuidelineProjectionValue = "summary",
    ) -> McpToolOutcome:
        """List semantic waiver heads at one explicit evaluation time."""

        from okto_pulse.core.application.use_cases import (
            ListSemanticMetricWaiversCommand,
            ListSemanticMetricWaiversUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import PolicyEntityType
        from okto_pulse.core.domain.guideline_semantic_exceptions import (
            SemanticMetricWaiverStatus,
        )
        from okto_pulse.core.domain.guideline_semantic_projection import (
            SemanticGuidelineProjection,
        )
        from okto_pulse.core.ports.guideline_policy import (
            SemanticWaiverListQuery,
        )

        def build_command(
            codec: object | None,
            _actor: object,
        ) -> object:
            return ListSemanticMetricWaiversCommand(
                SemanticWaiverListQuery(
                    board_id=board_id,
                    evaluated_at=evaluated_at,
                    limit=limit,
                    cursor=_decode(
                        codec,
                        cursor,
                        kind="semantic_waiver",
                    ),
                    finding_id=finding_id,
                    metric_result_id=metric_result_id,
                    receipt_id=receipt_id,
                    guideline_id=guideline_id,
                    binding_id=binding_id,
                    metric_id=metric_id,
                    entity_type=(
                        PolicyEntityType(entity_type)
                        if entity_type is not None
                        else None
                    ),
                    subject_id=subject_id,
                    status=(
                        SemanticMetricWaiverStatus(status)
                        if status is not None
                        else None
                    ),
                    projection=SemanticGuidelineProjection(profile),
                )
            )

        return await _execute(
            board_id,
            "list_waivers",
            None,
            ListSemanticMetricWaiversUseCase(),
            build_command=build_command,
            paginated=True,
        )

    async def okto_pulse_get_semantic_guideline_waiver(
        board_id: BoardId,
        waiver_id: WaiverId,
        evaluated_at: datetime,
        profile: SemanticGuidelineProjectionValue = "full",
    ) -> McpToolOutcome:
        """Read one semantic metric-waiver head at the list snapshot instant."""

        from okto_pulse.core.application.use_cases import (
            GetSemanticMetricWaiverCommand,
            GetSemanticMetricWaiverUseCase,
        )
        from okto_pulse.core.domain.guideline_semantic_projection import (
            SemanticGuidelineProjection,
        )

        try:
            command = GetSemanticMetricWaiverCommand(
                board_id=board_id,
                waiver_id=waiver_id,
                evaluated_at=evaluated_at,
                projection=SemanticGuidelineProjection(profile),
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "get_waiver",
            command,
            GetSemanticMetricWaiverUseCase(),
        )

    async def okto_pulse_list_semantic_guideline_waiver_events(
        board_id: BoardId,
        waiver_id: WaiverId,
    ) -> McpToolOutcome:
        """Read the append-only event history for one semantic waiver."""

        from okto_pulse.core.application.use_cases import (
            ListSemanticMetricWaiverEventsCommand,
            ListSemanticMetricWaiverEventsUseCase,
        )

        try:
            command = ListSemanticMetricWaiverEventsCommand(
                board_id=board_id,
                waiver_id=waiver_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "list_waiver_events",
            command,
            ListSemanticMetricWaiverEventsUseCase(),
        )

    async def okto_pulse_request_semantic_guideline_waiver(
        board_id: BoardId,
        metric_result_id: Annotated[
            str,
            Field(min_length=1, max_length=POLICY_RECEIPT_ID_MAX_LENGTH),
        ],
        finding_id: Annotated[
            str,
            Field(min_length=1, max_length=POLICY_FINDING_ID_MAX_LENGTH),
        ],
        receipt_id: ComplianceReceiptId,
        justification: Annotated[
            str,
            Field(min_length=1, max_length=20_000),
        ],
        evidence_refs: Annotated[
            list[SemanticEvidenceRefInput],
            Field(min_length=1, max_length=200),
        ],
        idempotency_key: IdempotencyKey,
        expires_at: datetime | None = None,
    ) -> McpToolOutcome:
        """Request an exact waiver for one current failed metric finding."""

        from okto_pulse.core.application.use_cases import (
            RequestSemanticMetricWaiverCommand,
            RequestSemanticMetricWaiverUseCase,
        )

        try:
            command = RequestSemanticMetricWaiverCommand(
                board_id=board_id,
                metric_result_id=metric_result_id,
                finding_id=finding_id,
                receipt_id=receipt_id,
                justification=justification,
                evidence_refs=_domain_evidence_refs(evidence_refs),
                expires_at=expires_at,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "request_waiver",
            command,
            RequestSemanticMetricWaiverUseCase(),
        )

    async def okto_pulse_review_semantic_guideline_waiver(
        board_id: BoardId,
        waiver_id: WaiverId,
        decision: PolicyWaiverDecisionValue,
        reason: Annotated[str, Field(min_length=1, max_length=20_000)],
        evidence_refs: Annotated[
            list[SemanticEvidenceRefInput],
            Field(min_length=1, max_length=200),
        ],
        expected_waiver_revision: PositiveRevision,
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Approve or reject a semantic waiver with independent review."""

        from okto_pulse.core.application.use_cases import (
            ReviewSemanticMetricWaiverCommand,
            ReviewSemanticMetricWaiverUseCase,
        )
        from okto_pulse.core.domain.guideline_semantic_exceptions import (
            SemanticMetricWaiverEventType,
        )

        try:
            command = ReviewSemanticMetricWaiverCommand(
                board_id=board_id,
                waiver_id=waiver_id,
                decision=(
                    SemanticMetricWaiverEventType.APPROVE
                    if decision == "approve"
                    else SemanticMetricWaiverEventType.REJECT
                ),
                reason=reason,
                evidence_refs=_domain_evidence_refs(evidence_refs),
                expected_waiver_revision=expected_waiver_revision,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "review_waiver",
            command,
            ReviewSemanticMetricWaiverUseCase(),
        )

    async def okto_pulse_revoke_semantic_guideline_waiver(
        board_id: BoardId,
        waiver_id: WaiverId,
        reason: Annotated[str, Field(min_length=1, max_length=20_000)],
        evidence_refs: Annotated[
            list[SemanticEvidenceRefInput],
            Field(min_length=1, max_length=200),
        ],
        expected_waiver_revision: PositiveRevision,
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Revoke one approved semantic waiver using optimistic CAS."""

        from okto_pulse.core.application.use_cases import (
            RevokeSemanticMetricWaiverCommand,
            RevokeSemanticMetricWaiverUseCase,
        )

        try:
            command = RevokeSemanticMetricWaiverCommand(
                board_id=board_id,
                waiver_id=waiver_id,
                reason=reason,
                evidence_refs=_domain_evidence_refs(evidence_refs),
                expected_waiver_revision=expected_waiver_revision,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "revoke_waiver",
            command,
            RevokeSemanticMetricWaiverUseCase(),
        )

    async def okto_pulse_revalidate_semantic_guideline_waiver(
        board_id: BoardId,
        waiver_id: WaiverId,
        expected_waiver_revision: PositiveRevision,
        evaluated_at: datetime,
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Revalidate an exact semantic waiver against current evidence."""

        from okto_pulse.core.application.use_cases import (
            RevalidateSemanticMetricWaiverCommand,
            RevalidateSemanticMetricWaiverUseCase,
        )

        try:
            command = RevalidateSemanticMetricWaiverCommand(
                board_id=board_id,
                waiver_id=waiver_id,
                expected_waiver_revision=expected_waiver_revision,
                evaluated_at=evaluated_at,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "revalidate_waiver",
            command,
            RevalidateSemanticMetricWaiverUseCase(),
        )

    for handler in (
        okto_pulse_list_guideline_revisions,
        okto_pulse_get_guideline_revision,
        okto_pulse_create_guideline_revision,
        okto_pulse_retire_guideline,
        okto_pulse_preview_guideline_impact,
        okto_pulse_get_guideline_impact,
        okto_pulse_list_guideline_impact_items,
        okto_pulse_adopt_guideline_revision,
        okto_pulse_record_semantic_guideline_assessment,
        okto_pulse_record_semantic_guideline_assessment_v2,
        okto_pulse_list_semantic_guideline_assessments,
        okto_pulse_get_semantic_guideline_assessment,
        okto_pulse_get_current_semantic_guideline_assessment,
        okto_pulse_list_semantic_guideline_findings,
        okto_pulse_list_semantic_guideline_waivers,
        okto_pulse_get_semantic_guideline_waiver,
        okto_pulse_list_semantic_guideline_waiver_events,
        okto_pulse_request_semantic_guideline_waiver,
        okto_pulse_review_semantic_guideline_waiver,
        okto_pulse_revoke_semantic_guideline_waiver,
        okto_pulse_revalidate_semantic_guideline_waiver,
    ):
        _closed_tool(mcp, handler)


__all__ = [
    "GuidelineMetricInput",
    "GuidelineRevisionPatchInput",
    "SemanticEvidenceRefInput",
    "SemanticMetricAssessmentInput",
    "SemanticMetricAssessmentV2Input",
    "SemanticPinpointInput",
    "SemanticPinpointV2Input",
    "SemanticGuidelineProjectionValue",
    "SEMANTIC_GUIDELINE_RESOURCE_URI",
    "POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION",
    "POLICY_PAGE_LIMIT_DEFAULT",
    "POLICY_PAGE_LIMIT_MAX",
    "authorize_policy_governance_mcp",
    "register_policy_governance_tools",
]
