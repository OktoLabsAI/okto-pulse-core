"""Board-scoped MCP surface for versioned guideline policy governance.

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
    StrictFloat,
    StrictInt,
    StrictStr,
    model_validator,
)

from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    COMPLIANCE_EVALUATE,
    COMPLIANCE_READ,
    IMPACT_PREVIEW,
    REVISIONS_CREATE,
    REVISIONS_READ,
    REVISIONS_RETIRE,
    RULES_AUTHOR_BLOCKING,
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
    POLICY_RECEIPT_ID_MAX_LENGTH,
    POLICY_RULE_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
    POLICY_SUBJECT_ID_MAX_LENGTH,
    POLICY_WAIVER_ID_MAX_LENGTH,
)
from okto_pulse.core.domain.guideline_lifecycle import GuidelineVersionBump
from okto_pulse.core.mcp.outcome import McpToolOutcome


POLICY_PAGE_LIMIT_DEFAULT = 50
POLICY_PAGE_LIMIT_MAX = 200
POLICY_COMPLIANCE_RESOURCE_URI = "okto-pulse://reference/policy-compliance"
POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION = {
    "list_revisions": (REVISIONS_READ,),
    "get_revision": (REVISIONS_READ,),
    "create_revision": (REVISIONS_CREATE,),
    "retire_guideline": (REVISIONS_RETIRE,),
    "preview_impact": (IMPACT_PREVIEW,),
    "get_impact": (IMPACT_PREVIEW,),
    "list_impact_items": (IMPACT_PREVIEW,),
    "adopt_revision": (ADOPTION_MANAGE,),
    "evaluate_compliance": (COMPLIANCE_EVALUATE,),
    "list_compliance_receipts": (COMPLIANCE_READ,),
    "get_compliance_receipt": (COMPLIANCE_READ,),
    "get_current_compliance": (COMPLIANCE_READ,),
    "list_compliance_findings": (COMPLIANCE_READ,),
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
PolicyEvaluationOutcomeValue = Literal[
    "pass",
    "fail",
    "not_applicable",
    "error",
]
PolicyCurrentnessValue = Literal["current", "stale"]
PolicyWaiverStatusValue = Literal[
    "requested",
    "approved",
    "rejected",
    "revoked",
    "expired",
]
PolicyWaiverDecisionValue = Literal["approve", "reject"]
PolicyScalar = StrictStr | StrictInt | StrictFloat | bool | None

_SERVER_ID_NAMESPACE = uuid.UUID("dd3e22d4-c700-5e18-a5f6-ce7fa2ff27ad")


class _ClosedInput(BaseModel):
    """Base for nested MCP values with a closed JSON-object contract."""

    model_config = ConfigDict(extra="forbid")


class PresenceParametersInput(_ClosedInput):
    fact: str = Field(min_length=1, max_length=200)


class EqualityParametersInput(_ClosedInput):
    fact: str = Field(min_length=1, max_length=200)
    value: PolicyScalar


class MembershipParametersInput(_ClosedInput):
    fact: str = Field(min_length=1, max_length=200)
    values: list[PolicyScalar] = Field(min_length=1)


class NumericParametersInput(_ClosedInput):
    fact: str = Field(min_length=1, max_length=200)
    value: StrictInt | StrictFloat


class CountParametersInput(_ClosedInput):
    fact: str = Field(min_length=1, max_length=200)
    value: StrictInt = Field(ge=0)


class ContainsParametersInput(_ClosedInput):
    fact: str = Field(min_length=1, max_length=200)
    value: StrictStr = Field(min_length=1)


class PresencePredicateInput(_ClosedInput):
    predicate_code: Literal["exists", "not_exists"]
    parameters: PresenceParametersInput


class EqualityPredicateInput(_ClosedInput):
    predicate_code: Literal["eq", "ne"]
    parameters: EqualityParametersInput


class MembershipPredicateInput(_ClosedInput):
    predicate_code: Literal["in", "not_in"]
    parameters: MembershipParametersInput


class NumericPredicateInput(_ClosedInput):
    predicate_code: Literal["gt", "gte", "lt", "lte"]
    parameters: NumericParametersInput


class CountPredicateInput(_ClosedInput):
    predicate_code: Literal[
        "count_eq",
        "count_ne",
        "count_gt",
        "count_gte",
        "count_lt",
        "count_lte",
    ]
    parameters: CountParametersInput


class ContainsPredicateInput(_ClosedInput):
    predicate_code: Literal["contains", "not_contains"]
    parameters: ContainsParametersInput


GuidelinePredicateInput = Annotated[
    PresencePredicateInput
    | EqualityPredicateInput
    | MembershipPredicateInput
    | NumericPredicateInput
    | CountPredicateInput
    | ContainsPredicateInput,
    Field(discriminator="predicate_code"),
]


class GuidelineRuleInput(_ClosedInput):
    rule_id: str = Field(min_length=1, max_length=POLICY_RULE_ID_MAX_LENGTH)
    code: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=500)
    description: str = Field(min_length=1)
    target_entity_types: list[PolicyEntityTypeValue] = Field(min_length=1)
    predicates: list[GuidelinePredicateInput] = Field(min_length=1)
    enforcement: GuidelineEnforcementValue = "advisory"
    operator: Literal["all", "any"] = "all"
    waivable: bool = False
    policy_class: str = Field(default="standard", min_length=1, max_length=200)


class GuidelineRevisionPatchInput(_ClosedInput):
    title: str | None = Field(
        default=None,
        min_length=1,
        max_length=GUIDELINE_TITLE_MAX_LENGTH,
    )
    content: str | None = Field(default=None, min_length=1)
    tags: list[str] | None = None
    rules: list[GuidelineRuleInput] | None = None

    @model_validator(mode="after")
    def require_change(self) -> GuidelineRevisionPatchInput:
        if all(
            value is None
            for value in (self.title, self.content, self.tags, self.rules)
        ):
            raise ValueError("guideline_revision_patch_empty")
        return self


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
    return {
        "items": _native(getattr(page, "items")),
        "limit": getattr(page, "limit"),
        "has_more": getattr(page, "has_more"),
        "next_cursor": encode(next_cursor) if next_cursor is not None else None,
    }


def _result_payload(result: object, codec: object | None) -> object:
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


def _domain_rule(payload: GuidelineRuleInput) -> object:
    from okto_pulse.core.domain.guideline_policy import (
        GuidelineEnforcement,
        GuidelinePredicate,
        GuidelineRule,
        GuidelineRuleOperator,
        PolicyEntityType,
    )

    return GuidelineRule(
        rule_id=payload.rule_id,
        code=payload.code,
        title=payload.title,
        description=payload.description,
        target_entity_types=tuple(
            PolicyEntityType(value) for value in payload.target_entity_types
        ),
        predicates=tuple(
            GuidelinePredicate(
                item.predicate_code,
                tuple(sorted(item.parameters.model_dump(mode="python").items())),
            )
            for item in payload.predicates
        ),
        enforcement=GuidelineEnforcement(payload.enforcement),
        operator=GuidelineRuleOperator(payload.operator),
        waivable=payload.waivable,
        policy_class=payload.policy_class,
    )


def _domain_patch(payload: GuidelineRevisionPatchInput) -> object:
    from okto_pulse.core.domain.guideline_lifecycle import GuidelineRevisionPatch

    return GuidelineRevisionPatch(
        title=payload.title,
        content=payload.content,
        tags=(tuple(payload.tags) if payload.tags is not None else None),
        rules=(
            tuple(_domain_rule(item) for item in payload.rules)
            if payload.rules is not None
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
                    "metric_name": (
                        METRIC_POLICY_GOVERNANCE_AUTHORIZATION_DECISION
                    ),
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
    """Register the exact 20-tool B14 policy-governance MCP inventory."""

    async def _execute(
        board_id: str,
        operation: str,
        command: object | None,
        use_case: object,
        *,
        build_command: Callable[[object | None], object] | None = None,
        extra_capabilities: tuple[str, ...] = (),
        paginated: bool = False,
    ) -> McpToolOutcome:
        context = await get_board_agent(board_id)
        if context is None:
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
                command = build_command(codec)
            if command is None:
                raise TypeError("policy_governance_command_required")
            async with get_uow()(actor=actor) as uow:
                result = await use_case.execute(command, actor=actor, uow=uow)
            return McpToolOutcome.success(_result_payload(result, codec))
        except Exception as exc:
            try:
                return _error_outcome(exc)
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

        def build_command(codec: object | None) -> object:
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
            extra_capabilities=(
                (RULES_AUTHOR_BLOCKING,) if patch.rules is not None else ()
            ),
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
        proposed_default_enforcement: GuidelineEnforcementValue,
        idempotency_key: IdempotencyKey,
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
                proposed_default_enforcement=GuidelineEnforcement(
                    proposed_default_enforcement
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

        def build_command(codec: object | None) -> object:
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

    async def okto_pulse_evaluate_policy_compliance(
        board_id: BoardId,
        entity_type: PolicyEntityTypeValue,
        subject_id: Annotated[
            str,
            Field(min_length=1, max_length=POLICY_SUBJECT_ID_MAX_LENGTH),
        ],
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Evaluate the current subject snapshot against current board policy."""

        from okto_pulse.core.application.use_cases import (
            EvaluatePolicyComplianceCommand,
            EvaluatePolicyComplianceUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import PolicyEntityType

        try:
            command = EvaluatePolicyComplianceCommand(
                board_id=board_id,
                entity_type=PolicyEntityType(entity_type),
                subject_id=subject_id,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "evaluate_compliance",
            command,
            EvaluatePolicyComplianceUseCase(),
        )

    async def okto_pulse_list_policy_compliance_receipts(
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
        outcome: PolicyEvaluationOutcomeValue | None = None,
        currentness: PolicyCurrentnessValue | None = None,
        profile: PolicyProjectionValue = "summary",
    ) -> McpToolOutcome:
        """List immutable compliance receipts with honest currentness."""

        from okto_pulse.core.application.use_cases import (
            ListPolicyComplianceReceiptsCommand,
            ListPolicyComplianceReceiptsUseCase,
        )
        from okto_pulse.core.domain.guideline_compliance import PolicyProjection
        from okto_pulse.core.domain.guideline_policy import (
            PolicyCurrentness,
            PolicyEntityType,
            PolicyEvaluationOutcome,
        )
        from okto_pulse.core.ports.guideline_policy import (
            PolicyComplianceReceiptListQuery,
        )

        def build_command(codec: object | None) -> object:
            query = PolicyComplianceReceiptListQuery(
                board_id=board_id,
                limit=limit,
                cursor=_decode(codec, cursor, kind="receipt"),
                entity_type=(
                    PolicyEntityType(entity_type)
                    if entity_type is not None
                    else None
                ),
                subject_id=subject_id,
                outcome=(
                    PolicyEvaluationOutcome(outcome)
                    if outcome is not None
                    else None
                ),
                currentness=(
                    PolicyCurrentness(currentness)
                    if currentness is not None
                    else None
                ),
                projection=PolicyProjection(profile),
            )
            return ListPolicyComplianceReceiptsCommand(query=query)

        return await _execute(
            board_id,
            "list_compliance_receipts",
            None,
            ListPolicyComplianceReceiptsUseCase(),
            build_command=build_command,
            paginated=True,
        )

    async def okto_pulse_get_policy_compliance_receipt(
        board_id: BoardId,
        receipt_id: ComplianceReceiptId,
    ) -> McpToolOutcome:
        """Read one immutable compliance receipt and its findings."""

        from okto_pulse.core.application.use_cases import (
            GetPolicyComplianceReceiptCommand,
            GetPolicyComplianceReceiptUseCase,
        )

        try:
            command = GetPolicyComplianceReceiptCommand(
                board_id=board_id,
                receipt_id=receipt_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "get_compliance_receipt",
            command,
            GetPolicyComplianceReceiptUseCase(),
        )

    async def okto_pulse_get_current_policy_compliance_receipt(
        board_id: BoardId,
        entity_type: PolicyEntityTypeValue,
        subject_id: Annotated[
            str,
            Field(min_length=1, max_length=POLICY_SUBJECT_ID_MAX_LENGTH),
        ],
    ) -> McpToolOutcome:
        """Read the current receipt for one exact board subject."""

        from okto_pulse.core.application.use_cases import (
            GetCurrentPolicyComplianceReceiptCommand,
            GetCurrentPolicyComplianceReceiptUseCase,
        )
        from okto_pulse.core.domain.guideline_policy import PolicyEntityType

        try:
            command = GetCurrentPolicyComplianceReceiptCommand(
                board_id=board_id,
                entity_type=PolicyEntityType(entity_type),
                subject_id=subject_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "get_current_compliance",
            command,
            GetCurrentPolicyComplianceReceiptUseCase(),
        )

    async def okto_pulse_list_policy_compliance_findings(
        board_id: BoardId,
        limit: PageLimit = POLICY_PAGE_LIMIT_DEFAULT,
        cursor: CursorToken | None = None,
        receipt_id: ComplianceReceiptId | None = None,
        guideline_id: GuidelineId | None = None,
        rule_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_RULE_ID_MAX_LENGTH,
            ),
        ] = None,
        subject_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_SUBJECT_ID_MAX_LENGTH,
            ),
        ] = None,
        outcome: PolicyEvaluationOutcomeValue | None = None,
        profile: PolicyProjectionValue = "summary",
    ) -> McpToolOutcome:
        """List pinpointed compliance findings using a signed keyset cursor."""

        from okto_pulse.core.application.use_cases import (
            ListPolicyComplianceFindingsCommand,
            ListPolicyComplianceFindingsUseCase,
        )
        from okto_pulse.core.domain.guideline_compliance import PolicyProjection
        from okto_pulse.core.domain.guideline_policy import PolicyEvaluationOutcome
        from okto_pulse.core.ports.guideline_policy import (
            PolicyComplianceFindingListQuery,
        )

        def build_command(codec: object | None) -> object:
            query = PolicyComplianceFindingListQuery(
                board_id=board_id,
                limit=limit,
                cursor=_decode(codec, cursor, kind="finding"),
                receipt_id=receipt_id,
                guideline_id=guideline_id,
                rule_id=rule_id,
                subject_id=subject_id,
                outcome=(
                    PolicyEvaluationOutcome(outcome)
                    if outcome is not None
                    else None
                ),
                projection=PolicyProjection(profile),
            )
            return ListPolicyComplianceFindingsCommand(query=query)

        return await _execute(
            board_id,
            "list_compliance_findings",
            None,
            ListPolicyComplianceFindingsUseCase(),
            build_command=build_command,
            paginated=True,
        )

    async def okto_pulse_list_policy_waivers(
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
        receipt_id: ComplianceReceiptId | None = None,
        guideline_id: GuidelineId | None = None,
        revision_id: RevisionId | None = None,
        rule_id: Annotated[
            str | None,
            Field(
                default=None,
                min_length=1,
                max_length=POLICY_RULE_ID_MAX_LENGTH,
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
        subject_version: Annotated[
            int | None,
            Field(default=None, ge=1, le=POLICY_SQL_INTEGER_MAX),
        ] = None,
        status: PolicyWaiverStatusValue | None = None,
        profile: PolicyProjectionValue = "summary",
    ) -> McpToolOutcome:
        """List waiver heads at one explicit evaluation time."""

        from okto_pulse.core.application.use_cases import (
            ListPolicyWaiversCommand,
            ListPolicyWaiversUseCase,
        )
        from okto_pulse.core.domain.guideline_compliance import PolicyProjection
        from okto_pulse.core.domain.guideline_policy import (
            PolicyEntityType,
            PolicyWaiverStatus,
        )
        from okto_pulse.core.ports.guideline_policy import PolicyWaiverListQuery

        def build_command(codec: object | None) -> object:
            query = PolicyWaiverListQuery(
                board_id=board_id,
                evaluated_at=evaluated_at,
                limit=limit,
                cursor=_decode(codec, cursor, kind="waiver"),
                finding_id=finding_id,
                receipt_id=receipt_id,
                guideline_id=guideline_id,
                revision_id=revision_id,
                rule_id=rule_id,
                entity_type=(
                    PolicyEntityType(entity_type)
                    if entity_type is not None
                    else None
                ),
                subject_id=subject_id,
                subject_version=subject_version,
                status=(
                    PolicyWaiverStatus(status)
                    if status is not None
                    else None
                ),
                projection=PolicyProjection(profile),
            )
            return ListPolicyWaiversCommand(query=query)

        return await _execute(
            board_id,
            "list_waivers",
            None,
            ListPolicyWaiversUseCase(),
            build_command=build_command,
            paginated=True,
        )

    async def okto_pulse_get_policy_waiver(
        board_id: BoardId,
        waiver_id: WaiverId,
    ) -> McpToolOutcome:
        """Read one policy-waiver head."""

        from okto_pulse.core.application.use_cases import (
            GetPolicyWaiverCommand,
            GetPolicyWaiverUseCase,
        )

        try:
            command = GetPolicyWaiverCommand(
                board_id=board_id,
                waiver_id=waiver_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "get_waiver",
            command,
            GetPolicyWaiverUseCase(),
        )

    async def okto_pulse_list_policy_waiver_events(
        board_id: BoardId,
        waiver_id: WaiverId,
    ) -> McpToolOutcome:
        """Read the append-only event history for one policy waiver."""

        from okto_pulse.core.application.use_cases import (
            ListPolicyWaiverEventsCommand,
            ListPolicyWaiverEventsUseCase,
        )

        try:
            command = ListPolicyWaiverEventsCommand(
                board_id=board_id,
                waiver_id=waiver_id,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "list_waiver_events",
            command,
            ListPolicyWaiverEventsUseCase(),
        )

    async def okto_pulse_request_policy_waiver(
        board_id: BoardId,
        finding_id: Annotated[
            str,
            Field(min_length=1, max_length=POLICY_FINDING_ID_MAX_LENGTH),
        ],
        justification: Annotated[str, Field(min_length=1)],
        evidence_refs: Annotated[list[str], Field(min_length=1)],
        expires_at: datetime,
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Request a bounded waiver for one current, waivable finding."""

        from okto_pulse.core.application.use_cases import (
            RequestPolicyWaiverCommand,
            RequestPolicyWaiverUseCase,
        )

        try:
            command = RequestPolicyWaiverCommand(
                board_id=board_id,
                finding_id=finding_id,
                reason=justification,
                evidence_refs=tuple(evidence_refs),
                expires_at=expires_at,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "request_waiver",
            command,
            RequestPolicyWaiverUseCase(),
        )

    async def okto_pulse_review_policy_waiver(
        board_id: BoardId,
        waiver_id: WaiverId,
        decision: PolicyWaiverDecisionValue,
        reason: Annotated[str, Field(min_length=1)],
        evidence_refs: Annotated[list[str], Field(min_length=1)],
        expected_waiver_revision: PositiveRevision,
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Approve or reject using an explicit optimistic CAS precondition."""

        from okto_pulse.core.application.use_cases import (
            ReviewPolicyWaiverCommand,
            ReviewPolicyWaiverUseCase,
        )

        try:
            command = ReviewPolicyWaiverCommand(
                board_id=board_id,
                waiver_id=waiver_id,
                approve=decision == "approve",
                reason=reason,
                evidence_refs=tuple(evidence_refs),
                expected_waiver_revision=expected_waiver_revision,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "review_waiver",
            command,
            ReviewPolicyWaiverUseCase(),
        )

    async def okto_pulse_revoke_policy_waiver(
        board_id: BoardId,
        waiver_id: WaiverId,
        reason: Annotated[str, Field(min_length=1)],
        evidence_refs: Annotated[list[str], Field(min_length=1)],
        expected_waiver_revision: PositiveRevision,
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Revoke an approved waiver using an explicit CAS precondition."""

        from okto_pulse.core.application.use_cases import (
            RevokePolicyWaiverCommand,
            RevokePolicyWaiverUseCase,
        )

        try:
            command = RevokePolicyWaiverCommand(
                board_id=board_id,
                waiver_id=waiver_id,
                reason=reason,
                evidence_refs=tuple(evidence_refs),
                expected_waiver_revision=expected_waiver_revision,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "revoke_waiver",
            command,
            RevokePolicyWaiverUseCase(),
        )

    async def okto_pulse_revalidate_policy_waiver(
        board_id: BoardId,
        waiver_id: WaiverId,
        reason: Annotated[str, Field(min_length=1)],
        evidence_refs: Annotated[list[str], Field(min_length=1)],
        expected_waiver_revision: PositiveRevision,
        new_expires_at: datetime,
        idempotency_key: IdempotencyKey,
    ) -> McpToolOutcome:
        """Revalidate a stale/expired waiver against current source evidence."""

        from okto_pulse.core.application.use_cases import (
            RevalidatePolicyWaiverCommand,
            RevalidatePolicyWaiverUseCase,
        )

        try:
            command = RevalidatePolicyWaiverCommand(
                board_id=board_id,
                waiver_id=waiver_id,
                reason=reason,
                evidence_refs=tuple(evidence_refs),
                expected_waiver_revision=expected_waiver_revision,
                new_expires_at=new_expires_at,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            return _error_outcome(exc)
        return await _execute(
            board_id,
            "revalidate_waiver",
            command,
            RevalidatePolicyWaiverUseCase(),
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
        okto_pulse_evaluate_policy_compliance,
        okto_pulse_list_policy_compliance_receipts,
        okto_pulse_get_policy_compliance_receipt,
        okto_pulse_get_current_policy_compliance_receipt,
        okto_pulse_list_policy_compliance_findings,
        okto_pulse_list_policy_waivers,
        okto_pulse_get_policy_waiver,
        okto_pulse_list_policy_waiver_events,
        okto_pulse_request_policy_waiver,
        okto_pulse_review_policy_waiver,
        okto_pulse_revoke_policy_waiver,
        okto_pulse_revalidate_policy_waiver,
    ):
        _closed_tool(mcp, handler)


__all__ = [
    "GuidelinePredicateInput",
    "GuidelineRevisionPatchInput",
    "GuidelineRuleInput",
    "POLICY_COMPLIANCE_RESOURCE_URI",
    "POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION",
    "POLICY_PAGE_LIMIT_DEFAULT",
    "POLICY_PAGE_LIMIT_MAX",
    "authorize_policy_governance_mcp",
    "register_policy_governance_tools",
]
