"""Transport-free application boundary for governed guideline policy.

The immutable policy domain and persistence port intentionally expose more
power than an inbound adapter may call directly.  This module is the shared
REST/MCP application boundary: it checks the closed SK-B capability before
touching the unit of work, proves board/owner visibility, builds the canonical
domain plans, and gives every mutation one commit/rollback boundary.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.guideline_compliance import (
    GuidelineImpactItemPage,
    GuidelineRevisionProjectionPage,
    PolicyComplianceFindingPage,
    PolicyComplianceCurrentSnapshot,
    PolicyComplianceReceiptPage,
    PolicyProjection,
    PolicyWaiverPage,
    project_guideline_revision,
)
from okto_pulse.core.domain.guideline_lifecycle import (
    GuidelinePatchApplied,
    GuidelinePatchCommand,
    GuidelinePatchNoop,
    GuidelinePatchRejected,
    GuidelineRetirementCommand,
    GuidelineRevisionPatch,
    GuidelineVersionBump,
    execute_guideline_patch,
    plan_guideline_retirement,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_RETIREMENT_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH,
    POLICY_ACTOR_ID_MAX_LENGTH,
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_EVALUATION_ID_MAX_LENGTH,
    POLICY_FINDING_ID_MAX_LENGTH,
    POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
    POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
    POLICY_SUBJECT_ID_MAX_LENGTH,
    POLICY_WAIVER_EVENT_ID_MAX_LENGTH,
    POLICY_WAIVER_ID_MAX_LENGTH,
    BoardGuidelineBinding,
    Guideline,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelineRetirement,
    GuidelineRevision,
    PolicyComplianceReceipt,
    PolicyEntityType,
    PolicyEvaluationResult,
    PolicyWaiver,
    PolicyWaiverEvent,
    PolicyWaiverEventType,
    normalize_guideline_sha256,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_policy_evaluator import (
    build_policy_evaluation_input_v1,
    evaluate_policy,
)
from okto_pulse.core.domain.guideline_waiver_lifecycle import (
    request_policy_waiver,
    transition_policy_waiver,
)
from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.ports.guideline_policy import (
    GuidelineImpactListQuery,
    GuidelinePolicyIdempotencyConflict,
    GuidelinePolicyPersistencePort,
    GuidelineRetirementReplay,
    GuidelineRevisionNoopReplay,
    GuidelineRevisionReplay,
    GuidelineRevisionListQuery,
    PolicyComplianceFindingListQuery,
    PolicyComplianceReceiptListQuery,
    PolicyWaiverListQuery,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


Clock = Callable[[], datetime]
IdFactory = Callable[[str, str], str]

_READ_SHARES = None
_WRITE_SHARES = frozenset({"editor", "admin"})

REVISIONS_READ = "guidelines.revisions.read"
REVISIONS_CREATE = "guidelines.revisions.create"
REVISIONS_RETIRE = "guidelines.revisions.retire"
RULES_AUTHOR_BLOCKING = "guidelines.rules.author_blocking"
IMPACT_PREVIEW = "guidelines.impact.preview"
ADOPTION_MANAGE = "guidelines.adoption.manage"
COMPLIANCE_READ = "guidelines.compliance.read"
COMPLIANCE_EVALUATE = "guidelines.compliance.evaluate"
WAIVER_READ = "guidelines.waiver.read"
WAIVER_REQUEST = "guidelines.waiver.request"
WAIVER_REVIEW = "guidelines.waiver.review"
WAIVER_REVOKE = "guidelines.waiver.revoke"
WAIVER_REVALIDATE = "guidelines.waiver.revalidate"

POLICY_GOVERNANCE_CAPABILITIES = (
    REVISIONS_READ,
    REVISIONS_CREATE,
    REVISIONS_RETIRE,
    RULES_AUTHOR_BLOCKING,
    IMPACT_PREVIEW,
    ADOPTION_MANAGE,
    COMPLIANCE_READ,
    COMPLIANCE_EVALUATE,
    WAIVER_READ,
    WAIVER_REQUEST,
    WAIVER_REVIEW,
    WAIVER_REVOKE,
    WAIVER_REVALIDATE,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid5(kind: str, key: str) -> str:
    namespace = uuid.UUID("da5aec59-a5ad-5db8-91e0-739743a79de1")
    return str(uuid.uuid5(namespace, f"{kind}:{key}"))


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(code)
    return value.strip()


def _optional_text(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code)


def _bounded_text(value: object, max_length: int, code: str) -> str:
    return normalize_policy_bounded_text(
        value,
        max_length=max_length,
        code=code,
    )


def _bounded_optional_text(
    value: object,
    max_length: int,
    code: str,
) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, max_length, code)


def _evidence_refs(value: object) -> tuple[str, ...]:
    if not isinstance(value, tuple | list):
        raise ValueError("policy_waiver_evidence_refs_invalid")
    normalized = tuple(
        _required_text(item, "policy_waiver_evidence_ref_invalid") for item in value
    )
    if not normalized:
        raise ValueError("policy_waiver_evidence_refs_required")
    return tuple(sorted(set(normalized)))


def _aware_utc(value: datetime | None, clock: Clock, code: str) -> datetime:
    resolved = clock() if value is None else value
    if (
        not isinstance(resolved, datetime)
        or resolved.tzinfo is None
        or resolved.utcoffset() is None
    ):
        raise ValueError(code)
    return resolved.astimezone(timezone.utc)


def _canonical_time(value: datetime, code: str) -> str:
    resolved = _aware_utc(value, _utc_now, code)
    return resolved.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _actor_type(actor: ActorContext) -> str:
    return (
        "agent"
        if actor.source == "mcp"
        else "system"
        if actor.source == "system"
        else "user"
    )


def _mapping_permission(permissions: Mapping[str, Any], required: str) -> str | None:
    """Use the canonical PermissionSet, including introduced-authority bridges."""

    return PermissionSet(dict(permissions)).check(required)


def _flat_permission_mapping(permissions: tuple[str, ...]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for path in permissions:
        if not isinstance(path, str) or not path.strip():
            continue
        cursor = document
        parts = path.strip().split(".")
        for part in parts[:-1]:
            child = cursor.get(part)
            if not isinstance(child, dict):
                child = {}
                cursor[part] = child
            cursor = child
        cursor[parts[-1]] = True
    return document


def _permission_reason(actor: ActorContext, required: str) -> str | None:
    permissions = actor.permissions
    if isinstance(permissions, Mapping):
        return _mapping_permission(permissions, required)
    checker = getattr(permissions, "check", None)
    if callable(checker):
        try:
            return checker(required)
        except Exception:
            return f"Permission denied: requires '{required}'"
    if isinstance(permissions, (tuple, list, set, frozenset)):
        flat = tuple(permissions)
        if "*" in flat:
            return None
        return _mapping_permission(
            _flat_permission_mapping(flat),
            required,
        )
    return f"Permission denied: requires '{required}'"


def _require_capability(actor: ActorContext, *required: str) -> None:
    """Fail before any repository, service, adapter, or UoW method is touched."""

    _bounded_text(
        actor.actor_id,
        POLICY_ACTOR_ID_MAX_LENGTH,
        "policy_actor_id_invalid",
    )
    for capability in required:
        reason = _permission_reason(actor, capability)
        if reason is not None:
            raise PermissionDeniedError(reason)


def require_policy_governance_capabilities(
    actor: ActorContext,
    *required: str,
) -> None:
    """Public fail-closed preflight for inbound policy adapters.

    Use cases repeat the same check as defense in depth.  Calling this helper
    before an inbound adapter enters its unit of work prevents a denied actor
    from touching even the transaction boundary.
    """

    _require_capability(actor, *required)


async def _require_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    write: bool,
) -> Any:
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=_WRITE_SHARES if write else _READ_SHARES,
    )
    if board is None:
        raise EntityNotFoundError("board", board_id)
    return board


async def _write(uow: PulseUnitOfWork, operation: Callable[[], Any]) -> Any:
    try:
        result = await operation()
        await uow.commit()
        return result
    except Exception:
        await uow.rollback()
        raise


@dataclass(frozen=True, slots=True)
class _GuidelineAuthority:
    guideline: Guideline
    head: GuidelineHead
    revision: GuidelineRevision
    retirement: GuidelineRetirement | None


class GuidelineRevisionUnderBump(CommandValidationError):
    """Typed semantic rejection for a declared SemVer below the minimum."""

    __slots__ = (
        "minimum_bump",
        "minimum_semantic_version",
        "declared_semantic_version",
    )

    code = "guideline_semver_below_minimum"

    def __init__(
        self,
        *,
        minimum_bump: GuidelineVersionBump,
        minimum_semantic_version: str,
        declared_semantic_version: str,
    ) -> None:
        if not isinstance(minimum_bump, GuidelineVersionBump):
            raise ValueError("guideline_patch_minimum_bump_invalid")
        self.minimum_bump = minimum_bump
        self.minimum_semantic_version = _required_text(
            minimum_semantic_version,
            "minimum_semantic_version_required",
        )
        self.declared_semantic_version = _required_text(
            declared_semantic_version,
            "declared_semantic_version_required",
        )
        super().__init__(self.code)


async def _require_guideline_mutation_scope(
    *,
    port: GuidelinePolicyPersistencePort,
    actor: ActorContext,
    board_id: str,
    guideline_id: str,
) -> Guideline:
    """Prove the board path without requiring a now-terminal binding to be active.

    An idempotent retirement replay necessarily runs after its binding was
    unlinked.  Stable identity ownership plus an existing board binding proves
    the historical path without granting a new mutation on an inactive link.
    The normal planner path still reloads the stricter ACTIVE authority below.
    """

    identity = await port.get_guideline(guideline_id=guideline_id)
    if identity is None:
        raise EntityNotFoundError("guideline", guideline_id)
    if identity.board_id is not None:
        if identity.board_id != board_id:
            raise EntityNotFoundError("guideline", guideline_id)
        return identity
    binding = await port.get_binding(
        board_id=board_id,
        guideline_id=guideline_id,
    )
    if binding is None or identity.owner_id != actor.actor_id:
        raise EntityNotFoundError("guideline", guideline_id)
    return identity


async def _load_guideline_authority(
    *,
    port: GuidelinePolicyPersistencePort,
    actor: ActorContext,
    board_id: str,
    guideline_id: str,
    require_owner: bool,
    include_retired: bool = False,
) -> _GuidelineAuthority:
    identity = await port.get_guideline(guideline_id=guideline_id)
    if identity is None:
        raise EntityNotFoundError("guideline", guideline_id)
    retirement = await port.get_retirement(guideline_id=guideline_id)
    if identity.board_id is not None:
        if identity.board_id != board_id:
            raise EntityNotFoundError("guideline", guideline_id)
    else:
        binding = await port.get_binding(
            board_id=board_id,
            guideline_id=guideline_id,
        )
        if (
            binding is None
            or (
                binding.state is not GuidelineBindingState.ACTIVE
                and not (include_retired and retirement is not None)
            )
            or (require_owner and identity.owner_id != actor.actor_id)
        ):
            raise EntityNotFoundError("guideline", guideline_id)
    if retirement is not None and not include_retired:
        raise EntityNotFoundError("guideline", guideline_id)
    head = await port.get_head(guideline_id=guideline_id)
    if head is None:
        raise RuntimeError("guideline_authority_head_missing")
    revision = await port.get_revision(
        guideline_id=guideline_id,
        revision_id=head.revision_id,
    )
    if (
        revision is None
        or revision.revision_number != head.revision_number
        or revision.semantic_version != head.semantic_version
    ):
        raise RuntimeError("guideline_authority_revision_mismatch")
    return _GuidelineAuthority(identity, head, revision, retirement)


async def _replay_guideline_revision(
    *,
    port: GuidelinePolicyPersistencePort,
    replay: GuidelineRevisionReplay | GuidelineRevisionNoopReplay,
    command: CreateGuidelineRevisionCommand,
    actor: ActorContext,
    next_revision_id: str,
) -> CreateGuidelineRevisionResult:
    """Rebuild the original plan against its immutable parent and verify digest."""

    if isinstance(replay, GuidelineRevisionNoopReplay):
        candidate = execute_guideline_patch(
            GuidelinePatchCommand(
                current_revision=replay.revision,
                current_head=replay.original_head,
                patch=command.patch,
                next_revision_id=next_revision_id,
                actor_id=actor.actor_id,
                occurred_at=(
                    replay.original_head.updated_at + timedelta(microseconds=1)
                ),
                idempotency_key=command.idempotency_key,
                declared_semantic_version=command.declared_semantic_version,
            )
        )
        if (
            not isinstance(candidate, GuidelinePatchNoop)
            or candidate.request_digest != replay.request_digest
            or candidate.expected_head_revision
            != replay.original_head.head_revision
            or candidate.expected_revision_id != replay.revision.revision_id
        ):
            raise GuidelinePolicyIdempotencyConflict(
                "guideline_revision_idempotency_payload_mismatch"
            )
        return CreateGuidelineRevisionResult(
            status=candidate.status,
            revision=None,
            head=None,
            minimum_bump=None,
        )

    revision = replay.revision
    published_head = replay.published_head
    parent_id = revision.parent_revision_id
    if (
        revision.guideline_id != command.guideline_id
        or parent_id is None
        or revision.revision_number <= 1
        or published_head.head_revision <= 1
    ):
        raise GuidelinePolicyIdempotencyConflict(
            "guideline_revision_idempotency_scope_mismatch"
        )
    parent = await port.get_revision(
        guideline_id=command.guideline_id,
        revision_id=parent_id,
    )
    if parent is None:
        raise GuidelinePolicyIdempotencyConflict(
            "guideline_revision_idempotency_parent_missing"
        )
    original_head = GuidelineHead(
        guideline_id=parent.guideline_id,
        revision_id=parent.revision_id,
        revision_number=parent.revision_number,
        semantic_version=parent.semantic_version,
        head_revision=published_head.head_revision - 1,
        updated_at=parent.created_at,
    )
    candidate = execute_guideline_patch(
        GuidelinePatchCommand(
            current_revision=parent,
            current_head=original_head,
            patch=command.patch,
            next_revision_id=next_revision_id,
            actor_id=actor.actor_id,
            occurred_at=revision.created_at,
            idempotency_key=command.idempotency_key,
            declared_semantic_version=command.declared_semantic_version,
        )
    )
    if (
        not isinstance(candidate, GuidelinePatchApplied)
        or candidate.request_digest != replay.request_digest
        or candidate.revision != revision
        or candidate.head != published_head
    ):
        raise GuidelinePolicyIdempotencyConflict(
            "guideline_revision_idempotency_payload_mismatch"
        )
    return CreateGuidelineRevisionResult(
        status=candidate.status,
        revision=revision,
        head=published_head,
        minimum_bump=candidate.minimum_bump,
    )


async def _replay_guideline_retirement(
    *,
    port: GuidelinePolicyPersistencePort,
    replay: GuidelineRetirementReplay,
    command: RetireGuidelineCommand,
    actor: ActorContext,
) -> RetireGuidelineResult:
    """Rebuild the original terminal plan and compare immutable evidence."""

    retirement = replay.retirement
    if retirement.guideline_id != command.guideline_id:
        raise GuidelinePolicyIdempotencyConflict(
            "guideline_retirement_idempotency_scope_mismatch"
        )
    revision = await port.get_revision(
        guideline_id=command.guideline_id,
        revision_id=retirement.retired_revision_id,
    )
    if revision is None:
        raise GuidelinePolicyIdempotencyConflict(
            "guideline_retirement_idempotency_revision_missing"
        )
    original_head = GuidelineHead(
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        revision_number=revision.revision_number,
        semantic_version=revision.semantic_version,
        head_revision=retirement.retired_head_revision,
        updated_at=revision.created_at,
    )
    candidate = plan_guideline_retirement(
        GuidelineRetirementCommand(
            current_revision=revision,
            current_head=original_head,
            retirement_id=command.retirement_id,
            status=command.status,
            reason=command.reason,
            actor_id=actor.actor_id,
            occurred_at=retirement.retired_at,
            idempotency_key=command.idempotency_key,
            superseded_by_guideline_id=command.superseded_by_guideline_id,
        )
    )
    if (
        candidate.request_digest != replay.request_digest
        or candidate.retirement != retirement
    ):
        raise GuidelinePolicyIdempotencyConflict(
            "guideline_retirement_idempotency_payload_mismatch"
        )
    return RetireGuidelineResult(retirement)


def _current_snapshot_from_evaluation(
    evaluation_input: Any,
) -> PolicyComplianceCurrentSnapshot:
    snapshot = evaluation_input.subject_snapshot
    return PolicyComplianceCurrentSnapshot(
        subject=snapshot.subject,
        subject_content_digest=snapshot.content_digest,
        input_digest=evaluation_input.input_digest,
        policy_set_digest=evaluation_input.policy_set_digest,
        binding_head_digest=evaluation_input.binding_head_digest,
        catalog_version=evaluation_input.catalog_version,
        ruleset_version=evaluation_input.ruleset_version,
    )


def _waiver_request_digest(
    *,
    operation: str,
    board_id: str,
    idempotency_key: str,
    payload: Mapping[str, object],
) -> str:
    return canonical_sha256(
        {
            "contract": "policy-waiver-application-request/v1",
            "operation": operation,
            "board_id": board_id,
            "idempotency_key": idempotency_key,
            "payload": dict(payload),
        }
    )


@dataclass(frozen=True, slots=True)
class ListGuidelineRevisionsCommand:
    board_id: str
    guideline_id: str
    limit: int = 50
    cursor: Any | None = None
    projection: PolicyProjection = PolicyProjection.SUMMARY

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "guideline_id",
            _bounded_text(
                self.guideline_id,
                GUIDELINE_ID_MAX_LENGTH,
                "guideline_id_required",
            ),
        )
        # Reuse the closed port query for limit/cursor validation.
        query = GuidelineRevisionListQuery(
            guideline_id=self.guideline_id,
            limit=self.limit,
            cursor=self.cursor,
            projection=self.projection,
        )
        object.__setattr__(self, "limit", query.limit)
        object.__setattr__(self, "cursor", query.cursor)
        object.__setattr__(self, "projection", query.projection)


@dataclass(frozen=True, slots=True)
class ListGuidelineRevisionsResult:
    page: GuidelineRevisionProjectionPage
    projection: PolicyProjection


@dataclass(frozen=True, slots=True)
class GetGuidelineRevisionCommand:
    board_id: str
    guideline_id: str
    revision_id: str

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )


@dataclass(frozen=True, slots=True)
class GetGuidelineRevisionResult:
    guideline: Guideline
    revision: GuidelineRevision
    head: GuidelineHead
    retirement: GuidelineRetirement | None


@dataclass(frozen=True, slots=True)
class CreateGuidelineRevisionCommand:
    board_id: str
    guideline_id: str
    patch: GuidelineRevisionPatch
    idempotency_key: str
    next_revision_id: str | None = None
    declared_semantic_version: str | None = None
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "guideline_id",
            _bounded_text(
                self.guideline_id,
                GUIDELINE_ID_MAX_LENGTH,
                "guideline_id_required",
            ),
        )
        if not isinstance(self.patch, GuidelineRevisionPatch):
            raise ValueError("guideline_revision_patch_invalid")
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded_text(
                self.idempotency_key,
                POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                "guideline_revision_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "next_revision_id",
            _bounded_optional_text(
                self.next_revision_id,
                GUIDELINE_REVISION_ID_MAX_LENGTH,
                "guideline_next_revision_id_invalid",
            ),
        )
        object.__setattr__(
            self,
            "declared_semantic_version",
            _bounded_optional_text(
                self.declared_semantic_version,
                GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH,
                "guideline_semantic_version_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class CreateGuidelineRevisionResult:
    status: str
    revision: GuidelineRevision | None
    head: GuidelineHead | None
    minimum_bump: Any | None
    rejection_code: str | None = None


@dataclass(frozen=True, slots=True)
class RetireGuidelineCommand:
    board_id: str
    guideline_id: str
    retirement_id: str
    status: GuidelineLifecycleStatus
    reason: str
    idempotency_key: str
    superseded_by_guideline_id: str | None = None
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("retirement_id", GUIDELINE_RETIREMENT_ID_MAX_LENGTH),
            ("idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, "reason_required"),
        )
        if not isinstance(self.status, GuidelineLifecycleStatus):
            raise ValueError("guideline_retirement_status_invalid")
        object.__setattr__(
            self,
            "superseded_by_guideline_id",
            _bounded_optional_text(
                self.superseded_by_guideline_id,
                GUIDELINE_ID_MAX_LENGTH,
                "guideline_retirement_successor_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class RetireGuidelineResult:
    retirement: GuidelineRetirement


@dataclass(frozen=True, slots=True)
class PreviewGuidelineImpactCommand:
    board_id: str
    guideline_id: str
    proposed_priority: int
    proposed_default_enforcement: GuidelineEnforcement
    idempotency_key: str
    to_revision_id: str | None = None
    requested_at: datetime | None = None

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )
        if (
            not isinstance(self.proposed_priority, int)
            or isinstance(self.proposed_priority, bool)
            or not 0 <= self.proposed_priority <= POLICY_SQL_INTEGER_MAX
        ):
            raise ValueError("guideline_impact_priority_invalid")
        if not isinstance(
            self.proposed_default_enforcement,
            GuidelineEnforcement,
        ):
            raise ValueError("guideline_impact_enforcement_invalid")
        object.__setattr__(
            self,
            "to_revision_id",
            _bounded_optional_text(
                self.to_revision_id,
                GUIDELINE_REVISION_ID_MAX_LENGTH,
                "guideline_impact_revision_id_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class PreviewGuidelineImpactResult:
    receipt: Any


@dataclass(frozen=True, slots=True)
class GetGuidelineImpactCommand:
    board_id: str
    guideline_id: str
    impact_receipt_id: str

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("impact_receipt_id", POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )


@dataclass(frozen=True, slots=True)
class GetGuidelineImpactResult:
    receipt: Any


@dataclass(frozen=True, slots=True)
class ListGuidelineImpactItemsCommand:
    guideline_id: str
    query: GuidelineImpactListQuery

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "guideline_id",
            _bounded_text(
                self.guideline_id,
                GUIDELINE_ID_MAX_LENGTH,
                "guideline_id_required",
            ),
        )
        if not isinstance(self.query, GuidelineImpactListQuery):
            raise ValueError("guideline_impact_query_invalid")
        if self.query.guideline_id != self.guideline_id:
            raise ValueError("guideline_impact_query_scope_mismatch")


@dataclass(frozen=True, slots=True)
class ListGuidelineImpactItemsResult:
    page: GuidelineImpactItemPage


@dataclass(frozen=True, slots=True)
class AdoptGuidelineRevisionCommand:
    board_id: str
    guideline_id: str
    impact_receipt_id: str
    impact_digest: str
    idempotency_key: str
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("impact_receipt_id", POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH),
            ("idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )
        object.__setattr__(
            self,
            "impact_digest",
            normalize_guideline_sha256(
                self.impact_digest,
                "impact_digest_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class AdoptGuidelineRevisionResult:
    binding: BoardGuidelineBinding
    receipt: Any


@dataclass(frozen=True, slots=True)
class EvaluatePolicyComplianceCommand:
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    idempotency_key: str
    evaluation_id: str | None = None
    requested_at: datetime | None = None
    evaluated_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "subject_id",
            _bounded_text(
                self.subject_id,
                POLICY_SUBJECT_ID_MAX_LENGTH,
                "subject_id_required",
            ),
        )
        if not isinstance(self.entity_type, PolicyEntityType):
            raise ValueError("policy_entity_type_invalid")
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded_text(
                self.idempotency_key,
                POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                "policy_evaluation_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "evaluation_id",
            _bounded_optional_text(
                self.evaluation_id,
                POLICY_EVALUATION_ID_MAX_LENGTH,
                "policy_evaluation_id_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class EvaluatePolicyComplianceResult:
    evaluation: PolicyEvaluationResult


@dataclass(frozen=True, slots=True)
class GetPolicyComplianceReceiptCommand:
    board_id: str
    receipt_id: str

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )


@dataclass(frozen=True, slots=True)
class GetPolicyComplianceReceiptResult:
    receipt: PolicyComplianceReceipt


@dataclass(frozen=True, slots=True)
class GetCurrentPolicyComplianceReceiptCommand:
    board_id: str
    entity_type: PolicyEntityType
    subject_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _bounded_text(
                self.board_id,
                POLICY_BOARD_ID_MAX_LENGTH,
                "board_id_required",
            ),
        )
        object.__setattr__(
            self,
            "subject_id",
            _bounded_text(
                self.subject_id,
                POLICY_SUBJECT_ID_MAX_LENGTH,
                "subject_id_required",
            ),
        )
        if not isinstance(self.entity_type, PolicyEntityType):
            raise ValueError("policy_entity_type_invalid")


@dataclass(frozen=True, slots=True)
class GetCurrentPolicyComplianceReceiptResult:
    receipt: PolicyComplianceReceipt


@dataclass(frozen=True, slots=True)
class ListPolicyComplianceReceiptsCommand:
    query: PolicyComplianceReceiptListQuery

    def __post_init__(self) -> None:
        if not isinstance(self.query, PolicyComplianceReceiptListQuery):
            raise ValueError("policy_receipt_query_invalid")


@dataclass(frozen=True, slots=True)
class ListPolicyComplianceReceiptsResult:
    page: PolicyComplianceReceiptPage


@dataclass(frozen=True, slots=True)
class ListPolicyComplianceFindingsCommand:
    query: PolicyComplianceFindingListQuery

    def __post_init__(self) -> None:
        if not isinstance(self.query, PolicyComplianceFindingListQuery):
            raise ValueError("policy_finding_query_invalid")


@dataclass(frozen=True, slots=True)
class ListPolicyComplianceFindingsResult:
    page: PolicyComplianceFindingPage


@dataclass(frozen=True, slots=True)
class ListPolicyWaiversCommand:
    query: PolicyWaiverListQuery

    def __post_init__(self) -> None:
        if not isinstance(self.query, PolicyWaiverListQuery):
            raise ValueError("policy_waiver_query_invalid")


@dataclass(frozen=True, slots=True)
class ListPolicyWaiversResult:
    page: PolicyWaiverPage


@dataclass(frozen=True, slots=True)
class GetPolicyWaiverCommand:
    board_id: str
    waiver_id: str

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("waiver_id", POLICY_WAIVER_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )


@dataclass(frozen=True, slots=True)
class GetPolicyWaiverResult:
    waiver: PolicyWaiver


@dataclass(frozen=True, slots=True)
class ListPolicyWaiverEventsCommand(GetPolicyWaiverCommand):
    pass


@dataclass(frozen=True, slots=True)
class ListPolicyWaiverEventsResult:
    events: tuple[PolicyWaiverEvent, ...]


@dataclass(frozen=True, slots=True)
class RequestPolicyWaiverCommand:
    board_id: str
    finding_id: str
    reason: str
    evidence_refs: tuple[str, ...]
    expires_at: datetime
    idempotency_key: str
    waiver_id: str | None = None
    event_id: str | None = None
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("finding_id", POLICY_FINDING_ID_MAX_LENGTH),
            ("idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, "reason_required"),
        )
        object.__setattr__(
            self,
            "evidence_refs",
            _evidence_refs(self.evidence_refs),
        )
        for name, max_length in (
            ("waiver_id", POLICY_WAIVER_ID_MAX_LENGTH),
            ("event_id", POLICY_WAIVER_EVENT_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_optional_text(
                    getattr(self, name),
                    max_length,
                    f"policy_{name}_invalid",
                ),
            )


@dataclass(frozen=True, slots=True)
class PolicyWaiverMutationResult:
    waiver: PolicyWaiver
    event: PolicyWaiverEvent


@dataclass(frozen=True, slots=True)
class ReviewPolicyWaiverCommand:
    board_id: str
    waiver_id: str
    approve: bool
    reason: str
    idempotency_key: str
    expected_waiver_revision: int
    evidence_refs: tuple[str, ...] = ()
    event_id: str | None = None
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("waiver_id", POLICY_WAIVER_ID_MAX_LENGTH),
            ("idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, "reason_required"),
        )
        if not isinstance(self.approve, bool):
            raise ValueError("policy_waiver_review_decision_invalid")
        if (
            not isinstance(self.expected_waiver_revision, int)
            or isinstance(self.expected_waiver_revision, bool)
            or not 1
            <= self.expected_waiver_revision
            <= POLICY_SQL_INTEGER_MAX
        ):
            raise ValueError("policy_waiver_expected_revision_invalid")
        object.__setattr__(
            self,
            "evidence_refs",
            _evidence_refs(self.evidence_refs),
        )
        object.__setattr__(
            self,
            "event_id",
            _bounded_optional_text(
                self.event_id,
                POLICY_WAIVER_EVENT_ID_MAX_LENGTH,
                "policy_waiver_event_id_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class RevokePolicyWaiverCommand:
    board_id: str
    waiver_id: str
    reason: str
    idempotency_key: str
    expected_waiver_revision: int
    evidence_refs: tuple[str, ...] = ()
    event_id: str | None = None
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        for name, max_length in (
            ("board_id", POLICY_BOARD_ID_MAX_LENGTH),
            ("waiver_id", POLICY_WAIVER_ID_MAX_LENGTH),
            ("idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text(
                    getattr(self, name),
                    max_length,
                    f"{name}_required",
                ),
            )
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, "reason_required"),
        )
        if (
            not isinstance(self.expected_waiver_revision, int)
            or isinstance(self.expected_waiver_revision, bool)
            or not 1
            <= self.expected_waiver_revision
            <= POLICY_SQL_INTEGER_MAX
        ):
            raise ValueError("policy_waiver_expected_revision_invalid")
        object.__setattr__(
            self,
            "evidence_refs",
            _evidence_refs(self.evidence_refs),
        )
        object.__setattr__(
            self,
            "event_id",
            _bounded_optional_text(
                self.event_id,
                POLICY_WAIVER_EVENT_ID_MAX_LENGTH,
                "policy_waiver_event_id_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class RevalidatePolicyWaiverCommand(RevokePolicyWaiverCommand):
    new_expires_at: datetime | None = None

    def __post_init__(self) -> None:
        RevokePolicyWaiverCommand.__post_init__(self)
        if self.new_expires_at is None:
            raise ValueError("policy_waiver_revalidation_expiry_required")


class ListGuidelineRevisionsUseCase:
    async def execute(
        self,
        command: ListGuidelineRevisionsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListGuidelineRevisionsResult:
        _require_capability(actor, REVISIONS_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        await _load_guideline_authority(
            port=port,
            actor=actor,
            board_id=command.board_id,
            guideline_id=command.guideline_id,
            require_owner=False,
            include_retired=True,
        )
        page = await port.list_revisions(
            GuidelineRevisionListQuery(
                guideline_id=command.guideline_id,
                limit=command.limit,
                cursor=command.cursor,
                projection=command.projection,
            )
        )
        return ListGuidelineRevisionsResult(
            page=GuidelineRevisionProjectionPage(
                items=tuple(
                    project_guideline_revision(
                        revision,
                        projection=command.projection,
                    )
                    for revision in page.items
                ),
                limit=page.limit,
                next_cursor=page.next_cursor,
                has_more=page.has_more,
            ),
            projection=command.projection,
        )


class GetGuidelineRevisionUseCase:
    async def execute(
        self,
        command: GetGuidelineRevisionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetGuidelineRevisionResult:
        _require_capability(actor, REVISIONS_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        authority = await _load_guideline_authority(
            port=port,
            actor=actor,
            board_id=command.board_id,
            guideline_id=command.guideline_id,
            require_owner=False,
            include_retired=True,
        )
        revision = await port.get_revision(
            guideline_id=command.guideline_id,
            revision_id=command.revision_id,
        )
        if revision is None:
            raise EntityNotFoundError("guideline_revision", command.revision_id)
        return GetGuidelineRevisionResult(
            guideline=authority.guideline,
            revision=revision,
            head=authority.head,
            retirement=authority.retirement,
        )


class CreateGuidelineRevisionUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: CreateGuidelineRevisionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CreateGuidelineRevisionResult:
        required = [REVISIONS_CREATE]
        # A closed rule collection is an authority-bearing replacement, not a
        # merge. Requiring the blocking-author capability for any supplied rule
        # set also covers removal/downgrade of an existing blocking rule without
        # trusting an inbound "contains blocking changes" assertion.
        if command.patch.rules is not None:
            required.append(RULES_AUTHOR_BLOCKING)
        _require_capability(actor, *required)
        await _require_board(uow, command.board_id, actor, write=True)
        port = uow.services.guidelines.policy_persistence()
        await _require_guideline_mutation_scope(
            port=port,
            actor=actor,
            board_id=command.board_id,
            guideline_id=command.guideline_id,
        )
        next_revision_id = (
            command.next_revision_id
            or self._id_factory("guideline-revision", command.idempotency_key)
        )
        replay = await port.get_revision_result_by_idempotency(
            guideline_id=command.guideline_id,
            idempotency_key=command.idempotency_key,
        )
        if replay is not None:
            return await _replay_guideline_revision(
                port=port,
                replay=replay,
                command=command,
                actor=actor,
                next_revision_id=next_revision_id,
            )
        authority = await _load_guideline_authority(
            port=port,
            actor=actor,
            board_id=command.board_id,
            guideline_id=command.guideline_id,
            require_owner=True,
        )
        event_at = _aware_utc(
            command.occurred_at,
            self._clock,
            "guideline_patch_time_invalid",
        )
        if command.occurred_at is None and event_at <= authority.head.updated_at:
            event_at = authority.head.updated_at + timedelta(microseconds=1)
        result = execute_guideline_patch(
            GuidelinePatchCommand(
                current_revision=authority.revision,
                current_head=authority.head,
                patch=command.patch,
                next_revision_id=next_revision_id,
                actor_id=actor.actor_id,
                occurred_at=event_at,
                idempotency_key=command.idempotency_key,
                declared_semantic_version=command.declared_semantic_version,
            ),
            retirement=authority.retirement,
        )
        if isinstance(result, GuidelinePatchRejected):
            raise GuidelineRevisionUnderBump(
                minimum_bump=result.minimum_bump,
                minimum_semantic_version=result.minimum_semantic_version,
                declared_semantic_version=result.declared_semantic_version,
            )
        if isinstance(result, GuidelinePatchNoop):
            replay = GuidelineRevisionNoopReplay(
                revision=authority.revision,
                original_head=authority.head,
                request_digest=result.request_digest,
            )

            async def mutate_noop() -> GuidelineRevisionNoopReplay:
                return await port.record_revision_noop_cas(
                    replay=replay,
                    idempotency_key=result.idempotency_key,
                )

            await _write(uow, mutate_noop)
            return CreateGuidelineRevisionResult(
                status=result.status,
                revision=None,
                head=None,
                minimum_bump=None,
            )
        if not isinstance(result, GuidelinePatchApplied):
            raise RuntimeError("guideline_patch_result_invalid")

        async def mutate() -> tuple[GuidelineRevision, GuidelineHead]:
            return await port.append_revision_cas(
                revision=result.revision,
                next_head=result.head,
                expected_head_revision=result.expected_head_revision,
                idempotency_key=result.idempotency_key,
                request_digest=result.request_digest,
            )

        revision, head = await _write(uow, mutate)
        return CreateGuidelineRevisionResult(
            status=result.status,
            revision=revision,
            head=head,
            minimum_bump=result.minimum_bump,
        )


class RetireGuidelineUseCase:
    def __init__(self, *, clock: Clock = _utc_now) -> None:
        self._clock = clock

    async def execute(
        self,
        command: RetireGuidelineCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> RetireGuidelineResult:
        _require_capability(actor, REVISIONS_RETIRE)
        await _require_board(uow, command.board_id, actor, write=True)
        port = uow.services.guidelines.policy_persistence()
        await _require_guideline_mutation_scope(
            port=port,
            actor=actor,
            board_id=command.board_id,
            guideline_id=command.guideline_id,
        )
        replay = await port.get_retirement_result_by_idempotency(
            guideline_id=command.guideline_id,
            idempotency_key=command.idempotency_key,
        )
        if replay is not None:
            return await _replay_guideline_retirement(
                port=port,
                replay=replay,
                command=command,
                actor=actor,
            )
        authority = await _load_guideline_authority(
            port=port,
            actor=actor,
            board_id=command.board_id,
            guideline_id=command.guideline_id,
            require_owner=True,
            include_retired=True,
        )
        event_at = _aware_utc(
            command.occurred_at,
            self._clock,
            "guideline_retirement_time_invalid",
        )
        if command.occurred_at is None and event_at <= authority.head.updated_at:
            event_at = authority.head.updated_at + timedelta(microseconds=1)
        plan = plan_guideline_retirement(
            GuidelineRetirementCommand(
                current_revision=authority.revision,
                current_head=authority.head,
                retirement_id=command.retirement_id,
                status=command.status,
                reason=command.reason,
                actor_id=actor.actor_id,
                occurred_at=event_at,
                idempotency_key=command.idempotency_key,
                superseded_by_guideline_id=command.superseded_by_guideline_id,
            ),
            current_retirement=authority.retirement,
        )

        async def mutate() -> GuidelineRetirement:
            return await port.retire_guideline_cas(
                retirement=plan.retirement,
                expected_head_revision=plan.expected_head_revision,
                idempotency_key=plan.idempotency_key,
                request_digest=plan.request_digest,
                actor_type=_actor_type(actor),
            )

        return RetireGuidelineResult(await _write(uow, mutate))


class PreviewGuidelineImpactUseCase:
    def __init__(self, *, clock: Clock = _utc_now) -> None:
        self._clock = clock

    async def execute(
        self,
        command: PreviewGuidelineImpactCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> PreviewGuidelineImpactResult:
        _require_capability(actor, IMPACT_PREVIEW)
        await _require_board(uow, command.board_id, actor, write=True)
        service = uow.services.guidelines
        requested_at = _aware_utc(
            command.requested_at,
            self._clock,
            "guideline_impact_requested_at_invalid",
        )

        async def mutate() -> Any:
            return await service.preview_guideline_revision_impact(
                board_id=command.board_id,
                guideline_id=command.guideline_id,
                proposed_priority=command.proposed_priority,
                proposed_default_enforcement=(
                    command.proposed_default_enforcement
                ),
                requested_by=actor.actor_id,
                idempotency_key=command.idempotency_key,
                to_revision_id=command.to_revision_id,
                requested_at=requested_at,
            )

        return PreviewGuidelineImpactResult(await _write(uow, mutate))


class GetGuidelineImpactUseCase:
    async def execute(
        self,
        command: GetGuidelineImpactCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetGuidelineImpactResult:
        _require_capability(actor, IMPACT_PREVIEW)
        await _require_board(uow, command.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        receipt = await port.get_impact_receipt(
            board_id=command.board_id,
            impact_receipt_id=command.impact_receipt_id,
        )
        if receipt is None:
            raise EntityNotFoundError(
                "guideline_impact",
                command.impact_receipt_id,
            )
        if receipt.guideline_id != command.guideline_id:
            raise EntityNotFoundError(
                "guideline_impact",
                command.impact_receipt_id,
            )
        return GetGuidelineImpactResult(receipt)


class ListGuidelineImpactItemsUseCase:
    async def execute(
        self,
        command: ListGuidelineImpactItemsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListGuidelineImpactItemsResult:
        _require_capability(actor, IMPACT_PREVIEW)
        await _require_board(uow, command.query.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        receipt = await port.get_impact_receipt(
            board_id=command.query.board_id,
            impact_receipt_id=command.query.impact_receipt_id,
        )
        if receipt is None or receipt.guideline_id != command.guideline_id:
            raise EntityNotFoundError(
                "guideline_impact",
                command.query.impact_receipt_id,
            )
        return ListGuidelineImpactItemsResult(
            await port.list_impact_items(command.query)
        )


class AdoptGuidelineRevisionUseCase:
    def __init__(self, *, clock: Clock = _utc_now) -> None:
        self._clock = clock

    async def execute(
        self,
        command: AdoptGuidelineRevisionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> AdoptGuidelineRevisionResult:
        _require_capability(actor, ADOPTION_MANAGE)
        await _require_board(uow, command.board_id, actor, write=True)
        service = uow.services.guidelines
        occurred_at = _aware_utc(
            command.occurred_at,
            self._clock,
            "guideline_adoption_time_invalid",
        )

        async def mutate() -> tuple[BoardGuidelineBinding, Any]:
            return await service.adopt_guideline_revision(
                board_id=command.board_id,
                guideline_id=command.guideline_id,
                impact_receipt_id=command.impact_receipt_id,
                impact_digest=command.impact_digest,
                actor_id=actor.actor_id,
                actor_type=_actor_type(actor),
                idempotency_key=command.idempotency_key,
                occurred_at=occurred_at,
            )

        binding, receipt = await _write(uow, mutate)
        return AdoptGuidelineRevisionResult(binding, receipt)


class EvaluatePolicyComplianceUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: EvaluatePolicyComplianceCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> EvaluatePolicyComplianceResult:
        _require_capability(actor, COMPLIANCE_EVALUATE)
        await _require_board(uow, command.board_id, actor, write=True)
        port = uow.services.guidelines.policy_persistence()
        subject = await port.resolve_policy_subject_snapshot(
            board_id=command.board_id,
            entity_type=command.entity_type,
            subject_id=command.subject_id,
            lock=False,
        )
        if subject is None:
            raise EntityNotFoundError("policy_subject", command.subject_id)
        requested_at = _aware_utc(
            command.requested_at,
            self._clock,
            "policy_evaluation_requested_at_invalid",
        )
        evaluated_at = _aware_utc(
            command.evaluated_at,
            self._clock,
            "policy_evaluation_evaluated_at_invalid",
        )
        if command.evaluated_at is None and evaluated_at < requested_at:
            evaluated_at = requested_at
        bindings = await port.list_bindings(board_id=command.board_id)
        revisions: list[GuidelineRevision] = []
        for binding in bindings:
            revision = await port.get_revision(
                guideline_id=binding.guideline_id,
                revision_id=binding.revision_id,
            )
            if revision is None:
                raise RuntimeError("guideline_binding_revision_mismatch")
            revisions.append(revision)
        evaluation_input = build_policy_evaluation_input_v1(
            evaluation_id=(
                command.evaluation_id
                or self._id_factory("policy-evaluation", command.idempotency_key)
            ),
            subject_snapshot=subject,
            bindings=bindings,
            revisions=tuple(revisions),
            requested_by=actor.actor_id,
            requested_at=requested_at,
            idempotency_key=command.idempotency_key,
        )
        request_digest = canonical_sha256(
            {
                "contract": "policy-evaluation-application-request/v1",
                "board_id": command.board_id,
                "evaluation_id": evaluation_input.evaluation_id,
                "input_digest": evaluation_input.input_digest,
                "requested_by": actor.actor_id,
            }
        )
        replay = await port.resolve_idempotent_result(
            operation="policy_evaluation",
            scope_id=command.board_id,
            idempotency_key=command.idempotency_key,
            request_digest=request_digest,
        )
        if replay is not None:
            if not isinstance(replay, PolicyEvaluationResult):
                raise RuntimeError("policy_evaluation_replay_invalid")
            return EvaluatePolicyComplianceResult(replay)

        authorizations = []
        for revision in revisions:
            for rule in revision.rules:
                if not rule.applies_to(command.entity_type):
                    continue
                authorization = await port.resolve_effective_waiver(
                    board_id=command.board_id,
                    guideline_id=revision.guideline_id,
                    revision_id=revision.revision_id,
                    rule_id=rule.rule_id,
                    entity_type=command.entity_type,
                    subject_id=command.subject_id,
                    subject_version=subject.subject.subject_version,
                    evaluated_at=evaluated_at,
                )
                if authorization is not None:
                    authorizations.append(authorization)
        output = evaluate_policy(
            evaluation_input,
            revisions=tuple(revisions),
            waivers=tuple(authorizations),
            evaluated_at=evaluated_at,
            evaluated_by=actor.actor_id,
        )
        current_snapshot = _current_snapshot_from_evaluation(evaluation_input)

        async def mutate() -> PolicyEvaluationResult:
            return await port.save_evaluation_result(
                result=output.result,
                current_snapshot=current_snapshot,
                idempotency_key=command.idempotency_key,
                request_digest=request_digest,
            )

        return EvaluatePolicyComplianceResult(await _write(uow, mutate))


class GetPolicyComplianceReceiptUseCase:
    async def execute(
        self,
        command: GetPolicyComplianceReceiptCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetPolicyComplianceReceiptResult:
        _require_capability(actor, COMPLIANCE_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        receipt = await port.get_compliance_receipt(
            board_id=command.board_id,
            receipt_id=command.receipt_id,
        )
        if receipt is None:
            raise EntityNotFoundError("policy_compliance_receipt", command.receipt_id)
        return GetPolicyComplianceReceiptResult(receipt)


class GetCurrentPolicyComplianceReceiptUseCase:
    async def execute(
        self,
        command: GetCurrentPolicyComplianceReceiptCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetCurrentPolicyComplianceReceiptResult:
        _require_capability(actor, COMPLIANCE_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        receipt = await port.get_current_compliance_receipt(
            board_id=command.board_id,
            entity_type=command.entity_type,
            subject_id=command.subject_id,
        )
        if receipt is None:
            raise EntityNotFoundError(
                "current_policy_compliance_receipt",
                command.subject_id,
            )
        return GetCurrentPolicyComplianceReceiptResult(receipt)


class ListPolicyComplianceReceiptsUseCase:
    async def execute(
        self,
        command: ListPolicyComplianceReceiptsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListPolicyComplianceReceiptsResult:
        _require_capability(actor, COMPLIANCE_READ)
        await _require_board(uow, command.query.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        return ListPolicyComplianceReceiptsResult(
            await port.list_compliance_receipts(command.query)
        )


class ListPolicyComplianceFindingsUseCase:
    async def execute(
        self,
        command: ListPolicyComplianceFindingsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListPolicyComplianceFindingsResult:
        _require_capability(actor, COMPLIANCE_READ)
        await _require_board(uow, command.query.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        return ListPolicyComplianceFindingsResult(
            await port.list_compliance_findings(command.query)
        )


class ListPolicyWaiversUseCase:
    async def execute(
        self,
        command: ListPolicyWaiversCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListPolicyWaiversResult:
        _require_capability(actor, WAIVER_READ)
        await _require_board(uow, command.query.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        return ListPolicyWaiversResult(await port.list_waivers(command.query))


class GetPolicyWaiverUseCase:
    async def execute(
        self,
        command: GetPolicyWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetPolicyWaiverResult:
        _require_capability(actor, WAIVER_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        waiver = await port.get_waiver(
            board_id=command.board_id,
            waiver_id=command.waiver_id,
        )
        if waiver is None:
            raise EntityNotFoundError("policy_waiver", command.waiver_id)
        return GetPolicyWaiverResult(waiver)


class ListPolicyWaiverEventsUseCase:
    async def execute(
        self,
        command: ListPolicyWaiverEventsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ListPolicyWaiverEventsResult:
        _require_capability(actor, WAIVER_READ)
        await _require_board(uow, command.board_id, actor, write=False)
        port = uow.services.guidelines.policy_persistence()
        events = await port.list_waiver_events(
            board_id=command.board_id,
            waiver_id=command.waiver_id,
        )
        if not events:
            waiver = await port.get_waiver(
                board_id=command.board_id,
                waiver_id=command.waiver_id,
            )
            if waiver is None:
                raise EntityNotFoundError("policy_waiver", command.waiver_id)
        return ListPolicyWaiverEventsResult(events)


class RequestPolicyWaiverUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: RequestPolicyWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> PolicyWaiverMutationResult:
        _require_capability(actor, WAIVER_REQUEST)
        await _require_board(uow, command.board_id, actor, write=True)
        request_digest = _waiver_request_digest(
            operation="request",
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
            payload={
                "finding_id": command.finding_id,
                "reason": command.reason,
                "evidence_refs": command.evidence_refs,
                "expires_at": _canonical_time(
                    command.expires_at,
                    "policy_waiver_event_expires_at_invalid",
                ),
                "waiver_id": command.waiver_id,
                "requested_by": actor.actor_id,
            },
        )
        port = uow.services.guidelines.policy_persistence()
        replay = await port.resolve_idempotent_result(
            operation="create_waiver",
            scope_id=command.board_id,
            idempotency_key=command.idempotency_key,
            request_digest=request_digest,
        )
        if replay is not None:
            if (
                not isinstance(replay, tuple)
                or len(replay) != 2
                or not isinstance(replay[0], PolicyWaiver)
                or not isinstance(replay[1], PolicyWaiverEvent)
            ):
                raise RuntimeError("policy_waiver_replay_invalid")
            return PolicyWaiverMutationResult(replay[0], replay[1])
        async def mutate() -> tuple[PolicyWaiver, PolicyWaiverEvent]:
            source = await port.resolve_policy_waiver_source(
                board_id=command.board_id,
                finding_id=command.finding_id,
                require_current=True,
                lock=True,
            )
            if source is None:
                raise EntityNotFoundError(
                    "policy_compliance_finding",
                    command.finding_id,
                )
            mutation = request_policy_waiver(
                event_id=(
                    command.event_id
                    or self._id_factory(
                        "policy-waiver-request-event",
                        command.idempotency_key,
                    )
                ),
                waiver_id=(
                    command.waiver_id
                    or self._id_factory("policy-waiver", command.idempotency_key)
                ),
                source=source,
                requester_id=actor.actor_id,
                reason=command.reason,
                evidence_refs=command.evidence_refs,
                expires_at=command.expires_at,
                occurred_at=_aware_utc(
                    command.occurred_at,
                    self._clock,
                    "policy_waiver_event_time_invalid",
                ),
            )
            return await port.create_waiver(
                mutation=mutation,
                idempotency_key=command.idempotency_key,
                request_digest=request_digest,
            )

        waiver, event = await _write(uow, mutate)
        return PolicyWaiverMutationResult(waiver, event)


async def _load_waiver_for_mutation(
    *,
    port: GuidelinePolicyPersistencePort,
    board_id: str,
    waiver_id: str,
    expected_revision: int,
) -> PolicyWaiver:
    waiver = await port.get_waiver(board_id=board_id, waiver_id=waiver_id)
    if waiver is None:
        raise EntityNotFoundError("policy_waiver", waiver_id)
    if waiver.waiver_revision != expected_revision:
        from okto_pulse.core.ports.guideline_policy import GuidelinePolicyCasConflict

        raise GuidelinePolicyCasConflict(
            "policy_waiver_compare_and_swap_conflict",
            details=(
                ("expected_revision", str(expected_revision)),
                ("current_revision", str(waiver.waiver_revision)),
            ),
        )
    return waiver


async def _waiver_transition_replay(
    *,
    port: GuidelinePolicyPersistencePort,
    board_id: str,
    idempotency_key: str,
    request_digest: str,
) -> PolicyWaiverMutationResult | None:
    replay = await port.resolve_idempotent_result(
        operation="transition_waiver_cas",
        scope_id=board_id,
        idempotency_key=idempotency_key,
        request_digest=request_digest,
    )
    if replay is None:
        return None
    if (
        not isinstance(replay, tuple)
        or len(replay) != 2
        or not isinstance(replay[0], PolicyWaiver)
        or not isinstance(replay[1], PolicyWaiverEvent)
    ):
        raise RuntimeError("policy_waiver_replay_invalid")
    return PolicyWaiverMutationResult(replay[0], replay[1])


class ReviewPolicyWaiverUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: ReviewPolicyWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> PolicyWaiverMutationResult:
        _require_capability(actor, WAIVER_REVIEW)
        await _require_board(uow, command.board_id, actor, write=True)
        port = uow.services.guidelines.policy_persistence()
        event_type = (
            PolicyWaiverEventType.APPROVE
            if command.approve
            else PolicyWaiverEventType.REJECT
        )
        request_digest = _waiver_request_digest(
            operation=event_type.value,
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
            payload={
                "waiver_id": command.waiver_id,
                "expected_waiver_revision": command.expected_waiver_revision,
                "reason": command.reason,
                "evidence_refs": command.evidence_refs,
                "actor_id": actor.actor_id,
            },
        )
        replay = await _waiver_transition_replay(
            port=port,
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
            request_digest=request_digest,
        )
        if replay is not None:
            return replay
        waiver = await _load_waiver_for_mutation(
            port=port,
            board_id=command.board_id,
            waiver_id=command.waiver_id,
            expected_revision=command.expected_waiver_revision,
        )
        async def mutate() -> tuple[PolicyWaiver, PolicyWaiverEvent]:
            source = (
                await port.resolve_policy_waiver_source(
                    board_id=command.board_id,
                    finding_id=waiver.finding_id,
                    require_current=True,
                    lock=True,
                )
                if command.approve
                else None
            )
            if command.approve and source is None:
                raise EntityNotFoundError(
                    "policy_compliance_finding",
                    waiver.finding_id,
                )
            mutation = transition_policy_waiver(
                waiver=waiver,
                event_id=(
                    command.event_id
                    or self._id_factory(
                        "policy-waiver-review-event",
                        command.idempotency_key,
                    )
                ),
                event_type=event_type,
                actor_id=actor.actor_id,
                reason=command.reason,
                occurred_at=_aware_utc(
                    command.occurred_at,
                    self._clock,
                    "policy_waiver_event_time_invalid",
                ),
                expected_waiver_revision=command.expected_waiver_revision,
                evidence_refs=command.evidence_refs,
                source=source,
            )
            return await port.transition_waiver_cas(
                mutation=mutation,
                expected_waiver_revision=command.expected_waiver_revision,
                idempotency_key=command.idempotency_key,
                request_digest=request_digest,
            )

        next_waiver, event = await _write(uow, mutate)
        return PolicyWaiverMutationResult(next_waiver, event)


class RevokePolicyWaiverUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: RevokePolicyWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> PolicyWaiverMutationResult:
        _require_capability(actor, WAIVER_REVOKE)
        await _require_board(uow, command.board_id, actor, write=True)
        port = uow.services.guidelines.policy_persistence()
        request_digest = _waiver_request_digest(
            operation=PolicyWaiverEventType.REVOKE.value,
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
            payload={
                "waiver_id": command.waiver_id,
                "expected_waiver_revision": command.expected_waiver_revision,
                "reason": command.reason,
                "evidence_refs": command.evidence_refs,
                "actor_id": actor.actor_id,
            },
        )
        replay = await _waiver_transition_replay(
            port=port,
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
            request_digest=request_digest,
        )
        if replay is not None:
            return replay
        waiver = await _load_waiver_for_mutation(
            port=port,
            board_id=command.board_id,
            waiver_id=command.waiver_id,
            expected_revision=command.expected_waiver_revision,
        )
        mutation = transition_policy_waiver(
            waiver=waiver,
            event_id=(
                command.event_id
                or self._id_factory("policy-waiver-revoke-event", command.idempotency_key)
            ),
            event_type=PolicyWaiverEventType.REVOKE,
            actor_id=actor.actor_id,
            reason=command.reason,
            occurred_at=_aware_utc(
                command.occurred_at,
                self._clock,
                "policy_waiver_event_time_invalid",
            ),
            expected_waiver_revision=command.expected_waiver_revision,
            evidence_refs=command.evidence_refs,
        )

        async def mutate() -> tuple[PolicyWaiver, PolicyWaiverEvent]:
            return await port.transition_waiver_cas(
                mutation=mutation,
                expected_waiver_revision=command.expected_waiver_revision,
                idempotency_key=command.idempotency_key,
                request_digest=request_digest,
            )

        next_waiver, event = await _write(uow, mutate)
        return PolicyWaiverMutationResult(next_waiver, event)


class RevalidatePolicyWaiverUseCase:
    def __init__(
        self,
        *,
        clock: Clock = _utc_now,
        id_factory: IdFactory = _uuid5,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    async def execute(
        self,
        command: RevalidatePolicyWaiverCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> PolicyWaiverMutationResult:
        _require_capability(actor, WAIVER_REVALIDATE)
        await _require_board(uow, command.board_id, actor, write=True)
        port = uow.services.guidelines.policy_persistence()
        request_digest = _waiver_request_digest(
            operation=PolicyWaiverEventType.REVALIDATE.value,
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
            payload={
                "waiver_id": command.waiver_id,
                "expected_waiver_revision": command.expected_waiver_revision,
                "reason": command.reason,
                "evidence_refs": command.evidence_refs,
                "new_expires_at": _canonical_time(
                    command.new_expires_at,
                    "policy_waiver_event_expires_at_invalid",
                ),
                "actor_id": actor.actor_id,
            },
        )
        replay = await _waiver_transition_replay(
            port=port,
            board_id=command.board_id,
            idempotency_key=command.idempotency_key,
            request_digest=request_digest,
        )
        if replay is not None:
            return replay
        waiver = await _load_waiver_for_mutation(
            port=port,
            board_id=command.board_id,
            waiver_id=command.waiver_id,
            expected_revision=command.expected_waiver_revision,
        )
        async def mutate() -> tuple[PolicyWaiver, PolicyWaiverEvent]:
            source = await port.resolve_policy_waiver_source(
                board_id=command.board_id,
                finding_id=waiver.finding_id,
                require_current=True,
                lock=True,
            )
            if source is None:
                raise EntityNotFoundError(
                    "policy_compliance_finding",
                    waiver.finding_id,
                )
            mutation = transition_policy_waiver(
                waiver=waiver,
                event_id=(
                    command.event_id
                    or self._id_factory(
                        "policy-waiver-revalidate-event",
                        command.idempotency_key,
                    )
                ),
                event_type=PolicyWaiverEventType.REVALIDATE,
                actor_id=actor.actor_id,
                reason=command.reason,
                occurred_at=_aware_utc(
                    command.occurred_at,
                    self._clock,
                    "policy_waiver_event_time_invalid",
                ),
                expected_waiver_revision=command.expected_waiver_revision,
                evidence_refs=command.evidence_refs,
                new_expires_at=command.new_expires_at,
                source=source,
            )
            return await port.transition_waiver_cas(
                mutation=mutation,
                expected_waiver_revision=command.expected_waiver_revision,
                idempotency_key=command.idempotency_key,
                request_digest=request_digest,
            )

        next_waiver, event = await _write(uow, mutate)
        return PolicyWaiverMutationResult(next_waiver, event)


__all__ = [
    "ADOPTION_MANAGE",
    "COMPLIANCE_EVALUATE",
    "COMPLIANCE_READ",
    "IMPACT_PREVIEW",
    "POLICY_GOVERNANCE_CAPABILITIES",
    "REVISIONS_CREATE",
    "REVISIONS_READ",
    "REVISIONS_RETIRE",
    "RULES_AUTHOR_BLOCKING",
    "WAIVER_READ",
    "WAIVER_REQUEST",
    "WAIVER_REVALIDATE",
    "WAIVER_REVIEW",
    "WAIVER_REVOKE",
    "AdoptGuidelineRevisionCommand",
    "AdoptGuidelineRevisionResult",
    "AdoptGuidelineRevisionUseCase",
    "CreateGuidelineRevisionCommand",
    "CreateGuidelineRevisionResult",
    "CreateGuidelineRevisionUseCase",
    "GuidelineRevisionUnderBump",
    "EvaluatePolicyComplianceCommand",
    "EvaluatePolicyComplianceResult",
    "EvaluatePolicyComplianceUseCase",
    "GetCurrentPolicyComplianceReceiptCommand",
    "GetCurrentPolicyComplianceReceiptResult",
    "GetCurrentPolicyComplianceReceiptUseCase",
    "GetGuidelineImpactCommand",
    "GetGuidelineImpactResult",
    "GetGuidelineImpactUseCase",
    "GetGuidelineRevisionCommand",
    "GetGuidelineRevisionResult",
    "GetGuidelineRevisionUseCase",
    "GetPolicyComplianceReceiptCommand",
    "GetPolicyComplianceReceiptResult",
    "GetPolicyComplianceReceiptUseCase",
    "GetPolicyWaiverCommand",
    "GetPolicyWaiverResult",
    "GetPolicyWaiverUseCase",
    "ListGuidelineImpactItemsCommand",
    "ListGuidelineImpactItemsResult",
    "ListGuidelineImpactItemsUseCase",
    "ListGuidelineRevisionsCommand",
    "ListGuidelineRevisionsResult",
    "ListGuidelineRevisionsUseCase",
    "ListPolicyComplianceFindingsCommand",
    "ListPolicyComplianceFindingsResult",
    "ListPolicyComplianceFindingsUseCase",
    "ListPolicyComplianceReceiptsCommand",
    "ListPolicyComplianceReceiptsResult",
    "ListPolicyComplianceReceiptsUseCase",
    "ListPolicyWaiverEventsCommand",
    "ListPolicyWaiverEventsResult",
    "ListPolicyWaiverEventsUseCase",
    "ListPolicyWaiversCommand",
    "ListPolicyWaiversResult",
    "ListPolicyWaiversUseCase",
    "PolicyWaiverMutationResult",
    "PreviewGuidelineImpactCommand",
    "PreviewGuidelineImpactResult",
    "PreviewGuidelineImpactUseCase",
    "RequestPolicyWaiverCommand",
    "RequestPolicyWaiverUseCase",
    "RetireGuidelineCommand",
    "RetireGuidelineResult",
    "RetireGuidelineUseCase",
    "ReviewPolicyWaiverCommand",
    "ReviewPolicyWaiverUseCase",
    "RevalidatePolicyWaiverCommand",
    "RevalidatePolicyWaiverUseCase",
    "RevokePolicyWaiverCommand",
    "RevokePolicyWaiverUseCase",
    "require_policy_governance_capabilities",
]
