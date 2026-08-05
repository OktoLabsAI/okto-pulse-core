"""Transport-free application boundary for governed semantic guidelines.

The immutable guideline domain and persistence port intentionally expose more
power than an inbound adapter may call directly.  This module is the shared
REST/MCP application boundary: it checks the closed SK-B capability before
touching the unit of work, proves board/owner visibility, builds the canonical
domain plans, and gives every mutation one commit/rollback boundary.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
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
    PolicyProjection,
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
    POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
    POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
    BoardGuidelineBinding,
    Guideline,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelineRetirement,
    GuidelineRevision,
    normalize_guideline_sha256,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticGuidelineAssessmentContext,
    SemanticGuidelineAssessmentResult,
    SemanticGuidelineAssessmentSubmission,
    record_semantic_guideline_assessment,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    semantic_assessment_current_snapshot_from_context,
)
from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.ports.guideline_policy import (
    GuidelineImpactListQuery,
    GuidelinePolicyDigestConflict,
    GuidelinePolicyIdempotencyConflict,
    GuidelinePolicyPersistencePort,
    GuidelineRetirementReplay,
    GuidelineRevisionNoopReplay,
    GuidelineRevisionReplay,
    GuidelineRevisionListQuery,
    SemanticGuidelineAssessmentPersistencePort,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


Clock = Callable[[], datetime]
IdFactory = Callable[[str, str], str]

_READ_SHARES = None
_WRITE_SHARES = frozenset({"editor", "admin"})

REVISIONS_READ = "guidelines.revisions.read"
REVISIONS_CREATE = "guidelines.revisions.create"
REVISIONS_RETIRE = "guidelines.revisions.retire"
METRICS_AUTHOR = "guidelines.metrics.author"
IMPACT_PREVIEW = "guidelines.impact.preview"
ADOPTION_MANAGE = "guidelines.adoption.manage"
ASSESSMENTS_READ = "guidelines.assessments.read"
ASSESSMENTS_RECORD = "guidelines.assessments.record"
WAIVER_READ = "guidelines.waiver.read"
WAIVER_REQUEST = "guidelines.waiver.request"
WAIVER_REVIEW = "guidelines.waiver.review"
WAIVER_REVOKE = "guidelines.waiver.revoke"
WAIVER_REVALIDATE = "guidelines.waiver.revalidate"

POLICY_GOVERNANCE_CAPABILITIES = (
    REVISIONS_READ,
    REVISIONS_CREATE,
    REVISIONS_RETIRE,
    METRICS_AUTHOR,
    IMPACT_PREVIEW,
    ADOPTION_MANAGE,
    ASSESSMENTS_READ,
    ASSESSMENTS_RECORD,
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


def _aware_utc(value: datetime | None, clock: Clock, code: str) -> datetime:
    resolved = clock() if value is None else value
    if (
        not isinstance(resolved, datetime)
        or resolved.tzinfo is None
        or resolved.utcoffset() is None
    ):
        raise ValueError(code)
    return resolved.astimezone(timezone.utc)


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
    """Prove guideline ownership without widening the board authority path.

    A global guideline is authored independently from any board binding, so its
    exact owner may create revisions or retire it before the first adoption (or
    after every board has unlinked it).  The caller still has to pass the board
    access preflight and the operation capability before reaching this helper.

    A board binding never grants global-authoring authority to a non-owner.
    The normal planner path reloads the stricter authority below before
    applying a mutation.
    """

    identity = await port.get_guideline(guideline_id=guideline_id)
    if identity is None:
        raise EntityNotFoundError("guideline", guideline_id)
    if identity.board_id is not None:
        if identity.board_id != board_id:
            raise EntityNotFoundError("guideline", guideline_id)
        return identity
    if identity.owner_id == actor.actor_id:
        return identity
    raise EntityNotFoundError("guideline", guideline_id)


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
        is_owner = identity.owner_id == actor.actor_id
        if not is_owner:
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
                or require_owner
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
            or candidate.expected_head_revision != replay.original_head.head_revision
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
    proposed_enforcement: GuidelineEnforcement
    proposed_minimum_confidence: int
    proposed_metric_threshold_overrides: Mapping[str, int]
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
            self.proposed_enforcement,
            GuidelineEnforcement,
        ):
            raise ValueError("guideline_impact_enforcement_invalid")
        if (
            not isinstance(self.proposed_minimum_confidence, int)
            or isinstance(self.proposed_minimum_confidence, bool)
            or not 0 <= self.proposed_minimum_confidence <= 100
            or not isinstance(self.proposed_metric_threshold_overrides, Mapping)
        ):
            raise ValueError("guideline_impact_semantic_configuration_invalid")
        normalized_overrides: dict[str, int] = {}
        for metric_code, threshold in self.proposed_metric_threshold_overrides.items():
            code = _required_text(
                metric_code,
                "guideline_impact_metric_code_invalid",
            )
            if (
                code in normalized_overrides
                or not isinstance(threshold, int)
                or isinstance(threshold, bool)
                or not 0 <= threshold <= 100
            ):
                raise ValueError("guideline_impact_metric_threshold_override_invalid")
            normalized_overrides[code] = threshold
        object.__setattr__(
            self,
            "proposed_metric_threshold_overrides",
            dict(sorted(normalized_overrides.items())),
        )
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
class RecordSemanticGuidelineAssessmentCommand:
    board_id: str
    submission: SemanticGuidelineAssessmentSubmission
    receipt_id: str | None = None
    recorded_at: datetime | None = None

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
        if not isinstance(
            self.submission,
            SemanticGuidelineAssessmentSubmission,
        ):
            raise ValueError("semantic_assessment_submission_invalid")
        if self.submission.subject.board_id != self.board_id:
            raise ValueError("semantic_assessment_subject_board_mismatch")
        object.__setattr__(
            self,
            "receipt_id",
            _bounded_optional_text(
                self.receipt_id,
                POLICY_RECEIPT_ID_MAX_LENGTH,
                "semantic_assessment_receipt_id_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class RecordSemanticGuidelineAssessmentResult:
    assessment: SemanticGuidelineAssessmentResult

    def __post_init__(self) -> None:
        if not isinstance(
            self.assessment,
            SemanticGuidelineAssessmentResult,
        ):
            raise ValueError("semantic_assessment_result_invalid")


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
        # Metrics are an authority-bearing closed replacement, never an
        # inferred merge. Any supplied set (including removal) requires the
        # explicit semantic-metric authoring capability.
        if command.patch.metrics is not None:
            required.append(METRICS_AUTHOR)
        _require_capability(actor, *required)
        await _require_board(uow, command.board_id, actor, write=True)
        port = uow.services.guidelines.policy_persistence()
        await _require_guideline_mutation_scope(
            port=port,
            actor=actor,
            board_id=command.board_id,
            guideline_id=command.guideline_id,
        )
        next_revision_id = command.next_revision_id or self._id_factory(
            "guideline-revision", command.idempotency_key
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
                proposed_enforcement=command.proposed_enforcement,
                proposed_minimum_confidence=(command.proposed_minimum_confidence),
                proposed_metric_threshold_overrides=(
                    command.proposed_metric_threshold_overrides
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


def _semantic_assessment_replay_matches(
    replay: SemanticGuidelineAssessmentResult,
    submission: SemanticGuidelineAssessmentSubmission,
) -> bool:
    receipt = replay.receipt
    if (
        receipt.subject != submission.subject
        or receipt.binding_id != submission.binding_id
        or receipt.binding_revision != submission.expected_binding_revision
        or receipt.guideline_revision_id != submission.guideline_revision_id
        or receipt.idempotency_key != submission.idempotency_key
        or receipt.assessor != submission.assessor
        or receipt.confidence != submission.confidence
        or len(receipt.metric_results) != len(submission.metric_results)
    ):
        return False
    recorded_by_metric = {result.metric_id: result for result in receipt.metric_results}
    for submitted in submission.metric_results:
        recorded = recorded_by_metric.get(submitted.metric_id)
        if recorded is None or (
            recorded.score != submitted.score
            or recorded.rationale != submitted.rationale
            or recorded.evidence_refs != submitted.evidence_refs
            or tuple(
                (
                    pinpoint.anchor_type,
                    pinpoint.anchor_ref,
                    pinpoint.excerpt_hash,
                )
                for pinpoint in recorded.pinpoints
            )
            != tuple(
                (
                    pinpoint.anchor_type,
                    pinpoint.anchor_ref,
                    pinpoint.excerpt_hash,
                )
                for pinpoint in submitted.pinpoints
            )
        ):
            return False
    return True


class RecordSemanticGuidelineAssessmentUseCase:
    """Validate and atomically persist external cognition against exact fences."""

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
        command: RecordSemanticGuidelineAssessmentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> RecordSemanticGuidelineAssessmentResult:
        _require_capability(actor, ASSESSMENTS_RECORD)
        await _require_board(uow, command.board_id, actor, write=True)
        submission = command.submission
        if submission.assessor.agent_id != actor.actor_id:
            raise PermissionDeniedError("semantic_assessment_assessor_mismatch")
        port = uow.services.guidelines.policy_persistence()
        semantic_port: SemanticGuidelineAssessmentPersistencePort = (
            uow.services.guidelines.semantic_policy_persistence()
        )
        replay = await semantic_port.get_semantic_assessment_result_by_idempotency(
            board_id=command.board_id,
            binding_id=submission.binding_id,
            idempotency_key=submission.idempotency_key,
        )
        if replay is not None:
            if not _semantic_assessment_replay_matches(replay, submission):
                raise GuidelinePolicyIdempotencyConflict(
                    "semantic_assessment_idempotency_conflict"
                )
            return RecordSemanticGuidelineAssessmentResult(
                replace(replay, replayed=True)
            )

        subject_snapshot = await semantic_port.resolve_policy_subject_snapshot(
            board_id=command.board_id,
            entity_type=submission.subject.entity_type,
            subject_id=submission.subject.subject_id,
            lock=True,
        )
        if subject_snapshot is None:
            raise EntityNotFoundError(
                "policy_subject",
                submission.subject.subject_id,
            )
        bindings = tuple(
            binding
            for binding in await port.list_bindings(board_id=command.board_id)
            if binding.state is GuidelineBindingState.ACTIVE
        )
        selected = tuple(
            binding
            for binding in bindings
            if binding.binding_id == submission.binding_id
        )
        if len(selected) != 1:
            raise EntityNotFoundError(
                "guideline_binding",
                submission.binding_id,
            )
        binding = selected[0]
        selected_revision: GuidelineRevision | None = None
        for active_binding in bindings:
            revision = await port.get_revision(
                guideline_id=active_binding.guideline_id,
                revision_id=active_binding.revision_id,
            )
            if revision is None:
                raise RuntimeError("guideline_binding_revision_mismatch")
            if active_binding.binding_id == binding.binding_id:
                selected_revision = revision
        if (
            selected_revision is None
            or selected_revision.revision_id != submission.guideline_revision_id
        ):
            raise EntityNotFoundError(
                "guideline_revision",
                submission.guideline_revision_id,
            )
        current_snapshot = (
            await semantic_port.resolve_semantic_assessment_current_snapshot(
                board_id=command.board_id,
                entity_type=submission.subject.entity_type,
                subject_id=submission.subject.subject_id,
                binding_id=binding.binding_id,
                lock=True,
            )
        )
        if current_snapshot is None:
            raise EntityNotFoundError(
                "guideline_binding",
                submission.binding_id,
            )
        context = SemanticGuidelineAssessmentContext(
            subject_snapshot=subject_snapshot,
            binding=binding,
            revision=selected_revision,
            policy_set_digest=current_snapshot.policy_set_digest,
            binding_head_digest=current_snapshot.binding_head_digest,
        )
        if (
            semantic_assessment_current_snapshot_from_context(context)
            != current_snapshot
        ):
            raise GuidelinePolicyDigestConflict("semantic_assessment_authority_stale")
        recorded_at = _aware_utc(
            command.recorded_at,
            self._clock,
            "semantic_assessment_recorded_at_invalid",
        )
        result = record_semantic_guideline_assessment(
            submission,
            context,
            receipt_id=(
                command.receipt_id
                or self._id_factory(
                    "semantic-guideline-assessment",
                    f"{command.board_id}:{submission.idempotency_key}",
                )
            ),
            recorded_at=recorded_at,
        )

        async def mutate() -> SemanticGuidelineAssessmentResult:
            return await semantic_port.save_semantic_assessment_result(
                result=result,
                request_digest=result.request_digest,
            )

        saved = await _write(uow, mutate)
        return RecordSemanticGuidelineAssessmentResult(saved)


__all__ = [
    "ADOPTION_MANAGE",
    "ASSESSMENTS_READ",
    "ASSESSMENTS_RECORD",
    "IMPACT_PREVIEW",
    "METRICS_AUTHOR",
    "POLICY_GOVERNANCE_CAPABILITIES",
    "REVISIONS_CREATE",
    "REVISIONS_READ",
    "REVISIONS_RETIRE",
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
    "GetGuidelineImpactCommand",
    "GetGuidelineImpactResult",
    "GetGuidelineImpactUseCase",
    "GetGuidelineRevisionCommand",
    "GetGuidelineRevisionResult",
    "GetGuidelineRevisionUseCase",
    "ListGuidelineImpactItemsCommand",
    "ListGuidelineImpactItemsResult",
    "ListGuidelineImpactItemsUseCase",
    "ListGuidelineRevisionsCommand",
    "ListGuidelineRevisionsResult",
    "ListGuidelineRevisionsUseCase",
    "PreviewGuidelineImpactCommand",
    "PreviewGuidelineImpactResult",
    "PreviewGuidelineImpactUseCase",
    "RecordSemanticGuidelineAssessmentCommand",
    "RecordSemanticGuidelineAssessmentResult",
    "RecordSemanticGuidelineAssessmentUseCase",
    "RetireGuidelineCommand",
    "RetireGuidelineResult",
    "RetireGuidelineUseCase",
    "require_policy_governance_capabilities",
]
