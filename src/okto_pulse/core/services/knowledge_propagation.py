"""Pure orchestration for selective Knowledge Base propagation v2.

The service validates complete requests, performs idempotency replay lookup
before optimistic-revision checks, builds one immutable mutation plan in
memory, and asks the edition adapter to stage that plan exactly once.  It
never commits, rolls back, imports persistence models, or infers lineage
identity.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from types import MappingProxyType
from typing import Any, cast
from uuid import uuid4

from okto_pulse.core.domain.knowledge_selection import (
    KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
    KnowledgeAssignment,
    KnowledgeAssignmentState,
    KnowledgeOriginClass,
    KnowledgePropagationMode,
    KnowledgeRelevanceEntityType,
    KnowledgeRelevanceLink,
    KnowledgeSelection,
    KnowledgeSelectionState,
)
from okto_pulse.core.domain.resource_revision import ResourceRevisionStamp
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgeIdempotencyLookup,
    KnowledgeLegacyAttachment,
    KnowledgeMutationAttempt,
    KnowledgeMutationKind,
    KnowledgeMutationLedgerEntry,
    KnowledgeMutationOutcome,
    KnowledgeMutationPlan,
    KnowledgeMutationReceipt,
    KnowledgePropagationPort,
    KnowledgePropagationPortError,
    KnowledgePropagationScope,
    KnowledgePropagationSnapshot,
    KnowledgePropagationTombstone,
    KnowledgeRecordKind,
    KnowledgeScopeLookup,
    KnowledgeSelectableSource,
    KnowledgeTargetKey,
    KnowledgeTemporalWindow,
    KnowledgeSupersessionLink,
    TemporalKnowledgeAssignment,
    get_knowledge_propagation_port,
)


class KnowledgePropagationServiceError(RuntimeError):
    """Stable application error independent of REST, MCP, and persistence."""

    def __init__(
        self,
        code: str,
        detail: str,
        *,
        details: Mapping[str, object] | None = None,
        ledger_attempt: KnowledgeMutationAttempt | None = None,
    ) -> None:
        self.code = _required_text(code, "error_code")
        self.detail = _required_text(detail, "error_detail")
        if details is not None and not isinstance(details, Mapping):
            raise TypeError("knowledge_propagation_error_details_invalid")
        if ledger_attempt is not None and not isinstance(
            ledger_attempt,
            KnowledgeMutationAttempt,
        ):
            raise TypeError("knowledge_propagation_error_ledger_attempt_invalid")
        self.details = MappingProxyType(dict(details or {}))
        self.ledger_attempt = ledger_attempt
        super().__init__(self.code)

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "detail": self.detail,
            "details": dict(self.details),
        }

    def to_error_dict(self) -> dict[str, object]:
        return self.as_dict()


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    return normalized


def _optional_text(value: object | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    normalized = value.strip()
    return normalized or None


def _revision(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("knowledge_propagation_expected_revision_invalid")
    return value


def _canonical_ids(
    values: Sequence[str],
    field_name: str,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    return tuple(sorted({_required_text(value, field_name) for value in values}))


def _canonical_links(
    values: Sequence[KnowledgeRelevanceLink],
) -> tuple[KnowledgeRelevanceLink, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError("knowledge_propagation_relevance_links_invalid")
    links: dict[tuple[str, str], KnowledgeRelevanceLink] = {}
    for value in values:
        if not isinstance(value, KnowledgeRelevanceLink):
            raise ValueError("knowledge_propagation_relevance_links_invalid")
        key = (
            cast(KnowledgeRelevanceEntityType, value.entity_type).value,
            value.entity_id,
        )
        if key in links:
            raise ValueError("knowledge_propagation_relevance_links_duplicate")
        links[key] = value
    return tuple(links[key] for key in sorted(links))


@dataclass(frozen=True, slots=True)
class KnowledgeMutationCommand:
    """Replace, omitted, explicit-empty, or drop-delta command."""

    target: KnowledgeTargetKey
    selection: KnowledgeSelection
    actor_id: str
    expected_revision: int
    idempotency_key: str
    justification: str | None = None
    relevance_links: tuple[KnowledgeRelevanceLink, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_command_target_invalid")
        if not isinstance(self.selection, KnowledgeSelection):
            raise ValueError("knowledge_propagation_command_selection_invalid")
        actor_id = _required_text(self.actor_id, "actor_id")
        expected_revision = _revision(self.expected_revision)
        idempotency_key = _required_text(
            self.idempotency_key,
            "idempotency_key",
        )
        justification = _optional_text(self.justification, "justification")
        if (
            self.selection.selection_state is not KnowledgeSelectionState.OMITTED
            and justification is None
        ):
            raise ValueError("knowledge_propagation_justification_required")
        object.__setattr__(self, "actor_id", actor_id)
        object.__setattr__(self, "expected_revision", expected_revision)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "justification", justification)
        object.__setattr__(
            self,
            "relevance_links",
            _canonical_links(self.relevance_links),
        )


@dataclass(frozen=True, slots=True)
class KnowledgeRefreshCommand:
    """Explicit refresh of selected current snapshot assignments."""

    target: KnowledgeTargetKey
    assignment_ids: tuple[str, ...]
    actor_id: str
    justification: str
    expected_revision: int
    idempotency_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_refresh_target_invalid")
        assignment_ids = _canonical_ids(self.assignment_ids, "assignment_ids")
        if not assignment_ids:
            raise ValueError("knowledge_propagation_assignment_ids_empty")
        object.__setattr__(self, "assignment_ids", assignment_ids)
        object.__setattr__(
            self,
            "actor_id",
            _required_text(self.actor_id, "actor_id"),
        )
        object.__setattr__(
            self,
            "justification",
            _required_text(self.justification, "justification"),
        )
        object.__setattr__(
            self,
            "expected_revision",
            _revision(self.expected_revision),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(self.idempotency_key, "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class KnowledgeGrandfatherEvidence:
    """Bounded evidence used to classify one physical legacy attachment."""

    durable_selection_evidence: bool = False
    origin_missing: bool = False
    origin_cycle: bool = False
    content_divergent: bool = False

    def __post_init__(self) -> None:
        for field_name in (
            "durable_selection_evidence",
            "origin_missing",
            "origin_cycle",
            "content_divergent",
        ):
            if type(getattr(self, field_name)) is not bool:
                raise ValueError(f"knowledge_propagation_{field_name}_invalid")


def classify_legacy_origin(
    evidence: KnowledgeGrandfatherEvidence,
) -> KnowledgeOriginClass:
    """Classify conservatively without rewriting the physical attachment."""

    if not isinstance(evidence, KnowledgeGrandfatherEvidence):
        raise TypeError("knowledge_propagation_grandfather_evidence_invalid")
    if evidence.origin_missing or evidence.origin_cycle or evidence.content_divergent:
        return KnowledgeOriginClass.LEGACY_UNRESOLVED
    if evidence.durable_selection_evidence:
        return KnowledgeOriginClass.SELECTED_LEGACY
    return KnowledgeOriginClass.LEGACY_ALL


@dataclass(frozen=True, slots=True)
class ResolvedKnowledgeAssignment:
    """Read-side resolution of one current v2 assignment."""

    assignment: KnowledgeAssignment
    state: KnowledgeAssignmentState
    effective: bool
    revision_stamp: ResourceRevisionStamp
    content_bytes: bytes | None = field(default=None, repr=False)
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.assignment, KnowledgeAssignment):
            raise ValueError("knowledge_propagation_resolved_assignment_invalid")
        if not isinstance(self.state, KnowledgeAssignmentState):
            raise ValueError("knowledge_propagation_resolved_state_invalid")
        if type(self.effective) is not bool:
            raise ValueError("knowledge_propagation_resolved_effective_invalid")
        if not isinstance(self.revision_stamp, ResourceRevisionStamp):
            raise ValueError("knowledge_propagation_resolved_stamp_invalid")
        if self.content_bytes is not None and not isinstance(self.content_bytes, bytes):
            raise ValueError("knowledge_propagation_resolved_content_invalid")
        object.__setattr__(self, "reason", _optional_text(self.reason, "reason"))

    def to_dict(self) -> dict[str, object]:
        return {
            "assignment": self.assignment.to_dict(),
            "state": self.state.value,
            "effective": self.effective,
            "revision_stamp": self.revision_stamp.to_dict(),
            "content_available": self.content_bytes is not None,
            "content_size_bytes": (
                None if self.content_bytes is None else len(self.content_bytes)
            ),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class KnowledgePropagationReadResult:
    """Dual-read result that never hides physical legacy history."""

    target: KnowledgeTargetKey
    scope_revision: int
    v2_active: bool
    selection_state: KnowledgeSelectionState | None
    resolved_assignments: tuple[ResolvedKnowledgeAssignment, ...]
    effective_legacy_attachments: tuple[KnowledgeLegacyAttachment, ...]
    history_assignments: tuple[TemporalKnowledgeAssignment, ...]
    history_legacy_attachments: tuple[KnowledgeLegacyAttachment, ...]
    tombstones: tuple[KnowledgePropagationTombstone, ...]
    snapshots: tuple[KnowledgePropagationSnapshot, ...]

    @property
    def effective_assignments(self) -> tuple[ResolvedKnowledgeAssignment, ...]:
        return tuple(item for item in self.resolved_assignments if item.effective)

    @property
    def effective_count(self) -> int:
        return len(self.effective_assignments) + len(self.effective_legacy_attachments)

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
            "target": self.target.to_dict(),
            "scope_revision": self.scope_revision,
            "v2_active": self.v2_active,
            "selection_state": (
                None if self.selection_state is None else self.selection_state.value
            ),
            "resolved_assignments": [
                item.to_dict() for item in self.resolved_assignments
            ],
            "effective_assignment_ids": [
                item.assignment.assignment_id for item in self.effective_assignments
            ],
            "effective_legacy_attachments": [
                item.to_dict() for item in self.effective_legacy_attachments
            ],
            "history_assignments": [
                item.to_dict() for item in self.history_assignments
            ],
            "history_legacy_attachments": [
                item.to_dict() for item in self.history_legacy_attachments
            ],
            "tombstones": [item.to_dict() for item in self.tombstones],
            "snapshots": [item.to_dict() for item in self.snapshots],
            "effective_count": self.effective_count,
        }


class KnowledgePropagationService:
    """Validate, plan, stage, refresh, and resolve selective propagation."""

    def __init__(
        self,
        port: KnowledgePropagationPort | None = None,
        *,
        now: Callable[[], datetime] | None = None,
        id_factory: Callable[[str], str] | None = None,
    ) -> None:
        self._configured_port = port
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._id_factory = id_factory or (lambda prefix: f"{prefix}_{uuid4().hex}")

    @property
    def _port(self) -> KnowledgePropagationPort:
        return self._configured_port or get_knowledge_propagation_port()

    async def mutate(
        self,
        context: Any,
        command: KnowledgeMutationCommand,
    ) -> KnowledgeMutationReceipt:
        """Stage one all-or-nothing selection mutation."""

        if not isinstance(command, KnowledgeMutationCommand):
            raise TypeError("knowledge_propagation_command_invalid")
        request_hash = self._mutation_request_hash(command)
        operation_kind = self._mutation_kind(command.selection)
        try:
            replay = await self._replay(
                context,
                target=command.target,
                actor_id=command.actor_id,
                operation_kind=operation_kind,
                idempotency_key=command.idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay

            source_ids = (
                command.selection.knowledge_ids
                if command.selection.selection_state
                is KnowledgeSelectionState.EXPLICIT_IDS
                else ()
            )
            scope = await self._port.load_scope(
                context,
                KnowledgeScopeLookup(
                    target=command.target,
                    source_knowledge_ids=source_ids,
                ),
            )
            self._require_scope_target(scope, command.target)
            self._require_revision(scope, command.expected_revision)
            sources = self._validated_sources(command.selection, scope)
            plan = self._build_mutation_plan(
                command,
                scope=scope,
                sources=sources,
                request_hash=request_hash,
            )
            return await self._stage(context, plan)
        except KnowledgePropagationServiceError as exc:
            wrapped = self._with_rejection_attempt(
                exc,
                target=command.target,
                actor_id=command.actor_id,
                operation_kind=operation_kind,
                idempotency_key=command.idempotency_key,
                request_hash=request_hash,
            )
            if wrapped is exc:
                raise
            raise wrapped from exc

    async def refresh(
        self,
        context: Any,
        command: KnowledgeRefreshCommand,
    ) -> KnowledgeMutationReceipt:
        """Stage an explicit append-only refresh of snapshot assignments."""

        if not isinstance(command, KnowledgeRefreshCommand):
            raise TypeError("knowledge_propagation_refresh_command_invalid")
        request_hash = self._refresh_request_hash(command)
        operation_kind = KnowledgeMutationKind.REFRESH_SNAPSHOT
        try:
            replay = await self._replay(
                context,
                target=command.target,
                actor_id=command.actor_id,
                operation_kind=operation_kind,
                idempotency_key=command.idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay

            scope = await self._port.load_scope(
                context,
                KnowledgeScopeLookup(target=command.target),
            )
            self._require_scope_target(scope, command.target)
            self._require_revision(scope, command.expected_revision)
            plan = self._build_refresh_plan(
                command,
                scope=scope,
                request_hash=request_hash,
            )
            return await self._stage(context, plan)
        except KnowledgePropagationServiceError as exc:
            wrapped = self._with_rejection_attempt(
                exc,
                target=command.target,
                actor_id=command.actor_id,
                operation_kind=operation_kind,
                idempotency_key=command.idempotency_key,
                request_hash=request_hash,
            )
            if wrapped is exc:
                raise
            raise wrapped from exc

    async def read(
        self,
        context: Any,
        target: KnowledgeTargetKey,
    ) -> KnowledgePropagationReadResult:
        """Resolve current v2 assignments or legacy fallback, never both."""

        if not isinstance(target, KnowledgeTargetKey):
            raise TypeError("knowledge_propagation_read_target_invalid")
        scope = await self._port.load_scope(
            context,
            KnowledgeScopeLookup(target=target),
        )
        self._require_scope_target(scope, target)
        resolved = self._resolve_v2_assignments(scope) if scope.v2_active else ()
        effective_legacy = (
            ()
            if scope.v2_active
            else tuple(
                item
                for item in scope.legacy_attachments
                if item.effective
                and item.origin_class is not KnowledgeOriginClass.LEGACY_UNRESOLVED
            )
        )
        return KnowledgePropagationReadResult(
            target=target,
            scope_revision=scope.scope_revision,
            v2_active=scope.v2_active,
            selection_state=cast(
                KnowledgeSelectionState | None,
                scope.selection_state,
            ),
            resolved_assignments=resolved,
            effective_legacy_attachments=effective_legacy,
            history_assignments=scope.assignments,
            history_legacy_attachments=scope.legacy_attachments,
            tombstones=scope.tombstones,
            snapshots=scope.snapshots,
        )

    @staticmethod
    def _mutation_kind(
        selection: KnowledgeSelection,
    ) -> KnowledgeMutationKind:
        if selection.selection_state is KnowledgeSelectionState.OMITTED:
            return KnowledgeMutationKind.REPLACE_OMITTED
        if selection.selection_state is KnowledgeSelectionState.EXPLICIT_EMPTY:
            return KnowledgeMutationKind.REPLACE_EMPTY
        if selection.mode is KnowledgePropagationMode.DROP:
            return KnowledgeMutationKind.DROP_DELTA
        return KnowledgeMutationKind.REPLACE

    def _with_rejection_attempt(
        self,
        error: KnowledgePropagationServiceError,
        *,
        target: KnowledgeTargetKey,
        actor_id: str,
        operation_kind: KnowledgeMutationKind,
        idempotency_key: str,
        request_hash: str,
    ) -> KnowledgePropagationServiceError:
        if error.ledger_attempt is not None:
            return error
        return KnowledgePropagationServiceError(
            error.code,
            error.detail,
            details=error.details,
            ledger_attempt=KnowledgeMutationAttempt(
                attempt_id=self._id_factory("kbatm"),
                target=target,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                operation_kind=operation_kind,
                actor_id=actor_id,
                outcome=KnowledgeMutationOutcome.REJECTED,
                recorded_at=self._operation_time(),
                reason_code=error.code,
                reason_detail=error.detail,
                details=error.details,
            ),
        )

    async def _replay(
        self,
        context: Any,
        *,
        target: KnowledgeTargetKey,
        actor_id: str,
        operation_kind: KnowledgeMutationKind,
        idempotency_key: str,
        request_hash: str,
    ) -> KnowledgeMutationReceipt | None:
        entry = await self._port.get_idempotency_entry(
            context,
            KnowledgeIdempotencyLookup(
                target=target,
                idempotency_key=idempotency_key,
            ),
        )
        if entry is None:
            return None
        if (
            entry.target != target
            or entry.idempotency_key != idempotency_key
            or entry.request_hash != request_hash
            or entry.operation_kind is not operation_kind
            or entry.actor_id != actor_id
        ):
            details = {
                "idempotency_key": idempotency_key,
                "original_request_hash": entry.request_hash,
                "request_hash": request_hash,
            }
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_idempotency_conflict",
                "idempotency key was already used with a different request",
                details=details,
                ledger_attempt=KnowledgeMutationAttempt(
                    attempt_id=self._id_factory("kbatm"),
                    target=target,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    operation_kind=operation_kind,
                    actor_id=actor_id,
                    outcome=KnowledgeMutationOutcome.REJECTED,
                    recorded_at=self._operation_time(),
                    original_operation_id=entry.receipt.operation_id,
                    reason_code="knowledge_propagation_idempotency_conflict",
                    reason_detail=(
                        "idempotency key was already used with a different request"
                    ),
                    details=details,
                ),
            )
        replay_attempt = KnowledgeMutationAttempt(
            attempt_id=self._id_factory("kbatm"),
            target=target,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            operation_kind=operation_kind,
            actor_id=actor_id,
            outcome=KnowledgeMutationOutcome.REPLAYED,
            recorded_at=self._operation_time(),
            original_operation_id=entry.receipt.operation_id,
        )
        if entry.receipt.outcome is KnowledgeMutationOutcome.REJECTED:
            raise KnowledgePropagationServiceError(
                cast(str, entry.receipt.reason_code),
                cast(str, entry.receipt.reason_detail),
                details=entry.receipt.details,
                ledger_attempt=replay_attempt,
            )
        try:
            await self._port.stage_attempt(context, replay_attempt)
        except KnowledgePropagationPortError as exc:
            raise KnowledgePropagationServiceError(
                exc.code,
                exc.detail,
                details=exc.details,
            ) from exc
        return entry.receipt.as_replay()

    @staticmethod
    def _require_scope_target(
        scope: KnowledgePropagationScope,
        target: KnowledgeTargetKey,
    ) -> None:
        if not isinstance(scope, KnowledgePropagationScope) or scope.target != target:
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_scope_mismatch",
                "persistence returned a scope for a different target",
            )

    @staticmethod
    def _require_revision(
        scope: KnowledgePropagationScope,
        expected_revision: int,
    ) -> None:
        if scope.scope_revision != expected_revision:
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_revision_conflict",
                "expected revision does not match the current scope revision",
                details={
                    "expected_revision": expected_revision,
                    "current_revision": scope.scope_revision,
                },
            )

    @staticmethod
    def _validated_sources(
        selection: KnowledgeSelection,
        scope: KnowledgePropagationScope,
    ) -> tuple[KnowledgeSelectableSource, ...]:
        if selection.selection_state is not KnowledgeSelectionState.EXPLICIT_IDS:
            return ()
        requested = selection.knowledge_ids
        by_requested = {
            source.requested_knowledge_id: source for source in scope.sources
        }
        matched = tuple(sorted(set(requested).intersection(by_requested)))
        missing = tuple(sorted(set(requested).difference(by_requested)))
        invalid: list[str] = []
        ambiguous: list[str] = []
        roots: dict[str, str] = {}
        for requested_id in matched:
            source = by_requested[requested_id]
            if (
                source.source_deleted
                and selection.mode is not KnowledgePropagationMode.DROP
            ):
                invalid.append(requested_id)
            prior = roots.get(source.revision_stamp.root_id)
            if prior is not None and prior != requested_id:
                ambiguous.extend((prior, requested_id))
            roots[source.revision_stamp.root_id] = requested_id
            if selection.mode is KnowledgePropagationMode.SNAPSHOT and (
                source.content_bytes is None
                or hashlib.sha256(source.content_bytes).hexdigest()
                != source.revision_stamp.source_content_sha256
            ):
                invalid.append(requested_id)
        if missing or invalid or ambiguous or len(matched) != len(requested):
            raise KnowledgePropagationServiceError(
                "knowledge_selection_invalid",
                "all requested Knowledge Base ids must resolve uniquely before mutation",
                details={
                    "requested": list(requested),
                    "matched": list(matched),
                    "missing": list(missing),
                    "invalid": sorted(set(invalid)),
                    "ambiguous": sorted(set(ambiguous)),
                },
            )
        return tuple(by_requested[item] for item in requested)

    def _build_mutation_plan(
        self,
        command: KnowledgeMutationCommand,
        *,
        scope: KnowledgePropagationScope,
        sources: tuple[KnowledgeSelectableSource, ...],
        request_hash: str,
    ) -> KnowledgeMutationPlan:
        now = self._operation_time()
        next_revision = scope.scope_revision + 1
        selection_state = cast(
            KnowledgeSelectionState,
            command.selection.selection_state,
        )
        mode = cast(KnowledgePropagationMode | None, command.selection.mode)
        current_assignments = tuple(
            item for item in scope.assignments if item.temporal.is_current
        )
        current_snapshots = tuple(
            item for item in scope.snapshots if item.temporal.is_current
        )
        current_tombstones = tuple(
            item for item in scope.tombstones if item.temporal.is_current
        )

        assignments_to_open: list[TemporalKnowledgeAssignment] = []
        tombstones_to_open: list[KnowledgePropagationTombstone] = []
        snapshots_to_open: list[KnowledgePropagationSnapshot] = []
        assignment_ids_to_close: set[str] = set()
        snapshot_ids_to_close: set[str] = set()
        tombstone_ids_to_close: set[str] = set()

        if selection_state is KnowledgeSelectionState.OMITTED:
            kind = KnowledgeMutationKind.REPLACE_OMITTED
            assignment_ids_to_close.update(
                item.assignment.assignment_id for item in current_assignments
            )
        elif selection_state is KnowledgeSelectionState.EXPLICIT_EMPTY:
            kind = KnowledgeMutationKind.REPLACE_EMPTY
            assignment_ids_to_close.update(
                item.assignment.assignment_id for item in current_assignments
            )
            tombstones_to_open.append(
                KnowledgePropagationTombstone(
                    tombstone_id=self._id_factory("kbtmb"),
                    target=command.target,
                    root_id=None,
                    actor_id=command.actor_id,
                    justification=cast(str, command.justification),
                    temporal=KnowledgeTemporalWindow(effective_from=now),
                )
            )
            tombstone_ids_to_close.update(
                item.tombstone_id for item in current_tombstones
            )
        elif mode is KnowledgePropagationMode.DROP:
            kind = KnowledgeMutationKind.DROP_DELTA
            selected_roots = {source.revision_stamp.root_id for source in sources}
            for item in current_assignments:
                if item.assignment.revision_stamp.root_id in selected_roots:
                    assignment_ids_to_close.add(item.assignment.assignment_id)
            for source in sources:
                assignment_id = self._id_factory("kbasg")
                assignments_to_open.append(
                    self._new_assignment(
                        assignment_id=assignment_id,
                        command=command,
                        source=source,
                        mode=KnowledgePropagationMode.DROP,
                        state=KnowledgeAssignmentState.DROPPED,
                        revision=next_revision,
                        now=now,
                    )
                )
                tombstones_to_open.append(
                    KnowledgePropagationTombstone(
                        tombstone_id=self._id_factory("kbtmb"),
                        target=command.target,
                        root_id=source.revision_stamp.root_id,
                        actor_id=command.actor_id,
                        justification=cast(str, command.justification),
                        temporal=KnowledgeTemporalWindow(effective_from=now),
                    )
                )
            for current_tombstone in current_tombstones:
                if (
                    current_tombstone.root_id is None
                    or current_tombstone.root_id in selected_roots
                ):
                    tombstone_ids_to_close.add(current_tombstone.tombstone_id)
        else:
            kind = KnowledgeMutationKind.REPLACE
            assignment_ids_to_close.update(
                item.assignment.assignment_id for item in current_assignments
            )
            selected_roots = {source.revision_stamp.root_id for source in sources}
            for current_tombstone in current_tombstones:
                if (
                    current_tombstone.root_id is None
                    or current_tombstone.root_id in selected_roots
                ):
                    tombstone_ids_to_close.add(current_tombstone.tombstone_id)
            for source in sources:
                assignment_id = self._id_factory("kbasg")
                assignments_to_open.append(
                    self._new_assignment(
                        assignment_id=assignment_id,
                        command=command,
                        source=source,
                        mode=cast(KnowledgePropagationMode, mode),
                        state=KnowledgeAssignmentState.ACTIVE,
                        revision=next_revision,
                        now=now,
                    )
                )
                if mode is KnowledgePropagationMode.SNAPSHOT:
                    assert source.content_bytes is not None
                    snapshots_to_open.append(
                        KnowledgePropagationSnapshot(
                            snapshot_id=self._id_factory("kbsnp"),
                            assignment_id=assignment_id,
                            revision_stamp=source.revision_stamp,
                            content_bytes=source.content_bytes,
                            temporal=KnowledgeTemporalWindow(effective_from=now),
                        )
                    )

        if assignment_ids_to_close:
            snapshot_ids_to_close.update(
                snapshot.snapshot_id
                for snapshot in current_snapshots
                if snapshot.assignment_id in assignment_ids_to_close
            )

        supersession_links = self._mutation_supersession_links(
            current_assignments=current_assignments,
            current_snapshots=current_snapshots,
            current_tombstones=current_tombstones,
            assignments_to_open=tuple(assignments_to_open),
            snapshots_to_open=tuple(snapshots_to_open),
            tombstones_to_open=tuple(tombstones_to_open),
            assignment_ids_to_close=assignment_ids_to_close,
            snapshot_ids_to_close=snapshot_ids_to_close,
            tombstone_ids_to_close=tombstone_ids_to_close,
        )
        return self._plan(
            kind=kind,
            target=command.target,
            selection=command.selection,
            expected_revision=scope.scope_revision,
            next_scope_selection_state=selection_state,
            actor_id=command.actor_id,
            idempotency_key=command.idempotency_key,
            request_hash=request_hash,
            occurred_at=now,
            assignments_to_open=tuple(assignments_to_open),
            assignment_ids_to_close=tuple(assignment_ids_to_close),
            tombstones_to_open=tuple(tombstones_to_open),
            tombstone_ids_to_close=tuple(tombstone_ids_to_close),
            snapshots_to_open=tuple(snapshots_to_open),
            snapshot_ids_to_close=tuple(snapshot_ids_to_close),
            supersession_links=supersession_links,
        )

    def _build_refresh_plan(
        self,
        command: KnowledgeRefreshCommand,
        *,
        scope: KnowledgePropagationScope,
        request_hash: str,
    ) -> KnowledgeMutationPlan:
        if (
            not scope.v2_active
            or scope.selection_state is not KnowledgeSelectionState.EXPLICIT_IDS
        ):
            raise KnowledgePropagationServiceError(
                "knowledge_assignment_not_refreshable",
                "snapshot refresh requires an active v2 scope",
            )
        current = {
            item.assignment.assignment_id: item
            for item in scope.assignments
            if item.temporal.is_current
        }
        current_snapshots_by_assignment = {
            item.assignment_id: item
            for item in scope.snapshots
            if item.temporal.is_current
        }
        current_tombstones = tuple(
            item for item in scope.tombstones if item.temporal.is_current
        )
        global_drop = any(item.root_id is None for item in current_tombstones)
        dropped_roots = {
            item.root_id for item in current_tombstones if item.root_id is not None
        }
        matched = tuple(
            assignment_id
            for assignment_id in command.assignment_ids
            if assignment_id in current
        )
        missing = tuple(
            assignment_id
            for assignment_id in command.assignment_ids
            if assignment_id not in current
        )
        invalid: list[str] = []
        sources_by_identity: dict[str, KnowledgeSelectableSource] = {}
        for source in scope.sources:
            sources_by_identity[source.requested_knowledge_id] = source
            sources_by_identity[source.source_knowledge_id] = source
        selected: list[
            tuple[TemporalKnowledgeAssignment, KnowledgeSelectableSource]
        ] = []
        for assignment_id in matched:
            temporal = current[assignment_id]
            assignment = temporal.assignment
            selected_source = sources_by_identity.get(assignment.source_knowledge_id)
            if (
                assignment.mode is not KnowledgePropagationMode.SNAPSHOT
                or assignment.state
                not in {
                    KnowledgeAssignmentState.ACTIVE,
                    KnowledgeAssignmentState.STALE,
                }
                or selected_source is None
                or selected_source.source_deleted
                or selected_source.content_bytes is None
                or assignment.assignment_id not in current_snapshots_by_assignment
                or global_drop
                or assignment.revision_stamp.root_id in dropped_roots
            ):
                invalid.append(assignment_id)
                continue
            selected.append((temporal, selected_source))
        if missing or invalid or len(selected) != len(command.assignment_ids):
            raise KnowledgePropagationServiceError(
                "knowledge_assignment_not_refreshable",
                "all requested assignments must be current verifiable snapshots",
                details={
                    "requested": list(command.assignment_ids),
                    "matched": list(matched),
                    "missing": list(missing),
                    "invalid": sorted(invalid),
                },
            )

        now = self._operation_time()
        next_revision = scope.scope_revision + 1
        assignments_to_open: list[TemporalKnowledgeAssignment] = []
        snapshots_to_open: list[KnowledgePropagationSnapshot] = []
        assignment_ids_to_close: list[str] = []
        snapshot_ids_to_close: list[str] = []
        supersession_links: list[KnowledgeSupersessionLink] = []
        current_snapshots = tuple(
            item for item in scope.snapshots if item.temporal.is_current
        )
        for temporal, source in selected:
            old = temporal.assignment
            assignment_id = self._id_factory("kbasg")
            assignment = KnowledgeAssignment(
                assignment_id=assignment_id,
                board_id=command.target.board_id,
                target_type=command.target.target_type,
                target_id=command.target.target_id,
                source_knowledge_id=source.source_knowledge_id,
                revision_stamp=source.revision_stamp,
                mode=KnowledgePropagationMode.SNAPSHOT,
                state=KnowledgeAssignmentState.ACTIVE,
                origin_class=KnowledgeOriginClass.V2,
                actor_id=command.actor_id,
                revision=next_revision,
                justification=command.justification,
                relevance_links=old.relevance_links,
            )
            assignments_to_open.append(
                TemporalKnowledgeAssignment(
                    assignment=assignment,
                    temporal=KnowledgeTemporalWindow(effective_from=now),
                )
            )
            assert source.content_bytes is not None
            snapshot_id = self._id_factory("kbsnp")
            snapshots_to_open.append(
                KnowledgePropagationSnapshot(
                    snapshot_id=snapshot_id,
                    assignment_id=assignment_id,
                    revision_stamp=source.revision_stamp,
                    content_bytes=source.content_bytes,
                    temporal=KnowledgeTemporalWindow(effective_from=now),
                )
            )
            assignment_ids_to_close.append(old.assignment_id)
            supersession_links.append(
                KnowledgeSupersessionLink(
                    record_kind=KnowledgeRecordKind.ASSIGNMENT,
                    previous_id=old.assignment_id,
                    successor_id=assignment_id,
                )
            )
            for snapshot in current_snapshots:
                if snapshot.assignment_id != old.assignment_id:
                    continue
                snapshot_ids_to_close.append(snapshot.snapshot_id)
                supersession_links.append(
                    KnowledgeSupersessionLink(
                        record_kind=KnowledgeRecordKind.SNAPSHOT,
                        previous_id=snapshot.snapshot_id,
                        successor_id=snapshot_id,
                    )
                )

        return self._plan(
            kind=KnowledgeMutationKind.REFRESH_SNAPSHOT,
            target=command.target,
            selection=None,
            expected_revision=scope.scope_revision,
            next_scope_selection_state=cast(
                KnowledgeSelectionState,
                scope.selection_state,
            ),
            actor_id=command.actor_id,
            idempotency_key=command.idempotency_key,
            request_hash=request_hash,
            occurred_at=now,
            assignments_to_open=tuple(assignments_to_open),
            assignment_ids_to_close=tuple(assignment_ids_to_close),
            snapshots_to_open=tuple(snapshots_to_open),
            snapshot_ids_to_close=tuple(snapshot_ids_to_close),
            supersession_links=tuple(supersession_links),
        )

    @staticmethod
    def _mutation_supersession_links(
        *,
        current_assignments: tuple[TemporalKnowledgeAssignment, ...],
        current_snapshots: tuple[KnowledgePropagationSnapshot, ...],
        current_tombstones: tuple[KnowledgePropagationTombstone, ...],
        assignments_to_open: tuple[TemporalKnowledgeAssignment, ...],
        snapshots_to_open: tuple[KnowledgePropagationSnapshot, ...],
        tombstones_to_open: tuple[KnowledgePropagationTombstone, ...],
        assignment_ids_to_close: set[str],
        snapshot_ids_to_close: set[str],
        tombstone_ids_to_close: set[str],
    ) -> tuple[KnowledgeSupersessionLink, ...]:
        links: list[KnowledgeSupersessionLink] = []
        new_assignments_by_root = {
            item.assignment.revision_stamp.root_id: item.assignment.assignment_id
            for item in assignments_to_open
        }
        old_assignments_by_id = {
            item.assignment.assignment_id: item.assignment
            for item in current_assignments
        }
        for previous_id in assignment_ids_to_close:
            old = old_assignments_by_id.get(previous_id)
            if old is None:
                continue
            successor_id = new_assignments_by_root.get(old.revision_stamp.root_id)
            if successor_id is not None:
                links.append(
                    KnowledgeSupersessionLink(
                        record_kind=KnowledgeRecordKind.ASSIGNMENT,
                        previous_id=previous_id,
                        successor_id=successor_id,
                    )
                )

        new_assignments = {
            item.assignment.assignment_id: item.assignment
            for item in assignments_to_open
        }
        new_snapshots_by_root = {
            new_assignments[item.assignment_id].revision_stamp.root_id: item.snapshot_id
            for item in snapshots_to_open
        }
        for old_snapshot in current_snapshots:
            if old_snapshot.snapshot_id not in snapshot_ids_to_close:
                continue
            old_assignment = old_assignments_by_id.get(old_snapshot.assignment_id)
            if old_assignment is None:
                continue
            successor_id = new_snapshots_by_root.get(
                old_assignment.revision_stamp.root_id
            )
            if successor_id is not None:
                links.append(
                    KnowledgeSupersessionLink(
                        record_kind=KnowledgeRecordKind.SNAPSHOT,
                        previous_id=old_snapshot.snapshot_id,
                        successor_id=successor_id,
                    )
                )

        new_tombstones_by_root = {
            item.root_id: item.tombstone_id for item in tombstones_to_open
        }
        for old_tombstone in current_tombstones:
            if old_tombstone.tombstone_id not in tombstone_ids_to_close:
                continue
            successor_id = new_tombstones_by_root.get(old_tombstone.root_id)
            if successor_id is not None:
                links.append(
                    KnowledgeSupersessionLink(
                        record_kind=KnowledgeRecordKind.TOMBSTONE,
                        previous_id=old_tombstone.tombstone_id,
                        successor_id=successor_id,
                    )
                )
        return tuple(links)

    def _new_assignment(
        self,
        *,
        assignment_id: str,
        command: KnowledgeMutationCommand,
        source: KnowledgeSelectableSource,
        mode: KnowledgePropagationMode,
        state: KnowledgeAssignmentState,
        revision: int,
        now: datetime,
    ) -> TemporalKnowledgeAssignment:
        return TemporalKnowledgeAssignment(
            assignment=KnowledgeAssignment(
                assignment_id=assignment_id,
                board_id=command.target.board_id,
                target_type=command.target.target_type,
                target_id=command.target.target_id,
                source_knowledge_id=source.source_knowledge_id,
                revision_stamp=source.revision_stamp,
                mode=mode,
                state=state,
                origin_class=KnowledgeOriginClass.V2,
                actor_id=command.actor_id,
                revision=revision,
                justification=command.justification,
                relevance_links=command.relevance_links,
            ),
            temporal=KnowledgeTemporalWindow(effective_from=now),
        )

    def _plan(
        self,
        *,
        kind: KnowledgeMutationKind,
        target: KnowledgeTargetKey,
        selection: KnowledgeSelection | None,
        expected_revision: int,
        next_scope_selection_state: KnowledgeSelectionState,
        actor_id: str,
        idempotency_key: str,
        request_hash: str,
        occurred_at: datetime,
        assignments_to_open: tuple[TemporalKnowledgeAssignment, ...] = (),
        assignment_ids_to_close: tuple[str, ...] = (),
        tombstones_to_open: tuple[KnowledgePropagationTombstone, ...] = (),
        tombstone_ids_to_close: tuple[str, ...] = (),
        snapshots_to_open: tuple[KnowledgePropagationSnapshot, ...] = (),
        snapshot_ids_to_close: tuple[str, ...] = (),
        supersession_links: tuple[KnowledgeSupersessionLink, ...] = (),
    ) -> KnowledgeMutationPlan:
        operation_id = self._id_factory("kbop")
        receipt = KnowledgeMutationReceipt(
            operation_id=operation_id,
            target=target,
            operation_kind=kind,
            previous_revision=expected_revision,
            revision=expected_revision + 1,
            request_hash=request_hash,
            applied_at=occurred_at,
        )
        ledger = KnowledgeMutationLedgerEntry(
            target=target,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            operation_kind=kind,
            receipt=receipt,
            recorded_at=occurred_at,
            actor_id=actor_id,
        )
        return KnowledgeMutationPlan(
            operation_id=operation_id,
            target=target,
            operation_kind=kind,
            selection=selection,
            expected_revision=expected_revision,
            next_revision=expected_revision + 1,
            actor_id=actor_id,
            occurred_at=occurred_at,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            next_scope_selection_state=next_scope_selection_state,
            assignments_to_open=assignments_to_open,
            assignment_ids_to_close=assignment_ids_to_close,
            tombstones_to_open=tombstones_to_open,
            tombstone_ids_to_close=tombstone_ids_to_close,
            snapshots_to_open=snapshots_to_open,
            snapshot_ids_to_close=snapshot_ids_to_close,
            supersession_links=supersession_links,
            ledger_entry=ledger,
        )

    async def _stage(
        self,
        context: Any,
        plan: KnowledgeMutationPlan,
    ) -> KnowledgeMutationReceipt:
        try:
            receipt = await self._port.stage_mutation(context, plan)
        except KnowledgePropagationPortError as exc:
            raise KnowledgePropagationServiceError(
                exc.code,
                exc.detail,
                details=exc.details,
            ) from exc
        expected = plan.ledger_entry
        assert expected is not None
        if receipt != expected.receipt:
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_stage_receipt_mismatch",
                "persistence returned a receipt that differs from the staged ledger",
            )
        return receipt

    def _resolve_v2_assignments(
        self,
        scope: KnowledgePropagationScope,
    ) -> tuple[ResolvedKnowledgeAssignment, ...]:
        sources: dict[str, KnowledgeSelectableSource] = {}
        for source in scope.sources:
            sources[source.requested_knowledge_id] = source
            sources[source.source_knowledge_id] = source
        current_snapshots = {
            item.assignment_id: item
            for item in scope.snapshots
            if item.temporal.is_current
        }
        current_tombstones = tuple(
            item for item in scope.tombstones if item.temporal.is_current
        )
        global_drop = any(item.root_id is None for item in current_tombstones)
        dropped_roots = {
            item.root_id for item in current_tombstones if item.root_id is not None
        }
        resolved: list[ResolvedKnowledgeAssignment] = []
        current_assignments = sorted(
            (item.assignment for item in scope.assignments if item.temporal.is_current),
            key=lambda item: item.assignment_id,
        )
        for assignment in current_assignments:
            mode = cast(KnowledgePropagationMode, assignment.mode)
            state = cast(KnowledgeAssignmentState, assignment.state)
            if global_drop or assignment.revision_stamp.root_id in dropped_roots:
                resolved.append(
                    ResolvedKnowledgeAssignment(
                        assignment=assignment,
                        state=KnowledgeAssignmentState.DROPPED,
                        effective=False,
                        revision_stamp=assignment.revision_stamp,
                        reason="tombstoned",
                    )
                )
                continue
            if mode is KnowledgePropagationMode.DROP:
                resolved.append(
                    ResolvedKnowledgeAssignment(
                        assignment=assignment,
                        state=KnowledgeAssignmentState.DROPPED,
                        effective=False,
                        revision_stamp=assignment.revision_stamp,
                        reason="dropped",
                    )
                )
                continue
            if state is KnowledgeAssignmentState.INACTIVE:
                resolved.append(
                    ResolvedKnowledgeAssignment(
                        assignment=assignment,
                        state=state,
                        effective=False,
                        revision_stamp=assignment.revision_stamp,
                        reason="inactive",
                    )
                )
                continue
            current_source = sources.get(assignment.source_knowledge_id)
            if mode is KnowledgePropagationMode.REFERENCE:
                source_deleted = (
                    state is KnowledgeAssignmentState.SOURCE_DELETED
                    or current_source is None
                    or current_source.source_deleted
                )
                resolved.append(
                    ResolvedKnowledgeAssignment(
                        assignment=assignment,
                        state=(
                            KnowledgeAssignmentState.SOURCE_DELETED
                            if source_deleted
                            else KnowledgeAssignmentState.ACTIVE
                        ),
                        effective=not source_deleted,
                        revision_stamp=(
                            assignment.revision_stamp
                            if current_source is None
                            else current_source.revision_stamp
                        ),
                        reason=("source_deleted" if source_deleted else None),
                    )
                )
                continue

            snapshot = current_snapshots.get(assignment.assignment_id)
            if snapshot is None:
                resolved.append(
                    ResolvedKnowledgeAssignment(
                        assignment=assignment,
                        state=KnowledgeAssignmentState.INACTIVE,
                        effective=False,
                        revision_stamp=assignment.revision_stamp,
                        reason="snapshot_missing",
                    )
                )
                continue
            source_deleted = (
                state is KnowledgeAssignmentState.SOURCE_DELETED
                or current_source is None
                or current_source.source_deleted
            )
            stale = (
                not source_deleted
                and current_source is not None
                and current_source.revision_stamp != assignment.revision_stamp
            )
            resolved.append(
                ResolvedKnowledgeAssignment(
                    assignment=assignment,
                    state=(
                        KnowledgeAssignmentState.SOURCE_DELETED
                        if source_deleted
                        else (KnowledgeAssignmentState.STALE if stale else state)
                    ),
                    effective=not source_deleted,
                    revision_stamp=snapshot.revision_stamp,
                    content_bytes=snapshot.content_bytes,
                    reason=(
                        "source_deleted"
                        if source_deleted
                        else ("source_changed" if stale else None)
                    ),
                )
            )
        return tuple(resolved)

    def _mutation_request_hash(
        self,
        command: KnowledgeMutationCommand,
    ) -> str:
        return self._hash(
            {
                "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
                "operation": "mutate",
                "target": command.target.to_dict(),
                "selection": command.selection.to_dict(),
                "actor_id": command.actor_id,
                "expected_revision": command.expected_revision,
                "justification": command.justification,
                "relevance_links": [item.to_dict() for item in command.relevance_links],
            }
        )

    def _refresh_request_hash(
        self,
        command: KnowledgeRefreshCommand,
    ) -> str:
        return self._hash(
            {
                "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
                "operation": "refresh_snapshot",
                "target": command.target.to_dict(),
                "assignment_ids": list(command.assignment_ids),
                "actor_id": command.actor_id,
                "expected_revision": command.expected_revision,
                "justification": command.justification,
            }
        )

    @staticmethod
    def _hash(payload: Mapping[str, object]) -> str:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _operation_time(self) -> datetime:
        value = self._now()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_clock_invalid",
                "operation clock must return a timezone-aware datetime",
            )
        return value.astimezone(timezone.utc)


__all__ = [
    "KnowledgeGrandfatherEvidence",
    "KnowledgeMutationCommand",
    "KnowledgePropagationReadResult",
    "KnowledgePropagationService",
    "KnowledgePropagationServiceError",
    "KnowledgeRefreshCommand",
    "ResolvedKnowledgeAssignment",
    "classify_legacy_origin",
]
