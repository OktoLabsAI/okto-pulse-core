"""Persistence boundary for selective Knowledge Base propagation v2.

Core owns the mutation vocabulary and the complete atomic write plan.  An
edition-owned adapter may stage that plan in its current unit of work, but it
must never commit or roll back the caller's transaction.

The contract deliberately keeps idempotency replay lookup separate from scope
loading.  Services can therefore return a successful replay before applying an
optimistic-revision check, while divergent reuse of an idempotency key remains
fail-closed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import re
from types import MappingProxyType
from typing import Any, Protocol, TypeVar, cast

from okto_pulse.core.domain.knowledge_selection import (
    KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
    KnowledgeAssignment,
    KnowledgeOriginClass,
    KnowledgePropagationMode,
    KnowledgeSelection,
    KnowledgeSelectionState,
    KnowledgeTargetType,
)
from okto_pulse.core.domain.resource_revision import ResourceRevisionStamp
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_RUNTIME_KEY = "ports.knowledge_propagation"
_EnumT = TypeVar("_EnumT", bound=Enum)


class KnowledgeMutationKind(str, Enum):
    """Semantically distinct writes understood by the persistence adapter."""

    REPLACE = "replace"
    DROP_DELTA = "drop_delta"
    REPLACE_EMPTY = "replace_empty"
    REFRESH_SNAPSHOT = "refresh_snapshot"
    GRANDFATHER = "grandfather"


class KnowledgePropagationPortError(RuntimeError):
    """Stable error envelope emitted at the selective-propagation boundary."""

    def __init__(
        self,
        code: str,
        detail: str,
        *,
        details: Mapping[str, object] | None = None,
    ) -> None:
        self.code = _required_text(code, "error_code")
        self.detail = _required_text(detail, "error_detail")
        if details is not None and not isinstance(details, Mapping):
            raise TypeError("knowledge_propagation_error_details_invalid")
        self.details = MappingProxyType(dict(details or {}))
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
    return _required_text(value, field_name)


def _coerce_enum(
    value: object,
    enum_type: type[_EnumT],
    field_name: str,
) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        try:
            return enum_type(value.strip())
        except ValueError:
            pass
    raise ValueError(f"knowledge_propagation_{field_name}_invalid")


def _non_negative_int(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    return value


def _utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    offset = value.utcoffset()
    if offset is None:
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    return value.astimezone(timezone.utc)


def _canonical_text_tuple(
    values: Sequence[str],
    field_name: str,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    normalized = {_required_text(value, field_name) for value in values}
    return tuple(sorted(normalized))


def _canonical_objects(
    values: Sequence[Any],
    expected_type: type[Any],
    *,
    field_name: str,
    identity: Any,
) -> tuple[Any, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    result: dict[str, Any] = {}
    for item in values:
        if not isinstance(item, expected_type):
            raise ValueError(f"knowledge_propagation_{field_name}_invalid")
        key = identity(item)
        if key in result:
            raise ValueError(f"knowledge_propagation_{field_name}_duplicate")
        result[key] = item
    return tuple(result[key] for key in sorted(result))


def _canonical_stamp(
    value: object,
    *,
    require_revision_evidence: bool,
) -> ResourceRevisionStamp:
    if not isinstance(value, ResourceRevisionStamp):
        raise ValueError("knowledge_propagation_revision_stamp_invalid")
    root_id = _required_text(value.root_id, "root_id")
    immediate_parent_id = _optional_text(
        value.immediate_parent_id,
        "immediate_parent_id",
    )
    source_revision = _optional_text(value.source_revision, "source_revision")
    source_hash = _optional_text(
        value.source_content_sha256,
        "source_content_sha256",
    )
    if source_hash is not None and _SHA256_HEX.fullmatch(source_hash) is None:
        raise ValueError("knowledge_propagation_source_content_sha256_invalid")
    if require_revision_evidence and (
        source_revision is None or source_hash is None
    ):
        raise ValueError("knowledge_propagation_revision_evidence_required")
    return ResourceRevisionStamp(
        root_id=root_id,
        immediate_parent_id=immediate_parent_id,
        source_revision=source_revision,
        source_content_sha256=source_hash,
    )


@dataclass(frozen=True, slots=True)
class KnowledgeTargetKey:
    """Board-scoped identity of a spec or card propagation target."""

    board_id: str
    target_type: KnowledgeTargetType | str
    target_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "board_id", _required_text(self.board_id, "board_id"))
        object.__setattr__(
            self,
            "target_type",
            _coerce_enum(self.target_type, KnowledgeTargetType, "target_type"),
        )
        object.__setattr__(
            self,
            "target_id",
            _required_text(self.target_id, "target_id"),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "board_id": self.board_id,
            "target_type": cast(KnowledgeTargetType, self.target_type).value,
            "target_id": self.target_id,
        }


@dataclass(frozen=True, slots=True)
class KnowledgeTemporalWindow:
    """Effective-time evidence for immutable propagation records."""

    effective_from: datetime
    effective_to: datetime | None = None
    superseded_by_id: str | None = None

    def __post_init__(self) -> None:
        effective_from = _utc(self.effective_from, "effective_from")
        effective_to = (
            None
            if self.effective_to is None
            else _utc(self.effective_to, "effective_to")
        )
        superseded_by_id = _optional_text(
            self.superseded_by_id,
            "superseded_by_id",
        )
        if effective_to is not None and effective_to < effective_from:
            raise ValueError("knowledge_propagation_effective_window_invalid")
        if (effective_to is None) != (superseded_by_id is None):
            raise ValueError(
                "knowledge_propagation_supersession_window_incoherent"
            )
        object.__setattr__(self, "effective_from", effective_from)
        object.__setattr__(self, "effective_to", effective_to)
        object.__setattr__(self, "superseded_by_id", superseded_by_id)

    @property
    def is_current(self) -> bool:
        return self.effective_to is None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                None if self.effective_to is None else self.effective_to.isoformat()
            ),
            "superseded_by_id": self.superseded_by_id,
        }


@dataclass(frozen=True, slots=True)
class TemporalKnowledgeAssignment:
    """A durable assignment plus its append-only effective-time envelope."""

    assignment: KnowledgeAssignment
    temporal: KnowledgeTemporalWindow

    def __post_init__(self) -> None:
        if not isinstance(self.assignment, KnowledgeAssignment):
            raise ValueError("knowledge_propagation_assignment_invalid")
        if not isinstance(self.temporal, KnowledgeTemporalWindow):
            raise ValueError("knowledge_propagation_assignment_temporal_invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "assignment": self.assignment.to_dict(),
            "temporal": self.temporal.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class KnowledgePropagationTombstone:
    """Durable anti-resurrection marker for one target/root pair."""

    tombstone_id: str
    target: KnowledgeTargetKey
    root_id: str
    actor_id: str
    justification: str
    temporal: KnowledgeTemporalWindow

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tombstone_id",
            _required_text(self.tombstone_id, "tombstone_id"),
        )
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_tombstone_target_invalid")
        object.__setattr__(self, "root_id", _required_text(self.root_id, "root_id"))
        object.__setattr__(self, "actor_id", _required_text(self.actor_id, "actor_id"))
        object.__setattr__(
            self,
            "justification",
            _required_text(self.justification, "justification"),
        )
        if not isinstance(self.temporal, KnowledgeTemporalWindow):
            raise ValueError("knowledge_propagation_tombstone_temporal_invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
            "tombstone_id": self.tombstone_id,
            "target": self.target.to_dict(),
            "root_id": self.root_id,
            "actor_id": self.actor_id,
            "justification": self.justification,
            "temporal": self.temporal.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class KnowledgePropagationSnapshot:
    """Immutable canonical bytes owned by a snapshot assignment."""

    snapshot_id: str
    assignment_id: str
    revision_stamp: ResourceRevisionStamp
    content_bytes: bytes
    temporal: KnowledgeTemporalWindow

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "snapshot_id",
            _required_text(self.snapshot_id, "snapshot_id"),
        )
        object.__setattr__(
            self,
            "assignment_id",
            _required_text(self.assignment_id, "assignment_id"),
        )
        stamp = _canonical_stamp(
            self.revision_stamp,
            require_revision_evidence=True,
        )
        if not isinstance(self.content_bytes, bytes):
            raise ValueError("knowledge_propagation_snapshot_content_invalid")
        if hashlib.sha256(self.content_bytes).hexdigest() != (
            stamp.source_content_sha256
        ):
            raise ValueError("knowledge_propagation_snapshot_hash_mismatch")
        if not isinstance(self.temporal, KnowledgeTemporalWindow):
            raise ValueError("knowledge_propagation_snapshot_temporal_invalid")
        object.__setattr__(self, "revision_stamp", stamp)

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
            "snapshot_id": self.snapshot_id,
            "assignment_id": self.assignment_id,
            "revision_stamp": self.revision_stamp.to_dict(),
            "content_sha256": hashlib.sha256(self.content_bytes).hexdigest(),
            "content_size_bytes": len(self.content_bytes),
            "temporal": self.temporal.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class KnowledgeLegacyAttachment:
    """Physical legacy attachment retained for lineage/history projection."""

    source_knowledge_id: str
    revision_stamp: ResourceRevisionStamp
    origin_class: KnowledgeOriginClass | str
    effective: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_knowledge_id",
            _required_text(self.source_knowledge_id, "source_knowledge_id"),
        )
        object.__setattr__(
            self,
            "revision_stamp",
            _canonical_stamp(
                self.revision_stamp,
                require_revision_evidence=False,
            ),
        )
        origin_class = _coerce_enum(
            self.origin_class,
            KnowledgeOriginClass,
            "origin_class",
        )
        if origin_class is KnowledgeOriginClass.V2:
            raise ValueError("knowledge_propagation_legacy_origin_class_invalid")
        if type(self.effective) is not bool:
            raise ValueError("knowledge_propagation_legacy_effective_invalid")
        object.__setattr__(self, "origin_class", origin_class)

    def to_dict(self) -> dict[str, object]:
        return {
            "source_knowledge_id": self.source_knowledge_id,
            "revision_stamp": self.revision_stamp.to_dict(),
            "origin_class": cast(KnowledgeOriginClass, self.origin_class).value,
            "effective": self.effective,
        }


@dataclass(frozen=True, slots=True)
class KnowledgeSelectableSource:
    """One verified source resolved for a requested Knowledge Base token."""

    requested_knowledge_id: str
    source_knowledge_id: str
    revision_stamp: ResourceRevisionStamp
    content_bytes: bytes | None = field(default=None, repr=False)
    source_deleted: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "requested_knowledge_id",
            _required_text(
                self.requested_knowledge_id,
                "requested_knowledge_id",
            ),
        )
        object.__setattr__(
            self,
            "source_knowledge_id",
            _required_text(self.source_knowledge_id, "source_knowledge_id"),
        )
        stamp = _canonical_stamp(
            self.revision_stamp,
            require_revision_evidence=True,
        )
        if self.content_bytes is not None:
            if not isinstance(self.content_bytes, bytes):
                raise ValueError("knowledge_propagation_source_content_invalid")
            if hashlib.sha256(self.content_bytes).hexdigest() != (
                stamp.source_content_sha256
            ):
                raise ValueError("knowledge_propagation_source_hash_mismatch")
        if type(self.source_deleted) is not bool:
            raise ValueError("knowledge_propagation_source_deleted_invalid")
        object.__setattr__(self, "revision_stamp", stamp)

    def to_dict(self) -> dict[str, object]:
        return {
            "requested_knowledge_id": self.requested_knowledge_id,
            "source_knowledge_id": self.source_knowledge_id,
            "revision_stamp": self.revision_stamp.to_dict(),
            "content_available": self.content_bytes is not None,
            "source_deleted": self.source_deleted,
        }


@dataclass(frozen=True, slots=True)
class KnowledgeScopeLookup:
    """Read request for current target state and selected source facts."""

    target: KnowledgeTargetKey
    source_knowledge_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_scope_target_invalid")
        object.__setattr__(
            self,
            "source_knowledge_ids",
            _canonical_text_tuple(
                self.source_knowledge_ids,
                "source_knowledge_ids",
            ),
        )


@dataclass(frozen=True, slots=True)
class KnowledgeIdempotencyLookup:
    """Cheap replay lookup performed before optimistic revision validation."""

    target: KnowledgeTargetKey
    idempotency_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_idempotency_target_invalid")
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(self.idempotency_key, "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class KnowledgePropagationScope:
    """Consistent read model used to build one complete mutation plan."""

    target: KnowledgeTargetKey
    scope_revision: int
    v2_active: bool
    selection_state: KnowledgeSelectionState | str | None
    assignments: tuple[TemporalKnowledgeAssignment, ...] = ()
    tombstones: tuple[KnowledgePropagationTombstone, ...] = ()
    snapshots: tuple[KnowledgePropagationSnapshot, ...] = ()
    legacy_attachments: tuple[KnowledgeLegacyAttachment, ...] = ()
    sources: tuple[KnowledgeSelectableSource, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_scope_target_invalid")
        object.__setattr__(
            self,
            "scope_revision",
            _non_negative_int(self.scope_revision, "scope_revision"),
        )
        if type(self.v2_active) is not bool:
            raise ValueError("knowledge_propagation_scope_v2_active_invalid")
        state = (
            None
            if self.selection_state is None
            else _coerce_enum(
                self.selection_state,
                KnowledgeSelectionState,
                "selection_state",
            )
        )
        if self.v2_active:
            if state not in {
                KnowledgeSelectionState.EXPLICIT_EMPTY,
                KnowledgeSelectionState.EXPLICIT_IDS,
            }:
                raise ValueError(
                    "knowledge_propagation_active_scope_state_invalid"
                )
        elif state is not None:
            raise ValueError("knowledge_propagation_inactive_scope_state_invalid")

        assignments = _canonical_objects(
            self.assignments,
            TemporalKnowledgeAssignment,
            field_name="assignments",
            identity=lambda item: item.assignment.assignment_id,
        )
        for item in assignments:
            assignment = item.assignment
            if (
                assignment.board_id != self.target.board_id
                or assignment.target_type != self.target.target_type
                or assignment.target_id != self.target.target_id
            ):
                raise ValueError(
                    "knowledge_propagation_assignment_target_mismatch"
                )
        tombstones = _canonical_objects(
            self.tombstones,
            KnowledgePropagationTombstone,
            field_name="tombstones",
            identity=lambda item: item.tombstone_id,
        )
        snapshots = _canonical_objects(
            self.snapshots,
            KnowledgePropagationSnapshot,
            field_name="snapshots",
            identity=lambda item: item.snapshot_id,
        )
        legacy = _canonical_objects(
            self.legacy_attachments,
            KnowledgeLegacyAttachment,
            field_name="legacy_attachments",
            identity=lambda item: item.source_knowledge_id,
        )
        sources = _canonical_objects(
            self.sources,
            KnowledgeSelectableSource,
            field_name="sources",
            identity=lambda item: item.requested_knowledge_id,
        )
        for item in tombstones:
            if item.target != self.target:
                raise ValueError(
                    "knowledge_propagation_tombstone_target_mismatch"
                )
        object.__setattr__(self, "selection_state", state)
        object.__setattr__(self, "assignments", assignments)
        object.__setattr__(self, "tombstones", tombstones)
        object.__setattr__(self, "snapshots", snapshots)
        object.__setattr__(self, "legacy_attachments", legacy)
        object.__setattr__(self, "sources", sources)


@dataclass(frozen=True, slots=True)
class KnowledgeMutationReceipt:
    """Immutable success value stored in and returned from the ledger."""

    operation_id: str
    target: KnowledgeTargetKey
    operation_kind: KnowledgeMutationKind | str
    previous_revision: int
    revision: int
    request_hash: str
    applied_at: datetime
    replayed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "operation_id",
            _required_text(self.operation_id, "operation_id"),
        )
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_receipt_target_invalid")
        operation_kind = _coerce_enum(
            self.operation_kind,
            KnowledgeMutationKind,
            "operation_kind",
        )
        previous_revision = _non_negative_int(
            self.previous_revision,
            "previous_revision",
        )
        revision = _non_negative_int(self.revision, "revision")
        if revision != previous_revision + 1:
            raise ValueError("knowledge_propagation_receipt_revision_invalid")
        request_hash = _required_text(self.request_hash, "request_hash")
        if _SHA256_HEX.fullmatch(request_hash) is None:
            raise ValueError("knowledge_propagation_request_hash_invalid")
        if type(self.replayed) is not bool:
            raise ValueError("knowledge_propagation_replayed_invalid")
        object.__setattr__(self, "operation_kind", operation_kind)
        object.__setattr__(self, "previous_revision", previous_revision)
        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "request_hash", request_hash)
        object.__setattr__(self, "applied_at", _utc(self.applied_at, "applied_at"))

    def as_replay(self) -> "KnowledgeMutationReceipt":
        return KnowledgeMutationReceipt(
            operation_id=self.operation_id,
            target=self.target,
            operation_kind=self.operation_kind,
            previous_revision=self.previous_revision,
            revision=self.revision,
            request_hash=self.request_hash,
            applied_at=self.applied_at,
            replayed=True,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
            "operation_id": self.operation_id,
            "target": self.target.to_dict(),
            "operation_kind": cast(
                KnowledgeMutationKind,
                self.operation_kind,
            ).value,
            "previous_revision": self.previous_revision,
            "revision": self.revision,
            "request_hash": self.request_hash,
            "applied_at": self.applied_at.isoformat(),
            "replayed": self.replayed,
        }


@dataclass(frozen=True, slots=True)
class KnowledgeMutationLedgerEntry:
    """Immutable idempotency record written atomically with a mutation."""

    target: KnowledgeTargetKey
    idempotency_key: str
    request_hash: str
    operation_kind: KnowledgeMutationKind | str
    receipt: KnowledgeMutationReceipt
    recorded_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_ledger_target_invalid")
        idempotency_key = _required_text(
            self.idempotency_key,
            "idempotency_key",
        )
        request_hash = _required_text(self.request_hash, "request_hash")
        if _SHA256_HEX.fullmatch(request_hash) is None:
            raise ValueError("knowledge_propagation_request_hash_invalid")
        operation_kind = _coerce_enum(
            self.operation_kind,
            KnowledgeMutationKind,
            "operation_kind",
        )
        if not isinstance(self.receipt, KnowledgeMutationReceipt):
            raise ValueError("knowledge_propagation_ledger_receipt_invalid")
        if (
            self.receipt.target != self.target
            or self.receipt.request_hash != request_hash
            or self.receipt.operation_kind != operation_kind
            or self.receipt.replayed
        ):
            raise ValueError("knowledge_propagation_ledger_receipt_incoherent")
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "request_hash", request_hash)
        object.__setattr__(self, "operation_kind", operation_kind)
        object.__setattr__(
            self,
            "recorded_at",
            _utc(self.recorded_at, "recorded_at"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
            "target": self.target.to_dict(),
            "idempotency_key": self.idempotency_key,
            "request_hash": self.request_hash,
            "operation_kind": cast(
                KnowledgeMutationKind,
                self.operation_kind,
            ).value,
            "receipt": self.receipt.to_dict(),
            "recorded_at": self.recorded_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class KnowledgeMutationPlan:
    """Complete, cancellation-safe write set staged exactly once by an adapter."""

    operation_id: str
    target: KnowledgeTargetKey
    operation_kind: KnowledgeMutationKind | str
    selection: KnowledgeSelection | None
    expected_revision: int
    next_revision: int
    actor_id: str
    occurred_at: datetime
    idempotency_key: str
    request_hash: str
    next_scope_selection_state: KnowledgeSelectionState | str
    assignments_to_open: tuple[TemporalKnowledgeAssignment, ...] = ()
    assignment_ids_to_close: tuple[str, ...] = ()
    tombstones_to_open: tuple[KnowledgePropagationTombstone, ...] = ()
    tombstone_ids_to_close: tuple[str, ...] = ()
    snapshots_to_open: tuple[KnowledgePropagationSnapshot, ...] = ()
    snapshot_ids_to_close: tuple[str, ...] = ()
    ledger_entry: KnowledgeMutationLedgerEntry | None = None

    def __post_init__(self) -> None:
        operation_id = _required_text(self.operation_id, "operation_id")
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_plan_target_invalid")
        kind = _coerce_enum(
            self.operation_kind,
            KnowledgeMutationKind,
            "operation_kind",
        )
        expected_revision = _non_negative_int(
            self.expected_revision,
            "expected_revision",
        )
        next_revision = _non_negative_int(self.next_revision, "next_revision")
        if next_revision != expected_revision + 1:
            raise ValueError("knowledge_propagation_next_revision_invalid")
        actor_id = _required_text(self.actor_id, "actor_id")
        occurred_at = _utc(self.occurred_at, "occurred_at")
        idempotency_key = _required_text(
            self.idempotency_key,
            "idempotency_key",
        )
        request_hash = _required_text(self.request_hash, "request_hash")
        if _SHA256_HEX.fullmatch(request_hash) is None:
            raise ValueError("knowledge_propagation_request_hash_invalid")
        next_state = _coerce_enum(
            self.next_scope_selection_state,
            KnowledgeSelectionState,
            "next_scope_selection_state",
        )
        if next_state is KnowledgeSelectionState.OMITTED:
            raise ValueError("knowledge_propagation_persisted_omitted_state_invalid")
        if self.selection is not None and not isinstance(
            self.selection,
            KnowledgeSelection,
        ):
            raise ValueError("knowledge_propagation_selection_invalid")

        assignments = _canonical_objects(
            self.assignments_to_open,
            TemporalKnowledgeAssignment,
            field_name="assignments_to_open",
            identity=lambda item: item.assignment.assignment_id,
        )
        assignment_ids = _canonical_text_tuple(
            self.assignment_ids_to_close,
            "assignment_ids_to_close",
        )
        tombstones = _canonical_objects(
            self.tombstones_to_open,
            KnowledgePropagationTombstone,
            field_name="tombstones_to_open",
            identity=lambda item: item.tombstone_id,
        )
        tombstone_ids = _canonical_text_tuple(
            self.tombstone_ids_to_close,
            "tombstone_ids_to_close",
        )
        snapshots = _canonical_objects(
            self.snapshots_to_open,
            KnowledgePropagationSnapshot,
            field_name="snapshots_to_open",
            identity=lambda item: item.snapshot_id,
        )
        snapshot_ids = _canonical_text_tuple(
            self.snapshot_ids_to_close,
            "snapshot_ids_to_close",
        )
        for assignment in assignments:
            value = assignment.assignment
            if (
                value.board_id != self.target.board_id
                or value.target_type != self.target.target_type
                or value.target_id != self.target.target_id
                or value.revision != next_revision
                or assignment.temporal.effective_from != occurred_at
                or not assignment.temporal.is_current
            ):
                raise ValueError(
                    "knowledge_propagation_plan_assignment_incoherent"
                )
        for tombstone in tombstones:
            if (
                tombstone.target != self.target
                or tombstone.temporal.effective_from != occurred_at
                or not tombstone.temporal.is_current
            ):
                raise ValueError(
                    "knowledge_propagation_plan_tombstone_incoherent"
                )
        snapshot_assignments = {
            assignment.assignment.assignment_id: assignment.assignment
            for assignment in assignments
        }
        for snapshot in snapshots:
            assignment = snapshot_assignments.get(snapshot.assignment_id)
            if (
                assignment is None
                or snapshot.temporal.effective_from != occurred_at
                or not snapshot.temporal.is_current
                or assignment.mode is not KnowledgePropagationMode.SNAPSHOT
                or assignment.revision_stamp != snapshot.revision_stamp
            ):
                raise ValueError(
                    "knowledge_propagation_plan_snapshot_incoherent"
                )

        if kind is KnowledgeMutationKind.REPLACE:
            if (
                self.selection is None
                or self.selection.selection_state
                is not KnowledgeSelectionState.EXPLICIT_IDS
                or self.selection.mode
                not in {
                    KnowledgePropagationMode.REFERENCE,
                    KnowledgePropagationMode.SNAPSHOT,
                }
                or next_state is not KnowledgeSelectionState.EXPLICIT_IDS
            ):
                raise ValueError("knowledge_propagation_replace_plan_invalid")
        elif kind is KnowledgeMutationKind.DROP_DELTA:
            if (
                self.selection is None
                or self.selection.selection_state
                is not KnowledgeSelectionState.EXPLICIT_IDS
                or self.selection.mode is not KnowledgePropagationMode.DROP
                or not tombstones
            ):
                raise ValueError("knowledge_propagation_drop_delta_plan_invalid")
        elif kind is KnowledgeMutationKind.REPLACE_EMPTY:
            if (
                self.selection is None
                or self.selection.selection_state
                is not KnowledgeSelectionState.EXPLICIT_EMPTY
                or assignments
                or next_state is not KnowledgeSelectionState.EXPLICIT_EMPTY
            ):
                raise ValueError("knowledge_propagation_replace_empty_plan_invalid")
        elif kind is KnowledgeMutationKind.REFRESH_SNAPSHOT:
            if self.selection is not None or not snapshots:
                raise ValueError(
                    "knowledge_propagation_refresh_snapshot_plan_invalid"
                )
        elif self.selection is not None:
            raise ValueError("knowledge_propagation_grandfather_plan_invalid")

        if not isinstance(self.ledger_entry, KnowledgeMutationLedgerEntry):
            raise ValueError("knowledge_propagation_ledger_entry_required")
        receipt = self.ledger_entry.receipt
        if (
            self.ledger_entry.target != self.target
            or self.ledger_entry.idempotency_key != idempotency_key
            or self.ledger_entry.request_hash != request_hash
            or self.ledger_entry.operation_kind != kind
            or receipt.operation_id != operation_id
            or receipt.previous_revision != expected_revision
            or receipt.revision != next_revision
            or receipt.applied_at != occurred_at
            or self.ledger_entry.recorded_at != occurred_at
        ):
            raise ValueError("knowledge_propagation_ledger_entry_incoherent")

        object.__setattr__(self, "operation_id", operation_id)
        object.__setattr__(self, "operation_kind", kind)
        object.__setattr__(self, "expected_revision", expected_revision)
        object.__setattr__(self, "next_revision", next_revision)
        object.__setattr__(self, "actor_id", actor_id)
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "request_hash", request_hash)
        object.__setattr__(self, "next_scope_selection_state", next_state)
        object.__setattr__(self, "assignments_to_open", assignments)
        object.__setattr__(self, "assignment_ids_to_close", assignment_ids)
        object.__setattr__(self, "tombstones_to_open", tombstones)
        object.__setattr__(self, "tombstone_ids_to_close", tombstone_ids)
        object.__setattr__(self, "snapshots_to_open", snapshots)
        object.__setattr__(self, "snapshot_ids_to_close", snapshot_ids)


class KnowledgePropagationPort(Protocol):
    """Edition-owned storage adapter; every method uses the caller's UoW."""

    async def get_idempotency_entry(
        self,
        context: Any,
        request: KnowledgeIdempotencyLookup,
    ) -> KnowledgeMutationLedgerEntry | None: ...

    async def load_scope(
        self,
        context: Any,
        request: KnowledgeScopeLookup,
    ) -> KnowledgePropagationScope: ...

    async def stage_mutation(
        self,
        context: Any,
        plan: KnowledgeMutationPlan,
    ) -> KnowledgeMutationReceipt: ...


def register_knowledge_propagation_port(port: KnowledgePropagationPort) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_knowledge_propagation_port() -> KnowledgePropagationPort:
    return require_runtime_value(
        _RUNTIME_KEY,
        "knowledge_propagation_port_not_configured",
    )


def reset_knowledge_propagation_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "KnowledgeIdempotencyLookup",
    "KnowledgeLegacyAttachment",
    "KnowledgeMutationKind",
    "KnowledgeMutationLedgerEntry",
    "KnowledgeMutationPlan",
    "KnowledgeMutationReceipt",
    "KnowledgePropagationPort",
    "KnowledgePropagationPortError",
    "KnowledgePropagationScope",
    "KnowledgePropagationSnapshot",
    "KnowledgePropagationTombstone",
    "KnowledgeScopeLookup",
    "KnowledgeSelectableSource",
    "KnowledgeTargetKey",
    "KnowledgeTemporalWindow",
    "TemporalKnowledgeAssignment",
    "get_knowledge_propagation_port",
    "register_knowledge_propagation_port",
    "reset_knowledge_propagation_port_for_tests",
]
