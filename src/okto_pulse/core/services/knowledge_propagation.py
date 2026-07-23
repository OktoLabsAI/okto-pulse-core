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
from uuid import UUID, uuid4, uuid5

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
    KnowledgeTargetType,
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
    KnowledgeParentEvidence,
    KnowledgeParentKey,
    KnowledgeParentLookup,
    KnowledgeParentType,
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


_KNOWLEDGE_TARGET_NAMESPACE = UUID("56d7316a-f60b-5b74-b7d0-b870f0b6e1cb")


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


def _optional_sha256(value: object | None, field_name: str) -> str | None:
    normalized = _optional_text(value, field_name)
    if normalized is None:
        return None
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    return normalized


def deterministic_knowledge_target_id(
    parent: KnowledgeParentKey,
    target_type: KnowledgeTargetType | str,
    idempotency_key: str,
) -> str:
    """Return the stable UUIDv5 identity for one semantic create attempt."""

    if not isinstance(parent, KnowledgeParentKey):
        raise ValueError("knowledge_propagation_parent_invalid")
    canonical_target_type = (
        target_type
        if isinstance(target_type, KnowledgeTargetType)
        else KnowledgeTargetType(_required_text(target_type, "target_type"))
    )
    canonical_idempotency_key = _required_text(
        idempotency_key,
        "idempotency_key",
    )
    identity = json.dumps(
        {
            "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
            "parent": parent.to_dict(),
            "creation_operation": (
                "derive_spec"
                if canonical_target_type is KnowledgeTargetType.SPEC
                else "create_card"
            ),
            "target_type": canonical_target_type.value,
            "idempotency_key": canonical_idempotency_key,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return str(uuid5(_KNOWLEDGE_TARGET_NAMESPACE, identity))


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


def _freeze_json(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _canonical_json_mapping(
    value: Mapping[str, object] | None,
    field_name: str,
) -> Mapping[str, object]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    try:
        encoded = json.dumps(
            _thaw_json(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"knowledge_propagation_{field_name}_invalid") from exc
    if not isinstance(decoded, dict):
        raise ValueError(f"knowledge_propagation_{field_name}_invalid")
    return cast(Mapping[str, object], _freeze_json(decoded))


def _require_creation_result_target(
    value: Mapping[str, object],
    target: KnowledgeTargetKey,
) -> None:
    if not value:
        return
    spec_id = value.get("spec_id")
    spec = value.get("spec")
    card = value.get("card")
    projected_target_id: object | None = None
    if spec_id is not None:
        projected_target_id = spec_id
    elif isinstance(spec, Mapping):
        projected_target_id = spec.get("id")
    elif isinstance(card, Mapping):
        projected_target_id = card.get("id")
    if projected_target_id != target.target_id:
        raise ValueError("knowledge_propagation_creation_result_target_mismatch")


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
    semantic_creation_hash: str | None = None
    parent: KnowledgeParentKey | None = None
    creation_result: Mapping[str, object] = field(default_factory=dict)

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
        semantic_creation_hash = _optional_sha256(
            self.semantic_creation_hash,
            "semantic_creation_hash",
        )
        if semantic_creation_hash is not None and expected_revision != 0:
            raise ValueError("knowledge_propagation_creation_expected_revision_invalid")
        object.__setattr__(
            self,
            "semantic_creation_hash",
            semantic_creation_hash,
        )
        if self.parent is not None:
            if not isinstance(self.parent, KnowledgeParentKey):
                raise ValueError("knowledge_propagation_command_parent_invalid")
            if self.parent.board_id != self.target.board_id:
                raise ValueError("knowledge_propagation_command_parent_board_mismatch")
            if (
                self.target.target_type is KnowledgeTargetType.CARD
                and self.parent.parent_type is not KnowledgeParentType.SPEC
            ):
                raise ValueError("knowledge_propagation_command_parent_target_invalid")
        if self.relevance_links and self.parent is None:
            raise ValueError("knowledge_propagation_relevance_parent_required")
        if semantic_creation_hash is not None and self.parent is None:
            raise ValueError("knowledge_propagation_creation_parent_required")
        creation_result = _canonical_json_mapping(
            self.creation_result,
            "creation_result",
        )
        if creation_result and self.parent is None:
            raise ValueError("knowledge_propagation_creation_parent_required")
        _require_creation_result_target(creation_result, self.target)
        object.__setattr__(self, "creation_result", creation_result)


@dataclass(frozen=True, slots=True)
class KnowledgeRefreshCommand:
    """Deprecated internal refresh by assignment identity.

    Public v2 boundaries must use :class:`KnowledgeRefreshByKnowledgeIdsCommand`
    so retries do not depend on replaceable assignment row identities.
    """

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
class KnowledgeRefreshByKnowledgeIdsCommand:
    """Public v2 refresh selected by stable parent Knowledge Base ids."""

    target: KnowledgeTargetKey
    knowledge_ids: tuple[str, ...]
    actor_id: str
    expected_revision: int
    idempotency_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_refresh_target_invalid")
        knowledge_ids = _canonical_ids(self.knowledge_ids, "knowledge_ids")
        if not knowledge_ids:
            raise ValueError("knowledge_propagation_knowledge_ids_empty")
        object.__setattr__(self, "knowledge_ids", knowledge_ids)
        object.__setattr__(
            self,
            "actor_id",
            _required_text(self.actor_id, "actor_id"),
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
class KnowledgeCreationPreflightCommand:
    """Validate a complete v2 create before its deterministic target exists."""

    parent: KnowledgeParentKey
    target_type: KnowledgeTargetType | str
    selection: KnowledgeSelection
    actor_id: str
    idempotency_key: str
    expected_revision: int | None = None
    justification: str | None = None
    relevance_links: tuple[KnowledgeRelevanceLink, ...] = ()
    semantic_creation_hash: str | None = None
    creation_result: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.parent, KnowledgeParentKey):
            raise ValueError("knowledge_propagation_preflight_parent_invalid")
        target_type = (
            self.target_type
            if isinstance(self.target_type, KnowledgeTargetType)
            else KnowledgeTargetType(_required_text(self.target_type, "target_type"))
        )
        if (
            target_type is KnowledgeTargetType.SPEC
            and self.parent.parent_type
            not in {KnowledgeParentType.IDEATION, KnowledgeParentType.REFINEMENT}
        ) or (
            target_type is KnowledgeTargetType.CARD
            and self.parent.parent_type is not KnowledgeParentType.SPEC
        ):
            raise ValueError("knowledge_propagation_preflight_parent_target_invalid")
        if not isinstance(self.selection, KnowledgeSelection):
            raise ValueError("knowledge_propagation_preflight_selection_invalid")
        actor_id = _required_text(self.actor_id, "actor_id")
        idempotency_key = _required_text(
            self.idempotency_key,
            "idempotency_key",
        )
        if self.expected_revision not in {None, 0}:
            raise ValueError("knowledge_propagation_creation_expected_revision_invalid")
        justification = _optional_text(self.justification, "justification")
        if (
            self.selection.selection_state is not KnowledgeSelectionState.OMITTED
            and justification is None
        ):
            raise ValueError("knowledge_propagation_justification_required")
        object.__setattr__(self, "target_type", target_type)
        object.__setattr__(self, "actor_id", actor_id)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "expected_revision", 0)
        object.__setattr__(self, "justification", justification)
        object.__setattr__(
            self,
            "relevance_links",
            _canonical_links(self.relevance_links),
        )
        object.__setattr__(
            self,
            "semantic_creation_hash",
            _optional_sha256(
                self.semantic_creation_hash,
                "semantic_creation_hash",
            ),
        )
        object.__setattr__(
            self,
            "creation_result",
            _canonical_json_mapping(
                self.creation_result,
                "creation_result",
            ),
        )

    @property
    def target(self) -> KnowledgeTargetKey:
        return KnowledgeTargetKey(
            board_id=self.parent.board_id,
            target_type=self.target_type,
            target_id=deterministic_knowledge_target_id(
                self.parent,
                self.target_type,
                self.idempotency_key,
            ),
        )

    def to_mutation_command(self) -> KnowledgeMutationCommand:
        return KnowledgeMutationCommand(
            target=self.target,
            selection=self.selection,
            actor_id=self.actor_id,
            expected_revision=0,
            idempotency_key=self.idempotency_key,
            justification=self.justification,
            relevance_links=self.relevance_links,
            semantic_creation_hash=self.semantic_creation_hash,
            parent=self.parent,
            creation_result=self.creation_result,
        )


@dataclass(frozen=True, slots=True)
class KnowledgeMutationPreparation:
    """Immutable successful target-independent creation preflight."""

    parent: KnowledgeParentKey
    command: KnowledgeMutationCommand
    evidence_fingerprint: str

    def __post_init__(self) -> None:
        if not isinstance(self.parent, KnowledgeParentKey):
            raise ValueError("knowledge_propagation_preparation_parent_invalid")
        if not isinstance(self.command, KnowledgeMutationCommand):
            raise ValueError("knowledge_propagation_preparation_command_invalid")
        if self.command.target.board_id != self.parent.board_id:
            raise ValueError("knowledge_propagation_preparation_board_mismatch")
        expected_target_id = deterministic_knowledge_target_id(
            self.parent,
            self.command.target.target_type,
            self.command.idempotency_key,
        )
        if self.command.target.target_id != expected_target_id:
            raise ValueError("knowledge_propagation_preparation_target_invalid")
        object.__setattr__(
            self,
            "evidence_fingerprint",
            cast(
                str,
                _optional_sha256(
                    self.evidence_fingerprint,
                    "evidence_fingerprint",
                ),
            ),
        )


@dataclass(frozen=True, slots=True)
class KnowledgeMutationResultV2:
    """Canonical versioned API result persisted inside receipt details."""

    operation_id: str
    target: KnowledgeTargetKey
    operation_kind: KnowledgeMutationKind | str
    previous_revision: int
    revision: int
    selection_state: KnowledgeSelectionState | str | None
    assignments: tuple[KnowledgeAssignment, ...] = ()
    refreshed_knowledge_ids: tuple[str, ...] = ()
    creation_result: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "operation_id",
            _required_text(self.operation_id, "operation_id"),
        )
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_result_target_invalid")
        operation_kind = (
            self.operation_kind
            if isinstance(self.operation_kind, KnowledgeMutationKind)
            else KnowledgeMutationKind(
                _required_text(self.operation_kind, "operation_kind")
            )
        )
        previous_revision = _revision(self.previous_revision)
        revision = _revision(self.revision)
        if revision != previous_revision + 1:
            raise ValueError("knowledge_propagation_result_revision_invalid")
        selection_state = (
            None
            if self.selection_state is None
            else (
                self.selection_state
                if isinstance(self.selection_state, KnowledgeSelectionState)
                else KnowledgeSelectionState(
                    _required_text(self.selection_state, "selection_state")
                )
            )
        )
        assignments: dict[str, KnowledgeAssignment] = {}
        for assignment in self.assignments:
            if not isinstance(assignment, KnowledgeAssignment):
                raise ValueError("knowledge_propagation_result_assignments_invalid")
            if (
                assignment.board_id != self.target.board_id
                or assignment.target_type is not self.target.target_type
                or assignment.target_id != self.target.target_id
            ):
                raise ValueError(
                    "knowledge_propagation_result_assignment_target_mismatch"
                )
            if assignment.assignment_id in assignments:
                raise ValueError("knowledge_propagation_result_assignments_duplicate")
            assignments[assignment.assignment_id] = assignment
        object.__setattr__(self, "operation_kind", operation_kind)
        object.__setattr__(self, "previous_revision", previous_revision)
        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "selection_state", selection_state)
        object.__setattr__(
            self,
            "assignments",
            tuple(assignments[key] for key in sorted(assignments)),
        )
        object.__setattr__(
            self,
            "refreshed_knowledge_ids",
            _canonical_ids(
                self.refreshed_knowledge_ids,
                "refreshed_knowledge_ids",
            ),
        )
        creation_result = _canonical_json_mapping(
            self.creation_result,
            "result_creation_result",
        )
        _require_creation_result_target(creation_result, self.target)
        object.__setattr__(self, "creation_result", creation_result)

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
            "selection_state": (
                None
                if self.selection_state is None
                else cast(KnowledgeSelectionState, self.selection_state).value
            ),
            "assignments": [assignment.to_dict() for assignment in self.assignments],
            "refreshed_knowledge_ids": list(self.refreshed_knowledge_ids),
            "creation_result": _thaw_json(self.creation_result),
        }


class KnowledgeMutationResultV2Projector:
    """Create and recover the canonical result carried by every receipt."""

    @staticmethod
    def project(
        *,
        operation_id: str,
        target: KnowledgeTargetKey,
        operation_kind: KnowledgeMutationKind,
        previous_revision: int,
        selection_state: KnowledgeSelectionState | None,
        assignments: tuple[TemporalKnowledgeAssignment, ...],
        refreshed_knowledge_ids: tuple[str, ...] = (),
        creation_result: Mapping[str, object] | None = None,
    ) -> KnowledgeMutationResultV2:
        return KnowledgeMutationResultV2(
            operation_id=operation_id,
            target=target,
            operation_kind=operation_kind,
            previous_revision=previous_revision,
            revision=previous_revision + 1,
            selection_state=selection_state,
            assignments=tuple(item.assignment for item in assignments),
            refreshed_knowledge_ids=refreshed_knowledge_ids,
            creation_result=creation_result or {},
        )

    @staticmethod
    def from_receipt(
        receipt: KnowledgeMutationReceipt,
    ) -> KnowledgeMutationResultV2:
        if not isinstance(receipt, KnowledgeMutationReceipt):
            raise ValueError("knowledge_propagation_result_receipt_invalid")
        payload = receipt.details.get("result_v2")
        if not isinstance(payload, Mapping):
            raise ValueError("knowledge_propagation_result_v2_missing")
        if payload.get("contract_version") != (KNOWLEDGE_PROPAGATION_CONTRACT_VERSION):
            raise ValueError("knowledge_propagation_result_contract_version_invalid")
        target_payload = payload.get("target")
        assignments_payload = payload.get("assignments")
        if (
            not isinstance(target_payload, Mapping)
            or not isinstance(
                assignments_payload,
                Sequence,
            )
            or isinstance(assignments_payload, (str, bytes))
        ):
            raise ValueError("knowledge_propagation_result_v2_invalid")
        assignments: list[KnowledgeAssignment] = []
        for item in assignments_payload:
            if not isinstance(item, Mapping):
                raise ValueError("knowledge_propagation_result_v2_invalid")
            stamp = item.get("revision_stamp")
            links = item.get("relevance_links", ())
            if (
                not isinstance(stamp, Mapping)
                or not isinstance(
                    links,
                    Sequence,
                )
                or isinstance(links, (str, bytes))
            ):
                raise ValueError("knowledge_propagation_result_v2_invalid")
            assignments.append(
                KnowledgeAssignment(
                    assignment_id=cast(str, item.get("assignment_id")),
                    board_id=cast(str, item.get("board_id")),
                    target_type=cast(str, item.get("target_type")),
                    target_id=cast(str, item.get("target_id")),
                    source_knowledge_id=cast(
                        str,
                        item.get("source_knowledge_id"),
                    ),
                    revision_stamp=ResourceRevisionStamp(
                        root_id=cast(str, stamp.get("root_id")),
                        immediate_parent_id=cast(
                            str | None,
                            stamp.get("immediate_parent_id"),
                        ),
                        source_revision=cast(
                            str | None,
                            stamp.get("source_revision"),
                        ),
                        source_content_sha256=cast(
                            str | None,
                            stamp.get("source_content_sha256"),
                        ),
                    ),
                    mode=cast(str, item.get("mode")),
                    state=cast(str, item.get("state")),
                    origin_class=cast(str, item.get("origin_class")),
                    actor_id=cast(str, item.get("actor_id")),
                    revision=cast(int, item.get("revision")),
                    justification=cast(str | None, item.get("justification")),
                    relevance_links=tuple(
                        KnowledgeRelevanceLink(
                            entity_type=cast(str, link.get("entity_type")),
                            entity_id=cast(str, link.get("entity_id")),
                        )
                        for link in links
                        if isinstance(link, Mapping)
                    ),
                )
            )
        refreshed = payload.get("refreshed_knowledge_ids", ())
        if not isinstance(refreshed, Sequence) or isinstance(
            refreshed,
            (str, bytes),
        ):
            raise ValueError("knowledge_propagation_result_v2_invalid")
        result = KnowledgeMutationResultV2(
            operation_id=cast(str, payload.get("operation_id")),
            target=KnowledgeTargetKey(
                board_id=cast(str, target_payload.get("board_id")),
                target_type=cast(str, target_payload.get("target_type")),
                target_id=cast(str, target_payload.get("target_id")),
            ),
            operation_kind=cast(str, payload.get("operation_kind")),
            previous_revision=cast(int, payload.get("previous_revision")),
            revision=cast(int, payload.get("revision")),
            selection_state=cast(
                str | None,
                payload.get("selection_state"),
            ),
            assignments=tuple(assignments),
            refreshed_knowledge_ids=tuple(cast(Sequence[str], refreshed)),
            creation_result=cast(
                Mapping[str, object],
                payload.get("creation_result", {}),
            ),
        )
        if (
            result.operation_id != receipt.operation_id
            or result.target != receipt.target
            or result.operation_kind is not receipt.operation_kind
            or result.previous_revision != receipt.previous_revision
            or result.revision != receipt.revision
        ):
            raise ValueError("knowledge_propagation_result_receipt_mismatch")
        return result


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

    def to_dict(self) -> dict[str, bool]:
        return {
            "durable_selection_evidence": self.durable_selection_evidence,
            "origin_missing": self.origin_missing,
            "origin_cycle": self.origin_cycle,
            "content_divergent": self.content_divergent,
        }


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
class KnowledgeGrandfatherAttachment:
    """One physical legacy attachment and its bounded classification evidence."""

    source_knowledge_id: str
    revision_stamp: ResourceRevisionStamp
    evidence: KnowledgeGrandfatherEvidence
    physical_locator: Mapping[str, str]

    def __post_init__(self) -> None:
        source_knowledge_id = _required_text(
            self.source_knowledge_id,
            "source_knowledge_id",
        )
        if not isinstance(self.evidence, KnowledgeGrandfatherEvidence):
            raise ValueError("knowledge_propagation_grandfather_evidence_invalid")
        origin_class = classify_legacy_origin(self.evidence)
        canonical_legacy = KnowledgeLegacyAttachment(
            source_knowledge_id=source_knowledge_id,
            revision_stamp=self.revision_stamp,
            origin_class=origin_class,
            effective=origin_class is not KnowledgeOriginClass.LEGACY_UNRESOLVED,
        )
        if not isinstance(self.physical_locator, Mapping):
            raise ValueError(
                "knowledge_propagation_grandfather_physical_locator_invalid"
            )
        required_locator_fields = (
            "storage_kind",
            "table",
            "owner_id",
            "attachment_id",
        )
        if set(self.physical_locator) != set(required_locator_fields):
            raise ValueError(
                "knowledge_propagation_grandfather_physical_locator_invalid"
            )
        locator = {
            field_name: _required_text(
                self.physical_locator[field_name],
                f"physical_locator_{field_name}",
            )
            for field_name in required_locator_fields
        }
        if locator["storage_kind"] not in {"entity_row", "card_json"}:
            raise ValueError("knowledge_propagation_grandfather_storage_kind_invalid")
        object.__setattr__(
            self,
            "source_knowledge_id",
            canonical_legacy.source_knowledge_id,
        )
        object.__setattr__(
            self,
            "revision_stamp",
            canonical_legacy.revision_stamp,
        )
        object.__setattr__(
            self,
            "physical_locator",
            MappingProxyType(locator),
        )

    @property
    def origin_class(self) -> KnowledgeOriginClass:
        return classify_legacy_origin(self.evidence)

    @property
    def effective(self) -> bool:
        return self.origin_class is not KnowledgeOriginClass.LEGACY_UNRESOLVED

    def to_legacy_attachment(self) -> KnowledgeLegacyAttachment:
        return KnowledgeLegacyAttachment(
            source_knowledge_id=self.source_knowledge_id,
            revision_stamp=self.revision_stamp,
            origin_class=self.origin_class,
            effective=self.effective,
        )

    def to_dict(self) -> dict[str, object]:
        stamp = self.revision_stamp
        return {
            "source_knowledge_id": self.source_knowledge_id,
            "root_id": stamp.root_id,
            "immediate_parent_id": stamp.immediate_parent_id,
            "source_revision": stamp.source_revision,
            "source_content_sha256": stamp.source_content_sha256,
            "origin_class": self.origin_class.value,
            "effective": self.effective,
            "evidence": self.evidence.to_dict(),
            "physical_locator": dict(self.physical_locator),
        }


@dataclass(frozen=True, slots=True)
class KnowledgeGrandfatherCommand:
    """Classify all physical legacy attachments for one inactive target."""

    target: KnowledgeTargetKey
    attachments: tuple[KnowledgeGrandfatherAttachment, ...]
    actor_id: str
    expected_revision: int
    idempotency_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.target, KnowledgeTargetKey):
            raise ValueError("knowledge_propagation_grandfather_target_invalid")
        if isinstance(self.attachments, (str, bytes)) or not isinstance(
            self.attachments,
            Sequence,
        ):
            raise ValueError("knowledge_propagation_grandfather_attachments_invalid")
        attachments: dict[str, KnowledgeGrandfatherAttachment] = {}
        physical_identities: set[tuple[str, str, str, str]] = set()
        for item in self.attachments:
            if not isinstance(item, KnowledgeGrandfatherAttachment):
                raise ValueError(
                    "knowledge_propagation_grandfather_attachments_invalid"
                )
            if item.source_knowledge_id in attachments:
                raise ValueError("knowledge_propagation_grandfather_source_duplicate")
            locator = item.physical_locator
            physical_identity = (
                locator["storage_kind"],
                locator["table"],
                locator["owner_id"],
                locator["attachment_id"],
            )
            if physical_identity in physical_identities:
                raise ValueError("knowledge_propagation_grandfather_locator_duplicate")
            attachments[item.source_knowledge_id] = item
            physical_identities.add(physical_identity)
        if not attachments:
            raise ValueError("knowledge_propagation_grandfather_attachments_empty")
        ordered = tuple(
            sorted(
                attachments.values(),
                key=lambda item: (
                    item.source_knowledge_id,
                    item.physical_locator["storage_kind"],
                    item.physical_locator["table"],
                    item.physical_locator["owner_id"],
                    item.physical_locator["attachment_id"],
                ),
            )
        )
        object.__setattr__(self, "attachments", ordered)
        object.__setattr__(
            self,
            "actor_id",
            _required_text(self.actor_id, "actor_id"),
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

    async def preflight_creation(
        self,
        context: Any,
        command: KnowledgeCreationPreflightCommand,
    ) -> KnowledgeMutationPreparation | KnowledgeMutationReceipt:
        """Validate a future deterministic target without creating it."""

        if not isinstance(command, KnowledgeCreationPreflightCommand):
            raise TypeError("knowledge_propagation_preflight_command_invalid")
        mutation_command = command.to_mutation_command()
        request_hash = self._mutation_request_hash(mutation_command)
        operation_kind = self._mutation_kind(command.selection)
        try:
            replay = await self._replay(
                context,
                target=mutation_command.target,
                actor_id=mutation_command.actor_id,
                operation_kind=operation_kind,
                idempotency_key=mutation_command.idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay
            evidence = await self._load_and_validate_parent_evidence(
                context,
                parent=command.parent,
                command=mutation_command,
            )
            return KnowledgeMutationPreparation(
                parent=command.parent,
                command=mutation_command,
                evidence_fingerprint=self._parent_evidence_fingerprint(
                    evidence,
                ),
            )
        except KnowledgePropagationServiceError as exc:
            wrapped = self._with_rejection_attempt(
                exc,
                target=mutation_command.target,
                actor_id=mutation_command.actor_id,
                operation_kind=operation_kind,
                idempotency_key=mutation_command.idempotency_key,
                request_hash=request_hash,
            )
            if wrapped is exc:
                raise
            raise wrapped from exc

    async def mutate(
        self,
        context: Any,
        command: KnowledgeMutationCommand | KnowledgeMutationPreparation,
    ) -> KnowledgeMutationReceipt:
        """Stage one all-or-nothing selection mutation."""

        preparation = (
            command if isinstance(command, KnowledgeMutationPreparation) else None
        )
        mutation_command = (
            command.command
            if isinstance(command, KnowledgeMutationPreparation)
            else command
        )
        if not isinstance(mutation_command, KnowledgeMutationCommand):
            raise TypeError("knowledge_propagation_command_invalid")
        request_hash = self._mutation_request_hash(mutation_command)
        operation_kind = self._mutation_kind(mutation_command.selection)
        try:
            replay = await self._replay(
                context,
                target=mutation_command.target,
                actor_id=mutation_command.actor_id,
                operation_kind=operation_kind,
                idempotency_key=mutation_command.idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay

            parent = (
                preparation.parent
                if preparation is not None
                else mutation_command.parent
            )
            evidence: KnowledgeParentEvidence | None = None
            if parent is not None:
                evidence = await self._load_and_validate_parent_evidence(
                    context,
                    parent=parent,
                    command=mutation_command,
                )
                if preparation is not None and (
                    self._parent_evidence_fingerprint(evidence)
                    != preparation.evidence_fingerprint
                ):
                    raise KnowledgePropagationServiceError(
                        "knowledge_propagation_preflight_stale",
                        "parent evidence changed after creation preflight",
                    )

            source_ids = (
                mutation_command.selection.knowledge_ids
                if mutation_command.selection.selection_state
                is KnowledgeSelectionState.EXPLICIT_IDS
                else ()
            )
            scope = await self._port.load_scope(
                context,
                KnowledgeScopeLookup(
                    target=mutation_command.target,
                    source_knowledge_ids=source_ids,
                ),
            )
            self._require_scope_target(scope, mutation_command.target)
            self._require_revision(scope, mutation_command.expected_revision)
            sources = self._validated_sources(
                mutation_command.selection,
                scope,
            )
            plan = self._build_mutation_plan(
                mutation_command,
                scope=scope,
                sources=sources,
                request_hash=request_hash,
                parent_evidence=evidence,
            )
            return await self._stage(context, plan)
        except KnowledgePropagationServiceError as exc:
            wrapped = self._with_rejection_attempt(
                exc,
                target=mutation_command.target,
                actor_id=mutation_command.actor_id,
                operation_kind=operation_kind,
                idempotency_key=mutation_command.idempotency_key,
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

    async def refresh_by_knowledge_ids(
        self,
        context: Any,
        command: KnowledgeRefreshByKnowledgeIdsCommand,
    ) -> KnowledgeMutationReceipt:
        """Refresh snapshots by stable parent/root identities."""

        if not isinstance(command, KnowledgeRefreshByKnowledgeIdsCommand):
            raise TypeError("knowledge_propagation_refresh_command_invalid")
        request_hash = self._refresh_by_knowledge_ids_request_hash(command)
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
                KnowledgeScopeLookup(
                    target=command.target,
                    source_knowledge_ids=command.knowledge_ids,
                ),
            )
            self._require_scope_target(scope, command.target)
            self._require_revision(scope, command.expected_revision)
            plan = self._build_refresh_by_knowledge_ids_plan(
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

    async def grandfather(
        self,
        context: Any,
        command: KnowledgeGrandfatherCommand,
    ) -> KnowledgeMutationReceipt:
        """Stage one durable, non-activating classification of legacy history."""

        if not isinstance(command, KnowledgeGrandfatherCommand):
            raise TypeError("knowledge_propagation_grandfather_command_invalid")
        request_hash = self._grandfather_request_hash(command)
        operation_kind = KnowledgeMutationKind.GRANDFATHER
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
            if scope.v2_active:
                raise KnowledgePropagationServiceError(
                    "knowledge_propagation_grandfather_v2_active",
                    "legacy classification cannot replace an active v2 scope",
                )
            physical_ids = {
                item.source_knowledge_id for item in scope.legacy_attachments
            }
            requested_ids = {item.source_knowledge_id for item in command.attachments}
            if physical_ids != requested_ids:
                raise KnowledgePropagationServiceError(
                    "knowledge_propagation_grandfather_attachment_mismatch",
                    "grandfathering must classify every current physical "
                    "legacy attachment exactly once",
                    details={
                        "requested": sorted(requested_ids),
                        "matched": sorted(physical_ids & requested_ids),
                        "missing": sorted(requested_ids - physical_ids),
                        "unclassified": sorted(physical_ids - requested_ids),
                    },
                )
            occurred_at = self._operation_time()
            plan = self._plan(
                kind=operation_kind,
                target=command.target,
                selection=None,
                expected_revision=command.expected_revision,
                next_scope_selection_state=None,
                next_scope_v2_active=False,
                actor_id=command.actor_id,
                idempotency_key=command.idempotency_key,
                request_hash=request_hash,
                occurred_at=occurred_at,
                outcome=KnowledgeMutationOutcome.GRANDFATHERED,
                receipt_details={
                    "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
                    "legacy_content_preserved": True,
                    "grandfathered_attachments": [
                        item.to_dict() for item in command.attachments
                    ],
                },
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

    async def _load_and_validate_parent_evidence(
        self,
        context: Any,
        *,
        parent: KnowledgeParentKey,
        command: KnowledgeMutationCommand,
    ) -> KnowledgeParentEvidence:
        source_ids = (
            command.selection.knowledge_ids
            if command.selection.selection_state is KnowledgeSelectionState.EXPLICIT_IDS
            else ()
        )
        lookup = KnowledgeParentLookup(
            parent=parent,
            source_knowledge_ids=source_ids,
            relevance_links=command.relevance_links,
        )
        try:
            evidence = await self._port.load_parent_evidence(context, lookup)
        except KnowledgePropagationPortError as exc:
            raise KnowledgePropagationServiceError(
                exc.code,
                exc.detail,
                details=exc.details,
            ) from exc
        if (
            not isinstance(evidence, KnowledgeParentEvidence)
            or evidence.parent != parent
        ):
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_parent_evidence_mismatch",
                "persistence returned evidence for a different parent",
            )
        if not evidence.parent_exists or not evidence.same_board:
            raise KnowledgePropagationServiceError(
                "knowledge_propagation_parent_ineligible",
                "parent must exist on the requested board",
                details={
                    "parent_exists": evidence.parent_exists,
                    "same_board": evidence.same_board,
                    "parent_state": evidence.parent_state,
                },
            )
        self._validated_sources_from(
            command.selection,
            evidence.sources,
        )
        self._validate_relevance_links(
            parent=parent,
            links=command.relevance_links,
            evidence=evidence,
        )
        return evidence

    @staticmethod
    def _validate_relevance_links(
        *,
        parent: KnowledgeParentKey,
        links: tuple[KnowledgeRelevanceLink, ...],
        evidence: KnowledgeParentEvidence,
    ) -> None:
        if not links:
            return
        linked_spec_id = evidence.linked_spec_id
        if linked_spec_id is None or (
            parent.parent_type is KnowledgeParentType.SPEC
            and linked_spec_id != parent.parent_id
        ):
            raise KnowledgePropagationServiceError(
                "knowledge_relevance_spec_mismatch",
                "relevance links must resolve against the target's linked spec",
            )
        allowed = {
            KnowledgeRelevanceEntityType.FUNCTIONAL_REQUIREMENT: set(
                evidence.functional_requirement_ids
            ),
            KnowledgeRelevanceEntityType.ACCEPTANCE_CRITERION: set(
                evidence.acceptance_criterion_ids
            ),
            KnowledgeRelevanceEntityType.TEST_SCENARIO: set(evidence.test_scenario_ids),
        }
        requested = tuple(
            sorted(
                (
                    cast(KnowledgeRelevanceEntityType, item.entity_type).value,
                    item.entity_id,
                )
                for item in links
            )
        )
        matched = tuple(
            item
            for item in requested
            if item[1] in allowed[KnowledgeRelevanceEntityType(item[0])]
        )
        missing = tuple(item for item in requested if item not in matched)
        if missing:

            def render(item: tuple[str, str]) -> str:
                return f"{item[0]}:{item[1]}"

            raise KnowledgePropagationServiceError(
                "knowledge_relevance_invalid",
                "all relevance links must belong to the linked spec",
                details={
                    "requested": [render(item) for item in requested],
                    "matched": [render(item) for item in matched],
                    "missing": [render(item) for item in missing],
                },
            )

    @classmethod
    def _parent_evidence_fingerprint(
        cls,
        evidence: KnowledgeParentEvidence,
    ) -> str:
        return cls._hash(
            {
                "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
                "evidence": evidence.to_dict(),
            }
        )

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
        return KnowledgePropagationService._validated_sources_from(
            selection,
            scope.sources,
        )

    @staticmethod
    def _validated_sources_from(
        selection: KnowledgeSelection,
        source_facts: tuple[KnowledgeSelectableSource, ...],
    ) -> tuple[KnowledgeSelectableSource, ...]:
        if selection.selection_state is not KnowledgeSelectionState.EXPLICIT_IDS:
            return ()
        requested = selection.knowledge_ids
        by_requested = {
            source.requested_knowledge_id: source for source in source_facts
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
        parent_evidence: KnowledgeParentEvidence | None = None,
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
            parent=command.parent,
            parent_evidence=parent_evidence,
            assignments_to_open=tuple(assignments_to_open),
            assignment_ids_to_close=tuple(assignment_ids_to_close),
            tombstones_to_open=tuple(tombstones_to_open),
            tombstone_ids_to_close=tuple(tombstone_ids_to_close),
            snapshots_to_open=tuple(snapshots_to_open),
            snapshot_ids_to_close=tuple(snapshot_ids_to_close),
            supersession_links=supersession_links,
            creation_result=command.creation_result,
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

    def _build_refresh_by_knowledge_ids_plan(
        self,
        command: KnowledgeRefreshByKnowledgeIdsCommand,
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
                "snapshot refresh requires an active v2 explicit-id scope",
            )
        sources_by_requested = {
            source.requested_knowledge_id: source for source in scope.sources
        }
        matched = tuple(
            knowledge_id
            for knowledge_id in command.knowledge_ids
            if knowledge_id in sources_by_requested
        )
        missing: set[str] = set(command.knowledge_ids) - set(matched)
        invalid: set[str] = set()
        ambiguous: set[str] = set()
        roots_by_requested: dict[str, str] = {}
        requested_by_root: dict[str, str] = {}
        for knowledge_id in matched:
            source = sources_by_requested[knowledge_id]
            root_id = source.revision_stamp.root_id
            prior = requested_by_root.get(root_id)
            if prior is not None and prior != knowledge_id:
                ambiguous.update((prior, knowledge_id))
            requested_by_root[root_id] = knowledge_id
            roots_by_requested[knowledge_id] = root_id
            if (
                source.source_deleted
                or source.content_bytes is None
                or hashlib.sha256(source.content_bytes).hexdigest()
                != source.revision_stamp.source_content_sha256
            ):
                invalid.add(knowledge_id)

        current_by_root: dict[
            str,
            list[TemporalKnowledgeAssignment],
        ] = {}
        for temporal in scope.assignments:
            if not temporal.temporal.is_current:
                continue
            current_by_root.setdefault(
                temporal.assignment.revision_stamp.root_id,
                [],
            ).append(temporal)
        current_snapshots_by_assignment: dict[
            str,
            list[KnowledgePropagationSnapshot],
        ] = {}
        for snapshot in scope.snapshots:
            if snapshot.temporal.is_current:
                current_snapshots_by_assignment.setdefault(
                    snapshot.assignment_id,
                    [],
                ).append(snapshot)
        current_tombstones = tuple(
            item for item in scope.tombstones if item.temporal.is_current
        )
        global_drop = any(item.root_id is None for item in current_tombstones)
        dropped_roots = {
            item.root_id for item in current_tombstones if item.root_id is not None
        }

        selected: list[
            tuple[
                str,
                TemporalKnowledgeAssignment,
                KnowledgeSelectableSource,
                KnowledgePropagationSnapshot,
            ]
        ] = []
        for knowledge_id in matched:
            root_id = roots_by_requested[knowledge_id]
            candidates = current_by_root.get(root_id, [])
            if not candidates:
                missing.add(knowledge_id)
                continue
            if len(candidates) != 1:
                ambiguous.add(knowledge_id)
                continue
            temporal = candidates[0]
            assignment = temporal.assignment
            snapshots = current_snapshots_by_assignment.get(
                assignment.assignment_id,
                [],
            )
            source = sources_by_requested[knowledge_id]
            if (
                assignment.mode is not KnowledgePropagationMode.SNAPSHOT
                or assignment.state
                not in {
                    KnowledgeAssignmentState.ACTIVE,
                    KnowledgeAssignmentState.STALE,
                }
                or len(snapshots) != 1
                or global_drop
                or root_id in dropped_roots
                or knowledge_id in invalid
            ):
                invalid.add(knowledge_id)
                continue
            selected.append(
                (
                    knowledge_id,
                    temporal,
                    source,
                    snapshots[0],
                )
            )

        if (
            missing
            or invalid
            or ambiguous
            or len(selected) != len(command.knowledge_ids)
        ):
            raise KnowledgePropagationServiceError(
                "knowledge_assignment_not_refreshable",
                "each requested Knowledge Base id must resolve to exactly one "
                "current snapshot assignment on this target",
                details={
                    "requested": list(command.knowledge_ids),
                    "matched": sorted(knowledge_id for knowledge_id, *_ in selected),
                    "missing": sorted(missing),
                    "invalid": sorted(invalid),
                    "ambiguous": sorted(ambiguous),
                },
            )

        now = self._operation_time()
        next_revision = scope.scope_revision + 1
        assignments_to_open: list[TemporalKnowledgeAssignment] = []
        snapshots_to_open: list[KnowledgePropagationSnapshot] = []
        assignment_ids_to_close: list[str] = []
        snapshot_ids_to_close: list[str] = []
        supersession_links: list[KnowledgeSupersessionLink] = []
        for _, temporal, source, current_snapshot in selected:
            old = temporal.assignment
            assignment_id = self._id_factory("kbasg")
            refreshed = KnowledgeAssignment(
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
                justification=old.justification,
                relevance_links=old.relevance_links,
            )
            assignments_to_open.append(
                TemporalKnowledgeAssignment(
                    assignment=refreshed,
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
            snapshot_ids_to_close.append(current_snapshot.snapshot_id)
            supersession_links.extend(
                (
                    KnowledgeSupersessionLink(
                        record_kind=KnowledgeRecordKind.ASSIGNMENT,
                        previous_id=old.assignment_id,
                        successor_id=assignment_id,
                    ),
                    KnowledgeSupersessionLink(
                        record_kind=KnowledgeRecordKind.SNAPSHOT,
                        previous_id=current_snapshot.snapshot_id,
                        successor_id=snapshot_id,
                    ),
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
            refreshed_knowledge_ids=tuple(
                source.revision_stamp.root_id for _, _, source, _ in selected
            ),
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
        next_scope_selection_state: KnowledgeSelectionState | None,
        actor_id: str,
        idempotency_key: str,
        request_hash: str,
        occurred_at: datetime,
        next_scope_v2_active: bool = True,
        parent: KnowledgeParentKey | None = None,
        parent_evidence: KnowledgeParentEvidence | None = None,
        outcome: KnowledgeMutationOutcome = KnowledgeMutationOutcome.APPLIED,
        receipt_details: Mapping[str, object] | None = None,
        assignments_to_open: tuple[TemporalKnowledgeAssignment, ...] = (),
        assignment_ids_to_close: tuple[str, ...] = (),
        tombstones_to_open: tuple[KnowledgePropagationTombstone, ...] = (),
        tombstone_ids_to_close: tuple[str, ...] = (),
        snapshots_to_open: tuple[KnowledgePropagationSnapshot, ...] = (),
        snapshot_ids_to_close: tuple[str, ...] = (),
        supersession_links: tuple[KnowledgeSupersessionLink, ...] = (),
        refreshed_knowledge_ids: tuple[str, ...] = (),
        creation_result: Mapping[str, object] | None = None,
    ) -> KnowledgeMutationPlan:
        operation_id = self._id_factory("kbop")
        result_v2 = KnowledgeMutationResultV2Projector.project(
            operation_id=operation_id,
            target=target,
            operation_kind=kind,
            previous_revision=expected_revision,
            selection_state=next_scope_selection_state,
            assignments=assignments_to_open,
            refreshed_knowledge_ids=refreshed_knowledge_ids,
            creation_result=creation_result,
        )
        details = dict(receipt_details or {})
        details["result_v2"] = result_v2.to_dict()
        receipt = KnowledgeMutationReceipt(
            operation_id=operation_id,
            target=target,
            operation_kind=kind,
            previous_revision=expected_revision,
            revision=expected_revision + 1,
            request_hash=request_hash,
            applied_at=occurred_at,
            outcome=outcome,
            details=details,
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
            next_scope_v2_active=next_scope_v2_active,
            parent=parent,
            parent_evidence=parent_evidence,
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
            late_replay = (
                receipt.replayed
                and receipt.target == plan.target
                and receipt.operation_kind is plan.operation_kind
                and receipt.request_hash == plan.request_hash
                and receipt.original_outcome
                in {
                    KnowledgeMutationOutcome.APPLIED,
                    KnowledgeMutationOutcome.GRANDFATHERED,
                    KnowledgeMutationOutcome.NOOP,
                }
            )
            if not late_replay:
                raise KnowledgePropagationServiceError(
                    "knowledge_propagation_stage_receipt_mismatch",
                    (
                        "persistence returned a receipt that differs from "
                        "the staged ledger"
                    ),
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
                "semantic_creation_hash": command.semantic_creation_hash,
                "parent": (
                    None if command.parent is None else command.parent.to_dict()
                ),
                "creation_result": _thaw_json(command.creation_result),
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

    def _grandfather_request_hash(
        self,
        command: KnowledgeGrandfatherCommand,
    ) -> str:
        return self._hash(
            {
                "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
                "operation": "grandfather",
                "target": command.target.to_dict(),
                "attachments": [item.to_dict() for item in command.attachments],
                "actor_id": command.actor_id,
                "expected_revision": command.expected_revision,
            }
        )

    def _refresh_by_knowledge_ids_request_hash(
        self,
        command: KnowledgeRefreshByKnowledgeIdsCommand,
    ) -> str:
        return self._hash(
            {
                "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
                "operation": "refresh_snapshot",
                "target": command.target.to_dict(),
                "knowledge_ids": list(command.knowledge_ids),
                "actor_id": command.actor_id,
                "expected_revision": command.expected_revision,
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
    "KnowledgeCreationPreflightCommand",
    "KnowledgeGrandfatherAttachment",
    "KnowledgeGrandfatherCommand",
    "KnowledgeGrandfatherEvidence",
    "KnowledgeMutationCommand",
    "KnowledgeMutationPreparation",
    "KnowledgeMutationResultV2",
    "KnowledgeMutationResultV2Projector",
    "KnowledgePropagationReadResult",
    "KnowledgePropagationService",
    "KnowledgePropagationServiceError",
    "KnowledgeRefreshCommand",
    "KnowledgeRefreshByKnowledgeIdsCommand",
    "ResolvedKnowledgeAssignment",
    "classify_legacy_origin",
    "deterministic_knowledge_target_id",
]
