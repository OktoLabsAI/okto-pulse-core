"""Pure bundle preparation for the Refinement research-decision ledger."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable
from uuid import uuid4

from okto_pulse.core.domain.enums import RefinementStatus
from okto_pulse.core.domain.research_decision_ledger import (
    RefinementLedgerContext,
    RefinementVersionBump,
    ResearchDecisionAction,
    ResearchDecisionContent,
    ResearchDecisionCursor,
    ResearchDecisionDerivation,
    ResearchDecisionDerivationRef,
    ResearchDecisionEntry,
    ResearchDecisionEventIntent,
    ResearchDecisionEventType,
    ResearchDecisionFrozenHead,
    ResearchDecisionHead,
    ResearchDecisionHistoryIntent,
    ResearchDecisionLedgerSnapshot,
    ResearchDecisionOutboxIntent,
    ResearchDecisionPage,
    ResearchDecisionStatus,
    ResearchDecisionWriteBundle,
    research_decision_content_digest,
)
from okto_pulse.core.ports.research_decision_ledger import (
    ResearchDecisionListQuery,
)


class ResearchDecisionLedgerError(RuntimeError):
    """Structured transport-neutral service failure."""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        retryable: bool = False,
    ) -> None:
        self.code = code
        self.message = message or code
        self.retryable = retryable
        super().__init__(self.message)


class ResearchDecisionValidationError(ResearchDecisionLedgerError):
    pass


class ResearchDecisionConflictError(ResearchDecisionLedgerError):
    pass


@dataclass(frozen=True, slots=True)
class AppendResearchDecisionCommand:
    board_id: str
    refinement_id: str
    expected_refinement_version: int
    content: ResearchDecisionContent
    actor_id: str
    expected_head_revision: int = 0
    idempotency_key: str | None = None
    actor_type: str = "agent"


@dataclass(frozen=True, slots=True)
class SupersedeResearchDecisionCommand:
    board_id: str
    refinement_id: str
    ledger_id: str
    predecessor_entry_id: str
    expected_refinement_version: int
    expected_head_revision: int
    content: ResearchDecisionContent
    actor_id: str
    idempotency_key: str | None = None
    actor_type: str = "agent"


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchDecisionValidationError(code)
    return value.strip()


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ResearchDecisionValidationError(code)
    return value


class ResearchDecisionLedgerService:
    """Prepare immutable atomic bundles without touching persistence."""

    def __init__(
        self,
        *,
        id_factory: Callable[[str], str] = _default_id_factory,
        clock: Callable[[], datetime] = _default_clock,
    ) -> None:
        self._id_factory = id_factory
        self._clock = clock

    def prepare_append(
        self,
        command: AppendResearchDecisionCommand,
        *,
        context: RefinementLedgerContext,
    ) -> ResearchDecisionWriteBundle:
        if not isinstance(command, AppendResearchDecisionCommand):
            raise ResearchDecisionValidationError(
                "research_decision_append_command_invalid"
            )
        self._validate_write_context(
            context=context,
            board_id=command.board_id,
            refinement_id=command.refinement_id,
            expected_refinement_version=command.expected_refinement_version,
        )
        if (
            not isinstance(command.expected_head_revision, int)
            or isinstance(command.expected_head_revision, bool)
            or command.expected_head_revision != 0
        ):
            raise ResearchDecisionValidationError(
                "research_decision_append_requires_empty_head"
            )
        self._validate_content(command.content)
        actor_id = _required_text(
            command.actor_id,
            "research_decision_actor_id_required",
        )
        ledger_id = self._new_id("rdl")
        return self._build_bundle(
            context=context,
            content=command.content,
            actor_id=actor_id,
            actor_type=command.actor_type,
            ledger_id=ledger_id,
            expected_head_revision=0,
            predecessor_entry_id=None,
            idempotency_key=command.idempotency_key,
        )

    def prepare_supersede(
        self,
        command: SupersedeResearchDecisionCommand,
        *,
        context: RefinementLedgerContext,
        current_head: ResearchDecisionHead,
        predecessor: ResearchDecisionEntry,
    ) -> ResearchDecisionWriteBundle:
        if not isinstance(command, SupersedeResearchDecisionCommand):
            raise ResearchDecisionValidationError(
                "research_decision_supersede_command_invalid"
            )
        self._validate_write_context(
            context=context,
            board_id=command.board_id,
            refinement_id=command.refinement_id,
            expected_refinement_version=command.expected_refinement_version,
        )
        self._validate_content(command.content)
        ledger_id = _required_text(
            command.ledger_id,
            "research_decision_ledger_id_required",
        )
        predecessor_entry_id = _required_text(
            command.predecessor_entry_id,
            "research_decision_predecessor_entry_id_required",
        )
        expected_head_revision = _positive_int(
            command.expected_head_revision,
            "research_decision_head_revision_invalid",
        )
        actor_id = _required_text(
            command.actor_id,
            "research_decision_actor_id_required",
        )
        if not isinstance(current_head, ResearchDecisionHead):
            raise ResearchDecisionValidationError(
                "research_decision_current_head_invalid"
            )
        if not isinstance(predecessor, ResearchDecisionEntry):
            raise ResearchDecisionValidationError(
                "research_decision_predecessor_invalid"
            )
        expected_scope = (context.board_id, context.refinement_id, ledger_id)
        if (
            current_head.board_id,
            current_head.refinement_id,
            current_head.ledger_id,
        ) != expected_scope or (
            predecessor.board_id,
            predecessor.refinement_id,
            predecessor.ledger_id,
        ) != expected_scope:
            raise ResearchDecisionValidationError("research_decision_scope_mismatch")
        if current_head.revision != expected_head_revision:
            raise ResearchDecisionConflictError(
                "research_decision_head_revision_conflict",
                retryable=True,
            )
        if (
            current_head.current_entry_id != predecessor_entry_id
            or predecessor.id != predecessor_entry_id
        ):
            raise ResearchDecisionConflictError(
                "research_decision_head_entry_conflict",
                retryable=True,
            )
        if current_head.refinement_version > context.version:
            raise ResearchDecisionConflictError(
                "research_decision_head_version_conflict",
                retryable=True,
            )
        return self._build_bundle(
            context=context,
            content=command.content,
            actor_id=actor_id,
            actor_type=command.actor_type,
            ledger_id=ledger_id,
            expected_head_revision=expected_head_revision,
            predecessor_entry_id=predecessor_entry_id,
            idempotency_key=command.idempotency_key,
        )

    def freeze_heads(
        self,
        *,
        context: RefinementLedgerContext,
        current: Iterable[
            tuple[ResearchDecisionEntry, ResearchDecisionHead]
        ],
    ) -> ResearchDecisionLedgerSnapshot:
        if not isinstance(context, RefinementLedgerContext):
            raise ResearchDecisionValidationError(
                "research_decision_refinement_context_invalid"
            )
        frozen: list[ResearchDecisionFrozenHead] = []
        seen_ledgers: set[str] = set()
        seen_entries: set[str] = set()
        for pair in current:
            if (
                not isinstance(pair, tuple | list)
                or len(pair) != 2
                or not isinstance(pair[0], ResearchDecisionEntry)
                or not isinstance(pair[1], ResearchDecisionHead)
            ):
                raise ResearchDecisionValidationError("research_decision_head_invalid")
            entry, head = pair
            if (
                head.board_id != context.board_id
                or head.refinement_id != context.refinement_id
                or entry.board_id != context.board_id
                or entry.refinement_id != context.refinement_id
                or entry.id != head.current_entry_id
                or entry.ledger_id != head.ledger_id
                or entry.status is not head.status
            ):
                raise ResearchDecisionValidationError(
                    "research_decision_scope_mismatch"
                )
            if head.refinement_version > context.version:
                raise ResearchDecisionValidationError(
                    "research_decision_snapshot_future_head"
                )
            if head.ledger_id in seen_ledgers or head.current_entry_id in seen_entries:
                raise ResearchDecisionValidationError(
                    "research_decision_snapshot_duplicate_head"
                )
            seen_ledgers.add(head.ledger_id)
            seen_entries.add(head.current_entry_id)
            frozen.append(
                ResearchDecisionFrozenHead(
                    ledger_id=head.ledger_id,
                    entry_id=head.current_entry_id,
                    head_revision=head.revision,
                    head_refinement_version=head.refinement_version,
                    status=head.status,
                    content_digest=research_decision_content_digest(
                        entry.content
                    ),
                )
            )
        return ResearchDecisionLedgerSnapshot(
            id=self._new_id("rdls"),
            board_id=context.board_id,
            refinement_id=context.refinement_id,
            refinement_version=context.version,
            heads=tuple(frozen),
            created_at=self._now(),
        )

    def derive_resolved_references(
        self,
        *,
        snapshot: ResearchDecisionLedgerSnapshot,
        board_id: str,
        spec_id: str,
        spec_version: int,
    ) -> ResearchDecisionDerivation:
        if not isinstance(snapshot, ResearchDecisionLedgerSnapshot):
            raise ResearchDecisionValidationError("research_decision_snapshot_invalid")
        normalized_board_id = _required_text(
            board_id,
            "research_decision_board_id_required",
        )
        if normalized_board_id != snapshot.board_id:
            raise ResearchDecisionValidationError("research_decision_scope_mismatch")
        normalized_spec_id = _required_text(
            spec_id,
            "research_decision_derivation_spec_id_required",
        )
        normalized_spec_version = _positive_int(
            spec_version,
            "research_decision_derivation_spec_version_invalid",
        )
        references = tuple(
            ResearchDecisionDerivationRef(
                source_snapshot_id=snapshot.id,
                source_refinement_id=snapshot.refinement_id,
                source_refinement_version=snapshot.refinement_version,
                ledger_id=head.ledger_id,
                entry_id=head.entry_id,
                head_revision=head.head_revision,
                content_digest=head.content_digest,
            )
            for head in snapshot.heads
            if head.status is ResearchDecisionStatus.RESOLVED
        )
        return ResearchDecisionDerivation(
            board_id=normalized_board_id,
            spec_id=normalized_spec_id,
            spec_version=normalized_spec_version,
            source_refinement_id=snapshot.refinement_id,
            source_refinement_version=snapshot.refinement_version,
            source_snapshot_id=snapshot.id,
            references=references,
        )

    def paginate_entries(
        self,
        entries: Iterable[ResearchDecisionEntry],
        query: ResearchDecisionListQuery,
    ) -> ResearchDecisionPage[ResearchDecisionEntry]:
        """Reference keyset implementation for adapters and contract tests."""

        if not isinstance(query, ResearchDecisionListQuery):
            raise ResearchDecisionValidationError(
                "research_decision_list_query_invalid"
            )
        selected: list[ResearchDecisionEntry] = []
        for entry in entries:
            if not isinstance(entry, ResearchDecisionEntry):
                raise ResearchDecisionValidationError("research_decision_entry_invalid")
            if (
                entry.board_id != query.board_id
                or entry.refinement_id != query.refinement_id
            ):
                continue
            if query.ledger_id is not None and entry.ledger_id != query.ledger_id:
                continue
            if query.status is not None and entry.status is not query.status:
                continue
            if query.cursor is not None and not self._is_after_cursor(
                entry,
                query.cursor,
            ):
                continue
            selected.append(entry)
        selected.sort(
            key=lambda entry: (entry.created_at, entry.id),
            reverse=True,
        )
        has_more = len(selected) > query.limit
        page_items = tuple(selected[: query.limit])
        next_cursor = (
            ResearchDecisionCursor(
                created_at=page_items[-1].created_at,
                entry_id=page_items[-1].id,
            )
            if has_more and page_items
            else None
        )
        return ResearchDecisionPage(
            items=page_items,
            limit=query.limit,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    def _build_bundle(
        self,
        *,
        context: RefinementLedgerContext,
        content: ResearchDecisionContent,
        actor_id: str,
        actor_type: str,
        ledger_id: str,
        expected_head_revision: int,
        predecessor_entry_id: str | None,
        idempotency_key: str | None,
    ) -> ResearchDecisionWriteBundle:
        occurred_at = self._now()
        resulting_version = context.version + 1
        normalized_idempotency_key = (
            _required_text(
                idempotency_key,
                "research_decision_idempotency_key_required",
            )
            if idempotency_key is not None
            else None
        )
        request_digest = (
            self.request_digest(
                board_id=context.board_id,
                refinement_id=context.refinement_id,
                expected_refinement_version=context.version,
                content=content,
                actor_id=actor_id,
                ledger_id=ledger_id if predecessor_entry_id is not None else None,
                expected_head_revision=expected_head_revision,
                predecessor_entry_id=predecessor_entry_id,
            )
            if normalized_idempotency_key is not None
            else None
        )
        entry = ResearchDecisionEntry(
            id=self._new_id("rdle"),
            ledger_id=ledger_id,
            board_id=context.board_id,
            refinement_id=context.refinement_id,
            refinement_version=resulting_version,
            predecessor_entry_id=predecessor_entry_id,
            content=content,
            created_by=actor_id,
            created_at=occurred_at,
        )
        next_head = ResearchDecisionHead(
            ledger_id=ledger_id,
            board_id=context.board_id,
            refinement_id=context.refinement_id,
            current_entry_id=entry.id,
            revision=expected_head_revision + 1,
            refinement_version=resulting_version,
            status=entry.status,
            updated_by=actor_id,
            updated_at=occurred_at,
        )
        action = (
            ResearchDecisionAction.APPEND
            if predecessor_entry_id is None
            else ResearchDecisionAction.SUPERSEDE
        )
        event_type = (
            ResearchDecisionEventType.APPENDED
            if predecessor_entry_id is None
            else ResearchDecisionEventType.SUPERSEDED
        )
        event = ResearchDecisionEventIntent(
            id=self._new_id("evt"),
            event_type=event_type,
            board_id=context.board_id,
            refinement_id=context.refinement_id,
            refinement_version=resulting_version,
            ledger_id=ledger_id,
            entry_id=entry.id,
            actor_id=actor_id,
            actor_type=actor_type,
            occurred_at=occurred_at,
        )
        return ResearchDecisionWriteBundle(
            expected_head_revision=expected_head_revision,
            expected_head_entry_id=predecessor_entry_id,
            expected_refinement_status=RefinementStatus.DRAFT,
            expected_refinement_archived=False,
            version_bump=RefinementVersionBump(
                board_id=context.board_id,
                refinement_id=context.refinement_id,
                expected_version=context.version,
                resulting_version=resulting_version,
            ),
            entry=entry,
            next_head=next_head,
            history=ResearchDecisionHistoryIntent(
                id=self._new_id("rdlh"),
                action=action,
                board_id=context.board_id,
                refinement_id=context.refinement_id,
                ledger_id=ledger_id,
                entry_id=entry.id,
                predecessor_entry_id=predecessor_entry_id,
                from_refinement_version=context.version,
                to_refinement_version=resulting_version,
                actor_id=actor_id,
                created_at=occurred_at,
            ),
            event=event,
            outbox=ResearchDecisionOutboxIntent(
                id=self._new_id("out"),
                event_id=event.id,
                event_type=event_type,
                partition_key=context.board_id,
                available_at=occurred_at,
            ),
            idempotency_key=normalized_idempotency_key,
            request_digest=request_digest,
        )

    @staticmethod
    def request_digest(
        *,
        board_id: str,
        refinement_id: str,
        expected_refinement_version: int | None,
        content: ResearchDecisionContent,
        actor_id: str,
        ledger_id: str | None,
        expected_head_revision: int,
        predecessor_entry_id: str | None,
    ) -> str:
        payload = {
            "actor_id": actor_id,
            "board_id": board_id,
            "content": {
                "alternatives": list(content.alternatives),
                "anchor": {
                    "anchor_ref": content.anchor.anchor_ref,
                    "anchor_type": content.anchor.anchor_type.value,
                },
                "confidence": content.confidence,
                "decision": content.decision,
                "evidence_absence_justification": (
                    content.evidence_absence_justification
                ),
                "evidence_refs": list(content.evidence_refs),
                "rationale": content.rationale,
                "status": content.status.value,
                "unknown": content.unknown,
            },
            "expected_head_revision": expected_head_revision,
            "expected_refinement_version": expected_refinement_version,
            "ledger_id": ledger_id,
            "predecessor_entry_id": predecessor_entry_id,
            "refinement_id": refinement_id,
        }
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def _validate_content(content: object) -> None:
        if not isinstance(content, ResearchDecisionContent):
            raise ResearchDecisionValidationError("research_decision_content_invalid")

    @staticmethod
    def _validate_write_context(
        *,
        context: RefinementLedgerContext,
        board_id: object,
        refinement_id: object,
        expected_refinement_version: object,
    ) -> None:
        if not isinstance(context, RefinementLedgerContext):
            raise ResearchDecisionValidationError(
                "research_decision_refinement_context_invalid"
            )
        normalized_board_id = _required_text(
            board_id,
            "research_decision_board_id_required",
        )
        normalized_refinement_id = _required_text(
            refinement_id,
            "research_decision_refinement_id_required",
        )
        expected_version = _positive_int(
            expected_refinement_version,
            "research_decision_refinement_version_invalid",
        )
        if (
            normalized_board_id != context.board_id
            or normalized_refinement_id != context.refinement_id
        ):
            raise ResearchDecisionValidationError("research_decision_scope_mismatch")
        if context.archived:
            raise ResearchDecisionConflictError("research_decision_refinement_archived")
        if context.status is not RefinementStatus.DRAFT:
            raise ResearchDecisionConflictError(
                "research_decision_refinement_not_draft"
            )
        if context.version != expected_version:
            raise ResearchDecisionConflictError(
                "research_decision_refinement_version_conflict",
                retryable=True,
            )

    def _new_id(self, prefix: str) -> str:
        return _required_text(
            self._id_factory(prefix),
            "research_decision_generated_id_invalid",
        )

    def _now(self) -> datetime:
        value = self._clock()
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ResearchDecisionValidationError("research_decision_clock_invalid")
        return value.astimezone(timezone.utc)

    @staticmethod
    def _is_after_cursor(
        entry: ResearchDecisionEntry,
        cursor: ResearchDecisionCursor,
    ) -> bool:
        return (entry.created_at, entry.id) < (
            cursor.created_at,
            cursor.entry_id,
        )


__all__ = [
    "AppendResearchDecisionCommand",
    "ResearchDecisionConflictError",
    "ResearchDecisionLedgerError",
    "ResearchDecisionLedgerService",
    "ResearchDecisionValidationError",
    "SupersedeResearchDecisionCommand",
]
