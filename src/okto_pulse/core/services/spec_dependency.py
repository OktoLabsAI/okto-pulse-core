"""Operational Spec precedence service.

All writes run through a transaction-bound edition adapter.  The service owns
the domain policy, typed errors, optimistic fences and audit/event effects; it
never creates a transaction or commits one.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Sequence

from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.spec_dependency import (
    SPEC_DEPENDENCY_REMOVAL_REASON_MAX_LENGTH,
    SpecDependencyMutationReceipt,
    SpecDependencyOperationError,
    SpecDependencyPage,
    SpecDependencyReadiness,
    SpecDependencyRecord,
    SpecDependencySpecSnapshot,
    normalize_spec_dependency_blocker_limit,
    spec_dependency_blocked_guidance,
    spec_dependency_blocking_facts,
    spec_current_edition_started,
    spec_dependency_cycle_path,
    spec_dependency_is_satisfied,
)
from okto_pulse.core.events import publish as event_publish
from okto_pulse.core.events.types import (
    SpecDependencyAdded,
    SpecDependencyRemoved,
    SpecVersionBumped,
)
from okto_pulse.core.ports.application_persistence import (
    ApplicationRecord,
    get_application_persistence_port,
)
from okto_pulse.core.ports.spec_dependency import SpecDependencyPersistencePort
from okto_pulse.core.services.spec_dependency_observability import (
    mark_spec_dependency_critical_section_started,
    observe_spec_dependency_gate,
)


def _request_digest(operation: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        {"operation": operation, **payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _require_expected_snapshot(
    snapshot: SpecDependencySpecSnapshot | None,
    *,
    spec_id: str,
    expected_version: int,
    expected_edition: int,
    expected_status: SpecStatus | str | None = None,
) -> SpecDependencySpecSnapshot:
    if snapshot is None:
        raise SpecDependencyOperationError(
            "dependency_target_unavailable",
            "Spec was not found in the requested board.",
            facts={"spec_id": spec_id},
        )
    if snapshot.archived:
        raise SpecDependencyOperationError(
            "spec_dependency_state_conflict",
            "Archived Specs cannot mutate operational dependencies.",
            remediation="restore_spec",
            facts={"spec_id": spec_id},
        )
    if snapshot.version != expected_version:
        raise SpecDependencyOperationError(
            "spec_dependency_version_conflict",
            "Spec changed after the dependency form was loaded.",
            remediation="refresh_spec",
            facts={
                "spec_id": spec_id,
                "expected_spec_version": expected_version,
                "current_spec_version": snapshot.version,
            },
        )
    if snapshot.edition != expected_edition:
        raise SpecDependencyOperationError(
            "spec_dependency_state_conflict",
            "Spec lifecycle edition changed after the dependency form was loaded.",
            remediation="refresh_spec",
            facts={
                "spec_id": spec_id,
                "expected_spec_edition": expected_edition,
                "current_spec_edition": snapshot.edition,
            },
        )
    if expected_status is not None and snapshot.status.value != str(
        getattr(expected_status, "value", expected_status)
    ):
        # Stateful authorization is evaluated before the graph critical
        # section. A lifecycle transition can preserve both version and
        # edition, so bind the authorization decision to the status that was
        # actually authorized and reject a different locked snapshot.
        raise SpecDependencyOperationError(
            "spec_dependency_state_conflict",
            "Spec lifecycle status changed while the dependency operation was starting.",
            facts={"spec_id": spec_id},
        )
    return snapshot


def _target_matches_authorized_snapshot(
    snapshot: SpecDependencySpecSnapshot | None,
    authorized: SpecDependencySpecSnapshot,
) -> bool:
    """Match only fields that define the authorized target lifecycle leaf."""

    if snapshot is None:
        return False
    return (
        str(snapshot.board_id) == str(authorized.board_id)
        and str(snapshot.id) == str(authorized.id)
        and int(snapshot.version) == int(authorized.version)
        and int(snapshot.edition) == int(authorized.edition)
        and snapshot.status.value == authorized.status.value
        and bool(snapshot.archived) == bool(authorized.archived)
    )


def _assert_replay_matches(
    replay: SpecDependencyMutationReceipt,
    request_digest: str,
) -> SpecDependencyMutationReceipt:
    if replay.request_digest != request_digest:
        raise SpecDependencyOperationError(
            "spec_dependency_state_conflict",
            "Idempotency key was already used with a different request.",
            remediation="use_a_new_idempotency_key",
            facts={"conflict_kind": "idempotency_key_reuse"},
        )
    return replace(replay, replayed=True)


def _require_idempotency_key(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise SpecDependencyOperationError(
            "invalid_spec_dependency_request",
            "A non-empty idempotency key is required.",
        )
    if len(normalized) > 255:
        raise SpecDependencyOperationError(
            "invalid_spec_dependency_request",
            "Idempotency key must contain at most 255 characters.",
        )
    return normalized


class SpecDependencyService:
    def __init__(
        self,
        persistence: SpecDependencyPersistencePort,
        relational_context: object,
    ) -> None:
        self.persistence = persistence
        self.relational_context = relational_context

    async def get_readiness(
        self,
        *,
        board_id: str,
        spec_id: str,
        blocker_limit: int = 100,
    ) -> SpecDependencyReadiness:
        return await self.persistence.get_readiness(
            board_id=board_id,
            spec_id=spec_id,
            blocker_limit=normalize_spec_dependency_blocker_limit(blocker_limit),
        )

    async def list_page(self, query: Any) -> SpecDependencyPage:
        return await self.persistence.list_page(query)

    async def add_dependency(
        self,
        *,
        board_id: str,
        source_spec_id: str,
        target_spec_id: str,
        expected_spec_version: int,
        expected_spec_edition: int,
        idempotency_key: str,
        actor_id: str,
        actor_type: str,
        actor_name: str | None,
        expected_spec_status: SpecStatus | str | None = None,
        authorized_target_snapshot: SpecDependencySpecSnapshot | None = None,
    ) -> SpecDependencyMutationReceipt:
        idempotency_key = _require_idempotency_key(idempotency_key)
        if source_spec_id == target_spec_id:
            raise SpecDependencyOperationError(
                "spec_dependency_self_reference",
                "A Spec cannot depend on itself.",
                facts={"spec_id": source_spec_id},
            )
        request_digest = _request_digest(
            "add",
            {
                "board_id": board_id,
                "source_spec_id": source_spec_id,
                "target_spec_id": target_spec_id,
                "expected_spec_version": expected_spec_version,
                "expected_spec_edition": expected_spec_edition,
            },
        )
        await self.persistence.acquire_board_graph_lock(board_id)
        mark_spec_dependency_critical_section_started()
        replay = await self.persistence.lookup_mutation_replay(
            board_id=board_id,
            operation="add",
            idempotency_key=idempotency_key,
            actor_id=actor_id,
            actor_type=actor_type,
        )
        if replay is not None:
            return _assert_replay_matches(replay, request_digest)

        # Lock both endpoints in a stable order. The board graph fence protects
        # dependency mutations; ordered row locks also interoperate safely with
        # unrelated Spec writers that do not acquire that fence.
        snapshots: dict[str, SpecDependencySpecSnapshot | None] = {}
        for endpoint_id in sorted({source_spec_id, target_spec_id}):
            snapshots[endpoint_id] = await self.persistence.get_spec_snapshot(
                board_id=board_id,
                spec_id=endpoint_id,
                for_update=True,
            )
        source = _require_expected_snapshot(
            snapshots[source_spec_id],
            spec_id=source_spec_id,
            expected_version=expected_spec_version,
            expected_edition=expected_spec_edition,
            expected_status=expected_spec_status,
        )
        target = snapshots[target_spec_id]
        target_authorization_changed = (
            authorized_target_snapshot is not None
            and not _target_matches_authorized_snapshot(
                target,
                authorized_target_snapshot,
            )
        )
        if target is None or target.archived or target_authorization_changed:
            # The caller already proved visibility of one concrete target
            # snapshot.  A missing, archived or changed locked row is masked as
            # unavailable so the race cannot become an existence/state oracle.
            raise SpecDependencyOperationError(
                "dependency_target_unavailable",
                "Dependency target is unavailable.",
                facts={"spec_id": source_spec_id},
            )

        existing = await self.persistence.find_active_dependency(
            board_id=board_id,
            source_spec_id=source_spec_id,
            target_spec_id=target_spec_id,
        )
        if existing is not None:
            raise SpecDependencyOperationError(
                "spec_dependency_state_conflict",
                "An active dependency between these Specs already exists.",
                remediation="use_the_existing_dependency",
                facts={
                    "conflict_kind": "active_duplicate",
                    "dependency_id": existing.id,
                    "spec_id": source_spec_id,
                    "target_spec_id": target_spec_id,
                },
            )

        current_cycle_started = spec_current_edition_started(
            source
        ) or source.status in {
            SpecStatus.IN_PROGRESS,
            SpecStatus.DONE,
            SpecStatus.CANCELLED,
        }
        target_done = spec_dependency_is_satisfied(target.status)
        if current_cycle_started and not target_done:
            raise SpecDependencyOperationError(
                "spec_dependency_state_conflict",
                "After execution starts, a new prerequisite must already be Done.",
                remediation="complete_target_spec_or_return_source_to_draft",
                facts={
                    "spec_id": source_spec_id,
                    "target_spec_id": target_spec_id,
                    "target_status": target.status.value,
                    "spec_edition": source.edition,
                },
            )

        active_edges = await self.persistence.list_active_board_edges(board_id=board_id)
        cycle_path = spec_dependency_cycle_path(
            active_edges,
            source_spec_id=source_spec_id,
            target_spec_id=target_spec_id,
        )
        if cycle_path is not None:
            raise SpecDependencyOperationError(
                "spec_dependency_cycle",
                "This dependency would create a cycle between Specs.",
                remediation="choose_a_non_descendant_prerequisite",
                facts={
                    "spec_id": source_spec_id,
                    "target_spec_id": target_spec_id,
                    "cycle_path": list(cycle_path),
                },
            )

        created_at = datetime.now(timezone.utc)
        resulting_version = expected_spec_version + 1
        dependency = SpecDependencyRecord(
            id=str(uuid.uuid4()),
            board_id=board_id,
            source_spec_id=source_spec_id,
            target_spec_id=target_spec_id,
            created_at=created_at,
            created_by=actor_id,
            created_by_type=actor_type,
            created_by_name=actor_name,
            source_version_on_create=resulting_version,
            source_status_on_create=source.status,
            target_status_on_create=target.status,
            target_version_on_create=target.version,
            resolved_on_create=target_done,
            retrospective=current_cycle_started,
            add_idempotency_key=idempotency_key,
            source_title_on_create=source.title,
            source_edition_on_create=source.edition,
            target_title_on_create=target.title,
            target_edition_on_create=target.edition,
        )
        await self.persistence.insert_dependency(
            dependency,
            request_digest=request_digest,
        )
        source_after = await self.persistence.bump_source_spec_version(
            board_id=board_id,
            spec_id=source_spec_id,
            expected_version=expected_spec_version,
            expected_edition=expected_spec_edition,
        )
        if source_after is None:
            raise SpecDependencyOperationError(
                "spec_dependency_version_conflict",
                "Spec changed while the dependency was being added.",
                remediation="refresh_spec",
            )
        receipt = SpecDependencyMutationReceipt(
            operation="add",
            dependency=dependency,
            source_spec=source_after,
            request_digest=request_digest,
            satisfied=target_done,
        )
        await self.persistence.store_mutation_receipt(
            receipt,
            idempotency_key=idempotency_key,
            actor_id=actor_id,
            actor_type=actor_type,
            actor_name=actor_name,
        )
        await self._record_effects(
            receipt,
            actor_id=actor_id,
            actor_type=actor_type,
            actor_name=actor_name,
        )
        return receipt

    async def remove_dependency(
        self,
        *,
        board_id: str,
        source_spec_id: str,
        dependency_id: str,
        reason: str,
        expected_spec_version: int,
        expected_spec_edition: int,
        idempotency_key: str,
        actor_id: str,
        actor_type: str,
        actor_name: str | None,
        expected_spec_status: SpecStatus | str | None = None,
    ) -> SpecDependencyMutationReceipt:
        idempotency_key = _require_idempotency_key(idempotency_key)
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise SpecDependencyOperationError(
                "invalid_spec_dependency_request",
                "A removal reason is required to preserve the dependency lifecycle.",
            )
        if len(normalized_reason) > SPEC_DEPENDENCY_REMOVAL_REASON_MAX_LENGTH:
            raise SpecDependencyOperationError(
                "invalid_spec_dependency_request",
                "The removal reason exceeds the supported length.",
            )
        request_digest = _request_digest(
            "remove",
            {
                "board_id": board_id,
                "source_spec_id": source_spec_id,
                "dependency_id": dependency_id,
                "reason": normalized_reason,
                "expected_spec_version": expected_spec_version,
                "expected_spec_edition": expected_spec_edition,
            },
        )
        await self.persistence.acquire_board_graph_lock(board_id)
        mark_spec_dependency_critical_section_started()
        replay = await self.persistence.lookup_mutation_replay(
            board_id=board_id,
            operation="remove",
            idempotency_key=idempotency_key,
            actor_id=actor_id,
            actor_type=actor_type,
        )
        if replay is not None:
            return _assert_replay_matches(replay, request_digest)

        source = _require_expected_snapshot(
            await self.persistence.get_spec_snapshot(
                board_id=board_id,
                spec_id=source_spec_id,
                for_update=True,
            ),
            spec_id=source_spec_id,
            expected_version=expected_spec_version,
            expected_edition=expected_spec_edition,
            expected_status=expected_spec_status,
        )
        dependency = await self.persistence.get_dependency(
            board_id=board_id,
            dependency_id=dependency_id,
        )
        if (
            dependency is None
            or not dependency.active
            or dependency.source_spec_id != source_spec_id
        ):
            raise SpecDependencyOperationError(
                "spec_dependency_not_found",
                "Active Spec dependency was not found.",
                facts={"dependency_id": dependency_id},
            )

        target = await self.persistence.get_spec_snapshot(
            board_id=board_id,
            spec_id=dependency.target_spec_id,
        )
        target_satisfied = target is not None and spec_dependency_is_satisfied(
            target.status,
            target_archived=target.archived,
        )

        removed = await self.persistence.tombstone_dependency(
            board_id=board_id,
            dependency_id=dependency_id,
            removed_at=datetime.now(timezone.utc),
            removed_by=actor_id,
            removed_by_type=actor_type,
            removed_by_name=actor_name,
            removal_reason=normalized_reason,
            source_version_on_remove=expected_spec_version + 1,
            source_title_on_remove=source.title,
            source_edition_on_remove=source.edition,
            target_title_on_remove=target.title if target is not None else None,
            target_edition_on_remove=(
                target.edition if target is not None else None
            ),
            idempotency_key=idempotency_key,
            request_digest=request_digest,
        )
        source_after = await self.persistence.bump_source_spec_version(
            board_id=board_id,
            spec_id=source_spec_id,
            expected_version=expected_spec_version,
            expected_edition=expected_spec_edition,
        )
        if source_after is None:
            raise SpecDependencyOperationError(
                "spec_dependency_version_conflict",
                "Spec changed while the dependency was being removed.",
                remediation="refresh_spec",
            )
        receipt = SpecDependencyMutationReceipt(
            operation="remove",
            dependency=removed,
            source_spec=source_after,
            request_digest=request_digest,
            satisfied=target_satisfied,
        )
        await self.persistence.store_mutation_receipt(
            receipt,
            idempotency_key=idempotency_key,
            actor_id=actor_id,
            actor_type=actor_type,
            actor_name=actor_name,
        )
        await self._record_effects(
            receipt,
            actor_id=actor_id,
            actor_type=actor_type,
            actor_name=actor_name,
        )
        return receipt

    @observe_spec_dependency_gate("execution")
    async def require_ready_for_execution(
        self,
        *,
        board_id: str,
        spec_id: str,
        mark_started: bool,
        expected_edition: int,
        acquire_graph_lock: bool = True,
        expected_status: SpecStatus | str | None = None,
        expected_archived: bool | None = None,
    ) -> SpecDependencyReadiness:
        if acquire_graph_lock:
            await self.acquire_lifecycle_write_fence(board_id=board_id)
        snapshot = await self.persistence.get_spec_snapshot(
            board_id=board_id,
            spec_id=spec_id,
            for_update=True,
        )
        if snapshot is None:
            raise SpecDependencyOperationError(
                "dependency_target_unavailable",
                "Spec was not found in the requested board.",
                facts={"spec_id": spec_id},
            )
        if snapshot.edition != expected_edition:
            raise SpecDependencyOperationError(
                "spec_dependency_state_conflict",
                "Spec lifecycle edition changed while execution was starting.",
                remediation="refresh_spec",
                facts={
                    "spec_id": spec_id,
                    "expected_spec_edition": expected_edition,
                    "current_spec_edition": snapshot.edition,
                },
            )
        if expected_status is not None and snapshot.status.value != str(
            getattr(expected_status, "value", expected_status)
        ):
            raise SpecDependencyOperationError(
                "spec_dependency_state_conflict",
                "Spec lifecycle status changed while execution was starting.",
                facts={"spec_id": spec_id},
            )
        if expected_archived is not None and snapshot.archived != expected_archived:
            raise SpecDependencyOperationError(
                "spec_dependency_state_conflict",
                "Spec archive state changed while execution was starting.",
                facts={"spec_id": spec_id},
            )
        if snapshot.archived:
            raise SpecDependencyOperationError(
                "spec_dependency_state_conflict",
                "Archived Specs cannot start execution.",
                remediation="restore_spec",
                facts={"spec_id": spec_id},
            )
        readiness = await self.persistence.get_readiness(
            board_id=board_id,
            spec_id=spec_id,
            blocker_limit=100,
        )
        if readiness.current_edition != expected_edition:
            raise SpecDependencyOperationError(
                "spec_dependency_state_conflict",
                "Spec lifecycle edition changed while execution was starting.",
                remediation="refresh_spec",
                facts={
                    "spec_id": spec_id,
                    "expected_spec_edition": expected_edition,
                    "current_spec_edition": readiness.current_edition,
                },
            )
        if not readiness.ready:
            message, remediation = spec_dependency_blocked_guidance(
                archived_blocking_count=readiness.archived_blocking_count,
                unfinished_blocking_count=readiness.unfinished_blocking_count,
            )
            raise SpecDependencyOperationError(
                "spec_dependencies_incomplete",
                message,
                remediation=remediation,
                facts=spec_dependency_blocking_facts(
                    spec_id=spec_id,
                    blockers=readiness.blockers,
                    blocking_count=readiness.blocking_count,
                    archived_blocking_count=readiness.archived_blocking_count,
                    unfinished_blocking_count=readiness.unfinished_blocking_count,
                    blockers_truncated=readiness.blockers_truncated,
                ),
            )
        if mark_started and not readiness.current_edition_started:
            updated = await self.persistence.mark_spec_edition_started(
                board_id=board_id,
                spec_id=spec_id,
                expected_edition=expected_edition,
            )
            if updated is None:
                raise SpecDependencyOperationError(
                    "spec_dependency_state_conflict",
                    "Spec lifecycle edition changed while execution was starting.",
                    remediation="refresh_spec",
                )
        return readiness

    async def acquire_lifecycle_write_fence(self, *, board_id: str) -> None:
        """Serialize Spec lifecycle writes with dependency graph mutations.

        The caller owns the transaction, so this fence remains held through the
        eventual lifecycle write and commit.
        """

        await self.persistence.acquire_board_graph_lock(board_id)

    async def require_no_incoming_active(
        self,
        *,
        board_id: str,
        target_spec_ids: Sequence[str],
        exclude_source_spec_ids: Sequence[str] = (),
        operation: str,
    ) -> None:
        detail_limit = 100
        probe_limit = detail_limit + 1
        await self.persistence.acquire_board_graph_lock(board_id)
        incoming = await self.persistence.list_incoming_active(
            board_id=board_id,
            target_spec_ids=target_spec_ids,
            exclude_source_spec_ids=exclude_source_spec_ids,
            limit=probe_limit,
        )
        if incoming:
            has_more = len(incoming) > detail_limit
            facts: dict[str, object] = {
                "operation": operation,
                # The probe row makes this count an explicit lower bound when
                # the public detail collection is truncated.  Never present a
                # bounded read as an authoritative total.
                "incoming_count_lower_bound": len(incoming),
                "incoming_dependencies_truncated": has_more,
                "incoming_has_more": has_more,
                "incoming_dependencies": [
                    {
                        "dependency_id": item.id,
                        "source_spec_id": item.source_spec_id,
                        "target_spec_id": item.target_spec_id,
                    }
                    for item in incoming[:detail_limit]
                ],
            }
            if not has_more:
                facts["incoming_count"] = len(incoming)
            raise SpecDependencyOperationError(
                "spec_dependency_state_conflict",
                f"Cannot {operation}: another active Spec depends on this target.",
                remediation="remove_incoming_dependencies",
                facts=facts,
            )

    async def _record_effects(
        self,
        receipt: SpecDependencyMutationReceipt,
        *,
        actor_id: str,
        actor_type: str,
        actor_name: str | None,
    ) -> None:
        dependency = receipt.dependency
        source = receipt.source_spec
        old_version = source.version - 1
        await event_publish(
            SpecVersionBumped(
                board_id=source.board_id,
                actor_id=actor_id,
                actor_type=actor_type,
                spec_id=source.id,
                old_version=old_version,
                new_version=source.version,
                changed_fields=["dependencies"],
            ),
            session=self.relational_context,
        )
        if receipt.operation == "add":
            await event_publish(
                SpecDependencyAdded(
                    board_id=source.board_id,
                    actor_id=actor_id,
                    actor_type=actor_type,
                    spec_id=source.id,
                    dependency_id=dependency.id,
                    target_spec_id=dependency.target_spec_id,
                    projection_owner_spec_id=source.id,
                    source_version=source.version,
                    source_status_on_create=dependency.source_status_on_create.value,
                    resolved_on_create=dependency.resolved_on_create,
                ),
                session=self.relational_context,
            )
        else:
            await event_publish(
                SpecDependencyRemoved(
                    board_id=source.board_id,
                    actor_id=actor_id,
                    actor_type=actor_type,
                    spec_id=source.id,
                    dependency_id=dependency.id,
                    target_spec_id=dependency.target_spec_id,
                    projection_owner_spec_id=source.id,
                    source_version=source.version,
                    removal_reason=dependency.removal_reason or "",
                ),
                session=self.relational_context,
            )

        persistence = get_application_persistence_port()
        resolved_name = actor_name or actor_id
        action_suffix = "added" if receipt.operation == "add" else "removed"
        action = f"spec_dependency_{action_suffix}"
        details = {
            "spec_id": source.id,
            "dependency_id": dependency.id,
            "target_spec_id": dependency.target_spec_id,
            "spec_version": source.version,
            "source_status_on_create": dependency.source_status_on_create.value,
            "resolved_on_create": dependency.resolved_on_create,
            "retrospective": dependency.retrospective,
            **(
                {"removal_reason": dependency.removal_reason}
                if receipt.operation == "remove"
                else {}
            ),
        }
        await persistence.add(
            self.relational_context,
            ApplicationRecord(
                "activity_log",
                {
                    "id": str(uuid.uuid4()),
                    "board_id": source.board_id,
                    "card_id": None,
                    "action": action,
                    "actor_type": actor_type,
                    "actor_id": actor_id,
                    "actor_name": resolved_name,
                    "details": details,
                },
            ),
        )
        await persistence.add(
            self.relational_context,
            ApplicationRecord(
                "spec_history",
                {
                    "id": str(uuid.uuid4()),
                    "spec_id": source.id,
                    "action": f"dependency_{action_suffix}",
                    "actor_type": actor_type,
                    "actor_id": actor_id,
                    "actor_name": resolved_name,
                    "changes": [
                        {
                            "field": "dependencies",
                            "old": old_version,
                            "new": source.version,
                        }
                    ],
                    "summary": f"Dependency {action_suffix}: {dependency.target_spec_id}",
                    "version": source.version,
                },
            ),
        )


__all__ = ["SpecDependencyService"]
