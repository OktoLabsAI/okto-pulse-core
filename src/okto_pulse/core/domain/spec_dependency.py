"""Pure domain contracts for operational precedence between Specs.

The relational dependency ledger is the authority.  Knowledge-graph edges and
transport projections are derived views and must never be used to admit a
mutation or lifecycle transition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Iterable, Mapping

from okto_pulse.core.domain.enums import CardStatus, SpecStatus


SPEC_DEPENDENCY_BLOCKER_DETAIL_LIMIT_MAX = 100
SPEC_DEPENDENCY_CURSOR_MAX_LENGTH = 4096
SPEC_DEPENDENCY_REMOVAL_REASON_MAX_LENGTH = 2000


class SpecDependencyDirection(str, Enum):
    OUTGOING = "outgoing"
    INCOMING = "incoming"


class SpecDependencyLifecycleFilter(str, Enum):
    ACTIVE = "active"
    REMOVED = "removed"
    ALL = "all"


class SpecDependencySatisfactionFilter(str, Enum):
    ALL = "all"
    SATISFIED = "satisfied"
    BLOCKING = "blocking"


class SpecDependencyLineageFilter(str, Enum):
    ALL = "all"
    SAME_IDEATION = "same_ideation"
    CROSS_IDEATION = "cross_ideation"


class SpecDependencyOperationError(ValueError):
    """Typed, transport-neutral dependency workflow error."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        remediation: str | None = None,
        facts: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation
        self.facts = dict(facts or {})

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            # Only a fresh read/CAS retry can resolve version conflicts. Policy,
            # lifecycle, graph and validation conflicts require a user decision.
            "retryable": self.code == "spec_dependency_version_conflict",
        }
        if self.remediation:
            payload["remediation"] = self.remediation
        if self.facts:
            payload["facts"] = dict(self.facts)
        return payload


@dataclass(frozen=True, slots=True)
class SpecDependencySpecSnapshot:
    id: str
    board_id: str
    title: str
    status: SpecStatus
    edition: int
    version: int
    archived: bool = False
    ideation_id: str | None = None
    last_started_edition: int | None = None

    @property
    def current_edition_started(self) -> bool:
        return self.last_started_edition == self.edition


@dataclass(frozen=True, slots=True)
class SpecDependencyRecord:
    id: str
    board_id: str
    source_spec_id: str
    target_spec_id: str
    created_at: datetime
    created_by: str
    source_version_on_create: int
    source_status_on_create: SpecStatus
    target_status_on_create: SpecStatus
    target_version_on_create: int
    resolved_on_create: bool
    retrospective: bool = False
    created_by_type: str = "user"
    created_by_name: str | None = None
    removed_at: datetime | None = None
    removed_by: str | None = None
    removed_by_type: str | None = None
    removed_by_name: str | None = None
    removal_reason: str | None = None
    source_version_on_remove: int | None = None
    add_idempotency_key: str | None = None
    remove_idempotency_key: str | None = None

    @property
    def active(self) -> bool:
        return self.removed_at is None


@dataclass(frozen=True, slots=True)
class SpecDependencyBlocker:
    dependency_id: str
    source_spec_id: str
    target_spec_id: str
    target_title: str
    target_status: SpecStatus
    target_edition: int
    target_version: int
    target_archived: bool = False


@dataclass(frozen=True, slots=True)
class SpecDependencyCapabilities:
    can_remove: bool
    can_navigate: bool = True
    removal_blocked_reason: str | None = None


@dataclass(frozen=True, slots=True)
class SpecDependencyListItem:
    dependency: SpecDependencyRecord
    direction: SpecDependencyDirection
    related_spec: SpecDependencySpecSnapshot
    satisfied: bool
    retrospective: bool
    same_ideation: bool
    capabilities: SpecDependencyCapabilities = field(
        default_factory=lambda: SpecDependencyCapabilities(can_remove=False)
    )


@dataclass(frozen=True, slots=True)
class SpecDependencyReadiness:
    spec_id: str
    board_id: str
    current_edition: int
    last_started_edition: int | None
    active_dependency_count: int
    blocking_count: int
    archived_blocking_count: int
    unfinished_blocking_count: int
    blockers_truncated: bool
    blockers: tuple[SpecDependencyBlocker, ...] = ()

    @property
    def ready(self) -> bool:
        return self.blocking_count == 0

    @property
    def current_edition_started(self) -> bool:
        return self.last_started_edition == self.current_edition


@dataclass(frozen=True, slots=True)
class SpecDependencyListQuery:
    board_id: str
    spec_id: str
    direction: SpecDependencyDirection
    cursor: str | None = None
    limit: int = 25
    lifecycle: SpecDependencyLifecycleFilter = SpecDependencyLifecycleFilter.ACTIVE
    satisfaction: SpecDependencySatisfactionFilter = (
        SpecDependencySatisfactionFilter.ALL
    )
    lineage: SpecDependencyLineageFilter = SpecDependencyLineageFilter.ALL
    related_statuses: tuple[SpecStatus, ...] = ()
    retrospective: bool | None = None
    # Fail closed for persistence callers.  This is a transport-neutral
    # authorization outcome populated by ListSpecDependenciesUseCase; public
    # list commands deliberately do not expose a way to set it.
    can_manage_dependencies: bool = False

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= 100:
            raise SpecDependencyOperationError(
                "invalid_spec_dependency_request",
                "Spec dependency page limit must be between 1 and 100.",
                facts={"limit": self.limit},
            )
        if self.cursor is not None and (
            not isinstance(self.cursor, str)
            or len(self.cursor) > SPEC_DEPENDENCY_CURSOR_MAX_LENGTH
        ):
            raise SpecDependencyOperationError(
                "invalid_cursor",
                "The Spec dependency cursor is invalid for this query.",
            )


@dataclass(frozen=True, slots=True)
class SpecDependencyPage:
    items: tuple[SpecDependencyListItem, ...]
    total: int
    next_cursor: str | None
    readiness: SpecDependencyReadiness

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None


@dataclass(frozen=True, slots=True)
class SpecDependencyMutationReceipt:
    operation: str
    dependency: SpecDependencyRecord
    source_spec: SpecDependencySpecSnapshot
    request_digest: str
    # Satisfaction is evaluated against the target snapshot observed by the
    # mutation, not the immutable creation-time status carried by the edge.
    satisfied: bool
    replayed: bool = False


def normalize_spec_dependency_blocker_limit(limit: int) -> int:
    """Bound readiness detail samples while preserving an explicit zero."""

    return max(0, min(int(limit), SPEC_DEPENDENCY_BLOCKER_DETAIL_LIMIT_MAX))


def spec_dependency_is_satisfied(
    target_status: SpecStatus | str,
    *,
    target_archived: bool = False,
) -> bool:
    """Only an active, non-archived Done target satisfies precedence."""

    value = getattr(target_status, "value", target_status)
    return not target_archived and str(value) == SpecStatus.DONE.value


def spec_dependency_blocking_facts(
    *,
    spec_id: str,
    blockers: Iterable[SpecDependencyBlocker | object],
    blocking_count: int,
    archived_blocking_count: int,
    unfinished_blocking_count: int,
    blockers_truncated: bool,
) -> dict[str, Any]:
    """Project exact blocker totals plus the bounded diagnostic sample."""

    values = tuple(blockers)

    def _value(item: object, name: str) -> object | None:
        if isinstance(item, Mapping):
            return item.get(name)
        return getattr(item, name, None)

    return {
        "spec_id": spec_id,
        "blocking_count": blocking_count,
        "archived_blocking_count": archived_blocking_count,
        "unfinished_blocking_count": unfinished_blocking_count,
        "blockers_truncated": blockers_truncated,
        "blocking_dependencies": [
            {
                "dependency_id": _value(item, "dependency_id"),
                "target_spec_id": _value(item, "target_spec_id"),
                "target_title": _value(item, "target_title"),
                "target_status": getattr(
                    _value(item, "target_status"),
                    "value",
                    _value(item, "target_status"),
                ),
                "target_archived": bool(_value(item, "target_archived")),
            }
            for item in values
        ],
    }


def spec_dependency_blocked_guidance(
    *,
    archived_blocking_count: int,
    unfinished_blocking_count: int,
) -> tuple[str, str]:
    """Return the shared human guidance for an execution-readiness block.

    An archived prerequisite is not actionable through ordinary lifecycle
    completion.  Surface the restore/remove path explicitly so mutation errors
    and allowed-transition previews cannot drift into misleading "make it Done"
    advice.
    """

    if archived_blocking_count > 0:
        if unfinished_blocking_count > 0:
            message = (
                "Restore archived prerequisite Specs (and complete them when "
                "needed), complete unfinished prerequisite Specs, or remove the "
                "blocking dependencies before execution can start."
            )
        else:
            message = (
                "Restore archived prerequisite Specs (and complete them when needed), "
                "or remove their dependencies before execution can start."
            )
        return (
            message,
            "restore_archived_prerequisites_or_remove_dependencies",
        )
    return (
        "All prerequisite Specs must be Done before execution can start.",
        "complete_blocking_specs",
    )


def spec_dependency_readiness_projection(
    readiness: SpecDependencyReadiness,
) -> dict[str, Any]:
    """Return the single public readiness shape used by REST, MCP and Spec."""

    return {
        "spec_id": readiness.spec_id,
        "board_id": readiness.board_id,
        "can_start": readiness.ready,
        "ready": readiness.ready,
        "reason_code": (None if readiness.ready else "spec_dependencies_incomplete"),
        "current_edition": readiness.current_edition,
        "last_started_edition": readiness.last_started_edition,
        "current_edition_started": readiness.current_edition_started,
        "active_dependency_count": readiness.active_dependency_count,
        "unmet_count": readiness.blocking_count,
        "blocking_count": readiness.blocking_count,
        "archived_blocking_count": readiness.archived_blocking_count,
        "unfinished_blocking_count": readiness.unfinished_blocking_count,
        "blockers_truncated": readiness.blockers_truncated,
        "blockers": [
            {
                "dependency_id": blocker.dependency_id,
                "dependent_spec_id": blocker.source_spec_id,
                "prerequisite_spec_id": blocker.target_spec_id,
                "target_title": blocker.target_title,
                "target_status": getattr(
                    blocker.target_status,
                    "value",
                    blocker.target_status,
                ),
                "target_edition": blocker.target_edition,
                "target_version": blocker.target_version,
                "target_archived": blocker.target_archived,
            }
            for blocker in readiness.blockers
        ],
    }


def spec_current_edition_started(spec: SpecDependencySpecSnapshot | object) -> bool:
    edition = int(getattr(spec, "edition", 1) or 1)
    marker = getattr(spec, "last_started_edition", None)
    return marker is not None and int(marker) == edition


def transition_starts_spec_execution(
    old_status: SpecStatus | str,
    new_status: SpecStatus | str,
) -> bool:
    old_value = str(getattr(old_status, "value", old_status))
    new_value = str(getattr(new_status, "value", new_status))
    # The canonical Spec lifecycle admits execution only from the validated
    # state. Keep this predicate intentionally narrower than the Card predicate:
    # legacy/admin-only jumps into ``in_progress`` must not silently acquire the
    # current-edition execution marker through a preview or unrelated writer.
    return (
        old_value == SpecStatus.VALIDATED.value
        and new_value == SpecStatus.IN_PROGRESS.value
    )


def transition_starts_card_execution(
    old_status: CardStatus | str,
    new_status: CardStatus | str,
) -> bool:
    """Return whether a Card begins or resumes executable work.

    This exact-edge predicate intentionally includes Test/Bug direct starts,
    resumes and rework.  Cancellation, pausing, validation and ordering-only
    changes never trigger the Spec precedence gate.
    """

    old_value = str(getattr(old_status, "value", old_status))
    new_value = str(getattr(new_status, "value", new_status))
    return (old_value, new_value) in {
        (CardStatus.NOT_STARTED.value, CardStatus.STARTED.value),
        (CardStatus.NOT_STARTED.value, CardStatus.IN_PROGRESS.value),
        (CardStatus.STARTED.value, CardStatus.IN_PROGRESS.value),
        (CardStatus.ON_HOLD.value, CardStatus.STARTED.value),
        (CardStatus.ON_HOLD.value, CardStatus.IN_PROGRESS.value),
        (CardStatus.VALIDATION.value, CardStatus.IN_PROGRESS.value),
        (CardStatus.DONE.value, CardStatus.IN_PROGRESS.value),
    }


def spec_dependency_would_create_cycle(
    active_edges: Iterable[tuple[str, str] | SpecDependencyRecord],
    *,
    source_spec_id: str,
    target_spec_id: str,
) -> bool:
    """Evaluate the directed active graph for a prospective source→target edge."""

    if source_spec_id == target_spec_id:
        return True
    adjacency: dict[str, set[str]] = {}
    for edge in active_edges:
        if isinstance(edge, SpecDependencyRecord):
            if not edge.active:
                continue
            source, target = edge.source_spec_id, edge.target_spec_id
        else:
            source, target = edge
        adjacency.setdefault(str(source), set()).add(str(target))
    stack = [target_spec_id]
    visited: set[str] = set()
    while stack:
        current = stack.pop()
        if current == source_spec_id:
            return True
        if current in visited:
            continue
        visited.add(current)
        stack.extend(adjacency.get(current, ()))
    return False


def spec_dependency_cycle_path(
    active_edges: Iterable[tuple[str, str] | SpecDependencyRecord],
    *,
    source_spec_id: str,
    target_spec_id: str,
) -> tuple[str, ...] | None:
    """Return the deterministic prospective cycle path, if one exists.

    The returned path begins with the proposed source, follows the proposed
    edge to target, and then the lexicographically first shortest active path
    back to source. Only Spec identifiers are disclosed.
    """

    if source_spec_id == target_spec_id:
        return (source_spec_id, source_spec_id)
    adjacency: dict[str, set[str]] = {}
    for edge in active_edges:
        if isinstance(edge, SpecDependencyRecord):
            if not edge.active:
                continue
            source, target = edge.source_spec_id, edge.target_spec_id
        else:
            source, target = edge
        adjacency.setdefault(str(source), set()).add(str(target))

    queue: list[tuple[str, tuple[str, ...]]] = [(target_spec_id, (target_spec_id,))]
    visited: set[str] = set()
    while queue:
        current, path = queue.pop(0)
        if current == source_spec_id:
            return (source_spec_id, *path)
        if current in visited:
            continue
        visited.add(current)
        for neighbor in sorted(adjacency.get(current, ())):
            if neighbor not in visited:
                queue.append((neighbor, (*path, neighbor)))
    return None


__all__ = [
    "SPEC_DEPENDENCY_BLOCKER_DETAIL_LIMIT_MAX",
    "SPEC_DEPENDENCY_CURSOR_MAX_LENGTH",
    "SPEC_DEPENDENCY_REMOVAL_REASON_MAX_LENGTH",
    "SpecDependencyBlocker",
    "SpecDependencyCapabilities",
    "SpecDependencyDirection",
    "SpecDependencyLifecycleFilter",
    "SpecDependencyLineageFilter",
    "SpecDependencyListItem",
    "SpecDependencyListQuery",
    "SpecDependencyMutationReceipt",
    "SpecDependencyOperationError",
    "SpecDependencyPage",
    "SpecDependencyReadiness",
    "SpecDependencyRecord",
    "SpecDependencySatisfactionFilter",
    "SpecDependencySpecSnapshot",
    "normalize_spec_dependency_blocker_limit",
    "spec_current_edition_started",
    "spec_dependency_blocked_guidance",
    "spec_dependency_is_satisfied",
    "spec_dependency_readiness_projection",
    "spec_dependency_cycle_path",
    "spec_dependency_blocking_facts",
    "spec_dependency_would_create_cycle",
    "transition_starts_card_execution",
    "transition_starts_spec_execution",
]
