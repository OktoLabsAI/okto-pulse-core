"""Public, persistence-free authority contract for historical archive sections.

Captured decisions are scoped evidence, never credentials or a replacement for
current Board access. Missing grants deny; legacy permission defaults are applied
only by the canonical policy while capturing the source, not during archive reads.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Literal, Protocol, runtime_checkable

from okto_pulse.core.domain.permissions import ALL_FLAGS
from okto_pulse.core.ports.permission_policy import (
    DefaultPermissionPolicy,
    PermissionContext,
    PermissionPolicyPort,
    PermissionSet,
)


class ArchiveSection(str, Enum):
    CONTENT = "content"
    QA = "qa"
    EVALUATIONS = "evaluations"
    HISTORY = "history"


@dataclass(frozen=True, slots=True)
class ArchiveReadSections:
    content: bool
    qa: bool
    evaluations: bool
    history: bool

    def __post_init__(self) -> None:
        if any(type(getattr(self, section.value)) is not bool for section in ArchiveSection):
            raise ValueError("archive_section_decision_invalid")

    def allows(self, section: ArchiveSection) -> bool:
        if not isinstance(section, ArchiveSection):
            raise ValueError("archive_section_invalid")
        return self.content and getattr(self, section.value)


@dataclass(frozen=True, slots=True)
class ArchiveSourceScope:
    realm_id: str
    board_id: str
    origin_kind: str
    origin_id: str

    def __post_init__(self) -> None:
        for value in (self.realm_id, self.board_id, self.origin_kind, self.origin_id):
            if type(value) is not str or not value.strip() or len(value) > 255:
                raise ValueError("archive_source_scope_invalid")


@dataclass(frozen=True, slots=True)
class ArchiveReadGrant:
    scope: ArchiveSourceScope
    actor_kind: Literal["human", "agent"]
    actor_id: str
    sections: ArchiveReadSections

    def __post_init__(self) -> None:
        if (not isinstance(self.scope, ArchiveSourceScope)
                or not isinstance(self.sections, ArchiveReadSections)
                or self.actor_kind not in ("human", "agent")
                or type(self.actor_id) is not str or not self.actor_id.strip()
                or len(self.actor_id) > 255):
            raise ValueError("archive_read_grant_invalid")


def parse_archive_read_grant(value: object) -> ArchiveReadGrant:
    """Decode only the closed grant record; extra fields never confer authority."""
    if not isinstance(value, dict) or set(value) != {"scope", "actor_kind", "actor_id", "sections"}:
        raise ValueError("archive_read_grant_invalid")
    scope, sections = value["scope"], value["sections"]
    if (not isinstance(scope, dict) or set(scope) != {"realm_id", "board_id", "origin_kind", "origin_id"}
            or not isinstance(sections, dict) or set(sections) != {s.value for s in ArchiveSection}):
        raise ValueError("archive_read_grant_invalid")
    return ArchiveReadGrant(ArchiveSourceScope(**scope), value["actor_kind"], value["actor_id"],
        ArchiveReadSections(**sections))


def capture_archive_read_sections(
    permissions: PermissionSet,
    *,
    content_permission: str,
    qa_permission: str,
    evaluations_permission: str,
    history_permission: str,
    policy: PermissionPolicyPort | None = None,
) -> ArchiveReadSections:
    """Materialize four effective decisions through the source's existing policy.

    The migration supplies registered source authorities and an already-resolved
    permission set (including review status and Board overrides). A missing legacy
    leaf may mean True; do not substitute raw-key lookup or new registry defaults.
    """
    if not isinstance(permissions, PermissionSet):
        raise TypeError("archive_resolved_permissions_required")
    authorities = (content_permission, qa_permission, evaluations_permission, history_permission)
    if any(authority not in ALL_FLAGS for authority in authorities):
        raise ValueError("archive_source_authority_unknown")
    evaluator = policy or DefaultPermissionPolicy()
    decisions = tuple(evaluator.evaluate(PermissionContext(
        operation=authority, permissions=permissions,
    )).allowed for authority in authorities)
    root = decisions[0]
    return ArchiveReadSections(*(root and decision for decision in decisions))


def archive_section_is_readable(
    grant: ArchiveReadGrant | None,
    *,
    scope: ArchiveSourceScope,
    actor_kind: Literal["human", "agent"],
    actor_id: str,
    section: ArchiveSection,
    current_board_access: bool,
    current_section_permission: bool,
) -> bool:
    """Intersect an exact source grant with current authenticated authority.

    Adapters must resolve current access in the same read snapshot before loading
    content. Neither a historical grant nor Board access alone discloses a section.
    An explicit current revocation wins; no owner/role wildcard is introduced here.
    """
    return (
        isinstance(grant, ArchiveReadGrant)
        and current_board_access is True
        and current_section_permission is True
        and grant.scope == scope
        and grant.actor_kind == actor_kind
        and grant.actor_id == actor_id
        and grant.sections.allows(section)
    )


@dataclass(frozen=True, slots=True)
class ArchiveGrantState:
    """Current scoped authority, bounded above by immutable captured evidence.

    A migration replay must preserve this revision and its revocations. Neither
    this state nor the historical ceiling replaces current identity/Board access.
    """

    captured: ArchiveReadGrant
    sections: ArchiveReadSections
    archive_id: str
    archive_sha256: str
    revision: int

    def __post_init__(self) -> None:
        if (not isinstance(self.captured, ArchiveReadGrant)
                or not isinstance(self.sections, ArchiveReadSections)
                or type(self.archive_id) is not str or not self.archive_id.strip()
                or len(self.archive_id) > 255
                or type(self.archive_sha256) is not str or len(self.archive_sha256) != 64
                or any(c not in "0123456789abcdef" for c in self.archive_sha256)
                or type(self.revision) is not int or self.revision < 1):
            raise ValueError("archive_grant_state_invalid")
        if any(getattr(self.sections, section.value)
                and not getattr(self.captured.sections, section.value) for section in ArchiveSection):
            raise ValueError("archive_grant_exceeds_captured_authority")

    def effective_grant(self) -> ArchiveReadGrant:
        return ArchiveReadGrant(self.captured.scope, self.captured.actor_kind,
            self.captured.actor_id, self.sections)


class ArchiveGrantConflict(RuntimeError):
    """The expected authority revision is no longer current; retry after a read."""


def revoke_archive_sections(
    state: ArchiveGrantState, sections: tuple[ArchiveSection, ...],
) -> ArchiveReadSections:
    """Narrow an existing grant; this operation never re-enables an authority."""
    if (not isinstance(state, ArchiveGrantState) or type(sections) is not tuple
            or not sections or any(not isinstance(s, ArchiveSection) for s in sections)
            or len(set(sections)) != len(sections)):
        raise ValueError("archive_revocation_invalid")
    return ArchiveReadSections(*(getattr(state.sections, s.value) and s not in sections
        for s in ArchiveSection))


@runtime_checkable
class HistoricalArchiveGrantPort(Protocol):
    """Persistence seam, not an authorization endpoint.

    The application owns authentication, current Board/admin permission checks
    and the transaction. Reads share that snapshot; revocations use compare and
    swap and append their audit record atomically. No method grants new access,
    commits the caller's transaction, or returns archive bytes/storage paths.
    """

    async def get(
        self, *, scope: ArchiveSourceScope, actor_kind: Literal["human", "agent"], actor_id: str,
    ) -> ArchiveGrantState | None: ...

    async def revoke(
        self, *, scope: ArchiveSourceScope, actor_kind: Literal["human", "agent"], actor_id: str,
        expected_revision: int, sections: tuple[ArchiveSection, ...],
        performed_by_kind: Literal["human", "agent"], performed_by_id: str,
    ) -> ArchiveGrantState: ...
