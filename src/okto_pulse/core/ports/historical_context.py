"""Read-only context links: target access never replaces archived source access."""

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from okto_pulse.core.ports.context_disposition import ContextTarget
from okto_pulse.core.ports.historical_archive import ArchiveGrantState, ArchiveSection, ArchiveSourceScope
from okto_pulse.core.ports.historical_archive_read import ArchiveBoardScope, ArchiveDiscoveryRequest, ArchiveSectionPage
from okto_pulse.core.ports.permission_policy import DefaultPermissionPolicy, PermissionContext, PermissionSet


def context_target_permission_allows_read(target: ContextTarget, permissions: PermissionSet) -> bool:
    if not isinstance(target, ContextTarget) or not isinstance(permissions, PermissionSet):
        return False
    return DefaultPermissionPolicy().evaluate(PermissionContext(
        operation=f"{target.kind}.entity.read", permissions=permissions)).allowed


@dataclass(frozen=True, slots=True)
class HistoricalContextRequest:
    scope: ArchiveBoardScope
    target: ContextTarget
    offset: int = 0
    limit: int = 50

    def __post_init__(self):
        ArchiveDiscoveryRequest(self.scope, self.offset, self.limit)
        if not isinstance(self.target, ContextTarget):
            raise ValueError("historical_context_target_invalid")


@dataclass(frozen=True, slots=True)
class HistoricalContextBinding:
    """Opaque candidate metadata, verified against private evidence before output."""

    identity: str
    scope: ArchiveSourceScope
    target: ContextTarget
    archive_id: str
    archive_sha256: str
    section: ArchiveSection
    field: str | None = None

    def __post_init__(self):
        if (not isinstance(self.scope, ArchiveSourceScope) or not isinstance(self.target, ContextTarget)
                or not isinstance(self.section, ArchiveSection)
                or any(type(value) is not str or not value.strip() or len(value) > 255
                    for value in (self.identity, self.archive_id))
                or type(self.archive_sha256) is not str or len(self.archive_sha256) != 64
                or any(c not in "0123456789abcdef" for c in self.archive_sha256)
                or (self.section is ArchiveSection.CONTENT
                    and self.field not in ("description", "objective", "expected_outcome"))
                or (self.section is not ArchiveSection.CONTENT and self.field is not None)):
            raise ValueError("historical_context_binding_invalid")


@dataclass(frozen=True, slots=True)
class HistoricalContextItem:
    binding: HistoricalContextBinding
    source: ArchiveSectionPage

    def __post_init__(self):
        if (not isinstance(self.binding, HistoricalContextBinding) or not isinstance(self.source, ArchiveSectionPage)
                or self.source.request.scope != self.binding.scope
                or self.source.request.section != self.binding.section
                or self.source.archive_id != self.binding.archive_id
                or len(self.source.records_json) != 1 or self.source.next_offset is not None):
            raise ValueError("historical_context_projection_mismatch")


@dataclass(frozen=True, slots=True)
class HistoricalContextPage:
    request: HistoricalContextRequest
    items: tuple[HistoricalContextItem, ...]
    next_offset: int | None


@runtime_checkable
class HistoricalContextReadPort(Protocol):
    """Same UoW snapshot; no mutation, source recapture or new approval.

    Listing returns bounded opaque candidates only (100,000 / 64 MiB), never
    prose or storage handles. Reading rechecks current Board/section authority
    BEFORE opening either private evidence or the original archive, then verifies
    the selected binding against both. It returns one closed source projection,
    preserving attribution. Neither a link nor an old score confers authority.
    """

    async def has_current_target_access(self, *, request: HistoricalContextRequest,
        actor_kind: str, actor_id: str) -> bool: ...

    async def list_bindings(self, *, request: HistoricalContextRequest,
        actor_kind: str, actor_id: str) -> tuple[HistoricalContextBinding, ...]: ...

    async def read_binding(self, *, binding: HistoricalContextBinding,
        grant: ArchiveGrantState) -> HistoricalContextItem: ...
