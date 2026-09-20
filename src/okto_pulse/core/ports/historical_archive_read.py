"""Closed, scoped historical reads; no storage handle or relational row escapes."""

from dataclasses import dataclass
import json
from typing import Literal, Protocol, runtime_checkable

from okto_pulse.core.ports.historical_archive import ArchiveGrantState, ArchiveSection, ArchiveSourceScope


@dataclass(frozen=True, slots=True)
class ArchiveBoardScope:
    realm_id: str
    board_id: str

    def __post_init__(self):
        if any(type(value) is not str or not value.strip() or len(value) > 255
               for value in (self.realm_id, self.board_id)):
            raise ValueError("historical_archive_board_scope_invalid")


@dataclass(frozen=True, slots=True)
class ArchiveDiscoveryRequest:
    scope: ArchiveBoardScope
    offset: int = 0
    limit: int = 50

    def __post_init__(self):
        if (not isinstance(self.scope, ArchiveBoardScope)
                or type(self.offset) is not int or not 0 <= self.offset <= 100_000
                or type(self.limit) is not int or not 1 <= self.limit <= 200):
            raise ValueError("historical_archive_discovery_request_invalid")


@dataclass(frozen=True, slots=True)
class ArchiveDiscoveryItem:
    """Installed authority metadata, not a claim that source storage is available."""

    scope: ArchiveSourceScope
    archive_id: str
    sections: tuple[ArchiveSection, ...]


@dataclass(frozen=True, slots=True)
class ArchiveDiscoveryPage:
    request: ArchiveDiscoveryRequest
    items: tuple[ArchiveDiscoveryItem, ...]
    next_offset: int | None


@dataclass(frozen=True, slots=True)
class ArchiveReadRequest:
    scope: ArchiveSourceScope
    section: ArchiveSection
    offset: int = 0
    limit: int = 100

    def __post_init__(self):
        if (not isinstance(self.scope, ArchiveSourceScope) or not isinstance(self.section, ArchiveSection)
                or type(self.offset) is not int or not 0 <= self.offset <= 100_000
                or type(self.limit) is not int or not 1 <= self.limit <= 200):
            raise ValueError("historical_archive_read_request_invalid")


class ArchiveReadUnavailable(RuntimeError):
    """A committed archive could not be safely verified/projected; no partial data."""


class ArchiveReadLimitExceeded(RuntimeError):
    """An authorized page exceeds its explicit aggregate output limit."""


@dataclass(frozen=True, slots=True)
class ArchiveSectionPage:
    """Detached immutable JSON records, selected by an edition's closed projection.

    Keeping each record encoded preserves original Unicode, dates and whitespace
    without leaking mutable nested structures out of the consistent read. These
    are projected records, never the source archive or a generic SQL dump.
    """

    request: ArchiveReadRequest
    archive_id: str
    records_json: tuple[str, ...]
    next_offset: int | None

    def __post_init__(self):
        if (not isinstance(self.request, ArchiveReadRequest) or type(self.archive_id) is not str
                or not self.archive_id.strip() or len(self.archive_id) > 255
                or type(self.records_json) is not tuple or len(self.records_json) > self.request.limit
                or any(type(record) is not str for record in self.records_json)
                or (self.next_offset is not None and (type(self.next_offset) is not int
                    or self.next_offset != self.request.offset + len(self.records_json)
                    or not self.records_json or self.next_offset > 100_000))):
            raise ValueError("historical_archive_page_invalid")
        if sum(len(record.encode("utf-8")) for record in self.records_json) > 25 * 1024 * 1024:
            raise ArchiveReadLimitExceeded("historical_archive_page_limit")
        for record in self.records_json:
            parsed = json.loads(record, parse_constant=lambda value: _invalid_constant(value))
            if not isinstance(parsed, dict):
                raise ValueError("historical_archive_record_invalid")

    def records(self) -> list[dict]:
        """Each call gives the renderer its own detached values."""
        return [json.loads(record) for record in self.records_json]


def _invalid_constant(value):
    raise ValueError("historical_archive_record_invalid")


@runtime_checkable
class HistoricalArchiveReadPort(Protocol):
    """Edition mechanism bound to the caller's consistent UoW snapshot.

    Current access checks identity activity/review and the physical Board ACL,
    rather than trusting a previously resolved MCP binding. Current section
    authority lives in HistoricalArchiveGrantPort, scoped to the exact origin.
    A read rechecks those grants, verifies the committed source and returns only
    the requested projection. Neither operation commits or returns storage paths.
    """

    async def has_current_board_access(
        self, *, scope: ArchiveSourceScope | ArchiveBoardScope, actor_kind: Literal["human", "agent"], actor_id: str,
    ) -> bool: ...

    async def read_section(self, *, request: ArchiveReadRequest, grant: ArchiveGrantState) -> ArchiveSectionPage: ...


@runtime_checkable
class HistoricalArchiveDiscoveryPort(Protocol):
    """Bounded authority candidates from the same snapshot, never content blobs.

    The application filters with canonical section policy before paginating.
    No source titles, hidden counts, credentials or storage paths are exposed.
    Implementations fail closed above 100,000 candidates or 64 MiB aggregate.
    """

    async def list_grants(
        self, *, scope: ArchiveBoardScope, actor_kind: Literal["human", "agent"], actor_id: str,
    ) -> tuple[ArchiveGrantState, ...]: ...
