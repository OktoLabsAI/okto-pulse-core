"""Read immutable historical content through current, origin-scoped authority."""

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.ports.historical_archive import ArchiveGrantState, ArchiveSection, HistoricalArchiveGrantPort, archive_section_is_readable
from okto_pulse.core.ports.historical_archive_read import ArchiveDiscoveryItem, ArchiveDiscoveryPage, ArchiveDiscoveryRequest, ArchiveReadLimitExceeded, ArchiveReadRequest, ArchiveReadUnavailable, ArchiveSectionPage, HistoricalArchiveDiscoveryPort, HistoricalArchiveReadPort
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


class ReadHistoricalArchiveSectionUseCase:
    async def execute(
        self, request: ArchiveReadRequest, *, actor: ActorContext, uow: PulseUnitOfWork,
    ) -> ArchiveSectionPage:
        if not isinstance(request, ArchiveReadRequest):
            raise ValueError("historical_archive_read_request_invalid")
        missing = EntityNotFoundError("historical_archive", request.scope.origin_id)
        if (actor.actor_kind not in ("human", "agent") or actor.realm_id != request.scope.realm_id
                or uow.realm_scope.realm_id != request.scope.realm_id
                or (actor.board_id is not None and actor.board_id != request.scope.board_id)):
            raise missing
        await uow.begin_consistent_read()
        grants = getattr(uow, "historical_archive_grants", None)
        reader = getattr(uow, "historical_archive_reader", None)
        if not isinstance(grants, HistoricalArchiveGrantPort) or not isinstance(reader, HistoricalArchiveReadPort):
            raise TypeError("historical_archive_adapter_missing")
        if (await reader.has_current_board_access(scope=request.scope,
                actor_kind=actor.actor_kind, actor_id=actor.actor_id)) is not True:
            raise missing
        try:
            state = await grants.get(scope=request.scope, actor_kind=actor.actor_kind, actor_id=actor.actor_id)
        except (ValueError, TypeError) as exc:
            raise ArchiveReadUnavailable("historical_archive_authority_unavailable") from exc
        if state is None or not archive_section_is_readable(state.captured, scope=request.scope,
                actor_kind=actor.actor_kind, actor_id=actor.actor_id, section=request.section,
                current_board_access=True, current_section_permission=state.sections.allows(request.section)):
            raise missing
        page = await reader.read_section(request=request, grant=state)
        if (not isinstance(page, ArchiveSectionPage) or page.request != request
                or page.archive_id != state.archive_id):
            raise TypeError("historical_archive_reader_scope_mismatch")
        return page


class DiscoverHistoricalArchivesUseCase:
    async def execute(
        self, request: ArchiveDiscoveryRequest, *, actor: ActorContext, uow: PulseUnitOfWork,
    ) -> ArchiveDiscoveryPage:
        if not isinstance(request, ArchiveDiscoveryRequest):
            raise ValueError("historical_archive_discovery_request_invalid")
        missing = EntityNotFoundError("historical_archive", request.scope.board_id)
        if (actor.actor_kind not in ("human", "agent") or actor.realm_id != request.scope.realm_id
                or uow.realm_scope.realm_id != request.scope.realm_id
                or (actor.board_id is not None and actor.board_id != request.scope.board_id)):
            raise missing
        await uow.begin_consistent_read()
        reader = getattr(uow, "historical_archive_reader", None)
        if not isinstance(reader, HistoricalArchiveReadPort) or not isinstance(reader, HistoricalArchiveDiscoveryPort):
            raise TypeError("historical_archive_discovery_adapter_missing")
        if (await reader.has_current_board_access(scope=request.scope,
                actor_kind=actor.actor_kind, actor_id=actor.actor_id)) is not True:
            raise missing
        try:
            states = await reader.list_grants(scope=request.scope, actor_kind=actor.actor_kind, actor_id=actor.actor_id)
        except (ValueError, TypeError) as exc:
            raise ArchiveReadUnavailable("historical_archive_authority_unavailable") from exc
        if type(states) is not tuple or len(states) > 100_000:
            raise ArchiveReadLimitExceeded("historical_archive_discovery_limit")
        items, seen = [], set()
        for state in states:
            if not isinstance(state, ArchiveGrantState):
                raise ArchiveReadUnavailable("historical_archive_authority_unavailable")
            captured = state.captured
            scope = captured.scope
            if (scope.realm_id != request.scope.realm_id or scope.board_id != request.scope.board_id
                    or captured.actor_kind != actor.actor_kind or captured.actor_id != actor.actor_id
                    or (scope.origin_kind, scope.origin_id) in seen):
                raise ArchiveReadUnavailable("historical_archive_authority_scope_mismatch")
            seen.add((scope.origin_kind, scope.origin_id))
            sections = tuple(section for section in ArchiveSection if archive_section_is_readable(
                captured, scope=scope, actor_kind=actor.actor_kind, actor_id=actor.actor_id,
                section=section, current_board_access=True, current_section_permission=state.sections.allows(section)))
            if ArchiveSection.CONTENT in sections:
                items.append(ArchiveDiscoveryItem(scope, state.archive_id, sections))
        items.sort(key=lambda item: (item.scope.origin_kind, item.scope.origin_id))
        selected = tuple(items[request.offset:request.offset + request.limit])
        end = request.offset + len(selected)
        return ArchiveDiscoveryPage(request, selected, end if end < len(items) else None)
