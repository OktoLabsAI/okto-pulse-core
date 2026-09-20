"""Read immutable historical content through current, origin-scoped authority."""

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.ports.historical_archive import HistoricalArchiveGrantPort, archive_section_is_readable
from okto_pulse.core.ports.historical_archive_read import ArchiveReadRequest, ArchiveReadUnavailable, ArchiveSectionPage, HistoricalArchiveReadPort
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
