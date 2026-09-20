"""Intersect current destination access with each original archived section."""

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_card
from okto_pulse.core.application.use_cases.spec_crud import _require_actor_board_spec
from okto_pulse.core.ports.historical_archive import HistoricalArchiveGrantPort, archive_section_is_readable
from okto_pulse.core.ports.historical_archive_read import ArchiveReadLimitExceeded, ArchiveReadUnavailable, HistoricalArchiveReadPort
from okto_pulse.core.ports.historical_context import HistoricalContextBinding, HistoricalContextItem, HistoricalContextPage, HistoricalContextReadPort, HistoricalContextRequest
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


class ReadHistoricalContextUseCase:
    async def execute(self, request: HistoricalContextRequest, *, actor: ActorContext,
        uow: PulseUnitOfWork) -> HistoricalContextPage:
        if not isinstance(request, HistoricalContextRequest):
            raise ValueError("historical_context_request_invalid")
        missing = EntityNotFoundError(request.target.kind, request.target.identity)
        if (actor.actor_kind not in ("human", "agent") or actor.realm_id != request.scope.realm_id
                or uow.realm_scope.realm_id != request.scope.realm_id
                or actor.board_id is not None and actor.board_id != request.scope.board_id):
            raise missing
        await uow.begin_consistent_read()
        if request.target.kind == "card":
            target = await load_accessible_card(uow, request.target.identity, actor,
                expected_board_id=request.scope.board_id)
        else:
            target = await _require_actor_board_spec(uow, request.target.identity, actor)
        if target is None or target.board_id != request.scope.board_id:
            raise missing
        grants = getattr(uow, "historical_archive_grants", None)
        archives = getattr(uow, "historical_archive_reader", None)
        reader = getattr(uow, "historical_context_reader", None)
        if (not isinstance(grants, HistoricalArchiveGrantPort) or not isinstance(archives, HistoricalArchiveReadPort)
                or not isinstance(reader, HistoricalContextReadPort)):
            raise TypeError("historical_context_adapter_missing")
        if (await archives.has_current_board_access(scope=request.scope,
                actor_kind=actor.actor_kind, actor_id=actor.actor_id)) is not True:
            raise missing
        if (await reader.has_current_target_access(request=request,
                actor_kind=actor.actor_kind, actor_id=actor.actor_id)) is not True:
            raise missing
        bindings = await reader.list_bindings(request=request, actor_kind=actor.actor_kind, actor_id=actor.actor_id)
        if type(bindings) is not tuple or len(bindings) > 100_000:
            raise ArchiveReadLimitExceeded("historical_context_candidate_limit")
        visible, seen = [], set()
        for binding in bindings:
            if (not isinstance(binding, HistoricalContextBinding) or binding.target != request.target
                    or binding.scope.realm_id != request.scope.realm_id or binding.scope.board_id != request.scope.board_id
                    or binding.identity in seen):
                raise ArchiveReadUnavailable("historical_context_binding_scope_mismatch")
            seen.add(binding.identity)
            try:
                state = await grants.get(scope=binding.scope, actor_kind=actor.actor_kind, actor_id=actor.actor_id)
            except (ValueError, TypeError) as exc:
                raise ArchiveReadUnavailable("historical_context_authority_unavailable") from exc
            if state is None or not archive_section_is_readable(state.captured, scope=binding.scope,
                    actor_kind=actor.actor_kind, actor_id=actor.actor_id, section=binding.section,
                    current_board_access=True, current_section_permission=state.sections.allows(binding.section)):
                continue
            if state.archive_id != binding.archive_id or state.archive_sha256 != binding.archive_sha256:
                raise ArchiveReadUnavailable("historical_context_archive_mismatch")
            visible.append((binding, state))
        visible.sort(key=lambda pair: pair[0].identity)
        items, size = [], 0
        for binding, state in visible[request.offset:request.offset + request.limit]:
            item = await reader.read_binding(binding=binding, grant=state)
            if not isinstance(item, HistoricalContextItem) or item.binding != binding:
                raise ArchiveReadUnavailable("historical_context_projection_mismatch")
            size += sum(len(record.encode("utf-8")) for record in item.source.records_json)
            if size > 25 * 1024 * 1024:
                raise ArchiveReadLimitExceeded("historical_context_page_limit")
            items.append(item)
        end = request.offset + len(items)
        return HistoricalContextPage(request, tuple(items), end if end < len(visible) else None)
