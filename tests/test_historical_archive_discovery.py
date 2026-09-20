from dataclasses import replace
from itertools import product

import pytest

from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.application.use_cases.historical_archive import DiscoverHistoricalArchivesUseCase
from okto_pulse.core.ports.historical_archive import ArchiveReadSections, ArchiveSection
from okto_pulse.core.ports.historical_archive_read import ArchiveBoardScope, ArchiveDiscoveryRequest, ArchiveReadUnavailable
from test_historical_archive_read import ACTOR, Ports, SCOPE

REQUEST = ArchiveDiscoveryRequest(ArchiveBoardScope("local", "board"))


class DiscoveryPorts(Ports):
    async def list_grants(self, **kwargs):
        self.calls.append("list")
        return self.states


@pytest.mark.asyncio
@pytest.mark.parametrize("decisions", list(product((False, True), repeat=4)))
async def test_discovery_sections_use_the_same_captured_ceiling(decisions):
    ports = DiscoveryPorts(decisions)
    ports.states = (ports.state,)
    result = await DiscoverHistoricalArchivesUseCase().execute(REQUEST, actor=ACTOR, uow=ports.uow())
    assert ports.calls == ["snapshot", "board", "list"]
    assert result.next_offset is None
    if decisions[0]:
        assert result.items[0].sections == tuple(section for section in ArchiveSection if ports.state.sections.allows(section))
    else:
        assert result.items == ()


@pytest.mark.asyncio
async def test_pagination_filters_denied_origins_before_offsets_and_has_no_hidden_count():
    ports = DiscoveryPorts()
    def entry(name, sections=None):
        return replace(ports.state, captured=replace(ports.state.captured, scope=replace(SCOPE, origin_id=name)),
            sections=sections or ports.state.sections)
    ports.states = (entry("z"), entry("secret", ArchiveReadSections(False, False, False, False)), entry("a"))
    first = await DiscoverHistoricalArchivesUseCase().execute(replace(REQUEST, limit=1), actor=ACTOR, uow=ports.uow())
    second = await DiscoverHistoricalArchivesUseCase().execute(replace(REQUEST, offset=1, limit=1), actor=ACTOR, uow=ports.uow())
    assert first.items[0].scope.origin_id == "a" and first.next_offset == 1
    assert second.items[0].scope.origin_id == "z" and second.next_offset is None
    assert "secret" not in repr(first) + repr(second)


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong", ["board", "realm", "actor", "duplicate", "type"])
async def test_misbehaving_adapter_cannot_disclose_foreign_grants(wrong):
    ports = DiscoveryPorts()
    state = ports.state
    if wrong in ("board", "realm"):
        state = replace(state, captured=replace(state.captured, scope=replace(SCOPE, **{f"{wrong}_id": "foreign"})))
    elif wrong == "actor":
        state = replace(state, captured=replace(state.captured, actor_id="foreign"))
    ports.states = (state, state) if wrong == "duplicate" else (None,) if wrong == "type" else (state,)
    with pytest.raises(ArchiveReadUnavailable):
        await DiscoverHistoricalArchivesUseCase().execute(REQUEST, actor=ACTOR, uow=ports.uow())


@pytest.mark.asyncio
async def test_denied_board_does_not_enumerate_authority():
    ports = DiscoveryPorts(board=False)
    with pytest.raises(EntityNotFoundError):
        await DiscoverHistoricalArchivesUseCase().execute(REQUEST, actor=ACTOR, uow=ports.uow())
    assert ports.calls == ["snapshot", "board"]


@pytest.mark.parametrize("kwargs", [{"offset": True}, {"offset": -1}, {"limit": 0}, {"limit": 201}])
def test_discovery_rejects_unbounded_or_untyped_pagination(kwargs):
    with pytest.raises(ValueError):
        replace(REQUEST, **kwargs)
