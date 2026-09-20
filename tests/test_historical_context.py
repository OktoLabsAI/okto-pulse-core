from dataclasses import replace
from itertools import product
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.application.use_cases.historical_context import ReadHistoricalContextUseCase
from okto_pulse.core.ports.context_disposition import ContextTarget
from okto_pulse.core.ports.historical_archive import ArchiveReadSections, ArchiveSection
from okto_pulse.core.ports.historical_archive_read import ArchiveBoardScope, ArchiveReadRequest, ArchiveReadUnavailable, ArchiveSectionPage
from okto_pulse.core.ports.historical_context import HistoricalContextBinding, HistoricalContextItem, HistoricalContextRequest, context_target_permission_allows_read
from okto_pulse.core.ports.permission_policy import PermissionSet
from test_historical_archive_read import ACTOR, Ports, SCOPE

TARGET = ContextTarget(kind="spec", identity="spec")
REQUEST = HistoricalContextRequest(ArchiveBoardScope("local", "board"), TARGET)


class ContextPorts(Ports):
    def __init__(self, decisions=(True, True, True, True)):
        super().__init__(decisions)
        self.bindings = tuple(HistoricalContextBinding(section.value, SCOPE, TARGET, "archive", "a" * 64, section,
            "objective" if section is ArchiveSection.CONTENT else None) for section in ArchiveSection)
        self.target_access = True

    async def has_current_target_access(self, **kwargs):
        self.calls.append("target-access")
        return self.target_access

    async def list_bindings(self, **kwargs):
        self.calls.append("bindings")
        return self.bindings

    async def read_binding(self, *, binding, grant):
        self.calls.append("content:" + binding.section.value)
        return HistoricalContextItem(binding, ArchiveSectionPage(ArchiveReadRequest(binding.scope, binding.section, 0, 1),
            grant.archive_id, ('{"original":"  prose\\r\\n ","author":"source"}',), None))

    def uow(self):
        uow = super().uow()
        uow.historical_context_reader = self
        target = SimpleNamespace(id="spec", board_id="board")
        uow.services = SimpleNamespace(specs=SimpleNamespace(get_spec=AsyncMock(return_value=target)),
            cards=SimpleNamespace(get_card=AsyncMock(return_value=target)))
        uow.boards = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(realm_id="local", owner_id="owner")))
        return uow


@pytest.mark.asyncio
@pytest.mark.parametrize("decisions", list(product((False, True), repeat=4)))
async def test_only_effectively_authorized_sections_reach_private_content(decisions):
    ports = ContextPorts(decisions)
    result = await ReadHistoricalContextUseCase().execute(REQUEST, actor=ACTOR, uow=ports.uow())
    expected = sorted(section.value for section in ArchiveSection if ports.state.sections.allows(section))
    assert [item.binding.section.value for item in result.items] == expected
    assert [call for call in ports.calls if call.startswith("content:")] == ["content:" + value for value in expected]
    assert ports.calls[:4] == ["snapshot", "board", "target-access", "bindings"]
    assert result.next_offset is None


@pytest.mark.asyncio
@pytest.mark.parametrize("denial", ["missing-target", "cross-board", "target-permission", "board", "grant", "revoked"])
async def test_target_denials_and_source_revocations_never_read_private_content(denial):
    ports = ContextPorts()
    uow = ports.uow()
    if denial == "missing-target":
        uow.services.specs.get_spec.return_value = None
    elif denial == "cross-board":
        uow.services.specs.get_spec.return_value.board_id = "foreign"
    elif denial == "target-permission":
        ports.target_access = False
    elif denial == "board":
        ports.board = False
    elif denial == "grant":
        ports.state = None
    else:
        ports.state = replace(ports.state, sections=ArchiveReadSections(False, False, False, False))
    if denial in ("grant", "revoked"):
        assert (await ReadHistoricalContextUseCase().execute(REQUEST, actor=ACTOR, uow=uow)).items == ()
    else:
        with pytest.raises(EntityNotFoundError):
            await ReadHistoricalContextUseCase().execute(REQUEST, actor=ACTOR, uow=uow)
        assert "bindings" not in ports.calls
    assert not any(call.startswith("content:") for call in ports.calls)


@pytest.mark.asyncio
async def test_pagination_filters_denied_metadata_and_preserves_attribution():
    ports = ContextPorts((True, False, False, True))
    first = await ReadHistoricalContextUseCase().execute(replace(REQUEST, limit=1), actor=ACTOR, uow=ports.uow())
    second = await ReadHistoricalContextUseCase().execute(replace(REQUEST, limit=1, offset=1), actor=ACTOR, uow=ports.uow())
    assert first.next_offset == 1 and second.next_offset is None
    assert [first.items[0].binding.section.value, second.items[0].binding.section.value] == ["content", "history"]
    assert second.items[0].source.records() == [{"original": "  prose\r\n ", "author": "source"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong", ["target", "board", "realm", "duplicate", "archive", "projection"])
async def test_wrong_adapter_scope_and_provenance_fail_closed(wrong):
    ports = ContextPorts()
    binding = ports.bindings[0]
    if wrong == "target":
        binding = replace(binding, target=ContextTarget(kind="card", identity="other"))
    elif wrong in ("board", "realm"):
        binding = replace(binding, scope=replace(binding.scope, **{wrong + "_id": "foreign"}))
    elif wrong == "archive":
        binding = replace(binding, archive_sha256="b" * 64)
    elif wrong == "projection":
        original = ports.read_binding
        async def incorrect(**kwargs):
            return replace(await original(**kwargs), binding=replace(binding, identity="other"))
        ports.read_binding = incorrect
    ports.bindings = (binding, binding) if wrong == "duplicate" else (binding,)
    with pytest.raises(ArchiveReadUnavailable):
        await ReadHistoricalContextUseCase().execute(REQUEST, actor=ACTOR, uow=ports.uow())


@pytest.mark.parametrize("kind", ["spec", "card"])
def test_target_policy_uses_existing_entity_read_leaf(kind):
    target = ContextTarget(kind=kind, identity="target")
    assert not context_target_permission_allows_read(target, PermissionSet({kind: {"entity": {"read": False}}}))
    assert context_target_permission_allows_read(target, PermissionSet({kind: {"entity": {"read": True}}}))


@pytest.mark.parametrize("kwargs", [{"offset": True}, {"offset": -1}, {"limit": 0}, {"limit": 201}, {"target": {"kind": "sprint"}}])
def test_request_bounds_and_closed_target(kwargs):
    with pytest.raises(ValueError):
        replace(REQUEST, **kwargs)
