"""Archive reads require current scoped authority before touching the content port."""

from dataclasses import replace
from itertools import product
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.historical_archive import ReadHistoricalArchiveSectionUseCase
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.ports.historical_archive import ArchiveGrantState, ArchiveReadGrant, ArchiveReadSections, ArchiveSection, ArchiveSourceScope
from okto_pulse.core.ports.historical_archive_read import ArchiveReadRequest, ArchiveSectionPage


SCOPE = ArchiveSourceScope("local", "board", "opaque", "origin")
ACTOR = ActorContext("reader", "mcp", actor_kind="agent", board_id="board", realm_scope=RealmScope.local())


class Ports:
    def __init__(self, decisions=(True, True, True, True), current=None, board=True):
        captured = ArchiveReadGrant(SCOPE, "agent", "reader", ArchiveReadSections(*decisions))
        self.state = ArchiveGrantState(captured, current or captured.sections, "archive", "a" * 64, 1)
        self.board = board
        self.calls = []

    async def begin_consistent_read(self):
        self.calls.append("snapshot")

    async def has_current_board_access(self, **kwargs):
        self.calls.append("board")
        return self.board

    async def get(self, **kwargs):
        self.calls.append("grant")
        return self.state

    async def revoke(self, **kwargs):
        raise AssertionError("a read must not mutate grants")

    async def read_section(self, *, request, grant):
        self.calls.append("content")
        return ArchiveSectionPage(request, grant.archive_id, ('{"title":"Historical"}',), None)

    def uow(self):
        return SimpleNamespace(realm_scope=RealmScope.local(), begin_consistent_read=self.begin_consistent_read,
            historical_archive_reader=self, historical_archive_grants=self)


@pytest.mark.asyncio
@pytest.mark.parametrize("decisions", list(product((False, True), repeat=4)))
@pytest.mark.parametrize("section", list(ArchiveSection))
async def test_all_captured_decisions_enforced_before_content(decisions, section):
    ports = Ports(decisions)
    request = ArchiveReadRequest(SCOPE, section)
    if ports.state.captured.sections.allows(section):
        page = await ReadHistoricalArchiveSectionUseCase().execute(request, actor=ACTOR, uow=ports.uow())
        assert page.records() == [{"title": "Historical"}]
        assert ports.calls == ["snapshot", "board", "grant", "content"]
    else:
        with pytest.raises(EntityNotFoundError):
            await ReadHistoricalArchiveSectionUseCase().execute(request, actor=ACTOR, uow=ports.uow())
        assert ports.calls == ["snapshot", "board", "grant"]


@pytest.mark.asyncio
@pytest.mark.parametrize("denial", ["board", "grant", "current", "actor", "origin"])
async def test_revoked_and_wrong_identity_or_origin_do_not_reach_reader(denial):
    ports = Ports()
    if denial == "board":
        ports.board = False
    elif denial == "grant":
        ports.state = None
    elif denial == "current":
        ports.state = replace(ports.state, sections=ArchiveReadSections(True, False, True, True))
    elif denial == "actor":
        ports.state = replace(ports.state, captured=replace(ports.state.captured, actor_id="other"))
    else:
        ports.state = replace(ports.state, captured=replace(ports.state.captured, scope=replace(SCOPE, origin_id="other")))
    with pytest.raises(EntityNotFoundError):
        await ReadHistoricalArchiveSectionUseCase().execute(ArchiveReadRequest(SCOPE, ArchiveSection.QA), actor=ACTOR, uow=ports.uow())
    assert "content" not in ports.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"realm_id": "other"}, {"board_id": "other"}, {"actor_kind": "unknown"}])
async def test_actor_binding_mismatch_does_not_open_snapshot(changes):
    ports = Ports()
    actor = ActorContext("reader", "mcp", **{"actor_kind": "agent", "board_id": "board", "realm_id": "local", **changes})
    with pytest.raises(EntityNotFoundError):
        await ReadHistoricalArchiveSectionUseCase().execute(ArchiveReadRequest(SCOPE, ArchiveSection.CONTENT), actor=actor, uow=ports.uow())
    assert ports.calls == []


@pytest.mark.asyncio
async def test_adapter_cannot_return_another_origin_or_archive():
    ports = Ports()
    async def wrong(*, request, grant):
        return ArchiveSectionPage(request, "other-archive", (), None)
    ports.read_section = wrong
    with pytest.raises(TypeError, match="scope_mismatch"):
        await ReadHistoricalArchiveSectionUseCase().execute(ArchiveReadRequest(SCOPE, ArchiveSection.CONTENT), actor=ACTOR, uow=ports.uow())


@pytest.mark.parametrize("kwargs", [{"offset": -1}, {"offset": True}, {"limit": 0}, {"limit": 201}, {"section": "content"}])
def test_request_rejects_untyped_sections_and_unbounded_pages(kwargs):
    with pytest.raises(ValueError):
        ArchiveReadRequest(**{"scope": SCOPE, "section": ArchiveSection.CONTENT, **kwargs})


@pytest.mark.parametrize("records", [('null',), ('[]',), ('{"score":NaN}',), ('{"score":Infinity}',)])
def test_page_rejects_non_record_or_non_json_values(records):
    with pytest.raises(ValueError):
        ArchiveSectionPage(ArchiveReadRequest(SCOPE, ArchiveSection.CONTENT), "archive", records, None)


def test_page_records_are_detached_and_original_text_is_preserved():
    page = ArchiveSectionPage(ArchiveReadRequest(SCOPE, ArchiveSection.QA), "archive", ('{"question":"  A\\r\\n ","choices":["x"]}',), None)
    record = page.records()[0]
    record["choices"].append("changed")
    assert page.records() == [{"question": "  A\r\n ", "choices": ["x"]}]
