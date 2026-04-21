"""Unit tests for kg.parent_doc (ideação fe55ff7c).

Parser tests are pure and synchronous. Resolver tests use a stub async
DB session that captures the SQL statements issued and returns fake
rows, so we verify both the batch-by-type grouping and the payload
shape without needing a real Postgres/SQLite connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

import pytest

from okto_pulse.core.kg.parent_doc import (
    parse_artifact_ref,
    resolve_parent_artifacts,
)


# ===========================================================================
# parse_artifact_ref
# ===========================================================================


def test_parse_valid_spec_ref():
    assert parse_artifact_ref("spec:abc-123") == ("spec", "abc-123")


def test_parse_valid_card_ref():
    assert parse_artifact_ref("card:xyz") == ("card", "xyz")


def test_parse_valid_sprint_ref():
    assert parse_artifact_ref("sprint:p1") == ("sprint", "p1")


def test_parse_no_colon_returns_none():
    assert parse_artifact_ref("tech_entities.yml") is None


def test_parse_unknown_type_returns_none():
    assert parse_artifact_ref("foo:bar") is None


def test_parse_empty_returns_none():
    assert parse_artifact_ref("") is None
    assert parse_artifact_ref(None) is None


def test_parse_uuid_with_internal_colons():
    # Only the first ":" is treated as separator.
    assert parse_artifact_ref("spec:abc:def:ghi") == ("spec", "abc:def:ghi")


def test_parse_empty_uuid_returns_none():
    assert parse_artifact_ref("spec:") is None


# ===========================================================================
# resolve_parent_artifacts — stub DB
# ===========================================================================


class _FakeStatus(Enum):
    done = "done"
    draft = "draft"
    active = "active"


@dataclass
class _FakeRow:
    id: str
    title: str
    status: Any


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return [(r.id, r.title, r.status) for r in self._rows]


class _FakeDB:
    """Stub async SQLAlchemy session. Routes the select() by the model
    referenced in the statement's FROM clause, returning pre-seeded
    rows per type."""

    def __init__(self, specs=None, cards=None, sprints=None):
        self._by_type = {"spec": specs or [], "card": cards or [], "sprint": sprints or []}
        self.exec_calls = 0

    async def execute(self, statement):
        self.exec_calls += 1
        # Introspect the statement's froms to find which model is
        # being queried. SQLAlchemy exposes .column_descriptions but
        # the stub only needs a quick dispatch — look at the string.
        s = str(statement).lower()
        if "from specs" in s:
            return _FakeResult(self._by_type["spec"])
        if "from cards" in s:
            return _FakeResult(self._by_type["card"])
        if "from sprints" in s:
            return _FakeResult(self._by_type["sprint"])
        return _FakeResult([])


@pytest.mark.asyncio
async def test_resolve_mix_of_refs():
    db = _FakeDB(
        specs=[_FakeRow("s1", "My Spec", _FakeStatus.done)],
        cards=[_FakeRow("c1", "My Card", _FakeStatus.active)],
        sprints=[_FakeRow("sp1", "My Sprint", _FakeStatus.active)],
    )
    refs = ["spec:s1", "card:c1", "sprint:sp1", "bad", "spec:missing"]
    out = await resolve_parent_artifacts(db, refs)

    assert set(out.keys()) == {"spec:s1", "card:c1", "sprint:sp1"}
    assert out["spec:s1"]["type"] == "spec"
    assert out["spec:s1"]["id"] == "s1"
    assert out["spec:s1"]["title"] == "My Spec"
    assert out["spec:s1"]["status"] == "done"
    # Exactly 3 queries (one per type with at least one ref).
    assert db.exec_calls == 3


@pytest.mark.asyncio
async def test_resolve_only_calls_needed_types():
    db = _FakeDB(specs=[_FakeRow("s1", "Only Spec", _FakeStatus.draft)])
    out = await resolve_parent_artifacts(db, ["spec:s1"])
    assert db.exec_calls == 1  # only spec queried
    assert out["spec:s1"]["status"] == "draft"


@pytest.mark.asyncio
async def test_resolve_empty_refs_emits_no_queries():
    db = _FakeDB()
    out = await resolve_parent_artifacts(db, [])
    assert out == {}
    assert db.exec_calls == 0


@pytest.mark.asyncio
async def test_resolve_all_malformed_emits_no_queries():
    db = _FakeDB()
    out = await resolve_parent_artifacts(
        db, ["tech_entities.yml", "foo", "", "bar:"]
    )
    assert out == {}
    assert db.exec_calls == 0


@pytest.mark.asyncio
async def test_resolve_all_orphans_returns_empty():
    db = _FakeDB(specs=[])  # DB has nothing — all refs are orphan
    out = await resolve_parent_artifacts(db, ["spec:nonexistent"])
    # Still queries (1 call for type=spec) but returns nothing.
    assert out == {}


@pytest.mark.asyncio
async def test_resolve_status_as_string_also_works():
    """Some models use strings; some use Enum. The resolver normalises."""
    db = _FakeDB(
        specs=[_FakeRow("s1", "Spec", "in_progress")],
    )
    out = await resolve_parent_artifacts(db, ["spec:s1"])
    assert out["spec:s1"]["status"] == "in_progress"
