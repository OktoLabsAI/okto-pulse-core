"""MKG-A C3 — CognitiveSourceStore port: fail-closed resolver and DTO contract."""

from __future__ import annotations

import dataclasses

import pytest

from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceConflict,
    CognitiveSourceError,
    CognitiveSourceRecord,
    CognitiveSourceStore,
    canonical_cognitive_source_fingerprint,
    latest_cognitive_source_records,
    register_cognitive_source_store,
    require_cognitive_source_store,
    reset_cognitive_source_store_for_tests,
    resolve_cognitive_source_store,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_cognitive_source_store_for_tests()
    yield
    reset_cognitive_source_store_for_tests()


class _FakeStore:
    async def append(self, record: CognitiveSourceRecord) -> str:
        return record.node_id

    async def append_many(
        self, records: tuple[CognitiveSourceRecord, ...]
    ) -> tuple[str, ...]:
        return tuple(record.node_id for record in records)

    async def append_many_in_context(
        self,
        context: object,
        records: tuple[CognitiveSourceRecord, ...],
    ) -> tuple[str, ...]:
        del context
        return await self.append_many(records)

    async def enumerate(self, board_id: str) -> tuple[CognitiveSourceRecord, ...]:
        return ()


def test_require_without_registration_fails_closed():
    with pytest.raises(CognitiveSourceError) as excinfo:
        require_cognitive_source_store()
    assert excinfo.value.failure_reason == "cognitive_source_store_absent"
    assert excinfo.value.remediation


def test_register_resolve_require_roundtrip():
    store = _FakeStore()
    register_cognitive_source_store(store)
    assert resolve_cognitive_source_store() is store
    assert require_cognitive_source_store() is store
    reset_cognitive_source_store_for_tests()
    assert resolve_cognitive_source_store() is None


def test_fake_satisfies_protocol():
    assert isinstance(_FakeStore(), CognitiveSourceStore)


def test_record_is_frozen_and_defaults_are_safe():
    record = CognitiveSourceRecord(
        node_id="learning_abc",
        board_id="board-1",
        node_type="Learning",
        generation=0,
        payload={"title": "t"},
    )
    assert record.evidence_refs == ()
    assert record.source_session_id is None
    assert record.source_revision == 0
    assert len(record.record_fingerprint) == 64
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.node_id = "other"  # type: ignore[misc]


def test_fingerprint_is_canonical_and_excludes_revision_metadata():
    first = CognitiveSourceRecord(
        node_id="learning_abc",
        board_id="board-1",
        node_type="Learning",
        generation=0,
        payload={"nested": {"b": 2, "a": 1}, "title": "t"},
        evidence_refs=("spec:1",),
        source_session_id="session-one",
        committed_at="2026-07-22T00:00:00+00:00",
    )
    revised = CognitiveSourceRecord(
        node_id="learning_abc",
        board_id="board-1",
        node_type="Learning",
        generation=0,
        payload={"title": "t", "nested": {"a": 1, "b": 2}},
        evidence_refs=("spec:1",),
        source_session_id="session-two",
        committed_at="2026-07-22T00:01:00+00:00",
        metadata={"storage_id": "different"},
        source_revision=1,
    )

    assert first.record_fingerprint == revised.record_fingerprint
    assert first.record_fingerprint == canonical_cognitive_source_fingerprint(
        board_id=first.board_id,
        node_id=first.node_id,
        node_type=first.node_type,
        generation=first.generation,
        payload=first.payload,
        evidence_refs=first.evidence_refs,
    )


def test_record_rejects_mismatched_fingerprint_and_negative_revision():
    common = {
        "node_id": "learning_abc",
        "board_id": "board-1",
        "node_type": "Learning",
        "generation": 0,
        "payload": {"title": "t"},
    }
    with pytest.raises(ValueError, match="record_fingerprint"):
        CognitiveSourceRecord(**common, record_fingerprint="0" * 64)
    with pytest.raises(ValueError, match="non-negative"):
        CognitiveSourceRecord(**common, source_revision=-1)


def test_latest_records_select_highest_revision_and_parse_json_rows():
    rows = (
        {
            "board_id": "board-1",
            "node_id": "learning_abc",
            "node_type": "Learning",
            "generation": 0,
            "source_revision": 0,
            "payload": '{"title":"old"}',
            "evidence_refs": '["spec:1"]',
            "committed_at": "2026-07-22T00:00:00+00:00",
        },
        {
            "board_id": "board-1",
            "node_id": "learning_abc",
            "node_type": "Learning",
            "generation": 0,
            "source_revision": 1,
            "payload": '{"title":"new"}',
            "evidence_refs": '["spec:1"]',
            "committed_at": "2026-07-22T00:01:00+00:00",
        },
    )

    assert latest_cognitive_source_records(rows) == (rows[1],)


def test_latest_records_reject_divergent_same_revision():
    first = CognitiveSourceRecord(
        node_id="learning_abc",
        board_id="board-1",
        node_type="Learning",
        generation=0,
        payload={"title": "first"},
        source_revision=1,
    )
    divergent = CognitiveSourceRecord(
        node_id="learning_abc",
        board_id="board-1",
        node_type="Learning",
        generation=0,
        payload={"title": "divergent"},
        source_revision=1,
    )

    with pytest.raises(CognitiveSourceConflict) as excinfo:
        latest_cognitive_source_records((first, divergent))
    assert excinfo.value.failure_reason == "cognitive_source_revision_conflict"


def test_latest_records_accept_identical_retry_and_keep_board_scope():
    first = CognitiveSourceRecord(
        node_id="learning_abc",
        board_id="board-1",
        node_type="Learning",
        generation=7,
        payload={"title": "same"},
        source_session_id="session-first",
        source_revision=3,
    )
    retry = CognitiveSourceRecord(
        node_id="learning_abc",
        board_id="board-1",
        node_type="Learning",
        generation=7,
        payload={"title": "same"},
        source_session_id="session-retry",
        source_revision=3,
    )
    other_board = CognitiveSourceRecord(
        node_id="learning_abc",
        board_id="board-2",
        node_type="Learning",
        generation=7,
        payload={"title": "other board"},
        source_revision=3,
    )

    selected = latest_cognitive_source_records((first, retry, other_board))

    assert selected == (first, other_board)
    assert all(record.generation == 7 for record in selected)


def test_error_carries_structured_context():
    err = CognitiveSourceError(
        "cognitive_source_append_failed",
        board_id="b1",
        node_id="n1",
        remediation="retry",
    )
    assert err.board_id == "b1"
    assert err.node_id == "n1"
    assert "cognitive_source_append_failed" in str(err)


def test_port_is_reexported_from_ports_package():
    from okto_pulse.core import ports

    assert ports.CognitiveSourceStore is CognitiveSourceStore
    assert ports.CognitiveSourceRecord is CognitiveSourceRecord
    assert ports.CognitiveSourceConflict is CognitiveSourceConflict
    assert (
        ports.canonical_cognitive_source_fingerprint
        is canonical_cognitive_source_fingerprint
    )
    assert ports.latest_cognitive_source_records is latest_cognitive_source_records
