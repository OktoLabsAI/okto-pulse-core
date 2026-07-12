"""MKG-A C3 — CognitiveSourceStore port: fail-closed resolver and DTO contract."""

from __future__ import annotations

import dataclasses

import pytest

from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceError,
    CognitiveSourceRecord,
    CognitiveSourceStore,
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
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.node_id = "other"  # type: ignore[misc]


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
