"""MKG-A C5 — durable cognitive replay + manifest digest.

Covers spec MKG-A-S1 scenarios S4 (unreadable snapshot -> replay restores
100% from the durable source with identical ids), S5 (restore+replay never
duplicates; second replay is a no-op), S6 (human_curated content is never
clobbered) and S7 (source_set_hash deterministic; unchanged for boards
without durable records; includes the class when records exist).
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.canonical_cognitive_preservation import (
    replay_durable_cognitive,
)
from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id
from okto_pulse.core.kg.rebuild_sources import RebuildSourceEnumerator
from okto_pulse.core.kg.rebuild_sources import (
    _compose_source_set_hash as compose_hash,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceRecord,
    register_cognitive_source_store,
    reset_cognitive_source_store_for_tests,
)

from kg_schema_testing import (
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


class _MemoryStore:
    def __init__(self) -> None:
        self.records: list[CognitiveSourceRecord] = []

    async def append(self, record: CognitiveSourceRecord) -> str:
        return (await self.append_many((record,)))[0]

    async def append_many(
        self, records: tuple[CognitiveSourceRecord, ...]
    ) -> tuple[str, ...]:
        self.records.extend(records)
        return tuple(record.node_id for record in records)

    async def enumerate(self, board_id: str):
        return tuple(
            sorted(
                (r for r in self.records if r.board_id == board_id),
                key=lambda r: (
                    r.committed_at or "",
                    r.node_id,
                    r.generation,
                    r.source_revision,
                ),
            )
        )


@pytest.fixture(autouse=True)
def _reset_store():
    reset_cognitive_source_store_for_tests()
    yield
    reset_cognitive_source_store_for_tests()


@pytest.fixture
def kg_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_replay_"))
    monkeypatch.setenv("KG_BASE_DIR", str(base))
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    yield base
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _payload(title: str, *, human_curated: bool = False) -> dict:
    return {
        "title": title,
        "content": "lesson body",
        "context": "ctx",
        "justification": "evidence ref preserved",
        "source_artifact_ref": "",
        "graph_layer": "canonical",
        "maturity_status": "canonical_eligible",
        "created_at": "2026-07-11T20:00:00.000000",
        "created_by_agent": "system:layer1_worker",
        "source_confidence": 0.9,
        "relevance_score": 0.5,
        "query_hits": 0,
        "last_queried_at": None,
        "priority_boost": 0.0,
        "human_curated": human_curated,
        "generation": 0,
        "embedding": [0.0] * 384,
    }


def _record(board_id: str, node_type: str, title: str) -> CognitiveSourceRecord:
    node_id = mint_node_id(
        board_id, node_type, derive_natural_key("", node_type, title), 0
    )
    return CognitiveSourceRecord(
        node_id=node_id,
        board_id=board_id,
        node_type=node_type,
        generation=0,
        payload=_payload(title),
        evidence_refs=(),
        source_session_id="sess-replay",
        committed_at="2026-07-11T20:00:00+00:00",
    )


def _read_node(board_id: str, node_type: str, node_id: str) -> dict | None:
    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id "
            "RETURN n.id, n.title, n.human_curated, n.source_session_id LIMIT 1",
            {"id": node_id},
        )
        try:
            if res.has_next():
                row = res.get_next()
                return {
                    "id": row[0],
                    "title": row[1],
                    "human_curated": row[2],
                    "source_session_id": row[3],
                }
            return None
        finally:
            try:
                res.close()
            except Exception:
                pass


def test_replay_restores_from_durable_source_after_unreadable_snapshot(kg_tempdir):
    """S4: fresh (post-purge) graph + durable records => 100% restored."""
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)

    store = _MemoryStore()
    records = [
        _record(board_id, "Decision", "Decisao cognitiva um"),
        _record(board_id, "Learning", "Learning um"),
        _record(board_id, "Learning", "Learning dois"),
        _record(board_id, "Alternative", "Alternativa um"),
    ]
    store.records.extend(records)
    register_cognitive_source_store(store)

    summary = replay_durable_cognitive(board_id)
    assert summary["durable_source_status"] == "ok"
    assert summary["replayed_cognitive_count"] == 4
    assert summary["replay_failed"] == []
    for record in records:
        node = _read_node(board_id, record.node_type, record.node_id)
        assert node is not None, record.node_id
        assert node["title"] == record.payload["title"]


def test_replay_restores_only_latest_source_revision(kg_tempdir):
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)

    base = _record(board_id, "Learning", "Learning revisionada")
    revised_payload = {**base.payload, "title": "Learning revisionada latest"}
    revised = CognitiveSourceRecord(
        node_id=base.node_id,
        board_id=base.board_id,
        node_type=base.node_type,
        generation=base.generation,
        payload=revised_payload,
        evidence_refs=("spec:latest",),
        source_session_id="sess-replay-latest",
        committed_at="2026-07-11T20:01:00+00:00",
        source_revision=1,
    )
    store = _MemoryStore()
    store.records.extend((base, revised))
    register_cognitive_source_store(store)

    summary = replay_durable_cognitive(board_id)

    assert summary["durable_source_status"] == "ok"
    assert summary["replayed_cognitive_count"] == 1
    node = _read_node(board_id, revised.node_type, revised.node_id)
    assert node is not None
    assert node["title"] == "Learning revisionada latest"
    assert node["source_session_id"] == "sess-replay-latest"


def test_replay_is_idempotent_and_never_duplicates_restore(kg_tempdir):
    """S5: second run is a no-op; snapshot-restored nodes are skipped."""
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)

    store = _MemoryStore()
    store.records.append(_record(board_id, "Learning", "Learning idem"))
    register_cognitive_source_store(store)

    first = replay_durable_cognitive(board_id)
    assert first["replayed_cognitive_count"] == 1
    second = replay_durable_cognitive(board_id)
    assert second["replayed_cognitive_count"] == 0
    assert second["replay_skipped_present_count"] == 1

    conn = open_board_connection(board_id)
    with conn as (_kdb, kconn):
        res = kconn.execute("MATCH (n:Learning) RETURN count(n)")
        assert int(res.get_next()[0]) == 1
        res.close()


def test_replay_never_clobbers_human_curated_content(kg_tempdir):
    """S6: existing curated node with divergent durable record stays intact."""
    from okto_pulse.core.kg.canonical_cognitive_preservation import _create_node

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)

    record = _record(board_id, "Learning", "Titulo original")
    curated_attrs = _payload("Titulo CURADO pelo humano", human_curated=True)
    _create_node(board_id, "Learning", record.node_id, curated_attrs)

    store = _MemoryStore()
    store.records.append(record)  # divergent pre-curation payload
    register_cognitive_source_store(store)

    summary = replay_durable_cognitive(board_id)
    assert summary["replayed_cognitive_count"] == 0
    assert summary["replay_skipped_present_count"] == 1
    node = _read_node(board_id, "Learning", record.node_id)
    assert node["title"] == "Titulo CURADO pelo humano"
    assert bool(node["human_curated"]) is True


def test_replay_without_registered_store_reports_absent(kg_tempdir):
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    summary = replay_durable_cognitive(board_id)
    assert summary["durable_source_status"] == "absent"
    assert summary["replayed_cognitive_count"] == 0


def test_source_set_hash_deterministic_and_class_bound(kg_tempdir):
    """S7: same inputs => same hash; empty store => pre-feature hash;
    records present => hash differs and counters expose the class."""
    board_id = str(uuid.uuid4())

    enumerator = RebuildSourceEnumerator(source_store=lambda _bid: [])

    # Pre-feature baseline: no store registered.
    baseline = compose_hash(enumerator.enumerate(board_id=board_id))

    # Empty registered store: byte-identical to the baseline.
    store = _MemoryStore()
    register_cognitive_source_store(store)
    empty_hash = compose_hash(enumerator.enumerate(board_id=board_id))
    assert empty_hash == baseline

    # With durable records: hash changes, deterministically.
    store.records.append(_record(board_id, "Learning", "Learning hash"))
    with_records_1 = enumerator.enumerate(board_id=board_id)
    with_records_2 = enumerator.enumerate(board_id=board_id)
    h1 = compose_hash(with_records_1)
    h2 = compose_hash(with_records_2)
    assert h1 == h2
    assert h1 != baseline
    assert with_records_1.to_dict()["cognitive_durable_count"] == 1

    # Removing the records returns to the exact baseline (class unbinds).
    store.records.clear()
    assert compose_hash(enumerator.enumerate(board_id=board_id)) == baseline
