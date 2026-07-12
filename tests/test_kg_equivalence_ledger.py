"""MKG-C C1 — EquivalenceLedger port contract (scenario S1).

The SAME contract runs against the community SQLAlchemy adapter (real
kg_equivalence_ledger table) and the in-memory testing ledger: append with
complete snapshot, idempotent replay, revoke-preserves-record (append-only),
idempotent second revoke, active_for_board excluding revoked, and the
fail-closed resolver.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.ports.kg_equivalence_ledger import (
    EquivalenceLedger,
    EquivalenceLedgerError,
    EquivalenceRecord,
    register_equivalence_ledger,
    require_equivalence_ledger,
    reset_equivalence_ledger_for_tests,
    resolve_equivalence_ledger,
)

pytestmark = pytest.mark.asyncio

_EVIDENCE = {
    "nodes": [
        {"id": "entity_dup1", "attrs": {"title": "Dup 1", "content": "c1"}},
    ],
    "edges": [
        {
            "type": "belongs_to",
            "from": "entity_dup1",
            "to": "entity_root",
            "props": {
                "confidence": 1.0,
                "layer": "cognitive",
                "rule_id": "",
                "created_by": "s",
                "fallback_reason": "",
                "created_by_session_id": "ses1",
            },
        }
    ],
}


def _record(board_id: str, record_id: str | None = None) -> EquivalenceRecord:
    return EquivalenceRecord(
        record_id=record_id or f"eqv_{uuid.uuid4().hex[:16]}",
        board_id=board_id,
        node_type="Entity",
        survivor_id="entity_abc",
        merged_ids=("entity_dup1", "entity_dup2"),
        operation="dedup_entities",
        evidence=_EVIDENCE,
        created_by="test:mkgc",
    )


@pytest.fixture(autouse=True)
def _clean_port_registry():
    reset_equivalence_ledger_for_tests()
    yield
    reset_equivalence_ledger_for_tests()


async def _contract(ledger, board_id: str) -> None:
    """The shared S1 contract, store-agnostic."""

    rec = _record(board_id)
    rid = await ledger.append(rec)
    assert rid == rec.record_id

    # Idempotent replay of the same record_id.
    assert await ledger.append(rec) == rec.record_id

    # Complete snapshot recoverable.
    loaded = await ledger.get(rec.record_id)
    assert loaded is not None
    assert loaded.survivor_id == "entity_abc"
    assert loaded.merged_ids == ("entity_dup1", "entity_dup2")
    assert dict(loaded.evidence)["edges"][0]["props"]["confidence"] == 1.0
    assert loaded.is_active

    # Second record to prove filtering below; ordering is deterministic
    # (created_at, record_id) by contract.
    rec2 = _record(board_id)
    await ledger.append(rec2)
    active = await ledger.active_for_board(board_id)
    assert {r.record_id for r in active} == {rec.record_id, rec2.record_id}
    assert [r.record_id for r in active] == [
        r.record_id
        for r in sorted(active, key=lambda r: ((r.created_at or ""), r.record_id))
    ]

    # Revoke preserves the record (append-only) and is idempotent.
    revoked = await ledger.revoke(rec.record_id, "engano de curadoria")
    assert revoked.revoked_at is not None
    assert revoked.revoke_reason == "engano de curadoria"
    again = await ledger.revoke(rec.record_id, "outro motivo")
    assert again.revoked_at == revoked.revoked_at
    assert again.revoke_reason == "engano de curadoria"

    still_there = await ledger.get(rec.record_id)
    assert still_there is not None and not still_there.is_active

    active_after = await ledger.active_for_board(board_id)
    assert [r.record_id for r in active_after] == [rec2.record_id]

    # Unknown record fails structured.
    with pytest.raises(EquivalenceLedgerError):
        await ledger.revoke("eqv_nao_existe", "x")


async def test_s1_in_memory_ledger_contract():
    from kg_registry_testing import _InMemoryEquivalenceLedger

    await _contract(_InMemoryEquivalenceLedger(), str(uuid.uuid4()))


@pytest.fixture
def ledger_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_eqvledger_"))
    db_path = base / "pulse.db"
    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    yield base
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


@pytest.fixture(autouse=True)
def _restore_conftest_engine():
    from okto_pulse.core.infra.database import create_database, get_engine

    prior_url = str(get_engine().url)
    yield
    if str(get_engine().url) != prior_url:
        create_database(prior_url, echo=False)


async def test_s1_sqlalchemy_ledger_contract(ledger_tempdir):
    import os

    from okto_pulse.community.adapters.sqlalchemy_base import Base as CommunityBase
    from okto_pulse.community.adapters.sqlalchemy_kg_equivalence_ledger import (
        CommunitySqlAlchemyEquivalenceLedger,
    )
    from okto_pulse.core.infra.database import (
        create_database,
        get_engine,
        get_session_factory,
        init_db,
    )

    create_database(os.environ["DATABASE_URL"], echo=False)
    await init_db()
    async with get_engine().begin() as conn:
        await conn.run_sync(CommunityBase.metadata.create_all)

    ledger = CommunitySqlAlchemyEquivalenceLedger(get_session_factory())
    await _contract(ledger, str(uuid.uuid4()))


def test_s1_fail_closed_resolver_and_protocol():
    with pytest.raises(EquivalenceLedgerError) as excinfo:
        require_equivalence_ledger()
    assert excinfo.value.failure_reason == "kg_equivalence_ledger_unavailable"
    assert excinfo.value.remediation

    from kg_registry_testing import _InMemoryEquivalenceLedger

    ledger = _InMemoryEquivalenceLedger()
    register_equivalence_ledger(ledger)
    assert resolve_equivalence_ledger() is ledger
    assert require_equivalence_ledger() is ledger
    assert isinstance(ledger, EquivalenceLedger)
    reset_equivalence_ledger_for_tests()
    assert resolve_equivalence_ledger() is None
