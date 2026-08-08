"""MKG-B C5 — kg_provenance_drift report (scenario S6, AC6).

(a) artifact edited after consolidation → node listed (content_changed);
(a') node anchor diverges from the last consolidated state → content_changed;
(b) artifact deleted → artifact_missing (terminal);
(c) nothing changed → empty list; and the report NEVER writes to the graph.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg import provenance_drift as provenance_module
from okto_pulse.core.kg.provenance_drift import provenance_drift_report

from test_kg_dedup_nc8 import (  # noqa: F401  (harness reuse)
    _bootstrap_test_board as _bootstrap_uncomposed_test_board,
)
from test_kg_provenance_commit_fill import (  # noqa: F401  (harness reuse)
    _drive_with_provenance,
    _prov_row,
)

pytestmark = pytest.mark.asyncio


async def _bootstrap_test_board(monkeypatch):
    """Reuse the shared bootstrap with the official Community Session."""

    _plain_factory, board_id, spec_id = (
        await _bootstrap_uncomposed_test_board(monkeypatch)
    )
    from okto_pulse.community.adapters.sqlalchemy_database import (
        build_community_session_factory,
    )
    from okto_pulse.core.infra.database import get_engine

    session_factory = build_community_session_factory(get_engine())
    configure_real_graph_test_kg_registry(session_factory=session_factory)
    return session_factory, board_id, spec_id


@pytest.fixture(autouse=True)
def _restore_conftest_engine(preserve_relational_runtime):
    yield


@pytest.fixture
def drift_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_drift_"))
    db_path = base / "pulse.db"
    kg_path = base / "kg"
    kg_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("KG_BASE_DIR", str(kg_path))
    monkeypatch.setenv("KG_CLEANUP_ENABLED", "false")
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()

    yield base

    try:
        from kg_schema_testing import close_all_connections

        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


async def _insert_spec_row(session_factory, board_id: str, spec_id: str) -> None:
    from okto_pulse.community.adapters.sqlalchemy_models import Board, Spec

    async with session_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(
                Board(
                    id=board_id,
                    name=f"Provenance Drift {board_id}",
                    owner_id="mkgb-drift-test",
                )
            )
            await db.flush()
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec alvo do drift",
                created_by="mkgb-drift-test",
            )
        )
        await db.commit()


async def _touch_spec_row(session_factory, spec_id: str) -> None:
    """Simulate an artifact edit AFTER the consolidation landed."""
    from sqlalchemy import update

    from okto_pulse.community.adapters.sqlalchemy_models import Spec

    async with session_factory() as db:
        await db.execute(
            update(Spec)
            .where(Spec.id == spec_id)
            .values(
                title="Spec alvo do drift EDITADA",
                updated_at=datetime.now(timezone.utc) + timedelta(seconds=5),
            )
        )
        await db.commit()


async def _delete_spec_row(session_factory, spec_id: str) -> None:
    from sqlalchemy import delete

    from okto_pulse.community.adapters.sqlalchemy_models import Spec

    async with session_factory() as db:
        await db.execute(delete(Spec).where(Spec.id == spec_id))
        await db.commit()


async def _insert_card_row(
    session_factory,
    board_id: str,
    spec_id: str,
    card_id: str,
    card_type: str,
) -> None:
    from okto_pulse.community.adapters.sqlalchemy_models import Card

    async with session_factory() as db:
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title=f"Card {card_type} alvo do drift",
                card_type=card_type,
                created_by="mkgb-drift-test",
            )
        )
        await db.commit()


async def _graph_snapshot(board_id: str, artifact_ref: str) -> dict:
    return await run_blocking_graph_io(
        lambda: _prov_row(board_id, artifact_ref),
        task_name="test.kg.provenance_drift.graph_snapshot",
    )


async def test_s6_clean_board_reports_no_drift(drift_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    await _insert_spec_row(session_factory, board_id, spec_id)
    artifact_ref = f"spec:{spec_id}"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref, "[MKG-B] Drift limpo",
        content="conteudo",
    )
    report = await provenance_drift_report(board_id)
    # AC6(c): nothing changed → empty list; the consolidated node was checked.
    assert report["drifted_count"] == 0
    assert report["drifted"] == []
    assert report["checked_count"] >= 1
    # The technical root does not belong to this artifact session, so it gets
    # no fabricated source hash and never enters the drift scan.
    assert report["skipped_count"] == 0


async def test_s6_timestamp_only_edit_with_equal_committed_hash_is_not_drift(
    drift_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    await _insert_spec_row(session_factory, board_id, spec_id)
    artifact_ref = f"spec:{spec_id}"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref, "[MKG-B] Drift por edicao",
        content="conteudo",
    )
    await _touch_spec_row(session_factory, spec_id)

    report = await provenance_drift_report(board_id)
    drifted = {d["node_id"]: d for d in report["drifted"]}
    node = await _graph_snapshot(board_id, artifact_ref)
    # updated_at is volatile metadata. Persisted and latest committed canonical
    # hashes still match, so the report must not invent true drift.
    assert node["id"] not in drifted
    assert report["drifted_by_reason"]["content_changed"] == 0


async def test_s6_stale_anchor_vs_latest_consolidation_is_drifted(
    drift_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    await _insert_spec_row(session_factory, board_id, spec_id)
    artifact_ref = f"spec:{spec_id}"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref, "[MKG-B] Ancora velha",
        content="conteudo",
    )
    node = await _graph_snapshot(board_id, artifact_ref)
    # Diverge the persisted anchor from the latest audit (models a curated
    # node whose protected content no longer matches the source state).
    from kg_schema_testing import open_board_connection

    def _diverge_persisted_anchor() -> None:
        conn_ctx = open_board_connection(board_id)
        with conn_ctx as (_kdb, kconn):
            kconn.execute(
                "MATCH (n:Entity) WHERE n.id = $id "
                "SET n.source_content_hash = 'deadbeef'",
                {"id": node["id"]},
            )

    await run_blocking_graph_io(
        _diverge_persisted_anchor,
        task_name="test.kg.provenance_drift.diverge_anchor",
    )

    report = await provenance_drift_report(board_id, "Entity")
    drifted = {d["node_id"]: d for d in report["drifted"]}
    assert node["id"] in drifted
    assert drifted[node["id"]]["reason"] == "content_changed"
    assert drifted[node["id"]]["persisted_hash"] == "deadbeef"
    assert drifted[node["id"]]["current_hash"] not in (None, "deadbeef")


async def test_s6_deleted_artifact_is_terminal_drift_and_readonly(
    drift_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    await _insert_spec_row(session_factory, board_id, spec_id)
    artifact_ref = f"spec:{spec_id}"

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref, "[MKG-B] Fonte deletada",
        content="conteudo",
    )
    await _delete_spec_row(session_factory, spec_id)

    before = await _graph_snapshot(board_id, artifact_ref)
    report = await provenance_drift_report(board_id)
    after = await _graph_snapshot(board_id, artifact_ref)

    node_id = before["id"]
    drifted = {d["node_id"]: d for d in report["drifted"]}
    assert node_id in drifted
    # AC6(b): deleted source = terminal drift, a reason of its own.
    assert drifted[node_id]["reason"] == "artifact_missing"
    assert report["drifted_by_reason"]["artifact_missing"] >= 1
    # AC6: read-only — the graph is byte-identical after the report.
    assert before == after


@pytest.mark.parametrize("card_type", ["normal", "test", "bug"])
async def test_s6_generic_card_ref_resolves_semantic_source_alias(
    drift_tempdir, monkeypatch, card_type
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    await _insert_spec_row(session_factory, board_id, spec_id)
    card_id = f"00000000-0000-4000-8000-{card_type.encode().hex()[:12]:0<12}"
    await _insert_card_row(
        session_factory, board_id, spec_id, card_id, card_type
    )
    artifact_ref = f"card:{card_id}"

    await _drive_with_provenance(
        session_factory,
        board_id,
        artifact_ref,
        f"[MKG-B] Alias de card {card_type}",
        content="conteudo",
        artifact_type="card",
    )

    report = await provenance_drift_report(board_id, "Entity")
    drifted = {d["node_id"]: d for d in report["drifted"]}
    node = await _graph_snapshot(board_id, artifact_ref)
    assert node["id"] not in drifted


def _configure_auditless_provenance_probe(
    monkeypatch,
    *,
    source_rows: list[dict],
) -> None:
    source_ref = "spec:auditless-artifact"

    class _Reader:
        def fetch(self, board_id):
            assert board_id == "board-auditless"
            return SimpleNamespace(complete=True, cause=None, rows=source_rows)

    class _AuditRepo:
        async def get_latest_for_artifact(
            self,
            board_id,
            artifact_id,
            *,
            artifact_type,
        ):
            assert board_id == "board-auditless"
            assert artifact_id == "auditless-artifact"
            assert artifact_type == "spec"
            return None

    registry = SimpleNamespace(
        require_board_source_reader=lambda: _Reader(),
        audit_repo=_AuditRepo(),
    )
    monkeypatch.setattr(
        provenance_module,
        "get_kg_registry",
        lambda: registry,
    )
    monkeypatch.setattr(
        provenance_module,
        "_fetch_provenance_nodes",
        lambda board_id, node_types: [{
            "node_id": "node-auditless",
            "node_type": "Entity",
            "source_artifact_ref": source_ref,
            "persisted_hash": "persisted-without-audit",
        }],
    )


async def test_missing_source_without_audit_is_artifact_missing(monkeypatch):
    _configure_auditless_provenance_probe(monkeypatch, source_rows=[])

    report = await provenance_drift_report("board-auditless", "Entity")

    assert report["checked_count"] == 1
    assert report["skipped_count"] == 0
    assert report["drifted"][0]["reason"] == "artifact_missing"
    assert report["drifted"][0]["current_hash"] is None
    assert report["drifted_by_reason"]["artifact_missing"] == 1
    assert report["drifted_by_reason"]["audit_missing"] == 0


async def test_live_source_without_audit_is_audit_missing(monkeypatch):
    _configure_auditless_provenance_probe(
        monkeypatch,
        source_rows=[{
            "source_ref": "spec:auditless-artifact",
            "artifact_type": "spec",
            "id": "auditless-artifact",
        }],
    )

    report = await provenance_drift_report("board-auditless", "Entity")

    assert report["checked_count"] == 1
    assert report["skipped_count"] == 0
    assert report["drifted"][0]["reason"] == "audit_missing"
    assert report["drifted"][0]["current_hash"] is None
    assert report["drifted_by_reason"]["artifact_missing"] == 0
    assert report["drifted_by_reason"]["audit_missing"] == 1


async def test_audit_lookup_is_scoped_by_type_when_ids_collide(monkeypatch):
    shared_id = "00000000-0000-4000-8000-000000000123"
    audit_calls: list[tuple[str, str, str | None]] = []

    class _Reader:
        def fetch(self, board_id):
            assert board_id == "board-shared-id"
            return SimpleNamespace(
                complete=True,
                cause=None,
                rows=[
                    {
                        "source_ref": f"spec:{shared_id}",
                        "artifact_type": "spec",
                        "id": shared_id,
                    },
                    {
                        "source_ref": f"task:{shared_id}",
                        "artifact_type": "task",
                        "id": shared_id,
                    },
                ],
            )

    class _AuditRepo:
        async def get_latest_for_artifact(
            self,
            board_id,
            artifact_id,
            *,
            artifact_type,
        ):
            audit_calls.append((board_id, artifact_id, artifact_type))
            return SimpleNamespace(
                artifact_type=artifact_type,
                content_hash=f"{artifact_type}-hash",
            )

    registry = SimpleNamespace(
        require_board_source_reader=lambda: _Reader(),
        audit_repo=_AuditRepo(),
    )
    monkeypatch.setattr(
        provenance_module,
        "get_kg_registry",
        lambda: registry,
    )
    monkeypatch.setattr(
        provenance_module,
        "_fetch_provenance_nodes",
        lambda board_id, node_types: [
            {
                "node_id": "node-spec",
                "node_type": "Entity",
                "source_artifact_ref": f"spec:{shared_id}",
                "persisted_hash": "spec-hash",
            },
            {
                "node_id": "node-task",
                "node_type": "Entity",
                "source_artifact_ref": f"task:{shared_id}",
                "persisted_hash": "task-hash",
            },
        ],
    )

    report = await provenance_drift_report("board-shared-id", "Entity")

    # A cache/lookup keyed only by id would call the repository once and could
    # reuse the Spec audit for the Task node. Both typed audits are recovered.
    assert audit_calls == [
        ("board-shared-id", shared_id, "spec"),
        ("board-shared-id", shared_id, "task"),
    ]
    assert report["checked_count"] == 2
    assert report["drifted_count"] == 0
    assert report["drifted"] == []


@pytest.mark.parametrize(
    "child_suffix",
    [
        "fr:fr_login",
        "tr:tr_audit",
        "ac:ac_login",
        "business_rule:br_lockout",
        "test_scenario:ts_login",
        "api_contract:api_login",
        "integration_requirement:ir_events",
        "observability_requirement:or_latency",
        "decision:dec_queue",
        "learning:learn_retry",
        "alternative:alt_polling",
        "assumption:asm_volume",
    ],
)
async def test_child_source_refs_use_parent_source_and_audit(
    monkeypatch,
    child_suffix,
):
    source_id = "parent-spec"
    parent_ref = f"spec:{source_id}"
    child_ref = f"{parent_ref}:{child_suffix}"

    class _Reader:
        def fetch(self, board_id):
            return SimpleNamespace(
                complete=True,
                cause=None,
                rows=[
                    {
                        "source_ref": parent_ref,
                        "artifact_type": "spec",
                        "id": source_id,
                    }
                ],
            )

    class _AuditRepo:
        async def get_latest_for_artifact(
            self,
            board_id,
            artifact_id,
            *,
            artifact_type,
        ):
            assert artifact_id == source_id
            assert artifact_type == "spec"
            return SimpleNamespace(
                artifact_type="spec",
                content_hash="parent-hash",
            )

    registry = SimpleNamespace(
        require_board_source_reader=lambda: _Reader(),
        audit_repo=_AuditRepo(),
    )
    monkeypatch.setattr(provenance_module, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        provenance_module,
        "_fetch_provenance_nodes",
        lambda board_id, node_types: [
            {
                "node_id": f"node-{child_suffix}",
                "node_type": "Decision",
                "source_artifact_ref": child_ref,
                "persisted_hash": "parent-hash",
            }
        ],
    )

    report = await provenance_drift_report("board-child-ref", "Decision")

    assert report["checked_count"] == 1
    assert report["drifted_count"] == 0


async def test_s6_unknown_node_type_rejected(drift_tempdir, monkeypatch):
    await _bootstrap_test_board(monkeypatch)
    with pytest.raises(ValueError):
        await provenance_drift_report("board", "NotAType")


async def test_s6_mcp_tool_registered_within_budget():
    import importlib

    mod = importlib.import_module("okto_pulse.core.mcp.server")
    tools = await mod.mcp.get_tools()
    tool = tools.get("okto_pulse_kg_provenance_drift")
    assert tool is not None
    # TR do budget R1.1: descrição ≤900 chars.
    assert len(tool.description or "") <= 900
