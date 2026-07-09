from __future__ import annotations

import uuid
from pathlib import Path

from okto_pulse.core.application.boundary.rebuild_audit_storage_gate import (
    rebuild_audit_storage_fallback_ledger,
    run_rebuild_audit_storage_gate,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
from okto_pulse.core.kg.providers.testing.memory_rebuild_audit_storage import (
    InMemoryRebuildAuditArtifactStore,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitivePendingMarker,
    ConfirmationConsumptionAuditRecorder,
    KGRebuiltEventPublisher,
    compute_cognitive_item_id,
)


def _event_payload(board_id: str) -> dict[str, object]:
    return {
        "board_id": board_id,
        "previous_kg_generation_id": None,
        "kg_generation_id": str(uuid.uuid4()),
        "triggered_by": "pytest",
        "started_at": "2026-07-03T00:00:00+00:00",
        "finished_at": "2026-07-03T00:00:01+00:00",
        "status": "completed",
        "counts": {"nodes": 1},
        "report_ref": "report:pytest",
    }


def test_af27_rebuild_audit_consumers_use_in_memory_store_without_base_dir() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    board_id = "board-af27"
    generation_id = str(uuid.uuid4())

    publisher = KGRebuiltEventPublisher(artifact_store=store)
    published = publisher.publish(event_payload=_event_payload(board_id))
    assert published.accepted is True
    assert published.audit_ref
    assert store.list_json(RebuildAuditKey("event_audit", board_id))

    item_store = CognitiveConsolidationItemStore(artifact_store=store)
    materialized = item_store.materialize_from_marker(
        board_id=board_id,
        kg_generation_id=generation_id,
        event_ref=published.event_ref or "evt",
        source_set=[
            {
                "artifact_type": "spec",
                "source_ref": "spec:af27",
                "content_hash": "hash-v1",
            }
        ],
    )
    assert materialized.record_ref.startswith("rebuild-audit:/")
    assert item_store.record_exists(board_id, generation_id)
    assert item_store.latest_generation(board_id) == generation_id

    item_id = compute_cognitive_item_id(board_id, generation_id, "spec:af27")
    updated = item_store.update_item(
        board_id=board_id,
        kg_generation_id=generation_id,
        item_id=item_id,
        new_status="skipped",
        updated_by_agent_id="pytest",
        reason="contract test",
    )
    assert updated is not None
    assert updated.status == "skipped"

    marker = CognitivePendingMarker(artifact_store=store)
    mark_result = marker.mark_for_generation(
        board_id=board_id,
        kg_generation_id=str(uuid.uuid4()),
        source_set=[{"artifact_type": "decision", "source_ref": "decision:1"}],
        event_ref="evt:marker",
    )
    assert mark_result.record_ref and mark_result.record_ref.startswith(
        "rebuild-audit:/"
    )

    recorder = ConfirmationConsumptionAuditRecorder(artifact_store=store)
    audit_result = recorder.record(
        board_id=board_id,
        operation="rebuild",
        outcome="consumed",
        reason="pytest",
        actor_ref="agent:pytest",
    )
    assert audit_result.audit_ref
    assert store.list_json(RebuildAuditKey("confirmation_audit", board_id))


def test_af27_base_dir_path_gate_current_tree_matches_inventory() -> None:
    core_root = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
    assert run_rebuild_audit_storage_gate(core_root, enforce_stale_ledger=True) == ()

    ledger = rebuild_audit_storage_fallback_ledger()
    assert ledger
    assert all(entry.owner for entry in ledger)
    assert all(entry.classification for entry in ledger)
    assert all(entry.reason for entry in ledger)
    assert all(entry.removal_criterion for entry in ledger)
    assert {
        "legacy_compat_injection",
        "stress_chaos_evidence",
        "static_bundled_resource_path",
    } <= {entry.classification for entry in ledger}


def test_af27_base_dir_path_gate_catches_synthetic_durable_store(tmp_path: Path) -> None:
    core_root = tmp_path / "core"
    fixture = core_root / "kg"
    fixture.mkdir(parents=True)
    target = fixture / "new_durable_store.py"
    target.write_text(
        "from pathlib import Path\n"
        "class NewDurableStore:\n"
        "    def __init__(self, base_dir: Path) -> None:\n"
        "        self.base_dir = base_dir\n",
        encoding="utf-8",
    )

    violations = run_rebuild_audit_storage_gate(core_root)

    assert [(v.rule, v.path, v.symbol) for v in violations] == [
        (
            "base_dir_path_consumer",
            "kg/new_durable_store.py",
            "NewDurableStore.__init__.base_dir",
        )
    ]


def test_af27_base_dir_gate_is_occurrence_scoped_not_file_scoped(
    tmp_path: Path,
) -> None:
    core_root = tmp_path / "core"
    historical_file = core_root / "kg" / "rebuild_audit.py"
    historical_file.parent.mkdir(parents=True)
    historical_file.write_text(
        "from pathlib import Path\n"
        "class RogueStore:\n"
        "    base_dir: Path | None = None\n",
        encoding="utf-8",
    )

    violations = run_rebuild_audit_storage_gate(
        core_root,
        enforce_stale_ledger=False,
    )

    assert [(v.rule, v.path, v.symbol) for v in violations] == [
        ("base_dir_path_consumer", "kg/rebuild_audit.py", "RogueStore.base_dir")
    ]
