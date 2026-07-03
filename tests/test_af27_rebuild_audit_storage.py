from __future__ import annotations

import ast
import uuid
from pathlib import Path

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


_ALLOWLISTED_BASE_DIR_PATH_CONSUMERS = {
    "candidate_decision_store.py",
    "contingency.py",
    "global_discovery_reindex.py",
    "quarantine.py",
    "rebuild_audit.py",
    "rebuild_confirmation.py",
    "rebuild_generation.py",
    "rebuild_report.py",
    "rebuild_service.py",
    "rebuild_sources.py",
    "single_writer_lock.py",
    "stress_chaos_executor.py",
    "stress_runner.py",
}


def _path_store_offenders(root: Path) -> set[str]:
    offenders: set[str] = set()
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.arg) and node.arg == "base_dir":
                annotation = ast.unparse(node.annotation) if node.annotation else ""
                if "Path" in annotation:
                    offenders.add(path.name)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.target.id == "base_dir":
                    annotation = ast.unparse(node.annotation)
                    if "Path" in annotation:
                        offenders.add(path.name)
    return offenders


def test_af27_base_dir_path_gate_current_tree_matches_inventory() -> None:
    core_kg = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core" / "kg"
    offenders = _path_store_offenders(core_kg)
    assert offenders <= _ALLOWLISTED_BASE_DIR_PATH_CONSUMERS


def test_af27_base_dir_path_gate_catches_synthetic_durable_store(tmp_path: Path) -> None:
    fixture = tmp_path / "core" / "kg"
    fixture.mkdir(parents=True)
    (fixture / "new_durable_store.py").write_text(
        "from pathlib import Path\n"
        "class NewDurableStore:\n"
        "    def __init__(self, base_dir: Path) -> None:\n"
        "        self.base_dir = base_dir\n",
        encoding="utf-8",
    )
    assert _path_store_offenders(fixture) == {"new_durable_store.py"}
