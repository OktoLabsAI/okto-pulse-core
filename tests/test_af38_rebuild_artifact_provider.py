from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.composition import RuntimeProviderMissing
from okto_pulse.core.kg.candidate_decision_store import CandidateDecisionStore
from okto_pulse.core.kg.contingency import KGStorageBackendContingency
from okto_pulse.core.kg.global_discovery_reindex import (
    GlobalDiscoveryReindexStatusStore,
    ReindexReason,
    ReindexStatus,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
from okto_pulse.core.kg.providers.testing.memory_rebuild_audit_storage import (
    InMemoryRebuildAuditArtifactStore,
)
from okto_pulse.core.kg.rebuild_audit import (
    ConfirmationConsumptionAuditRecorder,
    KGRebuiltEventPublisher,
)
from okto_pulse.core.kg.rebuild_confirmation import RebuildConfirmationStore
from okto_pulse.core.kg.rebuild_generation import (
    PromotionOutcome,
    RebuildAuditKGGenerationRepository,
    generate_kg_generation_id,
)
from okto_pulse.core.kg.rebuild_report import (
    RebuildReportPayload,
    RebuildReportStore,
    RebuildReportSummary,
    ReportPersistOutcome,
)
from okto_pulse.core.kg.rebuild_sources import (
    KGRebuildSourceManifest,
    RebuildSourceRow,
    RebuildSourceSet,
    RevalidationResult,
    SourceSetRevalidation,
    _append_spec_manifest_rebaseline_audit,
    read_spec_manifest_rebaseline_audit,
)


def _event_payload(board_id: str, generation_id: str) -> dict[str, Any]:
    return {
        "board_id": board_id,
        "previous_kg_generation_id": None,
        "kg_generation_id": generation_id,
        "triggered_by": "pytest",
        "started_at": "2026-07-08T09:00:00+00:00",
        "finished_at": "2026-07-08T09:00:01+00:00",
        "status": "completed",
        "counts": {"nodes": 1},
        "report_ref": f"rebuild-audit:/{board_id}/report",
    }


def _report_payload(board_id: str, generation_id: str) -> RebuildReportPayload:
    return RebuildReportPayload(
        summary=RebuildReportSummary(
            board_id=board_id,
            run_id="run-af38",
            status="completed",
            started_at="2026-07-08T09:00:00+00:00",
            finished_at="2026-07-08T09:00:01+00:00",
            counts={"nodes": 1},
            kg_generation_id=generation_id,
        ),
        hashes={"source": "a" * 64},
        source_refs=("spec:af38",),
    )


def _source_set(board_id: str) -> RebuildSourceSet:
    row = RebuildSourceRow(
        artifact_type="spec",
        source_ref="spec:af38",
        source_version="1",
        content_hash="b" * 64,
        created_at=datetime.now(timezone.utc).isoformat(),
        id="af38",
    )
    return RebuildSourceSet(
        board_id=board_id,
        sources=(row,),
        skipped_cancelled_count=0,
        has_non_deterministic_inputs=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def test_af38_default_rebuild_artifact_consumers_use_registry_provider() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    configure_test_kg_registry(
        graph_provider="inmemory",
        rebuild_audit_artifact_store=store,
    )
    board_id = "board-af38-provider"
    generation_id = generate_kg_generation_id()

    published = KGRebuiltEventPublisher().publish(
        event_payload=_event_payload(board_id, generation_id)
    )
    assert published.accepted is True
    assert published.audit_ref and published.audit_ref.startswith("rebuild-audit:/")

    report = RebuildReportStore().persist(
        payload=_report_payload(board_id, generation_id)
    )
    assert report.outcome == ReportPersistOutcome.STORED.value
    assert report.report_ref and report.report_ref.startswith("rebuild-audit:/")

    promoted = RebuildAuditKGGenerationRepository().promote_current(
        board_id=board_id,
        previous_kg_generation_id=None,
        kg_generation_id=generation_id,
        report_ref=report.report_ref,
        status="completed",
        structural_hash="structural-hash",
        source_hash="source-hash",
        promoted_by="pytest",
        run_id="run-af38",
    )
    assert promoted.outcome == PromotionOutcome.PROMOTED.value
    assert promoted.history_ref and promoted.history_ref.startswith("rebuild-audit:/")

    token = RebuildConfirmationStore().issue(
        board_id=board_id,
        actor_id="agent-af38",
        operation="rebuild",
        preflight_hash="c" * 64,
        manifest_ref="rebuild_manifest_provider",
    )
    assert store.exists(RebuildAuditKey("confirmation_token", "_global", artifact_id=token.confirmation_id))

    candidate = CandidateDecisionStore().record(
        board_id=board_id,
        source_ref="spec:af38",
        source_generation_id=generation_id,
        consolidation_session_id="sess-af38",
        title="AF38 provider default",
        rationale="Default store resolution uses the registry provider.",
        created_by_agent_id="agent-af38",
    )
    assert store.exists(RebuildAuditKey("candidate_decision", board_id, artifact_id=candidate.candidate_id))

    manifest = KGRebuildSourceManifest().build(
        source_set=_source_set(board_id),
        preflight_hash="d" * 64,
    )
    assert store.exists(RebuildAuditKey("source_manifest", "_global", artifact_id=manifest.manifest_ref))

    _append_spec_manifest_rebaseline_audit(
        None,
        board_id=board_id,
        manifest_ref=manifest.manifest_ref,
        result=RevalidationResult(
            outcome=SourceSetRevalidation.REBASELINE,
            rebaselined_source_refs=("spec:af38",),
            from_manifest_schema_version=1,
            to_manifest_schema_version=2,
        ),
        recorded_at=datetime.now(timezone.utc).isoformat(),
    )
    assert read_spec_manifest_rebaseline_audit(None, board_id)[0]["manifest_ref"] == manifest.manifest_ref

    audit = ConfirmationConsumptionAuditRecorder().record(
        board_id=board_id,
        operation="rebuild",
        outcome="consumed",
        reason="pytest",
        actor_ref="agent:af38",
    )
    assert audit.audit_ref and audit.audit_ref.startswith("rebuild-audit:/")


def test_af38_missing_rebuild_artifact_provider_fails_closed() -> None:
    configure_test_kg_registry(
        graph_provider="inmemory",
        rebuild_audit_artifact_store=None,
    )

    with pytest.raises(RuntimeProviderMissing) as exc_info:
        RebuildReportStore()

    assert exc_info.value.provider_key == "rebuild_audit_artifact_store"


def test_af38_reindex_and_contingency_use_registry_provider() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    configure_test_kg_registry(
        graph_provider="inmemory",
        rebuild_audit_artifact_store=store,
    )
    board_id = "board-af38-narrow"
    generation_id = generate_kg_generation_id()

    reindex_store = GlobalDiscoveryReindexStatusStore()
    result = reindex_store.record(
        board_id=board_id,
        kg_generation_id=generation_id,
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
        status=ReindexStatus.REINDEX_PENDING.value,
        job_ref="job-af38",
    )

    assert result.record_ref is not None
    assert result.record_ref.startswith("rebuild-audit:/")
    reindex_key = RebuildAuditKey(
        namespace="global_discovery_reindex",
        board_id=board_id,
        kg_generation_id=generation_id,
    )
    assert store.read_json(reindex_key)["job_ref"] == "job-af38"
    assert reindex_store.latest_for_board(board_id)["kg_generation_id"] == generation_id

    contingency = KGStorageBackendContingency(
        boot_software_version="0.2.5",
        allow_unverified_quarantine_ids=True,
    )
    response = contingency.prepare(
        board_id=board_id,
        corruption_timeline_ref="rebuild-audit:/timeline/af38",
        quarantine_ids=["q_af38"],
        software_version="0.2.5",
    )

    assert response.contingency_ref.startswith("rebuild-audit:/")
    rows = store.list_json(RebuildAuditKey(namespace="contingency", board_id=board_id))
    assert [row["quarantine_ids"] for row in rows] == [["q_af38"]]


def test_af38_missing_provider_fails_closed_for_reindex_and_contingency() -> None:
    configure_test_kg_registry(
        graph_provider="inmemory",
        rebuild_audit_artifact_store=None,
    )

    with pytest.raises(RuntimeProviderMissing) as reindex_exc:
        GlobalDiscoveryReindexStatusStore()
    assert reindex_exc.value.provider_key == "rebuild_audit_artifact_store"

    with pytest.raises(RuntimeProviderMissing) as contingency_exc:
        KGStorageBackendContingency(
            boot_software_version="0.2.5",
            allow_unverified_quarantine_ids=True,
        )
    assert contingency_exc.value.provider_key == "rebuild_audit_artifact_store"
