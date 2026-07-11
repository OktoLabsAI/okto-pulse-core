from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from okto_pulse.community.api.architecture import _http_error_from_value
from okto_pulse.community.api.cards import _resource_gate_detail as _card_resource_gate_detail
from okto_pulse.community.api.specs import _resource_gate_detail as _spec_resource_gate_detail
from okto_pulse.core.mcp.server import (
    _mcp_architecture_error,
    _resource_gate_error_response,
)
from sqlalchemy_test_models import ArchitectureDesign, Board, Ideation
from okto_pulse.core.services.architecture import (
    ArchitectureFindingGate,
    ArchitectureDesignRepository,
    ArchitectureFindingRunStore,
    ArchitectureWarningAcknowledgementRequired,
    TopologyWarningEngine,
)
from okto_pulse.core.services.architecture_observability import (
    get_architecture_metric_labels,
)
from okto_pulse.core.services.main import SprintService
from okto_pulse.core.services.resource_gate import ResourceGateViolation

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "okto_pulse" / "core"


def _read_core_source(relative_path: str) -> str:
    return (SRC / relative_path).read_text(encoding="utf-8")


def test_architecture_finding_gate_consumes_persisted_runs_not_topology_critic() -> None:
    source = inspect.getsource(ArchitectureFindingGate.evaluate)

    assert "ArchitectureFindingRunStore" in source
    assert "list_findings" in source

    forbidden_tokens = (
        "TopologyWarningEngine",
        "critique_payload",
        "validate_payload",
        "_critique_for_save",
        "ArchitectureWarningRecord(",
        "structured_warnings",
    )
    for token in forbidden_tokens:
        assert token not in source, (
            "ArchitectureFindingGate must consume persisted finding lifecycle state, "
            f"not re-run or rebuild critic output via {token!r}."
        )


@pytest.mark.asyncio
async def test_architecture_finding_gate_runtime_follows_store_when_payload_would_diverge(
    db_factory,
    monkeypatch,
) -> None:
    board_id = "board-negative-wiring"
    ideation_id = "ideation-negative-wiring"
    design_id = "design-negative-wiring"
    actor = {
        "actor_type": "agent",
        "actor_id": "agent-negative-wiring",
        "actor_name": "Negative Wiring Guard",
    }
    stored_warning = {
        "code": "orphan_entity",
        "severity": "warning",
        "message": "Stored finding says this entity is orphaned.",
        "suggested_fix": "Connect the entity in the diagram.",
        "diagram_id": "diag-runtime",
        "diagram_type": "runtime",
        "entity_id": "entity-from-store",
        "path": "$.entities[0]",
    }

    def fail_if_topology_analyzer_is_called(*_args, **_kwargs):
        raise AssertionError("ArchitectureFindingGate must not call TopologyWarningEngine.evaluate")

    def fail_if_critique_payload_is_called(*_args, **_kwargs):
        raise AssertionError("ArchitectureFindingGate must not call ArchitectureDesignRepository.critique_payload")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Negative wiring board", owner_id=actor["actor_id"]))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Negative wiring ideation",
                created_by=actor["actor_id"],
            )
        )
        db.add(
            ArchitectureDesign(
                id=design_id,
                board_id=board_id,
                parent_type="ideation",
                ideation_id=ideation_id,
                title="Clean-looking architecture payload",
                global_description=(
                    "The stored finding intentionally diverges from the current payload; "
                    "gate decisions must follow the run store, not recomputation."
                ),
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=actor["actor_id"],
            )
        )
        await db.flush()

        store = ArchitectureFindingRunStore(db)
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=design_id,
            design_version=1,
            critic_run_id="stored-active-run",
            actor=actor,
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[stored_warning],
        )

        monkeypatch.setattr(
            TopologyWarningEngine,
            "evaluate",
            fail_if_topology_analyzer_is_called,
        )
        monkeypatch.setattr(
            ArchitectureDesignRepository,
            "critique_payload",
            fail_if_critique_payload_is_called,
        )

        active_result = await ArchitectureFindingGate(db).evaluate(
            board_id=board_id,
            owner_type="ideation",
            owner_id=ideation_id,
            architecture_refs=[
                {
                    "id": design_id,
                    "title": "Clean-looking architecture payload",
                    "source_entity_type": "ideation",
                    "source_entity_id": ideation_id,
                }
            ],
        )

        active_findings = active_result["architecture_findings"]
        assert active_result["allowed"] is False
        assert active_findings["blocking"] is True
        assert active_findings["active_count"] == 1
        assert active_findings["top_remediation"][0]["code"] == "orphan_entity"
        assert active_findings["top_remediation"][0]["target_ref"] == "entity-from-store"

        await store.upsert_latest_run(
            board_id=board_id,
            design_id=design_id,
            design_version=2,
            critic_run_id="stored-clean-run",
            actor=actor,
            validator_summary={"valid": True, "issues": []},
            structured_warnings=[],
        )

        clean_result = await ArchitectureFindingGate(db).evaluate(
            board_id=board_id,
            owner_type="ideation",
            owner_id=ideation_id,
            architecture_refs=[{"id": design_id, "title": "Clean-looking architecture payload"}],
        )

        clean_findings = clean_result["architecture_findings"]
        assert clean_result["allowed"] is True
        assert clean_findings["blocking"] is False
        assert clean_findings["active_count"] == 0
        assert clean_findings["resolved_count"] == 1


def test_resource_gate_is_the_only_done_blocking_wiring_surface() -> None:
    resource_gate_source = _read_core_source("services/resource_gate.py")
    resource_gate_impl_source = _read_core_source(
        "repositories/sqlalchemy/resource_gate_service.py"
    )
    transition_service_source = _read_core_source("services/main.py")

    # Robust against one-line vs multi-line import formatting: the wiring must
    # import ArchitectureFindingGate from the architecture service. The usage
    # assertion below pins the actual gate call, so behavior stays guarded.
    assert "from okto_pulse.core.services.architecture import" in resource_gate_impl_source
    assert "ArchitectureFindingGate" in resource_gate_impl_source
    assert "ArchitectureFindingGate(self.db).evaluate" in resource_gate_impl_source
    for token in (
        "TopologyWarningEngine",
        "ArchitectureWarningRecord",
        "stable_architecture_finding_key",
        "critique_payload",
        "structured_warnings",
    ):
        assert token not in resource_gate_source, (
            "ResourceGateService must delegate to ArchitectureFindingGate instead "
            f"of deriving architecture topology or warning lifecycle itself ({token})."
        )
        assert token not in transition_service_source, (
            "Status transition services must stay behind ResourceGateService and "
            f"must not re-run or classify architecture topology directly via {token!r}."
        )
    assert "ArchitectureFindingGate" not in transition_service_source, (
        "Status transition services must use ResourceGateService as the single gate "
        "integration seam instead of constructing ArchitectureFindingGate directly."
    )

    allowed_blocker_paths = {
        SRC / "repositories" / "sqlalchemy" / "resource_gate_service.py",
        SRC / "services" / "architecture_observability.py",
    }
    blocker_token = "architecture_findings_block_done"
    offenders: list[str] = []
    for path in SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if blocker_token in text and path not in allowed_blocker_paths:
            offenders.append(str(path.relative_to(ROOT)))
    assert offenders == [], (
        "architecture_findings_block_done must be produced by ResourceGateService "
        f"and observed by the metric sink only; found duplicate wiring in {offenders}."
    )


def test_rest_and_mcp_acknowledgement_required_payloads_share_source_contract() -> None:
    warning = {
        "finding_key": "finding-key-1",
        "code": "orphan_entity",
        "severity": "warning",
        "message": "Entity is not connected.",
        "normalized_target_kind": "entity",
        "target_ref": "entity-authz",
        "path": "$.entities[0]",
    }
    exc = ArchitectureWarningAcknowledgementRequired(
        design_id="design-1",
        warning_keys=["finding-key-1"],
        warnings=[warning],
    )

    expected = exc.to_payload()
    http_error = _http_error_from_value(exc)
    mcp_payload = json.loads(_mcp_architecture_error(exc))

    assert http_error.status_code == 409
    assert http_error.detail == expected
    assert mcp_payload == {"success": False, **expected}
    assert http_error.detail["structured_warnings"] == mcp_payload["structured_warnings"]
    assert http_error.detail["warning_keys"] == mcp_payload["warning_keys"]
    assert http_error.detail["structured_warnings"][0] == {
        "finding_key": "finding-key-1",
        "code": "orphan_entity",
        "severity": "warning",
        "message": "Entity is not connected.",
        "normalized_target_kind": "entity",
        "target_ref": "entity-authz",
        "path": "$.entities[0]",
    }
    assert set(expected) == {
        "error",
        "code",
        "design_id",
        "warning_keys",
        "structured_warnings",
        "message",
    }


def test_rest_and_mcp_done_blocker_payloads_preserve_same_remediation_details() -> None:
    details = {
        "board_id": "board-1",
        "entity_type": "card",
        "entity_id": "card-1",
        "phase": "card_done",
        "architecture_findings": {
            "active_count": 1,
            "top_remediation": [
                {
                    "code": "uncovered_interface",
                    "normalized_target_kind": "element",
                    "target_ref": "edge-1",
                    "path": "$.diagrams[0].elements[1]",
                    "remediation": "Update the Architecture Design.",
                }
            ],
        },
    }
    exc = ResourceGateViolation(
        "architecture_findings_block_done",
        "Cannot complete card: active Architecture Design finding(s) remain.",
        details=details,
    )

    card_payload = _card_resource_gate_detail(exc)
    spec_payload = _spec_resource_gate_detail(exc)
    mcp_payload = json.loads(_resource_gate_error_response(exc))

    assert card_payload == {
        "error": exc.code,
        "message": str(exc),
        "details": details,
    }
    assert spec_payload == card_payload
    assert mcp_payload == {
        "success": False,
        "error": str(exc),
        "code": exc.code,
        "details": details,
    }
    assert card_payload["details"]["architecture_findings"] == mcp_payload["details"]["architecture_findings"]
    assert (
        card_payload["details"]["architecture_findings"]["top_remediation"][0]
        == mcp_payload["details"]["architecture_findings"]["top_remediation"][0]
    )
    assert card_payload["details"]["architecture_findings"]["top_remediation"][0] == {
        "code": "uncovered_interface",
        "normalized_target_kind": "element",
        "target_ref": "edge-1",
        "path": "$.diagrams[0].elements[1]",
        "remediation": "Update the Architecture Design.",
    }


def test_sprint_close_remains_out_of_scope_for_architecture_finding_gate_v1() -> None:
    source = inspect.getsource(SprintService.move_sprint)

    for token in (
        "ArchitectureFindingGate",
        "ResourceGateService",
        "validate_or_raise_entity_completion",
        "validate_or_raise_spec_resource_task_coverage",
        "architecture_findings_block_done",
    ):
        assert token not in source, (
            "Sprint-level Architecture Finding Gate is explicitly out of scope "
            f"for v1, but SprintService.move_sprint references {token!r}."
        )


def test_architecture_metric_labels_exclude_raw_finding_or_diagram_fields() -> None:
    labels = set(get_architecture_metric_labels())

    forbidden_exact = {
        "diagram_id",
        "diagram_payload",
        "finding_message",
        "message",
        "path",
        "payload",
        "structured_warnings",
        "target_ref",
        "title",
    }
    assert labels.isdisjoint(forbidden_exact)
    for label in labels:
        lowered = label.lower()
        assert "payload" not in lowered
        assert "diagram" not in lowered
        assert "message" not in lowered
        assert "path" not in lowered
        assert "target" not in lowered
