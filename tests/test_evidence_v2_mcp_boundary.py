"""Transport-neutral Evidence V2 execution and status boundaries."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.spec_crud import (
    ExecuteTestScenarioEvidenceCommand,
    ExecuteTestScenarioEvidenceUseCase,
    SetTestScenarioStatusCommand,
    SetTestScenarioStatusUseCase,
)
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.ports.test_evidence import (
    TestEvidenceExecutionResult as EvidenceExecutionResult,
    TestEvidenceWriteVerification as EvidenceWriteVerification,
    register_test_evidence_execution_issuer,
    register_test_evidence_write_verifier,
    reset_test_evidence_execution_issuer_for_tests,
    reset_test_evidence_write_verifier_for_tests,
)
from okto_pulse.core.services.test_scenario_lifecycle import (
    compute_test_scenario_semantic_sha256,
)
from okto_pulse.core.mcp import server as mcp_server


def _evidence_execute_permissions() -> PermissionSet:
    return PermissionSet(
        {
            "spec": {
                "tests": {
                    "execute": True,
                    "update_status": True,
                }
            }
        }
    )


class _Issuer:
    def __init__(self, evidence):
        self.evidence = evidence
        self.calls = []

    async def execute(self, request):
        self.calls.append(request)
        return EvidenceExecutionResult(self.evidence)


class _Verifier:
    def __init__(self, result: EvidenceWriteVerification) -> None:
        self.result = result
        self.calls = []

    def verify(self, **request):  # noqa: ANN003, ANN201
        self.calls.append(request)
        return self.result


class _SpecService:
    def __init__(self) -> None:
        self.calls = []
        self.spec = SimpleNamespace(
            id="spec-1",
            board_id="board-1",
            status=SpecStatus.DRAFT,
            test_scenarios=[{"id": "scenario-1", "status": "ready"}],
        )

    async def get_spec(self, spec_id):  # noqa: ANN001, ANN201
        return self.spec if spec_id == self.spec.id else None

    async def set_test_scenario_status(self, *args):  # noqa: ANN002, ANN201
        self.calls.append(args)
        return {
            "scenario_id": args[2],
            "old_status": "ready",
            "new_status": args[3],
            "evidence_provided": True,
            "evidence_gate_skipped": False,
            "evidence_verification_status": "verified",
        }


class _BoardsService:
    def __init__(self, board=None) -> None:  # noqa: ANN001
        self.board = board
        self.calls = []

    async def get_board(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        self.calls.append((args, kwargs))
        return self.board


@pytest.fixture(autouse=True)
def _reset_ports():
    reset_test_evidence_execution_issuer_for_tests()
    reset_test_evidence_write_verifier_for_tests()
    yield
    reset_test_evidence_execution_issuer_for_tests()
    reset_test_evidence_write_verifier_for_tests()


@pytest.mark.asyncio
async def test_execution_use_case_binds_receipt_to_full_scope() -> None:
    evidence = {"execution_receipt": "opaque"}
    issuer = _Issuer(evidence)
    verifier = _Verifier(EvidenceWriteVerification(True))
    register_test_evidence_execution_issuer(issuer)
    register_test_evidence_write_verifier(verifier)
    specs = _SpecService()
    uow = SimpleNamespace(services=SimpleNamespace(specs=specs))

    result = await ExecuteTestScenarioEvidenceUseCase().execute(
        ExecuteTestScenarioEvidenceCommand(
            "spec-1", "scenario-1", "passed", "about.json"
        ),
        actor=ActorContext("agent", "mcp", board_id="board-1"),
        uow=uow,
    )

    assert result.evidence == evidence
    assert issuer.calls[0].board_id == "board-1"
    assert issuer.calls[0].spec_id == "spec-1"
    semantic_digest = compute_test_scenario_semantic_sha256(
        board_id="board-1",
        spec_id="spec-1",
        scenario=specs.spec.test_scenarios[0],
        acceptance_criteria=[],
    )
    assert issuer.calls[0].scenario_sha256 == semantic_digest
    assert issuer.calls[0].manifest_ref == "about.json"
    assert issuer.calls[0].inline_replay is None
    assert verifier.calls == [
        {
            "board_id": "board-1",
            "spec_id": "spec-1",
            "scenario_id": "scenario-1",
            "scenario_sha256": semantic_digest,
            "status": "passed",
            "actor_id": "agent",
            "evidence": evidence,
        }
    ]


@pytest.mark.asyncio
async def test_rest_execution_rejects_foreign_spec_before_trusted_runtime() -> None:
    issuer = _Issuer({"execution_receipt": "must-not-be-issued"})
    verifier = _Verifier(EvidenceWriteVerification(True))
    register_test_evidence_execution_issuer(issuer)
    register_test_evidence_write_verifier(verifier)
    boards = _BoardsService(board=None)
    uow = SimpleNamespace(
        services=SimpleNamespace(specs=_SpecService(), boards=boards),
        boards=SimpleNamespace(get=boards.get_board),
    )

    with pytest.raises(EntityNotFoundError):
        await ExecuteTestScenarioEvidenceUseCase().execute(
            ExecuteTestScenarioEvidenceCommand(
                "spec-1", "scenario-1", "passed", "about.json"
            ),
            actor=ActorContext(
                "foreign-user",
                "rest",
                realm_id="local",
                permissions=_evidence_execute_permissions(),
            ),
            uow=uow,
        )

    assert boards.calls
    assert issuer.calls == []
    assert verifier.calls == []


@pytest.mark.asyncio
async def test_execution_use_case_passes_inline_replay_as_opaque_transport() -> None:
    evidence = {"execution_receipt": "opaque"}
    issuer = _Issuer(evidence)
    register_test_evidence_execution_issuer(issuer)
    register_test_evidence_write_verifier(_Verifier(EvidenceWriteVerification(True)))
    replay = '{"steps":[{"path":"/health"}]}'
    uow = SimpleNamespace(services=SimpleNamespace(specs=_SpecService()))

    await ExecuteTestScenarioEvidenceUseCase().execute(
        ExecuteTestScenarioEvidenceCommand(
            "spec-1", "scenario-1", "passed", inline_replay=replay
        ),
        actor=ActorContext("agent", "mcp", board_id="board-1"),
        uow=uow,
    )

    assert issuer.calls[0].manifest_ref is None
    assert issuer.calls[0].inline_replay == replay


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("manifest_ref", "inline_replay", "reason"),
    [
        ("", "", "replay_source_required"),
        ("about.json", "{}", "replay_sources_mutually_exclusive"),
    ],
)
async def test_execution_use_case_requires_exactly_one_replay_source(
    manifest_ref: str,
    inline_replay: str,
    reason: str,
) -> None:
    uow = SimpleNamespace(services=SimpleNamespace(specs=_SpecService()))
    with pytest.raises(ValueError, match=reason):
        await ExecuteTestScenarioEvidenceUseCase().execute(
            ExecuteTestScenarioEvidenceCommand(
                "spec-1",
                "scenario-1",
                "passed",
                manifest_ref,
                inline_replay,
            ),
            actor=ActorContext("agent", "mcp", board_id="board-1"),
            uow=uow,
        )


@pytest.mark.asyncio
async def test_execution_runtime_and_verifier_are_required() -> None:
    specs = _SpecService()
    uow = SimpleNamespace(services=SimpleNamespace(specs=specs))
    with pytest.raises(ValueError, match="trusted_runtime_not_configured"):
        await ExecuteTestScenarioEvidenceUseCase().execute(
            ExecuteTestScenarioEvidenceCommand(
                "spec-1", "scenario-1", "passed", "about.json"
            ),
            actor=ActorContext("agent", "mcp", board_id="board-1"),
            uow=uow,
        )


@pytest.mark.asyncio
async def test_issuer_output_is_reverified_before_return() -> None:
    register_test_evidence_execution_issuer(_Issuer({"execution_receipt": "forged"}))
    register_test_evidence_write_verifier(
        _Verifier(
            EvidenceWriteVerification(False, ("evidence_v2.receipt_not_registered",))
        )
    )
    uow = SimpleNamespace(services=SimpleNamespace(specs=_SpecService()))
    with pytest.raises(ValueError, match="receipt_not_registered"):
        await ExecuteTestScenarioEvidenceUseCase().execute(
            ExecuteTestScenarioEvidenceCommand(
                "spec-1", "scenario-1", "passed", "about.json"
            ),
            actor=ActorContext("agent", "mcp", board_id="board-1"),
            uow=uow,
        )


@pytest.mark.asyncio
async def test_status_use_case_delegates_to_central_service_write_gate() -> None:
    specs = _SpecService()
    evidence = {"manifest_ref": "about.json", "execution_receipt": "opaque"}
    result = await SetTestScenarioStatusUseCase().execute(
        SetTestScenarioStatusCommand("spec-1", "scenario-1", "passed", evidence),
        actor=ActorContext("agent", "mcp", board_id="board-1"),
        uow=SimpleNamespace(services=SimpleNamespace(specs=specs)),
    )
    assert result.result["new_status"] == "passed"
    assert specs.calls[0][-1] == evidence


@pytest.mark.asyncio
async def test_mcp_status_adapter_projects_service_verification_status(
    monkeypatch,
) -> None:
    specs = _SpecService()
    uow = SimpleNamespace(services=SimpleNamespace(specs=specs))

    class _UowContext:
        async def __aenter__(self):
            return uow

        async def __aexit__(self, *_args):
            return None

    async def get_context(_board_id):
        return SimpleNamespace(
            agent_id="agent",
            agent_name="Agent",
            realm_id=None,
            permissions=["specs:update"],
        )

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", get_context)
    monkeypatch.setattr(mcp_server, "check_permission", lambda *_args: None)
    monkeypatch.setattr(
        mcp_server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _UowContext(),
    )

    payload = json.loads(
        await mcp_server.okto_pulse_update_test_scenario_status.fn(
            board_id="board-1",
            spec_id="spec-1",
            scenario_id="scenario-1",
            status="ready",
        )
    )

    assert payload["evidence_verification_status"] == "verified"


@pytest.mark.asyncio
async def test_mcp_evidence_adapter_routes_inline_replay_without_filesystem_input(
    monkeypatch,
) -> None:
    replay = '{"description":"health","steps":[]}'
    evidence = {"execution_receipt": "opaque"}
    issuer = _Issuer(evidence)
    register_test_evidence_execution_issuer(issuer)
    register_test_evidence_write_verifier(_Verifier(EvidenceWriteVerification(True)))
    uow = SimpleNamespace(services=SimpleNamespace(specs=_SpecService()))

    class _UowContext:
        async def __aenter__(self):
            return uow

        async def __aexit__(self, *_args):
            return None

    async def get_context(_board_id):
        return SimpleNamespace(
            agent_id="agent",
            agent_name="Agent",
            realm_id=None,
            permissions=_evidence_execute_permissions(),
        )

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", get_context)
    monkeypatch.setattr(mcp_server, "check_permission", lambda *_args: None)
    monkeypatch.setattr(
        mcp_server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _UowContext(),
    )

    payload = json.loads(
        await mcp_server.okto_pulse_execute_test_scenario_evidence.fn(
            board_id="board-1",
            spec_id="spec-1",
            scenario_id="scenario-1",
            status="passed",
            replay=replay,
        )
    )

    assert payload == {
        "success": True,
        "persisted": False,
        "scenario_persisted": False,
        "manifest_persisted": True,
        "evidence": evidence,
        "next_tool": "okto_pulse_update_test_scenario_status",
    }
    assert issuer.calls[0].manifest_ref is None
    assert issuer.calls[0].inline_replay == replay


def test_tool_resource_documents_inline_mode_and_status_projection() -> None:
    resource = (
        Path(mcp_server.__file__).parent
        / "resources"
        / "reference"
        / "tool-docs"
        / "test-scenario.md"
    ).read_text(encoding="utf-8")

    assert "Preferred MCP-only mode" in resource
    assert "inline-<sha256>.json" in resource
    assert "evidence_verification_status" in resource
    assert "manifest_persisted" in resource


@pytest.mark.asyncio
async def test_existing_evidence_tool_schema_exposes_optional_inline_replay() -> None:
    tools = await mcp_server.mcp.get_tools()
    tool = tools["okto_pulse_execute_test_scenario_evidence"]

    assert set(tool.parameters["properties"]) == {
        "board_id",
        "spec_id",
        "scenario_id",
        "status",
        "manifest_ref",
        "replay",
    }
    assert set(tool.parameters["required"]) == {
        "board_id",
        "spec_id",
        "scenario_id",
        "status",
    }
