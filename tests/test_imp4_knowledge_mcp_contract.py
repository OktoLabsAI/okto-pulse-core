"""Published MCP schema surface for selective Knowledge propagation v2."""

from __future__ import annotations

import inspect
import json
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application import knowledge_propagation_projection
from okto_pulse.core.application import use_cases
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.mcp import server
from okto_pulse.core.models.knowledge_propagation import (
    KnowledgeAssignmentDropRequest,
    KnowledgeAssignmentRefreshRequest,
    KnowledgeAssignmentReplaceRequest,
    KnowledgePropagationEnvelopeV2,
)
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgePropagationPortError,
)
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgePropagationServiceError,
)


def _request_properties(tool: object) -> dict[str, object]:
    parameters = getattr(tool, "parameters")
    request = parameters["properties"]["request"]
    return request["properties"]


def test_create_and_refinement_derive_expose_optional_v2_envelope() -> None:
    for tool in (
        server.okto_pulse_create_card,
        server.okto_pulse_derive_spec_from_refinement,
    ):
        signature = inspect.signature(tool.fn)
        assert signature.parameters["knowledge_propagation"].default is None
        schema = tool.parameters["properties"]["knowledge_propagation"]
        assert "knowledge_propagation" not in tool.parameters.get("required", [])
        assert schema["type"] == "object"
        envelope = schema
        states = schema["$defs"]["KnowledgeSelectionState"]["enum"]
        assert states == ["omitted", "explicit_empty", "explicit_ids"]
        assert envelope["properties"]["contract_version"]["const"] == 2
        assert envelope["properties"]["expected_revision"]["anyOf"][0]["const"] == 0


def test_mcp_creation_race_code_is_always_projected_retryable() -> None:
    payload = json.loads(
        server._knowledge_propagation_error(
            KnowledgePropagationServiceError(
                "knowledge_creation_race",
                "the bounded creation attempt lost",
            )
        )
    )

    assert payload["retryable"] is True


def test_four_card_assignment_tools_publish_exact_v2_request_contracts() -> None:
    replace = server.okto_pulse_replace_card_knowledge_assignments
    drop = server.okto_pulse_drop_card_knowledge_assignments
    refresh = server.okto_pulse_refresh_card_knowledge_assignments
    read = server.okto_pulse_get_card_knowledge_propagation

    assert set(replace.parameters["required"]) == {"board_id", "card_id", "request"}
    assert set(drop.parameters["required"]) == {"board_id", "card_id", "request"}
    assert set(refresh.parameters["required"]) == {"board_id", "card_id", "request"}
    assert set(read.parameters["required"]) == {"board_id", "card_id"}

    replace_props = _request_properties(replace)
    assert set(replace_props["mode"]["enum"]) == {"reference", "snapshot"}
    assert "linkage" in replace_props
    assert "relevance_links" not in replace_props

    drop_props = _request_properties(drop)
    assert "knowledge_ids" in drop_props
    assert "knowledge_ids" not in drop.parameters["properties"]["request"]["required"]

    refresh_props = _request_properties(refresh)
    assert "knowledge_ids" in refresh_props
    assert "justification" not in refresh_props


async def test_mcp_mutation_maps_port_error_without_terminal_audit(
    monkeypatch: Any,
) -> None:
    class _UowContext:
        async def __aenter__(self) -> Any:
            return SimpleNamespace()

        async def __aexit__(self, *args: Any) -> None:
            del args

    class _Factory:
        def __call__(self, **kwargs: Any) -> _UowContext:
            del kwargs
            return _UowContext()

    class _UseCase:
        async def execute(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise KnowledgePropagationPortError(
                "knowledge_store_unavailable",
                "the edition-owned propagation store is unavailable",
                details={"retryable": True},
            )

    audit_calls: list[Exception] = []

    async def _audit(error: Exception) -> None:
        audit_calls.append(error)

    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(),
    )
    monkeypatch.setattr(
        server,
        "_append_knowledge_attempt_after_rollback",
        _audit,
    )

    payload = json.loads(
        await server._mcp_card_knowledge_mutation(
            ctx=SimpleNamespace(
                agent_id="agent-1",
                agent_name="Agent",
                permissions=None,
                realm_id=None,
            ),
            board_id="board-1",
            command=object(),
            use_case=_UseCase(),
            projector=lambda result: result,
        )
    )

    assert payload["code"] == "knowledge_store_unavailable"
    assert payload["details"] == {"retryable": True}
    assert audit_calls == []


async def test_mcp_creation_retries_normalized_port_race_once(
    monkeypatch: Any,
) -> None:
    class _UowContext:
        async def __aenter__(self) -> Any:
            return SimpleNamespace()

        async def __aexit__(self, *args: Any) -> None:
            del args

    class _Factory:
        def __call__(self, **kwargs: Any) -> _UowContext:
            del kwargs
            return _UowContext()

    calls = 0
    mutation = object()

    class _UseCase:
        async def execute(self, *args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            del args, kwargs
            calls += 1
            if calls == 1:
                raise KnowledgePropagationPortError(
                    "knowledge_creation_race",
                    "concurrent creation won",
                )
            return SimpleNamespace(knowledge_mutation=mutation)

    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(),
    )
    monkeypatch.setattr(
        use_cases,
        "McpCreateCardUseCase",
        _UseCase,
    )
    monkeypatch.setattr(
        server,
        "_card_creation_v2_wire",
        lambda value: (
            {"card": {"id": "card-1"}} if value is mutation else {}
        ),
    )

    payload = json.loads(
        await server._mcp_create_card_v2(
            ctx=SimpleNamespace(
                agent_id="agent-1",
                agent_name="Agent",
                permissions=None,
                realm_id=None,
            ),
            board_id="board-1",
            spec_id="spec-1",
            card_create=object(),
            scenario_ids_list=None,
            title="Card",
            status="not_started",
            priority="none",
        )
    )

    assert calls == 2
    assert payload["success"] is True
    assert payload["card"]["id"] == "card-1"


async def test_four_card_assignment_mcp_tools_route_to_v2_use_cases(
    monkeypatch: Any,
) -> None:
    class _UowContext:
        async def __aenter__(self) -> Any:
            return SimpleNamespace()

        async def __aexit__(self, *args: Any) -> None:
            del args

    class _Factory:
        def __call__(self, **kwargs: Any) -> _UowContext:
            del kwargs
            return _UowContext()

    class _Projection:
        def __init__(self, operation: str) -> None:
            self.operation = operation

        def model_dump(self, **kwargs: Any) -> dict[str, str]:
            del kwargs
            return {"operation": self.operation}

    calls: list[tuple[str, object]] = []

    def _use_case(operation: str) -> type[Any]:
        class _UseCase:
            async def execute(
                self,
                command: object,
                **kwargs: Any,
            ) -> object:
                del kwargs
                calls.append((operation, command))
                if operation == "read":
                    return SimpleNamespace(read_result=object())
                return object()

        return _UseCase

    async def _agent_ctx(board_id: str) -> Any:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            board_id=board_id,
            permissions=None,
            realm_id=None,
        )

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(),
    )
    monkeypatch.setattr(
        use_cases,
        "ReplaceCardKnowledgeAssignmentsUseCase",
        _use_case("replace"),
    )
    monkeypatch.setattr(
        use_cases,
        "DropCardKnowledgeAssignmentsUseCase",
        _use_case("drop"),
    )
    monkeypatch.setattr(
        use_cases,
        "RefreshCardKnowledgeAssignmentsUseCase",
        _use_case("refresh"),
    )
    monkeypatch.setattr(
        use_cases,
        "GetCardKnowledgePropagationUseCase",
        _use_case("read"),
    )
    monkeypatch.setattr(
        knowledge_propagation_projection,
        "project_knowledge_mutation_response",
        lambda result: _Projection("mutation"),
    )
    monkeypatch.setattr(
        knowledge_propagation_projection,
        "project_refresh_response",
        lambda result: _Projection("refresh"),
    )
    monkeypatch.setattr(
        knowledge_propagation_projection,
        "project_technical_read_response",
        lambda result: _Projection("read"),
    )

    replace = json.loads(
        await server.okto_pulse_replace_card_knowledge_assignments.fn(
            board_id="board-1",
            card_id="card-1",
            request=KnowledgeAssignmentReplaceRequest(
                knowledge_ids=["root-1"],
                mode="reference",
                justification="relevant",
                idempotency_key="replace-1",
                expected_revision=0,
                linkage=[],
            ),
        )
    )
    drop = json.loads(
        await server.okto_pulse_drop_card_knowledge_assignments.fn(
            board_id="board-1",
            card_id="card-1",
            request=KnowledgeAssignmentDropRequest(
                knowledge_ids=[],
                justification="drop all",
                idempotency_key="drop-1",
                expected_revision=1,
            ),
        )
    )
    refresh = json.loads(
        await server.okto_pulse_refresh_card_knowledge_assignments.fn(
            board_id="board-1",
            card_id="card-1",
            request=KnowledgeAssignmentRefreshRequest(
                knowledge_ids=["root-1"],
                idempotency_key="refresh-1",
                expected_revision=2,
            ),
        )
    )
    read = json.loads(
        await server.okto_pulse_get_card_knowledge_propagation.fn(
            board_id="board-1",
            card_id="card-1",
        )
    )

    assert replace == {"success": True, "operation": "mutation"}
    assert drop == {"success": True, "operation": "mutation"}
    assert refresh == {"success": True, "operation": "refresh"}
    assert read == {"operation": "read"}
    assert [operation for operation, _ in calls] == [
        "replace",
        "drop",
        "refresh",
        "read",
    ]


async def test_refinement_mcp_rejects_v1_and_v2_before_opening_uow(
    monkeypatch: Any,
) -> None:
    async def _agent_ctx(board_id: str) -> Any:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            board_id=board_id,
            permissions=None,
            realm_id=None,
        )

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)

    def _unexpected_factory() -> Any:
        raise AssertionError("a conflicting request must not open a target UoW")

    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        _unexpected_factory,
    )

    payload = json.loads(
        await server.okto_pulse_derive_spec_from_refinement.fn(
            board_id="board-1",
            refinement_id="refinement-1",
            kb_ids=[],
            knowledge_propagation=KnowledgePropagationEnvelopeV2(
                selection_state="omitted",
                idempotency_key="derive-1",
            ),
        )
    )

    assert payload["code"] == "conflicting_propagation_parameters"
    assert payload["details"] == {}
    assert payload["retryable"] is False


async def test_all_assignment_mcp_tools_share_structured_card_not_found(
    monkeypatch: Any,
) -> None:
    class _UowContext:
        async def __aenter__(self) -> Any:
            return SimpleNamespace()

        async def __aexit__(self, *args: Any) -> None:
            del args

    class _Factory:
        def __call__(self, **kwargs: Any) -> _UowContext:
            del kwargs
            return _UowContext()

    class _MissingUseCase:
        async def execute(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise EntityNotFoundError("card", "card-404")

    async def _agent_ctx(board_id: str) -> Any:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            board_id=board_id,
            permissions=None,
            realm_id=None,
        )

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(),
    )

    # The common mutation adapter backs replace, drop, and refresh.
    mutation_payload = json.loads(
        await server._mcp_card_knowledge_mutation(
            ctx=await _agent_ctx("board-1"),
            board_id="board-1",
            command=object(),
            use_case=_MissingUseCase(),
            projector=lambda result: result,
        )
    )

    monkeypatch.setattr(
        use_cases,
        "GetCardKnowledgePropagationUseCase",
        _MissingUseCase,
    )
    read_payload = json.loads(
        await server.okto_pulse_get_card_knowledge_propagation.fn(
            board_id="board-1",
            card_id="card-404",
        )
    )

    for payload in (mutation_payload, read_payload):
        assert payload["error"] == "card_not_found"
        assert payload["code"] == "card_not_found"
        assert payload["retryable"] is False


@pytest.mark.parametrize(
    "error",
    [
        KnowledgePropagationPortError(
            "knowledge_store_unavailable",
            "the propagation store is unavailable",
            details={"component": "scope"},
        ),
        KnowledgePropagationServiceError(
            "knowledge_propagation_preflight_stale",
            "the parent changed after preflight",
            details={"revision": 2},
        ),
    ],
)
async def test_technical_read_mcp_maps_propagation_errors(
    monkeypatch: Any,
    error: Exception,
) -> None:
    class _UowContext:
        async def __aenter__(self) -> Any:
            return SimpleNamespace()

        async def __aexit__(self, *args: Any) -> None:
            del args

    class _Factory:
        def __call__(self, **kwargs: Any) -> _UowContext:
            del kwargs
            return _UowContext()

    class _FailingRead:
        async def execute(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise error

    async def _agent_ctx(board_id: str) -> Any:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            board_id=board_id,
            permissions=None,
            realm_id=None,
        )

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: _Factory(),
    )
    monkeypatch.setattr(
        use_cases,
        "GetCardKnowledgePropagationUseCase",
        _FailingRead,
    )

    payload = json.loads(
        await server.okto_pulse_get_card_knowledge_propagation.fn(
            board_id="board-1",
            card_id="card-1",
        )
    )

    assert payload == knowledge_propagation_projection.project_knowledge_propagation_error(
        error
    )
