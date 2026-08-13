from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.application.use_cases.spec_dependencies import (
    AddSpecDependencyUseCase,
)
from okto_pulse.core.domain.spec_dependency import (
    SPEC_DEPENDENCY_CURSOR_MAX_LENGTH,
    SPEC_DEPENDENCY_REMOVAL_REASON_MAX_LENGTH,
    SpecDependencyOperationError,
)
from okto_pulse.core.inbound.spec_dependency_error import (
    SPEC_DEPENDENCY_PUBLIC_ERROR_CODES,
    project_spec_dependency_error,
    spec_dependency_http_status,
)


@pytest.mark.parametrize(
    ("code", "expected_status"),
    [
        ("invalid_spec_dependency_request", 400),
        ("invalid_cursor", 400),
        ("spec_dependency_self_reference", 400),
        ("dependency_target_unavailable", 404),
        ("spec_dependency_not_found", 404),
        ("cross_board_dependency_forbidden", 409),
        ("spec_dependency_state_conflict", 409),
        ("spec_dependency_version_conflict", 409),
        ("spec_dependency_cycle", 409),
        ("spec_dependencies_incomplete", 409),
    ],
)
def test_known_spec_dependency_codes_use_closed_public_projection(
    code: str,
    expected_status: int,
) -> None:
    secret = "database token=DO-NOT-LEAK"
    error = SpecDependencyOperationError(
        code,
        secret,
        remediation=secret,
        facts={"unreviewed_fact": secret},
    )

    projected = project_spec_dependency_error(error)

    assert code in SPEC_DEPENDENCY_PUBLIC_ERROR_CODES
    assert projected["code"] == code
    assert projected["retryable"] is (code == "spec_dependency_version_conflict")
    assert spec_dependency_http_status(error) == expected_status
    assert secret not in repr(projected)
    assert "remediation" not in projected
    assert "facts" not in projected


def test_state_conflict_variant_preserves_only_reviewed_guidance_and_facts() -> None:
    secret = "postgresql://user:SECRET@host/database"
    projected = project_spec_dependency_error(
        SpecDependencyOperationError(
            "spec_dependency_state_conflict",
            secret,
            remediation="complete_target_spec_or_return_source_to_draft",
            facts={
                "spec_id": "spec-1",
                "target_spec_id": "spec-2",
                "target_status": "validated",
                "spec_edition": 3,
                "conflict_kind": "not-reviewed",
                "sql": secret,
            },
        )
    )

    assert projected == {
        "code": "spec_dependency_state_conflict",
        "message": ("After execution starts, a new prerequisite must already be Done."),
        "retryable": False,
        "remediation": "complete_target_spec_or_return_source_to_draft",
        "facts": {
            "spec_edition": 3,
            "spec_id": "spec-1",
            "target_spec_id": "spec-2",
            "target_status": "validated",
        },
    }
    assert secret not in repr(projected)


def test_unknown_code_and_exception_collapse_to_same_safe_internal_error() -> None:
    secret = "credential=DO-NOT-LEAK"
    unknown_code = SpecDependencyOperationError(
        "future_unreviewed_dependency_error",
        secret,
        remediation=secret,
        facts={"secret": secret},
    )
    expected = {
        "code": "spec_dependency_internal_error",
        "message": "Spec dependency operation could not be completed.",
        "retryable": False,
    }

    assert project_spec_dependency_error(unknown_code) == expected
    assert project_spec_dependency_error(RuntimeError(secret)) == expected
    assert spec_dependency_http_status(unknown_code) == 500
    assert spec_dependency_http_status(RuntimeError(secret)) == 500
    assert secret not in repr(project_spec_dependency_error(unknown_code))


def test_mcp_dependency_error_payload_uses_the_shared_projector() -> None:
    from okto_pulse.core.mcp.server import _spec_dependency_error_payload

    secret = "raw database exception DO-NOT-LEAK"
    payload = json.loads(
        _spec_dependency_error_payload(
            SpecDependencyOperationError(
                "spec_dependency_cycle",
                secret,
                remediation="choose_a_non_descendant_prerequisite",
                facts={
                    "spec_id": "spec-1",
                    "target_spec_id": "spec-2",
                    "cycle_path": ["spec-2", "spec-1"],
                    "raw": secret,
                },
            )
        )
    )

    assert payload == {
        "error": "spec_dependency_cycle",
        "code": "spec_dependency_cycle",
        "message": "This dependency would create a cycle between Specs.",
        "retryable": False,
        "remediation": "choose_a_non_descendant_prerequisite",
        "facts": {
            "cycle_path": ["spec-2", "spec-1"],
            "spec_id": "spec-1",
            "target_spec_id": "spec-2",
        },
    }
    assert secret not in repr(payload)


def test_mcp_dependency_tools_publish_shared_core_input_bounds() -> None:
    from okto_pulse.core.mcp import server as mcp_server

    remove_properties = mcp_server.mcp._tool_manager._tools[
        "okto_pulse_remove_spec_dependency"
    ].parameters["properties"]
    assert remove_properties["reason"]["minLength"] == 1
    assert (
        remove_properties["reason"]["maxLength"]
        == SPEC_DEPENDENCY_REMOVAL_REASON_MAX_LENGTH
    )

    list_properties = mcp_server.mcp._tool_manager._tools[
        "okto_pulse_list_spec_dependencies"
    ].parameters["properties"]
    cursor_string_schema = next(
        branch
        for branch in list_properties["cursor"]["anyOf"]
        if branch.get("type") == "string"
    )
    assert cursor_string_schema["maxLength"] == SPEC_DEPENDENCY_CURSOR_MAX_LENGTH


class _UowContext:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *_: object) -> bool:
        return False


class _UowFactory:
    def __call__(self, **_: object) -> _UowContext:
        return _UowContext()


@pytest.mark.asyncio
async def test_mcp_dependency_tool_fails_closed_for_unexpected_exception() -> None:
    from okto_pulse.core.mcp import server as mcp_server

    secret = "driver failure password=DO-NOT-LEAK"
    context = SimpleNamespace(
        agent_id="agent-1",
        agent_name="Agent",
        permissions=["*"],
    )
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=context),
        ),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            return_value=_UowFactory(),
        ),
        patch.object(
            AddSpecDependencyUseCase,
            "execute",
            AsyncMock(side_effect=RuntimeError(secret)),
        ),
    ):
        payload = json.loads(
            await mcp_server.okto_pulse_add_spec_dependency.fn(
                board_id="board-1",
                spec_id="spec-1",
                prerequisite_spec_id="spec-2",
                expected_spec_version=1,
                expected_spec_edition=1,
                idempotency_key="request-1",
            )
        )

    assert payload == {
        "error": "spec_dependency_internal_error",
        "code": "spec_dependency_internal_error",
        "message": "Spec dependency operation could not be completed.",
        "retryable": False,
    }
    assert secret not in repr(payload)
