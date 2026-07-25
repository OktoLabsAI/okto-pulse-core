"""Regression coverage for governed-deletion receipts and MCP telemetry."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.mcp_card_crud import (
    McpDeleteCardCommand,
    McpDeleteCardUseCase,
)
from okto_pulse.core.application.use_cases.mcp_ideation_crud import (
    McpDeleteIdeationCommand,
    McpDeleteIdeationUseCase,
)
from okto_pulse.core.application.use_cases.mcp_refinement_crud import (
    McpDeleteRefinementCommand,
    McpDeleteRefinementUseCase,
)
from okto_pulse.core.application.use_cases.spec_crud import (
    DeleteSpecCommand,
    DeleteSpecUseCase,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.services.main import GovernedArtifactDeletionReceipt


class _UowContext:
    def __init__(self, uow: object) -> None:
        self._uow = uow

    async def __aenter__(self) -> object:
        return self._uow

    async def __aexit__(self, *_args: object) -> None:
        return None


def _mcp_ctx() -> object:
    return SimpleNamespace(
        agent_id="takedown-reader",
        agent_name="Takedown Reader",
        permissions=["board:read"],
        realm_id=LOCAL_REALM_ID,
    )


@pytest.mark.asyncio
async def test_takedown_status_reads_exact_selector_through_uow() -> None:
    observed: list[dict[str, str | None]] = []

    class _Kg:
        async def query_takedown_telemetry(self, **selector: str | None):
            observed.append(selector)
            return {
                "found": True,
                "board_id": "board-takedown",
                "delete_event_id": "delete-1",
                "delivery_key": "gd_parity:board-takedown:card:card-1:1",
                "states": [{"state": "intent_created"}],
                "e2e_health": {"healthy": False},
            }

    uow = SimpleNamespace(services=SimpleNamespace(kg=_Kg()))

    def factory(**_kwargs: object) -> _UowContext:
        return _UowContext(uow)

    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_takedown_status")
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=_mcp_ctx()),
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            return_value=factory,
        ),
    ):
        payload = json.loads(
            await tool.fn(
                board_id="board-takedown",
                delete_event_id="delete-1",
            )
        )

    assert payload["found"] is True
    assert observed == [
        {
            "board_id": "board-takedown",
            "delete_event_id": "delete-1",
            "delivery_key": None,
        }
    ]


@pytest.mark.asyncio
async def test_takedown_status_keeps_transport_board_check_as_defense_in_depth() -> (
    None
):
    class _Kg:
        async def query_takedown_telemetry(self, **_selector: str | None):
            return {
                "found": True,
                "board_id": "other-board",
                "delete_event_id": "delete-1",
                "states": [{"state": "intent_created"}],
            }

    uow = SimpleNamespace(services=SimpleNamespace(kg=_Kg()))

    def factory(**_kwargs: object) -> _UowContext:
        return _UowContext(uow)

    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_takedown_status")
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=_mcp_ctx()),
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            return_value=factory,
        ),
    ):
        payload = json.loads(
            await tool.fn(
                board_id="board-takedown",
                delete_event_id="delete-1",
            )
        )

    assert payload == {
        "found": False,
        "error": "takedown_telemetry_not_found",
        "selector": {"delete_event_id": "delete-1"},
    }


@pytest.mark.asyncio
async def test_takedown_status_rejects_ambiguous_selector_before_uow() -> None:
    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_takedown_status")
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=_mcp_ctx()),
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            side_effect=AssertionError("UoW must not open for an invalid selector"),
        ),
    ):
        payload = json.loads(
            await tool.fn(
                board_id="board-takedown",
                delete_event_id="delete-1",
                delivery_key="delivery-1",
            )
        )

    assert payload == {
        "found": False,
        "error": "takedown_selector_invalid",
        "detail": "Provide exactly one of delete_event_id or delivery_key",
    }


@pytest.mark.asyncio
async def test_mcp_delete_use_case_returns_durable_takedown_receipt() -> None:
    receipt_payload = {
        "board_id": "board-takedown",
        "artifact_type": "card",
        "artifact_id": "card-1",
        "delete_event_id": "delete-1",
        "generation": 1,
        "reconcile_intent_id": "intent-1",
        "delivery_key": "gd_parity:board-takedown:card:card-1:1",
    }
    receipt = SimpleNamespace(to_dict=lambda: dict(receipt_payload))

    class _Cards:
        async def get_card(self, card_id: str) -> object:
            assert card_id == "card-1"
            return SimpleNamespace(board_id="board-takedown", title="Card 1")

        async def delete_card(
            self,
            card_id: str,
            actor_id: str,
            *,
            return_receipt: bool,
        ) -> object:
            assert (card_id, actor_id, return_receipt) == (
                "card-1",
                "agent-1",
                True,
            )
            return receipt

    class _Boards:
        async def _log_activity(self, **_kwargs: object) -> None:
            return None

    class _Uow:
        def __init__(self) -> None:
            self.services = SimpleNamespace(cards=_Cards(), boards=_Boards())
            self.committed = False

        async def commit(self) -> None:
            self.committed = True

    uow = _Uow()
    result = await McpDeleteCardUseCase().execute(
        McpDeleteCardCommand("card-1", "board-takedown"),
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_name="Agent 1",
            board_id="board-takedown",
            realm_id=LOCAL_REALM_ID,
        ),
        uow=uow,
    )

    assert result.deleted is True
    assert result.takedown == receipt_payload
    assert uow.committed is True


@pytest.mark.asyncio
async def test_delete_spec_use_case_returns_durable_takedown_receipt() -> None:
    receipt_payload = {
        "board_id": "board-takedown",
        "artifact_type": "spec",
        "artifact_id": "spec-1",
        "delete_event_id": "delete-spec-1",
        "generation": 1,
        "reconcile_intent_id": "intent-spec-1",
        "delivery_key": "gd_parity:board-takedown:spec:spec-1:1",
    }
    receipt = SimpleNamespace(to_dict=lambda: dict(receipt_payload))

    class _Specs:
        async def get_spec(self, spec_id: str) -> object:
            assert spec_id == "spec-1"
            return SimpleNamespace(board_id="board-takedown")

        async def delete_spec(
            self,
            spec_id: str,
            actor_id: str,
            *,
            return_receipt: bool,
        ) -> object:
            assert (spec_id, actor_id, return_receipt) == (
                "spec-1",
                "agent-1",
                True,
            )
            return receipt

    class _Uow:
        def __init__(self) -> None:
            self.services = SimpleNamespace(specs=_Specs())
            self.committed = False

        async def commit(self) -> None:
            self.committed = True

    uow = _Uow()
    result = await DeleteSpecUseCase().execute(
        DeleteSpecCommand("spec-1"),
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_name="Agent 1",
            board_id="board-takedown",
            realm_id=LOCAL_REALM_ID,
        ),
        uow=uow,
    )

    assert result.takedown == receipt_payload
    assert uow.committed is True


@pytest.mark.asyncio
async def test_delete_spec_mcp_envelope_exposes_takedown_receipt() -> None:
    receipt_payload = {
        "board_id": "board-takedown",
        "artifact_type": "spec",
        "artifact_id": "spec-1",
        "delete_event_id": "delete-spec-1",
        "generation": 1,
        "reconcile_intent_id": "intent-spec-1",
        "delivery_key": "gd_parity:board-takedown:spec:spec-1:1",
    }
    uow = SimpleNamespace(services=SimpleNamespace())

    def factory(**_kwargs: object) -> _UowContext:
        return _UowContext(uow)

    tool = await mcp_server.mcp.get_tool("okto_pulse_delete_spec")
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=_mcp_ctx()),
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            return_value=factory,
        ),
        patch.object(
            DeleteSpecUseCase,
            "execute",
            AsyncMock(return_value=SimpleNamespace(takedown=receipt_payload)),
        ),
    ):
        payload = json.loads(await tool.fn(board_id="board-takedown", spec_id="spec-1"))

    assert payload == {"success": True, "takedown": receipt_payload}


def test_governed_deletion_receipt_exposes_descendant_handles() -> None:
    child = GovernedArtifactDeletionReceipt(
        board_id="board-takedown",
        artifact_type="sprint",
        artifact_id="sprint-1",
        delete_event_id="delete-sprint-1",
        generation=1,
        reconcile_intent_id="intent-sprint-1",
        delivery_key="gd_parity:board-takedown:sprint:sprint-1:1",
    )
    parent = GovernedArtifactDeletionReceipt(
        board_id="board-takedown",
        artifact_type="spec",
        artifact_id="spec-1",
        delete_event_id="delete-spec-1",
        generation=1,
        reconcile_intent_id="intent-spec-1",
        delivery_key="gd_parity:board-takedown:spec:spec-1:1",
        descendant_deletions=(child,),
    )

    payload = parent.to_dict()

    assert payload["descendant_deletions"] == [child.to_dict()]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "use_case", "service_name", "get_name", "delete_name"),
    [
        (
            McpDeleteIdeationCommand("ideation-1", "board-takedown"),
            McpDeleteIdeationUseCase(),
            "ideations",
            "get_ideation",
            "delete_ideation",
        ),
        (
            McpDeleteRefinementCommand("refinement-1", "board-takedown"),
            McpDeleteRefinementUseCase(),
            "refinements",
            "get_refinement",
            "delete_refinement",
        ),
    ],
)
async def test_parent_delete_use_cases_return_governed_takedown(
    command: object,
    use_case: object,
    service_name: str,
    get_name: str,
    delete_name: str,
) -> None:
    artifact_id = command.ideation_id if service_name == "ideations" else command.refinement_id
    receipt_payload = {
        "board_id": "board-takedown",
        "artifact_type": service_name.removesuffix("s"),
        "artifact_id": artifact_id,
        "delete_event_id": f"delete-{artifact_id}",
        "generation": 1,
        "reconcile_intent_id": f"intent-{artifact_id}",
        "delivery_key": (
            f"gd_parity:board-takedown:{service_name.removesuffix('s')}:{artifact_id}:1"
        ),
    }
    receipt = SimpleNamespace(to_dict=lambda: dict(receipt_payload))
    service = SimpleNamespace()
    setattr(
        service,
        get_name,
        AsyncMock(return_value=SimpleNamespace(board_id="board-takedown")),
    )
    delete = AsyncMock(return_value=receipt)
    setattr(service, delete_name, delete)

    class _Uow:
        def __init__(self) -> None:
            self.services = SimpleNamespace(**{service_name: service})
            self.committed = False

        async def commit(self) -> None:
            self.committed = True

    uow = _Uow()
    result = await use_case.execute(
        command,
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_name="Agent 1",
            board_id="board-takedown",
            realm_id=LOCAL_REALM_ID,
        ),
        uow=uow,
    )

    assert result.deleted is True
    assert result.takedown == receipt_payload
    delete.assert_awaited_once_with(
        artifact_id,
        "agent-1",
        return_receipt=True,
    )
    assert uow.committed is True
