"""Pure contract tests for the governed-deletion delivery boundary."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from okto_pulse.core.ports.delivery_ledger import (
    DELIVERY_OUTBOX_EVENT_TYPE,
    DELIVERY_OUTBOX_REASON,
    DELIVERY_WORK_KIND,
    DeliveryCircuitSnapshot,
    DeliveryState,
    DeliveryTransferClaimConflict,
    DeliveryTransferError,
    DeliveryTransferReplayConflict,
    DeliveryTransferRequest,
    DeliveryTransferReceipt,
    build_attempt_event_key,
    build_delivery_key,
    get_delivery_ledger_port,
    register_delivery_ledger_port,
    reset_delivery_ledger_port_for_tests,
)


def _request(**overrides: object) -> DeliveryTransferRequest:
    values: dict[str, object] = {
        "entry_id": "queue-entry-g3",
        "claim_token": "claim-token-g3",
        "board_id": "board-card6",
        "artifact_type": "spec",
        "artifact_id": "deleted-spec",
        "generation": 3,
        "delete_event_id": "delete-event-g3",
        "target_state": DeliveryState.OUTBOX_PERSISTED,
    }
    values.update(overrides)
    return DeliveryTransferRequest(**values)  # type: ignore[arg-type]


def test_delivery_and_attempt_keys_are_literal_and_stable() -> None:
    delivery_key = build_delivery_key(
        board_id="board-card6",
        artifact_type="spec",
        artifact_id="deleted-spec",
        generation=3,
    )

    assert delivery_key == "gd_parity:board-card6:spec:deleted-spec:3"
    assert build_attempt_event_key(delivery_key, attempt=0) == (
        "gd_parity:board-card6:spec:deleted-spec:3:attempt:0"
    )
    assert build_attempt_event_key(delivery_key, attempt=7) == (
        "gd_parity:board-card6:spec:deleted-spec:3:attempt:7"
    )


@pytest.mark.parametrize(
    ("kwargs", "error_code"),
    [
        ({"board_id": ""}, "delivery_transfer_board_id_invalid"),
        ({"artifact_type": " spec"}, "delivery_transfer_artifact_type_invalid"),
        ({"artifact_id": ""}, "delivery_transfer_artifact_id_invalid"),
        ({"generation": 0}, "delivery_transfer_generation_invalid"),
        ({"generation": True}, "delivery_transfer_generation_invalid"),
    ],
)
def test_delivery_key_rejects_ambiguous_identity(
    kwargs: dict[str, object],
    error_code: str,
) -> None:
    values: dict[str, object] = {
        "board_id": "board-card6",
        "artifact_type": "spec",
        "artifact_id": "deleted-spec",
        "generation": 1,
    }
    values.update(kwargs)

    with pytest.raises(ValueError, match=error_code):
        build_delivery_key(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("attempt", [-1, True, 1.5])
def test_attempt_key_rejects_invalid_attempt(attempt: object) -> None:
    with pytest.raises(ValueError, match="delivery_transfer_attempt_invalid"):
        build_attempt_event_key(
            "gd_parity:board-card6:spec:deleted-spec:1",
            attempt=attempt,  # type: ignore[arg-type]
        )


def test_transfer_request_derives_an_immutable_deterministic_payload() -> None:
    first = _request()
    replay = _request(target_state="outbox_persisted")

    assert first.target_state is DeliveryState.OUTBOX_PERSISTED
    assert first.work_kind == DELIVERY_WORK_KIND
    assert first.attempt == 0
    assert first.delivery_key == "gd_parity:board-card6:spec:deleted-spec:3"
    assert first.attempt_event_key == f"{first.delivery_key}:attempt:0"
    assert first.outbox_event_type == DELIVERY_OUTBOX_EVENT_TYPE
    assert first.outbox_session_id == replay.outbox_session_id
    assert dict(first.payload) == dict(replay.payload) == {
        "event_id": first.attempt_event_key,
        "board_id": "board-card6",
        "session_id": first.outbox_session_id,
        "nodes_added": 0,
        "reason": DELIVERY_OUTBOX_REASON,
        "delivery_key": first.delivery_key,
        "attempt": 0,
        "artifact_type": "spec",
        "artifact_id": "deleted-spec",
        "generation": 3,
        "delete_event_id": "delete-event-g3",
    }

    with pytest.raises(FrozenInstanceError):
        first.claim_token = "replacement"  # type: ignore[misc]
    with pytest.raises(TypeError):
        first.payload["attempt"] = 9  # type: ignore[index]


def test_transfer_request_preserves_reconcile_evidence_and_timestamp() -> None:
    occurred_at = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)
    details = {"demoted_count": 2, "incomplete": False}

    request = _request(reconcile_details=details, occurred_at=occurred_at)
    details["demoted_count"] = 99

    assert request.occurred_at == occurred_at
    assert dict(request.reconcile_details) == {
        "demoted_count": 2,
        "incomplete": False,
    }
    with pytest.raises(TypeError):
        request.reconcile_details["demoted_count"] = 3  # type: ignore[index]

    with pytest.raises(ValueError, match="delivery_transfer_occurred_at_invalid"):
        _request(occurred_at="2026-07-21T18:00:00Z")


@pytest.mark.parametrize(
    "target_state",
    [DeliveryState.OUTBOX_PERSISTED, DeliveryState.DELIVERY_DEBT],
)
def test_transfer_request_accepts_only_initial_delivery_states(
    target_state: DeliveryState,
) -> None:
    assert _request(target_state=target_state).target_state is target_state


@pytest.mark.parametrize(
    ("overrides", "error_code"),
    [
        ({"entry_id": ""}, "delivery_transfer_entry_id_invalid"),
        ({"claim_token": " token"}, "delivery_transfer_claim_token_invalid"),
        ({"work_kind": "consolidate"}, "delivery_transfer_work_kind_invalid"),
        ({"generation": 0}, "delivery_transfer_generation_invalid"),
        ({"delete_event_id": ""}, "delivery_transfer_delete_event_id_invalid"),
        ({"attempt": 1}, "delivery_transfer_initial_attempt_invalid"),
        (
            {"target_state": DeliveryState.DELIVERED},
            "delivery_transfer_target_state_invalid",
        ),
        ({"target_state": "unknown"}, "delivery_transfer_target_state_invalid"),
    ],
)
def test_transfer_request_fails_closed_on_invalid_contract(
    overrides: dict[str, object],
    error_code: str,
) -> None:
    with pytest.raises(ValueError, match=error_code):
        _request(**overrides)


def test_transfer_receipt_encodes_persisted_and_debt_shapes() -> None:
    request = _request()
    persisted = DeliveryTransferReceipt(
        delivery_key=request.delivery_key,
        state="outbox_persisted",  # type: ignore[arg-type]
        attempt=0,
        attempt_event_key=request.attempt_event_key,
    )
    debt = DeliveryTransferReceipt(
        delivery_key=request.delivery_key,
        state=DeliveryState.DELIVERY_DEBT,
        attempt=0,
        attempt_event_key=None,
        replayed=True,
    )

    assert persisted.state is DeliveryState.OUTBOX_PERSISTED
    assert persisted.replayed is False
    assert debt.state is DeliveryState.DELIVERY_DEBT
    assert debt.replayed is True


@pytest.mark.parametrize(
    "receipt",
    [
        {
            "state": DeliveryState.OUTBOX_PERSISTED,
            "attempt": 0,
            "attempt_event_key": None,
        },
        {
            "state": DeliveryState.DELIVERY_DEBT,
            "attempt": 0,
            "attempt_event_key": "gd_parity:x:attempt:0",
        },
        {
            "state": DeliveryState.DELIVERED,
            "attempt": 0,
            "attempt_event_key": None,
        },
        {
            "state": DeliveryState.OUTBOX_PERSISTED,
            "attempt": 1,
            "attempt_event_key": "gd_parity:x:attempt:1",
        },
    ],
)
def test_transfer_receipt_rejects_inconsistent_shape(
    receipt: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        DeliveryTransferReceipt(
            delivery_key="gd_parity:board-card6:spec:deleted-spec:3",
            replayed=False,
            **receipt,  # type: ignore[arg-type]
        )


def test_circuit_snapshot_requires_explicit_boolean_and_reason() -> None:
    healthy = DeliveryCircuitSnapshot(degraded=False, reason="healthy")
    degraded = DeliveryCircuitSnapshot(
        degraded=True,
        reason="terminal_outbox_backlog",
    )

    assert healthy.degraded is False
    assert degraded.degraded is True
    with pytest.raises(ValueError, match="delivery_transfer_circuit_reason_invalid"):
        DeliveryCircuitSnapshot(degraded=True, reason="")
    with pytest.raises(ValueError, match="delivery_circuit_degraded_invalid"):
        DeliveryCircuitSnapshot(degraded=1, reason="unknown")  # type: ignore[arg-type]


def test_transfer_conflicts_have_a_typed_common_base() -> None:
    assert issubclass(DeliveryTransferClaimConflict, DeliveryTransferError)
    assert issubclass(DeliveryTransferReplayConflict, DeliveryTransferError)


@pytest.mark.asyncio
async def test_delivery_ledger_runtime_registration_round_trip() -> None:
    class _Adapter:
        async def read_circuit_snapshot(self, _context, *, board_id):
            assert board_id == "board-card6"
            return DeliveryCircuitSnapshot(degraded=False, reason="healthy")

        async def transfer_delivery_ownership(self, _context, request):
            return DeliveryTransferReceipt(
                delivery_key=request.delivery_key,
                state=request.target_state,
                attempt=request.attempt,
                attempt_event_key=(
                    request.attempt_event_key
                    if request.target_state is DeliveryState.OUTBOX_PERSISTED
                    else None
                ),
            )

    reset_delivery_ledger_port_for_tests()
    adapter = _Adapter()
    register_delivery_ledger_port(adapter)
    try:
        port = get_delivery_ledger_port()
        snapshot = await port.read_circuit_snapshot(
            object(),
            board_id="board-card6",
        )
        receipt = await port.transfer_delivery_ownership(object(), _request())
        assert snapshot == DeliveryCircuitSnapshot(degraded=False, reason="healthy")
        assert receipt.state is DeliveryState.OUTBOX_PERSISTED
    finally:
        reset_delivery_ledger_port_for_tests()

    with pytest.raises(RuntimeError, match="delivery_ledger_port_not_configured"):
        get_delivery_ledger_port()
