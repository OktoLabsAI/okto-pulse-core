"""KG-01.2 — KGBackpressureGate (FR4, contract api_a1ec2dcc).

Deterministic, dependency-free tests around the three-valued decision
surface and the two typed errors. No DB, no Kùzu, no asyncio.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.backpressure import (
    BackpressureConfig,
    BackpressureDecision,
    BackpressureError,
    BackpressureErrorCode,
    KGBackpressureGate,
    WriteIntent,
    get_decision_count,
    get_decision_label_samples,
    get_required_counter_labels,
    reset_decision_counter,
)


@pytest.fixture(autouse=True)
def _reset_counter():
    reset_decision_counter()
    yield
    reset_decision_counter()


def _intent(
    board_id: str = "b1",
    operation: str = "consolidate",
    idempotency_key: str | None = "ix-1",
    risk_state: str = "healthy",
) -> WriteIntent:
    return WriteIntent(
        board_id=board_id,
        operation=operation,
        idempotency_key=idempotency_key,
        risk_state=risk_state,
    )


# --- FR4: three-valued decision surface -----------------------------------------


def test_first_intent_is_accepted_with_correlation_id():
    gate = KGBackpressureGate()
    response = gate.submit_intent(_intent())
    assert response.decision is BackpressureDecision.ACCEPTED
    assert isinstance(response.correlation_id, str) and response.correlation_id
    assert response.retry_after_seconds is None
    assert response.reason is None
    assert get_decision_count("b1", "accepted") == 1


def test_queued_when_headroom_exhausted_with_idempotency_key():
    gate = KGBackpressureGate(BackpressureConfig(accept_concurrency=1, queue_capacity=4))
    accepted = gate.submit_intent(_intent(idempotency_key="ix-accepted"))
    queued = gate.submit_intent(_intent(idempotency_key="ix-queued"))
    assert accepted.decision is BackpressureDecision.ACCEPTED
    assert queued.decision is BackpressureDecision.QUEUED
    assert queued.retry_after_seconds is not None and queued.retry_after_seconds >= 1
    assert queued.queue_position == 0
    assert get_decision_count("b1", "queued") == 1


def test_rejected_retryable_when_queue_full():
    gate = KGBackpressureGate(
        BackpressureConfig(accept_concurrency=1, queue_capacity=1, hard_limit=10)
    )
    gate.submit_intent(_intent(idempotency_key="k1"))  # accepted
    gate.submit_intent(_intent(idempotency_key="k2"))  # queued
    rejected = gate.submit_intent(_intent(idempotency_key="k3"))
    assert rejected.decision is BackpressureDecision.REJECTED_RETRYABLE
    assert rejected.reason == "queue_full"
    assert rejected.retry_after_seconds is not None and rejected.retry_after_seconds >= 1
    assert get_decision_count("b1", "rejected_retryable") == 1


# --- FR4: typed errors short-circuit decision ----------------------------------


def test_non_idempotent_cannot_queue():
    gate = KGBackpressureGate(BackpressureConfig(accept_concurrency=1, queue_capacity=4))
    gate.submit_intent(_intent(idempotency_key="ix-accepted"))
    with pytest.raises(BackpressureError) as excinfo:
        gate.submit_intent(_intent(idempotency_key=None))
    err = excinfo.value
    assert err.code is BackpressureErrorCode.NON_IDEMPOTENT_QUEUE_FORBIDDEN
    assert err.retryable is False
    assert err.correlation_id  # contract: always present
    assert get_decision_count("b1", "error") == 1


def test_hard_limit_raises_retryable_error():
    gate = KGBackpressureGate(
        BackpressureConfig(accept_concurrency=1, queue_capacity=1, hard_limit=2)
    )
    gate.submit_intent(_intent(idempotency_key="k1"))  # accepted (1 in_flight)
    gate.submit_intent(_intent(idempotency_key="k2"))  # queued (2 total)
    with pytest.raises(BackpressureError) as excinfo:
        gate.submit_intent(_intent(idempotency_key="k3"))
    err = excinfo.value
    assert err.code is BackpressureErrorCode.BACKPRESSURE_HARD_LIMIT
    assert err.retryable is True
    assert err.retry_after_seconds is not None and err.retry_after_seconds >= 1
    assert "hard_limit=2" in (err.reason or "")


# --- Risk state policy --------------------------------------------------------


@pytest.mark.parametrize("risk_state", ["quarantined", "recovery_needed"])
def test_risk_state_hard_reject_does_not_consume_queue(risk_state):
    gate = KGBackpressureGate()
    response = gate.submit_intent(_intent(risk_state=risk_state))
    assert response.decision is BackpressureDecision.REJECTED_RETRYABLE
    assert response.reason == f"risk_state.{risk_state}"
    snapshot = gate.snapshot("b1")
    assert snapshot == {"in_flight": 0, "queue_depth": 0}
    assert get_decision_count("b1", "rejected_retryable") == 1


def test_retry_after_grows_with_degraded_risk_state():
    gate = KGBackpressureGate(
        BackpressureConfig(accept_concurrency=1, queue_capacity=4)
    )
    gate.submit_intent(_intent(idempotency_key="ix-accepted", risk_state="healthy"))
    queued_healthy = gate.submit_intent(
        _intent(idempotency_key="ix-q1", risk_state="healthy")
    )
    queued_at_risk = gate.submit_intent(
        _intent(idempotency_key="ix-q2", risk_state="at_risk")
    )
    queued_backpressure = gate.submit_intent(
        _intent(idempotency_key="ix-q3", risk_state="backpressure")
    )
    assert (
        queued_healthy.retry_after_seconds
        <= queued_at_risk.retry_after_seconds
        <= queued_backpressure.retry_after_seconds
    )


# --- Idempotency cache --------------------------------------------------------


def test_same_idempotency_key_returns_same_decision():
    gate = KGBackpressureGate(BackpressureConfig(accept_concurrency=1))
    first = gate.submit_intent(_intent(idempotency_key="ix-stable"))
    second = gate.submit_intent(_intent(idempotency_key="ix-stable"))
    assert second is first or second == first
    assert second.correlation_id == first.correlation_id
    # Counter should NOT count the second as a fresh accept.
    assert get_decision_count("b1", "accepted") == 1
    assert get_decision_count("b1", "idempotent_hit") == 1


def test_release_drains_queue_to_in_flight():
    gate = KGBackpressureGate(
        BackpressureConfig(accept_concurrency=1, queue_capacity=4)
    )
    accepted = gate.submit_intent(_intent(idempotency_key="ix-accepted"))
    queued = gate.submit_intent(_intent(idempotency_key="ix-queued"))
    assert queued.decision is BackpressureDecision.QUEUED

    gate.release(accepted.correlation_id, "b1")
    snapshot = gate.snapshot("b1")
    # release should promote the queued intent into in_flight.
    assert snapshot["queue_depth"] == 0
    assert snapshot["in_flight"] == 1


def test_consume_next_pops_in_fifo_order():
    """Worker drains the queue in FIFO order, but MUST release the
    in-flight slot first — consume_next respects admission control
    (validator val_bd6f656e bloqueio 2)."""
    gate = KGBackpressureGate(
        BackpressureConfig(accept_concurrency=1, queue_capacity=8)
    )
    accepted = gate.submit_intent(_intent(idempotency_key="ix-accepted"))
    first_queued = gate.submit_intent(_intent(idempotency_key="ix-q1"))
    second_queued = gate.submit_intent(_intent(idempotency_key="ix-q2"))

    # release() auto-promotes the next queued into in_flight. After this
    # call there is no headroom for an explicit consume_next.
    gate.release(accepted.correlation_id)
    assert gate.snapshot("b1") == {"in_flight": 1, "queue_depth": 1}

    # consume_next would violate the cap and MUST return None.
    assert gate.consume_next("b1") is None

    # Release the auto-promoted one to free a slot. The remaining queued
    # intent is now consumable.
    gate.release(first_queued.correlation_id)
    assert gate.snapshot("b1") == {"in_flight": 1, "queue_depth": 0}
    # Above auto-promoted second_queued. release again to drain.
    gate.release(second_queued.correlation_id)
    assert gate.snapshot("b1") == {"in_flight": 0, "queue_depth": 0}


# --- Validator val_bd6f656e regression suite ----------------------------------


def test_consume_next_does_not_bypass_admission_control():
    """val_bd6f656e bloqueio 2: consume_next anteriormente promovia
    intents queued sem checar accept_concurrency, deixando in_flight
    superior ao cap configurado."""
    gate = KGBackpressureGate(
        BackpressureConfig(accept_concurrency=1, queue_capacity=4)
    )
    accepted = gate.submit_intent(_intent(idempotency_key="a"))
    queued_b = gate.submit_intent(_intent(idempotency_key="b"))
    gate.submit_intent(_intent(idempotency_key="c"))

    # in_flight is already at the cap. consume_next MUST refuse to pop.
    first = gate.consume_next("b1")
    second = gate.consume_next("b1")
    assert first is None
    assert second is None
    assert gate.snapshot("b1") == {"in_flight": 1, "queue_depth": 2}

    # After a release the auto-drain promotes one queued. consume_next
    # still refuses because the cap is full again.
    gate.release(accepted.correlation_id)
    assert gate.snapshot("b1") == {"in_flight": 1, "queue_depth": 1}
    assert gate.consume_next("b1") is None

    # The auto-promoted intent is queued_b — release it so the last one
    # drains.
    gate.release(queued_b.correlation_id)
    assert gate.snapshot("b1") == {"in_flight": 1, "queue_depth": 0}


def test_release_with_wrong_board_does_not_leak_in_flight():
    """val_bd6f656e bloqueio 3: release(correlation_id, wrong_board) must
    not decrement in_flight on the real board or anywhere else."""
    gate = KGBackpressureGate(BackpressureConfig(accept_concurrency=2))
    accepted_b1 = gate.submit_intent(
        _intent(board_id="b1", idempotency_key="k1")
    )
    gate.submit_intent(_intent(board_id="b2", idempotency_key="k2"))

    assert gate.snapshot("b1") == {"in_flight": 1, "queue_depth": 0}
    assert gate.snapshot("b2") == {"in_flight": 1, "queue_depth": 0}

    # Caller misroutes the release — points to b2 but correlation_id is
    # bound to b1. The gate MUST refuse to mutate either board, and the
    # binding must remain so a correct release can still arrive later.
    gate.release(accepted_b1.correlation_id, board_id="b2")
    assert gate.snapshot("b1") == {"in_flight": 1, "queue_depth": 0}
    assert gate.snapshot("b2") == {"in_flight": 1, "queue_depth": 0}

    # Correct release still works.
    gate.release(accepted_b1.correlation_id, board_id="b1")
    assert gate.snapshot("b1") == {"in_flight": 0, "queue_depth": 0}
    assert gate.snapshot("b2") == {"in_flight": 1, "queue_depth": 0}


def test_release_resolves_board_from_correlation_id_when_omitted():
    """The gate's source of truth for board binding is _active; the
    board_id parameter is a defensive assert only. Omitting it works."""
    gate = KGBackpressureGate()
    accepted = gate.submit_intent(_intent(idempotency_key="k1"))
    assert gate.snapshot("b1") == {"in_flight": 1, "queue_depth": 0}

    gate.release(accepted.correlation_id)  # no board_id passed
    assert gate.snapshot("b1") == {"in_flight": 0, "queue_depth": 0}


def test_counter_carries_required_or_labels():
    """val_bd6f656e bloqueio 1: OR or_748bf163 mandates labels
    (board_id, operation, decision, reason, queue_depth_bucket)."""
    # Contract check: the canonical label tuple is exposed.
    assert get_required_counter_labels() == (
        "board_id",
        "operation",
        "decision",
        "reason",
        "queue_depth_bucket",
    )

    gate = KGBackpressureGate(
        BackpressureConfig(accept_concurrency=1, queue_capacity=4)
    )
    # accepted (no reason)
    gate.submit_intent(
        _intent(operation="consolidate", idempotency_key="ka")
    )
    # queued behind in_flight
    gate.submit_intent(
        _intent(operation="consolidate", idempotency_key="kb")
    )
    # rejected_retryable: hard reject by risk_state
    gate.submit_intent(
        _intent(
            operation="rebuild",
            idempotency_key="kc",
            risk_state="quarantined",
        )
    )
    # error: non_idempotent_queue_forbidden
    try:
        gate.submit_intent(
            _intent(operation="consolidate", idempotency_key=None)
        )
    except BackpressureError:
        pass

    samples = get_decision_label_samples()
    keys = {
        (s["operation"], s["decision"], s["reason"])
        for s in samples
    }
    assert ("consolidate", "accepted", "n/a") in keys
    assert ("consolidate", "queued", "queued_behind_in_flight") in keys
    assert ("rebuild", "rejected_retryable", "risk_state.quarantined") in keys
    assert (
        "consolidate",
        "error",
        "non_idempotent_queue_forbidden",
    ) in keys

    # Every sample carries all 5 required labels with non-empty strings.
    for s in samples:
        for label in get_required_counter_labels():
            assert label in s
            assert isinstance(s[label], str)
            assert s[label]
        assert isinstance(s["count"], int)
        assert s["count"] >= 1

    # Sliced queries respect the new labels.
    assert (
        get_decision_count(
            "b1",
            "queued",
            operation="consolidate",
            reason="queued_behind_in_flight",
        )
        == 1
    )
    assert (
        get_decision_count("b1", "queued", operation="rebuild") == 0
    )
    assert (
        get_decision_count(
            "b1",
            "error",
            reason="non_idempotent_queue_forbidden",
        )
        == 1
    )


def test_release_promotes_exactly_one_queued_respecting_cap():
    """release() promotes at most accept_concurrency intents into
    in_flight at a time, even with a long queue."""
    gate = KGBackpressureGate(
        BackpressureConfig(accept_concurrency=2, queue_capacity=8)
    )
    a = gate.submit_intent(_intent(idempotency_key="a"))
    gate.submit_intent(_intent(idempotency_key="b"))  # accepted
    gate.submit_intent(_intent(idempotency_key="c"))  # queued
    gate.submit_intent(_intent(idempotency_key="d"))  # queued
    gate.submit_intent(_intent(idempotency_key="e"))  # queued

    assert gate.snapshot("b1") == {"in_flight": 2, "queue_depth": 3}

    gate.release(a.correlation_id)
    snap = gate.snapshot("b1")
    assert snap["in_flight"] == 2
    assert snap["queue_depth"] == 2


# --- Counter & multi-board isolation ------------------------------------------


def test_counters_are_per_board():
    gate = KGBackpressureGate()
    gate.submit_intent(_intent(board_id="b1", idempotency_key="ka"))
    gate.submit_intent(_intent(board_id="b2", idempotency_key="kb"))
    assert get_decision_count("b1", "accepted") == 1
    assert get_decision_count("b2", "accepted") == 1
    assert get_decision_count("b3", "accepted") == 0


def test_snapshot_returns_zeros_for_unknown_board():
    gate = KGBackpressureGate()
    snapshot = gate.snapshot("unknown-board")
    assert snapshot == {"in_flight": 0, "queue_depth": 0}
