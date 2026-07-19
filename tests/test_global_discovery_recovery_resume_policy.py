from __future__ import annotations

from dataclasses import replace

import pytest

from okto_pulse.core.kg.global_discovery_recovery_control import (
    DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS,
    MAX_RECOVERY_CUMULATIVE_BUDGET_MS,
    RecoveryResumePolicy,
    RecoveryTerminalAttempt,
    RecoveryTerminalOutcome,
)


ATTEMPT_BUDGET_MS = 10 * 60 * 1_000
CUMULATIVE_BUDGET_MS = 15 * 60 * 1_000


def terminal_attempt(
    *,
    outcome: RecoveryTerminalOutcome,
    retryable: bool = False,
    cumulative_active_ms: int = 60_000,
) -> RecoveryTerminalAttempt:
    return RecoveryTerminalAttempt(
        run_id="run-resume",
        epoch=7,
        outcome=outcome,
        reason_code=f"fixture_{outcome.value}",
        retryable=retryable,
        active_elapsed_ms=min(60_000, cumulative_active_ms),
        cumulative_active_ms=cumulative_active_ms,
    )


@pytest.mark.parametrize(
    ("attempt", "admitted", "reason_code"),
    [
        (
            terminal_attempt(outcome=RecoveryTerminalOutcome.SUCCESS),
            False,
            "recovery_success_not_resumable",
        ),
        (
            terminal_attempt(outcome=RecoveryTerminalOutcome.PARTIAL),
            True,
            "recovery_resume_admitted",
        ),
        (
            terminal_attempt(outcome=RecoveryTerminalOutcome.CANCELLED),
            True,
            "recovery_resume_admitted",
        ),
        (
            terminal_attempt(
                outcome=RecoveryTerminalOutcome.FAILED,
                retryable=True,
            ),
            True,
            "recovery_resume_admitted",
        ),
        (
            terminal_attempt(
                outcome=RecoveryTerminalOutcome.FAILED,
                retryable=False,
            ),
            False,
            "recovery_failure_not_retryable",
        ),
        (
            terminal_attempt(
                outcome=RecoveryTerminalOutcome.TIMEOUT,
                retryable=True,
            ),
            False,
            "recovery_timeout_not_resumable",
        ),
    ],
)
def test_closed_terminal_matrix_is_deterministic(
    attempt: RecoveryTerminalAttempt,
    admitted: bool,
    reason_code: str,
) -> None:
    """ts_f3e9543d: only explicitly resumable terminal states are admitted."""

    decision = RecoveryResumePolicy().evaluate(attempt)

    assert decision.admitted is admitted
    assert decision.reason_code == reason_code
    assert decision.run_id == attempt.run_id
    assert decision.previous_epoch == attempt.epoch
    assert decision.next_epoch == (attempt.epoch + 1 if admitted else None)
    assert decision.attempt_budget_ms == (ATTEMPT_BUDGET_MS if admitted else 0)


@pytest.mark.parametrize(
    ("consumed_ms", "admitted", "attempt_budget_ms"),
    [
        (0, True, ATTEMPT_BUDGET_MS),
        (
            CUMULATIVE_BUDGET_MS - ATTEMPT_BUDGET_MS,
            True,
            ATTEMPT_BUDGET_MS,
        ),
        (
            CUMULATIVE_BUDGET_MS - ATTEMPT_BUDGET_MS + 1,
            True,
            ATTEMPT_BUDGET_MS - 1,
        ),
        (CUMULATIVE_BUDGET_MS - 1, True, 1),
        (CUMULATIVE_BUDGET_MS, False, 0),
        (CUMULATIVE_BUDGET_MS + 1, False, 0),
    ],
)
def test_resume_budget_is_bounded_at_ten_and_fifteen_minutes(
    consumed_ms: int,
    admitted: bool,
    attempt_budget_ms: int,
) -> None:
    attempt = terminal_attempt(
        outcome=RecoveryTerminalOutcome.PARTIAL,
        cumulative_active_ms=consumed_ms,
    )

    decision = RecoveryResumePolicy().evaluate(attempt)

    assert decision.admitted is admitted
    assert decision.attempt_budget_ms == attempt_budget_ms
    assert decision.remaining_cumulative_ms == max(
        0,
        CUMULATIVE_BUDGET_MS - consumed_ms,
    )
    assert decision.reason_code == (
        "recovery_resume_admitted"
        if admitted
        else "recovery_cumulative_budget_exhausted"
    )


def test_restart_uses_persisted_cumulative_consumption_without_reset() -> None:
    persisted = terminal_attempt(
        outcome=RecoveryTerminalOutcome.CANCELLED,
        cumulative_active_ms=14 * 60 * 1_000,
    )

    first_process = RecoveryResumePolicy().evaluate(persisted)
    restarted_process = RecoveryResumePolicy().evaluate(replace(persisted))

    assert first_process == restarted_process
    assert restarted_process.admitted is True
    assert restarted_process.next_epoch == 8
    assert restarted_process.attempt_budget_ms == 60_000
    assert restarted_process.remaining_cumulative_ms == 60_000


def test_budget_constants_are_the_normative_defaults() -> None:
    assert DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS == ATTEMPT_BUDGET_MS
    assert MAX_RECOVERY_CUMULATIVE_BUDGET_MS == CUMULATIVE_BUDGET_MS
