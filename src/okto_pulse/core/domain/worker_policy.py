"""Deterministic retry and transition policy shared by worker processors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class WorkState(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True)
class RetryDecision:
    state: WorkState
    delay_seconds: int
    terminal: bool


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    base: int = 2
    cap_seconds: int = 300

    def after_failure(self, attempts: int) -> RetryDecision:
        if attempts >= self.max_attempts:
            return RetryDecision(
                state=WorkState.DEAD_LETTER,
                delay_seconds=0,
                terminal=True,
            )
        return RetryDecision(
            state=WorkState.PENDING,
            delay_seconds=min(self.base**attempts, self.cap_seconds),
            terminal=False,
        )


__all__ = ["RetryDecision", "RetryPolicy", "WorkState"]
