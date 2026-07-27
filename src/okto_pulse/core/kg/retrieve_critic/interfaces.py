"""Enums and dataclasses for the critic/reflect module."""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.kg.interfaces.reflective_query import (
    Adequacy,
    CriticAction,
    CriticDecision,
)

__all__ = ["Adequacy", "CriticAction", "CriticDecision", "ReflectResult"]


@dataclass(frozen=True)
class ReflectResult:
    """Final result of the reflect orchestrator.

    ``iterations`` is a tuple of telemetry dicts (one per retrieve
    attempt) with keys ``iteration``, ``adequacy``, ``action``,
    ``rows_count``. ``stopped_reason`` is one of:

    - ``accepted`` — critic said stop (SUFFICIENT or ACCEPT).
    - ``retries_exhausted`` — hit max_retries without convergence.
    - ``rejected`` — critic explicitly rejected the available evidence.
    - ``critic_malformed`` — an executable action omitted required fields.
    - ``critic_error`` — critic_fn raised; we fell back to the last
      rows we had.
    """

    final_rows: tuple[dict, ...]
    iterations: tuple[dict, ...]
    final_adequacy: Adequacy
    stopped_reason: str
