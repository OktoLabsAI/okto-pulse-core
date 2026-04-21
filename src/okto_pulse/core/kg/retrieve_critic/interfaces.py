"""Enums and dataclasses for the critic/reflect module."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Adequacy(str, Enum):
    """Assessment of retrieval quality the critic emits."""

    SUFFICIENT = "sufficient"
    PARTIAL = "partial"
    IRRELEVANT = "irrelevant"


class CriticAction(str, Enum):
    """Corrective action the critic suggests when adequacy is not
    ``SUFFICIENT``. Only ``IRRELEVANT`` actually triggers a retry —
    ``PARTIAL`` means best-effort accept (the rows are useful, just
    not ideal)."""

    ACCEPT = "accept"
    RETRY_WITH_REWRITE = "retry_with_rewrite"
    EXPAND_HOPS = "expand_hops"
    FALLBACK_SEMANTIC = "fallback_semantic"
    CHANGE_INTENT = "change_intent"


@dataclass(frozen=True)
class CriticDecision:
    """Output of critic_evaluate. Frozen so callers can pass it
    around without worrying about mutation between stages."""

    adequacy: Adequacy
    reason: str
    suggested_action: CriticAction


@dataclass(frozen=True)
class ReflectResult:
    """Final result of the reflect orchestrator.

    ``iterations`` is a tuple of telemetry dicts (one per retrieve
    attempt) with keys ``iteration``, ``adequacy``, ``action``,
    ``rows_count``. ``stopped_reason`` is one of:

    - ``accepted`` — critic said stop (SUFFICIENT or ACCEPT).
    - ``retries_exhausted`` — hit max_retries without convergence.
    - ``change_intent_v1_not_implemented`` — CHANGE_INTENT is a V1
      stub; we stop and log.
    - ``critic_error`` — critic_fn raised; we fell back to the last
      rows we had.
    """

    final_rows: tuple[dict, ...]
    iterations: tuple[dict, ...]
    final_adequacy: Adequacy
    stopped_reason: str
