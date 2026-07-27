"""Ports and DTOs for the bounded reflective KG query loop.

The state machine lives in Core.  Editions provide retrieval, critic and
telemetry implementations through these narrow contracts.  No concrete graph,
LLM, filesystem or database dependency belongs here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Protocol, runtime_checkable


class Adequacy(str, Enum):
    SUFFICIENT = "sufficient"
    PARTIAL = "partial"
    IRRELEVANT = "irrelevant"


class CriticAction(str, Enum):
    ACCEPT = "accept"
    RETRY_WITH_REWRITE = "retry_with_rewrite"
    EXPAND_HOPS = "expand_hops"
    FALLBACK_SEMANTIC = "fallback_semantic"
    CHANGE_INTENT = "change_intent"
    REJECT = "reject"


@dataclass(frozen=True)
class CriticDecision:
    adequacy: Adequacy
    reason: str
    suggested_action: CriticAction
    confidence: float | None = None
    target_intent: str | None = None
    rewritten_query: str | None = None


REFLECTIVE_DEFAULT_EDGES: tuple[str, ...] = (
    "contradicts",
    "depends_on",
    "mentions",
    "relates_to",
    "supersedes",
    "validates",
    "violates",
)


@dataclass(frozen=True)
class ReflectiveRetrievalRequest:
    board_id: str
    query: str
    limit: int
    min_confidence: float
    graph_layer: str
    iteration: int
    action: CriticAction | None = None
    fixed_hops_hint: int = 1
    target_intent: str | None = None
    rewritten_query: str | None = None
    previous_rows: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class ReflectiveRetrievalBatch:
    rows: tuple[Mapping[str, Any], ...]
    graph_version: str
    retrieval_mode: str
    cost_units: int = 1
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReflectiveCriticRequest:
    board_id: str
    query: str
    iteration: int
    rows: tuple[Mapping[str, Any], ...]
    rows_digest: str
    previous_rows_digest: str | None
    previous_action: CriticAction | None
    remaining_budget_units: int
    elapsed_ms: float


@runtime_checkable
class ReflectiveRetrievalPort(Protocol):
    """Edition-owned retrieval adapter used by the Core state machine.

    Implementations are synchronous and MUST bound their own graph/provider
    work so one call cannot outlive the request deadline indefinitely.
    """

    identity: str
    version: str

    def retrieve(
        self, request: ReflectiveRetrievalRequest
    ) -> ReflectiveRetrievalBatch: ...


@runtime_checkable
class ReflectiveCriticPort(Protocol):
    """Edition-owned critic.  A deterministic implementation is mandatory.

    Implementations are synchronous and MUST bound any provider work; Core
    checks the deadline between calls but cannot pre-empt an executing adapter.
    """

    identity: str
    version: str

    def evaluate(self, request: ReflectiveCriticRequest) -> Any: ...


@runtime_checkable
class ReflectiveTelemetryPort(Protocol):
    """Safe telemetry sink.  Events contain hashes/counters, never query text."""

    def emit(self, event: Mapping[str, Any]) -> None: ...


__all__ = [
    "Adequacy",
    "CriticAction",
    "CriticDecision",
    "REFLECTIVE_DEFAULT_EDGES",
    "ReflectiveCriticPort",
    "ReflectiveCriticRequest",
    "ReflectiveRetrievalBatch",
    "ReflectiveRetrievalPort",
    "ReflectiveRetrievalRequest",
    "ReflectiveTelemetryPort",
]
