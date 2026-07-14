"""CognitiveSourceStore port (spec MKG-A-S1, contracts api_e3aad88b / api_33539a3f).

Durable, append-only source of truth for canonical COGNITIVE nodes
(Decision / Learning / Alternative / Assumption). A cognitive Decision can be
independent from the structured spec decisions materialized by the deterministic
writer, so these nodes may have no SQL artifact behind them. Before this port the
per-board graph was their ONLY home — an unreadable graph meant silent loss
(R2-IMP2 snapshots the LIVE graph; outcome ``unreadable`` == nothing preserved;
incident 2026-07-10 destroyed 73 cognitive nodes exactly this way).

Contract (spec BR2):
  * every cognitive commit APPENDS a full record (payload + evidence
    binding + generation) BEFORE the commit reports success — fail-closed:
    the graph is never ahead of the durable source;
  * records are immutable — never UPDATEd or DELETEd by this port;
  * ``enumerate`` returns a deterministic ordering (committed_at, node_id,
    generation) so the rebuild manifest hash is stable (spec TR5);
  * replay consumers restore records literally: no LLM, evidence binding
    preserved, ``human_curated`` content never clobbered (spec BR3).

Pure: stdlib ``dataclasses`` / ``typing`` only. It does NOT import
SQLAlchemy, engines or ``okto_pulse.community`` — the concrete Community
adapter (``sqlalchemy_kg_cognitive_source``) owns those.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

__all__ = [
    "CognitiveSourceError",
    "CognitiveSourceRecord",
    "CognitiveSourceStore",
    "register_cognitive_source_store",
    "require_cognitive_source_store",
    "reset_cognitive_source_store_for_tests",
    "resolve_cognitive_source_store",
]


class CognitiveSourceError(Exception):
    """Structured, fail-closed cognitive-source failure.

    Surfaced by the consolidation commit as the stable error code
    ``kg_cognitive_source_unavailable`` (spec AC3): the commit MUST abort —
    a cognitive node may never land in the graph without its durable record.
    """

    def __init__(
        self,
        failure_reason: str,
        *,
        board_id: str | None = None,
        node_id: str | None = None,
        remediation: str | None = None,
    ) -> None:
        self.failure_reason = failure_reason
        self.board_id = board_id
        self.node_id = node_id
        self.remediation = remediation
        detail = " ".join(
            part
            for part in (
                f"board_id={board_id}" if board_id else "",
                f"node_id={node_id}" if node_id else "",
            )
            if part
        )
        super().__init__(f"{failure_reason}{(' [' + detail + ']') if detail else ''}")


@dataclass(frozen=True)
class CognitiveSourceRecord:
    """One immutable durable record of a committed cognitive node.

    ``payload`` carries EVERY attribute persisted on the graph node so a
    replay is a literal restoration; ``evidence_refs`` preserves the
    original evidence binding (board decisions f47eff53e116/da16db6d1c4f:
    cognitive nodes are never re-generated without evidence).
    """

    node_id: str
    board_id: str
    node_type: str
    generation: int
    payload: Mapping[str, Any]
    evidence_refs: tuple[str, ...] = ()
    source_session_id: str | None = None
    committed_at: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class CognitiveSourceStore(Protocol):
    """Append-only durable store for cognitive node records."""

    async def append(self, record: CognitiveSourceRecord) -> str:
        """Persist ``record`` and return its storage id.

        MUST be idempotent per ``(node_id, generation)`` so a commit retry
        after a transient failure is safe. Raises
        :class:`CognitiveSourceError` when the store is unavailable —
        callers abort the commit (fail-closed, spec D5).
        """
        ...

    async def enumerate(self, board_id: str) -> tuple[CognitiveSourceRecord, ...]:
        """Return every record for ``board_id`` in the deterministic order
        ``(committed_at, node_id, generation)`` (spec TR5). Raises
        :class:`CognitiveSourceError` when the store cannot be read — the
        rebuild reports a structured error, never a silent partial success.
        """
        ...


_RUNTIME_KEY = "ports.kg.cognitive_source_store"


def register_cognitive_source_store(store: CognitiveSourceStore) -> None:
    """Register the edition-owned adapter (called by community main wiring)."""

    register_runtime_value(_RUNTIME_KEY, store)


def resolve_cognitive_source_store() -> CognitiveSourceStore | None:
    """Return the registered store, or ``None`` when absent (probe use only)."""

    return resolve_runtime_value(_RUNTIME_KEY)


def require_cognitive_source_store() -> CognitiveSourceStore:
    """Fail-closed resolver: a missing store NEVER degrades to a no-op.

    Raising here (instead of skipping the durable write) is what keeps the
    graph from silently running ahead of the durable source (spec BR2/D5).
    """

    store = resolve_cognitive_source_store()
    if store is None:
        raise CognitiveSourceError(
            "cognitive_source_store_absent",
            remediation=(
                "Register a CognitiveSourceStore adapter (community: "
                "sqlalchemy_kg_cognitive_source) via "
                "register_cognitive_source_store() before committing "
                "cognitive nodes."
            ),
        )
    return store


def reset_cognitive_source_store_for_tests() -> None:
    """Test-only: clear the registered store."""

    reset_runtime_values(_RUNTIME_KEY)
