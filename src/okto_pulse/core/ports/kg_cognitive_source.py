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
    binding + generation) BEFORE the commit reports success.  The embedded
    graph and relational store form a compensated saga: a bounded graph-ahead
    window exists after graph close, and append failure requests best-effort
    graph compensation before returning a stable fail-closed error;
  * records are immutable — never UPDATEd or DELETEd by this port;
  * ``enumerate`` returns a deterministic ordering (committed_at, node_id,
    generation, source_revision) so the rebuild manifest hash is stable
    (spec TR5);
  * replay consumers restore records literally: no LLM, evidence binding
    preserved, ``human_curated`` content never clobbered (spec BR3).

Pure: stdlib ``dataclasses`` / ``typing`` only. It does NOT import
SQLAlchemy, engines or ``okto_pulse.community`` — the concrete Community
adapter (``sqlalchemy_kg_cognitive_source``) owns those.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol, TypeVar, runtime_checkable

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

__all__ = [
    "CognitiveSourceConflict",
    "CognitiveSourceError",
    "CognitiveSourceRecord",
    "CognitiveSourceStore",
    "canonical_cognitive_source_fingerprint",
    "latest_cognitive_source_records",
    "register_cognitive_source_store",
    "require_cognitive_source_store",
    "reset_cognitive_source_store_for_tests",
    "resolve_cognitive_source_store",
]


class CognitiveSourceError(Exception):
    """Structured, fail-closed cognitive-source failure.

    Surfaced by the consolidation commit as the stable error code
    ``kg_cognitive_source_unavailable`` (spec AC3): the commit MUST abort —
    a cognitive commit may never report success without its durable record.
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


class CognitiveSourceConflict(CognitiveSourceError):
    """Immutable replay conflict for one scoped semantic revision key.

    Idempotency is valid only when the already-durable semantic record is the
    same one.  Reusing a key with a different board, type, payload or evidence
    is an integrity failure, not a successful retry.
    """


def canonical_cognitive_source_fingerprint(
    *,
    board_id: str,
    node_id: str,
    node_type: str,
    generation: int,
    payload: Mapping[str, Any],
    evidence_refs: Iterable[str] = (),
) -> str:
    """Return the immutable semantic fingerprint for one source revision.

    Revision/session/timestamp/metadata are intentionally excluded: they
    describe the append event, not the cognitive assertion. ``sort_keys``
    makes nested mapping order irrelevant while evidence order remains part
    of the literal evidence binding restored by replay.
    """

    if not isinstance(payload, Mapping):
        raise ValueError("cognitive source payload must be a mapping")
    canonical = {
        "board_id": str(board_id),
        "node_id": str(node_id),
        "node_type": str(node_type),
        "generation": int(generation),
        "payload": dict(payload),
        "evidence_refs": [str(ref) for ref in evidence_refs],
    }
    return hashlib.sha256(
        json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


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
    source_revision: int = 0
    record_fingerprint: str = ""

    def __post_init__(self) -> None:
        revision = int(self.source_revision)
        if revision < 0:
            raise ValueError("cognitive source_revision must be non-negative")
        evidence_refs = tuple(str(ref) for ref in self.evidence_refs)
        fingerprint = canonical_cognitive_source_fingerprint(
            board_id=self.board_id,
            node_id=self.node_id,
            node_type=self.node_type,
            generation=self.generation,
            payload=self.payload,
            evidence_refs=evidence_refs,
        )
        supplied = str(self.record_fingerprint or "")
        if supplied and supplied != fingerprint:
            raise ValueError(
                "cognitive source record_fingerprint does not match its "
                "canonical semantic content"
            )
        object.__setattr__(self, "source_revision", revision)
        object.__setattr__(self, "evidence_refs", evidence_refs)
        object.__setattr__(self, "record_fingerprint", fingerprint)


_CognitiveRecordT = TypeVar("_CognitiveRecordT")


def _record_value(record: object, field_name: str, default: object = None) -> object:
    if isinstance(record, Mapping):
        return record.get(field_name, default)
    return getattr(record, field_name, default)


def _record_payload(record: object) -> Mapping[str, Any]:
    payload = _record_value(record, "payload", {})
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, Mapping):
        raise ValueError("cognitive source payload must be a mapping")
    return payload


def _record_evidence_refs(record: object) -> tuple[str, ...]:
    refs = _record_value(record, "evidence_refs", ()) or ()
    if isinstance(refs, str):
        parsed = json.loads(refs)
        refs = parsed if isinstance(parsed, list) else (parsed,)
    return tuple(str(ref) for ref in refs)


def _verified_record_fingerprint(record: object) -> str:
    fingerprint = canonical_cognitive_source_fingerprint(
        board_id=str(_record_value(record, "board_id", "") or ""),
        node_id=str(_record_value(record, "node_id", "") or ""),
        node_type=str(_record_value(record, "node_type", "") or ""),
        generation=int(_record_value(record, "generation", 0) or 0),
        payload=_record_payload(record),
        evidence_refs=_record_evidence_refs(record),
    )
    supplied = str(_record_value(record, "record_fingerprint", "") or "")
    if supplied and supplied != fingerprint:
        raise CognitiveSourceConflict(
            "cognitive_source_fingerprint_mismatch",
            board_id=str(_record_value(record, "board_id", "") or "") or None,
            node_id=str(_record_value(record, "node_id", "") or "") or None,
            remediation=(
                "Treat the durable cognitive ledger as corrupted and repair "
                "the immutable revision before replay/rebuild."
            ),
        )
    return fingerprint


def latest_cognitive_source_records(
    records: Iterable[_CognitiveRecordT],
) -> tuple[_CognitiveRecordT, ...]:
    """Collapse an append-only ledger to its latest semantic revisions.

    The fully scoped semantic identity is ``(board_id, node_type, node_id,
    generation)``. Repeated rows for the same revision are accepted only when
    their canonical fingerprints match; divergent duplicates are
    durable-source corruption and fail closed instead of making replay depend
    on database row order.
    """

    latest: dict[tuple[str, str, str, int], tuple[int, _CognitiveRecordT]] = {}
    seen_revisions: dict[tuple[str, str, str, int, int], str] = {}
    for record in records:
        board_id = str(_record_value(record, "board_id", "") or "")
        node_id = str(_record_value(record, "node_id", "") or "")
        node_type = str(_record_value(record, "node_type", "") or "")
        generation = int(_record_value(record, "generation", 0) or 0)
        revision = int(_record_value(record, "source_revision", 0) or 0)
        if revision < 0:
            raise CognitiveSourceConflict(
                "cognitive_source_revision_invalid",
                board_id=board_id or None,
                node_id=node_id or None,
            )
        fingerprint = _verified_record_fingerprint(record)
        revision_key = (board_id, node_type, node_id, generation, revision)
        prior_fingerprint = seen_revisions.get(revision_key)
        if prior_fingerprint is not None and prior_fingerprint != fingerprint:
            raise CognitiveSourceConflict(
                "cognitive_source_revision_conflict",
                board_id=board_id or None,
                node_id=node_id or None,
                remediation=(
                    "Two immutable rows claim the same source revision with "
                    "different semantic content; repair the ledger before "
                    "replay/rebuild."
                ),
            )
        seen_revisions[revision_key] = fingerprint

        semantic_key = (board_id, node_type, node_id, generation)
        current = latest.get(semantic_key)
        if current is None or revision > current[0]:
            latest[semantic_key] = (revision, record)

    selected = [entry[1] for entry in latest.values()]
    selected.sort(
        key=lambda record: (
            str(_record_value(record, "committed_at", "") or ""),
            str(_record_value(record, "node_id", "") or ""),
            int(_record_value(record, "generation", 0) or 0),
            int(_record_value(record, "source_revision", 0) or 0),
        )
    )
    return tuple(selected)


@runtime_checkable
class CognitiveSourceStore(Protocol):
    """Append-only durable store for cognitive node records."""

    async def append(self, record: CognitiveSourceRecord) -> str:
        """Persist ``record`` and return its storage id.

        MUST be idempotent per ``(node_id, generation, source_revision)`` only
        for the same semantic record, so a commit retry after a transient
        failure is safe. Divergent reuse raises
        :class:`CognitiveSourceConflict`; store unavailability raises
        :class:`CognitiveSourceError`. Callers abort the commit in both cases
        (fail-closed, spec D5).
        """
        ...

    async def append_many(
        self, records: tuple[CognitiveSourceRecord, ...]
    ) -> tuple[str, ...]:
        """Atomically persist a batch and return ids in input order.

        A retry with the same ``(node_id, generation, source_revision)`` and
        semantic content is idempotent. A divergent replay MUST raise
        :class:`CognitiveSourceConflict`; partial batch persistence is
        forbidden.
        """
        ...

    async def append_many_in_context(
        self,
        context: object,
        records: tuple[CognitiveSourceRecord, ...],
    ) -> tuple[str, ...]:
        """Stage an atomic batch in the caller-owned relational UOW.

        Implementations MUST use ``context`` directly: opening a second
        connection can self-deadlock when the caller already owns SQLite's
        snapshot/writer slot. This operation never commits or rolls back the
        outer UOW; transaction completion remains the caller's responsibility.
        A raised exception makes the complete outer UOW uncommittable by the
        consolidation contract, so partial batch publication is forbidden.
        """
        ...

    async def enumerate(self, board_id: str) -> tuple[CognitiveSourceRecord, ...]:
        """Return every record for ``board_id`` in the deterministic order
        ``(committed_at, node_id, generation, source_revision)`` (spec TR5). Raises
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
    graph commit from silently reporting success ahead of the durable source
    (spec BR2/D5).
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
