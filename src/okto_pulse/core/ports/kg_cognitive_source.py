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
from typing import Any, Iterable, Literal, Mapping, Protocol, TypeVar, runtime_checkable

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

__all__ = [
    "COGNITIVE_SOURCE_FINGERPRINT_CONTRACT",
    "COGNITIVE_SOURCE_FINGERPRINT_CONTRACT_V3",
    "COGNITIVE_SOURCE_SEALED_BIRTH_FIELDS",
    "CognitiveSourceAppendDecision",
    "CognitiveSourceConflict",
    "CognitiveSourceError",
    "CognitiveSourcePersistedRevision",
    "CognitiveSourceRecord",
    "CognitiveSourceStore",
    "SealedBirthRestoration",
    "canonical_cognitive_source_fingerprint",
    "cognitive_source_semantic_key",
    "decide_cognitive_source_append",
    "latest_cognitive_source_records",
    "register_cognitive_source_store",
    "require_cognitive_source_store",
    "reset_cognitive_source_store_for_tests",
    "resolve_cognitive_source_store",
    "restore_sealed_birth_fields",
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


# Immutable predecessor identity used by Community schema convergence.  Keep
# historical contracts named: the moving head may advance, but an installed
# trigger must always be compared with the exact SQL generated for its epoch.
COGNITIVE_SOURCE_FINGERPRINT_CONTRACT_V3 = "cognitive-source-fingerprint/v3"
COGNITIVE_SOURCE_FINGERPRINT_CONTRACT = COGNITIVE_SOURCE_FINGERPRINT_CONTRACT_V3

#: Read-side usage statistics mutate without an attestation bump (every KG
#: query touches ``query_hits``/``last_queried_at``/``relevance_score``).
#: ``source_revision`` derives from ``attestation_count``, so including these
#: fields in the fingerprint turned any stat drift into a permanent
#: ``cognitive_source_replay_conflict`` poisoning the consolidation queue
#: (observed live on decision_059d5828). They describe USAGE of the
#: assertion, never the assertion itself; the stored payload keeps them for
#: literal rebuild restoration — only the identity fingerprint ignores them.
COGNITIVE_SOURCE_VOLATILE_USAGE_FIELDS: frozenset[str] = frozenset(
    {
        "last_attested_at",
        "last_queried_at",
        "last_recomputed_at",
        "pre_cancellation_relevance_score",
        "priority_boost",
        "query_hits",
        "relevance_score",
    }
)


#: Payload fields that record WHEN the assertion was first made, not what it
#: says. Consolidation re-derives a whole birth payload whenever it believes a
#: node is new — a legitimate belief once the graph projection has lost the
#: node (restore from an older copy, targeted removal, rebuild, DLQ replay).
#: The durable ledger still holds that birth, and BR2 makes the ledger the
#: source of truth, so its sealed value wins over the re-derived one. These
#: stay INSIDE the fingerprint: a birth stamp is part of the assertion's
#: identity, which is exactly why it may never be silently re-minted.
COGNITIVE_SOURCE_SEALED_BIRTH_FIELDS: frozenset[str] = frozenset({"created_at"})


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
    describe the append event, not the cognitive assertion. Read-side usage
    statistics (:data:`COGNITIVE_SOURCE_VOLATILE_USAGE_FIELDS`) are excluded
    for the same reason — they drift on every KG read without advancing
    ``attestation_count``/``source_revision``, and identity must be stable
    across such drift. ``sort_keys`` makes nested mapping order irrelevant
    while evidence order remains part of the literal evidence binding
    restored by replay.
    """

    if not isinstance(payload, Mapping):
        raise ValueError("cognitive source payload must be a mapping")
    identity_payload = {
        key: value
        for key, value in payload.items()
        if key not in COGNITIVE_SOURCE_VOLATILE_USAGE_FIELDS
    }
    canonical = {
        "contract": COGNITIVE_SOURCE_FINGERPRINT_CONTRACT,
        "board_id": str(board_id),
        "node_id": str(node_id),
        "node_type": str(node_type),
        "generation": int(generation),
        "payload": identity_payload,
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


@dataclass(frozen=True)
class CognitiveSourcePersistedRevision:
    """Storage-neutral identity of one already-durable ledger row.

    Adapters project only these three immutable fields when asking Core for an
    append decision. Payloads, SQL rows and transaction objects remain outside
    the policy boundary.
    """

    storage_id: str
    source_revision: int
    record_fingerprint: str

    def __post_init__(self) -> None:
        storage_id = str(self.storage_id).strip()
        revision = int(self.source_revision)
        fingerprint = str(self.record_fingerprint).lower()
        if not storage_id:
            raise ValueError("cognitive source storage_id must be non-empty")
        if revision < 0:
            raise ValueError("cognitive source_revision must be non-negative")
        if len(fingerprint) != 64 or any(
            character not in "0123456789abcdef" for character in fingerprint
        ):
            raise ValueError(
                "cognitive source record_fingerprint must be lowercase sha256"
            )
        object.__setattr__(self, "storage_id", storage_id)
        object.__setattr__(self, "source_revision", revision)
        object.__setattr__(self, "record_fingerprint", fingerprint)


@dataclass(frozen=True)
class CognitiveSourceAppendDecision:
    """Pure outcome for one append proposal against durable history."""

    outcome: Literal["append", "semantic_noop"]
    source_revision: int
    storage_id: str | None

    def __post_init__(self) -> None:
        revision = int(self.source_revision)
        if self.outcome not in {"append", "semantic_noop"}:
            raise ValueError("unsupported cognitive source append outcome")
        if revision < 0:
            raise ValueError("cognitive source_revision must be non-negative")
        if self.outcome == "append" and self.storage_id is not None:
            raise ValueError("append decisions cannot carry a storage_id")
        if self.outcome == "semantic_noop" and not str(self.storage_id or ""):
            raise ValueError("semantic no-op decisions require a storage_id")
        object.__setattr__(self, "source_revision", revision)


def decide_cognitive_source_append(
    *,
    persisted_revisions: Iterable[CognitiveSourcePersistedRevision],
    incoming_fingerprint: str,
) -> CognitiveSourceAppendDecision:
    """Resolve replay or allocate ``durable_high_water + 1``.

    The caller's projected ``source_revision`` is deliberately absent from
    this API. Relational history is the sole revision authority: an existing
    semantic fingerprint resolves to its oldest durable storage ID without a
    write; otherwise a new identity starts at revision zero and an existing
    identity advances monotonically from its highest durable revision.
    """

    fingerprint = str(incoming_fingerprint).lower()
    if len(fingerprint) != 64 or any(
        character not in "0123456789abcdef" for character in fingerprint
    ):
        raise ValueError("incoming cognitive fingerprint must be lowercase sha256")

    ordered = sorted(persisted_revisions, key=lambda item: item.source_revision)
    seen_revisions: set[int] = set()
    for item in ordered:
        if item.source_revision in seen_revisions:
            raise CognitiveSourceConflict(
                "cognitive_source_revision_conflict",
                remediation=(
                    "Repair duplicate durable source revisions before appending."
                ),
            )
        seen_revisions.add(item.source_revision)

    for item in ordered:
        if item.record_fingerprint == fingerprint:
            return CognitiveSourceAppendDecision(
                outcome="semantic_noop",
                source_revision=item.source_revision,
                storage_id=item.storage_id,
            )

    return CognitiveSourceAppendDecision(
        outcome="append",
        source_revision=(ordered[-1].source_revision + 1 if ordered else 0),
        storage_id=None,
    )


SemanticNodeKey = tuple[str, str, int]


def cognitive_source_semantic_key(record: Mapping[str, Any]) -> SemanticNodeKey:
    """Return the ``(node_type, node_id, generation)`` key of a record draft.

    Board scoping is implicit: one consolidation commit writes one board.
    """

    return (
        str(record.get("node_type") or ""),
        str(record.get("node_id") or ""),
        int(record.get("generation") or 0),
    )


@dataclass(frozen=True)
class SealedBirthRestoration:
    """One re-derived birth field replaced by its sealed durable value."""

    node_type: str
    node_id: str
    generation: int
    field: str
    rederived: Any
    sealed: Any


def restore_sealed_birth_fields(
    records: Iterable[Mapping[str, Any]],
    sealed_payloads: Mapping[SemanticNodeKey, Mapping[str, Any]],
) -> tuple[tuple[dict[str, Any], ...], tuple[SealedBirthRestoration, ...]]:
    """Re-seat re-derived birth fields on the values the ledger already sealed.

    Consolidation builds a fresh birth payload for every node it materializes,
    stamping ``created_at`` from the wall clock. That is right for a genuinely
    new assertion and wrong for one the durable ledger already recorded: the
    re-minted stamp changes the record fingerprint, so the append re-presents a
    sealed immutable revision with divergent content and fails closed
    (``cognitive_source_replay_conflict``), dead-lettering the consolidation.

    Restoring the sealed value makes the append byte-identical to the record
    already stored, so the existing idempotency path accepts it. Divergence in
    the assertion itself still changes the fingerprint and still fails closed —
    this narrows what counts as a replay, it does not weaken the guard.

    Returns the reconciled drafts plus every restoration performed, so the
    caller can log drift instead of healing it silently.
    """

    reconciled: list[dict[str, Any]] = []
    restorations: list[SealedBirthRestoration] = []
    for record in records:
        draft = dict(record)
        sealed = sealed_payloads.get(cognitive_source_semantic_key(draft))
        if not sealed:
            reconciled.append(draft)
            continue
        payload = dict(draft.get("payload") or {})
        changed = False
        for name in sorted(COGNITIVE_SOURCE_SEALED_BIRTH_FIELDS):
            if name not in sealed or name not in payload:
                continue
            if payload[name] == sealed[name]:
                continue
            restorations.append(
                SealedBirthRestoration(
                    node_type=str(draft.get("node_type") or ""),
                    node_id=str(draft.get("node_id") or ""),
                    generation=int(draft.get("generation") or 0),
                    field=name,
                    rederived=payload[name],
                    sealed=sealed[name],
                )
            )
            payload[name] = sealed[name]
            changed = True
        if changed:
            draft["payload"] = payload
        reconciled.append(draft)
    return tuple(reconciled), tuple(restorations)


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

        MUST resolve a fingerprint already durable for the same semantic
        identity to its oldest storage id without growing the ledger. New
        semantics allocate from durable high-water; the caller's projected
        revision is advisory only. Store corruption raises
        :class:`CognitiveSourceConflict`; unavailability raises
        :class:`CognitiveSourceError`.
        """
        ...

    async def append_many(
        self, records: tuple[CognitiveSourceRecord, ...]
    ) -> tuple[str, ...]:
        """Atomically persist a batch and return ids in input order.

        Semantic replays are no-ops even when their projected revision differs.
        Distinct new fingerprints allocate consecutive durable revisions in
        stable input order; partial batch persistence is forbidden.
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

    async def sealed_birth_payloads_in_context(
        self,
        context: object,
        board_id: str,
        keys: tuple[SemanticNodeKey, ...],
    ) -> Mapping[SemanticNodeKey, Mapping[str, Any]]:
        """Return the EARLIEST sealed payload for each ``(type, id, generation)``.

        The earliest record is the one that carries the assertion's birth, so
        this is what :func:`restore_sealed_birth_fields` reconciles against.
        Keys with no durable history are simply absent from the result — that
        is a genuinely new node, not a failure.

        Implementations MUST read through ``context`` for the same reason
        :meth:`append_many_in_context` does: opening a second connection can
        self-deadlock against the caller's SQLite snapshot/writer slot. This
        operation never writes and never completes the outer UOW.
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
