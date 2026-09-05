"""R2-IMP1 — internal audited stale-canonical reconciler (deterministic/SDLC).

Demotes / marks-stale the canonical publication of DETERMINISTIC/SDLC board-graph
nodes whose owning source is no longer canonical-eligible (e.g. spec/card
``done -> draft``, ``superseded``, expired working TTL, source deleted). Cognitive
canonical nodes (``Learning``/``Alternative``/``Assumption``, or any
cognitive-authored node) are NEVER demoted to working: they are preserved, and a
*material* bug-derived ``Learning`` whose Bug source regressed is routed to the
EXISTING R7 ``CanonicalDebt`` contract (no new taxonomy, idempotent).

Design constraints (R2-IMP1 spec 9aedfe78 / card affd0444):
- TR1: reuse ``classify_source_for_kg`` + the BoardSourceReader port as the maturity
  source of truth.
- TR2: internal audited primitive; NOT exposed as an MCP mutating tool. Preserves
  ``original_node_id`` / ``source_artifact_ref``. A missing governed source is
  always converged to an inactive tombstone (revocation + zero relevance),
  whether found by the targeted fast path or by a full sweep.
- TR7: idempotent / convergent — canonical nodes and interrupted working
  tombstones are acted on; complete tombstones are a NOOP. Fast-path
  (``source_refs``) and full sweep (``source_refs=None``) converge to the same
  final state (AC8).
- TR8 / FR8 / AC9: cognitive/canonical-origin nodes (incl. bug-derived Learning)
  are excluded from demotion-to-working.

Global Discovery digest convergence (FR7) is left as a HOOK for R2-IMP5; this
card does not touch the R1 parity primitive directly.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable

from okto_pulse.core.application.rebuild_ports import BoardSourceSnapshotCause
from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.canonical_learning_partition import (
    _bug_artifact_id,
    _is_bug_derived_ref,
)
from okto_pulse.core.kg.connectivity_guard import WriterClass, classify_writer_path
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.relational_projection import (
    is_relational_projection_node,
)
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.kg.source_maturity import (
    DEFAULT_WORKING_TTL_DAYS,
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_WORKING_STALE,
    SourceMaturityClassification,
    classify_source_for_kg,
)
from okto_pulse.core.ports.runtime_workers import BlockingExecutionPort
from okto_pulse.core.ports.stale_sweep import (
    GOVERNED_SWEEP_ARTIFACT_TYPES,
    StaleSweepCandidate,
)

logger = logging.getLogger("okto_pulse.kg.canonical_stale_reconciler")

# Cognitive-origin node types: never demoted to working by R2 (carve-out).
COGNITIVE_NODE_TYPES: frozenset[str] = frozenset(
    {"Learning", "Alternative", "Assumption"}
)

# Explicit policy is intentionally separate from the schema vocabulary. A new
# physical node type must choose a reconcile policy before the coverage gate
# lets it enter production.
STALE_RECONCILE_NODE_POLICY = MappingProxyType(
    {
        "Decision": "demote_deterministic",
        "Criterion": "demote_deterministic",
        "Constraint": "demote_deterministic",
        "Assumption": "preserve_cognitive",
        "Requirement": "demote_deterministic",
        "Entity": "demote_deterministic",
        "APIContract": "demote_deterministic",
        "TestScenario": "demote_deterministic",
        "Bug": "demote_deterministic",
        "Learning": "preserve_cognitive",
        "Alternative": "preserve_cognitive",
    }
)


def validate_stale_reconcile_ontology_coverage(
    node_types: tuple[str, ...] = NODE_TYPES,
    policy: Mapping[str, str] = STALE_RECONCILE_NODE_POLICY,
) -> tuple[str, ...]:
    """Fail when schema node types and stale-reconcile policies diverge."""

    schema = tuple(node_types)
    missing = tuple(node_type for node_type in schema if node_type not in policy)
    extra = tuple(node_type for node_type in policy if node_type not in schema)
    misclassified = tuple(
        node_type
        for node_type in schema
        if node_type in policy
        and policy[node_type]
        != (
            "preserve_cognitive"
            if node_type in COGNITIVE_NODE_TYPES
            else "demote_deterministic"
        )
    )
    if missing or extra or misclassified:
        raise RuntimeError(
            "stale_reconcile_ontology_coverage_mismatch "
            f"missing={list(missing)} extra={list(extra)} "
            f"misclassified={list(misclassified)}"
        )
    return schema


# Scan order and membership come from the canonical schema contract.
ALL_NODE_TYPES: tuple[str, ...] = validate_stale_reconcile_ontology_coverage()

# source_artifact_refs that are NOT SDLC sources (infra roots) — never demoted.
_SKIP_REFS: frozenset[str] = frozenset({"tech_entities.yml"})
_SKIP_REF_PREFIXES: tuple[str, ...] = ("board:",)
_SOURCE_REF_TYPES: frozenset[str] = frozenset(
    {
        "story",
        "ideation",
        "spec",
        "refinement",
        "sprint",
        "card",
        "task",
        "test",
        "bug",
        "amendment_hotfix_revision",
        "decision",
    }
)

# Ownership is type-qualified.  UUIDs are not globally unique across SDLC
# tables, so reducing a source reference to only its id can let (for example) a
# live ``card:X`` mask a deleted ``spec:X``.  Card subtypes are one governed
# owner family because their durable rows are stored in ``cards`` while graph
# projections use both ``card:`` and ``card_relationship_target:`` refs.
SourceIdentity = tuple[str, str]
_SOURCE_OWNER_FAMILY = MappingProxyType(
    {
        "card": "card",
        "card_relationship_target": "card",
        "task": "card",
        "test": "card",
        "bug": "card",
    }
)

# DTO-only result action labels (NEVER persisted enums).
ACTION_DEMOTED = "demoted_to_working"
ACTION_SKIPPED_COGNITIVE = "skipped_cognitive"
ACTION_ROUTED_DEBT = "routed_to_canonical_debt"
SOURCE_DELETED_REVOCATION_REASON = "source_deleted"
ERASED_SEMANTIC_TEXT = ""


def _semantic_payload_is_erased(
    *,
    title: Any,
    content: Any,
    context: Any,
    justification: Any,
    source_span_quote: Any,
) -> bool:
    """Whether a governed-deletion tombstone retains no semantic payload.

    A tombstone keeps only lineage/audit identity.  In particular, merely
    demoting a node to the working layer is not erasure: raw administrative
    Cypher reads may explicitly include that layer.
    """

    return (
        not str(title or "")
        and not str(content or "")
        and not str(context or "")
        and not str(justification or "")
        and not str(source_span_quote or "")
    )


@dataclass
class StaleReconcileResult:
    board_id: str
    correlation_id: str
    scanned: int = 0
    demoted: list[dict[str, Any]] = field(default_factory=list)
    skipped_cognitive: list[dict[str, Any]] = field(default_factory=list)
    routed_to_debt: list[dict[str, Any]] = field(default_factory=list)
    global_sync_enqueued: bool = False
    incomplete: bool = False
    incomplete_cause: BoardSourceSnapshotCause | str | None = None
    failed_types: list[str] = field(default_factory=list)
    # Per-type receipt: ``scanned`` counts canonical candidates, while
    # ``completed`` records that the type query finished successfully even
    # when it returned zero candidates. Query failures are represented only
    # by ``failed_types``/``incomplete``.
    scanned_by_type: dict[str, int] = field(
        default_factory=lambda: {node_type: 0 for node_type in ALL_NODE_TYPES}
    )
    completed_types: list[str] = field(default_factory=list)
    # Target outcome counters are populated only for the event fast-path
    # (``source_refs`` is not None).  They make a retry distinguish a graph
    # projection that was mutated now from one already converged by an earlier
    # graph commit, while keeping preserved cognitive material explicit.
    target_identity_count: int = 0
    target_found_count: int = 0
    target_demoted_count: int = 0
    target_already_converged_count: int = 0
    target_skipped_cognitive_count: int = 0
    target_preserved_canonical_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "correlation_id": self.correlation_id,
            "scanned": self.scanned,
            "demoted": self.demoted,
            "skipped_cognitive": self.skipped_cognitive,
            "routed_to_debt": self.routed_to_debt,
            "demoted_count": len(self.demoted),
            "skipped_cognitive_count": len(self.skipped_cognitive),
            "routed_to_debt_count": len(self.routed_to_debt),
            "global_sync_enqueued": self.global_sync_enqueued,
            "incomplete": self.incomplete,
            "incomplete_cause": self.incomplete_cause,
            "failed_types": list(self.failed_types),
            "scanned_by_type": dict(self.scanned_by_type),
            "completed_types": list(self.completed_types),
            "target_identity_count": self.target_identity_count,
            "target_found_count": self.target_found_count,
            "target_demoted_count": self.target_demoted_count,
            "target_already_converged_count": (self.target_already_converged_count),
            "target_skipped_cognitive_count": (self.target_skipped_cognitive_count),
            "target_preserved_canonical_count": (self.target_preserved_canonical_count),
        }


@dataclass(frozen=True, slots=True)
class StaleSweepPage:
    """Bounded, stable page of canonical sources absent from durable state."""

    board_id: str
    cursor: str
    next_cursor: str
    budget: int
    candidates: tuple[StaleSweepCandidate, ...]
    has_more: bool
    complete: bool
    incomplete_cause: BoardSourceSnapshotCause | str | None = None
    graph_rows_scanned: int = 0
    failed_types: tuple[str, ...] = ()


def _source_identity_from_ref(ref: str) -> SourceIdentity | None:
    """Return the governed ``(owner_family, artifact_id)`` for a source ref.

    Child refs such as ``spec:{id}:criterion:{criterion_id}`` retain their
    owning spec identity.  Infrastructure and unknown ref families are
    deliberately unowned so a full sweep never treats them as absent SQL
    artifacts.
    """

    if not isinstance(ref, str) or not ref or ref != ref.strip() or ref in _SKIP_REFS:
        return None
    if any(ref.startswith(prefix) for prefix in _SKIP_REF_PREFIXES):
        return None
    parts = ref.split(":")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        return None
    source_type = parts[0]
    if source_type not in _SOURCE_REF_TYPES and source_type not in _SOURCE_OWNER_FAMILY:
        return None
    return (_SOURCE_OWNER_FAMILY.get(source_type, source_type), parts[1])


def _owning_source_id(ref: str) -> str | None:
    """Extract the owning artifact id from a deterministic node's
    ``source_artifact_ref`` (``spec:{id}``, ``spec:{id}:fr:..``, ``card:{id}`` ...).
    Returns None for infra roots (board:/tech_entities) or malformed refs."""
    identity = _source_identity_from_ref(ref)
    return identity[1] if identity is not None else None


def encode_stale_sweep_cursor(candidate: StaleSweepCandidate) -> str:
    """Encode a stable opaque cursor ordered by ``(type, id)``."""

    return json.dumps(
        [candidate.artifact_type, candidate.artifact_id],
        ensure_ascii=True,
        separators=(",", ":"),
    )


def decode_stale_sweep_cursor(cursor: str) -> tuple[str, str] | None:
    """Decode the public string cursor; only ``""`` means the beginning."""

    if not isinstance(cursor, str):
        raise ValueError("stale_sweep_cursor_invalid")
    if cursor == "":
        return None
    try:
        raw = json.loads(cursor)
    except (TypeError, ValueError) as exc:
        raise ValueError("stale_sweep_cursor_invalid") from exc
    if (
        not isinstance(raw, list)
        or len(raw) != 2
        or raw[0] not in GOVERNED_SWEEP_ARTIFACT_TYPES
        or not isinstance(raw[1], str)
        or not raw[1]
        or raw[1] != raw[1].strip()
    ):
        raise ValueError("stale_sweep_cursor_invalid")
    return (str(raw[0]), raw[1])


def _source_ids_from_refs(
    source_refs: list[str] | None,
) -> set[SourceIdentity] | None:
    if source_refs is None:
        return None
    if not source_refs:
        raise ValueError("invalid_source_refs: empty list cannot authorize a sweep")
    identities: set[SourceIdentity] = set()
    for ref in source_refs:
        if not isinstance(ref, str) or ref != ref.strip():
            raise ValueError(f"invalid_source_refs: malformed ref {ref!r}")
        parts = ref.split(":")
        if len(parts) < 2 or parts[0] not in _SOURCE_REF_TYPES or not parts[1]:
            raise ValueError(f"invalid_source_refs: malformed ref {ref!r}")
        identity = _source_identity_from_ref(ref)
        if identity is None:
            raise ValueError(f"invalid_source_refs: unsupported ref {ref!r}")
        identities.add(identity)
    return identities


def _build_source_classification_map(
    board_id: str,
) -> tuple[
    dict[SourceIdentity, SourceMaturityClassification],
    bool,
    BoardSourceSnapshotCause | None,
]:
    """Type-qualified source classifications from the registered reader."""
    reader = get_kg_registry().require_board_source_reader()
    snapshot = reader.fetch(board_id)
    if not snapshot.complete:
        return {}, False, snapshot.cause
    out: dict[SourceIdentity, SourceMaturityClassification] = {}
    for row in snapshot.rows:
        sid = str(row.get("id") or "")
        artifact_type = str(row.get("artifact_type") or "").strip().lower()
        # BoardSourceReader is intentionally broader than the stale-sweep
        # domain: it also returns stories, derived decisions and code-
        # traceability sources.  Those rows do not own any identity that the
        # bounded graph query below is allowed to reconcile.  Ignore them by
        # the same explicit allow-list used by StaleSweepCandidate instead of
        # treating a valid non-governed source_ref as an incomplete realm.
        #
        # This is load-bearing for forward-compatible readers.  A newly added
        # source family must not poison every board's daily sweep merely
        # because this narrower reconciler has no policy for it yet.
        if artifact_type and artifact_type not in GOVERNED_SWEEP_ARTIFACT_TYPES:
            continue
        if not sid or not artifact_type:
            logger.warning(
                "kg.stale.source_snapshot_row_invalid board=%s reason=missing_identity",
                board_id,
            )
            return {}, False, "realm_incomplete"
        source_ref = str(row.get("source_ref") or f"{artifact_type}:{sid}")
        identity = _source_identity_from_ref(source_ref)
        if identity is None or identity[1] != sid:
            logger.warning(
                "kg.stale.source_snapshot_row_invalid board=%s ref=%s id=%s",
                board_id,
                source_ref,
                sid,
            )
            return {}, False, "realm_incomplete"
        ttl_raw = row.get("working_ttl_days")
        working_ttl_days = (
            int(ttl_raw) if ttl_raw is not None else DEFAULT_WORKING_TTL_DAYS
        )
        classification = classify_source_for_kg(
            artifact_type=artifact_type,
            artifact_status=row.get("source_artifact_status") or row.get("status"),
            content_hash=row.get("content_hash"),
            updated_at=row.get("updated_at"),
            working_ttl_days=working_ttl_days,
            has_minimal_evidence=bool(row.get("has_minimal_evidence", True)),
            lineage_complete=bool(row.get("lineage_complete", True)),
        )
        out[identity] = classification
    return out, True, None


async def enumerate_stale_sweep_page(
    board_id: str,
    *,
    cursor: str,
    budget: int,
) -> StaleSweepPage:
    """Return one stable page from the active/source anti-join.

    Canonical nodes and incomplete working tombstones whose governed owner is
    absent from a *complete* relational snapshot become catch-up work. Existing
    but regressed sources are intentionally not tombstoned. One cross-label
    keyset query materializes at most ``budget + 1`` distinct owner identities,
    which bounds both graph scanning and the resulting relational batch.
    """

    if (
        not board_id
        or isinstance(budget, bool)
        or not isinstance(budget, int)
        or budget < 1
    ):
        raise ValueError("stale_sweep_page_request_invalid")
    after = decode_stale_sweep_cursor(cursor)
    try:
        source_by_identity, source_complete, incomplete_cause = (
            _build_source_classification_map(board_id)
        )
    except Exception:
        logger.exception(
            "kg.stale_sweep.source_snapshot_failed board=%s",
            board_id,
        )
        return StaleSweepPage(
            board_id=board_id,
            cursor=cursor,
            next_cursor=cursor,
            budget=budget,
            candidates=(),
            has_more=False,
            complete=False,
            incomplete_cause="source_snapshot_unavailable",
        )
    if not source_complete:
        return StaleSweepPage(
            board_id=board_id,
            cursor=cursor,
            next_cursor=cursor,
            budget=budget,
            candidates=(),
            has_more=False,
            complete=False,
            incomplete_cause=incomplete_cause,
        )

    registry = get_kg_registry()
    try:
        graph_available = registry.graph_runtime_store.exists(board_id)
    except Exception:
        logger.exception(
            "kg.stale_sweep.graph_runtime_probe_failed board=%s",
            board_id,
        )
        return StaleSweepPage(
            board_id=board_id,
            cursor=cursor,
            next_cursor=cursor,
            budget=budget,
            candidates=(),
            has_more=False,
            complete=False,
            incomplete_cause="graph_runtime_probe_failed",
        )
    if not graph_available:
        return StaleSweepPage(
            board_id=board_id,
            cursor=cursor,
            next_cursor=cursor,
            budget=budget,
            candidates=(),
            has_more=False,
            complete=False,
            incomplete_cause="graph_unavailable",
        )

    scan_limit = budget + 1
    after_type, after_id = after or ("", "")
    # Ladybug/Kuzu list indexes are one-based. Normalizing card subtypes in the
    # query makes DISTINCT and keyset ordering operate on the governed owner,
    # rather than on child source-ref strings such as ``test:{card_id}:...``.
    query = """
        MATCH (n)
        WHERE (
            n.graph_layer = $canonical
            OR (
                n.graph_layer = $working
                AND (
                    coalesce(n.maturity_status, '') <> $working_stale
                    OR coalesce(n.revocation_reason, '') <> $source_deleted
                    OR n.relevance_score IS NULL
                    OR n.relevance_score > $deleted_relevance
                    OR coalesce(n.title, '') <> ''
                    OR coalesce(n.content, '') <> ''
                    OR coalesce(n.context, '') <> ''
                    OR coalesce(n.justification, '') <> ''
                    OR coalesce(n.source_span_quote, '') <> ''
                )
            )
        )
          AND n.source_artifact_ref IS NOT NULL
        WITH string_split(n.source_artifact_ref, ':') AS parts
        WHERE size(parts) >= 2
        WITH CASE parts[1]
          WHEN 'card' THEN 'card'
          WHEN 'card_relationship_target' THEN 'card'
          WHEN 'task' THEN 'card'
          WHEN 'test' THEN 'card'
          WHEN 'bug' THEN 'card'
          ELSE parts[1]
        END AS artifact_type, parts[2] AS artifact_id
        WHERE artifact_type IN $governed_types
          AND artifact_id IS NOT NULL
          AND artifact_id <> ''
          AND (
            $after_type = ''
            OR artifact_type > $after_type
            OR (artifact_type = $after_type AND artifact_id > $after_id)
          )
        RETURN DISTINCT artifact_type, artifact_id
        ORDER BY artifact_type ASC, artifact_id ASC
        LIMIT $scan_limit
    """
    params = {
        "canonical": GRAPH_LAYER_CANONICAL,
        "working": GRAPH_LAYER_WORKING,
        "working_stale": MATURITY_WORKING_STALE,
        "source_deleted": SOURCE_DELETED_REVOCATION_REASON,
        "deleted_relevance": 0.0,
        "governed_types": sorted(GOVERNED_SWEEP_ARTIFACT_TYPES),
        "after_type": after_type,
        "after_id": after_id,
        "scan_limit": scan_limit,
    }
    try:
        async with await registry.graph_transaction.begin(board_id) as scope:
            rows = list(scope.execute(query, params).rows)
        if len(rows) > scan_limit:
            raise RuntimeError("stale_sweep_scan_limit_exceeded")
        scanned_identities = tuple(
            StaleSweepCandidate(str(row[0] or ""), str(row[1] or "")) for row in rows
        )
        if scanned_identities != tuple(sorted(set(scanned_identities))):
            raise RuntimeError("stale_sweep_scan_order_invalid")
    except Exception:
        logger.exception(
            "kg.stale_sweep.scan_failed board=%s",
            board_id,
        )
        return StaleSweepPage(
            board_id=board_id,
            cursor=cursor,
            next_cursor=cursor,
            budget=budget,
            candidates=(),
            has_more=False,
            complete=False,
            incomplete_cause="graph_scan_incomplete",
            graph_rows_scanned=0,
            failed_types=ALL_NODE_TYPES,
        )

    has_more = len(scanned_identities) > budget
    scanned_page = scanned_identities[:budget]
    candidates = tuple(
        candidate
        for candidate in scanned_page
        if (candidate.artifact_type, candidate.artifact_id) not in source_by_identity
    )
    # The checkpoint follows the graph inventory, not only missing sources.
    # This guarantees progress through pages containing exclusively live rows
    # while the complete snapshot and the adapter's writer-slot recheck prevent
    # synthetic tombstones for recreated sources.
    next_cursor = (
        encode_stale_sweep_cursor(scanned_page[-1]) if scanned_page else cursor
    )
    return StaleSweepPage(
        board_id=board_id,
        cursor=cursor,
        next_cursor=next_cursor,
        budget=budget,
        candidates=candidates,
        has_more=has_more,
        complete=True,
        graph_rows_scanned=len(scanned_identities),
    )


def _record_failed_type(
    result: StaleReconcileResult,
    node_type: str,
) -> None:
    result.incomplete = True
    if node_type not in result.failed_types:
        result.failed_types.append(node_type)


def _is_cognitive(node_type: str, created_by_agent: str) -> bool:
    if node_type in COGNITIVE_NODE_TYPES:
        return True
    return classify_writer_path(str(created_by_agent or "")) == WriterClass.COGNITIVE


async def _scan_and_demote(
    board_id: str,
    source_by_identity: dict[SourceIdentity, SourceMaturityClassification],
    target_identities: set[SourceIdentity] | None,
    correlation_id: str,
    result: StaleReconcileResult,
) -> list[dict[str, Any]]:
    """Demote stale deterministic nodes and repair incomplete tombstones.

    Cognitive bug-derived material cases are collected as debt-routing intents
    (handled async by the caller). Returns the list of debt intents.
    """
    cognitive_debt_intents: list[dict[str, Any]] = []
    transaction = get_kg_registry().graph_transaction
    async with await transaction.begin(board_id) as scope:
        for ntype in ALL_NODE_TYPES:
            try:
                # Both modes inspect working nodes so a crash after the layer
                # demotion but before the tombstone fields were persisted can
                # be repaired. Canonical-only scans would permanently lose
                # sight of that incomplete state.
                res = scope.execute(
                    f"MATCH (n:{ntype}) "
                    "WHERE n.graph_layer = $c OR n.graph_layer = $w "
                    f"RETURN n.id, n.source_artifact_ref, n.created_by_agent, "
                    f"n.maturity_status, n.graph_layer, "
                    f"n.revocation_reason, n.relevance_score, "
                    f"n.title, n.content, n.context, n.justification, "
                    f"n.source_span_quote, n.embedding IS NULL, "
                    f"n.source_content_hash",
                    {
                        "c": GRAPH_LAYER_CANONICAL,
                        "w": GRAPH_LAYER_WORKING,
                    },
                )
                rows: list[
                    tuple[
                        str,
                        str,
                        str,
                        str,
                        str,
                        str | None,
                        float | None,
                        Any,
                        Any,
                        Any,
                        Any,
                        Any,
                        Any,
                        Any,
                    ]
                ] = []
                for row in res.rows:
                    rows.append(
                        (
                            str(row[0]),
                            str(row[1] or ""),
                            str(row[2] or ""),
                            str(row[3] or ""),
                            str(row[4] or ""),
                            (
                                str(row[5])
                                if len(row) > 5 and row[5] is not None
                                else None
                            ),
                            (
                                float(row[6])
                                if len(row) > 6 and row[6] is not None
                                else None
                            ),
                            row[7] if len(row) > 7 else None,
                            row[8] if len(row) > 8 else None,
                            row[9] if len(row) > 9 else None,
                            row[10] if len(row) > 10 else None,
                            row[11] if len(row) > 11 else None,
                            row[12] if len(row) > 12 else None,
                            row[13] if len(row) > 13 else None,
                        )
                    )
                for (
                    node_id,
                    ref,
                    writer,
                    cur_maturity,
                    graph_layer,
                    revocation_reason,
                    relevance_score,
                    title,
                    content,
                    context,
                    justification,
                    source_span_quote,
                    embedding_absent,
                    source_content_hash,
                ) in rows:
                    if graph_layer == GRAPH_LAYER_CANONICAL:
                        # Preserve the established meaning of ``scanned``: it
                        # counts canonical candidates, not the working rows
                        # additionally inspected to prove retry convergence.
                        result.scanned += 1
                        result.scanned_by_type[ntype] += 1
                    source_identity = _source_identity_from_ref(ref)
                    if target_identities is not None:
                        if source_identity not in target_identities:
                            continue  # outside the governed event scope
                        result.target_found_count += 1
                    tombstone_converged = (
                        graph_layer == GRAPH_LAYER_WORKING
                        and cur_maturity == MATURITY_WORKING_STALE
                        and revocation_reason == SOURCE_DELETED_REVOCATION_REASON
                        and relevance_score is not None
                        and relevance_score <= 0.0
                        and _semantic_payload_is_erased(
                            title=title,
                            content=content,
                            context=context,
                            justification=justification,
                            source_span_quote=source_span_quote,
                        )
                    )
                    if tombstone_converged:
                        if (
                            source_identity is not None
                            and source_by_identity.get(source_identity) is None
                        ):
                            replace_tombstone = getattr(
                                scope,
                                "replace_with_source_deleted_tombstone",
                                None,
                            )
                            if callable(replace_tombstone):
                                if not replace_tombstone(
                                    ntype,
                                    node_id,
                                    graph_layer=GRAPH_LAYER_WORKING,
                                    maturity_status=MATURITY_WORKING_STALE,
                                    revocation_reason=(
                                        SOURCE_DELETED_REVOCATION_REASON
                                    ),
                                    relevance_score=0.0,
                                ):
                                    continue
                            else:
                                if embedding_absent is not True:
                                    raise RuntimeError(
                                        "source_deleted_embedding_erasure_unsupported"
                                    )
                                scope.execute(
                                    f"MATCH (n:{ntype} {{id: $node_id}}) "
                                    "SET n.source_content_hash = $erased_text",
                                    {
                                        "node_id": node_id,
                                        "erased_text": ERASED_SEMANTIC_TEXT,
                                    },
                                )
                        if target_identities is not None:
                            result.target_already_converged_count += 1
                        continue
                    # ``Alternative`` is normally durable cognitive knowledge,
                    # but SK-A RDL alternatives are relationally-owned,
                    # rebuildable projections.  Establish that exception from
                    # the closed source-ref grammar plus system provenance
                    # before applying the cognitive carve-out.  A broad
                    # ``refinement:{id}:rdl:`` prefix would let unrelated
                    # cognitive nodes be erased by a source deletion.
                    relational_projection = is_relational_projection_node(
                        node_type=ntype,
                        source_artifact_ref=ref,
                        created_by_agent=writer,
                    )
                    if not relational_projection and _is_cognitive(ntype, writer):
                        intent = _classify_cognitive(
                            board_id,
                            ntype,
                            node_id,
                            ref,
                            target_identities,
                            source_by_identity,
                            correlation_id,
                        )
                        if intent is None:
                            continue  # out of fast-path scope
                        if target_identities is not None:
                            # Debt routing is also a cognitive preservation
                            # outcome: the canonical node is deliberately
                            # skipped by demotion even when debt is recorded.
                            result.target_skipped_cognitive_count += 1
                        if intent.get("_route"):
                            cognitive_debt_intents.append(intent)
                        else:
                            result.skipped_cognitive.append(intent)
                            logger.info(
                                "kg.stale.skipped_cognitive board=%s node=%s type=%s",
                                board_id,
                                node_id,
                                ntype,
                                extra={
                                    "event": "kg.stale.skipped_cognitive",
                                    "board_id": board_id,
                                    "node_id": node_id,
                                    "node_type": ntype,
                                    "correlation_id": correlation_id,
                                },
                            )
                        continue

                    # Deterministic node: resolve owning source + check staleness.
                    if source_identity is None:
                        continue  # infra root / unknown — not source-derived
                    if (
                        target_identities is not None
                        and source_identity not in target_identities
                    ):
                        continue  # fast-path scope
                    cls = source_by_identity.get(source_identity)
                    if (
                        graph_layer == GRAPH_LAYER_WORKING and cls is not None
                    ):
                        # Both scan modes include working rows solely to prove
                        # convergence or repair an interrupted source-deletion
                        # tombstone. An extant source already owns its working
                        # projection, so rewriting it would make both the
                        # targeted retry and the full sweep non-idempotent.
                        if target_identities is not None:
                            result.target_already_converged_count += 1
                        continue
                    if cls is not None and cls.graph_layer == GRAPH_LAYER_CANONICAL:
                        if target_identities is not None:
                            result.target_preserved_canonical_count += 1
                        continue  # source still canonical-eligible — not stale
                    new_maturity = (
                        cls.maturity_status if cls else MATURITY_WORKING_STALE
                    )
                    reason = cls.reason_code if cls else "source_absent"
                    src_status = cls.artifact_status if cls else ""
                    source_deleted = cls is None
                    if source_deleted:
                        replace_tombstone = getattr(
                            scope,
                            "replace_with_source_deleted_tombstone",
                            None,
                        )
                        if callable(replace_tombstone):
                            replaced = replace_tombstone(
                                ntype,
                                node_id,
                                graph_layer=GRAPH_LAYER_WORKING,
                                maturity_status=MATURITY_WORKING_STALE,
                                revocation_reason=(
                                    SOURCE_DELETED_REVOCATION_REASON
                                ),
                                relevance_score=0.0,
                            )
                            if not replaced:
                                continue
                        else:
                            if embedding_absent is not True:
                                raise RuntimeError(
                                    "source_deleted_embedding_erasure_unsupported"
                                )
                            scope.execute(
                                f"MATCH (n:{ntype} {{id: $node_id}}) "
                                "SET n.graph_layer = $graph_layer, "
                                "n.maturity_status = $maturity_status, "
                                "n.revocation_reason = $revocation_reason, "
                                "n.relevance_score = $relevance_score, "
                                "n.title = $erased_text, "
                                "n.content = $erased_text, "
                                "n.context = $erased_text, "
                                "n.justification = $erased_text, "
                                "n.source_span_quote = $erased_text, "
                                "n.source_content_hash = $erased_text",
                                {
                                    "node_id": node_id,
                                    "graph_layer": GRAPH_LAYER_WORKING,
                                    "maturity_status": MATURITY_WORKING_STALE,
                                    "revocation_reason": (
                                        SOURCE_DELETED_REVOCATION_REASON
                                    ),
                                    "relevance_score": 0.0,
                                    "erased_text": ERASED_SEMANTIC_TEXT,
                                },
                            )
                        new_maturity = MATURITY_WORKING_STALE
                    else:
                        scope.execute(
                            f"MATCH (n:{ntype} {{id: $node_id}}) "
                            "SET n.graph_layer = $graph_layer, "
                            "n.maturity_status = $maturity_status",
                            {
                                "node_id": node_id,
                                "graph_layer": GRAPH_LAYER_WORKING,
                                "maturity_status": new_maturity,
                            },
                        )
                    record = {
                        "node_id": node_id,
                        "node_type": ntype,
                        "source_artifact_ref": ref,
                        "owning_source_type": source_identity[0],
                        "owning_source_id": source_identity[1],
                        "prev_layer": graph_layer,
                        "expected_layer": GRAPH_LAYER_WORKING,
                        "maturity_status": new_maturity,
                        "source_status": src_status,
                        "reason_code": reason,
                        "revocation_reason": (
                            SOURCE_DELETED_REVOCATION_REASON
                            if source_deleted
                            else revocation_reason
                        ),
                        "correlation_id": correlation_id,
                        "action": ACTION_DEMOTED,
                    }
                    result.demoted.append(record)
                    if target_identities is not None:
                        result.target_demoted_count += 1
                    logger.info(
                        "kg.stale.demoted_to_working board=%s node=%s type=%s "
                        "reason=%s corr=%s",
                        board_id,
                        node_id,
                        ntype,
                        reason,
                        correlation_id,
                        extra={"event": "kg.stale.demoted_to_working", **record},
                    )
            except Exception as exc:  # per-type convergence guard
                _record_failed_type(result, ntype)
                logger.warning(
                    "kg.stale.scan_type_failed board=%s type=%s err=%s",
                    board_id,
                    ntype,
                    exc,
                )
                continue
            result.completed_types.append(ntype)
    return cognitive_debt_intents


def _classify_cognitive(
    board_id: str,
    ntype: str,
    node_id: str,
    ref: str,
    target_identities: set[SourceIdentity] | None,
    source_by_identity: dict[SourceIdentity, SourceMaturityClassification],
    correlation_id: str,
) -> dict[str, Any] | None:
    """Cognitive carve-out: NEVER demote. A bug-derived Learning whose Bug source
    regressed is a material R7-applicable irregularity -> mark for debt routing.
    Everything else is a preserved skip (no debt). Returns None if out of the
    fast-path scope."""
    base = {
        "node_id": node_id,
        "node_type": ntype,
        "source_artifact_ref": ref,
        "action": ACTION_SKIPPED_COGNITIVE,
        "_route": False,
    }
    if ntype != "Learning" or not _is_bug_derived_ref(ref):
        if target_identities is not None:
            # Non-bug-derived cognitive: only report inside fast-path scope when
            # its owning id (if resolvable) is targeted; otherwise still a skip.
            identity = _source_identity_from_ref(ref)
            if identity is not None and identity not in target_identities:
                return None
        return base
    bug_id = _bug_artifact_id(ref)
    bug_identity: SourceIdentity = ("card", bug_id)
    if target_identities is not None and bug_identity not in target_identities:
        return None  # fast-path scope
    bug_cls = source_by_identity.get(bug_identity)
    # A governed delete removes the durable Bug row before this intent drains.
    # Absence is therefore the strongest regression signal, not an unknown
    # state.  Preserve the canonical Learning itself, but route its now-orphaned
    # material evidence through the existing R7 debt contract.
    regressed = bug_cls is None or bug_cls.graph_layer != GRAPH_LAYER_CANONICAL
    if not regressed:
        return base  # bug evidence still canonical (or unknown) -> preserved skip
    return {
        **base,
        "action": ACTION_ROUTED_DEBT,
        "_route": True,
        "bug_id": bug_id,
        "source_status": bug_cls.artifact_status if bug_cls else "",
        "reason_code": bug_cls.reason_code if bug_cls else "source_absent",
        "correlation_id": correlation_id,
    }


async def _route_cognitive_to_debt(
    db: object, board_id: str, intent: dict[str, Any], correlation_id: str
) -> None:
    """Route a material bug-derived Learning irregularity to the EXISTING R7
    CanonicalDebt contract (idempotent; no new taxonomy). Preserves the canonical
    Learning — this records the irregularity, it does NOT demote."""
    from okto_pulse.core.kg.canonical_learning_partition import (
        HISTORICAL_DEBT_REASON,
        SOURCE_ABSENT_DEBT_REASON,
        upsert_canonical_learning_debt,
    )

    reason_code = str(intent.get("reason_code") or "")
    failure_reason = (
        SOURCE_ABSENT_DEBT_REASON
        if reason_code == SOURCE_ABSENT_DEBT_REASON
        else HISTORICAL_DEBT_REASON
    )
    await upsert_canonical_learning_debt(
        db,
        board_id=board_id,
        node_id=str(intent.get("node_id") or ""),
        source_ref=str(intent.get("source_artifact_ref") or ""),
        failure_reason=failure_reason,
        correlation_id=correlation_id,
    )


async def reconcile_stale_canonical(
    db: object,
    *,
    board_id: str,
    source_refs: list[str] | None = None,
    correlation_id: str | None = None,
    before_graph_write: Callable[[], None] | None = None,
    blocking_execution: BlockingExecutionPort | None = None,
) -> StaleReconcileResult:
    """Reconcile stale canonical publication for a board.

    ``source_refs`` scopes the run to specific artifacts (event fast-path, FR2);
    ``None`` is a full sweep (FR3). Both converge to the same final state and are
    idempotent (TR7/AC8). Deterministic stale canonical nodes are demoted to
    working in place; cognitive nodes are preserved (carve-out) and a material
    bug-derived Learning irregularity is routed to R7 CanonicalDebt.

    ``before_graph_write`` is invoked only after the source snapshot succeeds
    and immediately before the first graph scan/mutation. The governed queue
    worker uses it to acquire a durable board-writer fence lazily, so a
    source-snapshot failure opens no writer while every possible auto-commit is
    fenced.
    """
    import uuid as _uuid

    corr = correlation_id or _uuid.uuid4().hex
    result = StaleReconcileResult(board_id=board_id, correlation_id=corr)
    # Validate scope before touching the source database or graph. Only None is
    # an explicit full sweep; invalid durable intents remain retryable.
    target_identities = _source_ids_from_refs(source_refs)
    if target_identities is not None:
        result.target_identity_count = len(target_identities)
    source_by_identity, source_complete, incomplete_cause = (
        await run_blocking_graph_io(
            lambda: _build_source_classification_map(board_id),
            task_name="core.kg.stale_canonical_source_snapshot",
            blocking_execution=blocking_execution,
        )
    )
    if not source_complete:
        result.incomplete = True
        result.incomplete_cause = incomplete_cause
        logger.warning(
            "kg.stale.source_snapshot_incomplete board=%s cause=%s corr=%s",
            board_id,
            incomplete_cause,
            corr,
            extra={
                "event": "kg.stale.source_snapshot_incomplete",
                "board_id": board_id,
                "cause": incomplete_cause,
                "correlation_id": corr,
            },
        )
        return result

    if before_graph_write is not None:
        before_graph_write()

    debt_intents = await run_blocking_graph_io(
        lambda: run_async_blocking(
            _scan_and_demote(
                board_id,
                source_by_identity,
                target_identities,
                corr,
                result,
            )
        ),
        task_name="core.kg.stale_canonical_graph_scan",
        blocking_execution=blocking_execution,
    )
    for intent in debt_intents:
        try:
            await _route_cognitive_to_debt(db, board_id, intent, corr)
            result.routed_to_debt.append(intent)
            logger.info(
                "kg.stale.routed_to_canonical_debt board=%s node=%s bug=%s corr=%s",
                board_id,
                intent.get("node_id"),
                intent.get("bug_id"),
                corr,
                extra={
                    "event": "kg.stale.routed_to_canonical_debt",
                    "board_id": board_id,
                    "correlation_id": corr,
                    "node_id": intent.get("node_id"),
                    "bug_id": intent.get("bug_id"),
                },
            )
        except Exception as exc:  # pragma: no cover - debt routing best-effort
            _record_failed_type(result, str(intent.get("node_type") or "Learning"))
            logger.warning(
                "kg.stale.debt_routing_failed board=%s node=%s err=%s",
                board_id,
                intent.get("node_id"),
                exc,
            )

    # Delivery ownership deliberately does not live in this graph primitive.
    # The governed queue worker transfers a complete reconciliation to the
    # Delivery Ledger, the physical GD outbox and the queue ACK in one
    # relational transaction.  Keeping this function graph-only is essential:
    # a crash after the embedded graph auto-commit must leave the durable queue
    # row available so a retry (including one with zero new demotions) still
    # schedules Global Discovery parity exactly once.
    return result


__all__ = [
    "ACTION_DEMOTED",
    "ACTION_ROUTED_DEBT",
    "ACTION_SKIPPED_COGNITIVE",
    "COGNITIVE_NODE_TYPES",
    "ALL_NODE_TYPES",
    "STALE_RECONCILE_NODE_POLICY",
    "SourceIdentity",
    "StaleReconcileResult",
    "StaleSweepPage",
    "_source_identity_from_ref",
    "decode_stale_sweep_cursor",
    "encode_stale_sweep_cursor",
    "enumerate_stale_sweep_page",
    "reconcile_stale_canonical",
    "validate_stale_reconcile_ontology_coverage",
]
