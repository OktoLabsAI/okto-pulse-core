"""Global discovery outbox application processor.

Retry, idempotency and delivery transitions remain in Core. Polling and task
lifecycle are owned by an edition runner.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg.global_discovery.metrics import (
    emit_digest_upsert,
    emit_missing_embedding_skipped,
)
from okto_pulse.core.domain.worker_policy import RetryPolicy
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.cypher_templates import layer_label_projection
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.interfaces.graph_errors import GraphError
from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES
from okto_pulse.core.ports.coordination import ClaimRepository, get_claim_repository
from okto_pulse.core.ports.global_outbox import (
    GLOBAL_OUTBOX_DEAD_LETTER_SENTINEL,
    GLOBAL_OUTBOX_MAX_RETRIES,
    GlobalOutboxEventRecord,
    GlobalOutboxNodeRefFact,
    get_global_outbox_store,
)
from okto_pulse.core.ports.runtime_workers import (
    BlockingExecutionPort,
    WorkerClockPort,
)

logger = logging.getLogger("okto_pulse.kg.global_discovery.outbox")

MAX_RETRIES = GLOBAL_OUTBOX_MAX_RETRIES
DEAD_LETTER_SENTINEL = GLOBAL_OUTBOX_DEAD_LETTER_SENTINEL
GLOBAL_OPEN_ERROR_CODES = (
    "graph_corruption",
    "graph_unavailable",
    "graph_lock_contention",
)

# Node types mirrored into the global discovery layer as DecisionDigest.
# DERIVED from the per-board VECTOR_INDEX_TYPES (TR1, spec 849d6292): only nodes
# with an HNSW-backed embedding are worth digesting for cross-board semantic
# search, and aliasing the schema tuple keeps the two lists from drifting again
# (the old hand-maintained subset omitted Requirement/APIContract/TestScenario/
# Bug, so those canonical types were never globally searchable).
DIGESTED_NODE_TYPES: tuple[str, ...] = VECTOR_INDEX_TYPES
BOARD_SOURCE_INVENTORY_PAGE_SIZE = 5000
LEARNING_RELATION_GROUP_PAGE_SIZE = 5000


class _SourceInventoryPaginationError(RuntimeError):
    """The read port returned a non-advancing supposedly paginated page."""


class _SourceIdentityIntegrityError(RuntimeError):
    """The authoritative board source contains an ambiguous physical identity."""


def _global_discovery_runtime():
    return get_kg_registry().require_global_discovery_runtime()


class GlobalOutboxProcessor:
    def __init__(
        self,
        relational_scope_factory=None,
        interval_seconds: int = 5,
        *,
        claim_repository: ClaimRepository | None = None,
        clock: WorkerClockPort | None = None,
        retry_policy: RetryPolicy | None = None,
        blocking_execution: BlockingExecutionPort | None = None,
    ):
        if relational_scope_factory is None:
            from okto_pulse.core.ports.relational_runtime import get_db_session

            relational_scope_factory = get_db_session
        self._relational_scope_factory = relational_scope_factory
        self._interval = interval_seconds
        self._claim_repository = claim_repository
        self._clock = clock
        self._retry_policy = retry_policy or RetryPolicy(max_attempts=MAX_RETRIES)
        self._blocking_execution = blocking_execution

    async def _run_graph_io(self, operation: Callable[[], Any]) -> Any:
        """Run one graph critical section off-loop and drain it on cancel."""

        return await run_blocking_graph_io(
            operation,
            task_name="core.global_outbox.graph_io",
            blocking_execution=self._blocking_execution,
        )

    def _now(self) -> datetime:
        return (
            self._clock.now() if self._clock is not None else datetime.now(timezone.utc)
        )

    async def build_recovery_board_seed(
        self,
        db: Any,
        *,
        board_id: str,
        board_name: str,
        board_summary: str,
        captured_cognitive_pending_exclusions: Mapping[str, str] | None = None,
    ):
        """Read one complete, stable board projection for recovery materialization.

        This deliberately never opens Global Discovery.  It reuses the same
        authoritative board reads and canonical-layer policy as normal outbox
        reconciliation, then rechecks the source identity inventory so a board
        mutation cannot silently produce a torn candidate snapshot.
        """

        from okto_pulse.core.kg.canonical_partition_integrity import (
            canonical_debt_exclusions,
            pending_or_debt_exclusions,
        )
        from okto_pulse.core.kg.connectivity_guard import (
            CANONICAL_LEARNING_WORKING_ONLY_REASON,
        )
        from okto_pulse.core.kg.embedding import get_embedding_provider
        from okto_pulse.core.kg.global_discovery.layer_parity import (
            resolve_expected_digest_layer,
        )
        from okto_pulse.core.kg.interfaces.global_discovery_recovery import (
            GlobalDiscoveryBoardSeed,
            GlobalDiscoveryDigestSeed,
        )
        from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

        source_types = await self._run_graph_io(
            lambda: self._read_board_digestable_node_types(board_id)
        )
        if source_types is None:
            raise RuntimeError(
                "outbox.read_board_failed: could not enumerate recovery sources"
            )
        refs = [
            GlobalOutboxNodeRefFact(
                graph_node_id=node_id,
                graph_node_type=node_type,
            )
            for node_id, node_type in sorted(source_types.items())
        ]
        nodes = await self._run_graph_io(
            lambda: self._read_board_nodes_for_refs(board_id, refs)
        )
        if nodes is None:
            raise RuntimeError(
                "outbox.read_board_failed: could not materialize recovery sources"
            )
        layer_meta = await self._run_graph_io(
            lambda: self._read_board_layer_meta(board_id, source_types)
        )
        if layer_meta is None or set(layer_meta) != set(source_types):
            raise RuntimeError(
                "outbox.read_board_failed: incomplete recovery source metadata"
            )
        needs_overlay = any(
            row.get("node_type") == "Learning" and row.get("graph_layer") == "canonical"
            for row in layer_meta.values()
        )
        overlay: dict[str, str] = {}
        if needs_overlay:
            if captured_cognitive_pending_exclusions is None:
                overlay = await pending_or_debt_exclusions(db, board_id=board_id)
            else:
                overlay = {
                    str(artifact_id): str(reason)
                    for artifact_id, reason in (
                        captured_cognitive_pending_exclusions.items()
                    )
                }
                if any(
                    not artifact_id or reason != CANONICAL_LEARNING_WORKING_ONLY_REASON
                    for artifact_id, reason in overlay.items()
                ):
                    raise ValueError(
                        "captured cognitive pending exclusions are invalid"
                    )
                # Canonical debt is relational and therefore read from the
                # caller's already-open recovery snapshot.  It outranks the
                # captured cognitive hold for the same artifact.
                overlay.update(await canonical_debt_exclusions(db, board_id=board_id))

        seen: set[str] = set()
        digests: list[GlobalDiscoveryDigestSeed] = []
        for node in sorted(nodes, key=lambda row: str(row.get("id") or "")):
            node_id = str(node.get("id") or "")
            if not node_id or node_id in seen or node_id not in source_types:
                raise RuntimeError(
                    "outbox.source_identity_ambiguous: invalid recovery source identity"
                )
            seen.add(node_id)
            meta = layer_meta[node_id]
            artifact_id = normalize_cognitive_artifact_id(
                str(meta.get("source_artifact_ref") or "")
            )
            graph_layer, _exclusion = resolve_expected_digest_layer(
                node_type=str(meta["node_type"]),
                raw_graph_layer=str(meta["graph_layer"]),
                source_artifact_ref=str(meta.get("source_artifact_ref") or ""),
                canonical_bug_count=int(meta.get("canonical_bug_count") or 0),
                relates_to_endpoints=tuple(meta.get("relates_to_endpoints") or ()),
                overlay_exclusion_reason=overlay.get(artifact_id),
            )
            title = str(node.get("title") or "")
            digests.append(
                GlobalDiscoveryDigestSeed(
                    original_node_id=node_id,
                    title=title,
                    summary=title[:280],
                    node_type=source_types[node_id],
                    graph_layer=graph_layer,
                    source_artifact_ref=str(meta.get("source_artifact_ref") or ""),
                    embedding=tuple(float(value) for value in node["embedding"]),
                )
            )

        revalidated = await self._run_graph_io(
            lambda: self._read_board_digestable_node_types(board_id)
        )
        if revalidated != source_types:
            raise RuntimeError(
                "outbox.source_inventory_changed: board changed during recovery read"
            )
        fingerprint_payload = {
            "source_types": sorted(source_types.items()),
            "digests": [row.to_dict() for row in digests],
        }
        source_inventory_hash = hashlib.sha256(
            json.dumps(
                fingerprint_payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        summary_embedding = tuple(
            float(value)
            for value in get_embedding_provider().encode(
                f"Board {board_name or board_id}"
            )
        )
        return GlobalDiscoveryBoardSeed(
            board_id=board_id,
            board_name=board_name,
            summary=board_summary,
            summary_embedding=summary_embedding,
            digests=tuple(digests),
            source_inventory_hash=source_inventory_hash,
        )

    async def process_once(self) -> int:
        """Process pending outbox events. Returns count processed."""

        from okto_pulse.core.kg.global_discovery_writer import (
            GlobalDiscoveryWriterContention,
            GlobalDiscoveryWriterLease,
        )

        try:
            lease = await self._run_graph_io(
                lambda: GlobalDiscoveryWriterLease.acquire(
                    operation="global_outbox_apply",
                    ttl_seconds=3600,
                )
            )
        except GlobalDiscoveryWriterContention:
            # Recovery (or another process' outbox batch) owns the durable
            # fence.  Leave rows untouched and retry on the next worker tick.
            return 0
        try:
            with lease.guard():
                return await self._process_once_under_writer()
        finally:
            await self._run_graph_io(lease.release)

    async def _process_once_under_writer(self) -> int:
        processed = 0
        async with self._relational_scope_factory() as db:
            # Terminal rows are operator-owned.  The claim adapter excludes
            # the dead-letter sentinel, and only the explicit bounded
            # list/reprocess/verify service may reopen selected immutable IDs.
            # Inferring recovery from ``last_error`` here used to requeue DLQ
            # rows on startup, which bypassed the audit reason/selection CAS
            # and could race a fail-closed Global Discovery cutover.
            claim_repository = self._claim_repository or get_claim_repository()
            claimed = await claim_repository.claim_global_outbox(
                db,
                limit=50,
            )
            events = list(
                await get_global_outbox_store().materialize_claimed(db, claimed)
            )
            processed_events: list[
                tuple[GlobalOutboxEventRecord, dict[str, tuple[str, str]]]
            ] = []
            for event in events:
                try:
                    expected_state = await self._apply_event(event, db)
                    processed_events.append((event, expected_state))
                    processed += 1
                except Exception as exc:
                    event.retry_count += 1
                    event.last_error = _semantic_error_detail(exc)[:500]
                    if self._retry_policy.after_failure(event.retry_count).terminal:
                        event.retry_count = DEAD_LETTER_SENTINEL
                        logger.warning(
                            "outbox.dead_letter event=%s board=%s err=%s",
                            event.event_id,
                            event.board_id,
                            exc,
                            extra={
                                "event": "outbox.dead_letter",
                                "event_id": event.event_id,
                                "board_id": event.board_id,
                            },
                        )
            if processed_events:
                verification_errors, batch_flush_error = await self._run_graph_io(
                    lambda: self._verify_processed_batch(processed_events)
                )

                if batch_flush_error is not None:
                    logger.warning(
                        "outbox.global_discovery_post_flush_failed count=%d err=%s",
                        len(processed_events),
                        batch_flush_error,
                        extra={
                            "event": "outbox.global_discovery_post_flush_failed",
                            "count": len(processed_events),
                        },
                    )
                    for event, _expected in processed_events:
                        event.processed_at = None
                        event.retry_count += 1
                        event.last_error = (
                            "global_discovery_post_flush_failed: "
                            f"{_semantic_error_detail(batch_flush_error)}"
                        )[:500]
                        if self._retry_policy.after_failure(event.retry_count).terminal:
                            event.retry_count = DEAD_LETTER_SENTINEL
                    processed = 0
                else:
                    processed = 0
                    for event, _expected in processed_events:
                        error = verification_errors.get(event.board_id)
                        if error is None:
                            event.processed_at = self._now()
                            event.last_error = None
                            processed += 1
                            continue
                        event.processed_at = None
                        event.retry_count += 1
                        event.last_error = (
                            "global_discovery_post_flush_verification_failed: "
                            f"{_semantic_error_detail(error)}"
                        )[:500]
                        if self._retry_policy.after_failure(event.retry_count).terminal:
                            event.retry_count = DEAD_LETTER_SENTINEL
            await get_global_outbox_store().save_events(db, events)
            await get_global_outbox_store().commit(db)
        if processed:
            logger.info(
                "outbox.processed count=%d",
                processed,
                extra={"event": "outbox.processed", "count": processed},
            )
        return processed

    def _verify_processed_batch(
        self,
        processed_events: list[
            tuple[GlobalOutboxEventRecord, dict[str, tuple[str, str]]]
        ],
    ) -> tuple[dict[str, Exception], Exception | None]:
        """Flush and verify a claimed batch inside one blocking graph scope."""

        runtime = _global_discovery_runtime()
        verification_errors: dict[str, Exception] = {}
        batch_flush_error: Exception | None = None
        try:
            # The edition adapter holds its process-wide writer lease across
            # flush, close/reopen and every fresh global read. Keeping this
            # complete scope on the blocking executor prevents event-loop
            # stalls and prevents another writer from seeing a closed handle.
            with runtime.post_write_verification_scope():
                self._flush_global_discovery_storage_after_batch()
                latest_expected_by_board = {
                    event.board_id: expected for event, expected in processed_events
                }
                for board_id, expected in latest_expected_by_board.items():
                    runtime.close()
                    try:
                        expected_source_types = {
                            source_id: node_type
                            for source_id, (_layer, node_type) in expected.items()
                        }
                        self._assert_source_inventory_unchanged(
                            board_id,
                            expected_source_types,
                        )
                        self._verify_reconciled_digest_layers(
                            runtime,
                            board_id,
                            expected,
                        )
                        logger.info(
                            "outbox.digest_reconcile_post_flush_verified "
                            "board=%s verified=%d",
                            board_id,
                            len(expected),
                            extra={
                                "event": (
                                    "outbox.digest_reconcile_post_flush_verified"
                                ),
                                "board_id": board_id,
                                "verified_count": len(expected),
                                "duplicate_count": 0,
                            },
                        )
                    except Exception as exc:
                        verification_errors[board_id] = exc
                        logger.warning(
                            "outbox.global_discovery_post_flush_verification_"
                            "failed board=%s err=%s",
                            board_id,
                            exc,
                            extra={
                                "event": (
                                    "outbox.global_discovery_post_flush_"
                                    "verification_failed"
                                ),
                                "board_id": board_id,
                            },
                        )
                    finally:
                        runtime.close()
        except Exception as exc:
            batch_flush_error = exc
        return verification_errors, batch_flush_error

    async def _apply_event(
        self,
        event: GlobalOutboxEventRecord,
        db: Any,
    ) -> dict[str, tuple[str, str]]:
        """Apply a single outbox event to the global discovery meta-graph.

        Upserts the Board summary node, then mirrors every searchable node
        added during the session into DecisionDigest (id, title, summary,
        node_type, embedding) linked via (Board)-[:CONTAINS_DECISION]->.
        Without this mirror, `query_global` has nothing to search over.

        KG-01.3.1 boundary: this is a write path against the global graph.
        process_once wraps this call in ``under_global_safe_write`` and this
        method requires the global guard before touching the global graph.
        After a processed batch, process_once closes/fsyncs/reopen-probes the
        global graph so successful rows are not marked processed while the WAL
        is only readable through a live process handle.
        """
        from okto_pulse.core.kg.write_barrier import require_global_write_token

        payload = event.payload or {}
        board_id = event.board_id
        require_global_write_token()

        global_runtime = _global_discovery_runtime()
        await self._run_graph_io(global_runtime.ensure_layer_schema)
        session_id = payload.get("session_id", "") or event.session_id
        ts = self._now().strftime("%Y-%m-%dT%H:%M:%S")

        # 1) Fetch the per-session graph node refs — the authoritative list of
        # what was actually written to the per-board graph. We digest only
        # `add` ops; updates/supersedes don't produce new digest rows (a
        # future pass can refresh digest embeddings on update).
        refs = list(
            await get_global_outbox_store().list_added_node_refs(
                db,
                session_id=session_id,
                board_id=board_id,
                node_types=DIGESTED_NODE_TYPES,
            )
        )

        gconn = global_runtime
        try:
            from okto_pulse.core.kg.embedding import get_embedding_provider

            source_types_by_id = await self._run_graph_io(
                lambda: self._read_board_digestable_node_types(board_id)
            )
            if source_types_by_id is None:
                raise RuntimeError(
                    "outbox.read_board_failed: could not enumerate source graph "
                    "digest identities"
                )
            await self._run_graph_io(
                lambda: gconn.upsert_board_summary(
                    board_id=board_id,
                    name=board_id,
                    summary="",
                    summary_embedding=get_embedding_provider().encode(
                        f"Board {board_id}"
                    ),
                    # Absolute authoritative count keeps retries idempotent
                    # when a previous global write succeeded but its
                    # post-flush ACK failed.
                    decision_count=len(source_types_by_id),
                    synced_at=ts,
                )
            )
            await self._run_graph_io(
                lambda: self._prune_stale_board_digests(
                    gconn,
                    board_id,
                    set(source_types_by_id),
                )
            )

            # R1-IMP1 — parity reconciler (FR1/FR2). Correctness of
            # DecisionDigest.graph_layer cannot depend on a fresh `add` ref: in
            # EVERY event we re-derive the expected publication layer of the
            # board's already-published digests from the CURRENT board graph and
            # correct drift in place, preserving original_node_id. This runs
            # BEFORE the empty-refs early return so an out-of-band layer change
            # (e.g. a source promoted working->canonical, or a Learning whose
            # evidence matured) is reconciled even with no add refs this session.
            expected_state: dict[str, tuple[str, str]] = {}
            reconciled_republish_ids: set[str] = set()
            await self._reconcile_board_digest_layers(
                gconn,
                board_id,
                db,
                source_types_by_id=source_types_by_id,
                expected_state=expected_state,
                republished_source_ids=reconciled_republish_ids,
            )

            if not refs:
                return expected_state

            # 2) Read the just-added nodes back from the per-board graph backend to
            # pick up the title + embedding computed at consolidation time.
            per_board = await self._run_graph_io(
                lambda: self._read_board_nodes_for_refs(board_id, refs)
            )
            if per_board is None:
                raise RuntimeError(
                    "outbox.read_board_failed: could not read source graph nodes"
                )
            if not per_board:
                await self._run_graph_io(
                    lambda: self._assert_source_inventory_unchanged(
                        board_id,
                        source_types_by_id,
                    )
                )
                return expected_state

            # R7 IMP5 — canonical-only completeness. A canonical Learning whose
            # mandatory Bug evidence is working-only, or which carries an open
            # canonical_debt / active cognitive_pending hold, must NOT be
            # published as complete canonical. We correct the DIGEST publication
            # layer (to 'working') so canonical-only query_global skips it, while
            # all/working keep it diagnostically. The board-graph source node and
            # its maturity are NEVER touched here. Overlay (debt/pending) is only
            # fetched when a canonical Learning is actually being digested.
            from okto_pulse.core.kg.canonical_partition_integrity import (
                pending_or_debt_exclusions,
            )
            from okto_pulse.core.kg.global_discovery.layer_parity import (
                resolve_expected_digest_layer,
            )
            from okto_pulse.core.kg.global_discovery.metrics import (
                emit_canonical_incomplete_excluded,
            )
            from okto_pulse.core.kg.rebuild_audit import (
                normalize_cognitive_artifact_id,
            )

            has_canonical_learning = any(
                n.get("node_type") == "Learning" and n.get("graph_layer") == "canonical"
                for n in per_board
            )
            learning_exclusions = (
                await pending_or_debt_exclusions(db, board_id=board_id)
                if has_canonical_learning
                else {}
            )

            # 3) Create one DecisionDigest per node + CONTAINS_DECISION edge.
            # graph backend's HNSW-indexed columns (DecisionDigest.embedding) cannot be
            # mutated via SET after node creation — attempting it raises
            # "Cannot set property vec in table embeddings because it is used
            # in one or more indexes". So we MATCH first; on miss we CREATE
            # with the embedding baked in, on hit we SET only non-indexed
            # columns (title/summary/node_type may legitimately evolve; the
            # embedding is frozen for the life of the digest row).
            for node in per_board:
                # The authoritative reconciler may already have backfilled or
                # replaced this identity from the same current source graph.
                # Repeating the normal add-path upsert then performs a second
                # physical write and double-counts the upsert and canonical-
                # exclusion metrics for one event. Existing healthy identities
                # are deliberately not skipped: their add ref can represent a
                # legitimate title/summary refresh.
                if str(node["id"]) in reconciled_republish_ids:
                    continue
                digest_id = f"dd_{board_id[:8]}_{node['id']}"
                title = node["title"] or ""
                # R1-IMP1: the published layer is the expected_digest_layer
                # (publication layer), NOT the raw board node layer. The shared
                # resolver applies the R7 completeness carve-out for a canonical
                # Learning — publication-layer only, NEVER a source demotion.
                artifact_id = normalize_cognitive_artifact_id(
                    node.get("source_artifact_ref") or ""
                )
                effective_layer, exclusion_reason = resolve_expected_digest_layer(
                    node_type=node["node_type"],
                    raw_graph_layer=node["graph_layer"],
                    source_artifact_ref=node.get("source_artifact_ref") or "",
                    canonical_bug_count=int(node.get("canonical_bug_count") or 0),
                    relates_to_endpoints=tuple(node.get("relates_to_endpoints") or ()),
                    overlay_exclusion_reason=learning_exclusions.get(artifact_id),
                )
                expected_state[str(node["id"])] = (
                    effective_layer,
                    str(node["node_type"]),
                )
                if exclusion_reason:
                    emit_canonical_incomplete_excluded(
                        board_id=board_id,
                        reason_code=exclusion_reason,
                    )
                digest_outcome = await self._run_graph_io(
                    lambda node=node, digest_id=digest_id, title=title: (
                        gconn.upsert_decision_digest(
                            digest_id=digest_id,
                            board_id=board_id,
                            original_node_id=node["id"],
                            title=title,
                            summary=title[:280],
                            node_type=node["node_type"],
                            graph_layer=effective_layer,
                            embedding=node["embedding"],
                            created_at=ts,
                        )
                    )
                )
                # or_38b60fe1: record the digest upsert per node_type so a
                # regression that stops digesting a vector type is detectable.
                emit_digest_upsert(
                    board_id=board_id,
                    node_type=node["node_type"],
                    outcome=digest_outcome,
                )
                # Idempotent edge: MATCH both endpoints, then MERGE the rel.
                # MERGE on a relationship does not touch indexed node properties.
                await self._run_graph_io(
                    lambda digest_id=digest_id: gconn.link_board_digest(
                        board_id=board_id,
                        digest_id=digest_id,
                    )
                )
            await self._run_graph_io(
                lambda: self._assert_source_inventory_unchanged(
                    board_id,
                    source_types_by_id,
                )
            )
            return expected_state
        finally:
            pass

    @staticmethod
    def _read_board_digestable_node_types(
        board_id: str,
    ) -> dict[str, str] | None:
        """Return publishable ``source id -> source node type`` for a board.

        ``None`` means the source graph could not be read and pruning must not
        run. An empty dict is meaningful: the board currently has no digestable
        nodes, so all old global digests for that board are stale.

        The global digest's recorded ``node_type`` is intentionally ignored:
        historical physical graph corruption can expose a second row whose
        type/title belong to another offset.  Authority is the per-board graph,
        enumerated across every digestable source table. Nodes without an
        embedding are not publishable and remain on the existing structured
        ``missing_embedding_skipped`` path when referenced by an event. A source
        id present in more than one source table is an actual board-integrity
        ambiguity and is rejected rather than guessed.
        """

        types_by_id: dict[str, set[str]] = {}
        try:
            cypher = get_kg_registry().cypher_executor
            for ntype in DIGESTED_NODE_TYPES:
                after_id = ""
                while True:
                    result = cypher.execute_read_only(
                        board_id,
                        f"MATCH (n:{ntype}) "
                        "WHERE n.embedding IS NOT NULL AND n.id > $after_id "
                        "RETURN n.id, count(n) ORDER BY n.id "
                        f"LIMIT {BOARD_SOURCE_INVENTORY_PAGE_SIZE}",
                        {"after_id": after_id},
                        max_rows=BOARD_SOURCE_INVENTORY_PAGE_SIZE,
                    )
                    rows = list(result.get("rows", []))
                    page_ids: list[str] = []
                    for row in rows:
                        node_id = str(row[0] or "") if row else ""
                        physical_count = int(row[1] or 0) if len(row) > 1 else 0
                        if (
                            not node_id
                            or node_id <= after_id
                            or (page_ids and node_id <= page_ids[-1])
                        ):
                            raise _SourceInventoryPaginationError(
                                "outbox.source_inventory_pagination_stalled: "
                                f"board={board_id} node_type={ntype} "
                                f"after_id={after_id!r}"
                            )
                        if physical_count != 1:
                            raise _SourceIdentityIntegrityError(
                                "outbox.source_identity_duplicate: "
                                f"board={board_id} identity={node_id} "
                                f"node_type={ntype} physical_rows={physical_count}"
                            )
                        page_ids.append(node_id)
                        types_by_id.setdefault(node_id, set()).add(ntype)
                    if len(rows) < BOARD_SOURCE_INVENTORY_PAGE_SIZE:
                        break
                    if not page_ids:
                        raise _SourceInventoryPaginationError(
                            "outbox.source_inventory_pagination_stalled: "
                            f"board={board_id} node_type={ntype} empty_page"
                        )
                    after_id = page_ids[-1]
        except (_SourceInventoryPaginationError, _SourceIdentityIntegrityError):
            raise
        except Exception as exc:
            logger.warning(
                "outbox.digest_reconcile_board_read_failed board=%s err=%s",
                board_id,
                exc,
                extra={
                    "event": "outbox.digest_reconcile_board_read_failed",
                    "board_id": board_id,
                },
            )
            return None

        ambiguous = {
            node_id: sorted(node_types)
            for node_id, node_types in types_by_id.items()
            if len(node_types) != 1
        }
        if ambiguous:
            sample_id = sorted(ambiguous)[0]
            raise RuntimeError(
                "outbox.source_identity_ambiguous: "
                f"board={board_id} identity={sample_id} "
                f"node_types={ambiguous[sample_id]}"
            )
        return {
            node_id: next(iter(node_types))
            for node_id, node_types in types_by_id.items()
        }

    @staticmethod
    def _assert_source_inventory_unchanged(
        board_id: str,
        expected_types_by_id: dict[str, str],
    ) -> None:
        current = GlobalOutboxProcessor._read_board_digestable_node_types(board_id)
        if current is None:
            raise RuntimeError(
                "outbox.read_board_failed: could not revalidate authoritative "
                "source inventory"
            )
        if current == expected_types_by_id:
            return
        expected_ids = set(expected_types_by_id)
        current_ids = set(current)
        added = sorted(current_ids - expected_ids)
        removed = sorted(expected_ids - current_ids)
        type_changed = sorted(
            node_id
            for node_id in expected_ids & current_ids
            if expected_types_by_id[node_id] != current[node_id]
        )
        sample = (added or removed or type_changed or ["unknown"])[0]
        raise RuntimeError(
            "outbox.source_inventory_changed: "
            f"board={board_id} added={len(added)} removed={len(removed)} "
            f"type_changed={len(type_changed)} sample={sample}"
        )

    @staticmethod
    def _read_board_digestable_node_ids(board_id: str) -> set[str] | None:
        """Compatibility projection of the authoritative source inventory."""

        types_by_id = GlobalOutboxProcessor._read_board_digestable_node_types(board_id)
        return None if types_by_id is None else set(types_by_id)

    @staticmethod
    def _prune_stale_board_digests(
        gconn,
        board_id: str,
        current_node_ids: set[str],
    ) -> int:
        """Delete DecisionDigest rows whose source node vanished from the board."""

        res = gconn.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $bid "
            "RETURN d.id, d.original_node_id",
            {"bid": board_id},
        )
        stale_original_node_ids: set[str] = set()
        stale_row_count = 0
        malformed_row_count = 0
        for row in res.rows:
            original_node_id = row[1]
            if not original_node_id:
                malformed_row_count += 1
            elif str(original_node_id) not in current_node_ids:
                stale_original_node_ids.add(str(original_node_id))
                stale_row_count += 1

        pruned_row_count = 0
        if stale_original_node_ids or malformed_row_count:
            # The adapter preflights the complete target set before its first
            # delete. Stale cache rows with clustering-derived relationships
            # must fail closed instead of losing MENTIONS/DERIVES semantics via
            # an unguarded DETACH DELETE.
            pruned_row_count = gconn.delete_decision_digests_guarded(
                board_id=board_id,
                original_node_ids=tuple(sorted(stale_original_node_ids)),
                include_malformed=bool(malformed_row_count),
            )
            expected_pruned_rows = stale_row_count + malformed_row_count
            if pruned_row_count != expected_pruned_rows:
                raise RuntimeError(
                    "outbox.digest_reconcile_verification_failed: prune "
                    f"board={board_id} removed={pruned_row_count} "
                    f"expected={expected_pruned_rows}"
                )
        if pruned_row_count:
            logger.warning(
                "outbox.digest_reconcile_pruned_stale board=%s count=%d "
                "identity_count=%d malformed_count=%d",
                board_id,
                pruned_row_count,
                len(stale_original_node_ids),
                malformed_row_count,
                extra={
                    "event": "outbox.digest_reconcile_pruned_stale",
                    "board_id": board_id,
                    "count": pruned_row_count,
                    "identity_count": len(stale_original_node_ids),
                    "malformed_count": malformed_row_count,
                },
            )
        return pruned_row_count

    def _flush_global_discovery_storage_after_batch(self) -> None:
        """Ask the edition runtime to flush/probe the global discovery store.

        The global discovery graph is a rebuildable cache, but it still must
        not leave a large unprobed WAL after marking outbox events processed.
        If the probe fails, process_once keeps those events retryable so the
        next run can re-apply idempotently after operator recovery.
        """
        _global_discovery_runtime().flush_after_write_batch()

    @staticmethod
    def _read_board_nodes_for_refs(
        board_id: str,
        refs: list[GlobalOutboxNodeRefFact],
    ) -> list[dict] | None:
        """Read (id, title, embedding) from the per-board graph backend for the given
        node refs, bucketed by type so we issue one MATCH per type."""

        by_type: dict[str, list[str]] = {}
        for r in refs:
            by_type.setdefault(r.graph_node_type, []).append(r.graph_node_id)

        out: list[dict] = []
        try:
            cypher_executor = get_kg_registry().cypher_executor
            for ntype, ids in by_type.items():
                # FR3/TR2 (spec 849d6292): do NOT silently drop NULL-embedding
                # nodes with a WHERE filter. Read every requested node and
                # partition — nodes with an embedding are digested; an eligible
                # node missing its embedding is SKIPPED with a structured
                # diagnostic + counter.
                cypher = (
                    f"MATCH (n:{ntype}) WHERE n.id IN $ids "
                    f"RETURN n.id, n.title, n.embedding, "
                    f"{layer_label_projection('n')}"
                )
                result = cypher_executor.execute_read_only(
                    board_id, cypher, {"ids": ids}, max_rows=len(ids) or 1
                )
                for row in result.get("rows", []):
                    embedding = row[2]
                    if embedding is None:
                        emit_missing_embedding_skipped(
                            board_id=board_id, node_type=ntype
                        )
                        logger.warning(
                            "global_discovery.missing_embedding_skipped "
                            "board=%s node_type=%s original_node_id=%s",
                            board_id,
                            ntype,
                            row[0],
                            extra={
                                "event": "global_discovery.missing_embedding_skipped",
                                "board_id": board_id,
                                "node_type": ntype,
                                "original_node_id": row[0],
                            },
                        )
                        continue
                    out.append(
                        {
                            "id": row[0],
                            "title": row[1],
                            "embedding": embedding,
                            "graph_layer": row[3],
                            "node_type": ntype,
                        }
                    )
            # R7 IMP5: enrich digested Learning nodes with the data the
            # canonical-only completeness rule needs.
            learning_ids = by_type.get("Learning")
            if learning_ids:
                GlobalOutboxProcessor._attach_learning_completeness(
                    board_id, out, learning_ids
                )
        except Exception as exc:
            logger.warning(
                "outbox.read_board_failed board=%s err=%s",
                board_id,
                exc,
            )
            return None
        return out

    @staticmethod
    def _read_learning_completeness_evidence(
        board_id: str,
        learning_ids: list[str],
    ) -> dict[str, dict]:
        """Read Learning evidence without expanding one row per relationship.

        ``validates`` is aggregated to one row per Learning. ``relates_to`` is
        grouped by semantic endpoint tuple and keyset-paginated, so a Learning
        with more than the executor's row cap cannot be silently classified
        from a truncated prefix of its edges.
        """

        ids = sorted(set(learning_ids))
        evidence = {
            learning_id: {
                "canonical_bug_count": 0,
                "working_bug_count": 0,
                "relates_to_endpoints": [],
            }
            for learning_id in ids
        }
        if not ids:
            return evidence

        cypher_executor = get_kg_registry().cypher_executor
        result = cypher_executor.execute_read_only(
            board_id,
            "MATCH (l:Learning)-[:validates]->(b:Bug) "
            "WHERE l.id IN $ids RETURN l.id, "
            "sum(CASE WHEN b.graph_layer = 'canonical' THEN 1 ELSE 0 END), "
            "count(b)",
            {"ids": ids},
            max_rows=len(ids),
        )
        for row in result.get("rows", []):
            learning_id = str(row[0] or "")
            entry = evidence.get(learning_id)
            if entry is None:
                raise RuntimeError(
                    "outbox.learning_evidence_unexpected_identity: "
                    f"board={board_id} identity={learning_id}"
                )
            canonical_count = int(row[1] or 0)
            total_count = int(row[2] or 0)
            if canonical_count < 0 or total_count < canonical_count:
                raise RuntimeError(
                    "outbox.learning_evidence_invalid_count: "
                    f"board={board_id} identity={learning_id}"
                )
            entry["canonical_bug_count"] = canonical_count
            entry["working_bug_count"] = total_count - canonical_count

        after_key = ("", "", "")
        while True:
            result = cypher_executor.execute_read_only(
                board_id,
                "MATCH (l:Learning)-[:relates_to]->(t) "
                "WHERE l.id IN $ids AND ("
                "l.id > $after_learning_id OR "
                "(l.id = $after_learning_id AND "
                "label(t) > $after_node_type) OR "
                "(l.id = $after_learning_id AND "
                "label(t) = $after_node_type AND "
                "coalesce(t.graph_layer, 'legacy_unknown') > $after_layer)) "
                "RETURN l.id, label(t), "
                "coalesce(t.graph_layer, 'legacy_unknown'), count(t) "
                "ORDER BY l.id, label(t), "
                "coalesce(t.graph_layer, 'legacy_unknown') "
                f"LIMIT {LEARNING_RELATION_GROUP_PAGE_SIZE}",
                {
                    "ids": ids,
                    "after_learning_id": after_key[0],
                    "after_node_type": after_key[1],
                    "after_layer": after_key[2],
                },
                max_rows=LEARNING_RELATION_GROUP_PAGE_SIZE,
            )
            rows = list(result.get("rows", []))
            page_last_key = after_key
            for row in rows:
                key = (
                    str(row[0] or ""),
                    str(row[1] or ""),
                    str(row[2] or "legacy_unknown"),
                )
                grouped_count = int(row[3] or 0) if len(row) > 3 else 0
                if key <= page_last_key or grouped_count < 1:
                    raise RuntimeError(
                        "outbox.learning_evidence_pagination_stalled: "
                        f"board={board_id} after={after_key!r}"
                    )
                entry = evidence.get(key[0])
                if entry is None:
                    raise RuntimeError(
                        "outbox.learning_evidence_unexpected_identity: "
                        f"board={board_id} identity={key[0]}"
                    )
                entry["relates_to_endpoints"].append(
                    (key[1], None if key[2] == "legacy_unknown" else key[2])
                )
                page_last_key = key
            if len(rows) < LEARNING_RELATION_GROUP_PAGE_SIZE:
                break
            if page_last_key == after_key:
                raise RuntimeError(
                    "outbox.learning_evidence_pagination_stalled: "
                    f"board={board_id} empty_page"
                )
            after_key = page_last_key
        return evidence

    @staticmethod
    def _attach_learning_completeness(
        board_id: str,
        out: list[dict],
        learning_ids: list[str],
    ) -> None:
        """Attach ``source_artifact_ref`` + ``canonical_bug_count`` /
        ``working_bug_count`` + ``relates_to_endpoints`` to each digested Learning
        entry in ``out``.

        Scoped reads over the same board graph: the Learning's source ref, its
        ``validates -> Bug`` endpoints bucketed by the Bug's layer, and (S-KG-02)
        its ``relates_to`` taxonomy endpoints with type+layer so the central
        completeness rule can decide a NON-bug Learning's canonical publication.
        ``_apply_event`` feeds these into the rule; no decision is made here.
        """
        meta: dict[str, dict] = {}
        cypher_executor = get_kg_registry().cypher_executor
        result = cypher_executor.execute_read_only(
            board_id,
            "MATCH (l:Learning) WHERE l.id IN $ids RETURN l.id, l.source_artifact_ref",
            {"ids": learning_ids},
            max_rows=len(learning_ids) or 1,
        )
        for row in result.get("rows", []):
            meta[str(row[0])] = {
                "source_artifact_ref": str(row[1] or ""),
                "canonical_bug_count": 0,
                "working_bug_count": 0,
                "relates_to_endpoints": [],
            }
        evidence = GlobalOutboxProcessor._read_learning_completeness_evidence(
            board_id,
            learning_ids,
        )
        for learning_id, values in evidence.items():
            entry = meta.get(learning_id)
            if entry is not None:
                entry.update(values)
        for node in out:
            if node.get("node_type") != "Learning":
                continue
            m = meta.get(str(node["id"]))
            if m is not None:
                node.update(m)

    async def _reconcile_board_digest_layers(
        self,
        gconn,
        board_id: str,
        db: Any,
        *,
        source_types_by_id: dict[str, str] | None = None,
        expected_state: dict[str, tuple[str, str]] | None = None,
        republished_source_ids: set[str] | None = None,
    ) -> int:
        """R1-IMP1 — re-derive the expected publication layer for the board's
        already-published digests and correct drift in place.

        Runs in EVERY event regardless of add refs (FR1/FR2). Preserves
        ``original_node_id`` (global identity); only the ``graph_layer`` metadata
        is mutated. Existing digests are reconciled even when their source ids
        also occur in this event's add refs: the later add-path can legitimately
        omit a ref (for example while materializing the source node), and that
        must not turn the ref into a parity-reconciliation exclusion.
        ``republished_source_ids`` receives identities whose complete digest was
        already materialized from the current source graph, allowing the caller
        to suppress a redundant add-path write without suppressing healthy
        update refs.
        Returns the count of digests corrected.
        """
        from okto_pulse.core.kg.canonical_partition_integrity import (
            pending_or_debt_exclusions,
        )
        from okto_pulse.core.kg.global_discovery.layer_parity import (
            resolve_expected_digest_layer,
        )
        from okto_pulse.core.kg.global_discovery.metrics import (
            emit_canonical_incomplete_excluded,
        )
        from okto_pulse.core.kg.rebuild_audit import (
            normalize_cognitive_artifact_id,
        )

        if source_types_by_id is None:
            source_types_by_id = await self._run_graph_io(
                lambda: self._read_board_digestable_node_types(board_id)
            )
        if source_types_by_id is None:
            raise RuntimeError(
                "outbox.read_board_failed: could not enumerate authoritative "
                "source digest identities"
            )

        targets = await self._run_graph_io(
            lambda: self._read_global_digest_rows(gconn, board_id)
        )
        invalid_link_pruned = await self._run_graph_io(
            lambda: gconn.delete_invalid_board_digest_links(
                board_id=board_id,
                expected_digest_ids=tuple(
                    f"dd_{board_id[:8]}_{source_id}"
                    for source_id in sorted(source_types_by_id)
                ),
            )
        )
        edge_rows = await self._run_graph_io(
            lambda: self._read_board_digest_edge_rows(gconn, board_id)
        )
        inbound_edge_counts = await self._run_graph_io(
            lambda: self._read_digest_inbound_edge_counts(gconn, board_id)
        )
        targets_by_identity: dict[str, list[dict[str, str]]] = {}
        for target in targets:
            oid = target["original_node_id"]
            if oid:
                targets_by_identity.setdefault(oid, []).append(target)

        board_meta = await self._run_graph_io(
            lambda: self._read_board_layer_meta(
                board_id,
                source_types_by_id,
            )
        )
        if board_meta is None:
            # Board unreadable: do not guess a layer and, critically, do not ACK
            # the outbox row. Raising the established retryable marker leaves the
            # event pending so a later drain can converge the same digests after
            # the transient source-graph failure clears.
            raise RuntimeError(
                "outbox.read_board_failed: could not read source graph layer metadata"
            )
        missing_meta = sorted(set(source_types_by_id) - set(board_meta))
        if missing_meta:
            raise RuntimeError(
                "outbox.read_board_failed: authoritative source disappeared "
                f"during reconciliation board={board_id} "
                f"identity={missing_meta[0]}"
            )

        needs_overlay = any(
            m.get("node_type") == "Learning" and m.get("graph_layer") == "canonical"
            for m in board_meta.values()
        )
        overlay = (
            await pending_or_debt_exclusions(db, board_id=board_id)
            if needs_overlay
            else {}
        )

        # ``count`` is the number of concrete cache mutations performed. Invalid
        # outgoing Board links are mutations too, even when the correct digest
        # and its correct edge already existed.
        corrected = invalid_link_pruned
        repaired = 0
        backfilled = 0
        layer_corrected = 0
        link_repaired = 0
        duplicate_count = sum(
            1 for rows in targets_by_identity.values() if len(rows) > 1
        )
        expected_by_identity: dict[str, tuple[str, str]] = {}
        exclusion_by_identity: dict[str, str | None] = {}
        for original_node_id in sorted(source_types_by_id):
            meta = board_meta[original_node_id]
            artifact_id = normalize_cognitive_artifact_id(
                meta.get("source_artifact_ref") or ""
            )
            expected, exclusion_reason = resolve_expected_digest_layer(
                node_type=meta["node_type"],
                raw_graph_layer=meta["graph_layer"],
                source_artifact_ref=meta.get("source_artifact_ref") or "",
                canonical_bug_count=int(meta.get("canonical_bug_count") or 0),
                relates_to_endpoints=tuple(meta.get("relates_to_endpoints") or ()),
                overlay_exclusion_reason=overlay.get(artifact_id),
            )
            expected_by_identity[original_node_id] = (
                expected,
                source_types_by_id[original_node_id],
            )
            exclusion_by_identity[original_node_id] = exclusion_reason

        republish_ids: set[str] = set()
        for original_node_id, node_type in source_types_by_id.items():
            identity_rows = targets_by_identity.get(original_node_id, [])
            digest_id = f"dd_{board_id[:8]}_{original_node_id}"
            if (
                len(identity_rows) != 1
                or identity_rows[0]["digest_id"] != digest_id
                or identity_rows[0]["node_type"] != node_type
            ):
                republish_ids.add(original_node_id)

        source_by_identity: dict[str, dict] = {}
        if republish_ids:
            source_nodes = await self._run_graph_io(
                lambda: self._read_board_nodes_for_refs(
                    board_id,
                    [
                        GlobalOutboxNodeRefFact(
                            graph_node_id=original_node_id,
                            graph_node_type=source_types_by_id[original_node_id],
                        )
                        for original_node_id in sorted(republish_ids)
                    ],
                )
            )
            if source_nodes is None:
                raise RuntimeError(
                    "outbox.read_board_failed: could not materialize authoritative "
                    "sources for digest repair"
                )
            for source in source_nodes:
                source_id = str(source.get("id") or "")
                if source_id in source_by_identity:
                    raise RuntimeError(
                        "outbox.source_identity_ambiguous: duplicate source "
                        f"materialization board={board_id} identity={source_id}"
                    )
                source_by_identity[source_id] = source
            missing_sources = sorted(republish_ids - set(source_by_identity))
            if missing_sources:
                raise RuntimeError(
                    "outbox.digest_reconcile_verification_failed: source "
                    f"materialization missing board={board_id} "
                    f"identity={missing_sources[0]}"
                )

        global_digest_ids = {
            target["digest_id"] for target in targets if target["digest_id"]
        }
        timestamp = self._now().strftime("%Y-%m-%dT%H:%M:%S")
        for original_node_id in sorted(source_types_by_id):
            identity_rows = targets_by_identity.get(original_node_id, [])
            expected, node_type = expected_by_identity[original_node_id]
            exclusion_reason = exclusion_by_identity[original_node_id]
            digest_id = f"dd_{board_id[:8]}_{original_node_id}"
            mutated = False
            if original_node_id in republish_ids:
                source = source_by_identity[original_node_id]
                digest_values = {
                    "digest_id": digest_id,
                    "board_id": board_id,
                    "original_node_id": original_node_id,
                    "title": str(source.get("title") or ""),
                    "summary": str(source.get("title") or "")[:280],
                    "node_type": node_type,
                    "graph_layer": expected,
                    "embedding": list(source["embedding"]),
                    "created_at": timestamp,
                }
                if identity_rows or digest_id in global_digest_ids:
                    removed = await self._run_graph_io(
                        lambda digest_values=digest_values: (
                            gconn.replace_decision_digest_identity(**digest_values)
                        )
                    )
                    if removed < 1:
                        raise RuntimeError(
                            "outbox.digest_reconcile_verification_failed: "
                            f"replace removed no rows for board={board_id} "
                            f"identity={original_node_id}"
                        )
                    outcome = "updated"
                    repaired += 1
                else:
                    outcome = await self._run_graph_io(
                        lambda digest_values=digest_values: (
                            gconn.upsert_decision_digest(**digest_values)
                        )
                    )
                    await self._run_graph_io(
                        lambda digest_id=digest_id: gconn.link_board_digest(
                            board_id=board_id,
                            digest_id=digest_id,
                        )
                    )
                    backfilled += 1
                emit_digest_upsert(
                    board_id=board_id,
                    node_type=node_type,
                    outcome=outcome,
                )
                if republished_source_ids is not None:
                    republished_source_ids.add(original_node_id)
                corrected += 1
                mutated = True
            else:
                if identity_rows[0]["current_layer"] != expected:
                    await self._run_graph_io(
                        lambda original_node_id=original_node_id, expected=expected: (
                            gconn.execute(
                                "MATCH (d:DecisionDigest) "
                                "WHERE d.board_id = $bid "
                                "AND d.original_node_id = $oid "
                                "SET d.graph_layer = $layer",
                                {
                                    "bid": board_id,
                                    "oid": original_node_id,
                                    "layer": expected,
                                },
                            )
                        )
                    )
                    layer_corrected += 1
                    mutated = True

                correct_edge_count = self._correct_board_digest_edge_count(
                    edge_rows,
                    board_id=board_id,
                    digest_id=digest_id,
                    original_node_id=original_node_id,
                )
                total_inbound = inbound_edge_counts.get(digest_id, 0)
                if correct_edge_count == 0 and total_inbound == 0:
                    await self._run_graph_io(
                        lambda digest_id=digest_id: gconn.link_board_digest(
                            board_id=board_id,
                            digest_id=digest_id,
                        )
                    )
                    link_repaired += 1
                    mutated = True
                elif correct_edge_count != 1 or total_inbound != 1:
                    removed_links = await self._run_graph_io(
                        lambda digest_id=digest_id: gconn.normalize_board_digest_link(
                            board_id=board_id,
                            digest_id=digest_id,
                        )
                    )
                    if removed_links != total_inbound:
                        raise RuntimeError(
                            "outbox.digest_reconcile_verification_failed: "
                            f"link normalize board={board_id} "
                            f"digest={digest_id} removed={removed_links} "
                            f"expected={total_inbound}"
                        )
                    link_repaired += 1
                    mutated = True
                if mutated:
                    corrected += 1
            if mutated and exclusion_reason:
                emit_canonical_incomplete_excluded(
                    board_id=board_id,
                    reason_code=exclusion_reason,
                )

        # Never ACK based on the write call alone.  A historical Ladybug file
        # can contain duplicate physical rows with the same primary key; a
        # primary-key MATCH may update only one and still report success.
        await self._run_graph_io(
            lambda: self._verify_reconciled_digest_layers(
                gconn,
                board_id,
                expected_by_identity,
            )
        )
        await self._run_graph_io(
            lambda: self._assert_source_inventory_unchanged(
                board_id,
                source_types_by_id,
            )
        )
        if expected_state is not None:
            expected_state.update(expected_by_identity)
        logger.info(
            "outbox.digest_layer_reconciled board=%s count=%d repaired=%d "
            "backfilled=%d layer_corrected=%d link_repaired=%d "
            "invalid_link_pruned=%d duplicate_count=%d verified=%d",
            board_id,
            corrected,
            repaired,
            backfilled,
            layer_corrected,
            link_repaired,
            invalid_link_pruned,
            duplicate_count,
            len(expected_by_identity),
            extra={
                "event": "outbox.digest_layer_reconciled",
                "board_id": board_id,
                "count": corrected,
                "repaired_count": repaired,
                "backfilled_count": backfilled,
                "layer_corrected_count": layer_corrected,
                "link_repaired_count": link_repaired,
                "invalid_link_pruned_count": invalid_link_pruned,
                "duplicate_count": duplicate_count,
                "verified_count": len(expected_by_identity),
            },
        )
        return corrected

    @staticmethod
    def _read_global_digest_rows(gconn, board_id: str) -> list[dict[str, str]]:
        res = gconn.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $bid "
            "RETURN d.id, d.original_node_id, d.node_type, "
            "coalesce(d.graph_layer, 'legacy_unknown')",
            {"bid": board_id},
        )
        rows: list[dict[str, str]] = []
        for row in res.rows:
            rows.append(
                {
                    "digest_id": str(row[0] or ""),
                    "original_node_id": str(row[1] or ""),
                    "node_type": str(row[2] or ""),
                    "current_layer": str(row[3] or "legacy_unknown"),
                }
            )
        return rows

    @staticmethod
    def _read_board_digest_edge_rows(
        gconn,
        board_id: str,
    ) -> list[dict[str, str | int]]:
        result = gconn.execute(
            "MATCH (b:Board)-[r:CONTAINS_DECISION]->(d:DecisionDigest) "
            "WHERE b.board_id = $bid "
            "RETURN b.board_id, d.board_id, d.id, "
            "d.original_node_id, count(r)",
            {"bid": board_id},
        )
        return [
            {
                "source_board_id": str(row[0] or ""),
                "digest_board_id": str(row[1] or ""),
                "digest_id": str(row[2] or ""),
                "original_node_id": str(row[3] or ""),
                "edge_count": int(row[4] or 0),
            }
            for row in result.rows
        ]

    @staticmethod
    def _read_digest_inbound_edge_counts(
        gconn,
        board_id: str,
    ) -> dict[str, int]:
        result = gconn.execute(
            "MATCH (:Board)-[r:CONTAINS_DECISION]->(d:DecisionDigest) "
            "WHERE d.board_id = $bid RETURN d.id, count(r)",
            {"bid": board_id},
        )
        counts: dict[str, int] = {}
        for row in result.rows:
            digest_id = str(row[0] or "")
            counts[digest_id] = counts.get(digest_id, 0) + int(row[1] or 0)
        return counts

    @staticmethod
    def _correct_board_digest_edge_count(
        edge_rows: list[dict[str, str | int]],
        *,
        board_id: str,
        digest_id: str,
        original_node_id: str,
    ) -> int:
        return sum(
            int(row["edge_count"])
            for row in edge_rows
            if row["source_board_id"] == board_id
            and row["digest_board_id"] == board_id
            and row["digest_id"] == digest_id
            and row["original_node_id"] == original_node_id
        )

    def _verify_reconciled_digest_layers(
        self,
        gconn,
        board_id: str,
        expected_by_identity: dict[str, tuple[str, str]],
    ) -> None:
        rows = self._read_global_digest_rows(gconn, board_id)
        rows_by_identity: dict[str, list[dict[str, str]]] = {}
        missing_identity_rows = 0
        for row in rows:
            oid = row["original_node_id"]
            if oid:
                rows_by_identity.setdefault(oid, []).append(row)
            else:
                missing_identity_rows += 1

        violations: list[str] = []
        if missing_identity_rows:
            violations.append(f"missing_original_node_id_rows={missing_identity_rows}")
        for oid, identity_rows in sorted(rows_by_identity.items()):
            if len(identity_rows) != 1:
                violations.append(f"{oid}:physical_rows={len(identity_rows)}")

        unexpected = sorted(set(rows_by_identity) - set(expected_by_identity))
        for oid in unexpected[:5]:
            violations.append(f"{oid}:unexpected_global_identity")

        for oid, (expected_layer, expected_type) in sorted(
            expected_by_identity.items()
        ):
            identity_rows = rows_by_identity.get(oid, [])
            if len(identity_rows) != 1:
                if not identity_rows:
                    violations.append(f"{oid}:physical_rows=0")
                continue
            row = identity_rows[0]
            stable_digest_id = f"dd_{board_id[:8]}_{oid}"
            if row["digest_id"] != stable_digest_id:
                violations.append(f"{oid}:unstable_digest_id")
            if row["node_type"] != expected_type:
                violations.append(
                    f"{oid}:node_type={row['node_type']} expected={expected_type}"
                )
            if row["current_layer"] != expected_layer:
                violations.append(
                    f"{oid}:layer={row['current_layer']} expected={expected_layer}"
                )

        edge_rows = self._read_board_digest_edge_rows(gconn, board_id)
        inbound_edge_counts = self._read_digest_inbound_edge_counts(
            gconn,
            board_id,
        )
        expected_digest_ids = {
            f"dd_{board_id[:8]}_{oid}" for oid in expected_by_identity
        }
        for edge in edge_rows:
            oid = str(edge["original_node_id"])
            expected_digest_id = f"dd_{board_id[:8]}_{oid}"
            if (
                edge["source_board_id"] != board_id
                or edge["digest_board_id"] != board_id
                or oid not in expected_by_identity
                or edge["digest_id"] != expected_digest_id
            ):
                violations.append(
                    f"{oid or '<missing>'}:invalid_contains_edge "
                    f"source_board={edge['source_board_id']} "
                    f"digest_board={edge['digest_board_id']} "
                    f"digest_id={edge['digest_id']}"
                )
        for digest_id in sorted(set(inbound_edge_counts) - expected_digest_ids)[:5]:
            violations.append(f"{digest_id}:unexpected_inbound_edge")
        for oid in sorted(expected_by_identity):
            stable_digest_id = f"dd_{board_id[:8]}_{oid}"
            correct_edge_count = self._correct_board_digest_edge_count(
                edge_rows,
                board_id=board_id,
                digest_id=stable_digest_id,
                original_node_id=oid,
            )
            if correct_edge_count != 1:
                violations.append(f"{oid}:correct_contains_edges={correct_edge_count}")
            inbound_count = inbound_edge_counts.get(stable_digest_id, 0)
            if inbound_count != 1:
                violations.append(f"{oid}:total_inbound_contains_edges={inbound_count}")

        if violations:
            sample = ", ".join(violations[:5])
            raise RuntimeError(
                "outbox.digest_reconcile_verification_failed: "
                f"board={board_id} violations={len(violations)} sample={sample}"
            )

    @staticmethod
    def _read_board_layer_meta(
        board_id: str,
        node_types_by_id: dict[str, str],
    ) -> dict[str, dict] | None:
        """Read the CURRENT effective publication inputs for the given node ids,
        bucketed by the authoritative board source type. Returns
        ``{node_id: {node_type, graph_layer, source_artifact_ref,
        canonical_bug_count}}`` with ``graph_layer`` fail-closed to
        ``legacy_unknown``. ``None`` means the board graph could not be read."""

        by_type: dict[str, list[str]] = {}
        for nid, ntype in node_types_by_id.items():
            if ntype:
                by_type.setdefault(ntype, []).append(nid)

        out: dict[str, dict] = {}
        try:
            cypher_executor = get_kg_registry().cypher_executor
            for ntype, ids in by_type.items():
                result = cypher_executor.execute_read_only(
                    board_id,
                    f"MATCH (n:{ntype}) WHERE n.id IN $ids "
                    f"RETURN n.id, {layer_label_projection('n')}",
                    {"ids": ids},
                    max_rows=len(ids) or 1,
                )
                for row in result.get("rows", []):
                    out[str(row[0])] = {
                        "node_type": ntype,
                        "graph_layer": str(row[1] or "legacy_unknown"),
                        "source_artifact_ref": "",
                        "canonical_bug_count": 0,
                        "relates_to_endpoints": [],
                    }
            learning_ids = by_type.get("Learning") or []
            if learning_ids:
                result = cypher_executor.execute_read_only(
                    board_id,
                    "MATCH (l:Learning) WHERE l.id IN $ids "
                    "RETURN l.id, l.source_artifact_ref",
                    {"ids": learning_ids},
                    max_rows=len(learning_ids) or 1,
                )
                for row in result.get("rows", []):
                    m = out.get(str(row[0]))
                    if m is not None:
                        m["source_artifact_ref"] = str(row[1] or "")
                evidence = GlobalOutboxProcessor._read_learning_completeness_evidence(
                    board_id,
                    learning_ids,
                )
                for learning_id, values in evidence.items():
                    m = out.get(learning_id)
                    if m is not None:
                        m.update(values)
        except Exception as exc:
            logger.warning(
                "outbox.reconcile_board_read_failed board=%s err=%s",
                board_id,
                exc,
            )
            return None
        return out


def _is_retryable_global_open_error(error: str | None) -> bool:
    if not error:
        return False
    msg = error.lower()
    return any(code in msg for code in GLOBAL_OPEN_ERROR_CODES)


def _semantic_error_detail(exc: BaseException) -> str:
    if isinstance(exc, GraphError):
        return f"{exc.code}:{exc}"
    return str(exc)


__all__ = [
    "DEAD_LETTER_SENTINEL",
    "DIGESTED_NODE_TYPES",
    "GLOBAL_OPEN_ERROR_CODES",
    "GlobalOutboxProcessor",
    "MAX_RETRIES",
]
