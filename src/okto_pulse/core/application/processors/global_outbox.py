"""Global discovery outbox application processor.

Retry, idempotency and delivery transitions remain in Core. Polling and task
lifecycle are owned by an edition runner.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg.global_discovery.metrics import (
    DIGEST_UPSERT_CREATED,
    DIGEST_UPSERT_UPDATED,
    emit_digest_upsert,
    emit_missing_embedding_skipped,
)
from okto_pulse.core.domain.worker_policy import RetryPolicy
from okto_pulse.core.kg.cypher_templates import layer_label_projection
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES
from okto_pulse.core.ports.coordination import ClaimRepository, get_claim_repository
from okto_pulse.core.ports.global_outbox import (
    GlobalOutboxEventRecord,
    GlobalOutboxNodeRefFact,
    get_global_outbox_store,
)
from okto_pulse.core.ports.runtime_workers import WorkerClockPort

logger = logging.getLogger("okto_pulse.kg.global_discovery.outbox")

MAX_RETRIES = 5
DEAD_LETTER_SENTINEL = -1
GLOBAL_OPEN_ERROR_MARKERS = (
    "failed to open ladybugdb database",
    "discovery.lbug",
    "checksum verification failed",
    "corrupted wal file",
    "wal file is corrupted",
    "invalid wal record",
    "wal_record.cpp",
    "unreachable_code",
    "not a valid lbug database file",
)
BOARD_READ_ERROR_MARKERS = (
    "outbox.read_board_failed",
    "could not read source graph nodes",
    "existing ladybugdb graph could not be opened",
    "bootstrap_probe",
)

# Node types mirrored into the global discovery layer as DecisionDigest.
# DERIVED from the per-board VECTOR_INDEX_TYPES (TR1, spec 849d6292): only nodes
# with an HNSW-backed embedding are worth digesting for cross-board semantic
# search, and aliasing the schema tuple keeps the two lists from drifting again
# (the old hand-maintained subset omitted Requirement/APIContract/TestScenario/
# Bug, so those canonical types were never globally searchable).
DIGESTED_NODE_TYPES: tuple[str, ...] = VECTOR_INDEX_TYPES


def _global_discovery_runtime():
    return get_kg_registry().require_global_discovery_runtime()


class GlobalOutboxProcessor:
    def __init__(
        self,
        session_factory,
        interval_seconds: int = 5,
        *,
        claim_repository: ClaimRepository | None = None,
        clock: WorkerClockPort | None = None,
        retry_policy: RetryPolicy | None = None,
    ):
        self._factory = session_factory
        self._interval = interval_seconds
        self._claim_repository = claim_repository
        self._clock = clock
        self._retry_policy = retry_policy or RetryPolicy(max_attempts=MAX_RETRIES)

    def _now(self) -> datetime:
        return (
            self._clock.now()
            if self._clock is not None
            else datetime.now(timezone.utc)
        )

    async def process_once(self) -> int:
        """Process pending outbox events. Returns count processed."""
        processed = 0
        async with self._factory() as db:
            await self._recover_dead_lettered_global_open_failures(db)
            await self._recover_dead_lettered_board_read_failures(db)
            claim_repository = self._claim_repository or get_claim_repository()
            claimed = await claim_repository.claim_global_outbox(
                db,
                limit=50,
            )
            events = list(
                await get_global_outbox_store().materialize_claimed(db, claimed)
            )
            processed_events: list[GlobalOutboxEventRecord] = []
            for event in events:
                try:
                    from okto_pulse.core.kg.write_barrier import (
                        under_global_safe_write,
                    )

                    owner_token = f"global-outbox-{event.event_id}"
                    with under_global_safe_write(owner_token, "global_outbox_apply"):
                        await self._apply_event(event, db)
                    event.processed_at = self._now()
                    processed_events.append(event)
                    processed += 1
                except Exception as exc:
                    event.retry_count += 1
                    event.last_error = str(exc)[:500]
                    if self._retry_policy.after_failure(event.retry_count).terminal:
                        event.retry_count = DEAD_LETTER_SENTINEL
                        logger.warning(
                            "outbox.dead_letter event=%s board=%s err=%s",
                            event.event_id, event.board_id, exc,
                            extra={
                                "event": "outbox.dead_letter",
                                "event_id": event.event_id,
                                "board_id": event.board_id,
                            },
                        )
            if processed_events:
                try:
                    self._flush_global_discovery_storage_after_batch()
                except Exception as exc:
                    logger.warning(
                        "outbox.global_discovery_flush_failed count=%d err=%s",
                        len(processed_events), exc,
                        extra={
                            "event": "outbox.global_discovery_flush_failed",
                            "count": len(processed_events),
                        },
                    )
                    for event in processed_events:
                        event.processed_at = None
                        event.retry_count += 1
                        event.last_error = (
                            "global_discovery_flush_failed: "
                            f"{type(exc).__name__}: {exc}"
                        )[:500]
                    processed = 0
            await get_global_outbox_store().save_events(db, events)
            await get_global_outbox_store().commit(db)
        if processed:
            logger.info(
                "outbox.processed count=%d", processed,
                extra={"event": "outbox.processed", "count": processed},
            )
        return processed

    async def _recover_dead_lettered_global_open_failures(
        self,
        db: Any,
    ) -> int:
        """Requeue terminal rows caused by a recoverable global DB open error.

        The global discovery graph is a rebuildable cache fed by SQLite's
        transactional outbox. If LadybugDB reports WAL/open corruption, schema
        bootstrap purges and recreates that cache. Rows dead-lettered during the
        bad window would otherwise stay at ``retry_count = -1`` forever because
        the worker only selects non-negative retry counts.
        """
        candidates = await get_global_outbox_store().list_dead_letters(db, limit=50)
        rows = [
            row for row in candidates
            if _is_retryable_global_open_error(row.last_error)
        ]
        if not rows:
            return 0

        try:
            _global_discovery_runtime().execute("CALL SHOW_TABLES() RETURN name")
        except Exception as exc:
            logger.warning(
                "outbox.recovery.global_unavailable rows=%d err=%s",
                len(rows), exc,
                extra={
                    "event": "outbox.recovery.global_unavailable",
                    "rows": len(rows),
                },
            )
            return 0

        for row in rows:
            row.retry_count = 0
            row.last_error = None
        await get_global_outbox_store().save_events(db, rows)
        logger.warning(
            "outbox.recovery.requeued_dead_letters count=%d",
            len(rows),
            extra={
                "event": "outbox.recovery.requeued_dead_letters",
                "count": len(rows),
            },
        )
        return len(rows)

    async def _recover_dead_lettered_board_read_failures(
        self,
        db: Any,
    ) -> int:
        """Requeue terminal rows whose source board graph is now queryable.

        Board rebuild/recovery may happen after global outbox events already
        reached the terminal sentinel because ``_read_board_nodes_for_refs``
        could not open the per-board graph. Those events are not semantically
        invalid; they were blocked by a transient storage state. Once the
        source board opens again, requeue them so the global discovery cache
        catches up instead of carrying permanent operational debt.
        """

        candidates = await get_global_outbox_store().list_dead_letters(db, limit=50)
        rows = [
            row for row in candidates
            if _is_retryable_board_read_error(row.last_error)
        ]
        if not rows:
            return 0

        queryable_by_board: dict[str, bool] = {}
        requeued = 0
        for row in rows:
            board_id = row.board_id
            if not board_id:
                continue
            if board_id not in queryable_by_board:
                queryable_by_board[board_id] = self._board_graph_is_queryable(
                    board_id
                )
            if not queryable_by_board[board_id]:
                continue
            row.retry_count = 0
            row.last_error = None
            requeued += 1

        if not requeued:
            return 0
        await get_global_outbox_store().save_events(
            db,
            [row for row in rows if row.retry_count == 0 and row.last_error is None],
        )
        logger.warning(
            "outbox.recovery.requeued_board_read_dead_letters count=%d",
            requeued,
            extra={
                "event": "outbox.recovery.requeued_board_read_dead_letters",
                "count": requeued,
            },
        )
        return requeued

    async def _apply_event(self, event: GlobalOutboxEventRecord, db: Any) -> None:
        """Apply a single outbox event to the global discovery meta-graph.

        Upserts the Board summary node, then mirrors every searchable node
        added during the session into DecisionDigest (id, title, summary,
        node_type, embedding) linked via (Board)-[:CONTAINS_DECISION]->.
        Without this mirror, `query_global` has nothing to search over.

        KG-01.3.1 boundary: this is a write path against discovery.lbug.
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
        global_runtime.ensure_layer_schema()
        session_id = payload.get("session_id", "") or event.session_id
        nodes_added = payload.get("nodes_added", 0)
        ts = self._now().strftime("%Y-%m-%dT%H:%M:%S")

        # 1) Fetch the per-session kuzu node refs — the authoritative list of
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
            # Upsert Board summary node
            existing = gconn.execute(
                "MATCH (b:Board {board_id: $bid}) RETURN b.board_id",
                {"bid": board_id},
            )
            if existing.has_next():
                gconn.execute(
                    "MATCH (b:Board {board_id: $bid}) "
                    "SET b.decision_count = coalesce(b.decision_count, 0) + $n, "
                    "b.last_sync_at = timestamp($ts)",
                    {"bid": board_id, "n": nodes_added, "ts": ts},
                )
            else:
                from okto_pulse.core.kg.embedding import get_embedding_provider
                emb = get_embedding_provider().encode(f"Board {board_id}")
                gconn.execute(
                    "CREATE (b:Board {"
                    "board_id: $bid, name: $name, summary: $s, "
                    "summary_embedding: $emb, topic_count: 0, entity_count: 0, "
                    "decision_count: $n, "
                    "last_sync_at: timestamp($ts)})",
                    {"bid": board_id, "name": board_id, "s": "",
                     "emb": emb, "n": nodes_added, "ts": ts},
                )

            current_node_ids = self._read_board_digestable_node_ids(board_id)
            if current_node_ids is not None:
                self._prune_stale_board_digests(
                    gconn, board_id, current_node_ids,
                )

            # R1-IMP1 — parity reconciler (FR1/FR2). Correctness of
            # DecisionDigest.graph_layer cannot depend on a fresh `add` ref: in
            # EVERY event we re-derive the expected publication layer of the
            # board's already-published digests from the CURRENT board graph and
            # correct drift in place, preserving original_node_id. This runs
            # BEFORE the empty-refs early return so an out-of-band layer change
            # (e.g. a source promoted working->canonical, or a Learning whose
            # evidence matured) is reconciled even with no add refs this session.
            add_node_ids = {r.graph_node_id for r in refs}
            await self._reconcile_board_digest_layers(
                gconn, board_id, db, skip_node_ids=add_node_ids,
            )

            if not refs:
                return

            # 2) Read the just-added nodes back from the per-board Kùzu to
            # pick up the title + embedding computed at consolidation time.
            per_board = self._read_board_nodes_for_refs(board_id, refs)
            if per_board is None:
                raise RuntimeError(
                    "outbox.read_board_failed: could not read source graph nodes"
                )
            if not per_board:
                return

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
                n.get("node_type") == "Learning"
                and n.get("graph_layer") == "canonical"
                for n in per_board
            )
            learning_exclusions = (
                await pending_or_debt_exclusions(db, board_id=board_id)
                if has_canonical_learning
                else {}
            )

            # 3) Create one DecisionDigest per node + CONTAINS_DECISION edge.
            # Kuzu's HNSW-indexed columns (DecisionDigest.embedding) cannot be
            # mutated via SET after node creation — attempting it raises
            # "Cannot set property vec in table embeddings because it is used
            # in one or more indexes". So we MATCH first; on miss we CREATE
            # with the embedding baked in, on hit we SET only non-indexed
            # columns (title/summary/node_type may legitimately evolve; the
            # embedding is frozen for the life of the digest row).
            for node in per_board:
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
                if exclusion_reason:
                    emit_canonical_incomplete_excluded(
                        board_id=board_id, reason_code=exclusion_reason,
                    )
                existing_d = gconn.execute(
                    "MATCH (d:DecisionDigest {id: $did}) RETURN d.id",
                    {"did": digest_id},
                )
                if existing_d.has_next():
                    gconn.execute(
                        "MATCH (d:DecisionDigest {id: $did}) "
                        "SET d.board_id = $bid, d.original_node_id = $oid, "
                        "d.title = $title, d.one_line_summary = $summary, "
                        "d.node_type = $ntype, d.graph_layer = $layer",
                        {
                            "did": digest_id,
                            "bid": board_id,
                            "oid": node["id"],
                            "title": title,
                            "summary": title[:280],
                            "ntype": node["node_type"],
                            "layer": effective_layer,
                        },
                    )
                    digest_outcome = DIGEST_UPSERT_UPDATED
                else:
                    gconn.execute(
                        "CREATE (d:DecisionDigest {"
                        "id: $did, board_id: $bid, original_node_id: $oid, "
                        "title: $title, one_line_summary: $summary, "
                        "node_type: $ntype, graph_layer: $layer, embedding: $emb, "
                        "created_at: timestamp($ts)})",
                        {
                            "did": digest_id,
                            "bid": board_id,
                            "oid": node["id"],
                            "title": title,
                            "summary": title[:280],
                            "ntype": node["node_type"],
                            "layer": effective_layer,
                            "emb": node["embedding"],
                            "ts": ts,
                        },
                    )
                    digest_outcome = DIGEST_UPSERT_CREATED
                # or_38b60fe1: record the digest upsert per node_type so a
                # regression that stops digesting a vector type is detectable.
                emit_digest_upsert(
                    board_id=board_id,
                    node_type=node["node_type"],
                    outcome=digest_outcome,
                )
                # Idempotent edge: MATCH both endpoints, then MERGE the rel.
                # MERGE on a relationship does not touch indexed node properties.
                gconn.execute(
                    "MATCH (b:Board {board_id: $bid}), "
                    "(d:DecisionDigest {id: $did}) "
                    "MERGE (b)-[:CONTAINS_DECISION]->(d)",
                    {"bid": board_id, "did": digest_id},
                )
        finally:
            pass

    @staticmethod
    def _read_board_digestable_node_ids(board_id: str) -> set[str] | None:
        """Return current digestable node ids in the board graph.

        ``None`` means the source graph could not be read and pruning must not
        run. An empty set is meaningful: the board currently has no digestable
        nodes, so all old global digests for that board are stale.
        """

        ids: set[str] = set()
        try:
            cypher = get_kg_registry().cypher_executor
            for ntype in DIGESTED_NODE_TYPES:
                result = cypher.execute_read_only(
                    board_id,
                    f"MATCH (n:{ntype}) RETURN n.id",
                    max_rows=10000,
                )
                for row in result.get("rows", []):
                    if row and row[0]:
                        ids.add(str(row[0]))
        except Exception as exc:
            logger.warning(
                "outbox.digest_reconcile_board_read_failed board=%s err=%s",
                board_id, exc,
                extra={
                    "event": "outbox.digest_reconcile_board_read_failed",
                    "board_id": board_id,
                },
            )
            return None
        return ids

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
        stale_digest_ids: list[str] = []
        while res.has_next():
            row = res.get_next()
            digest_id = row[0]
            original_node_id = row[1]
            if digest_id and str(original_node_id) not in current_node_ids:
                stale_digest_ids.append(str(digest_id))

        for digest_id in stale_digest_ids:
            gconn.execute(
                "MATCH (d:DecisionDigest {id: $did}) DETACH DELETE d",
                {"did": digest_id},
            )

        if stale_digest_ids:
            logger.warning(
                "outbox.digest_reconcile_pruned_stale board=%s count=%d",
                board_id, len(stale_digest_ids),
                extra={
                    "event": "outbox.digest_reconcile_pruned_stale",
                    "board_id": board_id,
                    "count": len(stale_digest_ids),
                },
            )
        return len(stale_digest_ids)

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
        """Read (id, title, embedding) from the per-board Kùzu for the given
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
                            board_id, ntype, row[0],
                            extra={
                                "event": "global_discovery.missing_embedding_skipped",
                                "board_id": board_id,
                                "node_type": ntype,
                                "original_node_id": row[0],
                            },
                        )
                        continue
                    out.append({
                        "id": row[0],
                        "title": row[1],
                        "embedding": embedding,
                        "graph_layer": row[3],
                        "node_type": ntype,
                    })
            # R7 IMP5: enrich digested Learning nodes with the data the
            # canonical-only completeness rule needs.
            learning_ids = by_type.get("Learning")
            if learning_ids:
                GlobalOutboxProcessor._attach_learning_completeness(
                    board_id, out, learning_ids
                )
        except Exception as exc:
            logger.warning(
                "outbox.read_board_failed board=%s err=%s", board_id, exc,
            )
            return None
        return out

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
            "MATCH (l:Learning) WHERE l.id IN $ids "
            "RETURN l.id, l.source_artifact_ref",
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
        result = cypher_executor.execute_read_only(
            board_id,
            "MATCH (l:Learning)-[:validates]->(b:Bug) WHERE l.id IN $ids "
            "RETURN l.id, b.graph_layer",
            {"ids": learning_ids},
            max_rows=10000,
        )
        for row in result.get("rows", []):
            entry = meta.get(str(row[0]))
            if entry is None:
                continue
            if str(row[1] or "") == "canonical":
                entry["canonical_bug_count"] += 1
            else:
                entry["working_bug_count"] += 1
        # S-KG-02: relates_to -> taxonomy endpoints (the non-bug cognitive
        # provenance path) with type+layer, so the publication rule can canonize a
        # non-bug Learning only with a canonical S-KG-01 endpoint association.
        result = cypher_executor.execute_read_only(
            board_id,
            "MATCH (l:Learning)-[:relates_to]->(t) WHERE l.id IN $ids "
            "RETURN l.id, label(t), t.graph_layer",
            {"ids": learning_ids},
            max_rows=10000,
        )
        for row in result.get("rows", []):
            entry = meta.get(str(row[0]))
            if entry is None:
                continue
            entry["relates_to_endpoints"].append(
                (str(row[1] or ""), str(row[2] or "") or None)
            )
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
        skip_node_ids: set[str],
    ) -> int:
        """R1-IMP1 — re-derive the expected publication layer for the board's
        already-published digests and correct drift in place.

        Runs in EVERY event regardless of add refs (FR1/FR2). Preserves
        ``original_node_id`` (global identity); only the ``graph_layer`` metadata
        is mutated. ``skip_node_ids`` are the ids the add-path upsert below
        already handles this session (avoids double work / double metric).
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

        res = gconn.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $bid "
            "RETURN d.id, d.original_node_id, d.node_type, "
            "coalesce(d.graph_layer, 'legacy_unknown')",
            {"bid": board_id},
        )
        targets: list[dict] = []
        while res.has_next():
            row = res.get_next()
            oid = str(row[1]) if row[1] is not None else ""
            if not oid or oid in skip_node_ids:
                continue
            targets.append({
                "digest_id": str(row[0]),
                "original_node_id": oid,
                "node_type": str(row[2] or ""),
                "current_layer": str(row[3] or "legacy_unknown"),
            })
        if not targets:
            return 0

        board_meta = self._read_board_layer_meta(
            board_id, {t["original_node_id"]: t["node_type"] for t in targets},
        )
        if board_meta is None:
            # Board unreadable: do not guess a layer; leave digests for the next
            # drain (a transient storage state must not silently rewrite layers).
            return 0

        needs_overlay = any(
            m.get("node_type") == "Learning" and m.get("graph_layer") == "canonical"
            for m in board_meta.values()
        )
        overlay = (
            await pending_or_debt_exclusions(db, board_id=board_id)
            if needs_overlay else {}
        )

        corrected = 0
        for t in targets:
            meta = board_meta.get(t["original_node_id"])
            if meta is None:
                continue  # node vanished after prune; skip (prune handles it)
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
            if expected != t["current_layer"]:
                gconn.execute(
                    "MATCH (d:DecisionDigest {id: $did}) SET d.graph_layer = $layer",
                    {"did": t["digest_id"], "layer": expected},
                )
                corrected += 1
                if exclusion_reason:
                    emit_canonical_incomplete_excluded(
                        board_id=board_id, reason_code=exclusion_reason,
                    )
        if corrected:
            logger.info(
                "outbox.digest_layer_reconciled board=%s count=%d",
                board_id, corrected,
                extra={
                    "event": "outbox.digest_layer_reconciled",
                    "board_id": board_id,
                    "count": corrected,
                },
            )
        return corrected

    @staticmethod
    def _read_board_layer_meta(
        board_id: str,
        node_types_by_id: dict[str, str],
    ) -> dict[str, dict] | None:
        """Read the CURRENT effective publication inputs for the given node ids,
        bucketed by node_type (the digest's recorded type). Returns
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
                result = cypher_executor.execute_read_only(
                    board_id,
                    "MATCH (l:Learning)-[:validates]->(b:Bug) "
                    "WHERE l.id IN $ids RETURN l.id, b.graph_layer",
                    {"ids": learning_ids},
                    max_rows=10000,
                )
                for row in result.get("rows", []):
                    m = out.get(str(row[0]))
                    if m is not None and str(row[1] or "") == "canonical":
                        m["canonical_bug_count"] += 1
                # S-KG-02: relates_to -> taxonomy endpoints (type+layer) so the
                # publication rule canonizes a non-bug Learning only with a
                # canonical S-KG-01 endpoint association (else fail-closed).
                result = cypher_executor.execute_read_only(
                    board_id,
                    "MATCH (l:Learning)-[:relates_to]->(t) "
                    "WHERE l.id IN $ids RETURN l.id, label(t), t.graph_layer",
                    {"ids": learning_ids},
                    max_rows=10000,
                )
                for row in result.get("rows", []):
                    m = out.get(str(row[0]))
                    if m is not None:
                        m["relates_to_endpoints"].append(
                            (str(row[1] or ""), str(row[2] or "") or None)
                        )
        except Exception as exc:
            logger.warning(
                "outbox.reconcile_board_read_failed board=%s err=%s",
                board_id, exc,
            )
            return None
        return out

    @staticmethod
    def _board_graph_is_queryable(board_id: str) -> bool:
        try:
            get_kg_registry().cypher_executor.execute_read_only(
                board_id,
                "CALL SHOW_TABLES() RETURN name",
                max_rows=1,
            )
            return True
        except Exception as exc:
            logger.warning(
                "outbox.recovery.board_unavailable board=%s err=%s",
                board_id, exc,
                extra={
                    "event": "outbox.recovery.board_unavailable",
                    "board_id": board_id,
                },
            )
            return False

def _is_retryable_global_open_error(error: str | None) -> bool:
    if not error:
        return False
    msg = error.lower()
    return any(marker in msg for marker in GLOBAL_OPEN_ERROR_MARKERS)


def _is_retryable_board_read_error(error: str | None) -> bool:
    if not error:
        return False
    msg = error.lower()
    return any(marker in msg for marker in BOARD_READ_ERROR_MARKERS)


__all__ = [
    "BOARD_READ_ERROR_MARKERS",
    "DEAD_LETTER_SENTINEL",
    "DIGESTED_NODE_TYPES",
    "GLOBAL_OPEN_ERROR_MARKERS",
    "GlobalOutboxProcessor",
    "MAX_RETRIES",
]
