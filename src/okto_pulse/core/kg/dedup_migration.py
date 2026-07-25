"""NC-8 — consolidate duplicate graph backend nodes per (node_type, source_artifact_ref).

Spec 7f23535f originally re-pointed every edge of the duplicates to the
canonical node and `DETACH DELETE`d them. Spec MKG-C-S1 (BR1/D2) makes the
default REVERSIBLE BY CONSTRUCTION:

* the complete pre-operation snapshot (member attrs + every incident edge
  with every property) is appended to the off-graph EquivalenceLedger
  BEFORE the first graph write (fail-closed);
* duplicates are TOMBSTONED (``superseded_by = survivor``) — zero edge
  re-point and zero physical delete (the bulk in-place mutation class that
  corrupted the same engine elsewhere: marginalia ADR 0007 / KGD-01);
* the legacy physical path only exists behind ``hard_delete=True`` and is
  classified ``forbidden`` by the CurationPolicy — physical
  materialization happens only inside the deterministic rebuild.

Idempotent — groups consider ACTIVE members only, so a re-run after a
tombstone dedup reports 0 actions. Un-merge = ledger revoke +
de-tombstone (see ``kg unmerge``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import uuid

from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.curation_policy import require_curation_allowed
from okto_pulse.core.kg.guarded_write import (
    GuardedWriteLease,
    guarded_board_write,
)
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.ports.kg_equivalence_ledger import (
    EquivalenceRecord,
    require_equivalence_ledger,
)
from okto_pulse.core.ports.kg_curation_proposals import (
    CurationProposal,
    require_curation_proposal_store,
)
from okto_pulse.core.kg.schema_contract import (
    EDGE_METADATA_COLUMNS,
    MULTI_REL_TYPES,
    NODE_TYPES,
    REL_TYPES,
)

logger = logging.getLogger("okto_pulse.kg.dedup_migration")

_DEDUP_WRITE_OPERATION = "kg_dedup_entities"
_UNMERGE_WRITE_OPERATION = "kg_unmerge"

# Node attrs we read for tie-break / canonical pick. Must align with the
# schema in core/kg/schema.py — only `created_at` is required for the
# "most recent wins" rule. Others surface in the report for ops triage.
_NODE_REPORT_COLS = ("id", "created_at", "title", "human_curated")


class _LeaseFencedGraphScope:
    """Revalidate ownership immediately before every dedup graph statement.

    The heartbeat poisons ``GuardedWriteLease`` when renewal fails.  Embedded
    graph statements auto-commit, so merely checking at lifecycle time is too
    late: an expired writer could otherwise keep issuing statements until the
    body returns.  This narrow proxy makes the next statement fail closed.
    """

    def __init__(self, scope: Any, lease: GuardedWriteLease) -> None:
        self._scope = scope
        self._lease = lease

    def execute(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        self._lease.ensure_owned(failure_phase="before_graph_statement")
        if params is None:
            return self._scope.execute(statement)
        return self._scope.execute(statement, params)

    def mark_superseded(
        self,
        node_type: str,
        node_id: str,
        *,
        superseded_by: str,
        superseded_at: str,
        revocation_reason: str,
    ) -> None:
        self._lease.ensure_owned(failure_phase="before_mark_superseded")
        self._scope.mark_superseded(
            node_type,
            node_id,
            superseded_by=superseded_by,
            superseded_at=superseded_at,
            revocation_reason=revocation_reason,
        )


def _all_rel_pairs() -> list[tuple[str, str, str]]:
    """Flatten REL_TYPES + MULTI_REL_TYPES into a single list of triples
    `(rel_name, from_type, to_type)` so the migration can iterate over
    every edge variant without case-by-case logic.
    """
    out: list[tuple[str, str, str]] = list(REL_TYPES)
    for rel_name, pairs in MULTI_REL_TYPES:
        for from_t, to_t in pairs:
            out.append((rel_name, from_t, to_t))
    return out


def _fetch_groups(graph_scope, node_type: str) -> list[dict[str, Any]]:
    """Return list of duplicate groups for a node type.

    Each group: `{source_artifact_ref, count, members: [{id, created_at,
    title, human_curated}]}`. Empty list when no duplicates exist —
    callers can short-circuit.
    """
    # MKG-C-S1: ACTIVE members only — a tombstoned duplicate must not
    # re-enter the group on the next run (idempotency of the reversible
    # default) nor be counted as a duplicate again.
    res = graph_scope.execute(
        f"MATCH (n:{node_type}) "
        f"WHERE n.source_artifact_ref <> '' "
        f"AND n.superseded_by IS NULL "
        f"RETURN n.source_artifact_ref, n.id, n.created_at, n.title, "
        f"n.human_curated"
    )
    rows: dict[str, list[dict[str, Any]]] = {}
    for row in res.rows:
        ref = row[0]
        rows.setdefault(ref, []).append({
            "id": row[1],
            "created_at": row[2],
            "title": row[3],
            "human_curated": row[4],
        })
    groups: list[dict[str, Any]] = []
    for ref, members in rows.items():
        if len(members) <= 1:
            continue
        members.sort(key=lambda m: m["created_at"] or "", reverse=True)
        groups.append({
            "source_artifact_ref": ref,
            "count": len(members),
            "members": members,
        })
    return groups


def _repoint_edges(
    graph_scope,
    rel_name: str,
    from_type: str,
    to_type: str,
    duplicate_id: str,
    canonical_id: str,
) -> int:
    """Re-point every `(:from_type)-[r:rel_name]->(:to_type)` edge that
    touches `duplicate_id` to use `canonical_id` instead. Returns the
    number of edges re-pointed (0 if none exist for this rel pair).

    Strategy: read all matching edges with their attrs, DELETE old, then
    CREATE new with the same attrs against the canonical id. graph backend has no
    primitive REL UPDATE that swaps endpoints in place.
    """
    attr_cols = ", ".join(f"r.{name}" for name, _ in EDGE_METADATA_COLUMNS)
    # Outbound edges (duplicate is FROM endpoint)
    out_count = _repoint_outbound(
        graph_scope, rel_name, from_type, to_type, duplicate_id, canonical_id, attr_cols
    )
    # Inbound edges (duplicate is TO endpoint)
    in_count = _repoint_inbound(
        graph_scope, rel_name, from_type, to_type, duplicate_id, canonical_id, attr_cols
    )
    return out_count + in_count


def _read_edge_rows(graph_scope, cypher: str, params: dict) -> list[tuple]:
    res = graph_scope.execute(cypher, params)
    return [tuple(row) for row in res.rows]


def _repoint_outbound(
    graph_scope, rel_name, from_type, to_type, dup_id, canonical_id, attr_cols
) -> int:
    cypher_read = (
        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
        f"WHERE a.id = $dup "
        f"RETURN b.id, r.confidence, {attr_cols}"
    )
    rows = _read_edge_rows(graph_scope, cypher_read, {"dup": dup_id})
    if not rows:
        return 0
    # DELETE first to avoid graph backend uniqueness when re-creating same edge
    graph_scope.execute(
        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
        f"WHERE a.id = $dup DELETE r",
        {"dup": dup_id},
    )
    for row in rows:
        target_id, confidence = row[0], row[1]
        layer, rule_id, created_by, fallback_reason = row[2], row[3], row[4], row[5]
        params = {
            "src": canonical_id,
            "tgt": target_id,
            "conf": confidence,
            "layer": layer,
            "rule_id": rule_id,
            "created_by": created_by,
            "fallback_reason": fallback_reason,
        }
        graph_scope.execute(
            f"MATCH (a:{from_type}) WHERE a.id = $src "
            f"MATCH (b:{to_type}) WHERE b.id = $tgt "
            f"CREATE (a)-[:{rel_name} {{confidence: $conf, layer: $layer, "
            f"rule_id: $rule_id, created_by: $created_by, "
            f"fallback_reason: $fallback_reason}}]->(b)",
            params,
        )
    return len(rows)


def _repoint_inbound(
    graph_scope, rel_name, from_type, to_type, dup_id, canonical_id, attr_cols
) -> int:
    cypher_read = (
        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
        f"WHERE b.id = $dup "
        f"RETURN a.id, r.confidence, {attr_cols}"
    )
    rows = _read_edge_rows(graph_scope, cypher_read, {"dup": dup_id})
    if not rows:
        return 0
    graph_scope.execute(
        f"MATCH (a:{from_type})-[r:{rel_name}]->(b:{to_type}) "
        f"WHERE b.id = $dup DELETE r",
        {"dup": dup_id},
    )
    for row in rows:
        source_id, confidence = row[0], row[1]
        layer, rule_id, created_by, fallback_reason = row[2], row[3], row[4], row[5]
        params = {
            "src": source_id,
            "tgt": canonical_id,
            "conf": confidence,
            "layer": layer,
            "rule_id": rule_id,
            "created_by": created_by,
            "fallback_reason": fallback_reason,
        }
        graph_scope.execute(
            f"MATCH (a:{from_type}) WHERE a.id = $src "
            f"MATCH (b:{to_type}) WHERE b.id = $tgt "
            f"CREATE (a)-[:{rel_name} {{confidence: $conf, layer: $layer, "
            f"rule_id: $rule_id, created_by: $created_by, "
            f"fallback_reason: $fallback_reason}}]->(b)",
            params,
        )
    return len(rows)


def _delete_node(graph_scope, node_type: str, node_id: str) -> None:
    graph_scope.execute(
        f"MATCH (n:{node_type}) WHERE n.id = $id DETACH DELETE n",
        {"id": node_id},
    )


def _snapshot_group(
    graph_scope,
    node_type: str,
    members: list[dict[str, Any]],
    rel_pairs: list[tuple[str, str, str]],
) -> dict[str, Any]:
    """Complete pre-operation snapshot for the ledger (FR2, R1/R2).

    Node attrs are read whole (``RETURN n``) so nothing is hand-picked;
    the ``embedding`` vector is excluded — it is HNSW-locked, reproducible
    from title+content, and the tombstone default preserves the node
    itself anyway (the snapshot is reversal evidence, not a backup of the
    index). Edges carry EVERY metadata property plus the base attrs —
    never the 5 hardcoded ones of the legacy repoint (R2).
    """

    nodes: list[dict[str, Any]] = []
    for member in members:
        res = graph_scope.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $id RETURN n",
            {"id": member["id"]},
        )
        if res.rows:
            raw = res.rows[0][0]
            attrs = {
                k: v
                for k, v in dict(raw).items()
                if not k.startswith("_") and k != "embedding"
            }
            nodes.append({"id": member["id"], "attrs": attrs})

    attr_cols = ", ".join(f"r.{name}" for name, _ in EDGE_METADATA_COLUMNS)
    edges: list[dict[str, Any]] = []
    seen: set[tuple] = set()
    for member in members:
        for rel_name, from_t, to_t in rel_pairs:
            rows = _read_edge_rows(
                graph_scope,
                f"MATCH (a:{from_t})-[r:{rel_name}]->(b:{to_t}) "
                f"WHERE a.id = $id OR b.id = $id "
                f"RETURN a.id, b.id, r.confidence, "
                f"r.created_by_session_id, r.created_at, {attr_cols}",
                {"id": member["id"]},
            )
            for row in rows:
                key = (rel_name, from_t, to_t, row[0], row[1])
                if key in seen:
                    continue
                seen.add(key)
                props = {
                    "confidence": row[2],
                    "created_by_session_id": row[3],
                    "created_at": row[4],
                }
                for i, (name, _type) in enumerate(EDGE_METADATA_COLUMNS):
                    props[name] = row[5 + i]
                edges.append(
                    {
                        "type": rel_name,
                        "from_type": from_t,
                        "to_type": to_t,
                        "from": row[0],
                        "to": row[1],
                        "props": props,
                    }
                )
    return {"nodes": nodes, "edges": edges}


def _tombstone_members(
    graph_scope,
    node_type: str,
    duplicates: list[dict[str, Any]],
    canonical_id: str,
    record_id: str,
) -> int:
    """Reversible default (FR3/D2): mark duplicates superseded by the
    survivor — traceable to the ledger record; zero edge writes."""

    ts = datetime.now(timezone.utc).isoformat()
    for dup in duplicates:
        graph_scope.mark_superseded(
            node_type,
            dup["id"],
            superseded_by=canonical_id,
            superseded_at=ts,
            revocation_reason=f"dedup:{record_id}",
        )
    return len(duplicates)


def _reusable_equivalence_record(
    records: list[EquivalenceRecord],
    *,
    node_type: str,
    source_artifact_ref: str,
    canonical_id: str,
    duplicate_ids: set[str],
    proposal_id: str | None,
) -> EquivalenceRecord | None:
    """Return reversal evidence left by an interrupted prior attempt.

    The ledger append intentionally precedes graph mutation.  If the process
    then fails, a retry must reuse that append instead of creating a second
    active equivalence record for the same group.
    """

    for record in records:
        if (
            not record.is_active
            or record.operation != "dedup_entities"
            or record.node_type != node_type
            or record.survivor_id != canonical_id
            or not duplicate_ids.issubset(set(record.merged_ids))
        ):
            continue
        evidence = dict(record.evidence)
        recorded_ref = evidence.get("source_artifact_ref")
        if recorded_ref is not None and recorded_ref != source_artifact_ref:
            continue
        recorded_proposal = evidence.get("curation_proposal_id")
        if proposal_id is not None and recorded_proposal != proposal_id:
            continue
        if proposal_id is None and recorded_proposal is not None:
            continue
        return record
    return None


def migrate_dedup_entities(
    board_id: str,
    *,
    dry_run: bool = False,
    confirmed: bool = False,
    hard_delete: bool = False,
    created_by: str = "cli:kg-dedup",
) -> dict[str, Any]:
    """Run dedup for a single board, return structured report.

    Spec MKG-C-S1: the write path is gated by the CurationPolicy — a
    write without ``confirmed=True`` raises ``CurationPolicyError``
    (propose_only) and ``hard_delete=True`` is always refused
    (forbidden). The confirmed default records the complete snapshot in
    the EquivalenceLedger BEFORE the first graph write and tombstones
    the duplicates — no edge re-point, no physical delete (D2).

    On dry_run=True, all read steps execute (lookup duplicates, simulate
    edge counts) but nothing is written — graph, ledger and proposals
    all stay untouched (S8).
    """
    if dry_run:
        return _run_dedup_migration(
            board_id,
            dry_run=True,
            created_by=created_by,
            started=datetime.now(timezone.utc).isoformat(),
        )

    # FR5/BR2 — enforcement at the operation boundary: forbidden always
    # raises; an unconfirmed write raises with actionable remediation.
    require_curation_allowed(
        "kg_dedup_hard_delete" if hard_delete else _DEDUP_WRITE_OPERATION,
        confirmed=confirmed,
    )
    started = datetime.now(timezone.utc).isoformat()
    mutation_ref = f"kg-dedup:{board_id}:{uuid.uuid4().hex}"
    with guarded_board_write(
        board_id,
        operation=_DEDUP_WRITE_OPERATION,
        owner_id=created_by,
        mutation_ref=mutation_ref,
    ) as write_lease:
        return _run_dedup_migration(
            board_id,
            dry_run=False,
            created_by=created_by,
            started=started,
            write_lease=write_lease,
        )


def _run_dedup_migration(
    board_id: str,
    *,
    dry_run: bool,
    created_by: str,
    started: str,
    write_lease: GuardedWriteLease | None = None,
    proposal_id: str | None = None,
) -> dict[str, Any]:
    """Execute one dedup scan under the caller-owned board lease.

    The write caller owns exactly one lease from before the ledger append
    through graph durability and final cache invalidation.  Embedded graph
    adapters may auto-commit each statement, so an exception after a mutation
    was attempted still runs the lifecycle before the original failure escapes.
    """

    if dry_run != (write_lease is None):
        raise RuntimeError("dedup_write_lease_contract_violation")

    rel_pairs = _all_rel_pairs()
    groups_summary: list[dict[str, Any]] = []
    total_dups = 0
    total_edges = 0
    ledger_records = 0
    graph_write_possible = False
    active_records: list[EquivalenceRecord] = []

    async def _run_migration() -> None:
        # BR1 fail-closed: writing without a composed ledger must abort
        # BEFORE the graph transaction even opens.
        ledger = None if dry_run else require_equivalence_ledger()
        if ledger is not None:
            active_records.extend(await ledger.active_for_board(board_id))
        if write_lease is not None:
            write_lease.ensure_owned(failure_phase="before_graph_transaction")
        async with await get_kg_registry().graph_transaction.begin(
            board_id
        ) as raw_graph_scope:
            graph_scope = (
                _LeaseFencedGraphScope(raw_graph_scope, write_lease)
                if write_lease is not None
                else raw_graph_scope
            )
            await _run_migration_in_scope(graph_scope, ledger)
            if write_lease is not None:
                write_lease.ensure_owned(failure_phase="before_graph_scope_exit")

    async def _run_migration_in_scope(graph_scope: Any, ledger: Any) -> None:
        nonlocal total_dups, total_edges, ledger_records, graph_write_possible
        for node_type in NODE_TYPES:
            try:
                groups = _fetch_groups(graph_scope, node_type)
            except Exception as exc:
                logger.error(
                    "kg.dedup.scan_failed type=%s board=%s err=%s",
                    node_type, board_id, exc,
                    exc_info=True,
                    extra={
                        "event": "kg.dedup.scan_failed",
                        "node_type": node_type,
                        "board_id": board_id,
                    },
                )
                raise
            for group in groups:
                members = group["members"]
                canonical = members[0]
                duplicates = members[1:]
                edges_for_group = 0
                record_id = None
                if not dry_run:
                    # FR2 (BR1): complete snapshot appended BEFORE any
                    # graph write — an append failure aborts the whole
                    # operation with the graph untouched.
                    reusable = _reusable_equivalence_record(
                        active_records,
                        node_type=node_type,
                        source_artifact_ref=group["source_artifact_ref"],
                        canonical_id=canonical["id"],
                        duplicate_ids={d["id"] for d in duplicates},
                        proposal_id=proposal_id,
                    )
                    if reusable is not None:
                        record_id = reusable.record_id
                    else:
                        record_id = f"eqv_{uuid.uuid4().hex[:16]}"
                        evidence = _snapshot_group(
                            graph_scope, node_type, members, rel_pairs
                        )
                        evidence = {
                            **evidence,
                            "source_artifact_ref": group["source_artifact_ref"],
                        }
                        if proposal_id is not None:
                            evidence["curation_proposal_id"] = proposal_id
                        record = EquivalenceRecord(
                            record_id=record_id,
                            board_id=board_id,
                            node_type=node_type,
                            survivor_id=canonical["id"],
                            merged_ids=tuple(d["id"] for d in duplicates),
                            operation="dedup_entities",
                            evidence=evidence,
                            created_by=created_by,
                        )
                        write_lease.ensure_owned(
                            failure_phase="before_equivalence_append"
                        )
                        await ledger.append(record)
                        active_records.append(record)
                        ledger_records += 1
                    # FR3/D2: reversible default — tombstone, zero edge
                    # writes, zero deletes.
                    # Set the marker BEFORE the adapter call: an embedded
                    # auto-commit provider may persist and then raise.
                    graph_write_possible = True
                    _tombstone_members(
                        graph_scope, node_type, duplicates,
                        canonical["id"], record_id,
                    )
                else:
                    for dup in duplicates:
                        # Dry-run: count the edges that WOULD be involved.
                        for rel_name, from_t, to_t in rel_pairs:
                            res = graph_scope.execute(
                                f"MATCH (a:{from_t})-[r:{rel_name}]->"
                                f"(b:{to_t}) "
                                f"WHERE a.id = $dup OR b.id = $dup "
                                f"RETURN count(r)",
                                {"dup": dup["id"]},
                            )
                            if res.rows:
                                edges_for_group += int(res.rows[0][0])
                groups_summary.append({
                    "node_type": node_type,
                    "source_artifact_ref": group["source_artifact_ref"],
                    "duplicates_found": group["count"],
                    "canonical_id": canonical["id"],
                    "edges_repointed": 0 if not dry_run else edges_for_group,
                    "deleted_ids": [],
                    "tombstoned_ids": [d["id"] for d in duplicates],
                    "record_id": record_id,
                })
                total_dups += len(duplicates)
                total_edges += edges_for_group

    try:
        run_async_blocking(_run_migration())
    except BaseException as operation_exc:
        if write_lease is not None and graph_write_possible:
            try:
                write_lease.ensure_durable()
            except BaseException as lifecycle_exc:
                logger.error(
                    "kg.dedup.exception_durability_failed board=%s "
                    "operation_error=%s lifecycle_error=%s "
                    "write_may_be_applied=true",
                    board_id,
                    type(operation_exc).__name__,
                    type(lifecycle_exc).__name__,
                    exc_info=True,
                    extra={
                        "event": "kg.dedup.exception_durability_failed",
                        "board_id": board_id,
                        "operation_error_type": type(operation_exc).__name__,
                        "lifecycle_error_type": type(lifecycle_exc).__name__,
                        "write_may_be_applied": True,
                    },
                )
                raise lifecycle_exc from operation_exc
        raise

    if write_lease is not None:
        # This is deliberately inside the writer fence.  A lifecycle failure
        # therefore prevents both a success report and proposal resolution.
        write_lease.ensure_durable()

    if ledger_records:
        # FR6/TR5: every ledger write invalidates the fold cache.
        from okto_pulse.core.kg.equivalence_fold import (
            invalidate_equivalence_fold_cache,
        )

        if write_lease is not None:
            write_lease.ensure_owned(failure_phase="before_cache_invalidation")
        invalidate_equivalence_fold_cache(board_id)
    completed = datetime.now(timezone.utc).isoformat()
    report = {
        "board_id": board_id,
        "dry_run": dry_run,
        "mode": "dry_run" if dry_run else "tombstone",
        "groups": len(groups_summary),
        # Physical removals are ZERO by construction in the reversible
        # default (legacy key preserved for report consumers).
        "total_duplicates_removed": 0,
        "nodes_tombstoned": 0 if dry_run else total_dups,
        "ledger_records_created": ledger_records,
        "duplicates_planned": total_dups,
        "edges_repointed": 0,
        "edges_planned": total_edges if dry_run else 0,
        "started_at": started,
        "executed_at": completed,
        "details": groups_summary,
    }
    logger.info(
        "kg.dedup.completed board=%s dry_run=%s groups=%d dups=%d "
        "edges=%d",
        board_id, dry_run, report["groups"], total_dups, total_edges,
        extra={
            "event": "kg.dedup.completed",
            "board_id": board_id,
            "dry_run": dry_run,
            "groups": report["groups"],
            "duplicates_planned": total_dups,
            "edges_planned": total_edges,
        },
    )
    return report


class StaleProposalError(Exception):
    """Approval refused: the graph state diverged from the proposal (BR5).

    Stable code ``stale_proposal`` — the mutation did NOT happen; re-run
    ``--propose`` to capture a fresh plan.
    """

    code = "stale_proposal"

    def __init__(self, proposal_id: str, expected_hash: str, current_hash: str):
        self.proposal_id = proposal_id
        self.expected_hash = expected_hash
        self.current_hash = current_hash
        super().__init__(
            f"stale_proposal: proposal {proposal_id} hash {expected_hash[:12]} "
            f"!= current plan hash {current_hash[:12]} — nothing was mutated; "
            f"re-run --propose."
        )


def _count_incident_edges(graph_scope, rel_pairs, node_id: str) -> int:
    total = 0
    for rel_name, from_t, to_t in rel_pairs:
        res = graph_scope.execute(
            f"MATCH (a:{from_t})-[r:{rel_name}]->(b:{to_t}) "
            f"WHERE a.id = $id OR b.id = $id RETURN count(r)",
            {"id": node_id},
        )
        if res.rows:
            total += int(res.rows[0][0])
    return total


def _build_canonical_plan(board_id: str) -> dict[str, Any]:
    """Scan-only canonical plan of the dedup (TR7): groups ordered by
    (node_type, source_artifact_ref); member ids sorted; per-member edge
    counts included so ANY relevant state change flips the hash."""

    rel_pairs = _all_rel_pairs()
    plan_groups: list[dict[str, Any]] = []

    async def _scan() -> None:
        async with await get_kg_registry().graph_transaction.begin(board_id) as graph_scope:
            for node_type in NODE_TYPES:
                groups = _fetch_groups(graph_scope, node_type)
                for group in groups:
                    members = group["members"]
                    canonical = members[0]
                    duplicates = sorted(d["id"] for d in members[1:])
                    plan_groups.append({
                        "node_type": node_type,
                        "source_artifact_ref": group["source_artifact_ref"],
                        "survivor_id": canonical["id"],
                        "merged_ids": duplicates,
                        "edge_counts": {
                            dup_id: _count_incident_edges(
                                graph_scope, rel_pairs, dup_id
                            )
                            for dup_id in duplicates
                        },
                    })

    run_async_blocking(_scan())
    plan_groups.sort(
        key=lambda g: (g["node_type"], g["source_artifact_ref"])
    )
    return {"operation": "dedup_entities", "groups": plan_groups}


def _completed_proposal_records(
    board_id: str,
    proposal_id: str,
    plan: dict[str, Any],
) -> tuple[EquivalenceRecord, ...] | None:
    """Recognize a prior attempt that mutated fully but failed before ACK.

    Proposal approval applies graph lifecycle before resolving the relational
    proposal.  A lifecycle/resolve crash can therefore leave the exact proposed
    tombstones in place while the proposal remains pending.  The tagged
    append-only records plus live tombstone markers are the durable proof that a
    retry may finish lifecycle/ACK without treating its own prior write as stale.
    """

    expected_groups = list(plan.get("groups") or ())
    if not expected_groups:
        return None
    ledger = require_equivalence_ledger()

    async def _verify() -> tuple[EquivalenceRecord, ...] | None:
        active = await ledger.active_for_board(board_id)
        tagged = tuple(
            record
            for record in active
            if dict(record.evidence).get("curation_proposal_id") == proposal_id
        )
        if len(tagged) != len(expected_groups):
            return None

        records_by_key: dict[tuple[str, str], EquivalenceRecord] = {}
        for record in tagged:
            evidence = dict(record.evidence)
            source_ref = evidence.get("source_artifact_ref")
            if not isinstance(source_ref, str) or not source_ref:
                return None
            key = (record.node_type, source_ref)
            if key in records_by_key:
                return None
            records_by_key[key] = record

        expected_by_key = {
            (str(group["node_type"]), str(group["source_artifact_ref"])): group
            for group in expected_groups
        }
        if len(expected_by_key) != len(expected_groups):
            return None
        if records_by_key.keys() != expected_by_key.keys():
            return None

        async with await get_kg_registry().graph_transaction.begin(
            board_id
        ) as graph_scope:
            for key, expected in expected_by_key.items():
                record = records_by_key[key]
                if (
                    record.survivor_id != str(expected["survivor_id"])
                    or sorted(record.merged_ids)
                    != sorted(str(value) for value in expected["merged_ids"])
                ):
                    return None
                survivor = graph_scope.execute(
                    f"MATCH (n:{record.node_type}) WHERE n.id = $id "
                    "RETURN n.superseded_by",
                    {"id": record.survivor_id},
                )
                if not survivor.rows or survivor.rows[0][0] is not None:
                    return None
                for member_id in record.merged_ids:
                    state = graph_scope.execute(
                        f"MATCH (n:{record.node_type}) WHERE n.id = $id "
                        "RETURN n.superseded_by, n.revocation_reason",
                        {"id": member_id},
                    )
                    if not state.rows:
                        return None
                    row = state.rows[0]
                    if (
                        row[0] != record.survivor_id
                        or row[1] != f"dedup:{record.record_id}"
                    ):
                        return None
        return tagged

    return run_async_blocking(_verify())


def _already_applied_proposal_report(
    board_id: str,
    plan: dict[str, Any],
    *,
    started: str,
) -> dict[str, Any]:
    duplicates = sum(
        len(group.get("merged_ids") or ())
        for group in list(plan.get("groups") or ())
    )
    return {
        "board_id": board_id,
        "dry_run": False,
        "mode": "tombstone",
        "groups": 0,
        "total_duplicates_removed": 0,
        "nodes_tombstoned": 0,
        "ledger_records_created": 0,
        "duplicates_planned": duplicates,
        "edges_repointed": 0,
        "edges_planned": 0,
        "started_at": started,
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "details": [],
        "already_applied": True,
    }


def compute_proposal_hash(plan: dict[str, Any]) -> str:
    """sha256 of the canonically serialized plan (deterministic — TR7)."""

    import hashlib
    import json

    payload = json.dumps(plan, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def propose_dedup_entities(
    board_id: str, *, created_by: str = "cli:kg-dedup"
) -> dict[str, Any]:
    """FR7 (--propose): persist the canonical plan + hash. ZERO mutation —
    graph and equivalence ledger stay untouched."""

    plan = _build_canonical_plan(board_id)
    proposal_hash = compute_proposal_hash(plan)
    proposal = CurationProposal(
        proposal_id=f"prop_{uuid.uuid4().hex[:16]}",
        board_id=board_id,
        operation="dedup_entities",
        plan=plan,
        proposal_hash=proposal_hash,
        created_by=created_by,
    )
    store = require_curation_proposal_store()
    run_async_blocking(store.append(proposal))
    logger.info(
        "kg.curation.proposed proposal=%s board=%s groups=%d hash=%s",
        proposal.proposal_id, board_id, len(plan["groups"]), proposal_hash,
        extra={
            "event": "kg.curation.proposed",
            "proposal_id": proposal.proposal_id,
            "board_id": board_id,
            "groups": len(plan["groups"]),
            "proposal_hash": proposal_hash,
        },
    )
    return {
        "board_id": board_id,
        "proposal_id": proposal.proposal_id,
        "proposal_hash": proposal_hash,
        "groups": len(plan["groups"]),
        "duplicates_planned": sum(
            len(g["merged_ids"]) for g in plan["groups"]
        ),
        "plan": plan,
    }


def approve_dedup_proposal(
    board_id: str, proposal_id: str, *, created_by: str = "cli:kg-dedup"
) -> dict[str, Any]:
    """FR7/BR5 (--approve): recompute the plan and compare hashes BEFORE
    any write. Equal → execute the reversible tombstone path and mark the
    proposal resolved; different → ``StaleProposalError`` with the graph
    intact."""

    from okto_pulse.core.ports.kg_curation_proposals import (
        CurationProposalError,
    )

    def _validate_pending(proposal: CurationProposal | None) -> CurationProposal:
        if proposal is None:
            raise CurationProposalError(
                "curation_proposal_not_found", proposal_id=proposal_id
            )
        if proposal.board_id != board_id:
            raise CurationProposalError(
                "curation_proposal_board_mismatch",
                proposal_id=proposal_id,
                remediation=f"Proposal belongs to board {proposal.board_id}.",
            )
        if proposal.status != "pending":
            raise CurationProposalError(
                "curation_proposal_already_resolved",
                proposal_id=proposal_id,
                remediation=f"Proposal status is {proposal.status!r}.",
            )
        return proposal

    store = require_curation_proposal_store()
    _validate_pending(run_async_blocking(store.get(proposal_id)))
    require_curation_allowed(_DEDUP_WRITE_OPERATION, confirmed=True)

    # Recompute the proposal proof, mutate, apply lifecycle and resolve the
    # relational proposal ACK under ONE board lease.  This closes the race
    # where the old code released its graph transaction before resolve().
    with guarded_board_write(
        board_id,
        operation=_DEDUP_WRITE_OPERATION,
        owner_id=created_by,
        mutation_ref=f"kg-dedup-proposal:{proposal_id}",
    ) as write_lease:
        proposal = _validate_pending(run_async_blocking(store.get(proposal_id)))
        current_plan = _build_canonical_plan(board_id)
        current_hash = compute_proposal_hash(current_plan)
        proposal_plan = dict(proposal.plan)
        if current_hash != proposal.proposal_hash:
            completed_records = _completed_proposal_records(
                board_id,
                proposal_id,
                proposal_plan,
            )
            if completed_records is None:
                raise StaleProposalError(
                    proposal_id, proposal.proposal_hash, current_hash
                )
            # The previous attempt reached every tagged tombstone but failed
            # before proposal ACK. Retry the durability barrier and cache
            # finalization; never scan/mutate unrelated new groups.
            started = datetime.now(timezone.utc).isoformat()
            write_lease.ensure_durable()
            from okto_pulse.core.kg.equivalence_fold import (
                invalidate_equivalence_fold_cache,
            )

            write_lease.ensure_owned(
                failure_phase="before_cache_invalidation"
            )
            invalidate_equivalence_fold_cache(board_id)
            report = _already_applied_proposal_report(
                board_id,
                proposal_plan,
                started=started,
            )
        else:
            # Hash equality proves the current groups ARE the proposed plan —
            # executing the reversible default acts on exactly those groups.
            report = _run_dedup_migration(
                board_id,
                dry_run=False,
                created_by=created_by,
                started=datetime.now(timezone.utc).isoformat(),
                write_lease=write_lease,
                proposal_id=proposal_id,
            )
        # ACK only after successful graph durability, while ownership is still
        # held.  Any lifecycle/resolve/release failure prevents a success return.
        write_lease.ensure_owned(failure_phase="before_proposal_resolve")
        run_async_blocking(store.resolve(proposal_id, "resolved"))
        report["proposal_id"] = proposal_id
        report["proposal_status"] = "resolved"

    logger.info(
        "kg.curation.approved proposal=%s board=%s",
        proposal_id, board_id,
        extra={
            "event": "kg.curation.approved",
            "proposal_id": proposal_id,
            "board_id": board_id,
        },
    )
    return report


def unmerge_equivalence(board_id: str, record_id: str) -> dict[str, Any]:
    """Un-merge (spec MKG-C-S1 FR4/BR3): logically reverse a dedup merge.

    De-tombstones the members THIS record tombstoned (guarded by the
    traceable ``revocation_reason='dedup:<record_id>'`` so a member later
    superseded by something else is never touched), then revokes the
    ledger record. NEVER re-points edges (bulk in-place is forbidden —
    ADR 0007/KGD-01); the write order (graph first, revoke second) makes
    a partial failure convergent: re-running the un-merge repeats the
    idempotent de-tombstone and completes the revoke.

    An already-revoked record is an idempotent no-op with a warning.
    """

    from okto_pulse.core.ports.kg_equivalence_ledger import (
        EquivalenceLedgerError,
    )

    # The explicit record_id IS the confirmation artifact (same pattern as
    # the DLQ list->reprocess(id) pair) — the policy still gates the
    # operation class.
    require_curation_allowed("kg_unmerge", confirmed=True)

    result: dict[str, Any] = {
        "board_id": board_id,
        "record_id": record_id,
        "already_revoked": False,
        "members_restored": 0,
        "revoked": False,
    }

    def _require_record(record: EquivalenceRecord | None) -> EquivalenceRecord:
        if record is None:
            raise EquivalenceLedgerError(
                "equivalence_record_not_found", record_id=record_id
            )
        if record.board_id != board_id:
            raise EquivalenceLedgerError(
                "equivalence_record_board_mismatch",
                board_id=board_id,
                record_id=record_id,
                remediation=f"Record belongs to board {record.board_id}.",
            )
        return record

    ledger = require_equivalence_ledger()
    record = _require_record(run_async_blocking(ledger.get(record_id)))
    result["survivor_id"] = record.survivor_id
    if not record.is_active:
        logger.warning(
            "kg.equivalence.unmerge_noop record=%s already revoked at %s",
            record_id, record.revoked_at,
        )
        result["already_revoked"] = True
        return result

    graph_write_possible = False

    with guarded_board_write(
        board_id,
        operation=_UNMERGE_WRITE_OPERATION,
        owner_id="cli:kg-unmerge",
        mutation_ref=f"kg-unmerge:{record_id}",
    ) as write_lease:
        # Re-read after acquisition so a contender that completed immediately
        # before us cannot make this call apply the same finalization twice.
        record = _require_record(run_async_blocking(ledger.get(record_id)))
        result["survivor_id"] = record.survivor_id
        if not record.is_active:
            logger.warning(
                "kg.equivalence.unmerge_noop record=%s already revoked at %s",
                record_id, record.revoked_at,
            )
            result["already_revoked"] = True
            # The guard requires a closed lifecycle on normal exit.  This path
            # is only reachable for the acquisition race; it mutates no graph.
            write_lease.ensure_durable()
            return result

        restored = 0

        async def _restore_members() -> None:
            nonlocal restored, graph_write_possible
            write_lease.ensure_owned(failure_phase="before_graph_transaction")
            async with await get_kg_registry().graph_transaction.begin(
                board_id
            ) as raw_graph_scope:
                graph_scope = _LeaseFencedGraphScope(
                    raw_graph_scope,
                    write_lease,
                )
                for member_id in record.merged_ids:
                    # Mark before the call because an auto-commit adapter can
                    # persist the SET and then raise/cancel.
                    graph_write_possible = True
                    graph_scope.execute(
                        f"MATCH (n:{record.node_type}) WHERE n.id = $id "
                        f"AND n.revocation_reason = $reason "
                        f"SET n.superseded_by = NULL, n.superseded_at = NULL, "
                        f"n.revocation_reason = NULL",
                        {"id": member_id, "reason": f"dedup:{record_id}"},
                    )
                    restored += 1
                write_lease.ensure_owned(
                    failure_phase="before_graph_scope_exit"
                )

        try:
            run_async_blocking(_restore_members())
        except BaseException as operation_exc:
            if graph_write_possible:
                try:
                    write_lease.ensure_durable()
                except BaseException as lifecycle_exc:
                    logger.error(
                        "kg.equivalence.unmerge_exception_durability_failed "
                        "record=%s board=%s operation_error=%s "
                        "lifecycle_error=%s write_may_be_applied=true",
                        record_id,
                        board_id,
                        type(operation_exc).__name__,
                        type(lifecycle_exc).__name__,
                        exc_info=True,
                        extra={
                            "event": (
                                "kg.equivalence."
                                "unmerge_exception_durability_failed"
                            ),
                            "record_id": record_id,
                            "board_id": board_id,
                            "operation_error_type": type(
                                operation_exc
                            ).__name__,
                            "lifecycle_error_type": type(
                                lifecycle_exc
                            ).__name__,
                            "write_may_be_applied": True,
                        },
                    )
                    raise lifecycle_exc from operation_exc
            raise

        # Graph durability precedes the relational ledger ACK.  The lease stays
        # held through revoke + cache finalization and guard release.
        write_lease.ensure_durable()
        write_lease.ensure_owned(failure_phase="before_equivalence_revoke")
        run_async_blocking(ledger.revoke(record_id, "unmerge"))
        # FR6/TR5: revoke is a ledger write — desagrupa imediatamente.
        from okto_pulse.core.kg.equivalence_fold import (
            invalidate_equivalence_fold_cache,
        )

        write_lease.ensure_owned(failure_phase="before_cache_invalidation")
        invalidate_equivalence_fold_cache(board_id)
        result["members_restored"] = restored
        result["revoked"] = True
        logger.info(
            "kg.equivalence.unmerged record=%s board=%s members=%d",
            record_id, board_id, restored,
            extra={
                "event": "kg.equivalence.unmerged",
                "record_id": record_id,
                "board_id": board_id,
                "members_restored": restored,
            },
        )

    return result


def format_report_table(report: dict[str, Any]) -> str:
    """Render a human-readable table from the structured report.

    Fixed-width columns sized for ops triage in a terminal at 120 cols.
    Truncates source_artifact_ref and canonical_id to keep rows on one
    line; the JSON output (--json flag) carries the full IDs.
    """
    header = (
        f"{'node_type':<14} | {'source_artifact_ref':<40} | "
        f"{'duplicates_found':>16} | {'canonical_id':<32} | "
        f"{'edges_repointed':>15}"
    )
    separator = "-" * len(header)
    lines = [
        f"KG dedup migration — board {report['board_id']} "
        f"{'(DRY-RUN)' if report['dry_run'] else '(APPLIED)'}",
        header,
        separator,
    ]
    for entry in report["details"]:
        ref = entry["source_artifact_ref"]
        if len(ref) > 40:
            ref = ref[:37] + "..."
        canonical = entry["canonical_id"]
        if len(canonical) > 32:
            canonical = canonical[:29] + "..."
        lines.append(
            f"{entry['node_type']:<14} | {ref:<40} | "
            f"{entry['duplicates_found']:>16} | {canonical:<32} | "
            f"{entry['edges_repointed']:>15}"
        )
    lines.append(separator)
    summary_label = "Planned" if report["dry_run"] else "Applied"
    lines.append(
        f"{summary_label}: groups={report['groups']} "
        f"duplicates={report.get('duplicates_planned', report.get('total_duplicates_removed', 0))} "
        f"edges={report.get('edges_planned', report.get('edges_repointed', 0))}"
    )
    return "\n".join(lines)
