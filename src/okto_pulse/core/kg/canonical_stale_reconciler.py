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
  ``original_node_id`` / ``source_artifact_ref`` (only ``graph_layer`` /
  ``maturity_status`` mutate).
- TR7: idempotent / convergent — only nodes currently at ``graph_layer=canonical``
  are acted on, so a second run over the same stale state is a NOOP (already
  demoted / debt already keyed). Fast-path (``source_refs``) and full sweep
  (``source_refs=None``) converge to the same final state (AC8).
- TR8 / FR8 / AC9: cognitive/canonical-origin nodes (incl. bug-derived Learning)
  are excluded from demotion-to-working.

Global Discovery digest convergence (FR7) is left as a HOOK for R2-IMP5; this
card does not touch the R1 parity primitive directly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any


from okto_pulse.core.kg.canonical_learning_partition import (
    _bug_artifact_id,
    _is_bug_derived_ref,
)
from okto_pulse.core.kg.connectivity_guard import WriterClass, classify_writer_path
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_WORKING_STALE,
    SourceMaturityClassification,
    classify_source_for_kg,
)

logger = logging.getLogger("okto_pulse.kg.canonical_stale_reconciler")

# Cognitive-origin node types: never demoted to working by R2 (carve-out).
COGNITIVE_NODE_TYPES: frozenset[str] = frozenset({"Learning", "Alternative", "Assumption"})

# All board-graph node types to scan for stale canonical publication.
ALL_NODE_TYPES: tuple[str, ...] = (
    "Decision", "Requirement", "Criterion", "Constraint", "APIContract",
    "TestScenario", "Bug", "Entity", "Learning", "Alternative", "Assumption",
)

# source_artifact_refs that are NOT SDLC sources (infra roots) — never demoted.
_SKIP_REFS: frozenset[str] = frozenset({"tech_entities.yml"})
_SKIP_REF_PREFIXES: tuple[str, ...] = ("board:",)

# DTO-only result action labels (NEVER persisted enums).
ACTION_DEMOTED = "demoted_to_working"
ACTION_SKIPPED_COGNITIVE = "skipped_cognitive"
ACTION_ROUTED_DEBT = "routed_to_canonical_debt"


@dataclass
class StaleReconcileResult:
    board_id: str
    correlation_id: str
    scanned: int = 0
    demoted: list[dict[str, Any]] = field(default_factory=list)
    skipped_cognitive: list[dict[str, Any]] = field(default_factory=list)
    routed_to_debt: list[dict[str, Any]] = field(default_factory=list)
    global_sync_enqueued: bool = False

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
        }


def _owning_source_id(ref: str) -> str | None:
    """Extract the owning artifact id from a deterministic node's
    ``source_artifact_ref`` (``spec:{id}``, ``spec:{id}:fr:..``, ``card:{id}`` ...).
    Returns None for infra roots (board:/tech_entities) or malformed refs."""
    if not ref or ref in _SKIP_REFS:
        return None
    if any(ref.startswith(p) for p in _SKIP_REF_PREFIXES):
        return None
    parts = ref.split(":")
    if len(parts) < 2 or not parts[1]:
        return None
    return parts[1]


def _source_ids_from_refs(source_refs: list[str] | None) -> set[str] | None:
    if not source_refs:
        return None
    ids: set[str] = set()
    for ref in source_refs:
        sid = _owning_source_id(str(ref))
        if sid:
            ids.add(sid)
    return ids


def _build_source_classification_map(board_id: str) -> dict[str, SourceMaturityClassification]:
    """``{artifact_id: classification}`` from the registered source reader."""
    reader = get_kg_registry().require_board_source_reader()
    out: dict[str, SourceMaturityClassification] = {}
    for row in reader.fetch(board_id):
        sid = str(row.get("id") or "")
        if not sid:
            continue
        out[sid] = classify_source_for_kg(
            artifact_type=row.get("artifact_type"),
            artifact_status=row.get("source_artifact_status") or row.get("status"),
            content_hash=row.get("content_hash"),
            updated_at=row.get("updated_at"),
            has_minimal_evidence=bool(row.get("has_minimal_evidence", True)),
        )
    return out


def _is_cognitive(node_type: str, created_by_agent: str) -> bool:
    if node_type in COGNITIVE_NODE_TYPES:
        return True
    return classify_writer_path(str(created_by_agent or "")) == WriterClass.COGNITIVE


async def _scan_and_demote(
    board_id: str,
    source_by_id: dict[str, SourceMaturityClassification],
    target_ids: set[str] | None,
    correlation_id: str,
    result: StaleReconcileResult,
) -> list[dict[str, Any]]:
    """Sync pass: demote stale deterministic canonical nodes in place; collect
    cognitive bug-derived material cases as debt-routing intents (handled async by
    the caller). Returns the list of debt intents."""
    cognitive_debt_intents: list[dict[str, Any]] = []
    transaction = get_kg_registry().graph_transaction
    async with await transaction.begin(board_id) as scope:
        for ntype in ALL_NODE_TYPES:
            try:
                res = scope.execute(
                    f"MATCH (n:{ntype}) WHERE n.graph_layer = $c "
                    f"RETURN n.id, n.source_artifact_ref, n.created_by_agent, "
                    f"n.maturity_status",
                    {"c": GRAPH_LAYER_CANONICAL},
                )
            except Exception as exc:  # pragma: no cover - defensive per-type guard
                logger.warning(
                    "kg.stale.scan_type_failed board=%s type=%s err=%s",
                    board_id, ntype, exc,
                )
                continue
            rows: list[tuple[str, str, str, str]] = []
            while res.has_next():
                row = res.get_next()
                rows.append((
                    str(row[0]),
                    str(row[1] or ""),
                    str(row[2] or ""),
                    str(row[3] or ""),
                ))
            for node_id, ref, writer, _cur_maturity in rows:
                result.scanned += 1
                if _is_cognitive(ntype, writer):
                    intent = _classify_cognitive(
                        board_id, ntype, node_id, ref, target_ids, source_by_id,
                        correlation_id,
                    )
                    if intent is None:
                        continue  # out of fast-path scope
                    if intent.get("_route"):
                        cognitive_debt_intents.append(intent)
                    else:
                        result.skipped_cognitive.append(intent)
                        logger.info(
                            "kg.stale.skipped_cognitive board=%s node=%s type=%s",
                            board_id, node_id, ntype,
                            extra={
                                "event": "kg.stale.skipped_cognitive",
                                "board_id": board_id, "node_id": node_id,
                                "node_type": ntype, "correlation_id": correlation_id,
                            },
                        )
                    continue

                # Deterministic node: resolve owning source + check staleness.
                src_id = _owning_source_id(ref)
                if src_id is None:
                    continue  # infra root / unknown — not source-derived
                if target_ids is not None and src_id not in target_ids:
                    continue  # fast-path scope
                cls = source_by_id.get(src_id)
                if cls is not None and cls.graph_layer == GRAPH_LAYER_CANONICAL:
                    continue  # source still canonical-eligible — not stale
                new_maturity = cls.maturity_status if cls else MATURITY_WORKING_STALE
                reason = cls.reason_code if cls else "source_absent"
                src_status = cls.artifact_status if cls else ""
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
                    "owning_source_id": src_id,
                    "prev_layer": GRAPH_LAYER_CANONICAL,
                    "expected_layer": GRAPH_LAYER_WORKING,
                    "maturity_status": new_maturity,
                    "source_status": src_status,
                    "reason_code": reason,
                    "correlation_id": correlation_id,
                    "action": ACTION_DEMOTED,
                }
                result.demoted.append(record)
                logger.info(
                    "kg.stale.demoted_to_working board=%s node=%s type=%s "
                    "reason=%s corr=%s",
                    board_id, node_id, ntype, reason, correlation_id,
                    extra={"event": "kg.stale.demoted_to_working", **record},
                )
    return cognitive_debt_intents


def _classify_cognitive(
    board_id: str,
    ntype: str,
    node_id: str,
    ref: str,
    target_ids: set[str] | None,
    source_by_id: dict[str, SourceMaturityClassification],
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
        if target_ids is not None:
            # Non-bug-derived cognitive: only report inside fast-path scope when
            # its owning id (if resolvable) is targeted; otherwise still a skip.
            sid = _owning_source_id(ref)
            if sid is not None and sid not in target_ids:
                return None
        return base
    bug_id = _bug_artifact_id(ref)
    if target_ids is not None and bug_id not in target_ids:
        return None  # fast-path scope
    bug_cls = source_by_id.get(bug_id)
    regressed = bug_cls is not None and bug_cls.graph_layer != GRAPH_LAYER_CANONICAL
    if not regressed:
        return base  # bug evidence still canonical (or unknown) -> preserved skip
    return {
        **base,
        "action": ACTION_ROUTED_DEBT,
        "_route": True,
        "bug_id": bug_id,
        "source_status": bug_cls.artifact_status if bug_cls else "",
        "reason_code": bug_cls.reason_code if bug_cls else "bug_source_regressed",
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
        PARTITION_TARGET_STATUS,
    )
    from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

    bug_id = str(intent.get("bug_id") or "")
    await upsert_canonical_debt(
        db,
        board_id=board_id,
        artifact_type="bug",
        artifact_id=bug_id[:36],
        source_ref=str(intent.get("source_artifact_ref") or ""),
        content_hash=f"r2stale:{bug_id}",
        target_status=PARTITION_TARGET_STATUS,
        canonical_state="pending",
        failure_reason=HISTORICAL_DEBT_REASON,
        correlation_id=correlation_id,
    )


async def reconcile_stale_canonical(
    db: object,
    *,
    board_id: str,
    source_refs: list[str] | None = None,
    correlation_id: str | None = None,
) -> StaleReconcileResult:
    """Reconcile stale canonical publication for a board.

    ``source_refs`` scopes the run to specific artifacts (event fast-path, FR2);
    ``None`` is a full sweep (FR3). Both converge to the same final state and are
    idempotent (TR7/AC8). Deterministic stale canonical nodes are demoted to
    working in place; cognitive nodes are preserved (carve-out) and a material
    bug-derived Learning irregularity is routed to R7 CanonicalDebt.
    """
    import uuid as _uuid

    corr = correlation_id or _uuid.uuid4().hex
    result = StaleReconcileResult(board_id=board_id, correlation_id=corr)
    source_by_id = _build_source_classification_map(board_id)
    target_ids = _source_ids_from_refs(source_refs)

    debt_intents = await _scan_and_demote(
        board_id, source_by_id, target_ids, corr, result
    )
    for intent in debt_intents:
        try:
            await _route_cognitive_to_debt(db, board_id, intent, corr)
            result.routed_to_debt.append(intent)
            logger.info(
                "kg.stale.routed_to_canonical_debt board=%s node=%s bug=%s corr=%s",
                board_id, intent.get("node_id"), intent.get("bug_id"), corr,
                extra={"event": "kg.stale.routed_to_canonical_debt",
                       "board_id": board_id, "correlation_id": corr,
                       "node_id": intent.get("node_id"),
                       "bug_id": intent.get("bug_id")},
            )
        except Exception as exc:  # pragma: no cover - debt routing best-effort
            logger.warning(
                "kg.stale.debt_routing_failed board=%s node=%s err=%s",
                board_id, intent.get("node_id"), exc,
            )

    # R2-IMP5: after a deterministic demotion, sync Global Discovery via the R1
    # parity contract so query_global(canonical) stops returning the obsolete
    # canonical digest (all keeps it as working/diagnostic). Reuses the R1
    # reconciler (enqueue -> GD worker recomputes expected_digest_layer); additive
    # + best-effort (a sync-enqueue failure must NOT undo the board-graph demotion).
    if result.demoted:
        try:
            from okto_pulse.core.kg.canonical_demotion_global_sync import (
                sync_stale_demotion_to_global_discovery,
            )

            await sync_stale_demotion_to_global_discovery(db, board_id=board_id)
            result.global_sync_enqueued = True
        except Exception as exc:  # pragma: no cover - sync enqueue best-effort
            logger.warning(
                "kg.stale.global_sync_enqueue_failed board=%s err=%s",
                board_id, exc,
            )
    return result


__all__ = [
    "ACTION_DEMOTED",
    "ACTION_ROUTED_DEBT",
    "ACTION_SKIPPED_COGNITIVE",
    "COGNITIVE_NODE_TYPES",
    "StaleReconcileResult",
    "reconcile_stale_canonical",
]
