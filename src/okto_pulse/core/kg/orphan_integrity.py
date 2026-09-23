"""Orphan-node integrity scanner and report models for the KG.

KG-ZO-01 prevents new loose nodes at write time. This module is the corrective
read side for historical graph debt: it scans persisted graph nodes and reports
non-allowlisted zero-degree nodes without leaking user-authored payloads.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from contextvars import copy_context
from dataclasses import dataclass, field
from typing import Any, Iterable, Protocol

from okto_pulse.core.kg.board_rebuild_adapter import (
    CARD_SOURCE_ARTIFACT_TYPES,
    DETERMINISTIC_SOURCE_ARTIFACT_TYPES,
)
from okto_pulse.core.kg.connectivity_guard import (
    KGConnectivityRuleRegistry,
    SourceResolutionStatus,
)
from okto_pulse.core.kg.schema_contract import MULTI_REL_TYPES, NODE_TYPES, REL_TYPES
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult

logger = logging.getLogger("okto_pulse.kg.orphan_integrity")

MAX_ORPHAN_SAMPLE_LIMIT = 100
DEFAULT_ORPHAN_SAMPLE_LIMIT = 25
ZERO_DEGREE_REASON = "zero_graph_degree"
ALLOWLISTED_TECHNICAL_ROOT_REASON = "allowlisted_technical_root"
ZERO_ORPHAN_VALIDATION_PASSED = "passed"
ZERO_ORPHAN_VALIDATION_PENDING_BACKFILL = "pending_backfill"
ZERO_ORPHAN_VALIDATION_FAILED = "failed_orphan_validation"
ZERO_ORPHAN_VALIDATION_UNAVAILABLE = "unavailable"
ZERO_ORPHAN_VALIDATION_NOT_EVALUATED = "not_evaluated"
SOURCE_REF_MISSING = "missing_source_artifact_ref"
SOURCE_REF_RESOLVED = "resolved_source_artifact_ref"
SOURCE_REF_UNRESOLVED = SourceResolutionStatus.UNRESOLVED_SOURCE_REF.value
_RELATIONAL_SOURCE_LOOKUP_BATCH_SIZE = 400

_WRITE_KEYWORDS = (
    " CREATE ",
    " DELETE ",
    " DETACH DELETE ",
    " SET ",
    " MERGE ",
    " DROP ",
    " ALTER ",
)


SAFE_ORPHAN_SAMPLE_FIELDS: tuple[str, ...] = (
    "node_id",
    "node_type",
    "writer_path",
    "source_artifact_ref",
    "source_resolution_status",
    "generation_id",
    "reason",
    "correlation_id",
)

SAFE_ORPHAN_METRIC_LABELS: tuple[str, ...] = (
    "board_id",
    "node_type",
    "writer_path",
    "outcome",
    "reason",
    "source_resolution_status",
    "generation_id",
)

SAFE_ORPHAN_AUDIT_FIELDS: tuple[str, ...] = (
    "event_name",
    "board_id",
    "node_id",
    "node_type",
    "writer_path",
    "outcome",
    "reason",
    "source_resolution_status",
    "generation_id",
    "correlation_id",
    "sample_count",
)


@dataclass(frozen=True)
class OrphanMetricEvent:
    metric_name: str
    board_id: str
    node_type: str
    writer_path: str
    outcome: str
    reason: str
    source_resolution_status: str
    generation_id: str
    count: int = 1

    def labels(self) -> dict[str, str]:
        return {
            "board_id": self.board_id,
            "node_type": self.node_type,
            "writer_path": self.writer_path,
            "outcome": self.outcome,
            "reason": self.reason,
            "source_resolution_status": self.source_resolution_status,
            "generation_id": self.generation_id,
        }


@dataclass(frozen=True)
class OrphanAuditRecord:
    event_name: str
    board_id: str
    node_id: str | None
    node_type: str
    writer_path: str
    outcome: str
    reason: str
    source_resolution_status: str
    generation_id: str
    correlation_id: str
    sample_count: int = 1

    def to_safe_dict(self) -> dict[str, str | int | None]:
        return {
            "event_name": self.event_name,
            "board_id": self.board_id,
            "node_id": self.node_id,
            "node_type": self.node_type,
            "writer_path": self.writer_path,
            "outcome": self.outcome,
            "reason": self.reason,
            "source_resolution_status": self.source_resolution_status,
            "generation_id": self.generation_id,
            "correlation_id": self.correlation_id,
            "sample_count": self.sample_count,
        }


class OrphanMetricSinkProtocol(Protocol):
    def emit(self, event: OrphanMetricEvent) -> None:
        """Record one safe orphan-integrity metric event."""


class OrphanAuditSinkProtocol(Protocol):
    def emit(self, record: OrphanAuditRecord) -> None:
        """Record one safe orphan-integrity audit record."""


class OrphanSourceResolverProtocol(Protocol):
    def resolve_many(
        self,
        *,
        board_id: str,
        source_artifact_refs: tuple[str, ...],
    ) -> dict[str, str]:
        """Resolve source refs against their owning board's relational data."""


class RelationalOrphanSourceResolver:
    """Board-scoped source resolver backed by the edition persistence ports."""

    def resolve_many(
        self,
        *,
        board_id: str,
        source_artifact_refs: tuple[str, ...],
    ) -> dict[str, str]:
        refs = tuple(dict.fromkeys(source_artifact_refs))
        statuses = {ref: SOURCE_REF_UNRESOLVED for ref in refs}
        identities = tuple(
            identity
            for ref in refs
            if (identity := _parse_relational_source_ref(ref)) is not None
        )
        if not identities:
            return statuses
        try:
            resolved_refs = _run_async_blocking(
                _resolve_relational_source_identities(
                    board_id=board_id,
                    identities=identities,
                )
            )
        except Exception:
            # Resolution failure must never hide a zero-degree node. Keep every
            # affected ref unresolved and preserve the structural orphan signal.
            logger.warning(
                "kg.orphan.source_resolution_unavailable board=%s",
                board_id,
                exc_info=True,
            )
            return statuses
        for ref in resolved_refs:
            statuses[ref] = SOURCE_REF_RESOLVED
        return statuses


@dataclass
class InMemoryOrphanMetricSink:
    events: list[OrphanMetricEvent] = field(default_factory=list)

    def emit(self, event: OrphanMetricEvent) -> None:
        self.events.append(event)


@dataclass
class InMemoryOrphanAuditSink:
    records: list[OrphanAuditRecord] = field(default_factory=list)

    def emit(self, record: OrphanAuditRecord) -> None:
        self.records.append(record)


def _run_async_blocking(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: dict[str, Any] = {}

    def _runner() -> None:
        try:
            box["result"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - re-raised below
            box["error"] = exc

    context = copy_context()
    thread = threading.Thread(
        target=context.run,
        args=(_runner,),
        name="kg-orphan-integrity-graph-write",
        daemon=True,
    )
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box.get("result")


class _BoardGraphPortConnection:
    """Minimal connection-shaped adapter over the KG graph ports.

    The scanner/backfill code predates the hexagonal ports and expects a
    ``graph_scope.execute(...)`` object. This adapter preserves that local API without
    opening backend-specific graph connections from core.
    """

    def __init__(self, board_id: str) -> None:
        self._board_id = board_id

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> Any:
        from okto_pulse.core.kg.interfaces import get_kg_registry

        normalized = f" {cypher.upper()} "
        is_write = any(keyword in normalized for keyword in _WRITE_KEYWORDS)
        if not is_write:
            result = get_kg_registry().cypher_executor.execute_read_only(
                self._board_id,
                cypher,
                params or {},
                max_rows=10000,
            )
            if result.get("truncated"):
                raise RuntimeError("orphan integrity graph read was truncated")
            return GraphStatementResult.from_rows(result.get("rows", []))

        async def _run() -> Any:
            async with await get_kg_registry().graph_transaction.begin(
                self._board_id
            ) as scope:
                return scope.execute(cypher, params or {})

        return _run_async_blocking(_run())


class LoggingOrphanAuditSink:
    """Best-effort structured logger sink using only safe audit fields."""

    def emit(self, record: OrphanAuditRecord) -> None:
        logger.info(
            "kg.orphan.audit event=%s board=%s outcome=%s reason=%s",
            record.event_name,
            record.board_id,
            record.outcome,
            record.reason,
            extra={"event": record.event_name, "audit": record.to_safe_dict()},
        )


@dataclass(frozen=True)
class OrphanNodeSample:
    node_id: str
    node_type: str
    writer_path: str
    source_artifact_ref: str | None
    source_resolution_status: str
    generation_id: str | None
    reason: str
    correlation_id: str

    def to_safe_dict(self) -> dict[str, str | None]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "writer_path": self.writer_path,
            "source_artifact_ref": self.source_artifact_ref,
            "source_resolution_status": self.source_resolution_status,
            "generation_id": self.generation_id,
            "reason": self.reason,
            "correlation_id": self.correlation_id,
        }


@dataclass(frozen=True)
class OrphanScanReport:
    board_id: str
    generation_id: str | None
    orphan_count: int
    orphan_count_by_type: dict[str, int]
    orphan_count_by_writer_path: dict[str, int]
    samples: tuple[OrphanNodeSample, ...]
    unresolved_reasons: dict[str, int]
    allowlisted_root_count: int
    correlation_id: str

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "board_id": self.board_id,
            "generation_id": self.generation_id,
            "orphan_count": self.orphan_count,
            "orphan_count_by_type": dict(self.orphan_count_by_type),
            "orphan_count_by_writer_path": dict(self.orphan_count_by_writer_path),
            "samples": [sample.to_safe_dict() for sample in self.samples],
            "unresolved_reasons": dict(self.unresolved_reasons),
            "allowlisted_root_count": self.allowlisted_root_count,
            "correlation_id": self.correlation_id,
        }


@dataclass(frozen=True)
class OrphanIntegrityProjection:
    classification_delta: str
    integrity_warning: bool
    orphan_count: int
    orphan_count_by_type: dict[str, int]
    samples: tuple[OrphanNodeSample, ...]
    unresolved_reasons: dict[str, int]
    allowlisted_root_count: int
    generation_id: str | None
    correlation_id: str | None
    zero_orphan_validation: str
    reason: str

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "classification_delta": self.classification_delta,
            "integrity_warning": self.integrity_warning,
            "orphan_count": self.orphan_count,
            "orphan_count_by_type": dict(self.orphan_count_by_type),
            "samples": [sample.to_safe_dict() for sample in self.samples],
            "unresolved_reasons": dict(self.unresolved_reasons),
            "allowlisted_root_count": self.allowlisted_root_count,
            "generation_id": self.generation_id,
            "correlation_id": self.correlation_id,
            "zero_orphan_validation": self.zero_orphan_validation,
            "reason": self.reason,
        }


def build_orphan_integrity_projection(
    report: OrphanScanReport | None,
    *,
    scan_error: str | None = None,
    not_evaluated_reason: str | None = None,
) -> OrphanIntegrityProjection:
    """Project orphan scanner output into KG Health/rebuild status fields."""

    if report is None:
        if scan_error:
            return OrphanIntegrityProjection(
                classification_delta="none",
                integrity_warning=False,
                orphan_count=0,
                orphan_count_by_type={},
                samples=(),
                unresolved_reasons={"orphan_scan_unavailable": 1},
                allowlisted_root_count=0,
                generation_id=None,
                correlation_id=None,
                zero_orphan_validation=ZERO_ORPHAN_VALIDATION_UNAVAILABLE,
                reason="orphan_scan_unavailable",
            )
        return OrphanIntegrityProjection(
            classification_delta="none",
            integrity_warning=False,
            orphan_count=0,
            orphan_count_by_type={},
            samples=(),
            unresolved_reasons={},
            allowlisted_root_count=0,
            generation_id=None,
            correlation_id=None,
            zero_orphan_validation=ZERO_ORPHAN_VALIDATION_NOT_EVALUATED,
            reason=not_evaluated_reason or "orphan_scan_not_evaluated",
        )

    if report.orphan_count > 0:
        return OrphanIntegrityProjection(
            classification_delta="at_risk",
            integrity_warning=True,
            orphan_count=report.orphan_count,
            orphan_count_by_type=dict(report.orphan_count_by_type),
            samples=report.samples,
            unresolved_reasons=dict(report.unresolved_reasons),
            allowlisted_root_count=report.allowlisted_root_count,
            generation_id=report.generation_id,
            correlation_id=report.correlation_id,
            zero_orphan_validation=ZERO_ORPHAN_VALIDATION_PENDING_BACKFILL,
            reason="orphan_count_gt_zero",
        )

    return OrphanIntegrityProjection(
        classification_delta="none",
        integrity_warning=False,
        orphan_count=0,
        orphan_count_by_type={},
        samples=(),
        unresolved_reasons=dict(report.unresolved_reasons),
        allowlisted_root_count=report.allowlisted_root_count,
        generation_id=report.generation_id,
        correlation_id=report.correlation_id,
        zero_orphan_validation=ZERO_ORPHAN_VALIDATION_PASSED,
        reason="zero_non_allowlisted_orphans",
    )


@dataclass(frozen=True)
class _NodeRow:
    node_id: str
    node_type: str
    source_artifact_ref: str | None
    writer_path: str


class OrphanScanQueryError(RuntimeError):
    """Fail-closed signal for an incomplete orphan-integrity graph read."""

    def __init__(self, *, phase: str, node_type: str) -> None:
        self.phase = phase
        self.node_type = node_type
        super().__init__(
            f"orphan integrity {phase} query failed for node type {node_type}"
        )


class OrphanNodeScanner:
    """Scan graph nodes for non-allowlisted zero-degree nodes."""

    def __init__(
        self,
        *,
        registry: KGConnectivityRuleRegistry | None = None,
        metric_sink: OrphanMetricSinkProtocol | None = None,
        audit_sink: OrphanAuditSinkProtocol | None = None,
        source_resolver: OrphanSourceResolverProtocol | None = None,
    ) -> None:
        self._registry = registry or KGConnectivityRuleRegistry()
        self._metric_sink = metric_sink
        self._audit_sink = audit_sink or LoggingOrphanAuditSink()
        self._source_resolver = source_resolver or RelationalOrphanSourceResolver()

    def rule_node_types(self) -> tuple[str, ...]:
        return self._registry.rule_node_types()

    def scan(
        self,
        *,
        board_id: str,
        generation_id: str | None = None,
        limit: int = DEFAULT_ORPHAN_SAMPLE_LIMIT,
        node_type: str | None = None,
        connection: Any | None = None,
    ) -> OrphanScanReport:
        """Return a bounded safe report of non-allowlisted zero-degree nodes.

        ``limit`` bounds samples only. Counts are computed over every node in
        the selected labels. ``connection`` exists for tests and for callers
        that already hold a BoardConnection; production callers usually omit it.
        """

        sample_limit = _coerce_limit(limit)
        correlation_id = f"kg-orphan-scan-{uuid.uuid4().hex[:16]}"
        node_types = _selected_node_types(node_type)

        if connection is None:
            connection = _BoardGraphPortConnection(board_id)
        return self._scan_with_connection(
            connection,
            board_id=board_id,
            generation_id=generation_id,
            sample_limit=sample_limit,
            node_types=node_types,
            correlation_id=correlation_id,
        )

    def _scan_with_connection(
        self,
        graph_scope: Any,
        *,
        board_id: str,
        generation_id: str | None,
        sample_limit: int,
        node_types: tuple[str, ...],
        correlation_id: str,
    ) -> OrphanScanReport:
        samples: list[OrphanNodeSample] = []
        by_type: dict[str, int] = {}
        by_writer_path: dict[str, int] = {}
        unresolved_reasons: dict[str, int] = {}
        allowlisted_root_count = 0
        orphan_rows: list[_NodeRow] = []

        for current_type in node_types:
            try:
                node_rows = tuple(_iter_node_rows(graph_scope, current_type))
            except Exception as exc:
                raise OrphanScanQueryError(
                    phase="enumeration",
                    node_type=current_type,
                ) from exc

            try:
                connected_node_ids = _connected_node_ids(graph_scope, current_type)
            except Exception as exc:
                raise OrphanScanQueryError(
                    phase="connectivity",
                    node_type=current_type,
                ) from exc

            for row in node_rows:
                if row.node_id in connected_node_ids:
                    continue
                if self._is_allowlisted_root(row):
                    allowlisted_root_count += 1
                    continue

                by_type[row.node_type] = by_type.get(row.node_type, 0) + 1
                by_writer_path[row.writer_path] = (
                    by_writer_path.get(row.writer_path, 0) + 1
                )
                orphan_rows.append(row)

        source_statuses = self._source_resolver.resolve_many(
            board_id=board_id,
            source_artifact_refs=tuple(
                row.source_artifact_ref
                for row in orphan_rows
                if row.source_artifact_ref
            ),
        )
        for row in orphan_rows:
            status = _source_resolution_status(
                row.source_artifact_ref,
                source_statuses=source_statuses,
            )
            unresolved_reasons[status] = unresolved_reasons.get(status, 0) + 1
            if len(samples) < sample_limit:
                samples.append(
                    OrphanNodeSample(
                        node_id=row.node_id,
                        node_type=row.node_type,
                        writer_path=row.writer_path,
                        source_artifact_ref=row.source_artifact_ref,
                        source_resolution_status=status,
                        generation_id=generation_id,
                        reason=ZERO_DEGREE_REASON,
                        correlation_id=correlation_id,
                    )
                )

        orphan_count = sum(by_type.values())
        report = OrphanScanReport(
            board_id=board_id,
            generation_id=generation_id,
            orphan_count=orphan_count,
            orphan_count_by_type=by_type,
            orphan_count_by_writer_path=by_writer_path,
            samples=tuple(samples),
            unresolved_reasons=unresolved_reasons,
            allowlisted_root_count=allowlisted_root_count,
            correlation_id=correlation_id,
        )
        self._emit_detection_metrics(board_id, generation_id, report)
        self._emit_detection_audit(board_id, generation_id, report)
        return report

    def _is_allowlisted_root(self, row: _NodeRow) -> bool:
        return self._registry.is_technical_root_allowlisted(
            node_type=row.node_type,
            writer_path=row.writer_path,
            source_artifact_ref=row.source_artifact_ref,
        )

    def _emit_detection_metrics(
        self,
        board_id: str,
        generation_id: str | None,
        report: OrphanScanReport,
    ) -> None:
        if self._metric_sink is None:
            return
        for sample in report.samples:
            try:
                self._metric_sink.emit(
                    OrphanMetricEvent(
                        metric_name="kg_orphan_node_detected_total",
                        board_id=board_id,
                        node_type=sample.node_type,
                        writer_path=sample.writer_path,
                        outcome="detected",
                        reason=sample.reason,
                        source_resolution_status=sample.source_resolution_status,
                        generation_id=generation_id or "",
                    )
                )
            except Exception:
                logger.debug("kg.orphan.metric_emit_failed", exc_info=True)

    def _emit_detection_audit(
        self,
        board_id: str,
        generation_id: str | None,
        report: OrphanScanReport,
    ) -> None:
        if self._audit_sink is None:
            return
        for sample in report.samples:
            try:
                self._audit_sink.emit(
                    OrphanAuditRecord(
                        event_name="kg_orphan_node_detected",
                        board_id=board_id,
                        node_id=sample.node_id,
                        node_type=sample.node_type,
                        writer_path=sample.writer_path,
                        outcome="detected",
                        reason=sample.reason,
                        source_resolution_status=sample.source_resolution_status,
                        generation_id=generation_id or "",
                        correlation_id=report.correlation_id,
                        sample_count=len(report.samples),
                    )
                )
            except Exception:
                logger.debug("kg.orphan.audit_emit_failed", exc_info=True)


def schema_node_types_for_orphan_scanner() -> tuple[str, ...]:
    """Return the schema-backed node labels scanned for orphan debt."""

    return tuple(NODE_TYPES)


def schema_relationship_pairs_for_orphan_scanner() -> tuple[tuple[str, str, str], ...]:
    """Return every concrete relationship endpoint pair used for degree checks."""

    return tuple(_relationship_pairs())


def get_orphan_metric_labels() -> tuple[str, ...]:
    return SAFE_ORPHAN_METRIC_LABELS


def get_orphan_audit_fields() -> tuple[str, ...]:
    return SAFE_ORPHAN_AUDIT_FIELDS


def _selected_node_types(node_type: str | None) -> tuple[str, ...]:
    if node_type is None:
        return tuple(NODE_TYPES)
    if node_type not in NODE_TYPES:
        raise ValueError(f"unknown KG node_type for orphan scanner: {node_type}")
    return (node_type,)


def _relationship_pairs() -> Iterable[tuple[str, str, str]]:
    yield from REL_TYPES
    for rel_name, endpoint_pairs in MULTI_REL_TYPES:
        for from_type, to_type in endpoint_pairs:
            yield rel_name, from_type, to_type


def _iter_node_rows(graph_scope: Any, node_type: str) -> Iterable[_NodeRow]:
    result = graph_scope.execute(
        f"MATCH (n:{node_type}) "
        "RETURN n.id, n.source_artifact_ref, n.source_session_id, "
        "n.created_by_agent"
    )
    for row in result.rows:
        yield _NodeRow(
            node_id=str(row[0]),
            node_type=node_type,
            source_artifact_ref=_optional_str(row[1] if len(row) > 1 else None),
            writer_path=_safe_writer_path(
                row[3] if len(row) > 3 else None,
                row[2] if len(row) > 2 else None,
            ),
        )


def _connected_node_ids(graph_scope: Any, node_type: str) -> frozenset[str]:
    """Return connected ids for one label with one board-scoped graph query.

    Outgoing and incoming degree are aggregated separately so direction is
    explicit and each node is classified once. Query count is therefore bound
    by the selected schema labels, never by the number of nodes or edges.
    """

    result = graph_scope.execute(
        f"MATCH (n:{node_type}) "
        "OPTIONAL MATCH (n)-[r_out]->() "
        "WITH n, COUNT(r_out) AS out_degree "
        "OPTIONAL MATCH (n)<-[r_in]-() "
        "RETURN n.id, out_degree, COUNT(r_in) AS in_degree"
    )
    return frozenset(
        str(row[0])
        for row in result.rows
        if int(row[1] or 0) > 0 or int(row[2] or 0) > 0
    )


def _safe_writer_path(created_by_agent: Any, source_session_id: Any) -> str:
    raw = f"{created_by_agent or ''} {source_session_id or ''}".lower()
    if any(
        marker in raw
        for marker in (
            "deterministic",
            "worker_layer1",
            "rebuild",
            "discovery",
            "bootstrap",
            "schema",
            "system:",
        )
    ):
        return "deterministic_worker"
    if any(
        marker in raw
        for marker in (
            "cognitive",
            "commit_consolidation",
            "consolidation",
            "kgses_",
            "agent",
            "mcp",
        )
    ):
        return "commit_consolidation"
    return "unknown"


@dataclass(frozen=True, slots=True)
class _RelationalSourceIdentity:
    source_ref: str
    artifact_type: str
    artifact_id: str
    expected_card_type: str | None = None


_RELATIONAL_SOURCE_TYPES = frozenset(
    DETERMINISTIC_SOURCE_ARTIFACT_TYPES - CARD_SOURCE_ARTIFACT_TYPES
)
_CARD_SOURCE_TYPES = frozenset(
    {*CARD_SOURCE_ARTIFACT_TYPES, "card_relationship_target"}
)
_SOURCE_CHILD_MARKERS = frozenset(
    {
        "fr",
        "tr",
        "ac",
        "business_rule",
        "test_scenario",
        "api_contract",
        "integration_requirement",
        "observability_requirement",
        "decision",
        "decision_legacy",
        "learning",
        "alternative",
        "assumption",
        "boost_audit",
    }
)


def _parse_relational_source_ref(
    source_artifact_ref: str,
) -> _RelationalSourceIdentity | None:
    """Parse only writer-owned structured refs; malformed/unknown stays unresolved."""

    ref = str(source_artifact_ref or "")
    if not ref or ref != ref.strip() or any(char.isspace() for char in ref):
        return None
    parts = ref.split(":")
    if len(parts) < 2 or not all(parts):
        return None
    source_type = parts[0]
    if source_type == "board":
        if len(parts) != 2:
            return None
        return _RelationalSourceIdentity(ref, "board", parts[1])

    expected_card_type: str | None = None
    artifact_type = source_type
    suffix_index = 2
    if source_type == "card" and len(parts) >= 3 and parts[1] in {
        "bug",
        "test",
        "task",
    }:
        expected_card_type = parts[1]
        artifact_id = parts[2]
        artifact_type = "card"
        suffix_index = 3
    elif source_type in _CARD_SOURCE_TYPES:
        artifact_id = parts[1]
        artifact_type = "card"
        if source_type in {"bug", "test", "task"}:
            expected_card_type = source_type
    elif source_type in _RELATIONAL_SOURCE_TYPES:
        artifact_id = parts[1]
    else:
        return None

    suffix = parts[suffix_index:]
    if suffix and (
        len(suffix) != 2
        or suffix[0] not in _SOURCE_CHILD_MARKERS
    ):
        return None
    return _RelationalSourceIdentity(
        ref,
        artifact_type,
        artifact_id,
        expected_card_type,
    )


def _card_type_matches(row: object, expected: str | None) -> bool:
    if expected is None:
        return True
    raw = getattr(row, "card_type", None)
    actual = str(getattr(raw, "value", raw or "normal")).lower()
    if expected == "task":
        return actual not in {"bug", "test"}
    return actual == expected


async def _resolve_relational_source_identities(
    *,
    board_id: str,
    identities: tuple[_RelationalSourceIdentity, ...],
) -> frozenset[str]:
    """Resolve refs in bounded type batches and enforce board ownership."""

    from okto_pulse.core.ports.consolidation import (
        get_consolidation_persistence_port,
    )
    from okto_pulse.core.ports.relational_runtime import get_db_session

    persistence = get_consolidation_persistence_port()
    resolved: set[str] = set()
    async with get_db_session() as context:
        board_refs = tuple(
            identity
            for identity in identities
            if identity.artifact_type == "board"
        )
        if board_refs and await persistence.board_exists(
            context,
            board_id=board_id,
        ):
            resolved.update(
                identity.source_ref
                for identity in board_refs
                if identity.artifact_id == board_id
            )

        artifact_types = sorted(
            {
                identity.artifact_type
                for identity in identities
                if identity.artifact_type != "board"
            }
        )
        for artifact_type in artifact_types:
            scoped = tuple(
                identity
                for identity in identities
                if identity.artifact_type == artifact_type
            )
            identities_by_id: dict[str, list[_RelationalSourceIdentity]] = {}
            for identity in scoped:
                identities_by_id.setdefault(identity.artifact_id, []).append(identity)
            artifact_ids = tuple(identities_by_id)
            for start in range(
                0,
                len(artifact_ids),
                _RELATIONAL_SOURCE_LOOKUP_BATCH_SIZE,
            ):
                batch_ids = artifact_ids[
                    start : start + _RELATIONAL_SOURCE_LOOKUP_BATCH_SIZE
                ]
                rows = await persistence.list_artifacts(
                    context,
                    artifact_type=artifact_type,
                    artifact_ids=batch_ids,
                    board_id=board_id,
                )
                for row in rows:
                    row_id = str(getattr(row, "id", ""))
                    for identity in identities_by_id.get(row_id, ()):
                        if _card_type_matches(
                            row,
                            identity.expected_card_type,
                        ):
                            resolved.add(identity.source_ref)
    return frozenset(resolved)


def _source_resolution_status(
    source_artifact_ref: str | None,
    *,
    source_statuses: dict[str, str],
) -> str:
    if not source_artifact_ref:
        return SOURCE_REF_MISSING
    return (
        SOURCE_REF_RESOLVED
        if source_statuses.get(source_artifact_ref) == SOURCE_REF_RESOLVED
        else SOURCE_REF_UNRESOLVED
    )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _coerce_limit(limit: int) -> int:
    try:
        value = int(limit)
    except Exception:
        value = DEFAULT_ORPHAN_SAMPLE_LIMIT
    return max(0, min(value, MAX_ORPHAN_SAMPLE_LIMIT))


__all__ = [
    "ALLOWLISTED_TECHNICAL_ROOT_REASON",
    "DEFAULT_ORPHAN_SAMPLE_LIMIT",
    "InMemoryOrphanAuditSink",
    "InMemoryOrphanMetricSink",
    "LoggingOrphanAuditSink",
    "MAX_ORPHAN_SAMPLE_LIMIT",
    "OrphanAuditRecord",
    "OrphanAuditSinkProtocol",
    "OrphanIntegrityProjection",
    "OrphanMetricEvent",
    "OrphanMetricSinkProtocol",
    "OrphanNodeSample",
    "OrphanNodeScanner",
    "OrphanScanQueryError",
    "OrphanScanReport",
    "OrphanSourceResolverProtocol",
    "RelationalOrphanSourceResolver",
    "SAFE_ORPHAN_AUDIT_FIELDS",
    "SAFE_ORPHAN_METRIC_LABELS",
    "SAFE_ORPHAN_SAMPLE_FIELDS",
    "SOURCE_REF_MISSING",
    "SOURCE_REF_RESOLVED",
    "SOURCE_REF_UNRESOLVED",
    "ZERO_DEGREE_REASON",
    "ZERO_ORPHAN_VALIDATION_FAILED",
    "ZERO_ORPHAN_VALIDATION_NOT_EVALUATED",
    "ZERO_ORPHAN_VALIDATION_PASSED",
    "ZERO_ORPHAN_VALIDATION_PENDING_BACKFILL",
    "ZERO_ORPHAN_VALIDATION_UNAVAILABLE",
    "build_orphan_integrity_projection",
    "get_orphan_audit_fields",
    "get_orphan_metric_labels",
    "schema_node_types_for_orphan_scanner",
    "schema_relationship_pairs_for_orphan_scanner",
]
