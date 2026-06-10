"""Orphan-node integrity scanner and report models for the KG.

KG-ZO-01 prevents new loose nodes at write time. This module is the corrective
read side for historical graph debt: it scans persisted graph nodes and reports
non-allowlisted zero-degree nodes without leaking user-authored payloads.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable, Protocol

from okto_pulse.core.kg.connectivity_guard import (
    KGConnectivityRuleRegistry,
    SourceResolutionStatus,
)
from okto_pulse.core.kg.schema import MULTI_REL_TYPES, NODE_TYPES, REL_TYPES

logger = logging.getLogger("okto_pulse.kg.orphan_integrity")

MAX_ORPHAN_SAMPLE_LIMIT = 100
DEFAULT_ORPHAN_SAMPLE_LIMIT = 25
ZERO_DEGREE_REASON = "zero_graph_degree"
ALLOWLISTED_TECHNICAL_ROOT_REASON = "allowlisted_technical_root"
BACKFILL_SESSION_PREFIX = "kg_orphan_backfill"
ZERO_ORPHAN_VALIDATION_PASSED = "passed"
ZERO_ORPHAN_VALIDATION_PENDING_BACKFILL = "pending_backfill"
ZERO_ORPHAN_VALIDATION_FAILED = "failed_orphan_validation"
ZERO_ORPHAN_VALIDATION_UNAVAILABLE = "unavailable"
ZERO_ORPHAN_VALIDATION_NOT_EVALUATED = "not_evaluated"


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
class OrphanBackfillSample:
    node_id: str
    node_type: str
    writer_path: str
    outcome: str
    reason: str
    edge_type: str | None
    target_node_type: str | None
    target_node_id: str | None
    source_resolution_status: str
    generation_id: str | None
    correlation_id: str

    def to_safe_dict(self) -> dict[str, str | None]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "writer_path": self.writer_path,
            "outcome": self.outcome,
            "reason": self.reason,
            "edge_type": self.edge_type,
            "target_node_type": self.target_node_type,
            "target_node_id": self.target_node_id,
            "source_resolution_status": self.source_resolution_status,
            "generation_id": self.generation_id,
            "correlation_id": self.correlation_id,
        }


@dataclass(frozen=True)
class OrphanBackfillResult:
    detected: int
    connected: int
    noop: int
    unresolved: int
    ambiguous: int
    semantic_pending: int
    samples: tuple[OrphanBackfillSample, ...]
    correlation_id: str

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "detected": self.detected,
            "connected": self.connected,
            "noop": self.noop,
            "unresolved": self.unresolved,
            "ambiguous": self.ambiguous,
            "semantic_pending": self.semantic_pending,
            "samples": [sample.to_safe_dict() for sample in self.samples],
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


class OrphanNodeScanner:
    """Scan graph nodes for non-allowlisted zero-degree nodes."""

    def __init__(
        self,
        *,
        registry: KGConnectivityRuleRegistry | None = None,
        metric_sink: OrphanMetricSinkProtocol | None = None,
        audit_sink: OrphanAuditSinkProtocol | None = None,
    ) -> None:
        self._registry = registry or KGConnectivityRuleRegistry()
        self._metric_sink = metric_sink
        self._audit_sink = audit_sink or LoggingOrphanAuditSink()

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
            from okto_pulse.core.kg.schema import open_board_connection

            with open_board_connection(board_id) as (_db, kconn):
                return self._scan_with_connection(
                    kconn,
                    board_id=board_id,
                    generation_id=generation_id,
                    sample_limit=sample_limit,
                    node_types=node_types,
                    correlation_id=correlation_id,
                )
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
        kconn: Any,
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

        for current_type in node_types:
            for row in _iter_node_rows(kconn, current_type):
                if _node_degree(kconn, row.node_type, row.node_id) != 0:
                    continue
                if self._is_allowlisted_root(row):
                    allowlisted_root_count += 1
                    continue

                by_type[row.node_type] = by_type.get(row.node_type, 0) + 1
                by_writer_path[row.writer_path] = (
                    by_writer_path.get(row.writer_path, 0) + 1
                )
                status = _source_resolution_status(row.source_artifact_ref)
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


class OrphanBackfillReconciler:
    """Idempotently materialize safe edges for historical orphan nodes."""

    def __init__(
        self,
        *,
        scanner: OrphanNodeScanner | None = None,
        registry: KGConnectivityRuleRegistry | None = None,
        metric_sink: OrphanMetricSinkProtocol | None = None,
        audit_sink: OrphanAuditSinkProtocol | None = None,
    ) -> None:
        self._registry = registry or KGConnectivityRuleRegistry()
        self._metric_sink = metric_sink
        self._audit_sink = audit_sink or LoggingOrphanAuditSink()
        self._scanner = scanner or OrphanNodeScanner(
            registry=self._registry,
            metric_sink=metric_sink,
            audit_sink=self._audit_sink,
        )

    def rule_node_types(self) -> tuple[str, ...]:
        return self._registry.rule_node_types()

    def run(
        self,
        *,
        board_id: str,
        dry_run: bool = False,
        limit: int = DEFAULT_ORPHAN_SAMPLE_LIMIT,
        node_ids: Iterable[str] | None = None,
        generation_id: str | None = None,
        connection: Any | None = None,
    ) -> OrphanBackfillResult:
        correlation_id = f"kg-orphan-backfill-{uuid.uuid4().hex[:16]}"
        sample_limit = _coerce_limit(limit)
        requested_node_ids = tuple(str(node_id) for node_id in (node_ids or ()))

        if connection is None:
            from okto_pulse.core.kg.schema import open_board_connection

            with open_board_connection(board_id) as (_db, kconn):
                return self._run_with_connection(
                    kconn,
                    board_id=board_id,
                    dry_run=dry_run,
                    sample_limit=sample_limit,
                    requested_node_ids=requested_node_ids,
                    generation_id=generation_id,
                    correlation_id=correlation_id,
                )
        return self._run_with_connection(
            connection,
            board_id=board_id,
            dry_run=dry_run,
            sample_limit=sample_limit,
            requested_node_ids=requested_node_ids,
            generation_id=generation_id,
            correlation_id=correlation_id,
        )

    def _run_with_connection(
        self,
        kconn: Any,
        *,
        board_id: str,
        dry_run: bool,
        sample_limit: int,
        requested_node_ids: tuple[str, ...],
        generation_id: str | None,
        correlation_id: str,
    ) -> OrphanBackfillResult:
        rows = self._candidate_rows(
            kconn,
            board_id=board_id,
            generation_id=generation_id,
            limit=sample_limit,
            requested_node_ids=requested_node_ids,
        )
        counters = {
            "connected": 0,
            "noop": 0,
            "unresolved": 0,
            "ambiguous": 0,
            "semantic_pending": 0,
        }
        samples: list[OrphanBackfillSample] = []
        for row in rows:
            outcome = self._evaluate_row(
                kconn,
                board_id=board_id,
                row=row,
                dry_run=dry_run,
                generation_id=generation_id,
                correlation_id=correlation_id,
            )
            counters[outcome.outcome] = counters.get(outcome.outcome, 0) + 1
            if len(samples) < sample_limit:
                samples.append(outcome)
            self._emit_backfill_metric(board_id, generation_id, outcome)
            self._emit_backfill_audit(board_id, generation_id, outcome)

        return OrphanBackfillResult(
            detected=len(rows),
            connected=counters["connected"],
            noop=counters["noop"],
            unresolved=counters["unresolved"],
            ambiguous=counters["ambiguous"],
            semantic_pending=counters["semantic_pending"],
            samples=tuple(samples),
            correlation_id=correlation_id,
        )

    def _candidate_rows(
        self,
        kconn: Any,
        *,
        board_id: str,
        generation_id: str | None,
        limit: int,
        requested_node_ids: tuple[str, ...],
    ) -> tuple[_NodeRow, ...]:
        if requested_node_ids:
            rows: list[_NodeRow] = []
            for node_id in requested_node_ids:
                row = _lookup_node_row_by_id(kconn, node_id)
                if row is not None and not self._is_allowlisted_root(row):
                    rows.append(row)
            return tuple(rows[:limit])

        report = self._scanner.scan(
            board_id=board_id,
            generation_id=generation_id,
            limit=limit,
            connection=kconn,
        )
        return tuple(
            _NodeRow(
                node_id=sample.node_id,
                node_type=sample.node_type,
                source_artifact_ref=sample.source_artifact_ref,
                writer_path=sample.writer_path,
            )
            for sample in report.samples
        )

    def _is_allowlisted_root(self, row: _NodeRow) -> bool:
        return self._registry.is_technical_root_allowlisted(
            node_type=row.node_type,
            writer_path=row.writer_path,
            source_artifact_ref=row.source_artifact_ref,
        )

    def _evaluate_row(
        self,
        kconn: Any,
        *,
        board_id: str,
        row: _NodeRow,
        dry_run: bool,
        generation_id: str | None,
        correlation_id: str,
    ) -> OrphanBackfillSample:
        if _node_degree(kconn, row.node_type, row.node_id) != 0:
            return _backfill_sample(
                row,
                outcome="noop",
                reason="already_connected",
                source_resolution_status=SourceResolutionStatus.RESOLVED_EXISTING_NODE.value,
                generation_id=generation_id,
                correlation_id=correlation_id,
            )

        if row.node_type == "Learning" and _is_bug_derived_source_ref(
            row.source_artifact_ref
        ):
            bug_resolution = _resolve_bug_target(kconn, row.source_artifact_ref)
            if len(bug_resolution) == 1:
                bug_id = bug_resolution[0]
                if _edge_count(
                    kconn,
                    "validates",
                    "Learning",
                    "Bug",
                    row.node_id,
                    bug_id,
                ):
                    return _backfill_sample(
                        row,
                        outcome="noop",
                        reason="already_connected",
                        edge_type="validates",
                        target_node_type="Bug",
                        target_node_id=bug_id,
                        source_resolution_status=SourceResolutionStatus.RESOLVED_EXISTING_NODE.value,
                        generation_id=generation_id,
                        correlation_id=correlation_id,
                    )
                if not dry_run:
                    _create_edge(
                        kconn,
                        board_id=board_id,
                        edge_type="validates",
                        from_type="Learning",
                        to_type="Bug",
                        from_id=row.node_id,
                        to_id=bug_id,
                    )
                return _backfill_sample(
                    row,
                    outcome="connected",
                    reason="bug_learning_validates_bug",
                    edge_type="validates",
                    target_node_type="Bug",
                    target_node_id=bug_id,
                    source_resolution_status=SourceResolutionStatus.RESOLVED_EXISTING_NODE.value,
                    generation_id=generation_id,
                    correlation_id=correlation_id,
                )
            if len(bug_resolution) > 1:
                return _backfill_sample(
                    row,
                    outcome="ambiguous",
                    reason="ambiguous_source_ref",
                    source_resolution_status=SourceResolutionStatus.AMBIGUOUS_SOURCE_REF.value,
                    generation_id=generation_id,
                    correlation_id=correlation_id,
                )
            return _backfill_sample(
                row,
                outcome="semantic_pending",
                reason="unresolved_semantic_link_pending",
                source_resolution_status=SourceResolutionStatus.UNRESOLVED_SOURCE_REF.value,
                generation_id=generation_id,
                correlation_id=correlation_id,
            )

        entity_resolution = _resolve_entity_target(kconn, row.source_artifact_ref)
        if len(entity_resolution) == 1 and _belongs_to_accepts(row.node_type, "Entity"):
            entity_id = entity_resolution[0]
            if _edge_count(
                kconn,
                "belongs_to",
                row.node_type,
                "Entity",
                row.node_id,
                entity_id,
            ):
                return _backfill_sample(
                    row,
                    outcome="noop",
                    reason="already_connected",
                    edge_type="belongs_to",
                    target_node_type="Entity",
                    target_node_id=entity_id,
                    source_resolution_status=SourceResolutionStatus.RESOLVED_EXISTING_NODE.value,
                    generation_id=generation_id,
                    correlation_id=correlation_id,
                )
            if not dry_run:
                _create_edge(
                    kconn,
                    board_id=board_id,
                    edge_type="belongs_to",
                    from_type=row.node_type,
                    to_type="Entity",
                    from_id=row.node_id,
                    to_id=entity_id,
                )
            reason = (
                "provenance_connected_semantic_pending"
                if row.node_type in {"Learning", "Assumption", "Alternative"}
                else "provenance_connected"
            )
            return _backfill_sample(
                row,
                outcome=(
                    "semantic_pending"
                    if reason == "provenance_connected_semantic_pending"
                    else "connected"
                ),
                reason=reason,
                edge_type="belongs_to",
                target_node_type="Entity",
                target_node_id=entity_id,
                source_resolution_status=SourceResolutionStatus.RESOLVED_EXISTING_NODE.value,
                generation_id=generation_id,
                correlation_id=correlation_id,
            )
        if len(entity_resolution) > 1:
            return _backfill_sample(
                row,
                outcome="ambiguous",
                reason="ambiguous_source_ref",
                source_resolution_status=SourceResolutionStatus.AMBIGUOUS_SOURCE_REF.value,
                generation_id=generation_id,
                correlation_id=correlation_id,
            )
        if row.source_artifact_ref:
            return _backfill_sample(
                row,
                outcome="unresolved",
                reason="unresolved_source_ref",
                source_resolution_status=SourceResolutionStatus.UNRESOLVED_SOURCE_REF.value,
                generation_id=generation_id,
                correlation_id=correlation_id,
            )
        return _backfill_sample(
            row,
            outcome="semantic_pending",
            reason="fuzzy_or_prose_only_evidence",
            source_resolution_status="missing_source_artifact_ref",
            generation_id=generation_id,
            correlation_id=correlation_id,
        )

    def _emit_backfill_metric(
        self,
        board_id: str,
        generation_id: str | None,
        sample: OrphanBackfillSample,
    ) -> None:
        if self._metric_sink is None:
            return
        try:
            self._metric_sink.emit(
                OrphanMetricEvent(
                    metric_name="kg_orphan_backfill_total",
                    board_id=board_id,
                    node_type=sample.node_type,
                    writer_path=sample.writer_path,
                    outcome=sample.outcome,
                    reason=sample.reason,
                    source_resolution_status=sample.source_resolution_status,
                    generation_id=generation_id or "",
                )
            )
        except Exception:
            logger.debug("kg.orphan.backfill_metric_emit_failed", exc_info=True)

    def _emit_backfill_audit(
        self,
        board_id: str,
        generation_id: str | None,
        sample: OrphanBackfillSample,
    ) -> None:
        if self._audit_sink is None:
            return
        try:
            self._audit_sink.emit(
                OrphanAuditRecord(
                    event_name="kg_orphan_backfill_audit_record",
                    board_id=board_id,
                    node_id=sample.node_id,
                    node_type=sample.node_type,
                    writer_path=sample.writer_path,
                    outcome=sample.outcome,
                    reason=sample.reason,
                    source_resolution_status=sample.source_resolution_status,
                    generation_id=generation_id or "",
                    correlation_id=sample.correlation_id,
                )
            )
        except Exception:
            logger.debug("kg.orphan.backfill_audit_emit_failed", exc_info=True)


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


def _lookup_node_row_by_id(kconn: Any, node_id: str) -> _NodeRow | None:
    for node_type in NODE_TYPES:
        result = None
        try:
            result = kconn.execute(
                f"MATCH (n:{node_type}) "
                "WHERE n.id = $node_id "
                "RETURN n.id, n.source_artifact_ref, n.source_session_id, "
                "n.created_by_agent LIMIT 1",
                {"node_id": node_id},
            )
            if result.has_next():
                row = result.get_next()
                return _NodeRow(
                    node_id=str(row[0]),
                    node_type=node_type,
                    source_artifact_ref=_optional_str(row[1] if len(row) > 1 else None),
                    writer_path=_safe_writer_path(
                        row[3] if len(row) > 3 else None,
                        row[2] if len(row) > 2 else None,
                    ),
                )
        finally:
            _close_result(result)
    return None


def _resolve_entity_target(kconn: Any, source_artifact_ref: str | None) -> tuple[str, ...]:
    if not source_artifact_ref:
        return ()
    matches: list[str] = []
    for source_ref in _entity_source_ref_candidates(source_artifact_ref):
        matches.extend(_lookup_node_ids_by_source_ref(kconn, "Entity", source_ref))
    return _dedupe(matches)


def _resolve_bug_target(kconn: Any, source_artifact_ref: str | None) -> tuple[str, ...]:
    if not source_artifact_ref:
        return ()
    candidates = _bug_ref_candidates(source_artifact_ref)
    matches: list[str] = []
    for candidate in candidates:
        matches.extend(_lookup_node_ids_by_source_ref(kconn, "Bug", candidate))
        row = _lookup_node_row_by_id(kconn, candidate)
        if row is not None and row.node_type == "Bug":
            matches.append(row.node_id)
    return _dedupe(matches)


def _lookup_node_ids_by_source_ref(
    kconn: Any,
    node_type: str,
    source_artifact_ref: str,
) -> tuple[str, ...]:
    result = None
    try:
        result = kconn.execute(
            f"MATCH (n:{node_type}) "
            "WHERE n.source_artifact_ref = $ref RETURN n.id",
            {"ref": source_artifact_ref},
        )
        ids: list[str] = []
        while result.has_next():
            ids.append(str(result.get_next()[0]))
        return tuple(ids)
    finally:
        _close_result(result)


def _entity_source_ref_candidates(source_artifact_ref: str) -> tuple[str, ...]:
    parts = [part for part in source_artifact_ref.split(":") if part]
    candidates = [source_artifact_ref]
    for length in range(len(parts) - 1, 1, -1):
        candidates.append(":".join(parts[:length]))
    return _dedupe(candidates)


def _bug_ref_candidates(source_artifact_ref: str) -> tuple[str, ...]:
    parts = [part for part in source_artifact_ref.split(":") if part]
    candidates = [source_artifact_ref]
    bug_token: str | None = None
    if len(parts) >= 2 and parts[0] == "bug":
        bug_token = parts[1]
    elif len(parts) >= 3 and parts[0] == "card" and parts[1] == "bug":
        bug_token = parts[2]
    if bug_token:
        candidates.extend(
            (
                bug_token,
                f"bug:{bug_token}",
                f"card:bug:{bug_token}",
                f"card:{bug_token}",
            )
        )
    return _dedupe(candidates)


def _dedupe(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            deduped.append(value)
    return tuple(deduped)


def _is_bug_derived_source_ref(source_artifact_ref: str | None) -> bool:
    if not source_artifact_ref:
        return False
    return (
        source_artifact_ref.startswith("bug:")
        or source_artifact_ref.startswith("card:bug:")
    )


def _belongs_to_accepts(from_type: str, to_type: str) -> bool:
    for rel_name, pairs in MULTI_REL_TYPES:
        if rel_name == "belongs_to":
            return (from_type, to_type) in pairs
    return False


def _edge_count(
    kconn: Any,
    edge_type: str,
    from_type: str,
    to_type: str,
    from_id: str,
    to_id: str,
) -> int:
    return _count_relationship(
        kconn,
        f"MATCH (a:{from_type})-[r:{edge_type}]->(b:{to_type}) "
        "WHERE a.id = $from_id AND b.id = $to_id RETURN count(r)",
        {"from_id": from_id, "to_id": to_id},
    )


def _create_edge(
    kconn: Any,
    *,
    board_id: str,
    edge_type: str,
    from_type: str,
    to_type: str,
    from_id: str,
    to_id: str,
) -> None:
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    orch = TransactionOrchestrator(
        kuzu_conn=kconn,
        sqlite_session=None,
        session_id=f"{BACKFILL_SESSION_PREFIX}_{uuid.uuid4().hex[:8]}",
        board_id=board_id,
    )
    orch.create_edge(
        edge_type,
        from_id,
        to_id,
        attrs={
            "confidence": 1.0,
            "layer": "fallback",
            "rule_id": "KG-ZO-02.backfill",
            "fallback_reason": "orphan_backfill",
        },
        from_type=from_type,
        to_type=to_type,
    )


def _backfill_sample(
    row: _NodeRow,
    *,
    outcome: str,
    reason: str,
    source_resolution_status: str,
    generation_id: str | None,
    correlation_id: str,
    edge_type: str | None = None,
    target_node_type: str | None = None,
    target_node_id: str | None = None,
) -> OrphanBackfillSample:
    return OrphanBackfillSample(
        node_id=row.node_id,
        node_type=row.node_type,
        writer_path=row.writer_path,
        outcome=outcome,
        reason=reason,
        edge_type=edge_type,
        target_node_type=target_node_type,
        target_node_id=target_node_id,
        source_resolution_status=source_resolution_status,
        generation_id=generation_id,
        correlation_id=correlation_id,
    )


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


def _iter_node_rows(kconn: Any, node_type: str) -> Iterable[_NodeRow]:
    result = None
    try:
        result = kconn.execute(
            f"MATCH (n:{node_type}) "
            "RETURN n.id, n.source_artifact_ref, n.source_session_id, "
            "n.created_by_agent"
        )
        while result.has_next():
            row = result.get_next()
            yield _NodeRow(
                node_id=str(row[0]),
                node_type=node_type,
                source_artifact_ref=_optional_str(row[1] if len(row) > 1 else None),
                writer_path=_safe_writer_path(
                    row[3] if len(row) > 3 else None,
                    row[2] if len(row) > 2 else None,
                ),
            )
    finally:
        _close_result(result)


def _node_degree(kconn: Any, node_type: str, node_id: str) -> int:
    degree = 0
    for rel_name, from_type, to_type in _relationship_pairs():
        if from_type == node_type:
            degree += _count_relationship(
                kconn,
                f"MATCH (n:{from_type})-[r:{rel_name}]->(m:{to_type}) "
                "WHERE n.id = $node_id RETURN count(r)",
                node_id,
            )
        if to_type == node_type:
            degree += _count_relationship(
                kconn,
                f"MATCH (m:{from_type})-[r:{rel_name}]->(n:{to_type}) "
                "WHERE n.id = $node_id RETURN count(r)",
                node_id,
            )
        if degree:
            return degree
    return degree


def _count_relationship(kconn: Any, cypher: str, node_id: str) -> int:
    result = None
    try:
        params = node_id if isinstance(node_id, dict) else {"node_id": node_id}
        result = kconn.execute(cypher, params)
        if result.has_next():
            return int(result.get_next()[0])
    except Exception:
        logger.debug("kg.orphan.degree_query_failed", exc_info=True)
        return 0
    finally:
        _close_result(result)
    return 0


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
            "agent",
            "mcp",
        )
    ):
        return "commit_consolidation"
    return "unknown"


def _source_resolution_status(source_artifact_ref: str | None) -> str:
    if not source_artifact_ref:
        return "missing_source_artifact_ref"
    return SourceResolutionStatus.UNRESOLVED_SOURCE_REF.value


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


def _close_result(result: Any) -> None:
    if result is None:
        return
    close = getattr(result, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


__all__ = [
    "ALLOWLISTED_TECHNICAL_ROOT_REASON",
    "DEFAULT_ORPHAN_SAMPLE_LIMIT",
    "InMemoryOrphanAuditSink",
    "InMemoryOrphanMetricSink",
    "LoggingOrphanAuditSink",
    "MAX_ORPHAN_SAMPLE_LIMIT",
    "OrphanAuditRecord",
    "OrphanAuditSinkProtocol",
    "OrphanBackfillReconciler",
    "OrphanBackfillResult",
    "OrphanBackfillSample",
    "OrphanIntegrityProjection",
    "OrphanMetricEvent",
    "OrphanMetricSinkProtocol",
    "OrphanNodeSample",
    "OrphanNodeScanner",
    "OrphanScanReport",
    "SAFE_ORPHAN_AUDIT_FIELDS",
    "SAFE_ORPHAN_METRIC_LABELS",
    "SAFE_ORPHAN_SAMPLE_FIELDS",
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
