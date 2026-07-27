"""Rebuild report store + terminal state guard (KG-02.4).

This module implements the durability boundary required by TR7 + TR16 +
BR br_82deef11 + IR ir_6d092147 + API contract ``api_1b43931d``:

* ``RebuildReportStore.persist`` returns one of three outcomes —
  ``stored``, ``store_failed`` or ``sensitive_payload_rejected`` — and
  publishes counters ``kg_rebuild_report_total`` (OR or_56ec0300) and
  ``kg_rebuild_report_persist_total`` (OR or_9b4a7726).
* ``RebuildReportTerminalStateGuard.require_report_ref`` exposes
  ``api_1b43931d`` and produces the canonical ``publishable_status``
  (``report_persist_failed`` when persistence did not succeed), along
  with the bounded counter ``kg_rebuild_terminal_report_total`` (OR
  or_f933b2ba) so operators can prove every terminal status carries a
  report reference.

The store rejects payloads that look like they contain unredacted
secrets (TR7). The check is intentionally simple — a key-name scan
plus a value-shape heuristic — because the canonical report carries
hashes + refs + decision summaries, never raw credentials. Anything
that trips the filter is a sign that an upstream rebuilder leaked a
secret and must be redacted before persistence.

Layout:

    <base>/rebuild/reports/<report_id>.json
"""

from __future__ import annotations

import logging
import re
import threading
from okto_pulse.core.runtime_context import runtime_lock, runtime_state
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import (
    resolve_rebuild_audit_artifact_store,
)

logger = logging.getLogger("okto_pulse.kg.rebuild_report")


REBUILD_DIRNAME = "rebuild"
REPORTS_DIRNAME = "reports"
REPORT_REF_PREFIX = "report_"


TERMINAL_STATUSES_REQUIRING_REPORT: frozenset[str] = frozenset(
    {
        "completed",
        "partially_rebuilt",
        "rolled_back",
        "failed_orphan_validation",
        "rebuild_failed",
        "recovery_failed",
    }
)


# TR7 — sensitive key detection. The match is on the key NAME anywhere
# in the nested payload (case-insensitive). The report schema does not
# legitimately need any of these.
SENSITIVE_KEY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)password"),
    re.compile(r"(?i)secret"),
    re.compile(r"(?i)token"),
    re.compile(r"(?i)api[_-]?key"),
    re.compile(r"(?i)private[_-]?key"),
    re.compile(r"(?i)credential"),
    re.compile(r"(?i)bearer"),
    re.compile(r"(?i)session[_-]?id"),
)

# Allowlist for legitimate uses of names that contain "ref" or "token"
# in non-sensitive contexts. Anything ending with ``_ref`` or named
# ``report_ref`` / ``manifest_ref`` / ``confirmation_id`` is canonical
# refs the operator NEEDS to see.
#
# val_da282108 rework: ``owner_token`` was previously allowlisted under
# the rationale that reports don't usually surface it. But the report
# payload is operator-visible and could exfiltrate a live KG-01 lock
# token if a faulty step adapter included it in drilldown. Per TR7 the
# report may only carry refs/hashes/decisions — lock material is OUT.
_ALLOWLISTED_KEYS: frozenset[str] = frozenset(
    {
        "report_ref",
        "manifest_ref",
        "confirmation_id",
        "audit_ref",
        "history_ref",
    }
)


# Value-shape heuristic: long base64-ish strings inside arbitrary values
# also count as sensitive even if the key is innocuous. Empirically
# matches typical bearer tokens / API keys.
_SUSPECT_VALUE_RE = re.compile(r"^[A-Za-z0-9+/_\-]{32,}={0,2}$")


def _scan_for_sensitive(payload: Any, _path: tuple[str, ...] = ()) -> str | None:
    """Walk ``payload`` and return the first sensitive marker found, or
    ``None`` if everything is clean. Returns the dot-joined key path so
    operators see which field tripped the filter."""

    if isinstance(payload, dict):
        for key, value in payload.items():
            key_str = str(key)
            if key_str not in _ALLOWLISTED_KEYS:
                for pat in SENSITIVE_KEY_PATTERNS:
                    if pat.search(key_str):
                        return ".".join((*_path, key_str))
            nested = _scan_for_sensitive(value, (*_path, key_str))
            if nested is not None:
                return nested
        return None
    if isinstance(payload, (list, tuple)):
        for idx, item in enumerate(payload):
            nested = _scan_for_sensitive(item, (*_path, f"[{idx}]"))
            if nested is not None:
                return nested
        return None
    if isinstance(payload, str):
        # Skip allowlisted leaf paths
        if _path and _path[-1] in _ALLOWLISTED_KEYS:
            return None
        # Hash-like strings (64-hex) are NOT secrets — they are the
        # whole point of the report (TR7: persist hashes).
        if re.fullmatch(r"[0-9a-fA-F]{64}", payload):
            return None
        if _SUSPECT_VALUE_RE.match(payload) and len(payload) >= 40:
            return ".".join(_path) or "<value>"
    return None


class ReportPersistOutcome(str, Enum):
    """Bounded outcomes per OR or_9b4a7726 + api_1b43931d enum."""

    STORED = "stored"
    STORE_FAILED = "store_failed"
    SENSITIVE_PAYLOAD_REJECTED = "sensitive_payload_rejected"


class ReportEvent(str, Enum):
    """OR or_56ec0300 label vocabulary for ``kg_rebuild_report_total``."""

    CREATED = "created"
    FAILED = "failed"
    OPENED = "opened"


@dataclass(frozen=True, slots=True)
class RebuildReportSummary:
    """Executive summary (br_752614b4 — summary-first)."""

    board_id: str
    run_id: str
    status: str
    started_at: str
    finished_at: str
    counts: dict[str, int] = field(default_factory=dict)
    triggered_by: str | None = None
    previous_kg_generation_id: str | None = None
    kg_generation_id: str | None = None


@dataclass(frozen=True, slots=True)
class RebuildReportPayload:
    """Canonical report payload. ``drilldown`` carries the per-artifact
    detail; ``hashes`` carries content hashes (TR7 — never raw payload)."""

    summary: RebuildReportSummary
    hashes: dict[str, str] = field(default_factory=dict)
    source_refs: tuple[str, ...] = field(default_factory=tuple)
    reconciliation_decisions: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    drilldown: dict[str, Any] = field(default_factory=dict)
    operator_notes: str | None = None

    def to_persistable_dict(self) -> dict[str, Any]:
        return {
            "summary": {
                "board_id": self.summary.board_id,
                "run_id": self.summary.run_id,
                "status": self.summary.status,
                "started_at": self.summary.started_at,
                "finished_at": self.summary.finished_at,
                "counts": dict(self.summary.counts),
                "triggered_by": self.summary.triggered_by,
                "previous_kg_generation_id": self.summary.previous_kg_generation_id,
                "kg_generation_id": self.summary.kg_generation_id,
            },
            "hashes": dict(self.hashes),
            "source_refs": list(self.source_refs),
            "reconciliation_decisions": [
                dict(d) for d in self.reconciliation_decisions
            ],
            "drilldown": dict(self.drilldown),
            "operator_notes": self.operator_notes,
        }


@dataclass(frozen=True, slots=True)
class ReportPersistResult:
    """Frozen result of ``RebuildReportStore.persist``."""

    outcome: str  # ReportPersistOutcome value
    report_ref: str | None
    report_id: str | None
    board_id: str
    run_id: str
    persisted_at: str | None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class TerminalGuardDecision:
    """Frozen response for ``api_1b43931d``."""

    publishable_status: str
    report_ref_required: bool
    promotion_allowed: bool
    operator_action: str | None


# --- Counters ----------------------------------------------------------------

# OR or_56ec0300 — kg_rebuild_report_total (event: created|failed|opened)
_REPORT_LABELS = ("board_id", "status", "event")
_report_counter = runtime_state("kg.rebuild_report.report_counter", dict)
_report_counter_lock = runtime_lock("kg.rebuild_report.report_counter")


def _bump_report(*, board_id: str, status: str, event: str) -> None:
    key = (board_id, status, event)
    with _report_counter_lock:
        _report_counter[key] = _report_counter.get(key, 0) + 1


def get_report_count(
    board_id: str, *, status: str | None = None, event: str | None = None
) -> int:
    with _report_counter_lock:
        total = 0
        for (b, st, ev), value in _report_counter.items():
            if b != board_id:
                continue
            if status is not None and st != status:
                continue
            if event is not None and ev != event:
                continue
            total += value
        return total


def get_report_counter_labels() -> tuple[str, ...]:
    return _REPORT_LABELS


def get_report_samples() -> list[dict[str, Any]]:
    with _report_counter_lock:
        return [
            {"board_id": b, "status": st, "event": ev, "count": value}
            for (b, st, ev), value in _report_counter.items()
        ]


def reset_report_counter() -> None:
    with _report_counter_lock:
        _report_counter.clear()


# OR or_9b4a7726 — kg_rebuild_report_persist_total (outcome)
_PERSIST_LABELS = ("board_id", "status", "outcome")
_persist_counter = runtime_state("kg.rebuild_report.persist_counter", dict)
_persist_counter_lock = runtime_lock("kg.rebuild_report.persist_counter")


def _bump_persist(*, board_id: str, status: str, outcome: str) -> None:
    key = (board_id, status, outcome)
    with _persist_counter_lock:
        _persist_counter[key] = _persist_counter.get(key, 0) + 1


def get_persist_count(
    board_id: str, *, status: str | None = None, outcome: str | None = None
) -> int:
    with _persist_counter_lock:
        total = 0
        for (b, st, oc), value in _persist_counter.items():
            if b != board_id:
                continue
            if status is not None and st != status:
                continue
            if outcome is not None and oc != outcome:
                continue
            total += value
        return total


def get_persist_counter_labels() -> tuple[str, ...]:
    return _PERSIST_LABELS


def get_persist_samples() -> list[dict[str, Any]]:
    with _persist_counter_lock:
        return [
            {"board_id": b, "status": st, "outcome": oc, "count": value}
            for (b, st, oc), value in _persist_counter.items()
        ]


def reset_persist_counter() -> None:
    with _persist_counter_lock:
        _persist_counter.clear()


# OR or_f933b2ba — kg_rebuild_terminal_report_total
_TERMINAL_LABELS = (
    "board_id",
    "candidate_terminal_status",
    "publishable_status",
    "with_report_ref",
)
_terminal_counter = runtime_state("kg.rebuild_report.terminal_counter", dict)
_terminal_counter_lock = runtime_lock("kg.rebuild_report.terminal_counter")


def _bump_terminal(
    *,
    board_id: str,
    candidate_terminal_status: str,
    publishable_status: str,
    with_report_ref: bool,
) -> None:
    key = (
        board_id,
        candidate_terminal_status,
        publishable_status,
        "true" if with_report_ref else "false",
    )
    with _terminal_counter_lock:
        _terminal_counter[key] = _terminal_counter.get(key, 0) + 1


def get_terminal_count(
    board_id: str,
    *,
    candidate_terminal_status: str | None = None,
    publishable_status: str | None = None,
    with_report_ref: bool | None = None,
) -> int:
    with _terminal_counter_lock:
        total = 0
        for (b, cts, pst, wrr), value in _terminal_counter.items():
            if b != board_id:
                continue
            if (
                candidate_terminal_status is not None
                and cts != candidate_terminal_status
            ):
                continue
            if publishable_status is not None and pst != publishable_status:
                continue
            if with_report_ref is not None:
                want = "true" if with_report_ref else "false"
                if wrr != want:
                    continue
            total += value
        return total


def get_terminal_counter_labels() -> tuple[str, ...]:
    return _TERMINAL_LABELS


def get_terminal_samples() -> list[dict[str, Any]]:
    with _terminal_counter_lock:
        return [
            {
                "board_id": b,
                "candidate_terminal_status": cts,
                "publishable_status": pst,
                "with_report_ref": wrr,
                "count": value,
            }
            for (b, cts, pst, wrr), value in _terminal_counter.items()
        ]


def reset_terminal_counter() -> None:
    with _terminal_counter_lock:
        _terminal_counter.clear()


# --- Store -------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RebuildReportStore:
    """File-backed store for rebuild reports.

    The store is the ONLY way to obtain a ``report_ref`` durable enough
    to satisfy ``KGGenerationPromotionGuard`` and
    ``RebuildReportTerminalStateGuard``. ``persist`` performs the
    sensitive-payload check first, then writes the JSON atomically.
    """

    base_dir: object | None = None
    artifact_store: RebuildAuditArtifactStore | None = None
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    @staticmethod
    def _report_key(board_id: str, report_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="rebuild_report",
            board_id=board_id,
            artifact_id=report_id,
        )

    def _new_report_id(self) -> str:
        return f"{REPORT_REF_PREFIX}{uuid.uuid4().hex}"

    def persist(self, *, payload: RebuildReportPayload) -> ReportPersistResult:
        """Persist the report. Returns ``stored`` with a fresh report_ref
        on success, ``sensitive_payload_rejected`` if a secret-shaped
        field is detected, or ``store_failed`` on any IO error."""

        board_id = payload.summary.board_id
        run_id = payload.summary.run_id
        status = payload.summary.status

        # 1. Sensitive payload check (TR7).
        try:
            persistable = payload.to_persistable_dict()
        except Exception as exc:
            logger.error(
                "kg.rebuild.report.serialize_failed board=%s run=%s err=%s",
                board_id,
                run_id,
                exc,
            )
            _bump_persist(
                board_id=board_id,
                status=status,
                outcome=ReportPersistOutcome.STORE_FAILED.value,
            )
            _bump_report(
                board_id=board_id,
                status=status,
                event=ReportEvent.FAILED.value,
            )
            return ReportPersistResult(
                outcome=ReportPersistOutcome.STORE_FAILED.value,
                report_ref=None,
                report_id=None,
                board_id=board_id,
                run_id=run_id,
                persisted_at=None,
                detail=f"serialize_exception={type(exc).__name__}",
            )

        sensitive_path = _scan_for_sensitive(persistable)
        if sensitive_path is not None:
            logger.warning(
                "kg.rebuild.report.sensitive_rejected board=%s run=%s path=%s",
                board_id,
                run_id,
                sensitive_path,
            )
            _bump_persist(
                board_id=board_id,
                status=status,
                outcome=ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value,
            )
            _bump_report(
                board_id=board_id,
                status=status,
                event=ReportEvent.FAILED.value,
            )
            return ReportPersistResult(
                outcome=ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value,
                report_ref=None,
                report_id=None,
                board_id=board_id,
                run_id=run_id,
                persisted_at=None,
                detail=f"sensitive_field={sensitive_path}",
            )

        with self._lock:
            try:
                report_id = self._new_report_id()
                persisted_at = datetime.now(timezone.utc).isoformat()
                record = {
                    "report_id": report_id,
                    "persisted_at": persisted_at,
                    **persistable,
                }
                report_key = self._report_key(board_id, report_id)
                self.artifact_store.write_json_atomic(report_key, record)
                report_ref = self.artifact_store.reference(report_key)
            except Exception as exc:
                logger.error(
                    "kg.rebuild.report.persist_failed board=%s run=%s err=%s",
                    board_id,
                    run_id,
                    exc,
                )
                _bump_persist(
                    board_id=board_id,
                    status=status,
                    outcome=ReportPersistOutcome.STORE_FAILED.value,
                )
                _bump_report(
                    board_id=board_id,
                    status=status,
                    event=ReportEvent.FAILED.value,
                )
                return ReportPersistResult(
                    outcome=ReportPersistOutcome.STORE_FAILED.value,
                    report_ref=None,
                    report_id=None,
                    board_id=board_id,
                    run_id=run_id,
                    persisted_at=None,
                    detail=f"persist_exception={type(exc).__name__}",
                )

        _bump_persist(
            board_id=board_id,
            status=status,
            outcome=ReportPersistOutcome.STORED.value,
        )
        _bump_report(
            board_id=board_id,
            status=status,
            event=ReportEvent.CREATED.value,
        )
        return ReportPersistResult(
            outcome=ReportPersistOutcome.STORED.value,
            report_ref=report_ref,
            report_id=report_id,
            board_id=board_id,
            run_id=run_id,
            persisted_at=persisted_at,
            detail=None,
        )

    def load(self, report_ref: str) -> dict[str, Any] | None:
        """Open a persisted report. Bumps the ``opened`` counter so
        operators can see drilldown traffic (br_752614b4)."""

        try:
            record = self.artifact_store.read_json_reference(report_ref)
            if record is None:
                return None
        except Exception as exc:
            logger.error(
                "kg.rebuild.report.load_failed ref=%s err=%s",
                report_ref,
                exc,
            )
            return None
        summary = record.get("summary", {})
        board_id = summary.get("board_id", "unknown")
        status = summary.get("status", "unknown")
        _bump_report(
            board_id=board_id,
            status=status,
            event=ReportEvent.OPENED.value,
        )
        return record


# --- Terminal state guard ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class RebuildReportTerminalStateGuard:
    """Pure decision function exposing ``api_1b43931d``.

    The guard does not touch storage. It receives the candidate terminal
    status the rebuild service intends to publish and the outcome from
    ``RebuildReportStore.persist``; it returns the canonical
    ``publishable_status`` plus the promotion gate state. The rebuild
    service uses the decision to choose between completing the run
    cleanly, rolling back to the previous safe generation, or surfacing
    a retry/redact operator action.
    """

    @staticmethod
    def require_report_ref(
        *,
        board_id: str,
        run_id: str,
        candidate_terminal_status: str,
        report_persist_result: ReportPersistResult,
        previous_kg_generation_id: str | None,
        kg_generation_id: str | None,
    ) -> TerminalGuardDecision:
        if candidate_terminal_status not in TERMINAL_STATUSES_REQUIRING_REPORT:
            raise ValueError(f"invalid_terminal_status: {candidate_terminal_status!r}")

        outcome = report_persist_result.outcome
        has_ref = isinstance(report_persist_result.report_ref, str) and bool(
            report_persist_result.report_ref.strip()
        )

        if outcome == ReportPersistOutcome.STORED.value and has_ref:
            publishable = candidate_terminal_status
            promotion_allowed = candidate_terminal_status in {
                "completed",
                "partially_rebuilt",
                "rolled_back",
            }
            operator_action: str | None = None
        elif outcome == ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value:
            publishable = "report_persist_failed"
            promotion_allowed = False
            operator_action = "redact_payload_and_retry"
        else:
            # store_failed OR (stored but ref empty -> shouldn't happen)
            publishable = "report_persist_failed"
            promotion_allowed = False
            operator_action = "retry_report_persist"

        _bump_terminal(
            board_id=board_id,
            candidate_terminal_status=candidate_terminal_status,
            publishable_status=publishable,
            with_report_ref=has_ref and publishable == candidate_terminal_status,
        )

        return TerminalGuardDecision(
            publishable_status=publishable,
            report_ref_required=True,
            promotion_allowed=promotion_allowed,
            operator_action=operator_action,
        )


def list_sensitive_key_pattern_strings() -> tuple[str, ...]:
    return tuple(pat.pattern for pat in SENSITIVE_KEY_PATTERNS)


__all__ = [
    "RebuildReportPayload",
    "RebuildReportStore",
    "RebuildReportSummary",
    "RebuildReportTerminalStateGuard",
    "REPORTS_DIRNAME",
    "REPORT_REF_PREFIX",
    "REBUILD_DIRNAME",
    "ReportEvent",
    "ReportPersistOutcome",
    "ReportPersistResult",
    "SENSITIVE_KEY_PATTERNS",
    "TERMINAL_STATUSES_REQUIRING_REPORT",
    "TerminalGuardDecision",
    "get_persist_count",
    "get_persist_counter_labels",
    "get_persist_samples",
    "get_report_count",
    "get_report_counter_labels",
    "get_report_samples",
    "get_terminal_count",
    "get_terminal_counter_labels",
    "get_terminal_samples",
    "list_sensitive_key_pattern_strings",
    "reset_persist_counter",
    "reset_report_counter",
    "reset_terminal_counter",
]
