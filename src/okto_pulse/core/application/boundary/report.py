"""Deterministic GateReport contract for the core-pure boundary audit suite.

Spec #12, tr_c7a5949e + contract ``api_8c46772c``: every boundary gate returns a
``GateReport`` with a typed ``exception`` schema. An exception whose deadline is
past (or whose status is ``expired``/``revoked``) must NOT be silently treated as
``passed``/``baseline``/``xfail_advisory`` — it has to produce ``reject`` (or
``blocking``). See scenario ``ts_8dc314aa``.

Pure stdlib (``dataclasses`` + ``datetime``); no runtime/transport imports — this
module is conformance tooling, importable in an isolated environment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal

GateStatus = Literal["passed", "baseline", "xfail_advisory", "blocking", "reject"]
GateSeverity = Literal["low", "medium", "high", "critical"]
GateExceptionStatus = Literal["active", "expired", "revoked"]

#: Statuses that an expired/revoked exception must be forced to.
_NON_PROMOTABLE_STATUS: GateStatus = "reject"
#: Statuses that still count as "the gate let the violation through".
_PROMOTED_STATUSES: frozenset[str] = frozenset({"passed", "baseline", "xfail_advisory"})


@dataclass(frozen=True)
class GateException:
    """A typed, time-bounded waiver for a known violation.

    ``deadline`` is an ISO-8601 date (``YYYY-MM-DD``). An exception is only
    honoured while ``status == "active"`` AND the deadline has not passed.
    """

    owner: str
    justification: str
    deadline: str
    promotion_criteria: str
    status: GateExceptionStatus = "active"

    def is_expired(self, today: date) -> bool:
        """True when this exception can no longer waive a violation."""
        if self.status in ("expired", "revoked"):
            return True
        try:
            return date.fromisoformat(self.deadline) < today
        except (ValueError, TypeError):
            # An unparseable deadline is treated as expired — fail closed.
            return True

    def as_dict(self) -> dict[str, Any]:
        return {
            "owner": self.owner,
            "justification": self.justification,
            "deadline": self.deadline,
            "promotion_criteria": self.promotion_criteria,
            "status": self.status,
        }


@dataclass(frozen=True)
class GateReport:
    """Deterministic result of a single boundary gate (contract api_8c46772c)."""

    gate_id: str
    subject: str
    status: GateStatus
    severity: GateSeverity = "medium"
    owner: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    exception: GateException | None = None
    promotion_criteria: str | None = None
    observed_value: Any = None
    expected_value: Any = None
    remediation_hint: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Stable, JSON-serialisable projection with sorted evidence keys."""
        return {
            "gate_id": self.gate_id,
            "subject": self.subject,
            "status": self.status,
            "severity": self.severity,
            "owner": self.owner,
            "evidence": {k: self.evidence[k] for k in sorted(self.evidence)},
            "exception": self.exception.as_dict() if self.exception else None,
            "promotion_criteria": self.promotion_criteria,
            "observed_value": self.observed_value,
            "expected_value": self.expected_value,
            "remediation_hint": self.remediation_hint,
        }


def enforce_exception_policy(report: GateReport, *, today: date) -> GateReport:
    """Reject a report whose waiver is expired (ts_8dc314aa / tr_c7a5949e).

    When a report carries an exception that is expired/revoked AND the report was
    about to be promoted (``passed``/``baseline``/``xfail_advisory``), the status
    is forced to ``reject`` with reason ``expired_exception``. Owner, deadline,
    promotion_criteria and remediation_hint are preserved in the report.
    """
    exc = report.exception
    if exc is None or not exc.is_expired(today):
        return report
    if report.status not in _PROMOTED_STATUSES:
        return report
    evidence = dict(report.evidence)
    evidence["expired_exception"] = {
        "reason": "expired_exception",
        "deadline": exc.deadline,
        "exception_status": exc.status,
        "previous_status": report.status,
    }
    return GateReport(
        gate_id=report.gate_id,
        subject=report.subject,
        status=_NON_PROMOTABLE_STATUS,
        severity="high" if report.severity in ("low", "medium") else report.severity,
        owner=report.owner or exc.owner,
        evidence=evidence,
        exception=exc,
        promotion_criteria=report.promotion_criteria or exc.promotion_criteria,
        observed_value=report.observed_value,
        expected_value=report.expected_value,
        remediation_hint=(
            report.remediation_hint
            or "Exception deadline expired; renew the waiver with a new deadline or "
            "remediate the underlying violation before promotion."
        ),
    )
