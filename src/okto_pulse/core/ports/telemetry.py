"""Telemetry ports (spec R10-A, IMP1) — PURE contracts + DTOs for the telemetry
boundary, so the sender / store / product-aggregation can move to Community
behind a port in R10-B/C/D WITHOUT the core knowing the concrete persistence
(``LocalTelemetryStore``), transport (``requests`` / ``sqlite3``) or aggregator
(``ProductTelemetryAggregator``).

PURITY (TS01): this module imports stdlib ``dataclasses`` / ``typing`` /
``datetime`` ONLY. It does NOT import ``TelemetryService`` / ``LocalTelemetryStore``
/ ``ProductTelemetryAggregator`` / ``requests`` / ``sqlite3`` — importing
``okto_pulse.core.ports`` never loads the telemetry runtime.

The DTOs MIRROR the current serialization bit-for-bit (TS02):
  - ``TelemetryResult``  <-> ``TelemetryService.record_event`` return dict;
  - ``HealthReport``     <-> ``PublishHealth.to_dict`` (PUBLISH_HEALTH_FIELDS);
  - ``TelemetryState``   <-> the redacted consent/state VIEW surface;
  - ``ProductState``     <-> ``ProductTelemetryAggregator.aggregate`` output.

``resolve_publish_health`` stays the PURE resolver in ``telemetry.publish_health``
(it already takes the local/install/aws/report sources by contract and never
reads the FS/AWS directly); :class:`PublishHealthSource` is the port contract for
those source signals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable

# --- Product-aggregate family vocabulary (R10-D) -----------------------------
#: The CLOSED set of product-aggregate metric families. A PURE contract (no
#: sqlite3 / database_url / filesystem / Community) shared by concrete
#: aggregator adapters and the consumers
#: (``TelemetryService.summary``), so the families are known WITHOUT importing the
#: aggregator runtime. Bounded categorical counts only — never an id/title/payload.
PRODUCT_METRIC_KEYS: frozenset[str] = frozenset(
    {
        "product_feature_usage_counts",
        "product_flow_origin_counts",
        "product_flow_completion_counts",
        "product_work_item_type_counts",
        "product_workflow_stage_counts",
        "product_quality_signal_counts",
        "product_advanced_capability_counts",
    }
)

#: Deterministic ordered projection of :data:`PRODUCT_METRIC_KEYS`.
PRODUCT_AGGREGATE_FAMILIES: tuple[str, ...] = tuple(sorted(PRODUCT_METRIC_KEYS))

# --- DTOs (mirror the live serialization) -----------------------------------

#: Field order of the publish-health DTO — kept identical to
#: ``telemetry.publish_health.PUBLISH_HEALTH_FIELDS`` (the gate asserts parity).
HEALTH_REPORT_FIELDS: tuple[str, ...] = (
    "status",
    "source",
    "severity",
    "reason_code",
    "reason_category",
    "http_status",
    "last_success_at",
    "last_failure_at",
    "next_retry_at",
    "retry_count",
    "freshness",
    "install_id_redacted",
    "message",
    "sources",
    "redaction_applied",
)


@dataclass(frozen=True)
class TelemetryResult:
    """Mirror of ``TelemetryService.record_event`` — ``file`` / ``event_id`` are
    present ONLY on a written event (absent on disabled / schema-reject), exactly
    like the live dict."""

    written: bool
    mode: str
    schema_version: str
    rejected_fields_count: int = 0
    file: str | None = None
    event_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"written": self.written, "mode": self.mode}
        if self.file is not None:
            out["file"] = self.file
        if self.event_id is not None:
            out["event_id"] = self.event_id
        out["rejected_fields_count"] = self.rejected_fields_count
        out["schema_version"] = self.schema_version
        return out

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TelemetryResult":
        return cls(
            written=bool(d["written"]),
            mode=str(d["mode"]),
            schema_version=str(d["schema_version"]),
            rejected_fields_count=int(d.get("rejected_fields_count", 0)),
            file=d.get("file"),
            event_id=d.get("event_id"),
        )


@dataclass(frozen=True)
class TelemetryState:
    """Mirror of the persisted consent/state surface (the allowlisted, redacted
    view — NEVER install_token / token_hash)."""

    mode: str | None = None
    source: str | None = None
    changed_at: str | None = None
    policy_version: str | None = None
    schema_version: str | None = None
    normalized_from: str | None = None
    next_opt_in_prompt_after: str | None = None
    acknowledged_items: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "source": self.source,
            "changed_at": self.changed_at,
            "policy_version": self.policy_version,
            "schema_version": self.schema_version,
            "normalized_from": self.normalized_from,
            "next_opt_in_prompt_after": self.next_opt_in_prompt_after,
            "acknowledged_items": list(self.acknowledged_items),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TelemetryState":
        return cls(
            mode=d.get("mode"),
            source=d.get("source"),
            changed_at=d.get("changed_at"),
            policy_version=d.get("policy_version"),
            schema_version=d.get("schema_version"),
            normalized_from=d.get("normalized_from"),
            next_opt_in_prompt_after=d.get("next_opt_in_prompt_after"),
            acknowledged_items=tuple(d.get("acknowledged_items") or ()),
        )


@dataclass(frozen=True)
class HealthReport:
    """Mirror of ``PublishHealth.to_dict`` — the redacted publish-health surface."""

    status: str
    source: str
    severity: str
    reason_category: str
    message: str
    reason_code: str | None = None
    http_status: int | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    next_retry_at: str | None = None
    retry_count: int = 0
    freshness: dict[str, Any] = field(default_factory=dict)
    install_id_redacted: str | None = None
    sources: list[dict[str, Any]] = field(default_factory=list)
    redaction_applied: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in HEALTH_REPORT_FIELDS}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "HealthReport":
        return cls(**{name: d[name] for name in HEALTH_REPORT_FIELDS if name in d})


@dataclass(frozen=True)
class ProductState:
    """Mirror of ``ProductTelemetryAggregator.aggregate`` — bounded categorical
    counts per family (never an id/title/payload)."""

    metrics: dict[str, dict[str, int]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, dict[str, int]]:
        return {k: dict(v) for k, v in self.metrics.items()}

    @classmethod
    def from_dict(cls, d: dict[str, dict[str, int]]) -> "ProductState":
        return cls(metrics={k: dict(v) for k, v in (d or {}).items()})


# --- Protocol contracts ------------------------------------------------------


@runtime_checkable
class TelemetrySink(Protocol):
    """The delta/snapshot transmit boundary (today: ``TelemetrySender``). The
    concrete adapter owns ``requests`` / circuit-breaker / install-token; this
    port never references them."""

    def send_pending(self) -> dict[str, Any]:
        """Transmit pending batches; return a bounded, secret-free result."""
        ...

    def publish_product_snapshot(self) -> dict[str, Any]:
        """Persist/transmit the product snapshot; bounded result."""
        ...


@runtime_checkable
class TelemetryStateStore(Protocol):
    """Local CONSENT/STATE view boundary ONLY.

    This narrow DTO port is intentionally NOT the full persisted
    ``state.json`` carrier: it excludes install-token, watermark,
    failure-state, beacon, schema and unknown blocks. R12 keeps it as a
    redacted view contract so no full-dict state can be truncated into this DTO.
    It also does NOT own telemetry EVENT persistence (that is
    :class:`TelemetryEventStore`)."""

    def load_state(self) -> TelemetryState:
        """Load the persisted (redacted) consent/state surface."""
        ...

    def save_state(self, state: TelemetryState) -> None:
        """Persist the consent/state surface."""
        ...


@runtime_checkable
class TelemetryStateCarrier(Protocol):
    """Full-dict telemetry state carrier for ``metrics_dir/state.json``.

    The concrete adapter is edition-owned (Community for the local edition) and
    must preserve every existing key, including unknown fields, migration
    notices, history, watermark, failure_state, install-token lifecycle and
    beacon/schema fields. Public DTOs still use the redacted view contracts.
    """

    def load_state(self, metrics_dir: Path) -> dict[str, Any]:
        """Load the complete persisted telemetry state dictionary."""
        ...

    def save_state(self, metrics_dir: Path, state: dict[str, Any]) -> None:
        """Persist the complete telemetry state dictionary atomically."""
        ...


@runtime_checkable
class TelemetryEventStore(Protocol):
    """Local append-only telemetry EVENT persistence boundary (today:
    ``LocalTelemetryStore``). The concrete adapter owns the JSONL layout
    (``metrics_dir/{events,sent,failures,exports,snapshots}``), the
    confirmation-ledger, retention/prune, and the ``metrics_dir`` path-guard
    (``PATH_OUTSIDE_METRICS_DIR``); this port never references the FS itself.

    R10-B separates EVENT persistence (here) from CONSENT/STATE persistence
    (:class:`TelemetryStateStore`) so the store can move to the Community edition
    behind this contract without the core knowing the concrete layout."""

    def append_event(self, event: dict[str, Any]) -> Path:
        """Append a closed-schema event (filed by ``occurred_at`` date)."""
        ...

    def append_sent(self, record: dict[str, Any], *, failed: bool = False) -> Path:
        """Append a sent/confirmation (or, with ``failed=True``, a failure) record."""
        ...

    def append_snapshot(self, record: dict[str, Any]) -> Path:
        """Append a product-telemetry snapshot record (local, append-only)."""
        ...

    def confirmed_event_ids(self) -> set[str]:
        """The set of backend-confirmed local ``event_id``s (durable ledger)."""
        ...

    def iter_events(self, *, since: datetime | None = None) -> Iterable[dict[str, Any]]:
        """Iterate persisted events (optionally only those at/after ``since``)."""
        ...

    def summarize(self, *, window_days: int = 30) -> dict[str, Any]:
        """Aggregate the bounded local event summary over a window."""
        ...

    def prune_old(self, *, now: datetime | None = None) -> dict[str, int]:
        """Retention sweep: drop confirmed-and-old, preserve pending."""
        ...

    def export_local(self, output_path: Path | None = None) -> Path:
        """Export the local events to a JSONL file (within ``metrics_dir``)."""
        ...

    def purge_local(self) -> dict[str, int]:
        """Purge the local event/sent/failure/export files. Returns counts."""
        ...


@runtime_checkable
class PublishHealthSource(Protocol):
    """A single publish-health SIGNAL source (local / install_lifecycle /
    aws_ingest / report_athena). The pure ``resolve_publish_health`` classifier
    consumes these by contract — it never reads the FS/AWS itself."""

    @property
    def name(self) -> str: ...

    @property
    def available(self) -> bool: ...

    def signal(self) -> dict[str, Any]:
        """Return the bounded, secret-free source signal (status/timestamps)."""
        ...


@runtime_checkable
class ProductAggregationPort(Protocol):
    """The product-aggregation boundary (today: ``ProductTelemetryAggregator``).
    The concrete adapter owns ``sqlite3`` + the schema introspection; this port
    never references them."""

    def aggregate(self) -> ProductState:
        """Aggregate bounded categorical product metrics from the local store."""
        ...


@runtime_checkable
class TelemetryPort(Protocol):
    """The high-level telemetry facade (today: ``TelemetryService``). Records
    events under the CLOSED schema and surfaces summary / publish-health DTOs."""

    def record_event(
        self, event_type: str, payload: dict[str, Any] | None = None
    ) -> TelemetryResult:
        """Record a closed-schema event; return the bounded result DTO."""
        ...

    def summary(self, *, window_days: int = 30) -> dict[str, Any]:
        """Return the (redacted) telemetry summary surface."""
        ...

    def publish_health(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Return the (redacted) publish-health surface."""
        ...


__all__ = [
    "HEALTH_REPORT_FIELDS",
    "PRODUCT_METRIC_KEYS",
    "PRODUCT_AGGREGATE_FAMILIES",
    "TelemetryResult",
    "TelemetryState",
    "HealthReport",
    "ProductState",
    "TelemetrySink",
    "TelemetryStateStore",
    "TelemetryStateCarrier",
    "TelemetryEventStore",
    "PublishHealthSource",
    "ProductAggregationPort",
    "TelemetryPort",
]
