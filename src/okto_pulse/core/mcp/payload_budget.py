"""MCPPayloadBudgetScanner + MCPProjectionTelemetrySink (spec R5, card R5.2).

Shared contract SC4: R5 owns the SINGLE budget-lint framework for R1–R5 token
optimizations. It is **deterministic and char-based** — no LLM call, no network
call, no tokenizer/token estimate (BR br_e0237e4c, decisions dec_26386a86 /
dec_fa3cf8d6 / dec_4f3d28c1). R1 tool-description scanning is a *profile* of this
scanner, never a parallel token-based gate.

Concrete first R5 budget profile (br_6c2dd1a4 / api_d70929a6 / TR tr_39c8d7ef):
    per_tool_description_chars     <= 900
    aggregate_tool_description_chars <= 70000
    always_loaded_instruction_chars <= 8000
    + fixture-specific payload budgets declared in a checked manifest.

Every violation reports ``path``, ``actual_chars``, ``budget_chars``, ``reason``,
``budget_profile`` and ``unit="chars"`` — never ``actual_tokens``/``max_tokens``.

The ``MCPProjectionTelemetrySink`` records SAFE aggregate telemetry only —
profile, truncation, omitted/deduped counts, payload_bytes, legacy/full usage,
budget status — and rejects any label carrying a payload body, board content,
query text, email, PII fragment, or raw JSON/XML (BR br_1cb47bea, TR tr_7711d521,
or_7f86b398). It fails closed.

NOTE: the live MCP surface currently exceeds these target budgets — that is the
intended "work remaining" signal (R1–R4 trim the surface). Pass/fail is therefore
exercised against fixtures; live-surface scans are measurement/structure only
until the optimizations land. R5.2 is additive and does NOT alter R3.2's
``mcp_quickwin_compaction_total`` emission.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from okto_pulse.core.mcp.projection_envelope import ENVELOPE_METADATA_KEYS

BUDGET_UNIT = "chars"

# Metric names. Canonical SC4 budget metric is ``mcp_budget_violation_total``
# (or_789086d5); ``mcp_payload_budget_violation_total`` (or_e5300338) is kept as
# a compatibility alias. Projection metrics are or_13c4906b / or_6afaf91c /
# or_7f86b398.
METRIC_BUDGET_VIOLATION = "mcp_budget_violation_total"
METRIC_PAYLOAD_BUDGET_VIOLATION = "mcp_payload_budget_violation_total"
METRIC_PROJECTION_RESPONSE = "mcp_projection_response_total"
METRIC_PROJECTION_LEGACY_FULL = "mcp_projection_legacy_full_usage_total"
METRIC_UNSAFE_LABEL_REJECTED = "mcp_projection_unsafe_label_rejected_total"
# Context-projection observability (R2.2, fr_610801cc / or_*): a usage counter and
# a payload-bytes accumulator for the high-frequency get_*_context responses. They
# reuse the SAFE_PROJECTION_LABEL_KEYS gate — counts + identifiers only, no body.
METRIC_CONTEXT_PROJECTION_USAGE = "mcp_context_projection_usage_total"
METRIC_CONTEXT_PROJECTION_BYTES = "mcp_context_projection_payload_bytes"
# copy_architecture_to_card response observability (R2.3, or_a51c51b4): usage +
# serialized-bytes of the compacted copy response. Counts only — never a body.
METRIC_COPY_ARCHITECTURE_USAGE = "mcp_copy_architecture_response_total"
METRIC_COPY_ARCHITECTURE_BYTES = "mcp_copy_architecture_response_bytes"

UNSAFE_LABEL_REASON = "unsafe_label_detected"

# Fail-closed label safety: telemetry accepts ONLY these allowlisted label keys
# per surface; everything else is rejected. The allowlist is the primary gate;
# the denylist is belt-and-suspenders for semantically-forbidden keys that must
# never carry board content (br_1cb47bea / tr_7711d521 / or_7f86b398).
SAFE_PROJECTION_LABEL_KEYS = frozenset(
    {
        # api_71a31d9f request shape: tool_name + profile + payload_bytes +
        # truncated + omitted/deduped counts + outcome (or_13c4906b requires
        # measuring outcome on every projected response).
        "tool_name",
        "profile",
        "outcome",
        "truncated",
        "truncation_reason",
        "omitted_count",
        "deduped_count",
        "payload_bytes",
        "legacy_full",
        "tool_family",
    }
)
SAFE_BUDGET_LABEL_KEYS = frozenset(
    {
        "path",
        "reason",
        "budget_profile",
        "actual_chars",
        "budget_chars",
        "unit",
        "budget_status",
        "tool_family",
    }
)
# Keys that, by name alone, signal board content / payload body / query / PII and
# must be rejected even when their value looks short and format-safe.
FORBIDDEN_LABEL_KEYS = frozenset(
    {
        "payload",
        "body",
        "query",
        "content",
        "description",
        "title",
        "text",
        "comment",
        "answer",
        "email",
        "user_email",
        "raw",
        "json",
        "xml",
        "board_content",
        "message",
        "prompt",
        "value",
        "name",
        "question",
    }
)


# ---------------------------------------------------------------------------
# Budget profile + violation/report models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BudgetProfile:
    """A checked, concrete char-budget profile (br_6c2dd1a4)."""

    name: str = "r5_default"
    per_tool_description_chars: int = 900
    aggregate_tool_description_chars: int = 60000
    always_loaded_instruction_chars: int = 8000
    # Optional per-resource budget; None = resources measured but not gated.
    per_resource_chars: int | None = None
    # Fixture-specific payload budgets: {fixture_path: char_budget}.
    payload_budgets: Mapping[str, int] = field(default_factory=dict)


DEFAULT_BUDGET_PROFILE = BudgetProfile()

# R1 ``tool_descriptions`` budget profile (card R1.2, api_b37a6a75, dec_71488495).
# R1 is a PROFILE of this shared scanner — NOT a parallel token-based scanner.
# per_tool remains strict; aggregate is the reviewed post-R1 additive surface.
# always_loaded_
# instruction_chars was renegotiated from 8000 to 10000 by an explicit owner
# decision (recorded on the R1.1 card) prioritising agent assertiveness — it is
# formalised HERE in the shared, version-controlled profile contract, not only in
# a local test/comment.
TOOL_DESCRIPTIONS_BUDGET_PROFILE = BudgetProfile(
    name="tool_descriptions",
    per_tool_description_chars=900,
    aggregate_tool_description_chars=70000,
    always_loaded_instruction_chars=10000,
)


def load_budget_profile(
    manifest: Mapping[str, Any] | str | Path, *, name: str | None = None
) -> BudgetProfile:
    """Load a budget profile from a checked manifest (dict or JSON file path)."""
    if isinstance(manifest, (str, Path)):
        data = json.loads(Path(manifest).read_text(encoding="utf-8"))
    else:
        data = dict(manifest)
    return BudgetProfile(
        name=name or str(data.get("name", "custom")),
        per_tool_description_chars=int(data.get("per_tool_description_chars", 900)),
        aggregate_tool_description_chars=int(
            data.get("aggregate_tool_description_chars", 60000)
        ),
        always_loaded_instruction_chars=int(
            data.get("always_loaded_instruction_chars", 8000)
        ),
        per_resource_chars=data.get("per_resource_chars"),
        payload_budgets=dict(data.get("payload_budgets", {})),
    )


@dataclass
class BudgetViolation:
    path: str
    actual_chars: int
    budget_chars: int
    reason: str
    budget_profile: str
    unit: str = BUDGET_UNIT

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ScanReport:
    passed: bool
    violations: list[BudgetViolation]
    measurements: dict[str, int]
    budget_profile: str
    unit: str = BUDGET_UNIT

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "unit": self.unit,
            "budget_profile": self.budget_profile,
            "violations": [v.to_dict() for v in self.violations],
            "measurements": dict(self.measurements),
        }


def _chars(content: Any) -> int:
    """Deterministic character count. Strings count directly; other JSON-able
    content counts its stable serialization length (chars, NOT tokens)."""
    if isinstance(content, str):
        return len(content)
    return len(
        json.dumps(
            content, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False
        )
    )


def _envelope_missing_keys(response: Any) -> list[str]:
    if not isinstance(response, Mapping):
        return list(ENVELOPE_METADATA_KEYS)
    return [k for k in ENVELOPE_METADATA_KEYS if k not in response]


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------


class MCPPayloadBudgetScanner:
    """Deterministic char-based budget scanner over the MCP token surfaces."""

    def __init__(self, profile: BudgetProfile = DEFAULT_BUDGET_PROFILE) -> None:
        self.profile = profile

    def scan(
        self,
        *,
        tool_descriptions: Mapping[str, str] | None = None,
        instructions: Mapping[str, str] | str | None = None,
        resources: Mapping[str, str] | None = None,
        payload_fixtures: Mapping[str, Any] | None = None,
        envelope_responses: Mapping[str, Any] | None = None,
    ) -> ScanReport:
        p = self.profile
        violations: list[BudgetViolation] = []
        measurements: dict[str, int] = {}

        if tool_descriptions:
            aggregate = 0
            for name in sorted(tool_descriptions):
                count = _chars(tool_descriptions[name])
                aggregate += count
                if count > p.per_tool_description_chars:
                    violations.append(
                        BudgetViolation(
                            path=f"tool:{name}",
                            actual_chars=count,
                            budget_chars=p.per_tool_description_chars,
                            reason="per_tool_description_over_budget",
                            budget_profile=p.name,
                        )
                    )
            measurements["aggregate_tool_description_chars"] = aggregate
            measurements["tool_count"] = len(tool_descriptions)
            if aggregate > p.aggregate_tool_description_chars:
                violations.append(
                    BudgetViolation(
                        path="tools:aggregate",
                        actual_chars=aggregate,
                        budget_chars=p.aggregate_tool_description_chars,
                        reason="aggregate_tool_description_over_budget",
                        budget_profile=p.name,
                    )
                )

        if instructions:
            items = (
                instructions.items()
                if isinstance(instructions, Mapping)
                else [("instructions", instructions)]
            )
            for path, text in sorted(items):
                count = _chars(text)
                measurements[f"instruction_chars:{path}"] = count
                if count > p.always_loaded_instruction_chars:
                    violations.append(
                        BudgetViolation(
                            path=f"instruction:{path}",
                            actual_chars=count,
                            budget_chars=p.always_loaded_instruction_chars,
                            reason="always_loaded_instruction_over_budget",
                            budget_profile=p.name,
                        )
                    )

        if resources and p.per_resource_chars is not None:
            for path in sorted(resources):
                count = _chars(resources[path])
                if count > p.per_resource_chars:
                    violations.append(
                        BudgetViolation(
                            path=f"resource:{path}",
                            actual_chars=count,
                            budget_chars=p.per_resource_chars,
                            reason="resource_over_budget",
                            budget_profile=p.name,
                        )
                    )

        if payload_fixtures:
            for path in sorted(payload_fixtures):
                budget = p.payload_budgets.get(path)
                if budget is None:
                    continue  # no declared budget → not gated
                count = _chars(payload_fixtures[path])
                measurements[f"payload_chars:{path}"] = count
                if count > budget:
                    violations.append(
                        BudgetViolation(
                            path=f"payload:{path}",
                            actual_chars=count,
                            budget_chars=int(budget),
                            reason="payload_fixture_over_budget",
                            budget_profile=p.name,
                        )
                    )

        if envelope_responses:
            for path in sorted(envelope_responses):
                missing = _envelope_missing_keys(envelope_responses[path])
                if missing:
                    violations.append(
                        BudgetViolation(
                            path=f"envelope:{path}",
                            actual_chars=0,
                            budget_chars=0,
                            reason="envelope_noncompliant_missing:" + ",".join(missing),
                            budget_profile=p.name,
                        )
                    )

        return ScanReport(
            passed=not violations,
            violations=violations,
            measurements=measurements,
            budget_profile=p.name,
        )


# Live MCP registry snapshot helpers (ir_0e6ac2bc). Deterministic, no LLM.


def snapshot_tool_descriptions(mcp: Any) -> dict[str, str]:
    tools = getattr(getattr(mcp, "_tool_manager", None), "_tools", {}) or {}
    return {name: (getattr(tool, "description", "") or "") for name, tool in tools.items()}


def snapshot_instructions(server_module: Any) -> dict[str, str]:
    return {"agent_instructions.md": server_module._load_instructions()}


def snapshot_resources(server_module: Any) -> dict[str, str]:
    """(R11-A) Snapshot the EFFECTIVE resource catalog (core + Community-injected)
    via each spec's deterministic loader — the catalog is the authority, not the
    transitional ``_RESOURCE_REGISTRY`` projection."""
    out: dict[str, str] = {}
    for spec in server_module.effective_resource_catalog().specs():
        key = spec.path if spec.path is not None else spec.uri
        out[key] = spec.read()
    return out


# ---------------------------------------------------------------------------
# Safe telemetry sink
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9_.:/\-]+$")
# Characters that strongly indicate a body/JSON/XML/PII fragment leaked in.
_UNSAFE_CHARS = ("{", "}", "<", ">", '"', "'", "\n", "\t", " ")
_MAX_SAFE_LABEL_LEN = 120


def is_safe_label_value(value: Any) -> bool:
    """Fail-closed safety check for a telemetry label key or value."""
    if isinstance(value, bool) or isinstance(value, int) or value is None:
        return True
    if isinstance(value, float):
        return True
    if not isinstance(value, str):
        return False
    if len(value) > _MAX_SAFE_LABEL_LEN:
        return False
    if _EMAIL_RE.search(value):
        return False
    if any(ch in value for ch in _UNSAFE_CHARS):
        return False
    return bool(_SAFE_LABEL_RE.match(value))


def projection_labels_from_envelope(
    envelope: Mapping[str, Any], *, tool_name: str | None = None
) -> dict[str, Any]:
    """Build the SAFE projection telemetry labels from an R5.1 envelope — counts
    and identifiers only, never a body field.

    Propagates the canonical ``outcome`` so or_13c4906b can measure it on every
    projected response. ``tool_name`` (api_71a31d9f) is attached when the
    producer supplies it (the envelope helper itself is tool-agnostic)."""
    profile = str(envelope.get("profile", ""))
    labels: dict[str, Any] = {
        "profile": profile,
        "outcome": str(envelope.get("outcome", "")),
        "truncated": bool(envelope.get("truncated", False)),
        "omitted_count": int(envelope.get("omitted_count", 0) or 0),
        "deduped_count": int(envelope.get("deduped_count", 0) or 0),
        "payload_bytes": int(envelope.get("payload_bytes", 0) or 0),
        "legacy_full": profile in ("full", "legacy"),
    }
    if tool_name is not None:
        labels["tool_name"] = str(tool_name)
    return labels


@dataclass
class TelemetryResult:
    accepted: bool
    reason: str | None = None
    metric: str | None = None


class MCPProjectionTelemetrySink:
    """In-memory/log-backed sink that records SAFE aggregate telemetry only and
    rejects unsafe labels fail-closed (api_71a31d9f / br_1cb47bea)."""

    def __init__(self) -> None:
        self.metrics: dict[str, int] = {}
        self.events: list[dict[str, Any]] = []

    def _inc(self, name: str, amount: int = 1) -> None:
        self.metrics[name] = self.metrics.get(name, 0) + amount

    @staticmethod
    def _first_unsafe_label(
        labels: Mapping[str, Any], allowed: frozenset[str]
    ) -> str | None:
        """Return the first label key that is unsafe.

        Fail-closed: a key is unsafe if it is semantically forbidden, NOT in the
        per-surface allowlist, or carries a value whose format leaks content.
        """
        for key, value in labels.items():
            key_str = str(key)
            if key_str.lower() in FORBIDDEN_LABEL_KEYS:
                return key_str
            if key_str not in allowed:
                return key_str
            if not is_safe_label_value(key) or not is_safe_label_value(value):
                return key_str
        return None

    def record_projection(self, labels: Mapping[str, Any]) -> TelemetryResult:
        unsafe = self._first_unsafe_label(labels, SAFE_PROJECTION_LABEL_KEYS)
        if unsafe is not None:
            self._inc(METRIC_UNSAFE_LABEL_REJECTED)
            return TelemetryResult(
                False, reason=UNSAFE_LABEL_REASON, metric=METRIC_UNSAFE_LABEL_REJECTED
            )
        self.events.append({"type": "projection", **dict(labels)})
        self._inc(METRIC_PROJECTION_RESPONSE)
        if bool(labels.get("legacy_full")) or str(labels.get("profile")) in (
            "full",
            "legacy",
        ):
            self._inc(METRIC_PROJECTION_LEGACY_FULL)
        return TelemetryResult(True)

    def record_projection_envelope(
        self, envelope: Mapping[str, Any], *, tool_name: str | None = None
    ) -> TelemetryResult:
        return self.record_projection(
            projection_labels_from_envelope(envelope, tool_name=tool_name)
        )

    def record_context_projection(self, labels: Mapping[str, Any]) -> TelemetryResult:
        """Record a get_*_context projection (R2.2). Fail-closed on unsafe labels,
        then bump ``mcp_context_projection_usage_total`` and accumulate
        ``mcp_context_projection_payload_bytes`` from the envelope size. Counts +
        identifiers only — never a body field (same gate as ``record_projection``)."""
        unsafe = self._first_unsafe_label(labels, SAFE_PROJECTION_LABEL_KEYS)
        if unsafe is not None:
            self._inc(METRIC_UNSAFE_LABEL_REJECTED)
            return TelemetryResult(
                False, reason=UNSAFE_LABEL_REASON, metric=METRIC_UNSAFE_LABEL_REJECTED
            )
        self.events.append({"type": "context_projection", **dict(labels)})
        self._inc(METRIC_CONTEXT_PROJECTION_USAGE)
        self._inc(
            METRIC_CONTEXT_PROJECTION_BYTES, int(labels.get("payload_bytes", 0) or 0)
        )
        return TelemetryResult(True)

    def record_context_projection_envelope(
        self, envelope: Mapping[str, Any], *, tool_name: str | None = None
    ) -> TelemetryResult:
        return self.record_context_projection(
            projection_labels_from_envelope(envelope, tool_name=tool_name)
        )

    def record_budget_violation(
        self, violation: BudgetViolation | Mapping[str, Any]
    ) -> TelemetryResult:
        raw = violation.to_dict() if isinstance(violation, BudgetViolation) else dict(violation)
        labels = {
            "path": raw.get("path"),
            "reason": raw.get("reason"),
            "budget_profile": raw.get("budget_profile"),
            "actual_chars": int(raw.get("actual_chars", 0) or 0),
            "budget_chars": int(raw.get("budget_chars", 0) or 0),
            "unit": raw.get("unit", BUDGET_UNIT),
        }
        unsafe = self._first_unsafe_label(labels, SAFE_BUDGET_LABEL_KEYS)
        if unsafe is not None:
            self._inc(METRIC_UNSAFE_LABEL_REJECTED)
            return TelemetryResult(
                False, reason=UNSAFE_LABEL_REASON, metric=METRIC_UNSAFE_LABEL_REJECTED
            )
        self.events.append({"type": "budget", **labels})
        self._inc(METRIC_BUDGET_VIOLATION)
        self._inc(METRIC_PAYLOAD_BUDGET_VIOLATION)  # compatibility alias
        return TelemetryResult(True)
