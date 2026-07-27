"""TelemetryEventSchemaGate (spec R10-A, IMP2) — enforce the CLOSED telemetry
event vocabulary + payload-key allowlist so a ``record_event`` call cannot
introduce a PHANTOM schema (an event_type or payload key outside the contract),
while the existing anti-exfiltration redaction (``sanitize_payload``) is the
single source of truth and is NOT duplicated here.

This gate lives in the ``telemetry`` package (NOT ``application/boundary``)
because it consumes the closed-schema CONSTANTS — the pure ``application`` layer
must not import ``okto_pulse.core.telemetry`` (the #03 import-boundary rule).

Three surfaces:
  - :func:`run_telemetry_event_schema_gate` — deterministic structural check that
    the vocabulary is CLOSED and internally consistent: every declared
    ``TELEMETRY_EVENT_TYPES`` has an ``ALLOWED_PAYLOAD_KEYS`` entry, the
    EventType<->contract mapping holds (no ghost/phantom schema), and no removed
    type reappears. Reuses ``event_contract.contract_violations``.
  - :func:`validate_event_call` — runtime classifier for a single call:
    ``ok`` only when the event_type is in the closed vocabulary AND every payload
    key is allowed for that type.
  - :func:`scan_record_event_calls` — an AST SCANNER (import-light, ast+pathlib)
    that walks a source tree and DETECTS rogue ``record_event(...)`` calls: a
    LITERAL event_type outside ``TELEMETRY_EVENT_TYPES`` (phantom) or a LITERAL
    payload key outside ``ALLOWED_PAYLOAD_KEYS`` (out-of-schema) BLOCKS; dynamic
    event_type/payload (from a variable/expression) is recorded as ``advisory``
    (not a failure, since it cannot be statically resolved).
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okto_pulse.core.telemetry.event_contract import contract_violations
from okto_pulse.core.telemetry.schema import (
    ALLOWED_PAYLOAD_KEYS,
    TELEMETRY_EVENT_TYPES,
)


@dataclass(frozen=True)
class EventCallVerdict:
    ok: bool
    event_type: str
    phantom_event_type: bool
    rejected_keys: tuple[str, ...]
    reason: str | None


@dataclass
class TelemetryEventSchemaReport:
    ok: bool
    event_types: tuple[str, ...]
    contract_violations: tuple[str, ...] = ()
    vocabulary_mismatches: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "event_types": list(self.event_types),
            "contract_violations": list(self.contract_violations),
            "vocabulary_mismatches": list(self.vocabulary_mismatches),
        }


def validate_event_call(
    event_type: str, payload: dict[str, Any] | None = None
) -> EventCallVerdict:
    """Classify a ``record_event(event_type, payload)`` call against the closed
    schema. ``ok`` requires a known event_type AND payload keys ⊆ the allowlist
    for that type (the redaction itself stays in ``sanitize_payload``)."""
    if event_type not in ALLOWED_PAYLOAD_KEYS:
        return EventCallVerdict(
            ok=False,
            event_type=event_type,
            phantom_event_type=True,
            rejected_keys=tuple(sorted((payload or {}))),
            reason=f"phantom event_type {event_type!r} not in TELEMETRY_EVENT_TYPES",
        )
    allowed = ALLOWED_PAYLOAD_KEYS[event_type]
    rejected = tuple(sorted(k for k in (payload or {}) if k not in allowed))
    if rejected:
        return EventCallVerdict(
            ok=False,
            event_type=event_type,
            phantom_event_type=False,
            rejected_keys=rejected,
            reason=f"payload keys outside the closed schema for {event_type!r}: {list(rejected)}",
        )
    return EventCallVerdict(
        ok=True,
        event_type=event_type,
        phantom_event_type=False,
        rejected_keys=(),
        reason=None,
    )


def run_telemetry_event_schema_gate() -> TelemetryEventSchemaReport:
    """Fail-closed structural check that the telemetry event vocabulary is CLOSED
    and consistent (no phantom schema)."""
    declared = set(TELEMETRY_EVENT_TYPES)
    keyed = set(ALLOWED_PAYLOAD_KEYS)
    mismatches: list[str] = []
    for missing in sorted(declared - keyed):
        mismatches.append(
            f"declared event_type {missing!r} has no ALLOWED_PAYLOAD_KEYS entry"
        )
    for extra in sorted(keyed - declared):
        mismatches.append(
            f"ALLOWED_PAYLOAD_KEYS has entry {extra!r} not in TELEMETRY_EVENT_TYPES"
        )
    violations = tuple(contract_violations())
    ok = not mismatches and not violations
    return TelemetryEventSchemaReport(
        ok=ok,
        event_types=tuple(TELEMETRY_EVENT_TYPES),
        contract_violations=violations,
        vocabulary_mismatches=tuple(mismatches),
    )


# ---------------------------------------------------------------------------
# AST scanner — detect rogue record_event(...) calls in a source tree (AC4/TS04).
# ---------------------------------------------------------------------------

PHANTOM_EVENT_TYPE = "phantom_event_type"
OUT_OF_SCHEMA_PAYLOAD_KEY = "out_of_schema_payload_key"


@dataclass
class RecordEventScanReport:
    ok: bool
    scanned_calls: int = 0
    violations: tuple[dict, ...] = ()
    advisory: tuple[dict, ...] = field(default_factory=tuple)

    @property
    def advisory_count(self) -> int:
        return len(self.advisory)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "scanned_calls": self.scanned_calls,
            "violations": [dict(v) for v in self.violations],
            "advisory": [dict(a) for a in self.advisory],
            "advisory_count": self.advisory_count,
        }


def _resolve_record_event_aliases(tree: ast.AST) -> set[str]:
    """Local names bound (by import-as / from-import / assignment) to the free
    ``record_event`` symbol, so an aliased free-function call is still detected.
    The method form ``x.record_event(...)`` is matched by attribute name."""
    aliases: set[str] = {"record_event"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name == "record_event":
                    aliases.add(a.asname or a.name)
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
            if node.value.id in aliases:
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        aliases.add(t.id)
    return aliases


def _is_record_event_call(call: ast.Call, aliases: set[str]) -> bool:
    fn = call.func
    if isinstance(fn, ast.Attribute):
        return fn.attr == "record_event"
    if isinstance(fn, ast.Name):
        return fn.id in aliases
    return False


def _call_args(call: ast.Call) -> tuple[ast.expr | None, ast.expr | None]:
    """Return (event_type_node, payload_node) honoring positional + keyword form."""
    event_type = call.args[0] if call.args else None
    payload = call.args[1] if len(call.args) > 1 else None
    for kw in call.keywords:
        if kw.arg == "event_type":
            event_type = kw.value
        elif kw.arg == "payload":
            payload = kw.value
    return event_type, payload


def scan_record_event_calls(
    source_root: str | Path,
) -> RecordEventScanReport:
    """Walk ``source_root`` (a file or dir) and flag rogue ``record_event(...)``
    calls. LITERAL phantom event_type / out-of-schema payload key BLOCK
    (``ok=False``); dynamic (non-literal) arguments are recorded as ``advisory``.

    The gate module itself is skipped (it names the symbols as classification
    keys, not as calls)."""
    root = Path(source_root)
    self_file = Path(__file__).resolve()
    files = [root] if root.is_file() else sorted(root.rglob("*.py"))

    scanned = 0
    violations: list[dict] = []
    advisory: list[dict] = []
    for py in files:
        if "__pycache__" in py.parts or py.resolve() == self_file:
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        rel = py.name if root.is_file() else py.relative_to(root).as_posix()
        aliases = _resolve_record_event_aliases(tree)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and _is_record_event_call(node, aliases)):
                continue
            scanned += 1
            et_node, payload_node = _call_args(node)
            # event_type
            if isinstance(et_node, ast.Constant) and isinstance(et_node.value, str):
                if et_node.value not in ALLOWED_PAYLOAD_KEYS:
                    violations.append({
                        "file": rel, "line": node.lineno,
                        "kind": PHANTOM_EVENT_TYPE, "symbol": et_node.value,
                    })
                # payload keys are checked against THIS literal event_type's allowlist
                allowed = ALLOWED_PAYLOAD_KEYS.get(et_node.value, set())
                if isinstance(payload_node, ast.Dict):
                    for k in payload_node.keys:
                        if isinstance(k, ast.Constant) and isinstance(k.value, str):
                            if k.value not in allowed:
                                violations.append({
                                    "file": rel, "line": node.lineno,
                                    "kind": OUT_OF_SCHEMA_PAYLOAD_KEY, "symbol": k.value,
                                })
                        else:
                            advisory.append({"file": rel, "line": node.lineno,
                                             "kind": "dynamic_payload_key"})
                elif payload_node is not None:
                    advisory.append({"file": rel, "line": node.lineno,
                                     "kind": "dynamic_payload"})
            elif et_node is not None:
                # dynamic event_type -> cannot statically resolve -> advisory.
                advisory.append({"file": rel, "line": node.lineno,
                                 "kind": "dynamic_event_type"})

    return RecordEventScanReport(
        ok=not violations,
        scanned_calls=scanned,
        violations=tuple(violations),
        advisory=tuple(advisory),
    )


__all__ = [
    "EventCallVerdict",
    "TelemetryEventSchemaReport",
    "RecordEventScanReport",
    "PHANTOM_EVENT_TYPE",
    "OUT_OF_SCHEMA_PAYLOAD_KEY",
    "validate_event_call",
    "run_telemetry_event_schema_gate",
    "scan_record_event_calls",
]
