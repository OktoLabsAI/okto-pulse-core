"""PortConformanceGate — structural conformance of the runtime ports (spec #15).

Validates the four runtime Protocols delivered by card 72bcdfcf are still
``runtime_checkable`` and expose exactly their frozen member contract. Signature
drift blocks — keeping the ports honest as the strangler refactor proceeds.

Imports of the ports/adapters are deferred to ``run()`` so this module stays
import-side-effect free.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass

from .report import GateReport

#: Frozen member contract per runtime Protocol (module, qualname -> members).
PORT_CONTRACTS: dict[str, dict[str, object]] = {
    "SchedulerControl": {
        "module": "okto_pulse.core.ports.scheduler",
        "members": frozenset({"is_available", "reschedule_job", "shutdown"}),
    },
    "RuntimeSettingsPort": {
        "module": "okto_pulse.core.ports.runtime_settings",
        "members": frozenset({"load", "persist", "apply_runtime_effects"}),
    },
    "RuntimeControl": {
        "module": "okto_pulse.core.ports.runtime_control",
        "members": frozenset({"startup", "shutdown", "get_provider"}),
    },
    "RuntimeEventBusPort": {
        "module": "okto_pulse.core.ports.runtime_events",
        "members": frozenset({"publish_runtime_event", "flush"}),
    },
}

#: Edition-owned adapters are conformance-tested in their edition package.
ADAPTER_CONFORMANCE: tuple[tuple[str, str, str], ...] = ()


@dataclass(frozen=True)
class PortConformanceFinding:
    subject: str
    code: str
    detail: str


def _protocol_members(proto: type) -> set[str]:
    attrs = getattr(proto, "__protocol_attrs__", None)
    if attrs is not None:
        return set(attrs)
    members: set[str] = set()
    for klass in getattr(proto, "__mro__", [proto]):
        if klass.__name__ in {"Protocol", "Generic", "object"}:
            continue
        members.update(n for n in vars(klass) if not n.startswith("_"))
        members.update(getattr(klass, "__annotations__", {}))
    return members


def _is_runtime_checkable(proto: type) -> bool:
    return bool(getattr(proto, "_is_runtime_protocol", False))


class PortConformanceGate:
    """Checks the four runtime Protocols + known adapters conform."""

    gate_id = "port_conformance"

    def run(self) -> GateReport:
        findings: list[PortConformanceFinding] = []
        checked_ports: list[str] = []

        for name, spec in PORT_CONTRACTS.items():
            module_path = str(spec["module"])
            expected = set(spec["members"])  # type: ignore[arg-type]
            try:
                module = importlib.import_module(module_path)
                proto = getattr(module, name)
            except (ImportError, AttributeError) as exc:
                findings.append(
                    PortConformanceFinding(name, "port_missing", f"{module_path}.{name}: {exc}")
                )
                continue
            checked_ports.append(name)
            if not _is_runtime_checkable(proto):
                findings.append(
                    PortConformanceFinding(name, "not_runtime_checkable",
                                           f"{name} must be @runtime_checkable")
                )
            actual = _protocol_members(proto)
            if actual != expected:
                findings.append(
                    PortConformanceFinding(
                        name,
                        "member_drift",
                        f"{name} members {sorted(actual)} != contract {sorted(expected)}",
                    )
                )

        adapters_checked: list[str] = []
        for adapter_module, adapter_name, proto_name in ADAPTER_CONFORMANCE:
            try:
                module = importlib.import_module(adapter_module)
                adapter_cls = getattr(module, adapter_name)
                proto_module = importlib.import_module(str(PORT_CONTRACTS[proto_name]["module"]))
                proto = getattr(proto_module, proto_name)
            except (ImportError, AttributeError, KeyError) as exc:
                findings.append(
                    PortConformanceFinding(
                        adapter_name, "adapter_missing", f"{adapter_module}.{adapter_name}: {exc}"
                    )
                )
                continue
            adapters_checked.append(f"{adapter_name}->{proto_name}")
            if not isinstance(adapter_cls(), proto):
                missing = set(PORT_CONTRACTS[proto_name]["members"]) - {  # type: ignore[arg-type]
                    m for m in dir(adapter_cls) if not m.startswith("_")
                }
                findings.append(
                    PortConformanceFinding(
                        adapter_name,
                        "adapter_nonconformant",
                        f"{adapter_name} does not satisfy {proto_name}; missing {sorted(missing)}",
                    )
                )

        evidence = {
            "ports_checked": sorted(checked_ports),
            "adapters_checked": sorted(adapters_checked),
            "adapter_conformance_owner": "edition",
            "contract_members": {k: sorted(v["members"]) for k, v in PORT_CONTRACTS.items()},  # type: ignore[index]
            "findings": [
                {"subject": f.subject, "code": f.code, "detail": f.detail} for f in findings
            ],
        }

        if findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="runtime ports conformance",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "port_nonconformance"},
                observed_value=sorted(f"{f.subject}:{f.code}" for f in findings),
                expected_value=[],
                remediation_hint=(
                    "A runtime port drifted from its frozen contract or an adapter "
                    "stopped satisfying it. Restore the @runtime_checkable Protocol "
                    "member set, or fix the adapter to implement every port method."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="runtime ports conformance",
            status="passed",
            severity="low",
            owner="okto-pulse-core/architecture",
            evidence=evidence,
        )
