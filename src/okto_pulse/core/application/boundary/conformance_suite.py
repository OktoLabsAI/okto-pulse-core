"""Runtime conformance suite — per-axis GateReport aggregation (spec #15).

fr_13fe67d0: the conformance suite produces a report PER decoupling axis with
status ``baseline`` / ``xfail_advisory`` / ``blocking`` plus owner, evidence and
promotion_criteria. It aggregates the deterministic boundary gates already in
the suite (singleton, import boundary, provider-registration, port-conformance)
with two settings-axis checks (runtime settings effect split and the KG tick
reschedule signal). The two integration axes (Community smoke/reinstall and MCP
replay) are deferred to ``xfail_advisory`` by default — they need a live build /
composition and are exercised by the integration test card — unless concrete
reports are injected.
"""

from __future__ import annotations

from pathlib import Path
import ast

from .composition_gate import CompositionBoundaryGate, CompositionBoundaryGateInput
from .gates import ImportBoundaryGate, ImportBoundaryGateInput
from .global_discovery_consumer_gate import GlobalDiscoveryConsumerGate
from .lifecycle_fallback_gate import LifecycleFallbackGate
from .port_conformance import PortConformanceGate
from .core_settings_defaults_gate import (
    run_core_settings_defaults_gate,
    run_public_config_stability_gate,
)
from .relational_residue_gate import run_relational_residue_gate
from .report import GateReport
from .runtime_worker_gate import RuntimeWorkerBoundaryGate
from .scheduler_control_symbol_gate import SchedulerControlSymbolGate
from .singleton_gate import AntiSingletonGate, AntiSingletonGateInput
from .source_read_consumer_gate import SourceReadConsumerGate

#: Worst-first ordering used to compute the suite's overall status.
_STATUS_RANK = {
    "passed": 0,
    "baseline": 1,
    "xfail_advisory": 2,
    "blocking": 3,
    "reject": 4,
}

AXES: tuple[str, ...] = (
    "singleton",
    "import_boundary",
    "provider_registration",
    "port_conformance",
    "scheduler_control_symbol",
    "lifecycle_fallback",
    "runtime_worker_boundary",
    "global_discovery_consumer",
    "source_read_consumer",
    "core_settings_defaults",
    "public_config_stability",
    "af30_3c_relational_residue",
    "runtime_settings_effect_split",
    "scheduler_signal",
    "community_smoke",
    "mcp_replay",
)


def _settings_source(source_root: Path | None) -> str | None:
    root = source_root or Path(__file__).resolve().parents[4]
    path = root / "okto_pulse" / "core" / "services" / "settings_service.py"
    return path.read_text(encoding="utf-8") if path.exists() else None


def _core_package_root(source_root: Path | None) -> Path | None:
    if source_root is None:
        return None
    candidate = source_root / "okto_pulse" / "core"
    return candidate if candidate.exists() else source_root


def settings_split_conformance(source_root: Path | None = None) -> GateReport:
    """F4: the settings facade only hydrates startup through an edition port.

    Keep the existing gate identity, now rejecting a reintroduced tuning writer
    or runtime effect instead of requiring the retired controller's helpers.
    """
    src = _settings_source(source_root)
    if src is None:
        # A partial source_root (e.g. a focused test tree) has no settings_service:
        # advisory, never a crash and never a false pass.
        return GateReport(
            gate_id="runtime_settings_effect_split",
            subject="runtime settings effect split",
            status="xfail_advisory",
            severity="low",
            owner="okto-pulse-core/runtime",
            evidence={"error": "settings_service_not_found", "source_root": str(source_root)},
            promotion_criteria="Run against a source root that contains settings_service.py.",
        )
    try:
        tree = ast.parse(src)
    except SyntaxError:
        tree = ast.Module(body=[], type_ignores=[])
    definitions = [node for node in ast.walk(tree) if isinstance(
        node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
    )]
    calls = [node.func for node in ast.walk(tree) if isinstance(node, ast.Call)]
    expected_body = ast.parse(
        "async def startup():\n"
        "    return await resolve_runtime_settings_adapter().apply_persisted_settings_to_core_settings()\n"
    ).body[0].body
    checks = {
        "startup_only_facade": len(definitions) == 1 and isinstance(definitions[0], ast.AsyncFunctionDef)
        and definitions[0].name == "apply_persisted_settings_to_core_settings",
        "direct_startup_delegation": len(definitions) == 1 and isinstance(definitions[0], ast.AsyncFunctionDef)
        and [ast.dump(node) for node in definitions[0].body] == [ast.dump(node) for node in expected_body],
        "no_tuning_arguments": len(definitions) == 1 and isinstance(definitions[0], ast.AsyncFunctionDef)
        and not (definitions[0].args.posonlyargs or definitions[0].args.args
                 or definitions[0].args.kwonlyargs or definitions[0].args.vararg
                 or definitions[0].args.kwarg or definitions[0].decorator_list),
        "no_controller_aliases": all(
            isinstance(node, (ast.ImportFrom, ast.AsyncFunctionDef))
            or (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str))
            or (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id == "__all__"
                and isinstance(node.value, ast.List) and len(node.value.elts) == 1
                and isinstance(node.value.elts[0], ast.Constant)
                and node.value.elts[0].value == "apply_persisted_settings_to_core_settings")
            for node in tree.body
        ),
        "only_startup_port_calls": len(calls) == 2 and all(
            (isinstance(call, ast.Name) and call.id == "resolve_runtime_settings_adapter")
            or (isinstance(call, ast.Attribute) and call.attr == "apply_persisted_settings_to_core_settings")
            for call in calls
        ),
        "monolith_removed": "def _maybe_reschedule_tick" not in src,
        "no_scheduler_effect": "scheduler_control" not in src,
        "no_direct_singleton_import": ("kg." "scheduler_" "singleton") not in src,
        # R-P2-06C/R08 — the general settings-effects contract: no implicit
        # concrete effect provider in the core and an executable effect->port
        # inventory (SETTINGS_RUNTIME_EFFECT_PORTS) is the canonical source.
        "no_implicit_singleton_construction": ("Singleton" "SchedulerControl(") not in src,
        "no_effect_adapter_import": ("scheduler_control" "_adapter") not in src,
        "no_tuning_effect_inventory": "SETTINGS_RUNTIME_EFFECT_PORTS" not in src,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        return GateReport(
            gate_id="runtime_settings_effect_split",
            subject="runtime settings effect split",
            status="blocking",
            severity="high",
            owner="okto-pulse-core/runtime",
            evidence={"checks": checks, "failed": failed, "error": "settings_split_regressed"},
            observed_value=failed,
            expected_value=[],
            remediation_hint=(
                "settings_service must expose only startup hydration through its "
                "registered edition port, without tuning writers, scheduler effects "
                "or concrete adapter construction."
            ),
        )
    return GateReport(
        gate_id="runtime_settings_effect_split",
        subject="runtime settings effect split",
        status="baseline",
        severity="low",
        owner="okto-pulse-core/runtime",
        evidence={"checks": checks, "error": "settings_split_ok"},
        promotion_criteria=(
            "Startup hydration is the only settings facade operation; "
            "automatic scheduler ownership is checked by the composition gates."
        ),
    )


def scheduler_signal_conformance() -> GateReport:
    """fr_70c29790: the kg.tick.reschedule_failed signal carries every required
    field, no forbidden one, and a sanitized (secret-free) message."""
    from okto_pulse.core.ports.runtime_settings import (
        RESCHEDULE_FAILED_FORBIDDEN_FIELDS,
        RESCHEDULE_FAILED_REQUIRED_FIELDS,
        build_reschedule_failed_signal,
    )

    leaky = RuntimeError("connect failed token=SECRET-LEAK password=hunter2")
    payload = build_reschedule_failed_signal(
        error=leaky, actor_id="op-1", source="runtime_settings.put"
    )
    missing = [f for f in RESCHEDULE_FAILED_REQUIRED_FIELDS if f not in payload]
    forbidden = [f for f in RESCHEDULE_FAILED_FORBIDDEN_FIELDS if f in payload]
    leaked = "SECRET-LEAK" in payload.get("sanitized_message", "") or (
        "hunter2" in payload.get("sanitized_message", "")
    )
    problems = {"missing": missing, "forbidden_present": forbidden, "secret_leaked": leaked}
    if missing or forbidden or leaked:
        return GateReport(
            gate_id="scheduler_signal",
            subject="kg.tick.reschedule_failed signal",
            status="blocking",
            severity="critical",
            owner="okto-pulse-core/runtime",
            evidence={"signal": payload, "problems": problems, "error": "signal_contract_violation"},
            observed_value=sorted(k for k, v in problems.items() if v),
            expected_value=[],
            remediation_hint=(
                "Build the failure signal via build_reschedule_failed_signal: carry "
                "job_id/error_class/sanitized_message/actor_id/source, never a secret."
            ),
        )
    return GateReport(
        gate_id="scheduler_signal",
        subject="kg.tick.reschedule_failed signal",
        status="baseline",
        severity="low",
        owner="okto-pulse-core/runtime",
        evidence={"signal_fields": sorted(payload), "error": "signal_contract_ok"},
        promotion_criteria="Signal contract preserved; promote with the scheduler provider.",
    )


def _deferred_integration_axis(axis: str, owner: str, runner_tool: str) -> GateReport:
    return GateReport(
        gate_id=axis,
        subject=axis.replace("_", " "),
        status="xfail_advisory",
        severity="medium",
        owner=owner,
        evidence={"deferred": True, "runner": runner_tool, "error": "deferred_to_integration"},
        promotion_criteria=(
            f"Run {runner_tool} with a live build/composition in the integration "
            "test card; promote to passed once it runs green, or blocking on a delta."
        ),
        remediation_hint=(
            "Integration axis: inject a concrete report (community/replay) or run "
            "the dedicated gate with real inputs."
        ),
    )


class ConformanceSuite:
    """Aggregates the runtime decoupling axes into a per-axis report (fr_13fe67d0)."""

    gate_id = "conformance_suite"

    def run(
        self,
        *,
        source_root: Path | None = None,
        community_report: GateReport | None = None,
        mcp_replay_report: GateReport | None = None,
    ) -> dict[str, object]:
        axes: dict[str, GateReport] = {
            "singleton": AntiSingletonGate().run(
                AntiSingletonGateInput(source_root=source_root)
            ),
            "import_boundary": ImportBoundaryGate().run(
                ImportBoundaryGateInput(mode="bootstrap", source_root=source_root)
            ),
            "provider_registration": CompositionBoundaryGate().run(
                CompositionBoundaryGateInput(mode="bootstrap", source_root=source_root)
            ),
            "port_conformance": PortConformanceGate().run(),
            "scheduler_control_symbol": SchedulerControlSymbolGate().run(
                source_root=source_root
            ),
            "lifecycle_fallback": LifecycleFallbackGate().run(
                source_root=source_root
            ),
            "runtime_worker_boundary": RuntimeWorkerBoundaryGate().run(
                source_root=source_root
            ),
            "global_discovery_consumer": GlobalDiscoveryConsumerGate().run(
                source_root=source_root
            ),
            "source_read_consumer": SourceReadConsumerGate().run(
                source_root=source_root
            ),
            "core_settings_defaults": run_core_settings_defaults_gate(
                source_root=source_root
            ),
            "public_config_stability": run_public_config_stability_gate(
                source_root=source_root
            ),
            "af30_3c_relational_residue": run_relational_residue_gate(
                core_root=_core_package_root(source_root)
            ).as_gate_report(),
            "runtime_settings_effect_split": settings_split_conformance(source_root),
            "scheduler_signal": scheduler_signal_conformance(),
            "community_smoke": community_report
            or _deferred_integration_axis(
                "community_smoke",
                "okto-pulse-community/runtime",
                "CommunityRebuildReinstallSmokeGate",
            ),
            "mcp_replay": mcp_replay_report
            or _deferred_integration_axis(
                "mcp_replay", "okto-pulse-core/inbound-mcp", "CommunityLifespanReplay"
            ),
        }
        overall = max(
            (_STATUS_RANK.get(r.status, 0) for r in axes.values()), default=0
        )
        overall_status = next(
            s for s, rank in _STATUS_RANK.items() if rank == overall
        )
        return {
            "suite": self.gate_id,
            "axes": {name: report.as_dict() for name, report in axes.items()},
            "overall_status": overall_status,
            "blocking_axes": sorted(
                name for name, r in axes.items() if r.status in {"blocking", "reject"}
            ),
            "advisory_axes": sorted(
                name for name, r in axes.items() if r.status == "xfail_advisory"
            ),
        }
