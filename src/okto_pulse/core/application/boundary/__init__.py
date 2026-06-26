"""Core-pure boundary audit suite (spec #12, card 19f982ec).

Public API for the deterministic boundary gates and their shared GateReport
contract. Import-safe in isolation (no runtime/transport side effects).
"""

from __future__ import annotations

from .gates import (
    DEFAULT_CORE_PURE_FORBIDDEN_DEPENDENCIES,
    TOOLS_LOC_BASELINE,
    DependencyAuditGate,
    DependencyAuditGateInput,
    ImportBoundaryGate,
    ImportBoundaryGateInput,
    ImportSideEffectSmokeGate,
    ImportSideEffectSmokeInput,
    ImportViolation,
    PackageManifestGate,
    PackageManifestGateInput,
)
from .community_smoke import (
    CommandResult,
    CommunityRebuildReinstallSmokeGate,
    CommunitySmokeInput,
    CommunitySmokeOracle,
    CoreSmokeInstallGate,
    SmokeInstallInput,
)
from .composition_gate import (
    DEFAULT_COMPOSITION_BASELINE,
    DEFERRED_BOUNDARIES,
    OWNED_PROVIDER_KEYS,
    CompositionBoundaryGate,
    CompositionBoundaryGateInput,
    CompositionFinding,
)
from .import_matrix import LAYER_IMPORT_MATRIX, MATRIX_VERSION, AllowDenyRule, rule_for
from .lifespan_replay import (
    DEFAULT_ONLY_EXCLUSIONS,
    REQUIRED_LIFECYCLE_EVENTS,
    CommunityLifespanReplay,
    CommunityLifespanReplayInput,
    LifecycleReplayReport,
    NamedLifecycleHook,
)
from .layer_resolver import (
    LAYER_RESOLVER_VERSION,
    LayerResolution,
    LayerResolver,
)
from .report import (
    GateException,
    GateReport,
    GateSeverity,
    GateStatus,
    enforce_exception_policy,
)
from .singleton_gate import (
    BASELINE_SINGLETONS,
    SINGLETON_LEDGER,
    AntiSingletonGate,
    AntiSingletonGateInput,
)
from .port_conformance import (
    PORT_CONTRACTS,
    PortConformanceGate,
)
from .conformance_suite import (
    AXES,
    ConformanceSuite,
    scheduler_signal_conformance,
    settings_split_conformance,
)

__all__ = [
    "AXES",
    "AllowDenyRule",
    "AntiSingletonGate",
    "AntiSingletonGateInput",
    "BASELINE_SINGLETONS",
    "CommandResult",
    "ConformanceSuite",
    "PORT_CONTRACTS",
    "PortConformanceGate",
    "SINGLETON_LEDGER",
    "scheduler_signal_conformance",
    "settings_split_conformance",
    "CommunityLifespanReplay",
    "CommunityLifespanReplayInput",
    "CommunityRebuildReinstallSmokeGate",
    "CommunitySmokeInput",
    "CommunitySmokeOracle",
    "CoreSmokeInstallGate",
    "SmokeInstallInput",
    "CompositionBoundaryGate",
    "CompositionBoundaryGateInput",
    "CompositionFinding",
    "DEFAULT_COMPOSITION_BASELINE",
    "DEFAULT_CORE_PURE_FORBIDDEN_DEPENDENCIES",
    "DEFAULT_ONLY_EXCLUSIONS",
    "DEFERRED_BOUNDARIES",
    "DependencyAuditGate",
    "LifecycleReplayReport",
    "NamedLifecycleHook",
    "OWNED_PROVIDER_KEYS",
    "REQUIRED_LIFECYCLE_EVENTS",
    "DependencyAuditGateInput",
    "GateException",
    "GateReport",
    "GateSeverity",
    "GateStatus",
    "ImportBoundaryGate",
    "ImportBoundaryGateInput",
    "ImportSideEffectSmokeGate",
    "ImportSideEffectSmokeInput",
    "ImportViolation",
    "LAYER_IMPORT_MATRIX",
    "LAYER_RESOLVER_VERSION",
    "LayerResolution",
    "LayerResolver",
    "MATRIX_VERSION",
    "PackageManifestGate",
    "PackageManifestGateInput",
    "TOOLS_LOC_BASELINE",
    "enforce_exception_policy",
    "rule_for",
]
