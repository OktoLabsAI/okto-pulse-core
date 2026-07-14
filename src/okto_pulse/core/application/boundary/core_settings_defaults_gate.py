"""R17 CoreSettings default ownership inventory and stability gates.

R17 is not a broad settings migration. It locks the current ``CoreSettings``
surface behind an explicit field-by-field ownership inventory, proves Community
effective-value parity, and fails closed when a new concrete core default appears
without owner/refinement evidence.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Sequence

from .report import GateReport

SettingClassification = Literal[
    "common_agnostic",
    "core_contract_required",
    "edition_default_community",
    "test_default",
    "owned_by_refinement",
    "core_consumed_community_observable",
]

CleanupStatus = Literal[
    "keep",
    "register_before_remove",
    "defer_to_owner_spec",
    "test_only",
]

REQUIRED_INVENTORY_FIELDS: tuple[str, ...] = (
    "setting_name",
    "env_var",
    "default_repr",
    "classification",
    "owner",
    "spec_ref",
    "allowed_action",
    "cleanup_status",
    "public_contract",
    "effective_source",
    "compatibility_path",
    "removal_criterion",
)

COMMUNITY_PARITY_FIELDS: tuple[str, ...] = ()

KG_RUNTIME_KNOBS: tuple[str, ...] = ()

_BASELINE_CORE_SETTING_NAMES: tuple[str, ...] = (
    "app_name",
    "app_version",
    "metrics_mode",
    "metrics_retention_days",
    "metrics_policy_version",
    "metrics_schema_version",
    "metrics_opt_in_prompt_interval_days",
    "metrics_token_refresh_margin_hours",
    "kg_session_ttl_seconds",
    "kg_cleanup_interval_seconds",
    "kg_cleanup_enabled",
    "kg_max_queue_depth",
    "kg_queue_max_concurrent_workers",
    "kg_queue_min_interval_ms",
    "kg_queue_claim_timeout_s",
    "kg_queue_max_attempts",
    "kg_queue_alert_threshold",
    "kg_queue_stuck_age_seconds",
    "kg_queue_recovery_scan_interval_s",
    "cognitive_readiness_blocking_enabled",
    "kg_queue_dlq_auto_drain_backoff_s",
    "kg_queue_dlq_auto_drain_max_requeue_attempts",
    "kg_decay_tick_interval_minutes",
    "kg_decay_tick_staleness_days",
    "kg_decay_tick_max_age_days",
)

_EDITION_COMMUNITY_OWNERS: dict[str, tuple[str, str, str]] = {
    "database_url": (
        "okto-pulse-community/data",
        "R01A/R01B",
        "sqlite database location is derived by CommunitySettings.",
    ),
    "upload_dir": (
        "okto-pulse-community/storage",
        "R02",
        "Community storage owns the installed upload path.",
    ),
    "metrics_dir": (
        "okto-pulse-community/telemetry",
        "R12",
        "Community local telemetry owns the installed metrics path.",
    ),
    "kg_base_dir": (
        "okto-pulse-community/kg",
        "R07/R10",
        "Community KG adapters own the installed graph base path.",
    ),
    "kg_embedding_mode": (
        "okto-pulse-community/kg",
        "R14",
        "Community overrides the core stub mode with sentence-transformers.",
    ),
}

_KG_RUNTIME_OWNERS: dict[str, str] = {
    "kg_kuzu_buffer_pool_mb": (
        "Legacy public buffer knob retained for API/env compatibility; AF37 "
        "tracks the neutral graph_runtime_* alias plan."
    ),
    "kg_kuzu_max_db_size_gb": (
        "Legacy public storage knob retained for API/env compatibility; AF37 "
        "tracks the neutral graph_runtime_* alias plan."
    ),
    "kg_wal_salvage_enabled": (
        "WAL salvage toggle (KGD-01 FR1/TR3) exposed through the public "
        "runtime settings API; consumed by the edition graph adapter's "
        "single open factory."
    ),
    "kg_wal_only_recovery_enabled": (
        "Wal-only recovery toggle (KGD-01 FR3/BR2, degrau 2 of the recovery "
        "ladder) exposed through the public runtime settings API; consumed "
        "by the edition graph adapter's single open factory."
    ),
    "kg_connection_pool_size": (
        "Legacy public connection-pool knob retained for API/env compatibility; "
        "AF39 tracks the neutral graph_runtime_* alias plan."
    ),
}

_CORE_CONTRACT_OWNERS: dict[str, tuple[str, str]] = {
    "app_name": ("okto-pulse-core/runtime", "application identity"),
    "app_version": ("okto-pulse-core/runtime", "package version contract"),
    "debug": ("okto-pulse-core/runtime", "runtime diagnostic flag"),
    "environment": ("okto-pulse-core/runtime", "runtime environment selector"),
    "host": ("okto-pulse-core/inbound", "core server bind host"),
    "port": ("okto-pulse-core/inbound", "core server port"),
    "max_upload_size": ("okto-pulse-core/storage", "upload limit contract"),
    "metrics_mode": ("okto-pulse-core/telemetry", "telemetry consent mode"),
    "metrics_retention_days": ("okto-pulse-core/telemetry", "retention contract"),
    "metrics_beacon_url": ("okto-pulse-core/telemetry", "telemetry endpoint contract"),
    "metrics_policy_version": ("okto-pulse-core/telemetry", "policy version contract"),
    "metrics_schema_version": ("okto-pulse-core/telemetry", "schema version contract"),
    "metrics_opt_in_prompt_interval_days": (
        "okto-pulse-core/telemetry",
        "prompt cadence contract",
    ),
    "metrics_token_refresh_margin_hours": (
        "okto-pulse-core/telemetry",
        "token refresh margin contract",
    ),
    "mcp_server_name": ("okto-pulse-core/inbound-mcp", "MCP server identity"),
    "mcp_server_version": ("okto-pulse-core/inbound-mcp", "MCP version contract"),
    "mcp_port": ("okto-pulse-core/inbound-mcp", "MCP server port"),
    "cors_origins": ("okto-pulse-core/inbound", "CORS public contract"),
    "kg_embedding_model": ("okto-pulse-core/kg", "embedding model contract"),
    "kg_embedding_dim": ("okto-pulse-core/kg", "embedding dimensionality contract"),
    "kg_session_ttl_seconds": ("okto-pulse-core/kg", "KG session TTL"),
    "kg_cleanup_interval_seconds": ("okto-pulse-core/kg", "KG cleanup cadence"),
    "kg_cleanup_enabled": ("okto-pulse-core/kg", "KG cleanup toggle"),
    "kg_max_queue_depth": ("okto-pulse-core/kg", "legacy queue-depth compatibility"),
    "kg_queue_max_concurrent_workers": ("okto-pulse-core/kg", "queue worker cap"),
    "kg_queue_min_interval_ms": ("okto-pulse-core/kg", "queue polling interval"),
    "kg_queue_claim_timeout_s": ("okto-pulse-core/kg", "queue claim timeout"),
    "kg_queue_max_attempts": ("okto-pulse-core/kg", "queue retry cap"),
    "kg_queue_alert_threshold": ("okto-pulse-core/kg", "queue alert threshold"),
    "kg_queue_stuck_age_seconds": ("okto-pulse-core/kg", "queue stale classifier"),
    "kg_queue_recovery_scan_interval_s": (
        "okto-pulse-core/kg",
        "queue recovery cadence",
    ),
    "cognitive_readiness_blocking_enabled": (
        "okto-pulse-core/cognitive-readiness",
        "global enforcement rollout flag",
    ),
    "kg_queue_dlq_auto_drain_backoff_s": (
        "okto-pulse-core/kg",
        "DLQ auto-drain backoff",
    ),
    "kg_queue_dlq_auto_drain_max_requeue_attempts": (
        "okto-pulse-core/kg",
        "DLQ poison-pill threshold",
    ),
    "kg_decay_tick_interval_minutes": ("okto-pulse-core/kg", "decay tick interval"),
    "kg_decay_tick_staleness_days": ("okto-pulse-core/kg", "decay staleness"),
    "kg_decay_tick_max_age_days": ("okto-pulse-core/kg", "decay max-age cap"),
}

_LOCAL_FIRST_UPLOAD_DEFAULT_REPRS: frozenset[str] = frozenset(
    {"'./uploads'", '"./uploads"'}
)

_CORE_PUBLIC_CONTRACT = (
    "Core-owned public CoreSettings field and environment variable."
)
_CORE_EFFECTIVE_SOURCE = "Pre-materialized Core policy snapshot."
_CORE_COMPATIBILITY_PATH = (
    "Field name and env var remain stable; renames require PublicSettingAlias "
    "and an approved migration plan."
)
_CORE_REMOVAL_CRITERION = (
    "No planned removal; removal requires an approved public migration alias, "
    "downstream regression coverage and this inventory update."
)

_CORE_CONTRACT_MATRIX_OVERRIDES: dict[str, dict[str, str]] = {
    "metrics_beacon_url": {
        "public_contract": (
            "Telemetry beacon endpoint field is public; the core default is "
            "intentionally neutral."
        ),
        "effective_source": (
            "CoreSettings keeps a neutral empty default; Community telemetry "
            "capability supplies the installed endpoint and consent behavior."
        ),
        "compatibility_path": (
            "METRICS_BEACON_URL remains accepted while AF40 owns the telemetry "
            "provider pipeline and any endpoint migration."
        ),
        "removal_criterion": (
            "Only move/remove after AF40 registers a telemetry provider contract, "
            "legacy env alias and Community/runtime regression coverage."
        ),
    },
    "kg_embedding_model": {
        "public_contract": (
            "Embedding model field/env remains a public compatibility contract."
        ),
        "effective_source": (
            "Core preserves the historical model name for compatibility; the "
            "Community embedding provider is the installed effective provider."
        ),
        "compatibility_path": (
            "KG_EMBEDDING_MODEL stays stable until the Community provider declares "
            "the edition default and a migration alias."
        ),
        "removal_criterion": (
            "Only move/remove after provider-owned defaults, env alias coverage, "
            "runtime API parity and embedding provider regression tests pass."
        ),
    },
    "kg_embedding_dim": {
        "public_contract": (
            "Embedding dimension remains public because persisted vectors depend on it."
        ),
        "effective_source": (
            "CoreSettings preserves the compatibility dimension; Community "
            "embedding provider metadata must agree or override explicitly."
        ),
        "compatibility_path": (
            "KG_EMBEDDING_DIM remains stable until provider metadata is the single "
            "source of truth with migration coverage."
        ),
        "removal_criterion": (
            "Only move/remove after provider metadata owns the dimension, legacy "
            "env alias coverage exists and vector/index regression tests pass."
        ),
    },
}

_EDITION_MATRIX: dict[str, dict[str, str]] = {
    "database_url": {
        "effective_source": (
            "CommunitySettings derives data/pulse.db from data_dir; CoreSettings "
            "keeps a neutral empty default."
        ),
        "compatibility_path": (
            "DATABASE_URL remains public; CommunitySettings accepts empty or "
            "legacy local values and derives the installed database URL."
        ),
        "removal_criterion": (
            "Only move/remove after Community database provider registration, "
            "legacy env alias and settings parity tests replace this field."
        ),
    },
    "upload_dir": {
        "effective_source": (
            "Community storage derives data_dir/uploads; CoreSettings keeps a "
            "neutral empty default."
        ),
        "compatibility_path": (
            "UPLOAD_DIR remains public; CommunitySettings accepts empty or legacy "
            "./uploads values and derives the installed upload path."
        ),
        "removal_criterion": (
            "Only move/remove after StorageProvider registration, legacy env alias "
            "and Community settings parity tests replace this field."
        ),
    },
    "metrics_dir": {
        "effective_source": (
            "Community telemetry derives data_dir/metrics; CoreSettings keeps a "
            "neutral empty default."
        ),
        "compatibility_path": (
            "METRICS_DIR remains public; CommunitySettings derives the installed "
            "metrics path while AF40 owns the telemetry pipeline migration."
        ),
        "removal_criterion": (
            "Only move/remove after AF40 telemetry provider registration, legacy "
            "env alias and local telemetry parity tests replace this field."
        ),
    },
    "kg_base_dir": {
        "effective_source": (
            "CommunitySettings/KG capability derives data_dir; the core "
            "~/.okto-pulse literal is compatibility-only, not an installed source."
        ),
        "compatibility_path": (
            "KG_BASE_DIR remains public; CommunitySettings maps empty or legacy "
            "~/.okto-pulse values to data_dir."
        ),
        "removal_criterion": (
            "Only move/remove after KG capability/provider registration, legacy "
            "env alias and runtime settings parity tests replace this field."
        ),
    },
    "kg_embedding_mode": {
        "effective_source": (
            "CommunitySettings overrides the core stub mode with "
            "sentence-transformers for the installed edition."
        ),
        "compatibility_path": (
            "KG_EMBEDDING_MODE remains public; CommunitySettings preserves the "
            "edition override and tests the effective value."
        ),
        "removal_criterion": (
            "Only move/remove after embedding provider registration, legacy env "
            "alias and provider composition regression tests replace this field."
        ),
    },
}


@dataclass(frozen=True, slots=True)
class CoreSettingDefaultEntry:
    """One canonical row in the R17 CoreSettings ownership inventory."""

    setting_name: str
    env_var: str
    default_repr: str
    classification: SettingClassification
    owner: str
    spec_ref: str
    allowed_action: str
    cleanup_status: CleanupStatus
    rationale: str
    public_contract: str = ""
    effective_source: str = ""
    compatibility_path: str = ""
    removal_criterion: str = ""
    core_consumers: tuple[str, ...] = ()
    community_surfaces: tuple[str, ...] = ()
    migration_plan_ref: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "setting_name": self.setting_name,
            "env_var": self.env_var,
            "default_repr": self.default_repr,
            "classification": self.classification,
            "owner": self.owner,
            "spec_ref": self.spec_ref,
            "allowed_action": self.allowed_action,
            "cleanup_status": self.cleanup_status,
            "rationale": self.rationale,
            "public_contract": self.public_contract,
            "effective_source": self.effective_source,
            "compatibility_path": self.compatibility_path,
            "removal_criterion": self.removal_criterion,
            "core_consumers": list(self.core_consumers),
            "community_surfaces": list(self.community_surfaces),
            "migration_plan_ref": self.migration_plan_ref,
        }


@dataclass(frozen=True, slots=True)
class PublicSettingAlias:
    """Explicit migration alias for a public setting rename."""

    original_name: str
    new_name: str
    legacy_env_var: str
    migration_plan_ref: str

    def as_dict(self) -> dict[str, str]:
        return {
            "original_name": self.original_name,
            "new_name": self.new_name,
            "legacy_env_var": self.legacy_env_var,
            "migration_plan_ref": self.migration_plan_ref,
        }


@dataclass(frozen=True, slots=True)
class CoreSettingSourceField:
    """A field parsed from the CoreSettings class body."""

    setting_name: str
    default_repr: str
    line: int

    def as_dict(self) -> dict[str, object]:
        return {
            "setting_name": self.setting_name,
            "default_repr": self.default_repr,
            "line": self.line,
        }


@dataclass(frozen=True, slots=True)
class CoreSettingsFinding:
    """Bounded diagnostic row emitted by the R17 gates."""

    setting_name: str
    diagnostic_code: str
    reason: str
    remediation: str
    line: int | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "setting_name": self.setting_name,
            "diagnostic_code": self.diagnostic_code,
            "reason": self.reason,
            "remediation": self.remediation,
            "line": self.line,
        }


@dataclass(frozen=True, slots=True)
class CoreSettingsDefaultsReport:
    """Combined inventory + source audit report for R17."""

    entries: tuple[CoreSettingDefaultEntry, ...]
    source_fields: tuple[CoreSettingSourceField, ...]
    findings: tuple[CoreSettingsFinding, ...]

    @property
    def ok(self) -> bool:
        return not self.findings

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "entries": [entry.as_dict() for entry in self.entries],
            "source_fields": [field.as_dict() for field in self.source_fields],
            "findings": [finding.as_dict() for finding in self.findings],
        }

    def as_gate_report(self, gate_id: str = "core_settings_defaults") -> GateReport:
        status = "passed" if self.ok else "blocking"
        return GateReport(
            gate_id=gate_id,
            subject="CoreSettings default ownership inventory",
            status=status,
            severity="high" if self.findings else "medium",
            owner="okto-pulse-core/config",
            evidence={
                "entries": [entry.as_dict() for entry in self.entries],
                "findings": [finding.as_dict() for finding in self.findings],
            },
            observed_value=len(self.findings),
            expected_value=0,
            promotion_criteria=(
                "Every CoreSettings default is explicitly inventoried with owner, "
                "classification, public contract, effective source, compatibility "
                "path, allowed action, cleanup status and removal criterion."
            ),
            remediation_hint=(
                "Update the R17 CoreSettings ownership inventory, add a Community "
                "register-before-remove owner, or provide an explicit migration alias."
            )
            if self.findings
            else None,
        )


@dataclass(frozen=True, slots=True)
class CommunitySettingsParityReport:
    """Parity projection for CommunitySettings effective values."""

    ok: bool
    expected: dict[str, object]
    observed: dict[str, object]
    mismatches: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "expected": dict(sorted(self.expected.items())),
            "observed": dict(sorted(self.observed.items())),
            "mismatches": list(self.mismatches),
        }

    def as_gate_report(self) -> GateReport:
        return GateReport(
            gate_id="community_settings_parity",
            subject="CommunitySettings effective-value parity",
            status="passed" if self.ok else "blocking",
            severity="high" if self.mismatches else "medium",
            owner="okto-pulse-community/config",
            evidence=self.as_dict(),
            observed_value=list(self.mismatches),
            expected_value=[],
            promotion_criteria=(
                "CommunitySettings must keep installed effective values for "
                "database_url, upload_dir, metrics_dir, kg_base_dir and "
                "kg_embedding_mode unless an explicit migration exists."
            ),
            remediation_hint=(
                "Restore CommunitySettings literal-default derivation or add an "
                "approved migration plan before changing public defaults."
            )
            if self.mismatches
            else None,
        )


def _default_source_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _config_path(source_root: Path | None = None) -> Path:
    root = source_root or _default_source_root()
    return root / "okto_pulse" / "core" / "infra" / "config.py"


def _env_var(setting_name: str) -> str:
    return setting_name.upper()


def _field_default_repr(
    field_name: str,
    source_defaults: dict[str, str],
) -> str:
    return source_defaults.get(field_name, "<missing-in-source>")


def _core_matrix_value(field_name: str, key: str) -> str:
    return _CORE_CONTRACT_MATRIX_OVERRIDES.get(field_name, {}).get(
        key,
        {
            "public_contract": _CORE_PUBLIC_CONTRACT,
            "effective_source": _CORE_EFFECTIVE_SOURCE,
            "compatibility_path": _CORE_COMPATIBILITY_PATH,
            "removal_criterion": _CORE_REMOVAL_CRITERION,
        }[key],
    )


def _core_contract_entry(
    field_name: str,
    source_defaults: dict[str, str],
) -> CoreSettingDefaultEntry:
    owner, rationale = _CORE_CONTRACT_OWNERS[field_name]
    return CoreSettingDefaultEntry(
        setting_name=field_name,
        env_var=_env_var(field_name),
        default_repr=_field_default_repr(field_name, source_defaults),
        classification="core_contract_required",
        owner=owner,
        spec_ref="core-config-contract",
        allowed_action="keep; changes require explicit owner review",
        cleanup_status="keep",
        rationale=rationale,
        public_contract=_core_matrix_value(field_name, "public_contract"),
        effective_source=_core_matrix_value(field_name, "effective_source"),
        compatibility_path=_core_matrix_value(field_name, "compatibility_path"),
        removal_criterion=_core_matrix_value(field_name, "removal_criterion"),
    )


def build_core_settings_inventory(
    *,
    additional_entries: Sequence[CoreSettingDefaultEntry] = (),
    source_root: Path | None = None,
    source_text: str | None = None,
) -> tuple[CoreSettingDefaultEntry, ...]:
    """Return the canonical R17 field-by-field CoreSettings inventory."""

    source_defaults = {
        field.setting_name: field.default_repr
        for field in parse_core_settings_source_fields(
            source_root=source_root,
            source_text=source_text,
        )
    }
    rows: list[CoreSettingDefaultEntry] = []
    for field_name in _BASELINE_CORE_SETTING_NAMES:
        if field_name in _EDITION_COMMUNITY_OWNERS:
            owner, spec_ref, rationale = _EDITION_COMMUNITY_OWNERS[field_name]
            rows.append(
                CoreSettingDefaultEntry(
                    setting_name=field_name,
                    env_var=_env_var(field_name),
                    default_repr=_field_default_repr(field_name, source_defaults),
                    classification="edition_default_community",
                    owner=owner,
                    spec_ref=spec_ref,
                    allowed_action=(
                        "keep until Community registers equivalent effective value "
                        "and parity tests pass"
                    ),
                    cleanup_status="register_before_remove",
                    rationale=rationale,
                    public_contract=(
                        "Public CoreSettings field/env retained for "
                        "edition-neutral compatibility."
                    ),
                    effective_source=_EDITION_MATRIX[field_name]["effective_source"],
                    compatibility_path=_EDITION_MATRIX[field_name][
                        "compatibility_path"
                    ],
                    removal_criterion=_EDITION_MATRIX[field_name][
                        "removal_criterion"
                    ],
                    community_surfaces=("CommunitySettings",),
                )
            )
            continue
        if field_name in _KG_RUNTIME_OWNERS:
            rows.append(
                CoreSettingDefaultEntry(
                    setting_name=field_name,
                    env_var=_env_var(field_name),
                    default_repr=_field_default_repr(field_name, source_defaults),
                    classification="core_consumed_community_observable",
                    owner="okto-pulse-core/kg + okto-pulse-community/settings",
                    spec_ref="R17/RKG-runtime-settings",
                    allowed_action=(
                        "keep; removal requires Community provider/API/UI parity "
                        "and core consumer regression"
                    ),
                    cleanup_status="register_before_remove",
                    rationale=_KG_RUNTIME_OWNERS[field_name],
                    public_contract=(
                        "Public KG runtime field/env retained for /settings/runtime "
                        "and environment compatibility."
                    ),
                    effective_source=(
                        "Core KG consumers still read the field; Community "
                        "settings/runtime surfaces own the installed operator value."
                    ),
                    compatibility_path=(
                        "Keep the legacy field/env until provider-neutral runtime "
                        "aliases and Community API/UI parity are registered."
                    ),
                    removal_criterion=(
                        "Only move/remove after provider-neutral aliases, legacy env "
                        "migration, Community /settings/runtime parity and core KG "
                        "consumer regression tests pass."
                    ),
                    core_consumers=(
                        "okto_pulse.community.adapters.graph_connection_pool",
                        "okto_pulse.core.kg.primitives",
                        "okto_pulse.core.services.settings_service",
                    ),
                    community_surfaces=(
                        "CommunitySettings",
                        "/api/v1/settings/runtime",
                        "Community settings UI",
                    ),
                    migration_plan_ref="AF37/AF39 graph-runtime compatibility ledger",
                )
            )
            continue
        rows.append(_core_contract_entry(field_name, source_defaults))

    merged = {row.setting_name: row for row in rows}
    for row in additional_entries:
        merged[row.setting_name] = row
    return tuple(merged[name] for name in sorted(merged))


def parse_core_settings_source_fields(
    *,
    source_root: Path | None = None,
    source_text: str | None = None,
) -> tuple[CoreSettingSourceField, ...]:
    """Parse ``CoreSettings`` annotated fields from source text or source root."""

    if source_text is None:
        path = _config_path(source_root)
        source_text = path.read_text(encoding="utf-8")
    tree = ast.parse(source_text)
    class_node = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "CoreSettings"
        ),
        None,
    )
    if class_node is None:
        return ()

    fields: list[CoreSettingSourceField] = []
    for node in class_node.body:
        if not isinstance(node, ast.AnnAssign):
            continue
        if not isinstance(node.target, ast.Name):
            continue
        if node.value is None:
            default_repr = "<required>"
        else:
            try:
                default_repr = ast.unparse(node.value)
            except Exception:
                default_repr = "<unparseable>"
        fields.append(
            CoreSettingSourceField(
                setting_name=node.target.id,
                default_repr=default_repr,
                line=node.lineno,
            )
        )
    return tuple(fields)


def _validate_inventory_rows(
    entries: Sequence[CoreSettingDefaultEntry],
) -> tuple[CoreSettingsFinding, ...]:
    findings: list[CoreSettingsFinding] = []
    seen: set[str] = set()
    for entry in entries:
        missing = [
            field_name
            for field_name in REQUIRED_INVENTORY_FIELDS
            if not getattr(entry, field_name)
        ]
        if missing:
            findings.append(
                CoreSettingsFinding(
                    setting_name=entry.setting_name or "<missing>",
                    diagnostic_code="incomplete_inventory_row",
                    reason=f"Inventory row is missing required fields: {missing}",
                    remediation=(
                        "Provide setting_name/env/default/classification/owner/"
                        "spec/action/cleanup/public_contract/effective_source/"
                        "compatibility_path/removal_criterion fields before the "
                        "gate can pass."
                    ),
                )
            )
        if entry.setting_name in seen:
            findings.append(
                CoreSettingsFinding(
                    setting_name=entry.setting_name,
                    diagnostic_code="duplicate_inventory_row",
                    reason="The CoreSettings inventory has duplicate rows.",
                    remediation="Keep exactly one canonical ownership row per setting.",
                )
            )
        seen.add(entry.setting_name)
    return tuple(findings)


def _validate_source_fields_unique(
    source_fields: Sequence[CoreSettingSourceField],
) -> tuple[CoreSettingsFinding, ...]:
    findings: list[CoreSettingsFinding] = []
    seen: dict[str, CoreSettingSourceField] = {}
    for field in source_fields:
        first = seen.get(field.setting_name)
        if first is None:
            seen[field.setting_name] = field
            continue
        findings.append(
            CoreSettingsFinding(
                setting_name=field.setting_name,
                diagnostic_code="duplicate_core_settings_source_field",
                reason=(
                    "CoreSettings declares the same public setting field more "
                    "than once, making ownership/default review ambiguous."
                ),
                remediation=(
                    "Keep a single CoreSettings declaration for the field and "
                    "record any compatibility or cleanup intent in the AF39 "
                    "ownership inventory."
                ),
                line=field.line,
            )
        )
    return tuple(findings)


def _literal_string_default(default_repr: str) -> str | None:
    try:
        value = ast.literal_eval(default_repr)
    except (SyntaxError, ValueError):
        return None
    return value if isinstance(value, str) else None


def _is_local_first_default(default_repr: str) -> bool:
    value = _literal_string_default(default_repr)
    if value is None:
        return False
    normalized = value.replace("\\", "/")
    if not normalized:
        return False
    if normalized.startswith(("./", "../", "~/", "/")):
        return True
    if len(normalized) >= 3 and normalized[1:3] == ":/":
        return True
    return normalized.startswith("sqlite") and "///" in normalized


def _has_edition_local_default_owner(entry: CoreSettingDefaultEntry) -> bool:
    return (
        entry.classification == "edition_default_community"
        and entry.owner.startswith("okto-pulse-community/")
        and bool(entry.effective_source)
        and bool(entry.compatibility_path)
        and bool(entry.removal_criterion)
    )


def _validate_local_first_default_ownership(
    entries: Sequence[CoreSettingDefaultEntry],
    source_by_name: dict[str, CoreSettingSourceField],
) -> tuple[CoreSettingsFinding, ...]:
    findings: list[CoreSettingsFinding] = []
    for entry in entries:
        source_field = source_by_name.get(entry.setting_name)
        if source_field is None:
            continue
        if not (
            source_field.default_repr in _LOCAL_FIRST_UPLOAD_DEFAULT_REPRS
            or _is_local_first_default(source_field.default_repr)
        ):
            continue
        if _has_edition_local_default_owner(entry):
            continue
        findings.append(
            CoreSettingsFinding(
                setting_name=entry.setting_name,
                diagnostic_code="unowned_local_first_default",
                reason=(
                    "CoreSettings has a local-first literal default but the "
                    "inventory row does not assign edition ownership with an "
                    "effective source, compatibility path and removal criterion."
                ),
                remediation=(
                    "Keep the CoreSettings default neutral, or classify the local "
                    "value as edition_default_community with a Community owner and "
                    "complete AF39 ownership matrix fields."
                ),
                line=source_field.line,
            )
        )
    return tuple(findings)


def _alias_by_original(
    aliases: Sequence[PublicSettingAlias],
) -> dict[str, PublicSettingAlias]:
    return {alias.original_name: alias for alias in aliases}


def audit_core_settings_defaults(
    *,
    source_root: Path | None = None,
    source_text: str | None = None,
    entries: Sequence[CoreSettingDefaultEntry] | None = None,
    additional_entries: Sequence[CoreSettingDefaultEntry] = (),
    aliases: Sequence[PublicSettingAlias] = (),
) -> CoreSettingsDefaultsReport:
    """Audit CoreSettings against the R17 ownership inventory."""

    inventory = tuple(
        entries
        if entries is not None
        else build_core_settings_inventory(
            additional_entries=additional_entries,
            source_root=source_root,
            source_text=source_text,
        )
    )
    by_name = {entry.setting_name: entry for entry in inventory}
    source_fields = parse_core_settings_source_fields(
        source_root=source_root,
        source_text=source_text,
    )
    source_by_name = {field.setting_name: field for field in source_fields}
    alias_map = _alias_by_original(aliases)

    findings: list[CoreSettingsFinding] = list(_validate_inventory_rows(inventory))
    findings.extend(_validate_source_fields_unique(source_fields))
    findings.extend(_validate_local_first_default_ownership(inventory, source_by_name))
    baseline = set(_BASELINE_CORE_SETTING_NAMES)

    for field in source_fields:
        if field.setting_name in by_name:
            continue
        findings.append(
            CoreSettingsFinding(
                setting_name=field.setting_name,
                diagnostic_code="new_unowned_core_default",
                reason=(
                    "CoreSettings has a source field not present in the R17 "
                    "ownership inventory."
                ),
                remediation=(
                    "Declare owner/classification/spec/action in the R17 inventory "
                    "or add an explicit migration alias before adding the default."
                ),
                line=field.line,
            )
        )

    for setting_name in sorted(baseline):
        if setting_name in source_by_name:
            continue
        alias = alias_map.get(setting_name)
        if (
            alias is not None
            and alias.new_name in source_by_name
            and alias.legacy_env_var == _env_var(setting_name)
            and alias.migration_plan_ref
        ):
            continue
        findings.append(
            CoreSettingsFinding(
                setting_name=setting_name,
                diagnostic_code="missing_public_core_setting",
                reason=(
                    "A baseline CoreSettings field disappeared from the public "
                    "settings surface without a migration alias."
                ),
                remediation=(
                    "Restore the original setting name or provide a PublicSettingAlias "
                    "with the legacy env var and migration plan."
                ),
            )
        )

    return CoreSettingsDefaultsReport(
        entries=tuple(sorted(inventory, key=lambda entry: entry.setting_name)),
        source_fields=tuple(sorted(source_fields, key=lambda field: field.setting_name)),
        findings=tuple(sorted(findings, key=lambda f: (f.setting_name, f.diagnostic_code))),
    )


def run_core_settings_defaults_gate(
    *,
    source_root: Path | None = None,
    source_text: str | None = None,
    additional_entries: Sequence[CoreSettingDefaultEntry] = (),
    aliases: Sequence[PublicSettingAlias] = (),
) -> GateReport:
    """Run the R17 ownership/defaults gate."""

    if source_text is None and not _config_path(source_root).exists():
        return GateReport(
            gate_id="core_settings_defaults",
            subject="CoreSettings default ownership inventory",
            status="xfail_advisory",
            severity="low",
            owner="okto-pulse-core/config",
            evidence={"error": "core_config_source_not_found"},
            promotion_criteria="Run against a source root containing core/infra/config.py.",
        )
    return audit_core_settings_defaults(
        source_root=source_root,
        source_text=source_text,
        additional_entries=additional_entries,
        aliases=aliases,
    ).as_gate_report()


def run_public_config_stability_gate(
    *,
    source_root: Path | None = None,
    source_text: str | None = None,
    additional_entries: Sequence[CoreSettingDefaultEntry] = (),
    aliases: Sequence[PublicSettingAlias] = (),
) -> GateReport:
    """Run the R17 public settings name/env stability gate."""

    if source_text is None and not _config_path(source_root).exists():
        return GateReport(
            gate_id="public_config_stability",
            subject="CoreSettings public settings stability",
            status="xfail_advisory",
            severity="low",
            owner="okto-pulse-core/config",
            evidence={"error": "core_config_source_not_found"},
            promotion_criteria="Run against a source root containing core/infra/config.py.",
        )
    report = audit_core_settings_defaults(
        source_root=source_root,
        source_text=source_text,
        additional_entries=additional_entries,
        aliases=aliases,
    )
    public_findings = tuple(
        finding
        for finding in report.findings
        if finding.diagnostic_code
        in {
            "incomplete_inventory_row",
            "missing_public_core_setting",
            "new_unowned_core_default",
        }
    )
    return CoreSettingsDefaultsReport(
        entries=report.entries,
        source_fields=report.source_fields,
        findings=public_findings,
    ).as_gate_report(gate_id="public_config_stability")


def evaluate_community_settings_parity(
    *,
    community_settings: object,
) -> CommunitySettingsParityReport:
    """Validate CommunitySettings effective values against the installed contract.

    The function accepts an object instead of importing CommunitySettings so the
    core gate stays edition-neutral. Tests pass the real CommunitySettings.
    """

    data_dir = Path(str(getattr(community_settings, "data_dir"))).expanduser().resolve()
    installed_defaults: dict[str, object] = {
        "database_url": f"sqlite+aiosqlite:///{data_dir / 'data' / 'pulse.db'}",
        "upload_dir": str(data_dir / "uploads"),
        "metrics_dir": str(data_dir / "metrics"),
        "kg_base_dir": str(data_dir),
        "kg_embedding_mode": "sentence-transformers",
    }
    expected = {
        field: installed_defaults[field]
        for field in COMMUNITY_PARITY_FIELDS
    }
    observed = {
        field: getattr(community_settings, field)
        for field in COMMUNITY_PARITY_FIELDS
    }
    mismatches = tuple(
        field
        for field in COMMUNITY_PARITY_FIELDS
        if str(observed.get(field)) != str(expected[field])
    )
    return CommunitySettingsParityReport(
        ok=not mismatches,
        expected=expected,
        observed=observed,
        mismatches=mismatches,
    )


__all__ = [
    "COMMUNITY_PARITY_FIELDS",
    "KG_RUNTIME_KNOBS",
    "REQUIRED_INVENTORY_FIELDS",
    "CleanupStatus",
    "CommunitySettingsParityReport",
    "CoreSettingDefaultEntry",
    "CoreSettingSourceField",
    "CoreSettingsDefaultsReport",
    "CoreSettingsFinding",
    "PublicSettingAlias",
    "SettingClassification",
    "audit_core_settings_defaults",
    "build_core_settings_inventory",
    "evaluate_community_settings_parity",
    "parse_core_settings_source_fields",
    "run_core_settings_defaults_gate",
    "run_public_config_stability_gate",
]
