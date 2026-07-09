"""Graph runtime config-change guard (KG-01 FR10, contract api_4e07374f).

Any change to graph-runtime settings exposed through the legacy public
Kuzu/Ladybug setting names (buffer pool size, max DB size, WAL behaviour,
cache thresholds, connection pool size, ...) MUST flow through
``KGConfigChangeGuard.validate`` before being applied or persisted.
The spec rejects "hot config changes that could silently invalidate
graph storage": resizing the buffer pool while writes are in flight,
toggling WAL mode without checkpoint, shrinking max_db_size below the
current footprint, etc.

The guard is the canonical decision point:

* ``allowed=True`` + ``requires_restart`` flag — the change is safe
  with the supplied migration plan / restart policy. Caller may apply.
* ``allowed=False`` — change is blocked. ``reason`` carries a short
  bounded code (no payload values). Counter
  ``kg_config_change_blocked_total{setting_group, reason}`` (OR
  ``or_731d308e``) is incremented for observability.

Two typed errors short-circuit the decision:

* ``unsupported_ladybug_setting`` (non-retryable, legacy error code) —
  caller asked for a key the guard does not know how to validate. Reject
  loudly.
* ``atomic_validation_unavailable`` (retryable) — the supporting
  infrastructure (lock / DB probe) is temporarily not available.

Safe observability (TR12): the ``audit_event`` string MUST NOT contain
the requested value. The guard returns a stable identifier of the
form ``"kg.config_change.<setting_group>.<outcome>"`` so logs can be
joined to events without leaking payload. Counter labels use the
``setting_group`` bucket, not the raw setting name, to keep
cardinality bounded.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger("okto_pulse.kg.config_guard")


# --- Setting taxonomy ---------------------------------------------------------
#
# KG-01 TR14 calls out kg_kuzu_buffer_pool_mb, kg_kuzu_max_db_size_gb
# and "related LadybugDB parameters". The guard maintains an explicit
# allow-list per setting_group; unknown LadybugDB settings raise
# ``unsupported_ladybug_setting`` so callers can't sneak invalid keys
# past the guard.

SETTING_GROUP_BUFFER = "buffer"
SETTING_GROUP_STORAGE = "storage"
SETTING_GROUP_WAL = "wal"
SETTING_GROUP_CACHE = "cache"
SETTING_GROUP_INDEX = "index"
SETTING_GROUP_CONNECTION_POOL = "connection_pool"
SETTING_GROUP_UNRELATED = "unrelated"

# Map setting name -> setting_group. The public names are legacy compatibility
# names; the owner is the graph runtime capability. Anything not in this map
# and that matches one of the KG-relevant prefixes (kg_*, ladybug_*) raises
# unsupported_ladybug_setting. Keys outside the KG namespace are silently
# ignored because the guard only governs graph-runtime changes.
_KG_SETTING_GROUPS: dict[str, str] = {
    "kg_kuzu_buffer_pool_mb": SETTING_GROUP_BUFFER,
    "kg_kuzu_max_db_size_gb": SETTING_GROUP_STORAGE,
    "kg_connection_pool_size": SETTING_GROUP_CONNECTION_POOL,
    "kg_kuzu_wal_mode": SETTING_GROUP_WAL,
    "kg_kuzu_cache_threshold_pct": SETTING_GROUP_CACHE,
    "kg_kuzu_index_rebuild_on_open": SETTING_GROUP_INDEX,
    "ladybug_buffer_pool_mb": SETTING_GROUP_BUFFER,
    "ladybug_max_db_size_gb": SETTING_GROUP_STORAGE,
    "ladybug_wal_mode": SETTING_GROUP_WAL,
}

# Setting groups that MUST have a migration_plan_ref to mutate.
_GROUPS_REQUIRING_MIGRATION: frozenset[str] = frozenset({
    SETTING_GROUP_STORAGE,
    SETTING_GROUP_WAL,
    SETTING_GROUP_INDEX,
})

# Setting groups that REQUIRE a restart (cannot be hot-changed).
_GROUPS_REQUIRING_RESTART: frozenset[str] = frozenset({
    SETTING_GROUP_BUFFER,
    SETTING_GROUP_STORAGE,
    SETTING_GROUP_WAL,
    SETTING_GROUP_CONNECTION_POOL,
})

# Prefixes considered legacy graph-runtime compatibility. A setting starting
# with one of these prefixes but not in ``_KG_SETTING_GROUPS`` raises the
# legacy ``unsupported_ladybug_setting`` error. This forces explicit
# allow-listing.
_KG_PREFIXES = ("kg_kuzu_", "kg_ladybug_", "kg_connection_", "ladybug_")

_GRAPH_RUNTIME_SETTING_METADATA: dict[str, dict[str, str]] = {
    setting_name: {
        "setting_group": setting_group,
        "owner": "graph_runtime_capability",
        "public_contract": "legacy_runtime_settings_api",
        "compatibility_path": "provider_neutral_graph_runtime_alias_required",
    }
    for setting_name, setting_group in _KG_SETTING_GROUPS.items()
}


# --- Reason codes (bounded for OR or_731d308e label cardinality) -------------


class ConfigBlockReason(str, Enum):
    """Bounded reason codes — never expose raw values."""

    MIGRATION_PLAN_REQUIRED = "migration_plan_required"
    RESTART_POLICY_REQUIRED = "restart_policy_required"
    SHRINK_BELOW_CURRENT_FOOTPRINT = "shrink_below_current_footprint"
    UNSUPPORTED_SETTING = "unsupported_setting"
    ATOMIC_VALIDATION_UNAVAILABLE = "atomic_validation_unavailable"
    VALUE_NOT_CHANGED = "value_not_changed"


class ConfigGuardErrorCode(str, Enum):
    """Typed errors per contract api_4e07374f response_errors."""

    UNSUPPORTED_LADYBUG_SETTING = "unsupported_ladybug_setting"
    ATOMIC_VALIDATION_UNAVAILABLE = "atomic_validation_unavailable"


class ConfigGuardError(Exception):
    """Raised when the guard cannot decide at all.

    Carries the typed ``code`` and ``retryable`` flag so callers can
    pick a retry strategy without parsing strings.
    """

    def __init__(
        self,
        code: ConfigGuardErrorCode,
        *,
        retryable: bool,
        reason: str,
    ) -> None:
        super().__init__(f"{code.value}: {reason}")
        self.code = code
        self.retryable = retryable
        self.reason = reason


class RestartPolicy(str, Enum):
    NONE = "none"
    REQUIRED = "required"
    SCHEDULED = "scheduled"


@dataclass(frozen=True, slots=True)
class ConfigChangeDecision:
    """Frozen contract-shaped response per api_4e07374f success body."""

    allowed: bool
    reason: str
    requires_restart: bool
    audit_event: str
    # Internal/debug: which setting_group bucket the decision applied
    # to (or "noop" / "mixed" when multiple). Not in contract but
    # additive so callers can build a focused alert.
    setting_group: str = ""


# --- Counter (OR or_731d308e) ------------------------------------------------
#
# kg_config_change_blocked_total labels: setting_group, reason.
# Both are bounded enums (or pseudo-enums) — no free-text values.

_GUARD_COUNTER_LABELS = ("setting_group", "reason")

_GuardCounterKey = tuple[str, str]
_guard_counter: dict[_GuardCounterKey, int] = {}
_guard_counter_lock = threading.Lock()


def _bump_guard_counter(*, setting_group: str, reason: str) -> None:
    key: _GuardCounterKey = (setting_group, reason)
    with _guard_counter_lock:
        _guard_counter[key] = _guard_counter.get(key, 0) + 1


def get_config_block_count(
    setting_group: str, *, reason: str | None = None
) -> int:
    with _guard_counter_lock:
        total = 0
        for (sg, rsn), value in _guard_counter.items():
            if sg != setting_group:
                continue
            if reason is not None and rsn != reason:
                continue
            total += value
        return total


def get_config_block_samples() -> list[dict[str, Any]]:
    with _guard_counter_lock:
        return [
            {"setting_group": sg, "reason": rsn, "count": value}
            for (sg, rsn), value in _guard_counter.items()
        ]


def get_config_block_counter_labels() -> tuple[str, ...]:
    return _GUARD_COUNTER_LABELS


def reset_config_block_counter() -> None:
    with _guard_counter_lock:
        _guard_counter.clear()


# --- The guard ---------------------------------------------------------------


def _classify_setting(name: str) -> str:
    """Return the setting_group bucket for ``name``.

    Returns SETTING_GROUP_UNRELATED for settings outside the KG/ladybug
    namespace — the guard does NOT govern those, the caller should not
    have asked. Raises ``ConfigGuardError(UNSUPPORTED_LADYBUG_SETTING)``
    when the setting matches a KG prefix but isn't on the allow-list.
    """
    group = _KG_SETTING_GROUPS.get(name)
    if group is not None:
        return group
    lowered = name.lower()
    if any(lowered.startswith(p) for p in _KG_PREFIXES):
        raise ConfigGuardError(
            ConfigGuardErrorCode.UNSUPPORTED_LADYBUG_SETTING,
            retryable=False,
            reason=f"setting {name} is not in the KG allow-list",
        )
    return SETTING_GROUP_UNRELATED


def _audit_event(setting_group: str, outcome: str) -> str:
    return f"kg.config_change.{setting_group}.{outcome}"


@dataclass(frozen=True, slots=True)
class KGConfigChangeGuard:
    """Stateless validator.

    Two optional hooks let the guard ask the outside world about
    runtime conditions that change over time:

    * ``current_footprint_probe(setting_name) -> float | None`` — returns
      the current on-disk footprint (in the same unit as the setting)
      when the setting governs a size cap. Used to refuse shrinking
      ``max_db_size`` below what is already on disk.
    * ``atomic_validation_available() -> bool`` — returns False when the
      KG single-writer lock or another supporting primitive is unavailable.
      The guard then raises ``ATOMIC_VALIDATION_UNAVAILABLE`` so the
      caller backs off and retries.

    Both default to safe no-ops (probe returns None, atomic always
    available).
    """

    current_footprint_probe: Any = None
    atomic_validation_available: Any = None

    # --- public API --------------------------------------------------------

    def validate(
        self,
        *,
        board_id: str,
        current_settings: dict[str, Any],
        requested_settings: dict[str, Any],
        actor_id: str,
        migration_plan_ref: str | None = None,
        restart_policy: str = RestartPolicy.NONE.value,
    ) -> ConfigChangeDecision:
        """Validate the requested settings diff.

        Returns the contract-shaped decision. Raises ``ConfigGuardError``
        with the typed code for unsupported settings or unavailable
        validation infrastructure.
        """
        # 0. Atomic validation infra check.
        if (
            self.atomic_validation_available is not None
            and not self.atomic_validation_available()
        ):
            _bump_guard_counter(
                setting_group="unknown",
                reason=ConfigBlockReason.ATOMIC_VALIDATION_UNAVAILABLE.value,
            )
            raise ConfigGuardError(
                ConfigGuardErrorCode.ATOMIC_VALIDATION_UNAVAILABLE,
                retryable=True,
                reason="atomic validation infrastructure is offline",
            )

        # 1. Compute the diff. Anything in requested but unchanged or
        # missing in current is treated according to the rules below.
        if restart_policy not in (
            RestartPolicy.NONE.value,
            RestartPolicy.REQUIRED.value,
            RestartPolicy.SCHEDULED.value,
        ):
            raise ConfigGuardError(
                ConfigGuardErrorCode.UNSUPPORTED_LADYBUG_SETTING,
                retryable=False,
                reason=f"invalid restart_policy: {restart_policy}",
            )

        # Identify changed graph-runtime settings (others are out-of-scope).
        changed: list[tuple[str, str, Any, Any]] = []
        for name, requested_value in requested_settings.items():
            group = _classify_setting(name)
            if group == SETTING_GROUP_UNRELATED:
                continue
            current_value = current_settings.get(name)
            if requested_value == current_value:
                continue
            changed.append((name, group, current_value, requested_value))

        if not changed:
            # Nothing actually changes in the graph-runtime namespace.
            return ConfigChangeDecision(
                allowed=True,
                reason=ConfigBlockReason.VALUE_NOT_CHANGED.value,
                requires_restart=False,
                audit_event=_audit_event("noop", "allowed"),
                setting_group="noop",
            )

        # 2. For each changed setting, apply guard rules in order.
        primary_group = changed[0][1]
        any_requires_restart = False

        for name, group, current_value, requested_value in changed:
            # 2a. Storage size shrink: never below current footprint.
            if group == SETTING_GROUP_STORAGE:
                shrunk = self._is_size_shrink(
                    name, current_value, requested_value
                )
                if shrunk:
                    _bump_guard_counter(
                        setting_group=group,
                        reason=(
                            ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value
                        ),
                    )
                    logger.warning(
                        "kg.config_change.blocked board=%s actor=%s "
                        "setting_group=%s reason=%s",
                        board_id, actor_id, group,
                        ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value,
                        extra={
                            "event": _audit_event(group, "blocked"),
                            "board_id": board_id,
                            "actor_id": actor_id,
                            "setting_group": group,
                            "reason": (
                                ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value
                            ),
                        },
                    )
                    return ConfigChangeDecision(
                        allowed=False,
                        reason=(
                            ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value
                        ),
                        requires_restart=False,
                        audit_event=_audit_event(group, "blocked"),
                        setting_group=group,
                    )

            # 2b. Groups requiring migration_plan_ref.
            if (
                group in _GROUPS_REQUIRING_MIGRATION
                and not migration_plan_ref
            ):
                _bump_guard_counter(
                    setting_group=group,
                    reason=ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value,
                )
                logger.warning(
                    "kg.config_change.blocked board=%s actor=%s "
                    "setting_group=%s reason=%s",
                    board_id, actor_id, group,
                    ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value,
                    extra={
                        "event": _audit_event(group, "blocked"),
                        "board_id": board_id,
                        "actor_id": actor_id,
                        "setting_group": group,
                        "reason": (
                            ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value
                        ),
                    },
                )
                return ConfigChangeDecision(
                    allowed=False,
                    reason=ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value,
                    requires_restart=False,
                    audit_event=_audit_event(group, "blocked"),
                    setting_group=group,
                )

            # 2c. Groups requiring restart.
            if group in _GROUPS_REQUIRING_RESTART:
                if restart_policy == RestartPolicy.NONE.value:
                    _bump_guard_counter(
                        setting_group=group,
                        reason=(
                            ConfigBlockReason.RESTART_POLICY_REQUIRED.value
                        ),
                    )
                    logger.warning(
                        "kg.config_change.blocked board=%s actor=%s "
                        "setting_group=%s reason=%s",
                        board_id, actor_id, group,
                        ConfigBlockReason.RESTART_POLICY_REQUIRED.value,
                        extra={
                            "event": _audit_event(group, "blocked"),
                            "board_id": board_id,
                            "actor_id": actor_id,
                            "setting_group": group,
                            "reason": (
                                ConfigBlockReason.RESTART_POLICY_REQUIRED.value
                            ),
                        },
                    )
                    return ConfigChangeDecision(
                        allowed=False,
                        reason=(
                            ConfigBlockReason.RESTART_POLICY_REQUIRED.value
                        ),
                        requires_restart=False,
                        audit_event=_audit_event(group, "blocked"),
                        setting_group=group,
                    )
                any_requires_restart = True

        # 3. All checks passed — allow the change.
        logger.info(
            "kg.config_change.allowed board=%s actor=%s setting_group=%s "
            "requires_restart=%s migration_plan_ref=%s restart_policy=%s",
            board_id, actor_id, primary_group, any_requires_restart,
            bool(migration_plan_ref), restart_policy,
            extra={
                "event": _audit_event(primary_group, "allowed"),
                "board_id": board_id,
                "actor_id": actor_id,
                "setting_group": primary_group,
                "requires_restart": any_requires_restart,
            },
        )
        return ConfigChangeDecision(
            allowed=True,
            reason="ok",
            requires_restart=any_requires_restart,
            audit_event=_audit_event(primary_group, "allowed"),
            setting_group=primary_group if len(changed) == 1 else "mixed",
        )

    # --- internals ---------------------------------------------------------

    def _is_size_shrink(
        self, name: str, current: Any, requested: Any
    ) -> bool:
        try:
            current_f = float(current) if current is not None else None
            requested_f = float(requested)
        except (TypeError, ValueError):
            return False
        if current_f is not None and requested_f < current_f:
            return True
        # Probe current on-disk footprint when explicit current_settings
        # didn't include it.
        if self.current_footprint_probe is not None:
            try:
                actual = self.current_footprint_probe(name)
            except Exception:
                actual = None
            if actual is not None:
                try:
                    actual_f = float(actual)
                except (TypeError, ValueError):
                    return False
                if requested_f < actual_f:
                    return True
        return False


def get_setting_groups() -> dict[str, str]:
    """Public read-only view of the setting -> group allow-list.

    Useful for documentation, dashboard tooltips and tests. Returns a
    copy so callers can't mutate the table.
    """
    return dict(_KG_SETTING_GROUPS)


def get_graph_runtime_setting_metadata() -> dict[str, dict[str, str]]:
    """Public read-only metadata for legacy graph-runtime settings.

    The returned rows make the ownership explicit without renaming the public
    settings API: legacy ``kg_kuzu_*``/``kg_connection_pool_size`` names stay
    stable, while the capability owner and required alias path are visible to
    docs, gates and tests.
    """

    return {
        setting: dict(metadata)
        for setting, metadata in _GRAPH_RUNTIME_SETTING_METADATA.items()
    }


__all__ = [
    "ConfigBlockReason",
    "ConfigChangeDecision",
    "ConfigGuardError",
    "ConfigGuardErrorCode",
    "KGConfigChangeGuard",
    "RestartPolicy",
    "SETTING_GROUP_BUFFER",
    "SETTING_GROUP_CACHE",
    "SETTING_GROUP_CONNECTION_POOL",
    "SETTING_GROUP_INDEX",
    "SETTING_GROUP_STORAGE",
    "SETTING_GROUP_UNRELATED",
    "SETTING_GROUP_WAL",
    "get_config_block_count",
    "get_config_block_counter_labels",
    "get_config_block_samples",
    "get_graph_runtime_setting_metadata",
    "get_setting_groups",
    "reset_config_block_counter",
]
