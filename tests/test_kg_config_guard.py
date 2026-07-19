"""KG-01.5 — KGConfigChangeGuard (FR10, TR14, OR or_731d308e, contract api_4e07374f).

Deterministic tests against the runtime config-change guard. Each test
exercises one rule: shrink-below-footprint, migration_plan_required,
restart_policy_required, unsupported_setting, atomic_validation_unavailable,
counter shape, safe audit_event labels.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.config_guard import (
    ConfigBlockReason,
    ConfigGuardError,
    ConfigGuardErrorCode,
    GraphSettingPolicy,
    KGConfigChangeGuard,
    RestartPolicy,
    SETTING_GROUP_BUFFER,
    SETTING_GROUP_CACHE,
    SETTING_GROUP_CONNECTION_POOL,
    SETTING_GROUP_INDEX,
    SETTING_GROUP_STORAGE,
    SETTING_GROUP_WAL,
    get_config_block_count,
    get_config_block_counter_labels,
    get_config_block_samples,
    get_graph_runtime_setting_metadata,
    get_setting_groups,
    reset_config_block_counter,
)


TEST_GRAPH_SETTING_POLICY = GraphSettingPolicy(
    setting_groups={
        "kg_kuzu_buffer_pool_mb": SETTING_GROUP_BUFFER,
        "kg_kuzu_max_db_size_gb": SETTING_GROUP_STORAGE,
        "kg_connection_pool_size": SETTING_GROUP_CONNECTION_POOL,
        "kg_kuzu_wal_mode": SETTING_GROUP_WAL,
        "kg_kuzu_cache_threshold_pct": SETTING_GROUP_CACHE,
        "kg_kuzu_index_rebuild_on_open": SETTING_GROUP_INDEX,
        "ladybug_buffer_pool_mb": SETTING_GROUP_BUFFER,
        "ladybug_max_db_size_gb": SETTING_GROUP_STORAGE,
        "ladybug_wal_mode": SETTING_GROUP_WAL,
    },
    governed_prefixes=(
        "kg_kuzu_",
        "kg_ladybug_",
        "kg_connection_",
        "ladybug_",
    ),
    public_contract="legacy_runtime_settings_api",
)


def _guard(**kwargs) -> KGConfigChangeGuard:
    return KGConfigChangeGuard(policy=TEST_GRAPH_SETTING_POLICY, **kwargs)


@pytest.fixture(autouse=True)
def _reset_counter():
    reset_config_block_counter()
    yield
    reset_config_block_counter()


# --- Decision happy paths -----------------------------------------------------


def test_noop_change_is_allowed_with_value_not_changed_reason():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_buffer_pool_mb": 512},
        requested_settings={"kg_kuzu_buffer_pool_mb": 512},
        actor_id="actor-1",
    )
    assert decision.allowed is True
    assert decision.reason == ConfigBlockReason.VALUE_NOT_CHANGED.value
    assert decision.requires_restart is False
    assert decision.audit_event == "kg.config_change.noop.allowed"


def test_unrelated_setting_changes_are_ignored():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"random_app_flag": "off"},
        requested_settings={"random_app_flag": "on"},
        actor_id="actor-1",
    )
    # No KG/ladybug settings changed → treated as noop.
    assert decision.allowed is True
    assert decision.reason == ConfigBlockReason.VALUE_NOT_CHANGED.value


def test_cache_setting_change_with_no_extras_is_allowed():
    """cache is not in migration-required nor restart-required groups,
    so a simple cache change with no migration plan / no restart is OK."""
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_cache_threshold_pct": 70},
        requested_settings={"kg_kuzu_cache_threshold_pct": 80},
        actor_id="actor-1",
    )
    assert decision.allowed is True
    assert decision.requires_restart is False
    assert decision.setting_group == SETTING_GROUP_CACHE


def test_public_graph_runtime_knobs_have_groups_and_metadata():
    groups = get_setting_groups(TEST_GRAPH_SETTING_POLICY)
    metadata = get_graph_runtime_setting_metadata(TEST_GRAPH_SETTING_POLICY)

    expected = {
        "kg_kuzu_buffer_pool_mb": SETTING_GROUP_BUFFER,
        "kg_kuzu_max_db_size_gb": SETTING_GROUP_STORAGE,
        "kg_connection_pool_size": SETTING_GROUP_CONNECTION_POOL,
    }
    for setting_name, setting_group in expected.items():
        assert groups[setting_name] == setting_group
        assert metadata[setting_name] == {
            "setting_group": setting_group,
            "owner": "graph_runtime_capability",
            "public_contract": "legacy_runtime_settings_api",
        }


def test_buffer_change_with_restart_required_is_allowed_with_requires_restart():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_buffer_pool_mb": 512},
        requested_settings={"kg_kuzu_buffer_pool_mb": 1024},
        actor_id="actor-1",
        restart_policy=RestartPolicy.REQUIRED.value,
    )
    assert decision.allowed is True
    assert decision.requires_restart is True
    assert decision.setting_group == SETTING_GROUP_BUFFER


def test_connection_pool_change_requires_restart_and_is_allowed_with_policy():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_connection_pool_size": 8},
        requested_settings={"kg_connection_pool_size": 12},
        actor_id="actor-1",
        restart_policy=RestartPolicy.REQUIRED.value,
    )
    assert decision.allowed is True
    assert decision.requires_restart is True
    assert decision.setting_group == SETTING_GROUP_CONNECTION_POOL


def test_storage_grow_with_migration_plan_and_restart_is_allowed():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_max_db_size_gb": 4},
        requested_settings={"kg_kuzu_max_db_size_gb": 8},
        actor_id="actor-1",
        migration_plan_ref="MP-2026-05-26-001",
        restart_policy=RestartPolicy.SCHEDULED.value,
    )
    assert decision.allowed is True
    assert decision.requires_restart is True
    assert decision.setting_group == SETTING_GROUP_STORAGE


# --- FR10 / TR14 block rules --------------------------------------------------


def test_storage_shrink_below_current_is_blocked():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_max_db_size_gb": 8},
        requested_settings={"kg_kuzu_max_db_size_gb": 4},
        actor_id="actor-1",
        migration_plan_ref="MP-1",
        restart_policy=RestartPolicy.SCHEDULED.value,
    )
    assert decision.allowed is False
    assert decision.reason == ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value
    assert decision.audit_event == "kg.config_change.storage.blocked"
    assert (
        get_config_block_count(
            SETTING_GROUP_STORAGE,
            reason=ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value,
        )
        == 1
    )


def test_storage_shrink_below_probe_footprint_is_blocked():
    """Even when current_settings omits the size, the probe-supplied
    footprint blocks a shrink."""
    def probe(name):
        if name == "kg_kuzu_max_db_size_gb":
            return 12  # actual on-disk footprint
        return None

    guard = _guard(current_footprint_probe=probe)
    decision = guard.validate(
        board_id="b1",
        current_settings={},
        requested_settings={"kg_kuzu_max_db_size_gb": 8},
        actor_id="actor-1",
        migration_plan_ref="MP-1",
        restart_policy=RestartPolicy.SCHEDULED.value,
    )
    assert decision.allowed is False
    assert decision.reason == ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value


def test_storage_change_without_migration_plan_is_blocked():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_max_db_size_gb": 4},
        requested_settings={"kg_kuzu_max_db_size_gb": 8},  # grow but no MP
        actor_id="actor-1",
        restart_policy=RestartPolicy.REQUIRED.value,
    )
    assert decision.allowed is False
    assert decision.reason == ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value


def test_wal_change_without_migration_plan_is_blocked():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_wal_mode": "default"},
        requested_settings={"kg_kuzu_wal_mode": "aggressive"},
        actor_id="actor-1",
        restart_policy=RestartPolicy.SCHEDULED.value,
    )
    assert decision.allowed is False
    assert decision.reason == ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value


def test_buffer_change_without_restart_policy_is_blocked():
    """Buffer pool size cannot be hot-changed — needs restart."""
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_buffer_pool_mb": 512},
        requested_settings={"kg_kuzu_buffer_pool_mb": 1024},
        actor_id="actor-1",
        restart_policy=RestartPolicy.NONE.value,
    )
    assert decision.allowed is False
    assert decision.reason == ConfigBlockReason.RESTART_POLICY_REQUIRED.value


def test_connection_pool_change_without_restart_policy_is_blocked():
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_connection_pool_size": 8},
        requested_settings={"kg_connection_pool_size": 12},
        actor_id="actor-1",
        restart_policy=RestartPolicy.NONE.value,
    )
    assert decision.allowed is False
    assert decision.reason == ConfigBlockReason.RESTART_POLICY_REQUIRED.value
    assert decision.setting_group == SETTING_GROUP_CONNECTION_POOL


# --- Unsupported settings + atomic validation infra --------------------------


def test_unsupported_kg_setting_raises():
    """A setting matching kg_/ladybug_ prefix but absent from the
    allow-list raises ``unsupported_ladybug_setting`` so callers can't
    sneak unknown keys past the guard."""
    guard = _guard()
    with pytest.raises(ConfigGuardError) as excinfo:
        guard.validate(
            board_id="b1",
            current_settings={},
            requested_settings={"kg_kuzu_dangerous_undocumented_flag": True},
            actor_id="actor-1",
        )
    assert excinfo.value.code is ConfigGuardErrorCode.UNSUPPORTED_GRAPH_SETTING
    assert excinfo.value.retryable is False


def test_unsupported_connection_setting_raises():
    guard = _guard()
    with pytest.raises(ConfigGuardError) as excinfo:
        guard.validate(
            board_id="b1",
            current_settings={},
            requested_settings={"kg_connection_pool_backend": "direct"},
            actor_id="actor-1",
        )
    assert excinfo.value.code is ConfigGuardErrorCode.UNSUPPORTED_GRAPH_SETTING
    assert excinfo.value.retryable is False


def test_invalid_restart_policy_raises_unsupported():
    guard = _guard()
    with pytest.raises(ConfigGuardError) as excinfo:
        guard.validate(
            board_id="b1",
            current_settings={"kg_kuzu_buffer_pool_mb": 512},
            requested_settings={"kg_kuzu_buffer_pool_mb": 1024},
            actor_id="actor-1",
            restart_policy="bogus_value",
        )
    assert excinfo.value.code is ConfigGuardErrorCode.UNSUPPORTED_GRAPH_SETTING


def test_atomic_validation_unavailable_raises():
    guard = _guard(
        atomic_validation_available=lambda: False,
    )
    with pytest.raises(ConfigGuardError) as excinfo:
        guard.validate(
            board_id="b1",
            current_settings={"kg_kuzu_buffer_pool_mb": 512},
            requested_settings={"kg_kuzu_buffer_pool_mb": 1024},
            actor_id="actor-1",
            restart_policy=RestartPolicy.REQUIRED.value,
        )
    assert excinfo.value.code is ConfigGuardErrorCode.ATOMIC_VALIDATION_UNAVAILABLE
    assert excinfo.value.retryable is True


# --- OR or_731d308e: counter + safe labels -----------------------------------


def test_counter_carries_required_or_labels():
    assert get_config_block_counter_labels() == ("setting_group", "reason")

    guard = _guard()
    # Migration-required block
    guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_max_db_size_gb": 4},
        requested_settings={"kg_kuzu_max_db_size_gb": 8},
        actor_id="a1",
        restart_policy=RestartPolicy.REQUIRED.value,
    )
    # Shrink block
    guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_max_db_size_gb": 8},
        requested_settings={"kg_kuzu_max_db_size_gb": 4},
        actor_id="a1",
        migration_plan_ref="MP-1",
        restart_policy=RestartPolicy.REQUIRED.value,
    )
    # Restart-required block
    guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_buffer_pool_mb": 512},
        requested_settings={"kg_kuzu_buffer_pool_mb": 1024},
        actor_id="a1",
        restart_policy=RestartPolicy.NONE.value,
    )

    samples = get_config_block_samples()
    keys = {(s["setting_group"], s["reason"]) for s in samples}
    assert (
        SETTING_GROUP_STORAGE,
        ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value,
    ) in keys
    assert (
        SETTING_GROUP_STORAGE,
        ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value,
    ) in keys
    assert (
        SETTING_GROUP_BUFFER,
        ConfigBlockReason.RESTART_POLICY_REQUIRED.value,
    ) in keys

    for s in samples:
        for label in get_config_block_counter_labels():
            assert label in s and isinstance(s[label], str) and s[label]
        assert isinstance(s["count"], int) and s["count"] >= 1


def test_audit_event_format_is_safe_and_bounded():
    """TR12: audit_event MUST NOT contain raw values or unbounded text.
    The format is `kg.config_change.<setting_group>.<outcome>` —
    setting_group is a bounded vocabulary."""
    guard = _guard()
    decision = guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_buffer_pool_mb": 512},
        requested_settings={"kg_kuzu_buffer_pool_mb": 999999999},
        actor_id="actor-1",
        restart_policy=RestartPolicy.REQUIRED.value,
    )
    assert decision.audit_event.startswith("kg.config_change.")
    # The requested value (999999999) MUST NOT appear in the audit_event.
    assert "999999999" not in decision.audit_event
    assert "kg.config_change.buffer.allowed" == decision.audit_event


def test_get_setting_groups_returns_copy():
    groups = get_setting_groups(TEST_GRAPH_SETTING_POLICY)
    assert isinstance(groups, dict)
    # Mutating the returned dict must not affect the module state.
    groups["fake"] = "bogus"
    again = get_setting_groups(TEST_GRAPH_SETTING_POLICY)
    assert "fake" not in again


# --- Counter never includes raw setting values -------------------------------


def test_counter_never_includes_raw_values():
    """TR12 / safe observability: the requested/current values must
    never end up in the counter labels."""
    guard = _guard()
    guard.validate(
        board_id="b1",
        current_settings={"kg_kuzu_buffer_pool_mb": 512},
        requested_settings={"kg_kuzu_buffer_pool_mb": 9999},
        actor_id="actor-1",
        restart_policy=RestartPolicy.NONE.value,
    )
    samples = get_config_block_samples()
    for s in samples:
        for v in (s["setting_group"], s["reason"]):
            assert "9999" not in v
            assert "512" not in v
