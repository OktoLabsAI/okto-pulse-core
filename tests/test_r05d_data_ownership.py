"""R05-D (Onda B, CORE target) — data-provider ownership gate + registry
prefer-provided fallback.

  TS2 import-gate-recontamination — the fail-closed DataProviderOwnershipGate
       PASSES on the real core; a synthetic core→community import BLOCKS; a NEW
       data-adapter instantiation outside the single ledgered fallback BLOCKS.
  (fail-closed) — configure_kg_registry requires audit_repo / event_bus to be
       supplied explicitly; R-P2-02 retired the registry session_factory auto-wire.
"""

from __future__ import annotations

from okto_pulse.core.kg.data_provider_ownership_gate import (
    LEDGERED_DATA_FALLBACK,
    SQLALCHEMY_OWNERSHIP_STATUS,
    run_data_provider_ownership_gate,
)


# ===========================================================================
# TS2 — fail-closed ownership gate.
# ===========================================================================
def test_ts2_gate_passes_on_real_core_with_zero_ledgered_fallback():
    r1 = run_data_provider_ownership_gate()
    r2 = run_data_provider_ownership_gate()

    assert r1.as_dict() == r2.as_dict()  # deterministic
    assert r1.ok is True
    assert r1.community_import_offenders == []
    assert r1.new_data_consumers == []
    # R-P2-02 retired the final relational data-provider fallback; the ledger is
    # intentionally empty and the gate enforces zero core instantiation.
    assert r1.ledger == {}
    # SQLAlchemy is the gated #04 exception (documented, not a violation).
    assert r1.as_dict()["sqlalchemy_ownership"] == SQLALCHEMY_OWNERSHIP_STATUS


def test_ts2_gate_blocks_core_importing_community(tmp_path):
    (tmp_path / "rogue.py").write_text(
        "from okto_pulse.community.adapters.data import CommunityKGConfig\n",
        encoding="utf-8",
    )
    report = run_data_provider_ownership_gate(tmp_path)
    assert report.ok is False
    assert any(o["file"] == "rogue.py" for o in report.community_import_offenders)


def test_ts2_gate_blocks_new_unledgered_data_instantiation(tmp_path):
    # A NEW data-adapter instantiation in a non-ledgered core path blocks.
    svc = tmp_path / "services"
    svc.mkdir(parents=True, exist_ok=True)
    (svc / "rogue.py").write_text(
        "from x import SqlAlchemyAuditRepository\n"
        "repo = SqlAlchemyAuditRepository(sf)\n",
        encoding="utf-8",
    )
    report = run_data_provider_ownership_gate(tmp_path)
    assert report.ok is False
    hit = next(
        c for c in report.new_data_consumers if c["file"] == "services/rogue.py"
    )
    assert hit["symbol"] == "SqlAlchemyAuditRepository"


def test_ts2_gate_blocks_instantiation_even_in_registry(tmp_path):
    # R-P2-02 retired the ledgered fallback path: registry.py may no longer
    # instantiate relational data adapters.
    d = tmp_path / "kg" / "interfaces"
    d.mkdir(parents=True, exist_ok=True)
    (d / "registry.py").write_text(
        "from x import SqliteOutboxEventBus\n"
        "bus = SqliteOutboxEventBus(sf)\n",
        encoding="utf-8",
    )
    report = run_data_provider_ownership_gate(tmp_path)
    assert report.ok is False
    assert report.new_data_consumers == [
        {
            "file": "kg/interfaces/registry.py",
            "symbol": "SqliteOutboxEventBus",
            "line": 2,
        }
    ]
    assert LEDGERED_DATA_FALLBACK == {}


# ---------------------------------------------------------------------------
# TS2 (codex adversarial regression) — alias resolution: a renamed/assigned data
# adapter instantiation must NOT slip past the Call check.
# ---------------------------------------------------------------------------
def test_ts2_gate_blocks_aliased_import_instantiation(tmp_path):
    # `from x import SqliteOutboxEventBus as Bus; Bus(sf)` outside the ledger.
    svc = tmp_path / "services"
    svc.mkdir(parents=True, exist_ok=True)
    (svc / "alias.py").write_text(
        "from x import SqliteOutboxEventBus as Bus\nbus = Bus(sf)\n",
        encoding="utf-8",
    )
    report = run_data_provider_ownership_gate(tmp_path)
    assert report.ok is False
    hit = next(
        c for c in report.new_data_consumers if c["file"] == "services/alias.py"
    )
    # The CANONICAL symbol is reported, not the local alias.
    assert hit["symbol"] == "SqliteOutboxEventBus"


def test_ts2_gate_blocks_aliased_config_instantiation(tmp_path):
    # `from x import SettingsKGConfig as Config; Config()` outside the ledger.
    svc = tmp_path / "services"
    svc.mkdir(parents=True, exist_ok=True)
    (svc / "cfg.py").write_text(
        "from x import SettingsKGConfig as Config\ncfg = Config()\n",
        encoding="utf-8",
    )
    report = run_data_provider_ownership_gate(tmp_path)
    assert report.ok is False
    hit = next(
        c for c in report.new_data_consumers if c["file"] == "services/cfg.py"
    )
    assert hit["symbol"] == "SettingsKGConfig"


def test_r07_gate_allows_legitimate_community_protocol_imports(tmp_path):
    core_pkg = tmp_path / "core"
    community_pkg = tmp_path / "community"
    community_pkg.mkdir(parents=True, exist_ok=True)
    core_pkg.mkdir(parents=True, exist_ok=True)
    (community_pkg / "legit.py").write_text(
        "from okto_pulse.core.kg.interfaces.kg_config import KGConfig\n"
        "class CommunityKGConfig:\n"
        "    kg_base_dir = 'x'\n",
        encoding="utf-8",
    )

    report = run_data_provider_ownership_gate(
        core_pkg,
        community_root=community_pkg,
    )

    assert report.ok is True
    assert report.community_settings_config_offenders == []


def test_r07_gate_blocks_community_settingskgconfig_subclass(tmp_path):
    core_pkg = tmp_path / "core"
    community_pkg = tmp_path / "community"
    (community_pkg / "adapters").mkdir(parents=True, exist_ok=True)
    core_pkg.mkdir(parents=True, exist_ok=True)
    (community_pkg / "adapters" / "data.py").write_text(
        "from okto_pulse.core.kg.providers.testing.settings_config "
        "import SettingsKGConfig\n"
        "class CommunityKGConfig(SettingsKGConfig):\n"
        "    pass\n",
        encoding="utf-8",
    )

    report = run_data_provider_ownership_gate(
        core_pkg,
        community_root=community_pkg,
    )

    assert report.ok is False
    assert {
        ("adapters/data.py", "SettingsKGConfig", "import"),
        ("adapters/data.py", "SettingsKGConfig", "subclass"),
    } <= {
        (hit["file"], hit["symbol"], hit["kind"])
        for hit in report.community_settings_config_offenders
    }


def test_r07_gate_blocks_community_module_alias_reference(tmp_path):
    core_pkg = tmp_path / "core"
    community_pkg = tmp_path / "community"
    community_pkg.mkdir(parents=True, exist_ok=True)
    core_pkg.mkdir(parents=True, exist_ok=True)
    (community_pkg / "rogue.py").write_text(
        "import okto_pulse.core.kg.providers.testing.settings_config as sc\n"
        "cfg = sc.SettingsKGConfig()\n",
        encoding="utf-8",
    )

    report = run_data_provider_ownership_gate(
        core_pkg,
        community_root=community_pkg,
    )

    assert report.ok is False
    assert any(
        hit["file"] == "rogue.py"
        and hit["symbol"] == "SettingsKGConfig"
        and hit["kind"] == "reference"
        for hit in report.community_settings_config_offenders
    )


def test_ts2_gate_blocks_aliased_instantiation_in_registry(tmp_path):
    # The SAME alias trick INSIDE registry.py is also blocked after R-P2-02.
    d = tmp_path / "kg" / "interfaces"
    d.mkdir(parents=True, exist_ok=True)
    (d / "registry.py").write_text(
        "from x import SqliteOutboxEventBus as Bus\nbus = Bus(sf)\n",
        encoding="utf-8",
    )
    report = run_data_provider_ownership_gate(tmp_path)
    assert report.ok is False
    assert report.new_data_consumers == [
        {
            "file": "kg/interfaces/registry.py",
            "symbol": "SqliteOutboxEventBus",
            "line": 2,
        }
    ]


# ===========================================================================
# prefer-provided fallback (register-before-fallback) + R-P2-03 fail-closed
# (the TR3 implicit-defaults escape is retired).
# ===========================================================================
def test_prefer_provided_base_registry_slots_not_overwritten():
    from okto_pulse.core.kg.interfaces import registry as reg_mod
    from okto_pulse.core.kg.interfaces.registry import (
        KGProviderRegistry,
        configure_kg_registry,
    )

    saved = reg_mod.capture_registry_state_for_tests()

    class _SF:
        def __call__(self):  # pragma: no cover - never invoked here
            raise RuntimeError("session factory not used in this unit test")

    class _SentinelBus:
        async def publish(self, event):  # minimal EventBus-ish marker
            return "x"

        async def subscribe(self, t, h):
            ...

        async def start(self):
            ...

        async def stop(self):
            ...

    class _SentinelAudit:
        ...

    sentinel_bus = _SentinelBus()
    sentinel_audit = _SentinelAudit()
    try:
        # R-P2-02/R-P2-03D: data slots and config are required — the composition
        # must supply them and the core must not overwrite them.
        base = KGProviderRegistry(
            event_bus=sentinel_bus,
            audit_repo=sentinel_audit,
            config=object(),
            graph_store=object(),
            cypher_executor=object(),
            graph_transaction=object(),
            graph_schema_manager=object(),
            graph_lifecycle=object(),
            graph_runtime_store=object(),
            global_discovery_runtime=object(),
            board_source_reader=object(),
        )
        configure_kg_registry(base_registry=base)
        r = reg_mod.get_kg_registry()
        # prefer-provided: the composition's slots are NOT replaced by the
        # session_factory auto-wire.
        assert r.event_bus is sentinel_bus
        assert r.audit_repo is sentinel_audit
    finally:
        reg_mod.restore_registry_state_for_tests(saved)


def test_tr3_non_composed_call_fails_closed():
    # R-P2-03: the TR3 retro-compat escape (a bare configure_kg_registry with no
    # base_registry / defaults_factory auto-wiring the embedded defaults) is GONE.
    # The non-composed path now FAILS CLOSED — there is no implicit Onda A escape.
    import pytest

    from okto_pulse.core.kg.interfaces import registry as reg_mod
    from okto_pulse.core.kg.interfaces.registry import (
        configure_kg_registry,
        reset_registry_for_tests,
    )

    saved = reg_mod.capture_registry_state_for_tests()

    class _SF:
        def __call__(self):  # pragma: no cover
            raise RuntimeError("not used")

    try:
        # The bare configure_kg_registry (no base / factory) fails closed BEFORE
        # it mutates the singleton.
        with pytest.raises(RuntimeError):
            configure_kg_registry()  # NO base / factory
        # With no successful composition, an unconfigured registry also fails
        # closed on consumption (no implicit Onda A lazy-init).
        reset_registry_for_tests()
        with pytest.raises(RuntimeError):
            reg_mod.get_kg_registry()
    finally:
        reg_mod.restore_registry_state_for_tests(saved)
