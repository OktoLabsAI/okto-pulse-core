"""R05-D (Onda B, CORE target) — data-provider ownership gate + registry
prefer-provided fallback.

  TS2 import-gate-recontamination — the fail-closed DataProviderOwnershipGate
       PASSES on the real core; a synthetic core→community import BLOCKS; a NEW
       data-adapter instantiation outside the single ledgered fallback BLOCKS.
  (prefer-provided) — configure_kg_registry does NOT overwrite an audit_repo /
       event_bus the base_registry already supplied (register-before-fallback);
       a non-composed call still auto-wires the embedded (TR3 retro-compat).
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
def test_ts2_gate_passes_on_real_core_with_single_ledgered_fallback():
    r1 = run_data_provider_ownership_gate()
    r2 = run_data_provider_ownership_gate()

    assert r1.as_dict() == r2.as_dict()  # deterministic
    assert r1.ok is True
    assert r1.community_import_offenders == []
    assert r1.new_data_consumers == []
    # The single ledgered fallback is registry.py, with a full removal contract.
    assert set(r1.ledger) == {"kg/interfaces/registry.py"}
    entry = r1.ledger["kg/interfaces/registry.py"]
    assert entry["owner"].startswith("core")
    assert entry["target_ports"] == ["EventBus", "AuditRepository", "KGConfig"]
    assert entry["removal_criterion"].startswith("spec #04")
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


def test_ts2_gate_allows_instantiation_in_ledgered_registry(tmp_path):
    # The ledgered fallback path (kg/interfaces/registry.py) MAY instantiate.
    d = tmp_path / "kg" / "interfaces"
    d.mkdir(parents=True, exist_ok=True)
    (d / "registry.py").write_text(
        "from x import SqliteOutboxEventBus\n"
        "bus = SqliteOutboxEventBus(sf)\n",
        encoding="utf-8",
    )
    report = run_data_provider_ownership_gate(tmp_path)
    assert report.ok is True
    assert report.new_data_consumers == []
    assert "kg/interfaces/registry.py" in LEDGERED_DATA_FALLBACK


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


def test_ts2_gate_allows_aliased_instantiation_in_ledgered_registry(tmp_path):
    # The SAME alias trick INSIDE the ledgered registry.py stays permitted.
    d = tmp_path / "kg" / "interfaces"
    d.mkdir(parents=True, exist_ok=True)
    (d / "registry.py").write_text(
        "from x import SqliteOutboxEventBus as Bus\nbus = Bus(sf)\n",
        encoding="utf-8",
    )
    report = run_data_provider_ownership_gate(tmp_path)
    assert report.ok is True
    assert report.new_data_consumers == []


# ===========================================================================
# prefer-provided fallback (register-before-fallback) + TR3 retro-compat.
# ===========================================================================
def test_prefer_provided_base_registry_slots_not_overwritten():
    from okto_pulse.core.kg.interfaces import registry as reg_mod
    from okto_pulse.core.kg.interfaces.registry import (
        KGProviderRegistry,
        configure_kg_registry,
    )

    saved = (reg_mod._registry, reg_mod._configured)

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
    sf = _SF()
    try:
        base = KGProviderRegistry(event_bus=sentinel_bus, audit_repo=sentinel_audit)
        configure_kg_registry(session_factory=sf, base_registry=base)
        r = reg_mod.get_kg_registry()
        # prefer-provided: the composition's slots are NOT replaced by the
        # session_factory auto-wire.
        assert r.event_bus is sentinel_bus
        assert r.audit_repo is sentinel_audit
    finally:
        reg_mod._registry, reg_mod._configured = saved


def test_tr3_non_composed_call_still_autowires_embedded():
    from okto_pulse.core.kg.interfaces import registry as reg_mod
    from okto_pulse.core.kg.interfaces.registry import configure_kg_registry
    from okto_pulse.core.kg.providers.embedded.sqlalchemy_audit_repo import (
        SqlAlchemyAuditRepository,
    )
    from okto_pulse.core.kg.providers.embedded.sqlite_outbox_event_bus import (
        SqliteOutboxEventBus,
    )

    saved = (reg_mod._registry, reg_mod._configured)

    class _SF:
        def __call__(self):  # pragma: no cover
            raise RuntimeError("not used")

    try:
        configure_kg_registry(session_factory=_SF())  # NO base_registry
        r = reg_mod.get_kg_registry()
        # retro-compat: the embedded auto-wire fires exactly as before.
        assert type(r.event_bus) is SqliteOutboxEventBus
        assert type(r.audit_repo) is SqlAlchemyAuditRepository
    finally:
        reg_mod._registry, reg_mod._configured = saved
