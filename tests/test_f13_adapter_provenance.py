from __future__ import annotations

import asyncio
from pathlib import Path

from okto_pulse.core.application.boundary.adapter_provenance import (
    PUBLIC_CONTRACT,
    audit_adapter_provenance,
)
from okto_pulse.core.ports.f13 import (
    AdapterBridgeLedgerEntry,
    ApplicationScope,
    AdapterProvenanceRegistration,
    EditionOutcome,
    EditionPort,
)


PUBLIC = ("okto_pulse.core.ports",)


def _write(root: Path, relative: str, content: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_f13_edition_port_is_structural_and_scope_neutral() -> None:
    class FakeEdition:
        async def execute(self, scope, operation, payload):
            return EditionOutcome(operation, {"realm": scope.realm.realm_id, **payload})

    adapter = FakeEdition()
    result = asyncio.run(
        adapter.execute(ApplicationScope.local(), "inspect", {"value": 7})
    )

    assert isinstance(adapter, EditionPort)
    assert result == EditionOutcome("inspect", {"realm": "local", "value": 7})


def test_f13_gate_detects_every_bridge_shape(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/okto_pulse/community/adapters/bridge.py",
        "import importlib\n"
        "from okto_pulse.core.repositories.sqlalchemy.unit_of_work import "
        "SQLAlchemyUnitOfWork as LegacyUow\n"
        "AliasUow = LegacyUow\n"
        "__all__ = ['AliasUow']\n"
        "instance = AliasUow()\n"
        "module = importlib.import_module('okto_pulse.core.models.db')\n"
        "model = getattr(module, 'Board')\n",
    )

    report = audit_adapter_provenance(
        tmp_path,
        public_core_surfaces=PUBLIC,
        bridge_budget=0,
    )

    assert report["ok"] is False
    kinds = {item["bridge_kind"] for item in report["bridges"]}
    assert {
        "import_alias",
        "assignment_alias",
        "dunder_all_reexport",
        "constructor_target",
        "dynamic_import",
        "dynamic_getattr",
    } <= kinds
    assert report["bridge_count"] >= 6
    assert report["ledger_count"] == 0


def test_f13_real_adapter_must_be_locally_defined_against_public_port(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "src/okto_pulse/community/adapters/real.py",
        "from okto_pulse.core.ports.f13 import EditionOutcome, EditionPort\n"
        "class RealAdapter:\n"
        "    async def execute(self, scope, operation, payload):\n"
        "        return EditionOutcome(operation, payload)\n",
    )
    _write(tmp_path, "tests/test_real.py", "def test_contract():\n    pass\n")
    registration = AdapterProvenanceRegistration(
        adapter_key="real",
        owner="okto-pulse-community/adapters",
        implementation_module="okto_pulse.community.adapters.real",
        implementation_symbol="RealAdapter",
        port_module="okto_pulse.core.ports.f13",
        port_symbol="EditionPort",
        dependencies=("okto_pulse.core.ports.f13",),
        contract_test="tests/test_real.py::test_contract",
    )

    report = audit_adapter_provenance(
        tmp_path,
        public_core_surfaces=PUBLIC,
        registrations=(registration,),
    )

    assert report["ok"] is True, report
    assert report["bridge_count"] == 0
    assert report["registration_violations"] == ()
    assert set(report["inventory_by_classification"]) == {PUBLIC_CONTRACT}


def test_f13_imported_symbol_does_not_qualify_as_community_implementation(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "src/okto_pulse/community/adapters/nominal.py",
        "from okto_pulse.core.ports.f13 import EditionPort as NominalAdapter\n",
    )
    _write(tmp_path, "tests/test_nominal.py", "def test_contract():\n    pass\n")
    registration = AdapterProvenanceRegistration(
        adapter_key="nominal",
        owner="okto-pulse-community/adapters",
        implementation_module="okto_pulse.community.adapters.nominal",
        implementation_symbol="NominalAdapter",
        port_module="okto_pulse.core.ports.f13",
        port_symbol="EditionPort",
        dependencies=("okto_pulse.core.ports.f13",),
        contract_test="tests/test_nominal.py::test_contract",
    )

    report = audit_adapter_provenance(
        tmp_path,
        public_core_surfaces=PUBLIC,
        registrations=(registration,),
    )

    assert report["ok"] is False
    assert report["registration_violations"] == (
        {
            "adapter_key": "nominal",
            "category": "implementation_symbol_not_locally_defined",
            "symbol": "NominalAdapter",
        },
    )


def test_f13_removed_bridge_requires_ledger_entry_removal_in_same_change(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "src/okto_pulse/community/adapters/clean.py",
        "from okto_pulse.core.ports.f13 import EditionPort\n",
    )
    stale = AdapterBridgeLedgerEntry(
        file_path="src/okto_pulse/community/adapters/clean.py",
        scope="<module>",
        bridge_kind="import_from",
        target="okto_pulse.core.models.db.Board",
        owner="okto-pulse-community/adapters",
        target_port="okto_pulse.core.ports.application_persistence.ApplicationRecord",
        removal_path="replace the legacy model import with the public record port",
        withdrawal_criterion="contract replay passes without the private module",
    )

    report = audit_adapter_provenance(
        tmp_path,
        public_core_surfaces=PUBLIC,
        bridge_ledger=(stale,),
        bridge_budget=0,
    )

    assert report["ok"] is False
    assert report["bridge_count"] == 0
    assert report["stale_ledger"] == (
        {
            "file_path": stale.file_path,
            "scope": stale.scope,
            "bridge_kind": stale.bridge_kind,
            "target": stale.target,
            "owner": stale.owner,
            "target_port": stale.target_port,
            "removal_path": stale.removal_path,
            "withdrawal_criterion": stale.withdrawal_criterion,
        },
    )


def test_f13_specific_private_surface_overrides_public_parent(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/okto_pulse/community/adapters/registry_reach_in.py",
        "from okto_pulse.core.kg.interfaces import get_kg_registry\n",
    )

    report = audit_adapter_provenance(
        tmp_path,
        public_core_surfaces=("okto_pulse.core.kg.interfaces",),
        private_core_surfaces=(
            "okto_pulse.core.kg.interfaces.get_kg_registry",
        ),
    )

    assert report["ok"] is False
    assert report["bridge_count"] == 1
    assert report["bridges"][0]["target"] == (
        "okto_pulse.core.kg.interfaces.get_kg_registry"
    )
