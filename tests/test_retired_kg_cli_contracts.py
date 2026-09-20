"""Maintenance CLI retirement removes exclusive code, preserving historical reads."""

import importlib.util

import pytest

from okto_pulse.core.application import kg_operations
from okto_pulse.core.application.boundary.public_contract_manifest import (
    is_public_core_contract,
)


def test_dedup_maintenance_module_and_cli_export_wrapper_are_absent():
    assert importlib.util.find_spec("okto_pulse.core.kg.dedup_migration") is None
    assert not hasattr(kg_operations, "export_board_jsonld")


def test_retired_dedup_is_not_a_public_adapter_contract():
    assert not is_public_core_contract("okto_pulse.core.kg.dedup_migration")


@pytest.mark.parametrize("module", [
    "okto_pulse.core.ports.kg_curation_proposals",
    "okto_pulse.core.application.spec_materialization",
    "okto_pulse.core.domain.spec_materialization",
    "okto_pulse.core.ports.spec_materialization",
])
def test_exclusive_retired_maintenance_modules_are_absent(module):
    assert importlib.util.find_spec(module) is None
