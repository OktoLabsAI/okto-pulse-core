"""Retired storage values must not be exported as live Core domain types."""

import importlib

import pytest


@pytest.mark.parametrize("module_name", ["okto_pulse.core.domain.enums", "okto_pulse.core.models"])
@pytest.mark.parametrize("name", ["SprintStatus", "SprintLaneType"])
def test_retired_enums_are_absent_from_core_public_domain(module_name, name):
    module = importlib.import_module(module_name)
    assert name not in module.__all__
    assert name not in dir(module)
    with pytest.raises(AttributeError):
        getattr(module, name)
