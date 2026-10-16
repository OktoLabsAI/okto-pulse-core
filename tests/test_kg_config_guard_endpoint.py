"""F4: runtime tuning is absent from the public application contract."""
from okto_pulse.core.application.use_cases import operational_rest
from okto_pulse.core.application.service_catalog import CoreApplicationServiceCatalog
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog


def test_public_runtime_tuning_contract_is_retired():
    for name in (
        "GetRuntimeSettingsCommand", "PutRuntimeSettingsCommand",
        "GetRuntimeSettingsUseCase", "PutRuntimeSettingsUseCase",
        "_require_runtime_settings_authority",
    ):
        assert not hasattr(operational_rest, name)
    for contract in (CoreApplicationServiceCatalog, ApplicationServiceCatalog):
        assert not hasattr(contract, "get_runtime_settings")
        assert not hasattr(contract, "put_runtime_settings")
