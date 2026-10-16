"""F3/F5: Sprint forecasting has no active Core contract or application seam."""

from importlib.util import find_spec

import pytest

from okto_pulse.core.application import use_cases
from okto_pulse.core.application import service_catalog
from okto_pulse.core.ports.application_services import AnalyticsOperations
from okto_pulse.core.ports.relational_application import RelationalApplicationAdapter
from okto_pulse.core.testing.fake_saas_relational import (
    FakeSaaSRelationalApplicationAdapter,
)


@pytest.mark.parametrize(
    "module",
    [
        "okto_pulse.core.application.use_cases.delivery_forecast",
        "okto_pulse.core.services.delivery_forecast",
        "okto_pulse.core.ports.delivery_forecast",
    ],
)
def test_sprint_forecast_module_is_absent(module):
    assert find_spec(module) is None


def test_sprint_forecast_has_no_export_or_application_seam():
    for name in (
        "DeliveryForecastCommand",
        "DeliveryForecastResult",
        "DeliveryForecastUseCase",
    ):
        assert not hasattr(use_cases, name)
        assert name not in use_cases.__all__
    assert not hasattr(AnalyticsOperations, "delivery_forecast")
    assert not hasattr(RelationalApplicationAdapter, "delivery_forecast_read")
    assert not hasattr(FakeSaaSRelationalApplicationAdapter, "delivery_forecast_read")
    for value in vars(service_catalog).values():
        if isinstance(value, type) and value.__module__ == service_catalog.__name__:
            assert not hasattr(value, "delivery_forecast")
