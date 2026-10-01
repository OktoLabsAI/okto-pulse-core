"""The current contract exposes no operational Sprint authority."""

import pytest

from okto_pulse.core.application import errors
from okto_pulse.core.application.service_catalog import CoreApplicationServiceCatalog
from okto_pulse.core.application.use_cases.allowed_transitions import allowed_transitions_for_status
from okto_pulse.core.application.use_cases.base import CommandValidationError
from okto_pulse.core.domain.sdlc_registry import SDLC_REGISTRY
from okto_pulse.core import models
from okto_pulse.core.models import schemas
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.services import main


def test_service_and_catalog_expose_no_sprint_operations():
    assert not hasattr(main, "SprintService")
    assert not hasattr(main, "SprintQAService")
    assert not hasattr(main, "SprintOperationError")
    assert not hasattr(errors, "SprintOperationError")
    assert not hasattr(CoreApplicationServiceCatalog, "sprints")
    assert not hasattr(ApplicationServiceCatalog, "sprints")
    assert not hasattr(CoreApplicationServiceCatalog, "sprint_qa")
    assert not hasattr(ApplicationServiceCatalog, "sprint_qa")
    for namespace in (models, schemas):
        assert not hasattr(namespace, "SprintQACreate")
        assert not hasattr(namespace, "SprintQAAnswer")
    assert "sprint" not in SDLC_REGISTRY
    for definition in SDLC_REGISTRY.values():
        for edges in definition.transitions.values():
            for edge in edges:
                assert all("sprint" not in value for value in (*edge.preconditions, *edge.reason_codes))


@pytest.mark.parametrize("status", ["planned", "active", "closed"])
def test_retired_sprint_cannot_discover_a_lifecycle(status):
    with pytest.raises(CommandValidationError, match="Invalid entity_type"):
        allowed_transitions_for_status("sprint", status)
