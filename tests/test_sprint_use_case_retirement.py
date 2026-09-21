"""Retired Sprint application entry points must not survive as callable exports."""

import importlib.util

import pytest

from okto_pulse.core.application import use_cases
from okto_pulse.core.application.use_cases import mutation_permissions


@pytest.mark.parametrize("operation", (
    "ListBoardSprints", "ListSprints", "GetSprint", "ListSprintHistory", "SuggestSprints",
    "CreateSprint", "UpdateSprint", "MoveSprint", "DeleteSprint", "SubmitSprintEvaluation",
    "AssignSprintTasks", "UnassignSprintTasks",
))
@pytest.mark.parametrize("contract", ("Command", "Result", "UseCase"))
def test_retired_application_contract_is_not_exported(operation, contract):
    name = operation + contract
    assert name not in use_cases.__all__
    assert not hasattr(use_cases, name)


def test_retired_module_and_exclusive_permission_builders_are_absent():
    assert importlib.util.find_spec("okto_pulse.core.application.use_cases.sprints_crud") is None
    for name in ("sprint_requirement", "sprint_update_permission_requirements"):
        assert name not in mutation_permissions.__all__
        assert not hasattr(mutation_permissions, name)
