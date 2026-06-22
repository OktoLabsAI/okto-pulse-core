"""Regression: structured REST/MCP error exceptions must NOT be frozen dataclasses.

A frozen dataclass that subclasses Exception raises
``dataclasses.FrozenInstanceError: cannot assign to field '__traceback__'`` when Python
assigns ``__traceback__`` while the exception propagates — which happens when it unwinds
through a FastAPI ``Depends`` that yields (an AsyncExitStack). The result is a generic
HTTP 500 instead of the intended structured ``status_code`` (e.g. a 422 with an
actionable error code). This was the live `design_system_*` mockup-gate bug: blocking
violations surfaced as 500 instead of design_system_required/not_found/version_mismatch/
evidence_missing.

This guards every structured-error exception against re-freezing, and reproduces the
exact propagation path for DesignSystemError.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_design_system_error_propagation.py
"""

from __future__ import annotations

import dataclasses

import pytest
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from okto_pulse.core.kg.kg_service import KGToolError
from okto_pulse.core.services.amendment_revision_api import AmendmentRevisionApiError
from okto_pulse.core.services.bug_regression_preview import (
    BugRegressionScenarioPreviewError,
)
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
)
from okto_pulse.core.services.design_system import DesignSystemError

# Every structured error that is a @dataclass Exception raised through REST/MCP routes.
_STRUCTURED_ERRORS = [
    DesignSystemError,
    AmendmentRevisionApiError,
    BugRegressionScenarioPreviewError,
    DefaultBoardConfigurationError,
    KGToolError,
]


@pytest.mark.parametrize("exc_cls", _STRUCTURED_ERRORS, ids=lambda c: c.__name__)
def test_structured_error_is_a_non_frozen_dataclass(exc_cls):
    assert dataclasses.is_dataclass(exc_cls)
    assert exc_cls.__dataclass_params__.frozen is False, (
        f"{exc_cls.__name__} is a FROZEN dataclass Exception — Python cannot set "
        "__traceback__ on it during propagation (e.g. through a FastAPI Depends/yield "
        "exit stack), so it raises FrozenInstanceError and returns HTTP 500 instead of "
        "its status_code. Drop frozen=True."
    )


def _app_raising(exc: Exception) -> FastAPI:
    app = FastAPI()

    @app.exception_handler(DesignSystemError)
    async def _handler(_request, e: DesignSystemError):  # noqa: ANN001
        return JSONResponse(status_code=e.status_code, content=e.to_dict())

    async def _yielding_dep():
        # A dependency that yields is entered/exited via an AsyncExitStack; on an
        # exception the stack assigns __traceback__ to it — the frozen-dataclass trap.
        yield "ctx"

    @app.post("/boom")
    async def _boom(_dep=Depends(_yielding_dep)):  # noqa: ANN001
        raise exc

    return app


def test_design_system_error_through_yield_dependency_returns_status_not_500():
    app = _app_raising(
        DesignSystemError("design_system_evidence_missing", "missing evidence", 422, {"k": 1})
    )
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.post("/boom")
    assert resp.status_code == 422, resp.text  # frozen dataclass would make this a 500
    body = resp.json()
    assert body["code"] == "design_system_evidence_missing"
    assert body["status_code"] == 422
