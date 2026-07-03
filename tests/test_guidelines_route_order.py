"""Regression: the literal GET /guidelines/default-candidates route (owned by
default_board_config_router) must NOT be shadowed by the parametric
GET /guidelines/{guideline_id} route (guidelines_router).

FastAPI/Starlette match routes in registration order, so if guidelines_router were
included before default_board_config_router, the path `/guidelines/default-candidates`
would match `/guidelines/{guideline_id}` first (guideline_id="default-candidates") and
return 404 "Guideline not found" — which is exactly the bug this guards against.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_guidelines_route_order.py
"""

from __future__ import annotations

from fastapi import FastAPI

from okto_pulse.core.api.router import api_router


def _openapi_paths():
    app = FastAPI()
    app.include_router(api_router)
    return app.openapi()["paths"]


def test_default_candidates_route_is_not_shadowed():
    paths = _openapi_paths()
    literal = "/api/v1/guidelines/default-candidates"
    parametric = "/api/v1/guidelines/{guideline_id}"

    assert literal in paths, "no route registered for /guidelines/default-candidates"
    assert paths[literal]["get"]["operationId"].startswith(
        "list_default_guideline_candidates"
    )
    assert list(paths).index(literal) < list(paths).index(parametric), (
        "The literal /guidelines/default-candidates route must be registered before "
        "the parametric /guidelines/{guideline_id} route."
    )


def test_guideline_by_id_route_still_resolves():
    # The parametric route must keep working for a real id (no regression from the order).
    paths = _openapi_paths()
    operation = paths["/api/v1/guidelines/{guideline_id}"]["get"]
    assert operation["operationId"].startswith("get_guideline")
