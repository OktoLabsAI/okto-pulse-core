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
from starlette.routing import Match

from okto_pulse.core.api.router import api_router


def _first_matching_route(method: str, path: str):
    """Return the first route (in registration order) that FULLY matches, i.e. the one
    FastAPI/Starlette would actually dispatch to."""
    app = FastAPI()
    app.include_router(api_router)
    scope = {"type": "http", "method": method, "path": path, "headers": []}
    for route in app.router.routes:
        match, _child = route.matches(scope)
        if match == Match.FULL:
            return route
    return None


def test_default_candidates_route_is_not_shadowed():
    route = _first_matching_route("GET", "/api/v1/guidelines/default-candidates")
    assert route is not None, "no route matched /guidelines/default-candidates"
    assert route.endpoint.__name__ == "list_default_guideline_candidates", (
        f"/guidelines/default-candidates resolved to {route.endpoint.__name__!r} — the "
        "literal route is being shadowed by the parametric /guidelines/{guideline_id}. "
        "Keep default_board_config_router registered BEFORE guidelines_router."
    )


def test_guideline_by_id_route_still_resolves():
    # The parametric route must keep working for a real id (no regression from the order).
    route = _first_matching_route("GET", "/api/v1/guidelines/11111111-2222-3333-4444-555555555555")
    assert route is not None
    assert route.endpoint.__name__ == "get_guideline"
