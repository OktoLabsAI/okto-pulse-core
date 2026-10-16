"""F4: detailed operational pending trees are retired; Health is aggregate."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import kg_routes
from okto_pulse.community.api.auth_deps import require_principal
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.router import api_router


@pytest.mark.parametrize("depth", [0, 1, 2, 4])
def test_pending_tree_is_absent_before_authority_or_storage(depth):
    def forbidden():
        pytest.fail("retired pending tree resolved runtime dependencies")

    assert not hasattr(kg_routes, "list_pending_tree")
    app = FastAPI()
    app.include_router(api_router)
    app.dependency_overrides[require_principal] = forbidden
    app.dependency_overrides[get_unit_of_work] = forbidden
    assert "/api/v1/kg/boards/{board_id}/pending/tree" not in app.openapi()["paths"]
    with TestClient(app) as client:
        response = client.get("/api/v1/kg/boards/board/pending/tree", params={"depth": depth})
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}
