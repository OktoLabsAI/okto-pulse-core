"""Remaining enum normalizer and generic REST envelope boundedness guards.

Dedicated Sprint REST routes are removed; negative route coverage lives in
Community test_sprint_rest_retirement.py. Legacy enum migration/service cleanup
remains separate from the retired HTTP feature.
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from okto_pulse.community.app import install_request_validation_handler

class GenericRequest(BaseModel):
    title: str


def test_rest_unmapped_validation_error_uses_default():
    app = FastAPI()
    install_request_validation_handler(app)

    @app.post("/probe")
    def probe(data: GenericRequest):
        raise AssertionError("invalid request reached writer")

    with TestClient(app) as client:
        response = client.post("/probe", json={})
    assert response.status_code == 422
    assert isinstance(response.json()["detail"], list)
    assert response.json().get("code") != "invalid_lane_type"


def test_services_remain_transport_neutral():
    """Domain services do not import inbound exception rendering."""
    service_src = Path(
        "src/okto_pulse/core/services/main.py"
    ).read_text(encoding="utf-8")
    forbidden = (
        "enum_error_envelope",
        "canonical_enum_error",
        "RequestValidationError",
        "core.inbound",
        "core.mcp.server",
        "fastapi",
        "JSONResponse",
    )
    for token in forbidden:
        assert token not in service_src, (
            f"SprintService module must stay transport-neutral; found {token!r}"
        )
