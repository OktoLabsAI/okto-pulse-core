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
from okto_pulse.core.domain.enums import SprintLaneType
from okto_pulse.core.inbound.enum_error_envelope import canonical_enum_error

INVALID_LANE = "release_validation"
EXPECTED_ENVELOPE = {
    "code": "invalid_lane_type",
    "field": "lane_type",
    "received_value": INVALID_LANE,
    "accepted_values": ["normal", "hotfix"],
    "mutation_applied": False,
}


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


def test_ts_lane_06_enum_bounded_and_service_transport_neutral():
    """TS-LANE-06: enum stays {normal, hotfix} and SprintService imports no
    REST/MCP/envelope component (stays transport-neutral)."""
    assert [member.value for member in SprintLaneType] == ["normal", "hotfix"]

    # The envelope's accepted set is derived from the enum, never hardcoded apart.
    assert EXPECTED_ENVELOPE["accepted_values"] == [m.value for m in SprintLaneType]

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


def test_canonical_enum_error_is_bounded_to_lane_type():
    """The normalizer only fires for an ``enum``-typed lane_type error; any other
    shape returns None so the caller keeps default handling."""
    lane_error = [{"type": "enum", "loc": ["body", "lane_type"], "input": INVALID_LANE}]
    assert canonical_enum_error(lane_error) == EXPECTED_ENVELOPE

    # Unmapped field → None.
    assert canonical_enum_error(
        [{"type": "enum", "loc": ["body", "status"], "input": "weird"}]
    ) is None
    # Mapped field but non-enum error type → None (do not over-match).
    assert canonical_enum_error(
        [{"type": "string_type", "loc": ["body", "lane_type"], "input": 5}]
    ) is None
    # Empty / missing loc → None.
    assert canonical_enum_error([{"type": "enum", "loc": [], "input": "x"}]) is None
